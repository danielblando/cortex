package ring

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"net/http"
	"time"

	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	dto "github.com/prometheus/client_model/go"
)

// MultiReadRing implements ReadRing interface for shuffle-sharded multi-rings
type MultiReadRing struct {
	cfg MultiRingConfig

	primaryRing   ReadRing
	secondaryRing ReadRing

	// For percentage-based distribution
	hasher hash.Hash32

	logger log.Logger

	// Metrics (available in all MultiReadRing instances, including shuffle shards)
	primaryRingRequests   prometheus.Counter
	secondaryRingRequests prometheus.Counter
	ringSelectionDuration prometheus.Histogram
	totalRequests         prometheus.Counter

	reg prometheus.Registerer
}

// MultiRing wraps two rings and distributes traffic between them based on configuration
// Implements RingInterface (ReadRing + Service + http.Handler)
type MultiRing struct {
	*services.BasicService
	*MultiReadRing

	// Full ring interfaces (for service management)
	primaryRingInterface   RingInterface
	secondaryRingInterface RingInterface
}

// NewMultiReadRing creates a new MultiReadRing (for shuffle shards)
func NewMultiReadRing(cfg MultiRingConfig, primaryRing, secondaryRing ReadRing, logger log.Logger, reg prometheus.Registerer) (*MultiReadRing, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid multi-ring configuration: %w", err)
	}

	if primaryRing == nil {
		return nil, fmt.Errorf("primary ring cannot be nil")
	}
	if secondaryRing == nil {
		return nil, fmt.Errorf("secondary ring cannot be nil")
	}

	mr := &MultiReadRing{
		cfg:           cfg,
		primaryRing:   primaryRing,
		secondaryRing: secondaryRing,
		hasher:        fnv.New32a(),
		logger:        logger,
		reg:           reg,
	}

	// Initialize metrics if registerer is provided
	if reg != nil {
		mr.primaryRingRequests = promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_distributor_multi_ring_primary_requests_total",
			Help: "Total number of requests sent to primary ring",
		})

		mr.secondaryRingRequests = promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_distributor_multi_ring_secondary_requests_total",
			Help: "Total number of requests sent to secondary ring",
		})

		mr.ringSelectionDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_distributor_multi_ring_selection_duration_seconds",
			Help:    "Time spent selecting which ring to use",
			Buckets: prometheus.DefBuckets,
		})

		mr.totalRequests = promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_distributor_multi_ring_requests_total",
			Help: "Total number of requests processed by multi-ring",
		})
	}

	return mr, nil
}

// NewMultiRing creates a new multi-ring that distributes traffic between two rings
func NewMultiRing(cfg MultiRingConfig, primaryRing, secondaryRing RingInterface, logger log.Logger, reg prometheus.Registerer) (*MultiRing, error) {
	// Create the MultiReadRing core with metrics
	multiReadRing, err := NewMultiReadRing(cfg, primaryRing, secondaryRing, logger, reg)
	if err != nil {
		return nil, err
	}

	mr := &MultiRing{
		MultiReadRing:          multiReadRing,
		primaryRingInterface:   primaryRing,
		secondaryRingInterface: secondaryRing,
	}

	// Initialize the BasicService to manage underlying ring services
	mr.BasicService = services.NewBasicService(mr.starting, mr.running, mr.stopping)

	level.Info(logger).Log(
		"msg", "multi-ring initialized",
		"primary_type", cfg.PrimaryRingType,
		"secondary_type", cfg.SecondaryRingType,
		"primary_percentage", cfg.PrimaryRingPercentage,
		"secondary_percentage", 100.0-cfg.PrimaryRingPercentage,
	)

	return mr, nil
}

// selectRing determines which ring to use for a given key
func (mr *MultiReadRing) selectRing(key uint32) ReadRing {
	// Use metrics if available
	if mr.ringSelectionDuration != nil {
		timer := prometheus.NewTimer(mr.ringSelectionDuration)
		defer timer.ObserveDuration()
	}

	ring := mr.selectRingByPercentage(key)

	// Update metrics based on which ring was selected
	if ring == mr.primaryRing {
		if mr.primaryRingRequests != nil {
			mr.primaryRingRequests.Inc()
		}
	} else {
		if mr.secondaryRingRequests != nil {
			mr.secondaryRingRequests.Inc()
		}
	}

	return ring
}

// selectRingByPercentage selects ring based on percentage distribution
func (mr *MultiReadRing) selectRingByPercentage(key uint32) ReadRing {
	mr.hasher.Reset()
	if err := binary.Write(mr.hasher, binary.BigEndian, key); err != nil {
		level.Error(mr.logger).Log("msg", "failed to hash key, using primary ring", "err", err)
		return mr.primaryRing
	}

	hashValue := mr.hasher.Sum32()
	// Convert hash to percentage with decimal precision (0.00 to 99.99)
	percentage := float64(hashValue%10000) / 100.0

	if percentage < mr.cfg.PrimaryRingPercentage {
		return mr.primaryRing
	}

	return mr.secondaryRing
}

// ReadRing methods for MultiReadRing

// Get returns n (or more) instances which form the replicas for the given key
func (mr *MultiReadRing) Get(key uint32, op Operation, bufDescs []InstanceDesc, bufHosts []string, bufZones map[string]int) (ReplicationSet, error) {
	// Track that multi-ring Get method was called
	if mr.totalRequests != nil {
		mr.totalRequests.Inc()
	}

	ring := mr.selectRing(key)
	return ring.Get(key, op, bufDescs, bufHosts, bufZones)
}

// GetAllHealthy returns all healthy instances from both rings
func (mr *MultiReadRing) GetAllHealthy(op Operation) (ReplicationSet, error) {
	primaryRS, err1 := mr.primaryRing.GetAllHealthy(op)
	secondaryRS, err2 := mr.secondaryRing.GetAllHealthy(op)

	// If both rings fail, return the error
	if err1 != nil && err2 != nil {
		return ReplicationSet{}, fmt.Errorf("both rings failed: primary=%v, secondary=%v", err1, err2)
	}

	// If one ring fails, log warning but continue with the other
	if err1 != nil {
		level.Warn(mr.logger).Log("msg", "primary ring failed in GetAllHealthy", "err", err1)
		return secondaryRS, nil
	}
	if err2 != nil {
		level.Warn(mr.logger).Log("msg", "secondary ring failed in GetAllHealthy", "err", err2)
		return primaryRS, nil
	}

	// Merge the replication sets, removing duplicates
	instanceMap := make(map[string]InstanceDesc)
	for _, inst := range primaryRS.Instances {
		instanceMap[inst.Addr] = inst
	}
	for _, inst := range secondaryRS.Instances {
		instanceMap[inst.Addr] = inst
	}

	merged := ReplicationSet{
		Instances:           make([]InstanceDesc, 0, len(instanceMap)),
		MaxErrors:           primaryRS.MaxErrors + secondaryRS.MaxErrors,
		MaxUnavailableZones: max(primaryRS.MaxUnavailableZones, secondaryRS.MaxUnavailableZones),
	}

	for _, inst := range instanceMap {
		merged.Instances = append(merged.Instances, inst)
	}

	return merged, nil
}

// GetAllInstanceDescs returns all instance descriptions from both rings
func (mr *MultiReadRing) GetAllInstanceDescs(op Operation) ([]InstanceDesc, []InstanceDesc, error) {
	primaryHealthy, primaryUnhealthy, err1 := mr.primaryRing.GetAllInstanceDescs(op)
	secondaryHealthy, secondaryUnhealthy, err2 := mr.secondaryRing.GetAllInstanceDescs(op)

	if err1 != nil && err2 != nil {
		return nil, nil, fmt.Errorf("both rings failed: primary=%v, secondary=%v", err1, err2)
	}

	// Merge results, handling partial failures
	var allHealthy, allUnhealthy []InstanceDesc

	if err1 == nil {
		allHealthy = append(allHealthy, primaryHealthy...)
		allUnhealthy = append(allUnhealthy, primaryUnhealthy...)
	} else {
		level.Warn(mr.logger).Log("msg", "primary ring failed in GetAllInstanceDescs", "err", err1)
	}

	if err2 == nil {
		allHealthy = append(allHealthy, secondaryHealthy...)
		allUnhealthy = append(allUnhealthy, secondaryUnhealthy...)
	} else {
		level.Warn(mr.logger).Log("msg", "secondary ring failed in GetAllInstanceDescs", "err", err2)
	}

	// Remove duplicates based on instance address
	allHealthy = mr.removeDuplicateInstances(allHealthy)
	allUnhealthy = mr.removeDuplicateInstances(allUnhealthy)

	return allHealthy, allUnhealthy, nil
}

// GetInstanceDescsForOperation returns instance descriptions for a specific operation from both rings
func (mr *MultiReadRing) GetInstanceDescsForOperation(op Operation) (map[string]InstanceDesc, error) {
	primaryDescs, err1 := mr.primaryRing.GetInstanceDescsForOperation(op)
	secondaryDescs, err2 := mr.secondaryRing.GetInstanceDescsForOperation(op)

	if err1 != nil && err2 != nil {
		return nil, fmt.Errorf("both rings failed: primary=%v, secondary=%v", err1, err2)
	}

	merged := make(map[string]InstanceDesc)

	if err1 == nil {
		for id, desc := range primaryDescs {
			merged[id] = desc
		}
	} else {
		level.Warn(mr.logger).Log("msg", "primary ring failed in GetInstanceDescsForOperation", "err", err1)
	}

	if err2 == nil {
		for id, desc := range secondaryDescs {
			merged[id] = desc
		}
	} else {
		level.Warn(mr.logger).Log("msg", "secondary ring failed in GetInstanceDescsForOperation", "err", err2)
	}

	return merged, nil
}

// GetReplicationSetForOperation returns the replication set for a specific operation from both rings
func (mr *MultiReadRing) GetReplicationSetForOperation(op Operation) (ReplicationSet, error) {
	// For this method, we return the union of both rings since it's used for operations
	// that need to consider all available instances
	return mr.GetAllHealthy(op)
}

// ReplicationFactor returns the maximum replication factor of both rings
func (mr *MultiReadRing) ReplicationFactor() int {
	return max(mr.primaryRing.ReplicationFactor(), mr.secondaryRing.ReplicationFactor())
}

// InstancesCount returns the total number of unique instances across both rings
func (mr *MultiReadRing) InstancesCount() int {
	primaryDescs, _ := mr.primaryRing.GetInstanceDescsForOperation(Read)
	secondaryDescs, _ := mr.secondaryRing.GetInstanceDescsForOperation(Read)

	// Count unique instances by address
	instanceSet := make(map[string]struct{})
	for _, desc := range primaryDescs {
		instanceSet[desc.Addr] = struct{}{}
	}
	for _, desc := range secondaryDescs {
		instanceSet[desc.Addr] = struct{}{}
	}

	return len(instanceSet)
}

// ShuffleShard returns a subring for the provided identifier
func (mr *MultiReadRing) ShuffleShard(identifier string, size int) ReadRing {
	// For shuffle sharding, we need to create a new multi-read-ring with sharded subrings
	primaryShard := mr.primaryRing.ShuffleShard(identifier, size)
	secondaryShard := mr.secondaryRing.ShuffleShard(identifier, size)

	// Don't pass registerer to shuffle shards to avoid metric name conflicts
	// Shuffle shards will not have their own metrics
	shardedMultiReadRing, err := NewMultiReadRing(mr.cfg, primaryShard, secondaryShard, mr.logger, nil)
	if err != nil {
		level.Error(mr.logger).Log("msg", "failed to create sharded multi-read-ring", "err", err)
		return mr.primaryRing.ShuffleShard(identifier, size)
	}

	// Inherit metrics from parent to avoid creating duplicate metrics
	// All shuffle shards will contribute to the same parent metrics
	shardedMultiReadRing.primaryRingRequests = mr.primaryRingRequests
	shardedMultiReadRing.secondaryRingRequests = mr.secondaryRingRequests
	shardedMultiReadRing.ringSelectionDuration = mr.ringSelectionDuration
	shardedMultiReadRing.totalRequests = mr.totalRequests

	return shardedMultiReadRing
}

// ShuffleShardWithZoneStability returns a subring with zone stability
func (mr *MultiReadRing) ShuffleShardWithZoneStability(identifier string, size int) ReadRing {
	return mr.primaryRing.ShuffleShardWithZoneStability(identifier, size)
}

// GetInstanceState returns the state of an instance from either ring
func (mr *MultiReadRing) GetInstanceState(instanceID string) (InstanceState, error) {
	// Try primary ring first
	state, err := mr.primaryRing.GetInstanceState(instanceID)
	if err == nil {
		return state, nil
	}

	// If not found in primary, try secondary
	state, err2 := mr.secondaryRing.GetInstanceState(instanceID)
	if err2 == nil {
		return state, nil
	}

	// Return the original error if not found in either ring
	return PENDING, err
}

// GetInstanceIdByAddr returns the instance ID by address from either ring
func (mr *MultiReadRing) GetInstanceIdByAddr(addr string) (string, error) {
	// Try primary ring first
	id, err := mr.primaryRing.GetInstanceIdByAddr(addr)
	if err == nil {
		return id, nil
	}

	// If not found in primary, try secondary
	id, err2 := mr.secondaryRing.GetInstanceIdByAddr(addr)
	if err2 == nil {
		return id, nil
	}

	// Return the original error if not found in either ring
	return "", err
}

// ShuffleShardWithLookback returns a subring with lookback period
func (mr *MultiReadRing) ShuffleShardWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time) ReadRing {
	return mr.primaryRing.ShuffleShardWithLookback(identifier, size, lookbackPeriod, now)
}

// HasInstance returns whether either ring contains the instance
func (mr *MultiReadRing) HasInstance(instanceID string) bool {
	return mr.primaryRing.HasInstance(instanceID) || mr.secondaryRing.HasInstance(instanceID)
}

// CleanupShuffleShardCache cleans up shuffle shard cache for both rings
func (mr *MultiReadRing) CleanupShuffleShardCache(identifier string) {
	mr.primaryRing.CleanupShuffleShardCache(identifier)
	mr.secondaryRing.CleanupShuffleShardCache(identifier)
}

// removeDuplicateInstances removes duplicate instances based on address
func (mr *MultiReadRing) removeDuplicateInstances(instances []InstanceDesc) []InstanceDesc {
	seen := make(map[string]struct{})
	result := make([]InstanceDesc, 0, len(instances))

	for _, inst := range instances {
		if _, exists := seen[inst.Addr]; !exists {
			seen[inst.Addr] = struct{}{}
			result = append(result, inst)
		}
	}

	return result
}

// MultiRing methods (with metrics) - these override the embedded MultiReadRing methods

// Get returns n (or more) instances which form the replicas for the given key (MultiRing version)
// Note: Metrics are now handled in MultiReadRing.selectRing()
func (mr *MultiRing) Get(key uint32, op Operation, bufDescs []InstanceDesc, bufHosts []string, bufZones map[string]int) (ReplicationSet, error) {
	return mr.MultiReadRing.Get(key, op, bufDescs, bufHosts, bufZones)
}

// GetPrimaryRing returns the primary ring (for testing/debugging)
func (mr *MultiRing) GetPrimaryRing() RingInterface {
	return mr.primaryRingInterface
}

// GetSecondaryRing returns the secondary ring (for testing/debugging)
func (mr *MultiRing) GetSecondaryRing() RingInterface {
	return mr.secondaryRingInterface
}

// GetConfig returns the multi-ring configuration (for testing/debugging)
func (mr *MultiRing) GetConfig() MultiRingConfig {
	return mr.cfg
}

// ServeHTTP implements http.Handler to display both rings
func (mr *MultiRing) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Set content type
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Write HTML header
	w.Write([]byte(`<!DOCTYPE html>
<html>
<head>
    <title>Multi-Ring Status</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .ring-section { margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }
        .ring-title { font-size: 18px; font-weight: bold; margin-bottom: 10px; }
        .config-info { background-color: #f5f5f5; padding: 10px; margin: 10px 0; border-radius: 3px; }
        .metrics { background-color: #e8f4fd; padding: 10px; margin: 10px 0; border-radius: 3px; }
        iframe { width: 100%; height: 400px; border: 1px solid #ccc; }
    </style>
</head>
<body>
    <h1>Multi-Ring Status</h1>
`))

	// Write configuration information
	w.Write([]byte(fmt.Sprintf(`
    <div class="config-info">
        <h3>Configuration</h3>
        <p><strong>Primary Ring Type:</strong> %s (%0.1f%% traffic)</p>
        <p><strong>Secondary Ring Type:</strong> %s (%0.1f%% traffic)</p>
    </div>
`, mr.cfg.PrimaryRingType, mr.cfg.PrimaryRingPercentage, mr.cfg.SecondaryRingType, mr.cfg.GetSecondaryRingPercentage())))

	// Write metrics information if available
	if mr.MultiReadRing.primaryRingRequests != nil && mr.MultiReadRing.secondaryRingRequests != nil {
		// Get metric values using a DTO (Data Transfer Object)
		primaryMetric := &dto.Metric{}
		secondaryMetric := &dto.Metric{}
		totalMetric := &dto.Metric{}

		var primaryValue, secondaryValue, totalValue float64
		if err := mr.MultiReadRing.primaryRingRequests.Write(primaryMetric); err == nil && primaryMetric.Counter != nil {
			primaryValue = primaryMetric.Counter.GetValue()
		}
		if err := mr.MultiReadRing.secondaryRingRequests.Write(secondaryMetric); err == nil && secondaryMetric.Counter != nil {
			secondaryValue = secondaryMetric.Counter.GetValue()
		}
		if mr.MultiReadRing.totalRequests != nil {
			if err := mr.MultiReadRing.totalRequests.Write(totalMetric); err == nil && totalMetric.Counter != nil {
				totalValue = totalMetric.Counter.GetValue()
			}
		}

		totalRequests := primaryValue + secondaryValue
		var primaryPercent, secondaryPercent float64
		if totalRequests > 0 {
			primaryPercent = (primaryValue / totalRequests) * 100
			secondaryPercent = (secondaryValue / totalRequests) * 100
		}

		// Get histogram metrics for ring selection duration
		var avgDuration, totalSelections float64
		if mr.MultiReadRing.ringSelectionDuration != nil {
			histogramMetric := &dto.Metric{}
			if err := mr.MultiReadRing.ringSelectionDuration.Write(histogramMetric); err == nil && histogramMetric.Histogram != nil {
				totalSelections = float64(histogramMetric.Histogram.GetSampleCount())
				if totalSelections > 0 {
					avgDuration = histogramMetric.Histogram.GetSampleSum() / totalSelections
				}
			}
		}

		w.Write([]byte(fmt.Sprintf(`
    <div class="metrics">
        <h3>Traffic Distribution Metrics</h3>
        <table style="border-collapse: collapse; width: 100%%;">
            <tr style="background-color: #f0f0f0;">
                <th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Ring</th>
                <th style="border: 1px solid #ddd; padding: 8px; text-align: right;">Requests</th>
                <th style="border: 1px solid #ddd; padding: 8px; text-align: right;">Percentage</th>
            </tr>
            <tr>
                <td style="border: 1px solid #ddd; padding: 8px;">Primary (%s)</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: right;">%.0f</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: right;">%.1f%%</td>
            </tr>
            <tr>
                <td style="border: 1px solid #ddd; padding: 8px;">Secondary (%s)</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: right;">%.0f</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: right;">%.1f%%</td>
            </tr>
            <tr style="background-color: #f0f0f0; font-weight: bold;">
                <td style="border: 1px solid #ddd; padding: 8px;">Total</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: right;">%.0f</td>
                <td style="border: 1px solid #ddd; padding: 8px; text-align: right;">100.0%%</td>
            </tr>
        </table>
        <p style="margin-top: 10px;"><small>
            <strong>Total Multi-Ring Requests:</strong> %.0f<br>
            <strong>Configured Distribution:</strong> Primary %.1f%% / Secondary %.1f%%<br>
            <strong>Ring Selection:</strong> %.0f selections, avg %.3fms per selection<br>
            <strong>Prometheus Metrics:</strong><br>
            • cortex_distributor_multi_ring_requests_total<br>
            • cortex_distributor_multi_ring_primary_requests_total<br>
            • cortex_distributor_multi_ring_secondary_requests_total<br>
            • cortex_distributor_multi_ring_selection_duration_seconds
        </small></p>
    </div>
`, mr.cfg.PrimaryRingType, primaryValue, primaryPercent,
			mr.cfg.SecondaryRingType, secondaryValue, secondaryPercent,
			totalRequests,
			totalValue,
			mr.cfg.PrimaryRingPercentage, mr.cfg.GetSecondaryRingPercentage(),
			totalSelections, avgDuration*1000))) // Convert to milliseconds
	} else {
		// Metrics not available (likely no registerer provided or no traffic yet)
		w.Write([]byte(`
    <div class="metrics">
        <h3>Multi-Ring Usage Metrics</h3>
        <p><strong>Status:</strong> No metrics data available</p>
        <p><strong>Possible reasons:</strong></p>
        <ul>
            <li><strong>Multi-ring is not enabled</strong> (check -ring.multi-ring.enabled=true)</li>
            <li>No traffic has been processed yet</li>
            <li>Metrics collection not enabled (no registerer provided)</li>
        </ul>
        <p><strong>Configured Distribution:</strong> Primary ` + fmt.Sprintf("%.1f", mr.cfg.PrimaryRingPercentage) + `% / Secondary ` + fmt.Sprintf("%.1f", mr.cfg.GetSecondaryRingPercentage()) + `%</p>
        <p><small><strong>To enable multi-ring:</strong> Set -ring.multi-ring.enabled=true in your configuration</small></p>
        <p><small><strong>Key Metric:</strong> cortex_distributor_multi_ring_requests_total will show > 0 when multi-ring is active</small></p>
    </div>
`))
	}

	// Primary Ring Section
	w.Write([]byte(fmt.Sprintf(`
    <div class="ring-section">
        <div class="ring-title">Primary Ring (%s)</div>
        <iframe src="/ingester/ring/primary"></iframe>
    </div>
`, mr.cfg.PrimaryRingType)))

	// Secondary Ring Section
	w.Write([]byte(fmt.Sprintf(`
    <div class="ring-section">
        <div class="ring-title">Secondary Ring (%s)</div>
        <iframe src="/ingester/ring/secondary"></iframe>
    </div>
`, mr.cfg.SecondaryRingType)))

	// Write HTML footer
	w.Write([]byte(`
</body>
</html>`))
}

// Service lifecycle methods

func (mr *MultiRing) starting(ctx context.Context) error {
	// Start underlying ring services if they implement the Service interface
	if primaryService, ok := mr.primaryRingInterface.(services.Service); ok {
		if err := services.StartAndAwaitRunning(ctx, primaryService); err != nil {
			return fmt.Errorf("failed to start primary ring service: %w", err)
		}
	}

	if secondaryService, ok := mr.secondaryRingInterface.(services.Service); ok {
		if err := services.StartAndAwaitRunning(ctx, secondaryService); err != nil {
			return fmt.Errorf("failed to start secondary ring service: %w", err)
		}
	}

	level.Info(mr.logger).Log("msg", "multi-ring started successfully")
	return nil
}

func (mr *MultiRing) running(ctx context.Context) error {
	// Just wait for context cancellation
	<-ctx.Done()
	return nil
}

func (mr *MultiRing) stopping(_ error) error {
	// Stop underlying ring services if they implement the Service interface
	if primaryService, ok := mr.primaryRingInterface.(services.Service); ok {
		if err := services.StopAndAwaitTerminated(context.Background(), primaryService); err != nil {
			level.Warn(mr.logger).Log("msg", "failed to stop primary ring service", "err", err)
		}
	}

	if secondaryService, ok := mr.secondaryRingInterface.(services.Service); ok {
		if err := services.StopAndAwaitTerminated(context.Background(), secondaryService); err != nil {
			level.Warn(mr.logger).Log("msg", "failed to stop secondary ring service", "err", err)
		}
	}

	level.Info(mr.logger).Log("msg", "multi-ring stopped")
	return nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
