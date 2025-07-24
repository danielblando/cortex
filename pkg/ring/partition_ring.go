package ring

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/ring/kv"
	shardUtil "github.com/cortexproject/cortex/pkg/ring/shard"
	"github.com/cortexproject/cortex/pkg/util/backoff"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// PartitionRing represents a ring that distributes data across partitions.
// Instead of using tokens for distribution, it uses partitions as the primary
// unit of distribution.
type PartitionRing struct {
	services.Service

	cfg           Config
	strategy      ReplicationStrategy
	KVClient      kv.Client
	numPartitions uint32
	key           string

	mtx    sync.RWMutex
	desc   *PartitionRingDesc
	tokens []uint32

	// Map from token to partition ID
	tokenToPartition map[uint32]string

	// Map from partition ID to instances
	partitionOwners map[string][]string

	// Map from instance ID to instance desc
	instances map[string]InstanceDesc

	// List of zones for which there's at least 1 instance in the ring
	ringZones []string

	// Metrics
	operationDurationHistogram *prometheus.HistogramVec
	instanceStateGauge         *prometheus.GaugeVec
	partitionHealthGauge       *prometheus.GaugeVec
	partitionBalanceGauge      *prometheus.GaugeVec
	zoneBalanceGauge           *prometheus.GaugeVec

	// For shuffle sharding
	shuffledSubringCache map[subringCacheKey]*PartitionRing

	logger log.Logger
}

// NewPartitionRing creates a new partition-based ring.
func NewPartitionRing(cfg Config, name, key string, logger log.Logger, reg prometheus.Registerer) (RingInterface, error) {
	codec := GetPartitionCodec()
	// Suffix all client names with "-ring" to denote this kv client is used by the ring
	store, err := kv.NewClient(
		cfg.KVStore,
		codec,
		kv.RegistererWithKVName(reg, name+"-partition-ring"),
		logger,
	)
	if err != nil {
		return nil, err
	}

	return NewPartitionWithStoreClientAndStrategy(cfg, name, key, store, NewDefaultReplicationStrategy(), reg, logger)
}

// NewPartitionRingForKey creates a new partition-based ring with a specific key.
func NewPartitionWithStoreClientAndStrategy(cfg Config, name, key string, store kv.Client, strategy ReplicationStrategy, reg prometheus.Registerer, logger log.Logger) (RingInterface, error) {
	if cfg.ReplicationFactor <= 0 {
		return nil, fmt.Errorf("ReplicationFactor must be greater than zero: %d", cfg.ReplicationFactor)
	}

	r := &PartitionRing{
		cfg:                  cfg,
		key:                  key,
		strategy:             strategy,
		KVClient:             store,
		partitionOwners:      make(map[string][]string),
		tokenToPartition:     make(map[uint32]string),
		instances:            make(map[string]InstanceDesc),
		ringZones:            []string{},
		shuffledSubringCache: make(map[subringCacheKey]*PartitionRing),
		logger:               log.NewNopLogger(),
	}

	// Initialize metrics
	r.operationDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "partition_ring_operation_duration_seconds",
		Help:      "Time taken to perform operations on the partition ring.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"operation"})

	r.instanceStateGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "partition_ring_instance_state",
		Help:      "State of instances in the partition ring.",
	}, []string{"instance", "state"})

	r.partitionHealthGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "partition_ring_partition_health",
		Help:      "Health status of partitions in the ring.",
	}, []string{"status"})

	r.partitionBalanceGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "partition_ring_partition_balance",
		Help:      "Balance of partitions across instances.",
	}, []string{"metric"})

	r.zoneBalanceGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "partition_ring_zone_balance",
		Help:      "Balance of partitions across availability zones.",
	}, []string{"metric"})

	if reg != nil {
		reg.MustRegister(
			r.operationDurationHistogram,
			r.instanceStateGauge,
			r.partitionHealthGauge,
			r.partitionBalanceGauge,
			r.zoneBalanceGauge,
		)
	}

	r.Service = services.NewBasicService(r.starting, r.loop, nil).WithName(fmt.Sprintf("%s ring client", name))
	return r, nil
}

// Start starts watching the ring for changes.
func (r *PartitionRing) starting(ctx context.Context) error {
	if r.KVClient == nil {
		return fmt.Errorf("no KV client configured")
	}

	value, err := r.KVClient.Get(ctx, r.key)
	if err != nil {
		return errors.Wrap(err, "unable to initialise ring state")
	}
	if value != nil {
		r.updateRingState(value.(*PartitionRingDesc))
	} else {
		level.Info(r.logger).Log("msg", "ring doesn't exist in KV store yet")
	}

	return nil
}

func (r *PartitionRing) loop(ctx context.Context) error {
	// Update the ring metrics at start of the main loop.
	r.mtx.Lock()
	// Update metrics
	r.updateInstanceMetrics()
	r.updatePartitionMetrics()
	r.mtx.Unlock()

	// Start watching for changes
	r.KVClient.WatchKey(ctx, r.key, func(value interface{}) bool {
		if value == nil {
			level.Error(r.logger).Log("msg", "partition ring deleted from KV store")
			return true
		}

		desc, ok := value.(*PartitionRingDesc)
		if !ok {
			level.Error(r.logger).Log("msg", "received invalid partition ring description", "type", fmt.Sprintf("%T", value))
			return true
		}

		r.updateRingState(desc)
		return true
	})
	return nil
}

// updateRing updates the ring state based on the provided description.
func (r *PartitionRing) updateRingState(desc *PartitionRingDesc) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.desc = desc
	r.tokens = desc.GetTokens()
	r.tokenToPartition = make(map[uint32]string)
	r.partitionOwners = make(map[string][]string)
	r.instances = make(map[string]InstanceDesc)

	// Build token to partition mapping
	for id, p := range desc.Partitions {
		// Only include tokens from ACTIVE partitions
		if p.State == P_ACTIVE {
			for _, token := range p.Tokens {
				r.tokenToPartition[token] = id
			}
		}

		// Build partition owners mapping
		owners := make([]string, 0, len(p.Instances))
		for iid, inst := range p.Instances {
			owners = append(owners, iid)
			r.instances[iid] = inst
		}
		r.partitionOwners[id] = owners
	}

	// Build the list of zones
	zones := make(map[string]struct{})
	for _, instance := range r.instances {
		if instance.Zone != "" {
			zones[instance.Zone] = struct{}{}
		}
	}

	r.ringZones = make([]string, 0, len(zones))
	for zone := range zones {
		r.ringZones = append(r.ringZones, zone)
	}
	sort.Strings(r.ringZones)

	// Invalidate the shuffle shard cache
	r.shuffledSubringCache = make(map[subringCacheKey]*PartitionRing)

	// Update metrics
	r.updateInstanceMetrics()
	r.updatePartitionMetrics()
}

// Get returns the instances that own a given key.
// The key is mapped to a partition, and then the partition's owners are returned.
func (r *PartitionRing) Get(key uint32, op Operation, bufDescs []InstanceDesc, bufHosts []string, bufZones map[string]int) (ReplicationSet, error) {
	timer := prometheus.NewTimer(r.operationDurationHistogram.WithLabelValues("get"))
	defer timer.ObserveDuration()

	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.desc == nil || len(r.tokens) == 0 {
		return ReplicationSet{}, ErrEmptyRing
	}

	// Find the partition for this key
	var partitionID string
	if len(r.tokens) > 0 {
		pos := sort.Search(len(r.tokens), func(i int) bool {
			return r.tokens[i] > key
		})
		if pos >= len(r.tokens) {
			pos = 0
		}
		token := r.tokens[pos]
		partitionID = r.tokenToPartition[token]
	} else {
		return ReplicationSet{}, ErrEmptyRing
	}

	// Get the partition
	partition, ok := r.desc.Partitions[partitionID]
	if !ok {
		return ReplicationSet{}, fmt.Errorf("partition %s not found", partitionID)
	}

	// Check if the partition is in a valid state for the operation
	if op == Write && partition.State != P_ACTIVE {
		return ReplicationSet{}, fmt.Errorf("partition %s is not active for write operations", partitionID)
	}

	if op == Read && partition.State == P_NON_READY {
		return ReplicationSet{}, fmt.Errorf("partition %s is not ready for read operations", partitionID)
	}

	// Get the instances for this partition
	owners, ok := r.partitionOwners[partitionID]
	if !ok || len(owners) == 0 {
		return ReplicationSet{}, fmt.Errorf("no instances found for partition %s", partitionID)
	}

	// Build the replication set
	rs := ReplicationSet{
		Instances: make([]InstanceDesc, 0, len(owners)),
		MaxErrors: 0, // For partition ring, we don't allow errors
	}

	// Add all healthy instances from the partition
	storageLastUpdate := r.KVClient.LastUpdateTime(r.key)
	for _, id := range owners {
		inst, ok := r.instances[id]
		if !ok {
			continue
		}

		// Only include healthy instances
		if r.IsHealthy(&inst, op, storageLastUpdate) {
			rs.Instances = append(rs.Instances, inst)
		}
	}

	// Check if we have enough instances
	if len(rs.Instances) < int(r.cfg.ReplicationFactor) {
		return ReplicationSet{}, fmt.Errorf("partition %s does not have enough healthy instances", partitionID)
	}

	// For partition ring, we don't need to apply the replication strategy
	// because all instances in the partition are replicas of each other
	return rs, nil
}

// RegisterInstance adds or updates an instance in the ring.
func (r *PartitionRing) RegisterInstance(instanceID string, desc InstanceDesc, ownedPartitions []string) error {
	timer := prometheus.NewTimer(r.operationDurationHistogram.WithLabelValues("register_instance"))
	defer timer.ObserveDuration()

	if r.KVClient == nil {
		return fmt.Errorf("no KV client configured")
	}

	ctx := context.Background()
	return r.KVClient.CAS(ctx, r.cfg.KVStore.Prefix, func(in interface{}) (out interface{}, retry bool, err error) {
		var ringDesc *PartitionRingDesc
		if in == nil {
			ringDesc = NewPartitionRingDesc()
			ringDesc.EnsurePartitionCount(int(r.numPartitions))
		} else {
			var ok bool
			ringDesc, ok = in.(*PartitionRingDesc)
			if !ok {
				return nil, false, fmt.Errorf("expected *PartitionRingDesc, got %T", in)
			}
		}

		// Add instance to specified partitions
		for _, partitionID := range ownedPartitions {
			partition, ok := ringDesc.Partitions[partitionID]
			if !ok {
				continue
			}
			partition.AddIngester(instanceID, desc.Addr, desc.Zone, desc.State)
		}

		return ringDesc, true, nil
	})
}

// DeregisterInstance removes an instance from the ring.
func (r *PartitionRing) DeregisterInstance(instanceID string) {
	timer := prometheus.NewTimer(r.operationDurationHistogram.WithLabelValues("deregister_instance"))
	defer timer.ObserveDuration()

	if r.KVClient == nil {
		level.Error(r.logger).Log("msg", "cannot deregister instance, no KV client configured")
		return
	}

	ctx := context.Background()
	err := r.KVClient.CAS(ctx, r.cfg.KVStore.Prefix, func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			return nil, false, nil
		}

		ringDesc, ok := in.(*PartitionRingDesc)
		if !ok {
			return nil, false, fmt.Errorf("expected *PartitionRingDesc, got %T", in)
		}

		// Remove instance from all partitions
		for _, partition := range ringDesc.Partitions {
			if partition.Instances != nil {
				delete(partition.Instances, instanceID)
			}
		}

		return ringDesc, true, nil
	})

	if err != nil {
		level.Error(r.logger).Log("msg", "failed to deregister instance", "instance", instanceID, "err", err)
	}
}

// updateInstanceMetrics updates metrics related to instance state and ownership
func (r *PartitionRing) updateInstanceMetrics() {
	// Update instance state metrics
	states := []InstanceState{ACTIVE, PENDING, JOINING, LEAVING}

	// Reset all metrics
	for _, state := range states {
		for id := range r.instances {
			r.instanceStateGauge.WithLabelValues(id, state.String()).Set(0)
		}
	}

	// Set current state
	for id, inst := range r.instances {
		r.instanceStateGauge.WithLabelValues(id, inst.State.String()).Set(1) // Using the original String() method for instance state is fine
	}
}

// updatePartitionMetrics updates metrics related to partition health and replication
func (r *PartitionRing) updatePartitionMetrics() {
	activeCount := 0
	readonlyCount := 0
	nonReadyCount := 0

	for id, p := range r.desc.Partitions {
		switch p.State {
		case P_ACTIVE:
			// Check if the partition has enough instances
			if len(r.partitionOwners[id]) >= int(r.cfg.ReplicationFactor) {
				activeCount++
			} else {
				nonReadyCount++
			}
		case P_READONLY:
			readonlyCount++
		case P_NON_READY:
			nonReadyCount++
		}
	}

	r.partitionHealthGauge.WithLabelValues("active").Set(float64(activeCount))
	r.partitionHealthGauge.WithLabelValues("readonly").Set(float64(readonlyCount))
	r.partitionHealthGauge.WithLabelValues("non_ready").Set(float64(nonReadyCount))
}

// GetAllHealthy returns all healthy instances in the ring.
func (r *PartitionRing) GetAllHealthy(op Operation) (ReplicationSet, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.desc == nil {
		return ReplicationSet{}, ErrEmptyRing
	}

	instances := make([]InstanceDesc, 0, len(r.instances))
	for _, inst := range r.instances {
		if op == Write && inst.State != ACTIVE {
			continue
		}
		if op == Read && (inst.State != ACTIVE && inst.State != LEAVING) {
			continue
		}
		instances = append(instances, inst)
	}

	return ReplicationSet{
		Instances: instances,
	}, nil
}

// GetReplicationSetForOperation returns the replication set for a specific operation.
func (r *PartitionRing) GetReplicationSetForOperation(op Operation) (ReplicationSet, error) {
	return r.GetAllHealthy(op)
}

// PartitionForKey returns the partition ID for a given key.
func (r *PartitionRing) PartitionForKey(key uint32) (string, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.desc == nil || len(r.tokens) == 0 {
		// If the ring is empty, return a default partition ID based on the key
		// This ensures that even if the ring is not yet initialized, we can still route requests
		return fmt.Sprintf("p-%d", key%r.numPartitions), nil
	}

	pos := sort.Search(len(r.tokens), func(i int) bool {
		return r.tokens[i] > key
	})
	if pos >= len(r.tokens) {
		pos = 0
	}
	token := r.tokens[pos]
	partitionID := r.tokenToPartition[token]

	// Check if the partition is active
	partition, ok := r.desc.Partitions[partitionID]
	if !ok {
		return "", fmt.Errorf("partition %s not found", partitionID)
	}

	if partition.State != P_ACTIVE {
		// If the partition is not active, find the next active partition
		for i := 1; i < len(r.tokens); i++ {
			nextPos := (pos + i) % len(r.tokens)
			nextToken := r.tokens[nextPos]
			nextPartitionID := r.tokenToPartition[nextToken]
			nextPartition, ok := r.desc.Partitions[nextPartitionID]
			if ok && nextPartition.State == P_ACTIVE {
				return nextPartitionID, nil
			}
		}

		// If no active partition is found, return an error
		return "", fmt.Errorf("no active partition found for key %d", key)
	}

	return partitionID, nil
}

// GetInstancesForPartition returns the instances that own a specific partition.
func (r *PartitionRing) GetInstancesForPartition(partitionID string) ([]InstanceDesc, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.desc == nil {
		return nil, ErrEmptyRing
	}

	partition, ok := r.desc.Partitions[partitionID]
	if !ok {
		return nil, fmt.Errorf("partition %s not found", partitionID)
	}

	// Check if the partition is in a valid state
	if partition.State == P_NON_READY {
		return nil, fmt.Errorf("partition %s is not ready", partitionID)
	}

	instances := make([]InstanceDesc, 0, len(partition.Instances))
	for _, inst := range partition.Instances {
		// Only include healthy instances
		if r.IsHealthy(&inst, Read, r.KVClient.LastUpdateTime(r.key)) {
			instances = append(instances, inst)
		}
	}

	// Check if we have enough instances
	if len(instances) < int(r.cfg.ReplicationFactor) {
		return nil, fmt.Errorf("partition %s does not have enough healthy instances", partitionID)
	}

	return instances, nil
}

// CountPartitionsWithState returns the number of partitions in a specific state.
func (r *PartitionRing) CountPartitionsWithState(state PartitionState) int {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.desc == nil {
		return 0
	}

	count := 0
	for _, p := range r.desc.Partitions {
		if p.State == state {
			count++
		}
	}
	return count
}

// WaitRingStability waits for the ring to stabilize.
func (r *PartitionRing) WaitRingStability(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	backoff := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 1 * time.Second,
		MaxRetries: 0, // Unlimited
	})

	for backoff.Ongoing() {
		stable := r.isStable()
		if stable {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("ring did not stabilize within %v", timeout)
		}

		backoff.Wait()
	}

	return backoff.Err()
}

// isStable returns true if the ring is considered stable.
func (r *PartitionRing) isStable() bool {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.desc == nil {
		return false
	}

	// Check if all partitions are active
	for _, p := range r.desc.Partitions {
		if p.State != P_ACTIVE {
			return false
		}

		// Check if each partition has enough instances
		if len(p.Instances) < int(r.cfg.ReplicationFactor) {
			return false
		}
	}

	return true
}

// ReplicationFactor returns the replication factor of the ring.
func (r *PartitionRing) ReplicationFactor() int {
	return int(r.cfg.ReplicationFactor)
}

// IsPartitionRing returns if ring is using partitioning
func (r *PartitionRing) IsPartitionRing() bool {
	return r.cfg.PartitionRingEnabled
}

// InstancesCount returns the number of instances in the ring.
func (r *PartitionRing) InstancesCount() int {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	return len(r.instances)
}

// GetAllInstanceDescs returns a slice of healthy and unhealthy InstanceDesc.
func (r *PartitionRing) GetAllInstanceDescs(op Operation) ([]InstanceDesc, []InstanceDesc, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.desc == nil || len(r.instances) == 0 {
		return nil, nil, ErrEmptyRing
	}

	healthyInstances := make([]InstanceDesc, 0, len(r.instances))
	unhealthyInstances := make([]InstanceDesc, 0, len(r.instances))
	storageLastUpdate := r.KVClient.LastUpdateTime(r.key)

	for _, instance := range r.instances {
		if r.IsHealthy(&instance, op, storageLastUpdate) {
			healthyInstances = append(healthyInstances, instance)
		} else {
			unhealthyInstances = append(unhealthyInstances, instance)
		}
	}

	return healthyInstances, unhealthyInstances, nil
}

// GetInstanceDescsForOperation returns map of InstanceDesc with instance ID as the keys.
func (r *PartitionRing) GetInstanceDescsForOperation(op Operation) (map[string]InstanceDesc, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.desc == nil || len(r.instances) == 0 {
		return map[string]InstanceDesc{}, ErrEmptyRing
	}

	storageLastUpdate := r.KVClient.LastUpdateTime(r.key)
	instanceDescs := make(map[string]InstanceDesc, len(r.instances))

	for id, instance := range r.instances {
		if r.IsHealthy(&instance, op, storageLastUpdate) {
			instanceDescs[id] = instance
		}
	}

	return instanceDescs, nil
}

// GetInstanceState returns the current state of an instance or an error if the
// instance does not exist in the ring.
func (r *PartitionRing) GetInstanceState(instanceID string) (InstanceState, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	instance, ok := r.instances[instanceID]
	if !ok {
		return PENDING, ErrInstanceNotFound
	}

	return instance.State, nil
}

// GetInstanceIdByAddr returns the instance id from its address or an error if the
// instance does not exist in the ring.
func (r *PartitionRing) GetInstanceIdByAddr(addr string) (string, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	for id, instance := range r.instances {
		if instance.Addr == addr {
			return id, nil
		}
	}

	return "", ErrInstanceNotFound
}

// ShuffleShard returns a subring for the provided identifier (eg. a tenant ID)
// and size (number of instances).
func (r *PartitionRing) ShuffleShard(identifier string, size int) ReadRing {
	return r.shuffleShardWithCache(identifier, size, false)
}

// ShuffleShardWithZoneStability does the same as ShuffleShard but using a different shuffle sharding algorithm.
func (r *PartitionRing) ShuffleShardWithZoneStability(identifier string, size int) ReadRing {
	return r.shuffleShardWithCache(identifier, size, true)
}

// ShuffleShardWithLookback is like ShuffleShard() but the returned subring includes
// all instances that have been part of the identifier's shard since "now - lookbackPeriod".
func (r *PartitionRing) ShuffleShardWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time) ReadRing {
	// Nothing to do if the shard size is not smaller than the actual ring.
	if size <= 0 || r.InstancesCount() <= size {
		return r
	}

	return r.shuffleShard(identifier, size, lookbackPeriod, now, false)
}

// CleanupShuffleShardCache should delete cached shuffle-shard subrings for given identifier.
func (r *PartitionRing) CleanupShuffleShardCache(identifier string) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if r.shuffledSubringCache == nil {
		return
	}

	for key := range r.shuffledSubringCache {
		if key.identifier == identifier {
			delete(r.shuffledSubringCache, key)
		}
	}
}

// IsHealthy checks whether an instance is healthy for the given operation.
func (r *PartitionRing) IsHealthy(instance *InstanceDesc, op Operation, storageLastUpdate time.Time) bool {
	return instance.IsHealthy(op, r.cfg.HeartbeatTimeout, storageLastUpdate)
}

func (r *PartitionRing) getCachedShuffledSubring(identifier string, size int, zoneStableSharding bool) *PartitionRing {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.shuffledSubringCache == nil {
		return nil
	}

	return r.shuffledSubringCache[subringCacheKey{identifier: identifier, shardSize: size, zoneStableSharding: zoneStableSharding}]
}

func (r *PartitionRing) setCachedShuffledSubring(identifier string, size int, zoneStableSharding bool, subring *PartitionRing) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if r.shuffledSubringCache == nil {
		r.shuffledSubringCache = make(map[subringCacheKey]*PartitionRing)
	}

	r.shuffledSubringCache[subringCacheKey{identifier: identifier, shardSize: size, zoneStableSharding: zoneStableSharding}] = subring
}

func (r *PartitionRing) shuffleShardWithCache(identifier string, size int, zoneStableSharding bool) *PartitionRing {
	// Nothing to do if the shard size is not smaller than the actual ring.
	if size <= 0 || r.InstancesCount() <= size {
		return r
	}

	if cached := r.getCachedShuffledSubring(identifier, size, zoneStableSharding); cached != nil {
		return cached
	}

	result := r.shuffleShard(identifier, size, 0, time.Now(), zoneStableSharding)

	r.setCachedShuffledSubring(identifier, size, zoneStableSharding, result)
	return result
}

func (r *PartitionRing) shuffleShard(identifier string, size int, lookbackPeriod time.Duration, now time.Time, zoneStableSharding bool) *PartitionRing {
	lookbackUntil := now.Add(-lookbackPeriod).Unix()

	r.mtx.RLock()
	defer r.mtx.RUnlock()

	var (
		numInstancesPerZone    int
		actualZones            []string
		zonesWithExtraInstance int
	)

	if r.cfg.ZoneAwarenessEnabled {
		if zoneStableSharding {
			numInstancesPerZone = size / len(r.ringZones)
			zonesWithExtraInstance = size % len(r.ringZones)
		} else {
			numInstancesPerZone = shardUtil.ShuffleShardExpectedInstancesPerZone(size, len(r.ringZones))
		}
		actualZones = r.ringZones
	} else {
		numInstancesPerZone = size
		actualZones = []string{""}
	}

	// We need to create a new partition ring with only the selected instances
	subring := &PartitionRing{
		cfg:              r.cfg,
		strategy:         r.strategy,
		KVClient:         r.KVClient,
		numPartitions:    r.numPartitions,
		desc:             &PartitionRingDesc{Partitions: make(map[string]*PartitionDesc)},
		tokenToPartition: make(map[uint32]string),
		partitionOwners:  make(map[string][]string),
		instances:        make(map[string]InstanceDesc),
		logger:           r.logger,
	}

	// Select instances for each zone
	for _, zone := range actualZones {
		// Get instances for this zone
		zoneInstances := make([]string, 0)
		for id, instance := range r.instances {
			if instance.Zone == zone || zone == "" {
				zoneInstances = append(zoneInstances, id)
			}
		}

		// Sort instances by ID for stability
		sort.Strings(zoneInstances)

		// Select the first N instances
		numInstances := numInstancesPerZone
		if zone != "" && zonesWithExtraInstance > 0 {
			numInstances++
			zonesWithExtraInstance--
		}

		// Add selected instances to the subring
		for i := 0; i < min(numInstances, len(zoneInstances)); i++ {
			instanceID := zoneInstances[i]
			instance := r.instances[instanceID]

			// If lookback is enabled, check if the instance was registered before the lookback period
			if lookbackPeriod > 0 && instance.RegisteredTimestamp > lookbackUntil {
				continue
			}

			// Add the instance to the subring
			subring.instances[instanceID] = instance

			// Find which partitions this instance belongs to
			for partitionID, partition := range r.desc.Partitions {
				if _, ok := partition.Instances[instanceID]; ok {
					// Add the partition to the subring
					if _, exists := subring.desc.Partitions[partitionID]; !exists {
						subring.desc.Partitions[partitionID] = &PartitionDesc{
							State:               partition.State,
							Tokens:              partition.Tokens,
							Instances:           make(map[string]InstanceDesc),
							RegisteredTimestamp: partition.RegisteredTimestamp,
							Timestamp:           partition.Timestamp,
						}
					}
					subring.desc.Partitions[partitionID].Instances[instanceID] = instance

					// Update the partition owners
					if _, exists := subring.partitionOwners[partitionID]; !exists {
						subring.partitionOwners[partitionID] = make([]string, 0)
					}
					subring.partitionOwners[partitionID] = append(subring.partitionOwners[partitionID], instanceID)

					// Update the token to partition mapping
					for _, token := range partition.Tokens {
						subring.tokenToPartition[token] = partitionID
					}
				}
			}
		}
	}

	// Update the tokens list
	subring.tokens = subring.desc.GetTokens()

	return subring
}

// HasInstance returns whether the ring contains an instance matching the provided instanceID.
func (r *PartitionRing) HasInstance(instanceID string) bool {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	_, ok := r.instances[instanceID]
	return ok
}

// NumPartitions returns the number of partitions in the ring.
func (r *PartitionRing) NumPartitions() int {
	return int(r.numPartitions)
}

// GetInstances returns all instances in the ring.
func (r *PartitionRing) GetInstances() map[string]interface{} {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	result := make(map[string]interface{})
	for id, inst := range r.instances {
		result[id] = inst
	}
	return result
}

// SetPartitionState sets the state of a partition.
func (r *PartitionRing) SetPartitionState(partitionID string, state PartitionState) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if r.desc == nil {
		return ErrEmptyRing
	}

	partition, ok := r.desc.Partitions[partitionID]
	if !ok {
		return fmt.Errorf("partition %s not found", partitionID)
	}

	// Update the state
	partition.State = state
	partition.Timestamp = time.Now().Unix()

	// Store the updated ring
	ctx := context.Background()
	return r.KVClient.CAS(ctx, r.cfg.KVStore.Prefix, func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			return nil, false, fmt.Errorf("ring doesn't exist in KV store yet")
		}

		ringDesc, ok := in.(*PartitionRingDesc)
		if !ok {
			return nil, false, fmt.Errorf("expected *PartitionRingDesc, got %T", in)
		}

		// Update the partition state
		if p, ok := ringDesc.Partitions[partitionID]; ok {
			p.State = state
			p.Timestamp = time.Now().Unix()
		}

		return ringDesc, true, nil
	})
}

// IsPartitionActive checks if a partition is active.
func (r *PartitionRing) IsPartitionActive(partitionID string) (bool, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.desc == nil {
		return false, ErrEmptyRing
	}

	partition, ok := r.desc.Partitions[partitionID]
	if !ok {
		return false, fmt.Errorf("partition %s not found", partitionID)
	}

	return partition.State == P_ACTIVE, nil
}

// IsPartitionReadOnly checks if a partition is in read-only mode.
func (r *PartitionRing) IsPartitionReadOnly(partitionID string) (bool, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.desc == nil {
		return false, ErrEmptyRing
	}

	partition, ok := r.desc.Partitions[partitionID]
	if !ok {
		return false, fmt.Errorf("partition %s not found", partitionID)
	}

	return partition.State == P_READONLY, nil
}

// CheckPartitionHealth checks if a partition has enough healthy instances.
func (r *PartitionRing) CheckPartitionHealth(partitionID string) (PartitionState, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.desc == nil {
		return P_NON_READY, ErrEmptyRing
	}

	partition, ok := r.desc.Partitions[partitionID]
	if !ok {
		return P_NON_READY, fmt.Errorf("partition %s not found", partitionID)
	}

	// Count healthy instances per zone
	healthyInstancesPerZone := make(map[string]int)
	for _, inst := range partition.Instances {
		if r.IsHealthy(&inst, Read, r.KVClient.LastUpdateTime(r.key)) {
			healthyInstancesPerZone[inst.Zone]++
		}
	}

	// Check if we have enough healthy instances across zones
	if len(healthyInstancesPerZone) < int(r.cfg.ReplicationFactor) {
		return P_NON_READY, nil
	}

	// Check if we have at most one instance per zone
	for _, count := range healthyInstancesPerZone {
		if count > 1 {
			return P_NON_READY, nil
		}
	}

	return partition.State, nil
}

// CreatePartition creates a new partition with the given ID.
func (r *PartitionRing) CreatePartition(partitionID string, tokens []uint32) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if r.desc == nil {
		return ErrEmptyRing
	}

	// Check if the partition already exists
	if _, ok := r.desc.Partitions[partitionID]; ok {
		return fmt.Errorf("partition %s already exists", partitionID)
	}

	// Create the partition
	ctx := context.Background()
	return r.KVClient.CAS(ctx, r.cfg.KVStore.Prefix, func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			return nil, false, fmt.Errorf("ring doesn't exist in KV store yet")
		}

		ringDesc, ok := in.(*PartitionRingDesc)
		if !ok {
			return nil, false, fmt.Errorf("expected *PartitionRingDesc, got %T", in)
		}

		// Add the partition
		ringDesc.AddPartition(partitionID, P_NON_READY, tokens, time.Now())

		return ringDesc, true, nil
	})
}

// RemovePartition removes a partition from the ring.
func (r *PartitionRing) RemovePartition(partitionID string) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if r.desc == nil {
		return ErrEmptyRing
	}

	// Check if the partition exists
	if _, ok := r.desc.Partitions[partitionID]; !ok {
		return fmt.Errorf("partition %s not found", partitionID)
	}

	// Remove the partition
	ctx := context.Background()
	return r.KVClient.CAS(ctx, r.cfg.KVStore.Prefix, func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			return nil, false, fmt.Errorf("ring doesn't exist in KV store yet")
		}

		ringDesc, ok := in.(*PartitionRingDesc)
		if !ok {
			return nil, false, fmt.Errorf("expected *PartitionRingDesc, got %T", in)
		}

		// Remove the partition
		delete(ringDesc.Partitions, partitionID)

		return ringDesc, true, nil
	})
}

// GetPartitionState returns the state of a partition.
func (r *PartitionRing) GetPartitionState(partitionID string) (PartitionState, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.desc == nil {
		return P_NON_READY, ErrEmptyRing
	}

	partition, ok := r.desc.Partitions[partitionID]
	if !ok {
		return P_NON_READY, fmt.Errorf("partition %s not found", partitionID)
	}

	return partition.State, nil
}

// GetAllPartitions returns a list of all partition IDs in the ring.
func (r *PartitionRing) GetAllPartitions() []string {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	partitions := make([]string, 0, len(r.desc.Partitions))
	for id := range r.desc.Partitions {
		partitions = append(partitions, id)
	}
	return partitions
}
