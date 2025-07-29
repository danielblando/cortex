package ring

import (
	"flag"
	"net/http"
	"time"

	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// RingInterface defines the common interface for both regular and partition rings.
type RingInterface interface {
	services.Service
	ReadRing
	http.Handler
}

// ReadRing represents the read interface to the ring.
type ReadRing interface {
	// Get returns n (or more) instances which form the replicas for the given key.
	// bufDescs, bufHosts and bufZones are slices to be overwritten for the return value
	// to avoid memory allocation; can be nil, or created with ring.MakeBuffersForGet().
	Get(key uint32, op Operation, bufDescs []InstanceDesc, bufHosts []string, bufZones map[string]int) (ReplicationSet, error)

	// GetAllHealthy returns all healthy instances in the ring, for the given operation.
	// This function doesn't check if the quorum is honored, so doesn't fail if the number
	// of unhealthy instances is greater than the tolerated max unavailable.
	GetAllHealthy(op Operation) (ReplicationSet, error)

	// GetAllInstanceDescs returns a slice of healthy and unhealthy InstanceDesc.
	GetAllInstanceDescs(op Operation) ([]InstanceDesc, []InstanceDesc, error)

	// GetInstanceDescsForOperation returns map of InstanceDesc with instance ID as the keys.
	GetInstanceDescsForOperation(op Operation) (map[string]InstanceDesc, error)

	// GetReplicationSetForOperation returns all instances where the input operation should be executed.
	// The resulting ReplicationSet doesn't necessarily contains all healthy instances
	// in the ring, but could contain the minimum set of instances required to execute
	// the input operation.
	GetReplicationSetForOperation(op Operation) (ReplicationSet, error)

	ReplicationFactor() int

	// InstancesCount returns the number of instances in the ring.
	InstancesCount() int

	// ShuffleShard returns a subring for the provided identifier (eg. a tenant ID)
	// and size (number of instances).
	ShuffleShard(identifier string, size int) ReadRing

	// ShuffleShardWithZoneStability does the same as ShuffleShard but using a different shuffle sharding algorithm.
	// It doesn't round up shard size to be divisible to number of zones and make sure when scaling up/down one
	// shard size at a time, at most 1 instance can be changed.
	// It is only used in Store Gateway for now.
	ShuffleShardWithZoneStability(identifier string, size int) ReadRing

	// GetInstanceState returns the current state of an instance or an error if the
	// instance does not exist in the ring.
	GetInstanceState(instanceID string) (InstanceState, error)

	// GetInstanceIdByAddr returns the instance id from its address or an error if the
	//	// instance does not exist in the ring.
	GetInstanceIdByAddr(addr string) (string, error)

	// ShuffleShardWithLookback is like ShuffleShard() but the returned subring includes
	// all instances that have been part of the identifier's shard since "now - lookbackPeriod".
	ShuffleShardWithLookback(identifier string, size int, lookbackPeriod time.Duration, now time.Time) ReadRing

	// HasInstance returns whether the ring contains an instance matching the provided instanceID.
	HasInstance(instanceID string) bool

	// CleanupShuffleShardCache should delete cached shuffle-shard subrings for given identifier.
	CleanupShuffleShardCache(identifier string)
}

type subringCacheKey struct {
	identifier         string
	shardSize          int
	zoneStableSharding bool
}

// Config for a Ring
type Config struct {
	KVStore                kv.Config              `yaml:"kvstore"`
	HeartbeatTimeout       time.Duration          `yaml:"heartbeat_timeout"`
	ReplicationFactor      int                    `yaml:"replication_factor"`
	ZoneAwarenessEnabled   bool                   `yaml:"zone_awareness_enabled"`
	ExcludedZones          flagext.StringSliceCSV `yaml:"excluded_zones"`
	DetailedMetricsEnabled bool                   `yaml:"detailed_metrics_enabled"`

	// Partition ring configuration
	PartitionRingEnabled bool `yaml:"partition_ring_enabled"`
	NumPartitions        int  `yaml:"num_partitions"`

	// Whether the shuffle-sharding subring cache is disabled. This option is set
	// internally and never exposed to the user.
	SubringCacheDisabled bool `yaml:"-"`

	// Multi-ring migration configuration
	MultiRing MultiRingConfig `yaml:"multi_ring"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet with a specified prefix
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet with a specified prefix
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.KVStore.RegisterFlagsWithPrefix(prefix, "collectors/", f)

	f.DurationVar(&cfg.HeartbeatTimeout, prefix+"ring.heartbeat-timeout", time.Minute, "The heartbeat timeout after which ingesters are skipped for reads/writes. 0 = never (timeout disabled).")
	f.BoolVar(&cfg.DetailedMetricsEnabled, prefix+"ring.detailed-metrics-enabled", true, "Set to true to enable ring detailed metrics. These metrics provide detailed information, such as token count and ownership per tenant. Disabling them can significantly decrease the number of metrics emitted by the distributors.")
	f.IntVar(&cfg.ReplicationFactor, prefix+"distributor.replication-factor", 3, "The number of ingesters to write to and read from.")
	f.BoolVar(&cfg.ZoneAwarenessEnabled, prefix+"distributor.zone-awareness-enabled", false, "True to enable the zone-awareness and replicate ingested samples across different availability zones.")
	f.Var(&cfg.ExcludedZones, prefix+"distributor.excluded-zones", "Comma-separated list of zones to exclude from the ring. Instances in excluded zones will be filtered out from the ring.")

	// Partition ring configuration
	f.BoolVar(&cfg.PartitionRingEnabled, prefix+"ring.partition-ring-enabled", false, "Enable partition ring instead of standard ring.")
	f.IntVar(&cfg.NumPartitions, prefix+"ring.num-partitions", 16, "Number of partitions in the partition ring.")

	// Multi-ring migration configuration
	cfg.MultiRing.RegisterFlagsWithPrefix(prefix+"ring.", f)
}

// Validate validates the ring configuration
func (cfg *Config) Validate() error {
	return cfg.MultiRing.Validate()
}
