package ring

import (
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

// CreateRingFromConfig creates a ring based on the configuration
// This is a helper function to create either a default ring or partition ring
func CreateRingFromConfig(cfg Config, ringType, name, key string, logger log.Logger, reg prometheus.Registerer) (RingInterface, error) {
	if ringType == "partition" {
		partitionCfg := cfg
		partitionCfg.PartitionRingEnabled = true
		return NewPartitionRing(partitionCfg, name, key, logger, reg)
	} else {
		// For default ring, ensure partition ring is disabled
		defaultCfg := cfg
		defaultCfg.PartitionRingEnabled = false
		return New(defaultCfg, name, key, logger, reg)
	}
}
