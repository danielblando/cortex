package ring

import (
	"flag"
	"fmt"
)

// MultiRingConfig defines configuration for multi-ring migration
type MultiRingConfig struct {
	// Enable multi-ring migration mode
	Enabled bool `yaml:"enabled"`

	// Primary ring configuration and percentage (0-100)
	PrimaryRingType       string  `yaml:"primary_ring_type"`       // "default" or "partition"
	PrimaryRingPercentage float64 `yaml:"primary_ring_percentage"` // 0-100, secondary gets the remainder

	// Secondary ring configuration (percentage is automatically 100 - primary)
	SecondaryRingType string `yaml:"secondary_ring_type"` // "default" or "partition"
}

// GetSecondaryRingPercentage returns the calculated secondary ring percentage
func (cfg *MultiRingConfig) GetSecondaryRingPercentage() float64 {
	return 100.0 - cfg.PrimaryRingPercentage
}

// RegisterFlags adds the flags required to config this to the given FlagSet with a specified prefix
func (cfg *MultiRingConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet with a specified prefix
func (cfg *MultiRingConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, prefix+"multi-ring.enabled", false, "Enable multi-ring migration mode.")
	f.StringVar(&cfg.PrimaryRingType, prefix+"multi-ring.primary-ring-type", "default", "Type of primary ring: 'default' or 'partition'.")
	f.Float64Var(&cfg.PrimaryRingPercentage, prefix+"multi-ring.primary-ring-percentage", 100.0, "Percentage of traffic to send to primary ring (0-100). Secondary ring gets the remainder.")
	f.StringVar(&cfg.SecondaryRingType, prefix+"multi-ring.secondary-ring-type", "partition", "Type of secondary ring: 'default' or 'partition'.")
}

// Validate validates the multi-ring configuration
func (cfg *MultiRingConfig) Validate() error {
	if !cfg.Enabled {
		return nil
	}

	// Validate ring types
	if cfg.PrimaryRingType != "default" && cfg.PrimaryRingType != "partition" {
		return fmt.Errorf("invalid primary ring type: %s (must be 'default' or 'partition')", cfg.PrimaryRingType)
	}
	if cfg.SecondaryRingType != "default" && cfg.SecondaryRingType != "partition" {
		return fmt.Errorf("invalid secondary ring type: %s (must be 'default' or 'partition')", cfg.SecondaryRingType)
	}

	// Validate percentage
	if cfg.PrimaryRingPercentage < 0 || cfg.PrimaryRingPercentage > 100 {
		return fmt.Errorf("primary ring percentage must be between 0 and 100, got: %f", cfg.PrimaryRingPercentage)
	}

	// Ensure rings are different types
	if cfg.PrimaryRingType == cfg.SecondaryRingType {
		return fmt.Errorf("primary and secondary rings must be different types")
	}

	return nil
}
