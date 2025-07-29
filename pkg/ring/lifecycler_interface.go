package ring

import (
	"context"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// RingCount is the interface exposed by a ring implementation which allows
// to count members
type RingCount interface {
	HealthyInstancesCount() int
	ZonesCount() int
}

// LifecyclerInterface defines the common interface for both regular and partition lifecyclers.
type LifecyclerInterface interface {
	services.Service
	RingCount

	// GetInstanceID returns the instance ID.
	GetInstanceID() string

	// GetInstanceAddr returns the instance address.
	GetInstanceAddr() string

	// GetState returns the current state of the lifecycler.
	GetState() InstanceState

	GetZone() string

	// IsRegistered returns whether the instance is registered in the ring.
	IsRegistered() (bool, error)

	// SetFlushOnShutdown enables or disables flushing on shutdown.
	SetFlushOnShutdown(flush bool)

	ChangeState(ctx context.Context, state InstanceState) error

	Join()

	RenewTokens(ratio float64, ctx context.Context)

	FlushOnShutdown() bool

	ShouldUnregisterOnShutdown() bool

	SetUnregisterOnShutdown(enabled bool)

	CheckReady(ctx context.Context) error

	Zones() []string

	// RecoverFromLeavingState attempts to recover an instance stuck in LEAVING state
	RecoverFromLeavingState() error
}

type LifecyclerConfig struct {
	RingConfig Config `yaml:"ring"`

	// Config for the ingester lifecycle control
	NumTokens                int           `yaml:"num_tokens"`
	TokensGeneratorStrategy  string        `yaml:"tokens_generator_strategy"`
	HeartbeatPeriod          time.Duration `yaml:"heartbeat_period"`
	ObservePeriod            time.Duration `yaml:"observe_period"`
	JoinAfter                time.Duration `yaml:"join_after"`
	MinReadyDuration         time.Duration `yaml:"min_ready_duration"`
	InfNames                 []string      `yaml:"interface_names"`
	FinalSleep               time.Duration `yaml:"final_sleep"`
	TokensFilePath           string        `yaml:"tokens_file_path"`
	Zone                     string        `yaml:"availability_zone"`
	UnregisterOnShutdown     bool          `yaml:"unregister_on_shutdown"`
	ReadinessCheckRingHealth bool          `yaml:"readiness_check_ring_health"`
	PartitionID              string        `yaml:"partition_id"`

	// For testing, you can override the address and ID of this ingester
	Addr string `yaml:"address" doc:"hidden"`
	Port int    `doc:"hidden"`
	ID   string `doc:"hidden"`

	// Injected internally
	ListenPort int `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *LifecyclerConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet.
func (cfg *LifecyclerConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.RingConfig.RegisterFlagsWithPrefix(prefix, f)

	// In order to keep backwards compatibility all of these need to be prefixed
	// with "ingester."
	if prefix == "" {
		prefix = "ingester."
	}

	f.IntVar(&cfg.NumTokens, prefix+"num-tokens", 128, "Number of tokens for each ingester.")
	f.StringVar(&cfg.TokensGeneratorStrategy, prefix+"tokens-generator-strategy", randomTokenStrategy, fmt.Sprintf("EXPERIMENTAL: Algorithm used to generate new ring tokens. Supported Values: %s", strings.Join(supportedTokenStrategy, ",")))
	f.DurationVar(&cfg.HeartbeatPeriod, prefix+"heartbeat-period", 5*time.Second, "Period at which to heartbeat to consul. 0 = disabled.")
	f.DurationVar(&cfg.JoinAfter, prefix+"join-after", 0*time.Second, "Period to wait for a claim from another member; will join automatically after this.")
	f.DurationVar(&cfg.ObservePeriod, prefix+"observe-period", 0*time.Second, "Observe tokens after generating to resolve collisions. Useful when using gossiping ring.")
	f.DurationVar(&cfg.MinReadyDuration, prefix+"min-ready-duration", 15*time.Second, "Minimum duration to wait after the internal readiness checks have passed but before succeeding the readiness endpoint. This is used to slowdown deployment controllers (eg. Kubernetes) after an instance is ready and before they proceed with a rolling update, to give the rest of the cluster instances enough time to receive ring updates.")
	f.DurationVar(&cfg.FinalSleep, prefix+"final-sleep", 30*time.Second, "Duration to sleep for before exiting, to ensure metrics are scraped.")
	f.StringVar(&cfg.TokensFilePath, prefix+"tokens-file-path", "", "File path where tokens are stored. If empty, tokens are not stored at shutdown and restored at startup.")

	hostname, err := os.Hostname()
	if err != nil {
		panic(fmt.Errorf("failed to get hostname %s", err))
	}

	cfg.InfNames = []string{"eth0", "en0"}
	f.Var((*flagext.StringSlice)(&cfg.InfNames), prefix+"lifecycler.interface", "Name of network interface to read address from.")
	f.StringVar(&cfg.Addr, prefix+"lifecycler.addr", "", "IP address to advertise in the ring.")
	f.IntVar(&cfg.Port, prefix+"lifecycler.port", 0, "port to advertise in consul (defaults to server.grpc-listen-port).")
	f.StringVar(&cfg.ID, prefix+"lifecycler.ID", hostname, "ID to register in the ring.")
	f.StringVar(&cfg.Zone, prefix+"availability-zone", "", "The availability zone where this instance is running.")
	f.BoolVar(&cfg.UnregisterOnShutdown, prefix+"unregister-on-shutdown", true, "Unregister from the ring upon clean shutdown. It can be useful to disable for rolling restarts with consistent naming in conjunction with -distributor.extend-writes=false.")
	f.BoolVar(&cfg.ReadinessCheckRingHealth, prefix+"readiness-check-ring-health", true, "When enabled the readiness probe succeeds only after all instances are ACTIVE and healthy in the ring, otherwise only the instance itself is checked. This option should be disabled if in your cluster multiple instances can be rolled out simultaneously, otherwise rolling updates may be slowed down.")
	f.StringVar(&cfg.PartitionID, prefix+"partition-id", "", "Partition ID for partition ring (defaults to p-0). Only used when partition ring is enabled.")
}

func (cfg *LifecyclerConfig) Validate() error {
	if cfg.TokensGeneratorStrategy != "" && !slices.Contains(supportedTokenStrategy, strings.ToLower(cfg.TokensGeneratorStrategy)) {
		return errInvalidTokensGeneratorStrategy
	}

	return nil
}
