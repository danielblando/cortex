package ingester

import (
	"github.com/cortexproject/cortex/pkg/ring"
)

// OnRingInstanceHeartbeat implements the ring.LifecyclerDelegate interface.
func (i *Ingester) OnRingInstanceHeartbeat(lifecycler *ring.Lifecycler, ringDesc *ring.Desc) {
	// This method is required by the LifecyclerDelegate interface but is not used
	// for the partition ring, so we leave it empty.
}
