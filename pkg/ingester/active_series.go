package ingester

import (
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/go-kit/log/level"
	"math"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"
)

const (
	numActiveSeriesStripes = 512
)

// ActiveSeries is keeping track of recently active series for a single tenant.
type ActiveSeries struct {
	instanceTokens map[uint32]struct{}
	ringTokens     []uint32
	currHash       uint32
	lastUsedHash   uint32
	stripes        [numActiveSeriesStripes]activeSeriesStripe
}

// activeSeriesStripe holds a subset of the series timestamps for a single tenant.
type activeSeriesStripe struct {
	// Unix nanoseconds. Only used by purge. Zero = unknown.
	// Updated in purge and when old timestamp is used when updating series (in this case, oldestEntryTs is updated
	// without holding the lock -- hence the atomic).
	oldestEntryTs atomic.Int64

	mu                    sync.RWMutex
	refs                  map[uint64][]activeSeriesEntry
	active                int // Number of active entries in this stripe. Only decreased during purge or clear.
	activeNativeHistogram int // Number of active entries only for Native Histogram in this stripe. Only decreased during purge or clear.
	owned                 int // Number of owned entries in this stripe. Decrease during purge, clear or ring changes.
}

// activeSeriesEntry holds a timestamp for single series.
type activeSeriesEntry struct {
	lbs               labels.Labels
	key               uint32
	nanos             *atomic.Int64 // Unix timestamp in nanoseconds. Needs to be a pointer because we don't store pointers to entries in the stripe.
	isNativeHistogram bool
}

func NewActiveSeries() *ActiveSeries {
	c := &ActiveSeries{}

	// Stripes are pre-allocated so that we only read on them and no lock is required.
	for i := 0; i < numActiveSeriesStripes; i++ {
		c.stripes[i].refs = map[uint64][]activeSeriesEntry{}
	}

	return c
}

// UpdateSeries timestamp to 'now'. Function is called to make a copy of labels if entry doesn't exist yet.
func (c *ActiveSeries) UpdateSeries(series labels.Labels, hash uint64, key uint32, now time.Time, nativeHistogram bool, labelsCopy func(labels.Labels) labels.Labels) {
	stripeID := hash % numActiveSeriesStripes

	c.stripes[stripeID].updateSeriesTimestamp(now, series, hash, key, nativeHistogram, labelsCopy, c.ringTokens, c.instanceTokens)
}

func (c *ActiveSeries) updateTokens(instanceTokens []uint32, ringTokens []uint32) bool {
	atHash := getMapKeysHash(ringTokens)
	level.Info(util_log.Logger).Log("msg", "updating owned tokens", "atHash", atHash, "tokensHash", c.currHash, "instanceTokens", len(instanceTokens), "c.instanceTokens", len(c.instanceTokens), "ringTokens", len(ringTokens), "c.ringTokens", len(c.ringTokens))
	if len(ringTokens) > 0 && atHash != c.currHash {
		if cap(c.ringTokens) < len(ringTokens) {
			c.ringTokens = make([]uint32, len(ringTokens))
		} else {
			// Resize existing slice
			c.ringTokens = c.ringTokens[:len(ringTokens)]
		}

		c.instanceTokens = make(map[uint32]struct{}, len(instanceTokens))
		for _, token := range instanceTokens {
			c.instanceTokens[token] = struct{}{}
		}

		copy(c.ringTokens, ringTokens)
		c.currHash = atHash
		return true
	}
	return false
}

func getMapKeysHash(allTokens []uint32) uint32 {
	h := util.HashNew32()
	for _, token := range allTokens {
		h = util.HashAddUint32(h, token)
	}
	return h
}

func (c *ActiveSeries) UpdateMetrics(keepUntil time.Time, instanceTokens []uint32, ringTokens []uint32) {
	tokensChanged := c.updateTokens(instanceTokens, ringTokens)
	if tokensChanged {
		level.Info(util_log.Logger).Log("msg", "Tokens changed", "old", c.lastUsedHash, "new", c.currHash)
	}
	for s := 0; s < numActiveSeriesStripes; s++ {
		c.stripes[s].updateMetrics(keepUntil, tokensChanged, c.instanceTokens, c.ringTokens)
	}
}

// Purge removes expired entries from the cache. This function should be called
// periodically to avoid memory leaks.
func (c *ActiveSeries) Purge(keepUntil int64) {
	for s := 0; s < numActiveSeriesStripes; s++ {
		c.stripes[s].purge(keepUntil)
	}
}

// nolint // Linter reports that this method is unused, but it is.
func (c *ActiveSeries) clear() {
	for s := 0; s < numActiveSeriesStripes; s++ {
		c.stripes[s].clear()
	}
}

func (c *ActiveSeries) Active() int {
	total := 0
	for s := 0; s < numActiveSeriesStripes; s++ {
		total += c.stripes[s].getActive()
	}
	return total
}

func (c *ActiveSeries) Owned() int {
	total := 0
	for s := 0; s < numActiveSeriesStripes; s++ {
		total += c.stripes[s].getOwned()
	}
	return total
}

func (c *ActiveSeries) ActiveNativeHistogram() int {
	total := 0
	for s := 0; s < numActiveSeriesStripes; s++ {
		total += c.stripes[s].getActiveNativeHistogram()
	}
	return total
}

func (s *activeSeriesStripe) updateSeriesTimestamp(now time.Time, series labels.Labels, fingerprint uint64, key uint32, nativeHistogram bool, labelsCopy func(labels.Labels) labels.Labels, allTokens []uint32, instanceTokens map[uint32]struct{}) {
	nowNanos := now.UnixNano()

	e := s.findEntryForSeries(fingerprint, series)
	entryTimeSet := false
	if e == nil {
		e, entryTimeSet = s.findOrCreateEntryForSeries(fingerprint, key, series, nowNanos, nativeHistogram, labelsCopy, allTokens, instanceTokens)
		if e == nil {
			return
		}
	}

	if !entryTimeSet {
		if prev := e.Load(); nowNanos > prev {
			entryTimeSet = e.CompareAndSwap(prev, nowNanos)
		}
	}

	if entryTimeSet {
		for prevOldest := s.oldestEntryTs.Load(); nowNanos < prevOldest; {
			// If recent purge already removed entries older than "oldest entry timestamp", setting this to 0 will make
			// sure that next purge doesn't take the shortcut route.
			if s.oldestEntryTs.CompareAndSwap(prevOldest, 0) {
				break
			}
		}
	}
}

func (s *activeSeriesStripe) findEntryForSeries(fingerprint uint64, series labels.Labels) *atomic.Int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if already exists within the entries.
	for ix, entry := range s.refs[fingerprint] {
		if labels.Equal(entry.lbs, series) {
			return s.refs[fingerprint][ix].nanos
		}
	}

	return nil
}

func (s *activeSeriesStripe) findOrCreateEntryForSeries(fingerprint uint64, key uint32, series labels.Labels, nowNanos int64, nativeHistogram bool, labelsCopy func(labels.Labels) labels.Labels, ringTokens []uint32, instanceTokens map[uint32]struct{}) (*atomic.Int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already exists within the entries.
	for ix, entry := range s.refs[fingerprint] {
		if labels.Equal(entry.lbs, series) {
			return s.refs[fingerprint][ix].nanos, false
		}
	}

	if len(ringTokens) > 0 && !isOwnerByInstance(key, ringTokens, instanceTokens) {
		return nil, false
	}

	s.active++
	s.owned++
	if nativeHistogram {
		s.activeNativeHistogram++
	}
	e := activeSeriesEntry{
		lbs:               labelsCopy(series),
		key:               key,
		nanos:             atomic.NewInt64(nowNanos),
		isNativeHistogram: nativeHistogram,
	}

	s.refs[fingerprint] = append(s.refs[fingerprint], e)

	level.Info(util_log.Logger).Log("msg", "ddeluigg - findOrCreateEntryForSeries", "active", s.active, "owned", s.owned, "activeNativeHistogram", s.activeNativeHistogram)
	return e.nanos, true
}

// nolint // Linter reports that this method is unused, but it is.
func (s *activeSeriesStripe) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.oldestEntryTs.Store(0)
	s.refs = map[uint64][]activeSeriesEntry{}
	s.active = 0
	s.owned = 0
	s.activeNativeHistogram = 0
}

func (s *activeSeriesStripe) updateMetrics(keepUntil time.Time, tokenChange bool, instanceTokens map[uint32]struct{}, allTokens []uint32) {
	keepUntilNanos := keepUntil.UnixNano()
	if oldest := s.oldestEntryTs.Load(); oldest > 0 && keepUntilNanos <= oldest && !tokenChange {
		level.Info(util_log.Logger).Log("msg", "skipping purge", "oldest", oldest, "keepUntil", keepUntil)
		// Nothing to do.
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	active := 0
	owner := 0
	activeNativeHistogram := 0

	oldest := int64(math.MaxInt64)
	for fp, entries := range s.refs {
		// Since we do expect very few fingerprint collisions, we
		// have an optimized implementation for the common case.
		if len(entries) == 1 {
			ts := entries[0].nanos.Load()
			if tokenChange && !isOwnerByInstance(entries[0].key, allTokens, instanceTokens) {
				delete(s.refs, fp)
				continue
			}
			owner++
			if ts >= keepUntilNanos {
				active++
				if entries[0].isNativeHistogram {
					activeNativeHistogram++
				}
			}
			if ts < oldest {
				oldest = ts
			}
			continue
		}

		// We have more entries, which means there's a collision,
		// so we have to iterate over the entries.
		for i := 0; i < len(entries); {
			ts := entries[i].nanos.Load()
			if tokenChange && !isOwnerByInstance(entries[i].key, allTokens, instanceTokens) {
				level.Info(util_log.Logger).Log("msg", "Lost ownership token", "hash", entries[0].key)
				entries = append(entries[:i], entries[i+1:]...)
				continue
			}
			owner++
			if ts >= keepUntilNanos {
				active++
				if entries[i].isNativeHistogram {
					activeNativeHistogram++
				}
			}
			if ts < oldest {
				oldest = ts
			}
			i++
		}

		// Either update or delete the entries in the map
		if cnt := len(entries); cnt == 0 {
			delete(s.refs, fp)
		} else {
			s.refs[fp] = entries
		}
	}

	if oldest == math.MaxInt64 {
		s.oldestEntryTs.Store(0)
	} else {
		s.oldestEntryTs.Store(oldest)
	}
	s.active = active
	s.owned = owner
	s.activeNativeHistogram = activeNativeHistogram
	level.Info(util_log.Logger).Log("msg", "ddeluigg - updateMetrics", "active", s.active, "owned", s.owned, "activeNativeHistogram", s.activeNativeHistogram)

}

func (s *activeSeriesStripe) purge(keepUntil int64) {
	if oldest := s.oldestEntryTs.Load(); oldest > 0 && keepUntil <= oldest {
		level.Info(util_log.Logger).Log("msg", "skipping purge", "oldest", oldest, "keepUntil", keepUntil)
		// Nothing to do.
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	active := 0
	activeNativeHistogram := 0

	oldest := int64(math.MaxInt64)
	for fp, entries := range s.refs {
		// Since we do expect very few fingerprint collisions, we
		// have an optimized implementation for the common case.
		if len(entries) == 1 {
			ts := entries[0].nanos.Load()
			if ts < keepUntil {
				delete(s.refs, fp)
				continue
			}
			active++
			if entries[0].isNativeHistogram {
				activeNativeHistogram++
			}
			if ts < oldest {
				oldest = ts
			}
			continue
		}

		// We have more entries, which means there's a collision,
		// so we have to iterate over the entries.
		for i := 0; i < len(entries); {
			ts := entries[i].nanos.Load()
			if ts < keepUntil {
				entries = append(entries[:i], entries[i+1:]...)
			} else {
				if ts < oldest {
					oldest = ts
				}
				active++
				if entries[i].isNativeHistogram {
					activeNativeHistogram++
				}
				i++
			}
		}

		// Either update or delete the entries in the map
		if cnt := len(entries); cnt == 0 {
			delete(s.refs, fp)
		} else {
			s.refs[fp] = entries
		}
	}

	if oldest == math.MaxInt64 {
		s.oldestEntryTs.Store(0)
	} else {
		s.oldestEntryTs.Store(oldest)
	}

	s.active = active
	s.owned = active
	s.activeNativeHistogram = activeNativeHistogram
	level.Info(util_log.Logger).Log("msg", "ddeluigg - purge", "active", s.active, "owned", s.owned, "activeNativeHistogram", s.activeNativeHistogram)
}

func (s *activeSeriesStripe) getActive() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.active
}

func (s *activeSeriesStripe) getOwned() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.owned
}

func (s *activeSeriesStripe) getActiveNativeHistogram() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.activeNativeHistogram
}

func isOwnerByInstance(token uint32, ringTokens []uint32, instanceTokens map[uint32]struct{}) bool {
	i := ring.SearchToken(ringTokens, token)
	_, found := instanceTokens[ringTokens[i]]
	return found
}
