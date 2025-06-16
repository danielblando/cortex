package ingester

import (
	"encoding/binary"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/log/level"
	"hash/fnv"
	"math"
	"sort"
	"sync"
	"time"

	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"
)

const (
	numActiveSeriesStripes = 512
)

// ActiveSeries is keeping track of recently active series for a single tenant.
type ActiveSeries struct {
	ownerTokens    []uint32
	allTokens      []uint32
	tokensHash     uint32
	purgeTokenHash uint32
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
	hash              uint32
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

// UpdateSeries updates series timestamp to 'now'. Function is called to make a copy of labels if entry doesn't exist yet.
func (c *ActiveSeries) UpdateSeries(series labels.Labels, hash uint64, token uint32, now time.Time, nativeHistogram bool, labelsCopy func(labels.Labels) labels.Labels) {
	stripeID := hash % numActiveSeriesStripes

	c.stripes[stripeID].updateSeriesTimestamp(now, series, hash, token, nativeHistogram, labelsCopy)
}

func (c *ActiveSeries) UpdateTokens(ownerTokens []uint32, allTokens []uint32) {
	atHash := getMapKeysHash(allTokens)
	level.Info(util_log.Logger).Log("msg", "updating owned tokens", "atHash", atHash, "tokensHash", c.tokensHash, "ownerTokens", ownerTokens, "ownerTokens", allTokens)
	if atHash != c.tokensHash {
		copy(ownerTokens, c.ownerTokens)
		copy(allTokens, c.allTokens)
		c.tokensHash = atHash
	}
}

func getMapKeysHash(allTokens []uint32) uint32 {
	// Sort for consistent ordering
	sorted := make([]uint32, len(allTokens))
	copy(sorted, allTokens)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	h := fnv.New32a()
	for _, token := range sorted {
		binary.Write(h, binary.LittleEndian, token)
	}
	return h.Sum32()
}

// Purge removes expired entries from the cache. This function should be called
// periodically to avoid memory leaks.
func (c *ActiveSeries) Purge(keepUntil int64, softDelete bool) {
	tokenChanged := false
	if c.purgeTokenHash == 0 || c.tokensHash != c.purgeTokenHash {
		level.Info(util_log.Logger).Log("Tokens changed", "old", c.purgeTokenHash, "new", c.tokensHash)
		tokenChanged = true
		c.purgeTokenHash = c.tokensHash
	}
	for s := 0; s < numActiveSeriesStripes; s++ {
		c.stripes[s].purge(keepUntil, tokenChanged, softDelete, c.ownerTokens, c.allTokens)
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

func (s *activeSeriesStripe) updateSeriesTimestamp(now time.Time, series labels.Labels, fingerprint uint64, token uint32, nativeHistogram bool, labelsCopy func(labels.Labels) labels.Labels) {
	nowNanos := now.UnixNano()

	e := s.findEntryForSeries(fingerprint, series)
	entryTimeSet := false
	if e == nil {
		e, entryTimeSet = s.findOrCreateEntryForSeries(fingerprint, token, series, nowNanos, nativeHistogram, labelsCopy)
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

func (s *activeSeriesStripe) findOrCreateEntryForSeries(fingerprint uint64, token uint32, series labels.Labels, nowNanos int64, nativeHistogram bool, labelsCopy func(labels.Labels) labels.Labels) (*atomic.Int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already exists within the entries.
	for ix, entry := range s.refs[fingerprint] {
		if labels.Equal(entry.lbs, series) {
			return s.refs[fingerprint][ix].nanos, false
		}
	}

	s.active++
	if nativeHistogram {
		s.activeNativeHistogram++
	}
	e := activeSeriesEntry{
		lbs:               labelsCopy(series),
		hash:              token,
		nanos:             atomic.NewInt64(nowNanos),
		isNativeHistogram: nativeHistogram,
	}

	s.refs[fingerprint] = append(s.refs[fingerprint], e)

	return e.nanos, true
}

// searchToken returns the offset of the tokens entry holding the range for the provided key.
func searchToken(tokens []uint32, key uint32) int {
	i := sort.Search(len(tokens), func(x int) bool {
		return tokens[x] > key
	})
	if i >= len(tokens) {
		i = 0
	}
	return i
}

func (s *activeSeriesStripe) purge(keepUntil int64, tokenChange bool, softDelete bool, ownerTokens []uint32, allTokens []uint32) {
	if oldest := s.oldestEntryTs.Load(); oldest > 0 && keepUntil <= oldest {
		// Nothing to do.
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var mOwnerTokens map[uint32]struct{}
	if tokenChange {
		for _, token := range ownerTokens {
			mOwnerTokens[token] = struct{}{}
		}
	}

	active := 0
	owner := 0
	activeNativeHistogram := 0

	oldest := int64(math.MaxInt64)
	for fp, entries := range s.refs {
		// Since we do expect very few fingerprint collisions, we
		// have an optimized implementation for the common case.
		if len(entries) == 1 {
			ts := entries[0].nanos.Load()
			if !softDelete && ts < keepUntil {
				delete(s.refs, fp)
				continue
			}

			if tokenChange {
				i := searchToken(allTokens, entries[0].hash)
				level.Info(util_log.Logger).Log("Token found", "i", i, "hash", entries[0].hash, "allTokens", len(allTokens))
				if _, ok := mOwnerTokens[allTokens[i]]; ok {
					owner++
				} else {
					delete(s.refs, fp)
				}
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
			if !softDelete && ts < keepUntil {
				entries = append(entries[:i], entries[i+1:]...)
			} else {
				if ts < oldest {
					oldest = ts
				}
				if tokenChange {
					i := searchToken(allTokens, entries[0].hash)
					if _, ok := mOwnerTokens[allTokens[i]]; !ok {
						entries = append(entries[:i], entries[i+1:]...)
					}
				}
				i++
			}
		}

		// Either update or delete the entries in the map
		if cnt := len(entries); cnt == 0 {
			delete(s.refs, fp)
		} else {
			active += cnt
			owner += cnt
			for _, e := range entries {
				if e.isNativeHistogram {
					activeNativeHistogram++
				}
			}
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
	s.owned = active
	s.activeNativeHistogram = activeNativeHistogram
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
