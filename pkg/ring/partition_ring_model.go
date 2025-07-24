package ring

import (
	"fmt"
	"sort"
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/ring/kv/memberlist"
)

// GetPartitionCodec returns the codec used to encode and decode data being put by ring.
func GetPartitionCodec() codec.Codec {
	return codec.NewProtoCodec("partitionRingDesc", func() proto.Message {
		return NewPartitionRingDesc()
	})
}

var (
	_ codec.MultiKey       = &PartitionRingDesc{}
	_ memberlist.Mergeable = &PartitionRingDesc{}
)

func NewPartitionRingDesc() *PartitionRingDesc {
	return &PartitionRingDesc{
		Partitions: map[string]*PartitionDesc{},
	}
}

func (d *PartitionRingDesc) EnsurePartitionCount(c int) {
	if d.Partitions == nil {
		d.Partitions = map[string]*PartitionDesc{}
	}

	for i := len(d.Partitions); i < c; i++ {
		d.Partitions[d.GetIdByIndex(i)] = &PartitionDesc{
			State: P_NON_READY,
		}
	}
}

func (d *PartitionRingDesc) Merge(other memberlist.Mergeable, localCAS bool) (change memberlist.Mergeable, error error) {
	if other == nil {
		return nil, nil
	}

	otherDesc, ok := other.(*PartitionRingDesc)
	if !ok {
		return nil, fmt.Errorf("expected *PartitionRingDesc, got %T", other)
	}

	result := NewPartitionRingDesc()

	// First, copy all partitions from current desc
	for id, p := range d.Partitions {
		newP := &PartitionDesc{
			State:               p.State,
			Tokens:              append([]uint32(nil), p.Tokens...),
			Instances:           make(map[string]InstanceDesc, len(p.Instances)),
			RegisteredTimestamp: p.RegisteredTimestamp,
			Timestamp:           p.Timestamp,
		}

		for iid, inst := range p.Instances {
			newP.Instances[iid] = inst
		}

		result.Partitions[id] = newP
	}

	// Then merge with other desc
	for id, op := range otherDesc.Partitions {
		p, exists := result.Partitions[id]
		if !exists {
			// New partition
			newP := &PartitionDesc{
				State:               op.State,
				Tokens:              append([]uint32(nil), op.Tokens...),
				Instances:           make(map[string]InstanceDesc, len(op.Instances)),
				RegisteredTimestamp: op.RegisteredTimestamp,
				Timestamp:           op.Timestamp,
			}

			for iid, inst := range op.Instances {
				newP.Instances[iid] = inst
			}

			result.Partitions[id] = newP
			continue
		}

		// Existing partition, merge instances
		for iid, inst := range op.Instances {
			existingInst, exists := p.Instances[iid]
			if !exists || inst.Timestamp > existingInst.Timestamp {
				p.Instances[iid] = inst
			}
		}

		// Update state if needed
		if op.Timestamp > p.Timestamp {
			p.State = op.State
			p.Timestamp = op.Timestamp
		}

		// Update tokens if needed
		if len(op.Tokens) > 0 {
			p.Tokens = op.Tokens
		}
	}

	return result, nil
}

func (d *PartitionRingDesc) GetTokens() []uint32 {
	partitions := make([][]uint32, 0, len(d.Partitions))
	for _, instance := range d.Partitions {
		// Tokens may not be sorted for an older version which, so we enforce sorting here.
		tokens := instance.Tokens
		if !sort.IsSorted(Tokens(tokens)) {
			sort.Sort(Tokens(tokens))
		}

		partitions = append(partitions, tokens)
	}

	return MergeTokens(partitions)
}

type partitionInfo struct {
	partitionID   string
	instancesDesc map[string]InstanceDesc
}

func (d *PartitionRingDesc) GetInstances() map[string]interface{} {
	r := make(map[string]interface{}, len(d.Partitions))
	for k, v := range d.Partitions {
		r[k] = v
	}
	return r
}

func (d *PartitionRingDesc) MergeContent() []string {
	result := make([]string, 0, len(d.Partitions))
	for id := range d.Partitions {
		result = append(result, id)
	}
	return result
}

func (d *PartitionRingDesc) RemoveTombstones(limit time.Time) (total, removed int) {
	for id, p := range d.Partitions {
		if p.State == P_NON_READY && time.Unix(p.Timestamp, 0).Before(limit) {
			delete(d.Partitions, id)
			removed++
		}
		total++
	}
	return
}

func (d *PartitionRingDesc) Clone() interface{} {
	r := &PartitionRingDesc{}
	b, _ := d.Marshal()
	_ = proto.Unmarshal(b, r)
	return r
}

func (d *PartitionRingDesc) SplitByID() map[string]interface{} {
	out := make(map[string]interface{}, len(d.Partitions))
	for key := range d.Partitions {
		out[key] = d.Partitions[key]
	}
	return out
}

func (d *PartitionRingDesc) GetIdByIndex(index int) string {
	return fmt.Sprintf("p-%d", index)
}

func (d *PartitionRingDesc) HasInstance(id string) bool {
	for _, p := range d.Partitions {
		if p.Instances != nil {
			_, ok := p.Instances[id]
			if ok {
				return true
			}
		}
	}

	return false
}

// IsReady returns no error when all instance are ACTIVE and healthy,
// and the ring has some tokens.
func (d *PartitionRingDesc) IsReady(storageLastUpdated time.Time, heartbeatTimeout time.Duration) error {
	numTokens := 0
	for _, partition := range d.Partitions {
		for _, instance := range partition.Instances {
			if err := instance.IsReady(storageLastUpdated, heartbeatTimeout); err != nil {
				return err
			}
		}
		numTokens += len(partition.Tokens)
	}

	if numTokens == 0 {
		return fmt.Errorf("no tokens in ring")
	}
	return nil
}

func (d *PartitionRingDesc) JoinIds(in map[string]interface{}) {
	for key, value := range in {
		d.Partitions[key] = value.(*PartitionDesc)
	}
}

func (d *PartitionRingDesc) GetItemFactory() proto.Message {
	return &PartitionDesc{}
}

func (d *PartitionRingDesc) FindDifference(o codec.MultiKey) (interface{}, []string, error) {
	out, ok := o.(*PartitionRingDesc)
	if !ok {
		// This method only deals with non-nil rings.
		return nil, nil, fmt.Errorf("expected *ring.PartitionRingDesc, got %T", out)
	}

	toUpdated := NewPartitionRingDesc()
	toDelete := make([]string, 0)
	// If both are null
	if d == nil && out == nil {
		return toUpdated, toDelete, nil
	}

	// If new data is empty
	if out == nil {
		for k := range d.Partitions {
			toDelete = append(toDelete, k)
		}
		return toUpdated, toDelete, nil
	}

	//If existent data is empty
	if d == nil {
		for key, value := range out.Partitions {
			toUpdated.Partitions[key] = value
		}
		return toUpdated, toDelete, nil
	}

	//If new added
	for name, oing := range out.Partitions {
		if _, ok := d.Partitions[name]; !ok {
			toUpdated.Partitions[name] = oing
		}
	}

	// If removed or updated
	for name, ing := range d.Partitions {
		oing, ok := out.Partitions[name]
		if !ok {
			toDelete = append(toDelete, name)
		} else if !ing.Equal(oing) {
			if _, ok := toUpdated.Partitions[name]; !ok {
				toUpdated.Partitions[name] = &PartitionDesc{
					State:     oing.State,
					Timestamp: oing.Timestamp,
					Tokens:    oing.Tokens,
					Instances: map[string]InstanceDesc{},
				}
			}
			for s, desc := range oing.Instances {
				toUpdated.Partitions[name].Instances[s] = desc
			}
		}
	}

	return toUpdated, toDelete, nil
}

func (d *PartitionRingDesc) AddPartition(id string, s PartitionState, tokens []uint32, registeredAt time.Time) *PartitionDesc {
	if d.Partitions == nil {
		d.Partitions = map[string]*PartitionDesc{}
	}

	partition, ok := d.Partitions[id]
	instances := map[string]InstanceDesc{}
	if ok {
		instances = partition.Instances
	}

	registeredTimestamp := int64(0)
	if !registeredAt.IsZero() {
		registeredTimestamp = registeredAt.Unix()
	}

	d.Partitions[id] = &PartitionDesc{
		Tokens:              tokens,
		RegisteredTimestamp: registeredTimestamp,
		State:               s,
		Timestamp:           time.Now().Unix(),
		Instances:           instances,
	}

	return d.Partitions[id]
}

// TokensFor return all ring tokens and tokens for the input provided ID.
// Returned tokens are guaranteed to be sorted.
func (d *PartitionRingDesc) TokensFor(pId string) (myTokens, allTokens Tokens) {
	allTokens = d.GetTokens()
	p, ok := d.Partitions[pId]
	if !ok {
		return []uint32{}, allTokens
	}
	return p.Tokens, allTokens
}

// Equal is implemented in the generated proto code

// GetRegisteredAt returns the timestamp when the instance has been registered to the ring
// or a zero value if unknown.
func (i *PartitionDesc) GetRegisteredAt() time.Time {
	if i == nil || i.RegisteredTimestamp == 0 {
		return time.Time{}
	}

	return time.Unix(i.RegisteredTimestamp, 0)
}

// AddIngester adds the given ingester to the ring. Ingester will only use supplied tokens,
// any other tokens are removed.
func (m *PartitionDesc) AddIngester(id, addr, zone string, state InstanceState) InstanceDesc {
	if m.Instances == nil {
		m.Instances = map[string]InstanceDesc{}
	}

	ingester := InstanceDesc{
		Addr:      addr,
		State:     state,
		Timestamp: time.Now().Unix(),
		Zone:      zone,
	}

	m.Instances[id] = ingester
	return ingester
}

// RemoveIngester removes the given ingester and all its tokens.
func (d *PartitionRingDesc) RemoveIngester(partitionId string, id string) {
	delete(d.Partitions[partitionId].Instances, id)
}
