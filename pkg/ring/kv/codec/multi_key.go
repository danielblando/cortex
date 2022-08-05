package codec

import (
	"github.com/gogo/protobuf/proto"
)

type DifferenceResult int

const (
	Updated DifferenceResult = iota // Both rings contain same exact instances.
	Deleted
)

type MultiKey interface {
	SplitById() map[string]interface{}

	JoinIds(in map[string]interface{})

	GetChildFactory() proto.Message

	FindDifference(that MultiKey) (map[string]DifferenceResult, error)
}
