package codec

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
)

// Codec allows KV clients to serialise and deserialise values.
type Codec interface {
	Decode([]byte) (interface{}, error)
	Encode(interface{}) ([]byte, error)

	DecodeMultiKey(map[string][]byte) (interface{}, error)
	EncodeMultiKey(interface{}) (map[string][]byte, error)

	// CodecID is a short identifier to communicate what codec should be used to decode the value.
	// Once in use, this should be stable to avoid confusing other clients.
	CodecID() string

	GetFactory() func() proto.Message
}

// Proto is a Codec for proto/snappy
type Proto struct {
	id      string
	factory func() proto.Message
}

func NewProtoCodec(id string, factory func() proto.Message) Proto {
	return Proto{id: id, factory: factory}
}

func (p Proto) CodecID() string {
	return p.id
}

func (p Proto) GetFactory() func() proto.Message {
	return p.factory
}

// Decode implements Codec
func (p Proto) Decode(bytes []byte) (interface{}, error) {
	return p.decode(bytes, p.factory())
}

// Encode implements Codec
func (p Proto) Encode(msg interface{}) ([]byte, error) {
	bytes, err := proto.Marshal(msg.(proto.Message))
	if err != nil {
		return nil, err
	}
	return snappy.Encode(nil, bytes), nil
}

// EncodeMultiKey implements Codec
func (p Proto) EncodeMultiKey(msg interface{}) (map[string][]byte, error) {
	// Don't even try
	r, ok := msg.(MultiKey)
	if !ok || r == nil {
		return nil, fmt.Errorf("invalid type: %T, expected MultikKey", msg)
	}

	objs := r.SplitById()
	res := make(map[string][]byte)
	for key, value := range objs {
		bytes, err := proto.Marshal(value.(proto.Message))
		if err != nil {
			return nil, err
		}
		res[key] = snappy.Encode(nil, bytes)
	}
	return res, nil
}

// DecodeMultiKey implements Codec
func (p Proto) DecodeMultiKey(msg map[string][]byte) (interface{}, error) {
	out := p.factory()
	// Don't even try
	r, ok := out.(MultiKey)
	if !ok || r == nil {
		return nil, fmt.Errorf("invalid type: %T, expected MultikKey", out)
	}

	res := make(map[string]interface{})
	for key, bytes := range msg {
		decoded, err := p.decode(bytes, r.GetChildFactory())
		if err != nil {
			return nil, err
		}
		res[key] = decoded
	}
	r.JoinIds(res)

	return r, nil
}

func (p Proto) decode(bytes []byte, out proto.Message) (interface{}, error) {
	bytes, err := snappy.Decode(nil, bytes)
	if err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(bytes, out); err != nil {
		return nil, err
	}
	return out, nil
}

// String is a code for strings.
type String struct{}

func (String) CodecID() string {
	return "string"
}

// Decode implements Codec.
func (String) Decode(bytes []byte) (interface{}, error) {
	return string(bytes), nil
}

// Encode implements Codec.
func (String) Encode(msg interface{}) ([]byte, error) {
	return []byte(msg.(string)), nil
}
