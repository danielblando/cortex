package dynamodb

import (
	"context"
	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

const key = "test"

func Test_CAS_ErrorNoRetry(t *testing.T) {
	ddbMock := NewDynamodbClientMock()
	codecMock := &CodecMock{}
	descMock := &DescMock{}
	c := NewClientMock(ddbMock, codecMock, testLogger{}, prometheus.NewPedanticRegistry())
	expectedErr := errors.Errorf("test")

	ddbMock.On("Query").Return(map[string][]byte{}, nil).Once()
	codecMock.On("DecodeMultiKey").Return(descMock, nil).Twice()
	descMock.On("Clone").Return(descMock).Once()

	err := c.CAS(context.TODO(), key, func(in interface{}) (out interface{}, retry bool, err error) {
		return nil, false, expectedErr
	})

	require.Equal(t, err, expectedErr)
}

func Test_CAS_Backoff(t *testing.T) {
	backoffConfig.MinBackoff = 1 * time.Millisecond
	backoffConfig.MaxBackoff = 1 * time.Millisecond
	ddbMock := NewDynamodbClientMock()
	codecMock := &CodecMock{}
	descMock := &DescMock{}
	c := NewClientMock(ddbMock, codecMock, testLogger{}, prometheus.NewPedanticRegistry())
	expectedErr := errors.Errorf("test")

	ddbMock.On("Query").Return(map[string][]byte{}, expectedErr).Once()
	ddbMock.On("Query").Return(map[string][]byte{}, nil).Once()
	codecMock.On("DecodeMultiKey").Return(descMock, nil).Twice()
	descMock.On("Clone").Return(descMock).Once()
	descMock.On("FindDifference", descMock).Return(descMock, []string{}, nil).Once()
	codecMock.On("EncodeMultiKey").Return(map[string][]byte{}, nil).Twice()

	err := c.CAS(context.TODO(), key, func(in interface{}) (out interface{}, retry bool, err error) {
		return descMock, true, nil
	})

	require.NoError(t, err)
}

func Test_CAS_Failed(t *testing.T) {
	backoffConfig.MinBackoff = 1 * time.Millisecond
	backoffConfig.MaxBackoff = 1 * time.Millisecond
	ddbMock := NewDynamodbClientMock()
	codecMock := &CodecMock{}
	descMock := &DescMock{}
	c := NewClientMock(ddbMock, codecMock, testLogger{}, prometheus.NewPedanticRegistry())
	expectedErr := errors.Errorf("test")

	ddbMock.On("Query").Return(map[string][]byte{}, expectedErr)

	err := c.CAS(context.TODO(), key, func(in interface{}) (out interface{}, retry bool, err error) {
		return descMock, true, nil
	})

	ddbMock.AssertNumberOfCalls(t, "Query", 10)
	require.Equal(t, expectedErr, err)
}

func Test_CAS_Update(t *testing.T) {
	ddbMock := NewDynamodbClientMock()
	codecMock := &CodecMock{}
	descMock := &DescMock{}
	c := NewClientMock(ddbMock, codecMock, testLogger{}, prometheus.NewPedanticRegistry())
	expectedUpdatedKeys := []string{"t1", "t2"}
	expectedUpdated := map[string][]byte{
		expectedUpdatedKeys[0]: []byte(expectedUpdatedKeys[0]),
		expectedUpdatedKeys[1]: []byte(expectedUpdatedKeys[1]),
	}

	ddbMock.On("Query").Return(map[string][]byte{}, nil).Once()
	codecMock.On("DecodeMultiKey").Return(descMock, nil).Once()
	descMock.On("Clone").Return(descMock).Once()
	descMock.On("FindDifference", descMock).Return(descMock, []string{}, nil).Once()
	codecMock.On("EncodeMultiKey").Return(expectedUpdated, nil).Once()
	ddbMock.On("Put", context.TODO(), dynamodbKey{primaryKey: key, sortKey: expectedUpdatedKeys[0]}, []byte(expectedUpdatedKeys[0])).Once()
	ddbMock.On("Put", context.TODO(), dynamodbKey{primaryKey: key, sortKey: expectedUpdatedKeys[1]}, []byte(expectedUpdatedKeys[1])).Once()

	err := c.CAS(context.TODO(), key, func(in interface{}) (out interface{}, retry bool, err error) {
		return descMock, true, nil
	})

	require.NoError(t, err)
	ddbMock.AssertNumberOfCalls(t, "Put", 2)
	ddbMock.AssertNumberOfCalls(t, "DeleteKey", 0)
	ddbMock.AssertCalled(t, "Put", context.TODO(), dynamodbKey{primaryKey: key, sortKey: expectedUpdatedKeys[0]}, []byte(expectedUpdatedKeys[0]))
	ddbMock.AssertCalled(t, "Put", context.TODO(), dynamodbKey{primaryKey: key, sortKey: expectedUpdatedKeys[1]}, []byte(expectedUpdatedKeys[1]))
}

func Test_CAS_Delete(t *testing.T) {
	ddbMock := NewDynamodbClientMock()
	codecMock := &CodecMock{}
	descMock := &DescMock{}
	c := NewClientMock(ddbMock, codecMock, testLogger{}, prometheus.NewPedanticRegistry())
	expectedToDelete := []string{"test", "test2"}

	ddbMock.On("Query").Return(map[string][]byte{}, nil).Once()
	codecMock.On("DecodeMultiKey").Return(descMock, nil).Once()
	descMock.On("Clone").Return(descMock).Once()
	descMock.On("FindDifference", descMock).Return(descMock, expectedToDelete, nil).Once()
	codecMock.On("EncodeMultiKey").Return(map[string][]byte{}, nil).Once()
	ddbMock.On("DeleteKey", context.TODO(), dynamodbKey{primaryKey: key, sortKey: expectedToDelete[0]})
	ddbMock.On("DeleteKey", context.TODO(), dynamodbKey{primaryKey: key, sortKey: expectedToDelete[1]})

	err := c.CAS(context.TODO(), key, func(in interface{}) (out interface{}, retry bool, err error) {
		return descMock, true, nil
	})

	require.NoError(t, err)
	ddbMock.AssertNumberOfCalls(t, "Put", 0)
	ddbMock.AssertNumberOfCalls(t, "DeleteKey", 2)
	ddbMock.AssertCalled(t, "DeleteKey", context.TODO(), dynamodbKey{primaryKey: key, sortKey: expectedToDelete[0]})
	ddbMock.AssertCalled(t, "DeleteKey", context.TODO(), dynamodbKey{primaryKey: key, sortKey: expectedToDelete[1]})
}

func Test_WatchKey(t *testing.T) {
	backoffConfig.MinBackoff = 1 * time.Millisecond
	backoffConfig.MaxBackoff = 1 * time.Millisecond
	watchKeyWait = 1 * time.Second
	ddbMock := NewDynamodbClientMock()
	codecMock := &CodecMock{}
	descMock := &DescMock{}
	c := NewClientMock(ddbMock, codecMock, testLogger{}, prometheus.NewPedanticRegistry())
	timesCalled := 0

	ddbMock.On("Query").Return(map[string][]byte{}, nil)
	codecMock.On("DecodeMultiKey").Return(descMock, nil)

	c.WatchKey(context.TODO(), key, func(i interface{}) bool {
		ddbMock.AssertNumberOfCalls(t, "Query", timesCalled+1)
		codecMock.AssertNumberOfCalls(t, "DecodeMultiKey", timesCalled+1)
		require.EqualValues(t, descMock, i)
		timesCalled += 1
		return timesCalled < 5
	})
}

func Test_WatchKey_UpdateStale(t *testing.T) {
	backoffConfig.MinBackoff = 1 * time.Millisecond
	backoffConfig.MaxBackoff = 1 * time.Millisecond
	ddbMock := NewDynamodbClientMock()
	codecMock := &CodecMock{}
	c := NewClientMock(ddbMock, codecMock, testLogger{}, prometheus.NewPedanticRegistry())
	staleData := &DescMock{}

	ddbMock.On("Query").Return(map[string][]byte{}, nil).Once()
	codecMock.On("DecodeMultiKey").Return(staleData, nil)
	staleData.On("RefreshData").Once()

	c.WatchKey(context.TODO(), key, func(i interface{}) bool {
		ddbMock.AssertNumberOfCalls(t, "Query", 1)
		codecMock.AssertNumberOfCalls(t, "DecodeMultiKey", 1)
		require.EqualValues(t, staleData, i)
		return false
	})

	ddbMock.On("Query").Return(map[string][]byte{}, errors.Errorf("failed"))
	c.WatchKey(context.TODO(), key, func(i interface{}) bool {
		ddbMock.AssertNumberOfCalls(t, "Query", 12)
		codecMock.AssertNumberOfCalls(t, "DecodeMultiKey", 1)
		require.EqualValues(t, staleData, i)
		return false
	})
}

func Test_WatchPrefix(t *testing.T) {
	backoffConfig.MinBackoff = 1 * time.Millisecond
	backoffConfig.MaxBackoff = 1 * time.Millisecond
	ddbMock := NewDynamodbClientMock()
	codecMock := &CodecMock{}
	c := NewClientMock(ddbMock, codecMock, testLogger{}, prometheus.NewPedanticRegistry())
	data := map[string][]byte{}
	dataKey := []string{"t1", "t2"}
	data[dataKey[0]] = []byte("data" + dataKey[0])
	data[dataKey[1]] = []byte("data" + dataKey[1])
	calls := 0

	ddbMock.On("Query").Return(data, nil).Once()
	codecMock.On("Decode").Twice()

	c.WatchPrefix(context.TODO(), key, func(key string, i interface{}) bool {
		ddbMock.AssertNumberOfCalls(t, "Query", 1)
		require.EqualValues(t, key, dataKey[calls])
		require.EqualValues(t, string(data[dataKey[calls]]), i)
		calls += 1
		return calls < 1
	})
}

// NewClientMock makes a new local dynamodb client.
func NewClientMock(ddbClient dynamoDbClient, cc codec.Codec, logger log.Logger, registerer prometheus.Registerer) *Client {
	m := &Client{
		dynamoDbClient: ddbClient,
		ddbMetrics:     newDynamoDbMetrics(registerer),
		codec:          cc,
		logger:         logger,
		staleData:      make(map[string]codec.MultiKey),
	}

	return m
}

type mockDynamodbClient struct {
	mock.Mock
}

func NewDynamodbClientMock() *mockDynamodbClient {
	return &mockDynamodbClient{}
}

func (m *mockDynamodbClient) List(context.Context, dynamodbKey) ([]string, error) {
	args := m.Called()
	var err error
	if args.Get(1) != nil {
		err = args.Get(1).(error)
	}
	return args.Get(0).([]string), err
}
func (m *mockDynamodbClient) Query(context.Context, dynamodbKey, bool) (map[string][]byte, error) {
	args := m.Called()
	var err error
	if args.Get(1) != nil {
		err = args.Get(1).(error)
	}
	return args.Get(0).(map[string][]byte), err
}
func (m *mockDynamodbClient) DeleteKey(ctx context.Context, key dynamodbKey) error {
	m.Called(ctx, key)
	return nil
}
func (m *mockDynamodbClient) Put(ctx context.Context, key dynamodbKey, data []byte) error {
	m.Called(ctx, key, data)
	return nil
}

type testLogger struct {
}

func (l testLogger) Log(...interface{}) error {
	return nil
}

// String is a code for strings.
type CodecMock struct {
	mock.Mock
}

func (*CodecMock) CodecID() string {
	return "CodecMock"
}

// Decode implements Codec.
func (m *CodecMock) Decode(bytes []byte) (interface{}, error) {
	m.Called()
	return string(bytes), nil
}

// Encode implements Codec.
func (m *CodecMock) Encode(i interface{}) ([]byte, error) {
	m.Called()
	return []byte(i.(string)), nil
}

func (m *CodecMock) EncodeMultiKey(interface{}) (map[string][]byte, error) {
	args := m.Called()
	return args.Get(0).(map[string][]byte), nil
}

func (m *CodecMock) DecodeMultiKey(map[string][]byte) (interface{}, error) {
	args := m.Called()
	var err error
	if args.Get(1) != nil {
		err = args.Get(1).(error)
	}
	return args.Get(0), err
}

type DescMock struct {
	mock.Mock
}

func (m *DescMock) RefreshTimestamp() {
	m.Called()
}

func (m *DescMock) Clone() interface{} {
	args := m.Called()
	return args.Get(0)
}

func (m *DescMock) SplitById() map[string]interface{} {
	args := m.Called()
	return args.Get(0).(map[string]interface{})
}

func (m *DescMock) JoinIds(map[string]interface{}) {
	m.Called()
}

func (m *DescMock) GetItemFactory() proto.Message {
	args := m.Called()
	return args.Get(0).(proto.Message)
}

func (m *DescMock) FindDifference(that codec.MultiKey) (interface{}, []string, error) {
	args := m.Called(that)
	var err error
	if args.Get(2) != nil {
		err = args.Get(2).(error)
	}
	return args.Get(0).(interface{}), args.Get(1).([]string), err
}
