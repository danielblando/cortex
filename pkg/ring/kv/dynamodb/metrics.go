package dynamodb

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	grpcUtils "github.com/weaveworks/common/grpc"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/instrument"
	"strconv"
)

type dynamodbInstrumentation struct {
	kv         dynamodbKV
	ddbMetrics *dynamodbMetrics
}

type dynamodbMetrics struct {
	dynamodbRequestDuration *instrument.HistogramCollector
}

func newDynamoDbMetrics(registerer prometheus.Registerer) *dynamodbMetrics {
	dynamodbRequestDurationCollector := instrument.NewHistogramCollector(promauto.With(registerer).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dynamodb_kv_request_duration_seconds",
		Help:    "Time spent on dynamodb requests.",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation", "status_code"}))
	dynamodbMetrics := dynamodbMetrics{dynamodbRequestDurationCollector}
	return &dynamodbMetrics
}

func (d dynamodbInstrumentation) List(ctx context.Context, key dynamodbKey) ([]string, error) {
	var resp []string
	err := instrument.CollectedRequest(ctx, "List", d.ddbMetrics.dynamodbRequestDuration, errorCode, func(ctx context.Context) error {
		var err error
		resp, err = d.kv.List(ctx, key)
		return err
	})
	return resp, err
}

func (d dynamodbInstrumentation) Query(ctx context.Context, key dynamodbKey, isPrefix bool) (map[string][]byte, error) {
	var resp map[string][]byte
	err := instrument.CollectedRequest(ctx, "Query", d.ddbMetrics.dynamodbRequestDuration, errorCode, func(ctx context.Context) error {
		var err error
		resp, err = d.kv.Query(ctx, key, isPrefix)
		return err
	})
	return resp, err
}

func (d dynamodbInstrumentation) DeleteKey(ctx context.Context, key dynamodbKey) error {
	return instrument.CollectedRequest(ctx, "Delete", d.ddbMetrics.dynamodbRequestDuration, errorCode, func(ctx context.Context) error {
		return d.kv.DeleteKey(ctx, key)
	})
}

func (d dynamodbInstrumentation) Put(ctx context.Context, key dynamodbKey, data []byte) error {
	return instrument.CollectedRequest(ctx, "Put", d.ddbMetrics.dynamodbRequestDuration, errorCode, func(ctx context.Context) error {
		return d.kv.Put(ctx, key, data)
	})
}

// errorCode converts an error into an error code string.
func errorCode(err error) string {
	if err == nil {
		return "2xx"
	}

	if errResp, ok := httpgrpc.HTTPResponseFromError(err); ok {
		statusFamily := int(errResp.Code / 100)
		return strconv.Itoa(statusFamily) + "xx"
	} else if grpcUtils.IsCanceled(err) {
		return "cancel"
	} else {
		return "error"
	}
}
