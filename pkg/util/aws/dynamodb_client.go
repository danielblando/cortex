package aws

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/backoff"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/math"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/instrument"
	"golang.org/x/time/rate"
	"strings"
	"time"
)

var (
	ddbRequestDuration = instrument.NewHistogramCollector(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "dynamodb_request_duration_seconds",
		Help:      "Time spent doing dynamodb requests.",
		Buckets:   []float64{.025, .05, .1, .25, .5, 1, 2},
	}, []string{"operation", "status_code"}))
)

const (
	hashKey  = "h"
	rangeKey = "r"
	valueKey = "c"

	// For dynamodb errors
	tableNameLabel   = "table"
	errorReasonLabel = "error"
	otherError       = "other"

	// See http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html.
	dynamoDBMaxWriteBatchSize = 25
	dynamoDBMaxReadBatchSize  = 100
	validationException       = "ValidationException"
)

func init() {
	ddbRequestDuration.Register()
}

// MetricsAutoScalingConfig holds parameters to configure how it works
type MetricsAutoScalingConfig struct {
	URL              string  `yaml:"url"`                   // URL to contact Prometheus store on
	TargetQueueLen   int64   `yaml:"target_queue_length"`   // Queue length above which we will scale up capacity
	ScaleUpFactor    float64 `yaml:"scale_up_factor"`       // Scale up capacity by this multiple
	MinThrottling    float64 `yaml:"ignore_throttle_below"` // Ignore throttling below this level
	QueueLengthQuery string  `yaml:"queue_length_query"`    // Promql query to fetch ingester queue length
	ThrottleQuery    string  `yaml:"write_throttle_query"`  // Promql query to fetch throttle rate per table
	UsageQuery       string  `yaml:"write_usage_query"`     // Promql query to fetch write capacity usage per table
	ReadUsageQuery   string  `yaml:"read_usage_query"`      // Promql query to fetch read usage per table
	ReadErrorQuery   string  `yaml:"read_error_query"`      // Promql query to fetch read errors per table
}

// DynamoDBConfig specifies config for a DynamoDB database.
type DynamoDBConfig struct {
	DynamoDB               flagext.URLValue         `yaml:"dynamodb_url"`
	APILimit               float64                  `yaml:"api_limit"`
	ThrottleLimit          float64                  `yaml:"throttle_limit"`
	Metrics                MetricsAutoScalingConfig `yaml:"metrics"`
	ChunkGangSize          int                      `yaml:"chunk_gang_size"`
	ChunkGetMaxParallelism int                      `yaml:"chunk_get_max_parallelism"`
	BackoffConfig          backoff.Config           `yaml:"backoff_config"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *DynamoDBConfig) RegisterFlags(f *flag.FlagSet) {
	f.Var(&cfg.DynamoDB, "dynamodb.url", "DynamoDB endpoint URL with escaped Key and Secret encoded. "+
		"If only region is specified as a host, proper endpoint will be deduced. Use inmemory:///<table-name> to use a mock in-memory implementation.")
	f.Float64Var(&cfg.APILimit, "dynamodb.api-limit", 2.0, "DynamoDB table management requests per second limit.")
	f.Float64Var(&cfg.ThrottleLimit, "dynamodb.throttle-limit", 10.0, "DynamoDB rate cap to back off when throttled.")
	f.IntVar(&cfg.ChunkGangSize, "dynamodb.chunk-gang-size", 10, "Number of chunks to group together to parallelise fetches (zero to disable)")
	f.IntVar(&cfg.ChunkGetMaxParallelism, "dynamodb.chunk.get-max-parallelism", 32, "Max number of chunk-get operations to start in parallel")
	f.DurationVar(&cfg.BackoffConfig.MinBackoff, "dynamodb.min-backoff", 100*time.Millisecond, "Minimum backoff time")
	f.DurationVar(&cfg.BackoffConfig.MaxBackoff, "dynamodb.max-backoff", 50*time.Second, "Maximum backoff time")
	f.IntVar(&cfg.BackoffConfig.MaxRetries, "dynamodb.max-retries", 20, "Maximum number of times to retry an operation")
}

type DynamoDBStorageClient struct {
	cfg DynamoDBConfig

	DynamoDB dynamodbiface.DynamoDBAPI
	// These rate-limiters let us slow down when DynamoDB signals provision limits.
	writeThrottle *rate.Limiter

	// These functions exists for mocking, so we don't have to write a whole load
	// of boilerplate.
	batchGetItemRequestFn   func(ctx context.Context, input *dynamodb.BatchGetItemInput) dynamoDBRequest
	batchWriteItemRequestFn func(ctx context.Context, input *dynamodb.BatchWriteItemInput) dynamoDBRequest

	metrics *dynamoDBMetrics
}

// StorageConfig specifies config for storing data on AWS.
type StorageConfig struct {
	DynamoDBConfig `yaml:"dynamodb"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *StorageConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.DynamoDBConfig.RegisterFlags(f)
}

// NewDynamoDBStorageClient makes a new DynamoDB-backed IndexClient and chunk.Client.
func NewDynamoDBStorageClient(cfg DynamoDBConfig, reg prometheus.Registerer) (*DynamoDBStorageClient, error) {
	session, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	dynamoDB := dynamodb.New(session)
	client := &DynamoDBStorageClient{
		cfg:           cfg,
		DynamoDB:      dynamoDB,
		writeThrottle: rate.NewLimiter(rate.Limit(cfg.ThrottleLimit), dynamoDBMaxWriteBatchSize),
		metrics:       newMetrics(reg),
	}
	client.batchGetItemRequestFn = client.batchGetItemRequest
	client.batchWriteItemRequestFn = client.batchWriteItemRequest
	return client, nil
}

func (a DynamoDBStorageClient) BatchWrite(ctx context.Context, input dynamoDBWriteBatch) error {
	outstanding := input
	unprocessed := dynamoDBWriteBatch{}

	backoff := backoff.New(ctx, a.cfg.BackoffConfig)

	for outstanding.Len()+unprocessed.Len() > 0 && backoff.Ongoing() {
		requests := dynamoDBWriteBatch{}
		requests.TakeReqs(outstanding, dynamoDBMaxWriteBatchSize)
		requests.TakeReqs(unprocessed, dynamoDBMaxWriteBatchSize)

		request := a.batchWriteItemRequestFn(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems:           requests,
			ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		})

		err := instrument.CollectedRequest(ctx, "DynamoDB.BatchWriteItem", a.metrics.dynamoRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
			return request.Send()
		})
		resp := request.Data().(*dynamodb.BatchWriteItemOutput)

		for _, cc := range resp.ConsumedCapacity {
			a.metrics.dynamoConsumedCapacity.WithLabelValues("DynamoDB.BatchWriteItem", *cc.TableName).
				Add(float64(*cc.CapacityUnits))
		}

		if err != nil {
			for tableName := range requests {
				recordDynamoError(tableName, err, "DynamoDB.BatchWriteItem", a.metrics)
			}

			// If we get provisionedThroughputExceededException, then no items were processed,
			// so back off and retry all.
			if awsErr, ok := err.(awserr.Error); ok && ((awsErr.Code() == dynamodb.ErrCodeProvisionedThroughputExceededException) || request.Retryable()) {
				logWriteRetry(requests, a.metrics)
				unprocessed.TakeReqs(requests, -1)
				_ = a.writeThrottle.WaitN(ctx, len(requests))
				backoff.Wait()
				continue
			} else if ok && awsErr.Code() == validationException {
				// this write will never work, so the only option is to drop the offending items and continue.
				level.Warn(log.Logger).Log("msg", "Data lost while flushing to DynamoDB", "err", awsErr)
				level.Debug(log.Logger).Log("msg", "Dropped request details", "requests", requests)
				util.Event().Log("msg", "ValidationException", "requests", requests)
				// recording the drop counter separately from recordDynamoError(), as the error code alone may not provide enough context
				// to determine if a request was dropped (or not)
				for tableName := range requests {
					a.metrics.dynamoDroppedRequests.WithLabelValues(tableName, validationException, "DynamoDB.BatchWriteItem").Inc()
				}
				continue
			}

			// All other errors are critical.
			return err
		}

		// If there are unprocessed items, retry those items.
		unprocessedItems := dynamoDBWriteBatch(resp.UnprocessedItems)
		if len(unprocessedItems) > 0 {
			logWriteRetry(unprocessedItems, a.metrics)
			_ = a.writeThrottle.WaitN(ctx, unprocessedItems.Len())
			unprocessed.TakeReqs(unprocessedItems, -1)
		}

		backoff.Reset()
	}

	if valuesLeft := outstanding.Len() + unprocessed.Len(); valuesLeft > 0 {
		return fmt.Errorf("failed to write items to DynamoDB, %d values remaining: %s", valuesLeft, backoff.Err())
	}
	return backoff.Err()
}

func logWriteRetry(unprocessed dynamoDBWriteBatch, metrics *dynamoDBMetrics) {
	for table, reqs := range unprocessed {
		metrics.dynamoThrottled.WithLabelValues("DynamoDB.BatchWriteItem", table).Add(float64(len(reqs)))
		for _, req := range reqs {
			item := req.PutRequest.Item
			var hash, rnge string
			if hashAttr, ok := item[hashKey]; ok {
				if hashAttr.S != nil {
					hash = *hashAttr.S
				}
			}
			if rangeAttr, ok := item[rangeKey]; ok {
				rnge = string(rangeAttr.B)
			}
			util.Event().Log("msg", "store retry", "table", table, "hashKey", hash, "rangeKey", rnge)
		}
	}
}

func recordDynamoError(tableName string, err error, operation string, metrics *dynamoDBMetrics) {
	if awsErr, ok := err.(awserr.Error); ok {
		metrics.dynamoFailures.WithLabelValues(tableName, awsErr.Code(), operation).Add(float64(1))
	} else {
		metrics.dynamoFailures.WithLabelValues(tableName, otherError, operation).Add(float64(1))
	}
}

// map key is table name; value is a slice of things to 'put'
type dynamoDBWriteBatch map[string][]*dynamodb.WriteRequest

func (b dynamoDBWriteBatch) Len() int {
	result := 0
	for _, reqs := range b {
		result += len(reqs)
	}
	return result
}

func (b dynamoDBWriteBatch) String() string {
	var sb strings.Builder
	sb.WriteByte('{')
	for k, reqs := range b {
		sb.WriteString(k)
		sb.WriteString(": [")
		for _, req := range reqs {
			sb.WriteString(req.String())
			sb.WriteByte(',')
		}
		sb.WriteString("], ")
	}
	sb.WriteByte('}')
	return sb.String()
}

func (b dynamoDBWriteBatch) Add(tableName, hashValue string, rangeValue []byte, value []byte) {
	item := map[string]*dynamodb.AttributeValue{
		hashKey:  {S: aws.String(hashValue)},
		rangeKey: {B: rangeValue},
	}

	if value != nil {
		item[valueKey] = &dynamodb.AttributeValue{B: value}
	}

	b[tableName] = append(b[tableName], &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{
			Item: item,
		},
	})
}

func (b dynamoDBWriteBatch) Delete(tableName, hashValue string, rangeValue []byte) {
	b[tableName] = append(b[tableName], &dynamodb.WriteRequest{
		DeleteRequest: &dynamodb.DeleteRequest{
			Key: map[string]*dynamodb.AttributeValue{
				hashKey:  {S: aws.String(hashValue)},
				rangeKey: {B: rangeValue},
			},
		},
	})
}

func (b dynamoDBWriteBatch) TakeReqs(from dynamoDBWriteBatch, max int) {
	outLen, inLen := b.Len(), from.Len()
	toFill := inLen
	if max > 0 {
		toFill = math.Min(inLen, max-outLen)
	}
	for toFill > 0 {
		for tableName, fromReqs := range from {
			taken := math.Min(len(fromReqs), toFill)
			if taken > 0 {
				b[tableName] = append(b[tableName], fromReqs[:taken]...)
				from[tableName] = fromReqs[taken:]
				toFill -= taken
			}
		}
	}
}

type dynamoDBRequest interface {
	Send() error
	Data() interface{}
	Error() error
	Retryable() bool
}

func (a DynamoDBStorageClient) batchGetItemRequest(ctx context.Context, input *dynamodb.BatchGetItemInput) dynamoDBRequest {
	req, _ := a.DynamoDB.BatchGetItemRequest(input)
	req.SetContext(ctx)
	return dynamoDBRequestAdapter{req}
}

func (a DynamoDBStorageClient) batchWriteItemRequest(ctx context.Context, input *dynamodb.BatchWriteItemInput) dynamoDBRequest {
	req, _ := a.DynamoDB.BatchWriteItemRequest(input)
	req.SetContext(ctx)
	return dynamoDBRequestAdapter{req}
}

type dynamoDBRequestAdapter struct {
	request *request.Request
}

func (a dynamoDBRequestAdapter) Data() interface{} {
	return a.request.Data
}

func (a dynamoDBRequestAdapter) Send() error {
	return a.request.Send()
}

func (a dynamoDBRequestAdapter) Error() error {
	return a.request.Error
}

func (a dynamoDBRequestAdapter) Retryable() bool {
	return aws.BoolValue(a.request.Retryable)
}

type dynamoDBMetrics struct {
	dynamoRequestDuration  *instrument.HistogramCollector
	dynamoConsumedCapacity *prometheus.CounterVec
	dynamoThrottled        *prometheus.CounterVec
	dynamoFailures         *prometheus.CounterVec
	dynamoDroppedRequests  *prometheus.CounterVec
	dynamoQueryPagesCount  prometheus.Histogram
}

func newMetrics(r prometheus.Registerer) *dynamoDBMetrics {
	m := dynamoDBMetrics{}

	m.dynamoRequestDuration = instrument.NewHistogramCollector(promauto.With(r).NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "dynamo_request_duration_seconds",
		Help:      "Time spent doing DynamoDB requests.",

		// DynamoDB latency seems to range from a few ms to a several seconds and is
		// important.  So use 9 buckets from 1ms to just over 1 minute (65s).
		Buckets: prometheus.ExponentialBuckets(0.001, 4, 9),
	}, []string{"operation", "status_code"}))
	m.dynamoConsumedCapacity = promauto.With(r).NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "dynamo_consumed_capacity_total",
		Help:      "The capacity units consumed by operation.",
	}, []string{"operation", tableNameLabel})
	m.dynamoThrottled = promauto.With(r).NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "dynamo_throttled_total",
		Help:      "The total number of throttled events.",
	}, []string{"operation", tableNameLabel})
	m.dynamoFailures = promauto.With(r).NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "dynamo_failures_total",
		Help:      "The total number of errors while storing chunks to the chunk store.",
	}, []string{tableNameLabel, errorReasonLabel, "operation"})
	m.dynamoDroppedRequests = promauto.With(r).NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "dynamo_dropped_requests_total",
		Help:      "The total number of requests which were dropped due to errors encountered from dynamo.",
	}, []string{tableNameLabel, errorReasonLabel, "operation"})
	m.dynamoQueryPagesCount = promauto.With(r).NewHistogram(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "dynamo_query_pages_count",
		Help:      "Number of pages per query.",
		// Most queries will have one page, however this may increase with fuzzy
		// metric names.
		Buckets: prometheus.ExponentialBuckets(1, 4, 6),
	})

	return &m
}
