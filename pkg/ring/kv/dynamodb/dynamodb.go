package dynamodb

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/go-kit/log"
)

type dynamodbKey struct {
	primaryKey string
	sortKey    string
}

type dynamoDbClient interface {
	List(ctx context.Context, key dynamodbKey) ([]string, float64, error)
	Query(ctx context.Context, key dynamodbKey, isPrefix bool) (map[string][]byte, float64, error)
	Delete(ctx context.Context, key dynamodbKey) error
	Put(ctx context.Context, key dynamodbKey, data []byte) error
}

type dynamodbKV struct {
	dynamoDbClient

	ddbClient dynamodbiface.DynamoDBAPI
	logger    log.Logger
	tableName *string
	ttlValue  time.Duration
}

var (
	primaryKey  = "RingKey"
	sortKey     = "InstanceKey"
	contentData = "Data"
	timeToLive  = "ttl"
)

func newDynamodbKV(cfg Config, logger log.Logger) (dynamodbKV, error) {
	if err := validateConfigInput(cfg); err != nil {
		return dynamodbKV{}, err
	}

	sess, err := session.NewSession()
	if err != nil {
		return dynamodbKV{}, err
	}

	if len(cfg.Region) > 0 {
		sess.Config = &aws.Config{
			Region: aws.String(cfg.Region),
		}
	}

	dynamoDB := dynamodb.New(sess)

	ddbKV := &dynamodbKV{
		ddbClient: dynamoDB,
		logger:    logger,
		tableName: aws.String(cfg.TableName),
		ttlValue:  cfg.TTL,
	}

	return *ddbKV, nil
}

func validateConfigInput(cfg Config) error {
	if len(cfg.TableName) < 3 {
		return fmt.Errorf("invalid dynamodb table name: %s", cfg.TableName)
	}

	return nil
}

// for testing
func (kv dynamodbKV) getTTL() time.Duration {
	return kv.ttlValue
}

func (kv dynamodbKV) List(ctx context.Context, key dynamodbKey) ([]string, float64, error) {
	var keys []string
	var totalCapacity float64
	input := &dynamodb.QueryInput{
		TableName:              kv.tableName,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		KeyConditions: map[string]*dynamodb.Condition{
			primaryKey: {
				ComparisonOperator: aws.String("EQ"),
				AttributeValueList: []*dynamodb.AttributeValue{
					{
						S: aws.String(key.primaryKey),
					},
				},
			},
		},
		AttributesToGet: []*string{aws.String(sortKey)},
	}

	err := kv.ddbClient.QueryPagesWithContext(ctx, input, func(output *dynamodb.QueryOutput, _ bool) bool {
		totalCapacity += getCapacityUnits(output.ConsumedCapacity)
		for _, item := range output.Items {
			keys = append(keys, item[sortKey].String())
		}
		return true
	})
	if err != nil {
		return nil, totalCapacity, err
	}

	return keys, totalCapacity, nil
}

func (kv dynamodbKV) Query(ctx context.Context, key dynamodbKey, isPrefix bool) (map[string][]byte, float64, error) {
	keys := make(map[string][]byte)
	var totalCapacity float64
	co := dynamodb.ComparisonOperatorEq
	if isPrefix {
		co = dynamodb.ComparisonOperatorBeginsWith
	}
	input := &dynamodb.QueryInput{
		TableName:              kv.tableName,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		KeyConditions: map[string]*dynamodb.Condition{
			primaryKey: {
				ComparisonOperator: aws.String(co),
				AttributeValueList: []*dynamodb.AttributeValue{
					{
						S: aws.String(key.primaryKey),
					},
				},
			},
		},
	}

	err := kv.ddbClient.QueryPagesWithContext(ctx, input, func(output *dynamodb.QueryOutput, _ bool) bool {
		totalCapacity += getCapacityUnits(output.ConsumedCapacity)
		for _, item := range output.Items {
			keys[*item[sortKey].S] = item[contentData].B
		}
		return true
	})
	if err != nil {
		return nil, totalCapacity, err
	}

	return keys, totalCapacity, nil
}

func (kv dynamodbKV) Delete(ctx context.Context, key dynamodbKey) error {
	input := &dynamodb.DeleteItemInput{
		TableName: kv.tableName,
		Key:       generateItemKey(key),
	}
	_, err := kv.ddbClient.DeleteItemWithContext(ctx, input)
	return err
}

func (kv dynamodbKV) Put(ctx context.Context, key dynamodbKey, data []byte) error {
	item := generateItemKey(key)
	item[contentData] = &dynamodb.AttributeValue{
		B: data,
	}
	if kv.getTTL() > 0 {
		item[timeToLive] = &dynamodb.AttributeValue{
			N: aws.String(strconv.FormatInt(time.Now().UTC().Add(kv.getTTL()).Unix(), 10)),
		}
	}

	input := &dynamodb.PutItemInput{
		TableName: kv.tableName,
		Item:      item,
	}
	_, err := kv.ddbClient.PutItemWithContext(ctx, input)
	return err
}

func generateItemKey(key dynamodbKey) map[string]*dynamodb.AttributeValue {
	resp := map[string]*dynamodb.AttributeValue{
		primaryKey: {
			S: aws.String(key.primaryKey),
		},
	}
	if len(key.sortKey) > 0 {
		resp[sortKey] = &dynamodb.AttributeValue{
			S: aws.String(key.sortKey),
		}
	}

	return resp
}

func getCapacityUnits(cap *dynamodb.ConsumedCapacity) float64 {
	if cap != nil && cap.CapacityUnits != nil {
		return *cap.CapacityUnits
	}
	return 0
}
