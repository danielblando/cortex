package dynamodb

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/go-kit/log"
	"strconv"
	"time"
)

type dynamodbKey struct {
	primaryKey string
	sortKey    string
}

type dynamoDbClient interface {
	List(ctx context.Context, key dynamodbKey) ([]string, error)
	Query(ctx context.Context, key dynamodbKey, isPrefix bool) (map[string][]byte, error)
	DeleteKey(ctx context.Context, key dynamodbKey) error
	Put(ctx context.Context, key dynamodbKey, data []byte) error
}

type dynamodbKV struct {
	dynamoDbClient

	ddbClient dynamodbiface.DynamoDBAPI
	logger    log.Logger
	tableName *string
}

var (
	primaryKey   = "RingKey"
	sortKey      = "InstanceKey"
	contentData  = "Data"
	lastModified = "LastModified"
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
	}

	return *ddbKV, nil
}

func validateConfigInput(cfg Config) error {
	if len(cfg.TableName) < 3 {
		return fmt.Errorf("invalid dynamodb table name: %T", cfg.TableName)
	}

	return nil
}

func (kv dynamodbKV) List(ctx context.Context, key dynamodbKey) ([]string, error) {
	var keys []string
	input := &dynamodb.QueryInput{
		TableName: kv.tableName,
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
		for _, item := range output.Items {
			keys = append(keys, item[sortKey].String())
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	return keys, nil
}

func (kv dynamodbKV) Query(ctx context.Context, key dynamodbKey, isPrefix bool) (map[string][]byte, error) {
	keys := make(map[string][]byte)
	co := dynamodb.ComparisonOperatorEq
	if isPrefix {
		co = dynamodb.ComparisonOperatorBeginsWith
	}
	input := &dynamodb.QueryInput{
		TableName: kv.tableName,
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
		for _, item := range output.Items {
			keys[*item[sortKey].S] = item[contentData].B
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	return keys, nil
}

func (kv dynamodbKV) DeleteKey(ctx context.Context, key dynamodbKey) error {
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
	item[lastModified] = &dynamodb.AttributeValue{
		N: aws.String(strconv.FormatInt(time.Now().UTC().UnixMilli(), 10)),
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
