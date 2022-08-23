package dynamoDbMultiKey

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"strings"
	"time"
)

// Client implements kv.Client for etcd.
type Client struct {
	codec     codec.Codec
	ddbClient dynamodbiface.DynamoDBAPI
	logger    log.Logger
}

// NewClient returns a new Client.
func NewClient(codec codec.Codec, logger log.Logger) (*Client, error) {
	session, err := session.NewSession(&aws.Config{
		Region: aws.String("us-west-2"),
	})
	if err != nil {
		return nil, err
	}

	dynamoDB := dynamodb.New(session)

	c := &Client{
		ddbClient: dynamoDB,
		codec:     codec,
		logger:    logger,
	}
	return c, nil
}

// CAS implements kv.Client.
func (c *Client) CAS(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error {
	var lastErr error

	for i := 0; i < 10; i++ {
		resp, err := c.getWithPrefix(ctx, key)
		if err != nil {
			level.Error(c.logger).Log("msg", "error getting key", "key", key, "err", err)
			lastErr = err
			continue
		}

		var intermediate, original interface{}
		if len(resp) > 0 {
			original, err = c.codec.DecodeMultiKey(resp)
			intermediate, err = c.codec.DecodeMultiKey(resp)
			if err != nil {
				level.Error(c.logger).Log("msg", "error decoding key", "key", key, "err", err)
				lastErr = err
				continue
			}
		}

		var retry bool
		out, retry, err := f(intermediate)
		if err != nil {
			if !retry {
				return err
			}
			lastErr = err
			continue
		}

		// Callback returning nil means it doesn't want to CAS anymore.
		if out == nil {
			return nil
		}

		if original == nil {
			original = c.codec.GetFactory()()
		}
		toUpdate, toDelete, err := original.(codec.MultiKey).FindDifference(out.(codec.MultiKey))
		if err != nil {
			level.Error(c.logger).Log("msg", "error getting key", "key", key, "err", err)
			lastErr = err
			continue
		}

		for _, childKey := range toDelete {
			c.Delete(ctx, c.getKey(key, childKey))
		}

		buf, err := c.codec.EncodeMultiKey(toUpdate)
		if err != nil {
			level.Error(c.logger).Log("msg", "error serialising value", "key", key, "err", err)
			lastErr = err
			continue
		}

		for childKey, bytes := range buf {
			err := c.Save(ctx, key, childKey, bytes)
			if err != nil {
				level.Error(c.logger).Log("msg", "error CASing", "key", key, "err", err)
				lastErr = err
				continue
			}
		}

		return nil
	}

	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("failed to CAS %s", key)
}

// WatchKey implements kv.Client.
func (c *Client) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	for true {
		time.Sleep(1 * time.Minute)
		out, err := c.Get(ctx, key)
		if err != nil {
			level.Error(c.logger).Log("msg", "error decoding key", "key", key, "err", err)
			continue
		}
		if !f(out) {
			return
		}
	}
}

// WatchPrefix implements kv.Client.
func (c *Client) WatchPrefix(ctx context.Context, key string, f func(string, interface{}) bool) {
	level.Error(c.logger).Log("msg", "etcdMulti WatchPrefix not implemented")
	return
}

// List implements kv.Client.
func (c *Client) List(ctx context.Context, prefix string) ([]string, error) {
	return nil, errors.Errorf("msg", "etcdMult List not implemented")
}

// Get implements kv.Client.
func (c *Client) Get(ctx context.Context, prefix string) (interface{}, error) {
	resp, err := c.getWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	return c.codec.DecodeMultiKey(resp)
}

// Get implements kv.Client.
func (c *Client) getWithPrefix(ctx context.Context, prefix string) (map[string][]byte, error) {
	keys := make(map[string][]byte)
	input := &dynamodb.QueryInput{
		TableName: aws.String("CortexRingKVStore"),
		KeyConditions: map[string]*dynamodb.Condition{
			"RingKey": {
				ComparisonOperator: aws.String("EQ"),
				AttributeValueList: []*dynamodb.AttributeValue{
					{
						S: aws.String(prefix),
					},
				},
			},
		},
	}

	count := 0
	c.ddbClient.QueryPagesWithContext(ctx, input, func(output *dynamodb.QueryOutput, _ bool) bool {
		for _, item := range output.Items {
			keys[*item["InstanceKey"].S] = item["Data"].B
		}
		count++
		return true
	})
	return keys, nil
}

// Delete implements kv.Client.
func (c *Client) Delete(ctx context.Context, key string) error {
	keys := c.splitKey(key)
	input := &dynamodb.DeleteItemInput{
		TableName: aws.String("CortexRingKVStore"),
		Key: map[string]*dynamodb.AttributeValue{
			"RingKey": {
				S: aws.String(keys[0]),
			},
			"InstanceKey": {
				S: aws.String(keys[1]),
			},
		},
	}

	_, err := c.ddbClient.DeleteItemWithContext(ctx, input)
	return err
}

func (c *Client) Save(ctx context.Context, key string, childKey string, data []byte) error {
	input := &dynamodb.PutItemInput{
		TableName: aws.String("CortexRingKVStore"),
		Item: map[string]*dynamodb.AttributeValue{
			"RingKey": {
				S: aws.String(key),
			},
			"InstanceKey": {
				S: aws.String(childKey),
			},
			"Data": {
				B: data,
			},
		},
	}

	_, err := c.ddbClient.PutItemWithContext(ctx, input)
	return err
}

func (c *Client) getKey(ringKey string, innerKey string) string {
	return ringKey + "/" + innerKey
}

func (c *Client) splitKey(key string) []string {
	return strings.Split(key, "/")
}
