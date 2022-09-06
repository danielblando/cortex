package dynamodb

import (
	"context"
	"flag"
	"fmt"
	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/util/backoff"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
	"time"
)

// Config to create a ConsulClient
type Config struct {
	Region    string `yaml:"region"`
	TableName string `yaml:"table_name"`
}

type Client struct {
	dynamoDbClient
	codec      codec.Codec
	ddbMetrics *dynamodbMetrics
	logger     log.Logger

	staleDataLock sync.RWMutex
	staleData     map[string]codec.MultiKey
}

var (
	backoffConfig = backoff.Config{
		MinBackoff: 1 * time.Second,
		MaxBackoff: 1 * time.Minute,
		MaxRetries: 10,
	}

	watchKeyWait = 1 * time.Minute
)

// RegisterFlags adds the flags required to config this to the given FlagSet
// If prefix is not an empty string it should end with a period.
func (cfg *Config) RegisterFlags(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Region, prefix+"dynamodb.region", "", "Region to access dynamodb.")
	f.StringVar(&cfg.TableName, prefix+"dynamodb.table-name", "", "Table name to use on dynamodb.")
}

func NewClient(cfg Config, cc codec.Codec, logger log.Logger, registerer prometheus.Registerer) (*Client, error) {
	dynamoDB, err := newDynamodbKV(cfg, logger)
	if err != nil {
		return nil, err
	}

	ddbMetrics := newDynamoDbMetrics(registerer)

	c := &Client{
		dynamoDbClient: dynamodbInstrumentation{kv: dynamoDB, ddbMetrics: ddbMetrics},
		codec:          cc,
		logger:         logger,
		ddbMetrics:     ddbMetrics,
		staleData:      make(map[string]codec.MultiKey),
	}
	return c, nil
}

func (c Client) List(ctx context.Context, key string) ([]string, error) {
	resp, err := c.List(ctx, key)
	if err != nil {
		level.Warn(c.logger).Log("msg", "error listing dynamodb", "key", key, "err", err)
		return nil, err
	}
	return resp, err
}

func (c Client) Get(ctx context.Context, key string) (interface{}, error) {
	resp, err := c.query(ctx, key, false)
	if err != nil {
		level.Warn(c.logger).Log("msg", "error Get dynamodb", "key", key, "err", err)
		return nil, err
	}
	res, err := c.decodeMultikey(resp)
	if err != nil {
		return nil, err
	}
	c.updateStaleData(key, res)

	return res, nil
}

func (c Client) Delete(ctx context.Context, key string) error {
	resp, err := c.query(ctx, key, false)
	if err != nil {
		level.Warn(c.logger).Log("msg", "error Delete dynamodb", "key", key, "err", err)
		return err
	}

	for innerKey, _ := range resp {
		err := c.DeleteKey(ctx, dynamodbKey{
			primaryKey: key,
			sortKey:    innerKey,
		})
		if err != nil {
			level.Warn(c.logger).Log("msg", "error Delete dynamodb", "key", key, "innerKey", innerKey, "err", err)
			return err
		}
	}
	return err
}

func (c Client) CAS(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error {
	var lastErr error
	backoff := backoff.New(ctx, backoffConfig)
	for backoff.Ongoing() {
		resp, err := c.query(ctx, key, false)
		if err != nil {
			lastErr = err
			backoff.Wait()
			continue
		}

		current, err := c.decodeMultikey(resp)
		if err != nil {
			level.Error(c.logger).Log("msg", "error decoding key", "key", key, "err", err)
			lastErr = err
			continue
		}

		out, retry, err := f(current.Clone())
		if err != nil {
			if !retry {
				return err
			}
			lastErr = err
			backoff.Wait()
			continue
		}

		// Callback returning nil means it doesn't want to CAS anymore.
		if out == nil {
			return nil
		}
		// Don't even try

		r, ok := out.(codec.MultiKey)
		if !ok || r == nil {
			return fmt.Errorf("invalid type: %T, expected MultikKey", out)
		}

		toUpdate, toDelete, err := current.FindDifference(r)

		if err != nil {
			level.Error(c.logger).Log("msg", "error getting key", "key", key, "err", err)
			lastErr = err
			continue
		}

		buf, err := c.codec.EncodeMultiKey(toUpdate)
		if err != nil {
			level.Error(c.logger).Log("msg", "error serialising value", "key", key, "err", err)
			lastErr = err
			continue
		}

		for childKey, bytes := range buf {
			err := c.Put(ctx, dynamodbKey{primaryKey: key, sortKey: childKey}, bytes)
			if err != nil {
				level.Error(c.logger).Log("msg", "error CASing", "key", key, "err", err)
				lastErr = err
				continue
			}
		}

		for _, childKey := range toDelete {
			c.DeleteKey(ctx, dynamodbKey{primaryKey: key, sortKey: childKey})
		}

		return nil
	}

	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("failed to CAS %s", key)
}

func (c Client) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	backoff := backoff.New(ctx, backoffConfig)

	for true {
		out, err := c.Query(ctx, dynamodbKey{
			primaryKey: key,
		}, false)
		if err != nil {
			level.Error(c.logger).Log("msg", "WatchKey", "key", key, "err", err)

			if !backoff.Ongoing() && c.staleData[key] != nil {
				backoff.Reset()
				c.staleData[key].RefreshTimestamp()
				if !f(c.staleData[key]) {
					return
				}
			}
			backoff.Wait()
			continue
		}

		decoded, err := c.decodeMultikey(out)
		if err != nil {
			level.Error(c.logger).Log("msg", "error decoding key", "key", key, "err", err)
			continue
		}
		c.updateStaleData(key, decoded)

		if !f(decoded) {
			return
		}

		backoff.Reset()
		select {
		case <-ctx.Done():
			return
		case <-time.After(watchKeyWait):
		}
	}
}

func (c Client) WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool) {
	backoff := backoff.New(ctx, backoffConfig)

	for true {
		out, err := c.Query(ctx, dynamodbKey{
			primaryKey: prefix,
		}, true)
		if err != nil {
			level.Error(c.logger).Log("msg", "WatchPrefix", "prefix", prefix, "err", err)
			continue
		}

		for key, bytes := range out {
			decoded, err := c.codec.Decode(bytes)
			if err != nil {
				level.Error(c.logger).Log("msg", "error decoding key", "key", key, "err", err)
				continue
			}

			if !f(key, decoded) {
				return
			}

		}

		backoff.Reset()
		select {
		case <-ctx.Done():
			return
		case <-time.After(watchKeyWait):
		}
	}
}

func (c Client) query(ctx context.Context, pkey string, isPrefix bool) (map[string][]byte, error) {
	ddbKey := dynamodbKey{
		primaryKey: pkey,
	}

	out, err := c.Query(ctx, ddbKey, isPrefix)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (c Client) decodeMultikey(data map[string][]byte) (codec.MultiKey, error) {
	res, err := c.codec.DecodeMultiKey(data)
	if err != nil {
		return nil, err
	}
	out, ok := res.(codec.MultiKey)
	if !ok || out == nil {
		return nil, fmt.Errorf("invalid type: %T, expected MultikKey", out)
	}

	return out, nil
}

func (c Client) updateStaleData(key string, data codec.MultiKey) {
	c.staleDataLock.RLock()
	c.staleData[key] = data
	c.staleDataLock.RUnlock()
}
