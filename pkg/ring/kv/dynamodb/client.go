package dynamodb

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/util/backoff"
)

// Config to create a ConsulClient
type Config struct {
	Region    string        `yaml:"region"`
	TableName string        `yaml:"table_name"`
	TTL       time.Duration `yaml:"ttl"`
}

type Client struct {
	kv         dynamoDbClient
	codec      codec.Codec
	ddbMetrics *dynamodbMetrics
	logger     log.Logger

	staleDataLock sync.RWMutex
	staleData     map[string]staleData
}

type staleData struct {
	data      codec.MultiKey
	timestamp time.Time
}

var (
	backoffConfig = backoff.Config{
		MinBackoff: 1 * time.Second,
		MaxBackoff: 1 * time.Minute,
		MaxRetries: 0,
	}

	defaultLoopDelay = 1 * time.Minute
)

// RegisterFlags adds the flags required to config this to the given FlagSet
// If prefix is not an empty string it should end with a period.
func (cfg *Config) RegisterFlags(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Region, prefix+"dynamodb.region", "", "Region to access dynamodb.")
	f.StringVar(&cfg.TableName, prefix+"dynamodb.table-name", "", "Table name to use on dynamodb.")
	f.DurationVar(&cfg.TTL, prefix+"dynamodb.ttl-time", 0, "Time to expire items on dynamodb.")
}

func NewClient(cfg Config, cc codec.Codec, logger log.Logger, registerer prometheus.Registerer) (*Client, error) {
	dynamoDB, err := newDynamodbKV(cfg, logger)
	if err != nil {
		return nil, err
	}

	ddbMetrics := newDynamoDbMetrics(registerer)

	c := &Client{
		kv:         dynamodbInstrumentation{kv: dynamoDB, ddbMetrics: ddbMetrics},
		codec:      cc,
		logger:     logger,
		ddbMetrics: ddbMetrics,
		staleData:  make(map[string]staleData),
	}
	return c, nil
}

func (c *Client) List(ctx context.Context, key string) ([]string, error) {
	resp, _, err := c.kv.List(ctx, dynamodbKey{primaryKey: key})
	if err != nil {
		level.Warn(c.logger).Log("msg", "error listing dynamodb", "key", key, "err", err)
		return nil, err
	}
	return resp, err
}

func (c *Client) Get(ctx context.Context, key string) (interface{}, error) {
	resp, _, err := c.kv.Query(ctx, dynamodbKey{primaryKey: key}, false)
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

func (c *Client) Delete(ctx context.Context, key string) error {
	resp, _, err := c.kv.Query(ctx, dynamodbKey{primaryKey: key}, false)
	if err != nil {
		level.Warn(c.logger).Log("msg", "error Delete dynamodb", "key", key, "err", err)
		return err
	}

	for innerKey := range resp {
		err := c.kv.Delete(ctx, dynamodbKey{
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

func (c *Client) CAS(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error {
	bo := backoff.New(ctx, backoffConfig)
	for bo.Ongoing() {
		resp, _, err := c.kv.Query(ctx, dynamodbKey{primaryKey: key}, false)
		if err != nil {
			level.Error(c.logger).Log("msg", "error cas query", "key", key, "err", err)
			bo.Wait()
			continue
		}

		current, err := c.decodeMultikey(resp)
		if err != nil {
			level.Error(c.logger).Log("msg", "error decoding key", "key", key, "err", err)
			continue
		}

		out, retry, err := f(current.Clone())
		if err != nil {
			if !retry {
				return err
			}
			bo.Wait()
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
			continue
		}

		buf, err := c.codec.EncodeMultiKey(toUpdate)
		if err != nil {
			level.Error(c.logger).Log("msg", "error serialising value", "key", key, "err", err)
			continue
		}

		for childKey, bytes := range buf {
			err := c.kv.Put(ctx, dynamodbKey{primaryKey: key, sortKey: childKey}, bytes)
			if err != nil {
				level.Error(c.logger).Log("msg", "error CASing", "key", key, "err", err)
				continue
			}
		}

		for _, childKey := range toDelete {
			err := c.kv.Delete(ctx, dynamodbKey{primaryKey: key, sortKey: childKey})
			if err != nil {
				level.Error(c.logger).Log("msg", "error CASing", "key", key, "err", err)
				continue
			}
		}

		return nil
	}

	return fmt.Errorf("failed to CAS %s", key)
}

func (c *Client) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	bo := backoff.New(ctx, backoffConfig)

	for bo.Ongoing() {
		out, _, err := c.kv.Query(ctx, dynamodbKey{
			primaryKey: key,
		}, false)
		if err != nil {
			level.Error(c.logger).Log("msg", "WatchKey", "key", key, "err", err)

			if bo.NumRetries() > 10 {
				if staleData := c.getStaleData(key); staleData != nil {
					if !f(staleData) {
						return
					}
					bo.Reset()
				}
			}
			bo.Wait()
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

		bo.Reset()
		select {
		case <-ctx.Done():
			return
		case <-time.After(defaultLoopDelay):
		}
	}
}

func (c *Client) WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool) {
	bo := backoff.New(ctx, backoffConfig)

	for bo.Ongoing() {
		out, _, err := c.kv.Query(ctx, dynamodbKey{
			primaryKey: prefix,
		}, true)
		if err != nil {
			level.Error(c.logger).Log("msg", "WatchPrefix", "prefix", prefix, "err", err)
			bo.Wait()
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

		bo.Reset()
		select {
		case <-ctx.Done():
			return
		case <-time.After(defaultLoopDelay):
		}
	}
}

func (c *Client) decodeMultikey(data map[string][]byte) (codec.MultiKey, error) {
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

func (c *Client) LastUpdateTime(key string) time.Time {
	c.staleDataLock.Lock()
	defer c.staleDataLock.Unlock()

	data, ok := c.staleData[key]
	if !ok {
		return time.Time{}
	}

	return data.timestamp
}

func (c *Client) updateStaleData(key string, data codec.MultiKey) {
	c.staleDataLock.Lock()
	defer c.staleDataLock.Unlock()

	c.staleData[key] = staleData{
		data:      data,
		timestamp: time.Now().UTC(),
	}
}

func (c *Client) getStaleData(key string) codec.MultiKey {
	c.staleDataLock.RLock()
	defer c.staleDataLock.RUnlock()

	data, ok := c.staleData[key]
	if !ok {
		return nil
	}

	newD := data.data.Clone().(codec.MultiKey)

	return newD
}
