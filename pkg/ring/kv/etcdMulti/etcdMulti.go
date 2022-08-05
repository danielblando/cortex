package etcdMulti

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/util/backoff"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	cortextls "github.com/cortexproject/cortex/pkg/util/tls"
)

// Config for a new etcd.Client.
type Config struct {
	Endpoints   []string               `yaml:"endpoints"`
	DialTimeout time.Duration          `yaml:"dial_timeout"`
	MaxRetries  int                    `yaml:"max_retries"`
	EnableTLS   bool                   `yaml:"tls_enabled"`
	TLS         cortextls.ClientConfig `yaml:",inline"`

	UserName string `yaml:"username"`
	Password string `yaml:"password"`
}

// Clientv3Facade is a subset of all Etcd client operations that are required
// to implement an Etcd version of kv.Client
type Clientv3Facade interface {
	clientv3.KV
	clientv3.Watcher
}

// Client implements kv.Client for etcd.
type Client struct {
	cfg    Config
	codec  codec.Codec
	cli    Clientv3Facade
	logger log.Logger
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix(f, "")
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	cfg.Endpoints = []string{}
	f.Var((*flagext.StringSlice)(&cfg.Endpoints), prefix+"etcd.endpoints", "The etcd endpoints to connect to.")
	f.DurationVar(&cfg.DialTimeout, prefix+"etcd.dial-timeout", 10*time.Second, "The dial timeout for the etcd connection.")
	f.IntVar(&cfg.MaxRetries, prefix+"etcd.max-retries", 10, "The maximum number of retries to do for failed ops.")
	f.BoolVar(&cfg.EnableTLS, prefix+"etcd.tls-enabled", false, "Enable TLS.")
	f.StringVar(&cfg.UserName, prefix+"etcd.username", "", "Etcd username.")
	f.StringVar(&cfg.Password, prefix+"etcd.password", "", "Etcd password.")
	cfg.TLS.RegisterFlagsWithPrefix(prefix+"etcd", f)
}

// GetTLS sets the TLS config field with certs
func (cfg *Config) GetTLS() (*tls.Config, error) {
	if !cfg.EnableTLS {
		return nil, nil
	}
	tlsInfo := &transport.TLSInfo{
		CertFile:           cfg.TLS.CertPath,
		KeyFile:            cfg.TLS.KeyPath,
		TrustedCAFile:      cfg.TLS.CAPath,
		ServerName:         cfg.TLS.ServerName,
		InsecureSkipVerify: cfg.TLS.InsecureSkipVerify,
	}
	return tlsInfo.ClientConfig()
}

// New makes a new Client.
func New(cfg Config, codec codec.Codec, logger log.Logger) (*Client, error) {
	tlsConfig, err := cfg.GetTLS()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to initialise TLS configuration for etcd")
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.DialTimeout,
		// Configure the keepalive to make sure that the client reconnects
		// to the etcd service endpoint(s) in case the current connection is
		// dead (ie. the node where etcd is running is dead or a network
		// partition occurs).
		//
		// The settings:
		// - DialKeepAliveTime: time before the client pings the server to
		//   see if transport is alive (10s hardcoded)
		// - DialKeepAliveTimeout: time the client waits for a response for
		//   the keep-alive probe (set to 2x dial timeout, in order to avoid
		//   exposing another config option which is likely to be a factor of
		//   the dial timeout anyway)
		// - PermitWithoutStream: whether the client should send keepalive pings
		//   to server without any active streams (enabled)
		DialKeepAliveTime:    10 * time.Second,
		DialKeepAliveTimeout: 2 * cfg.DialTimeout,
		PermitWithoutStream:  true,
		TLS:                  tlsConfig,
		Username:             cfg.UserName,
		Password:             cfg.Password,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		cfg:    cfg,
		codec:  codec,
		cli:    cli,
		logger: logger,
	}, nil
}

// CAS implements kv.Client.
func (c *Client) CAS(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error {
	var lastErr error

	for i := 0; i < c.cfg.MaxRetries; i++ {
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
			c.Delete(ctx, c.getEtcdKey(key, childKey))
		}

		buf, err := c.codec.EncodeMultiKey(toUpdate)
		if err != nil {
			level.Error(c.logger).Log("msg", "error serialising value", "key", key, "err", err)
			lastErr = err
			continue
		}

		for childKey, bytes := range buf {
			_, err := c.cli.Do(ctx, clientv3.OpPut(c.getEtcdKey(key, childKey), string(bytes)))
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
	backoff := backoff.New(ctx, backoff.Config{
		MinBackoff: 1 * time.Second,
		MaxBackoff: 1 * time.Minute,
	})

	// Ensure the context used by the Watch is always cancelled.
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

outer:
	for backoff.Ongoing() {
		for resp := range c.cli.Watch(watchCtx, key, clientv3.WithPrefix()) {
			if err := resp.Err(); err != nil {
				level.Error(c.logger).Log("msg", "watch error", "key", key, "err", err)
				continue outer
			}

			backoff.Reset()

			if len(resp.Events) > 0 {
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
	resp, err := c.cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	keys := make(map[string][]byte, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		keys[c.removeEtcdKeyPrefix(prefix, string(kv.Key))] = kv.Value
	}

	return keys, nil
}

// Delete implements kv.Client.
func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.cli.Delete(ctx, key)
	return err
}

func (c *Client) getEtcdKey(ringKey string, innerKey string) string {
	return ringKey + "/" + innerKey
}

func (c *Client) removeEtcdKeyPrefix(key string, etcdKey string) string {
	return strings.TrimPrefix(etcdKey, key+"/")
}
