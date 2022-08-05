package dynamoDbMultiKey

import (
	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/util/aws"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

// Client implements kv.Client for etcd.
type Client struct {
	cfg       aws.DynamoDBConfig
	codec     codec.Codec
	ddbClient *aws.DynamoDBStorageClient
	logger    log.Logger
}

// NewClient returns a new Client.
func NewClient(cfg aws.DynamoDBConfig, codec codec.Codec, logger log.Logger, registerer prometheus.Registerer) (*Client, error) {
	ddbClient, err := aws.NewDynamoDBStorageClient(cfg, registerer)
	if err != nil {
		return nil, err
	}

	c := &Client{
		ddbClient: ddbClient,
		codec:     codec,
		cfg:       cfg,
		logger:    logger,
	}
	return c, nil
}
