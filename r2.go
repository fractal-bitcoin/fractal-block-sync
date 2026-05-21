package blocksync

import (
	"context"

	"fractal-block-sync/r2store"
)

const (
	envEndpointURL     = r2store.EnvEndpointURL
	envAccessKeyID     = r2store.EnvAccessKeyID
	envSecretAccessKey = r2store.EnvSecretAccessKey
	envBucketName      = r2store.EnvBucketName
)

// Config contains the Cloudflare R2 settings used by the client.
type Config = r2store.Config

// LoadConfigFromEnv loads R2 configuration from environment variables.
func LoadConfigFromEnv() (Config, error) {
	return r2store.LoadConfigFromEnv()
}

// Client uploads Bitcoin block objects in Cloudflare R2.
type Client struct {
	writer *r2store.Writer
}

// New creates a Cloudflare R2 client using S3-compatible APIs.
func New(ctx context.Context, cfg Config) (*Client, error) {
	writer, err := r2store.NewWriter(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Client{writer: writer}, nil
}

// BlockKey returns the object key for one Bitcoin block.
func BlockKey(hash string) (string, error) {
	return r2store.BlockKey(hash)
}

// UploadBlock uploads a single Bitcoin block as one R2 object.
func (c *Client) UploadBlock(ctx context.Context, hash string, data []byte) error {
	return c.writer.UploadBlock(ctx, hash, data)
}
