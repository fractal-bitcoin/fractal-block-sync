package r2store

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

const (
	EnvEndpointURL     = "ENDPOINT_URL"
	EnvAccessKeyID     = "ACCESS_KEY_ID"
	EnvSecretAccessKey = "SECRET_ACCESS_KEY"
	EnvBucketName      = "BUCKET_NAME"
)

// Config contains the Cloudflare R2 settings used by the S3-compatible writer.
type Config struct {
	EndpointURL     string
	AccessKeyID     string
	SecretAccessKey string
	BucketName      string
}

// LoadConfigFromEnv loads R2 writer configuration from environment variables.
func LoadConfigFromEnv() (Config, error) {
	cfg := Config{
		EndpointURL:     strings.TrimSpace(os.Getenv(EnvEndpointURL)),
		AccessKeyID:     strings.TrimSpace(os.Getenv(EnvAccessKeyID)),
		SecretAccessKey: strings.TrimSpace(os.Getenv(EnvSecretAccessKey)),
		BucketName:      strings.TrimSpace(os.Getenv(EnvBucketName)),
	}
	return cfg, cfg.Validate()
}

// Validate checks whether all required configuration values are present.
func (c Config) Validate() error {
	var missing []string
	if strings.TrimSpace(c.EndpointURL) == "" {
		missing = append(missing, EnvEndpointURL)
	}
	if strings.TrimSpace(c.AccessKeyID) == "" {
		missing = append(missing, EnvAccessKeyID)
	}
	if strings.TrimSpace(c.SecretAccessKey) == "" {
		missing = append(missing, EnvSecretAccessKey)
	}
	if strings.TrimSpace(c.BucketName) == "" {
		missing = append(missing, EnvBucketName)
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}

	parsed, err := url.Parse(c.EndpointURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("invalid %s: %q", EnvEndpointURL, c.EndpointURL)
	}

	return nil
}

// Writer uploads Bitcoin block and index objects to Cloudflare R2.
type Writer struct {
	bucket string
	s3     *s3.Client
}

// NewWriter creates a Cloudflare R2 writer using S3-compatible APIs.
func NewWriter(ctx context.Context, cfg Config) (*Writer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	awsCfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")),
		config.WithRegion("auto"),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.EndpointURL)
		o.UsePathStyle = true
	})

	return &Writer{
		bucket: cfg.BucketName,
		s3:     client,
	}, nil
}

// BlockKey returns the object key for one Bitcoin block.
func BlockKey(hash string) (string, error) {
	hash = strings.TrimSpace(hash)
	if hash == "" {
		return "", errors.New("block hash is required")
	}
	decoded, err := hex.DecodeString(hash)
	if err != nil || len(decoded) != 32 {
		return "", fmt.Errorf("block hash must be 32-byte hex: %q", hash)
	}
	return fmt.Sprintf("blocks/%s.blk", hash), nil
}

// UploadBlock uploads a single Bitcoin block as one R2 object.
func (w *Writer) UploadBlock(ctx context.Context, hash string, data []byte) error {
	key, err := BlockKey(hash)
	if err != nil {
		return err
	}
	return w.PutObject(ctx, key, data, "application/octet-stream")
}

// BlockExists reports whether a single Bitcoin block object exists.
func (w *Writer) BlockExists(ctx context.Context, hash string) (bool, error) {
	key, err := BlockKey(hash)
	if err != nil {
		return false, err
	}
	return w.ObjectExists(ctx, key)
}

// UploadRangeIndex uploads a range index object.
func (w *Writer) UploadRangeIndex(ctx context.Context, key string, data []byte) error {
	if strings.TrimSpace(key) == "" {
		return errors.New("range index key is required")
	}
	return w.PutObject(ctx, key, data, "application/octet-stream")
}

// ObjectExists reports whether one object key exists.
func (w *Writer) ObjectExists(ctx context.Context, key string) (bool, error) {
	if strings.TrimSpace(key) == "" {
		return false, errors.New("object key is required")
	}

	_, err := w.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return true, nil
	}
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return false, nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchKey") {
		return false, nil
	}
	return false, fmt.Errorf("head object %s: %w", key, err)
}

// PutObject uploads bytes to one object key.
func (w *Writer) PutObject(ctx context.Context, key string, data []byte, contentType string) error {
	if strings.TrimSpace(key) == "" {
		return errors.New("object key is required")
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	_, err := w.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(w.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   aws.String(contentType),
		IfNoneMatch:   aws.String("*"),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "PreconditionFailed" || apiErr.ErrorCode() == "ConditionalRequestConflict") {
			return nil
		}
		return fmt.Errorf("upload object %s: %w", key, err)
	}

	return nil
}

// PublicClient downloads objects from a public base URL.
type PublicClient struct {
	baseURL    *url.URL
	httpClient *http.Client
}

// ErrNotFound indicates that a public object returned 404.
var ErrNotFound = errors.New("object not found")

// NewPublicClient creates an HTTP downloader for public R2 objects.
func NewPublicClient(baseURL string, httpClient *http.Client) (*PublicClient, error) {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return nil, errors.New("base url is required")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("invalid base url: %q", baseURL)
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &PublicClient{
		baseURL:    parsed,
		httpClient: httpClient,
	}, nil
}

// DownloadObject downloads one object by key.
func (c *PublicClient) DownloadObject(ctx context.Context, key string) ([]byte, error) {
	u, err := c.objectURL(key)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("create download request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download object %s: %w", key, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("download object %s status %d: %s", key, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read object %s: %w", key, err)
	}
	return data, nil
}

// DownloadBlock downloads one block object by hash.
func (c *PublicClient) DownloadBlock(ctx context.Context, hash string) ([]byte, error) {
	key, err := BlockKey(hash)
	if err != nil {
		return nil, err
	}
	return c.DownloadObject(ctx, key)
}

func (c *PublicClient) objectURL(key string) (string, error) {
	key = strings.TrimLeft(strings.TrimSpace(key), "/")
	if key == "" {
		return "", errors.New("object key is required")
	}

	joined := *c.baseURL
	basePath := strings.TrimRight(joined.EscapedPath(), "/")
	parts := strings.Split(key, "/")
	escapedParts := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			return "", fmt.Errorf("invalid object key: %q", key)
		}
		escapedParts = append(escapedParts, url.PathEscape(part))
	}
	joined.RawPath = ""
	joined.Path = strings.TrimRight(c.baseURL.Path, "/") + "/" + strings.Join(parts, "/")
	if basePath != "" {
		joined.RawPath = basePath + "/" + strings.Join(escapedParts, "/")
	}
	return joined.String(), nil
}
