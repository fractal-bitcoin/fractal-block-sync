package r2store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestBlockKey(t *testing.T) {
	key, err := BlockKey("0000000000000000000320283a032748cef8227873ff4872689bf23f1cda83a5")
	if err != nil {
		t.Fatalf("BlockKey returned error: %v", err)
	}

	want := "blocks/0000000000000000000320283a032748cef8227873ff4872689bf23f1cda83a5.blk"
	if key != want {
		t.Fatalf("BlockKey() = %q, want %q", key, want)
	}
}

func TestBlockKeyRejectsInvalidHash(t *testing.T) {
	tests := []string{"", "  ", "bad/hash", "abc"}
	for _, tt := range tests {
		t.Run(tt, func(t *testing.T) {
			if _, err := BlockKey(tt); err == nil {
				t.Fatalf("BlockKey(%q) returned nil error", tt)
			}
		})
	}
}

func TestConfigValidate(t *testing.T) {
	cfg := Config{
		EndpointURL:     "https://example.r2.cloudflarestorage.com",
		AccessKeyID:     "access",
		SecretAccessKey: "secret",
		BucketName:      "blocks",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
}

func TestConfigValidateReportsMissingValues(t *testing.T) {
	err := (Config{}).Validate()
	if err == nil {
		t.Fatal("Validate returned nil error")
	}

	msg := err.Error()
	for _, want := range []string{EnvEndpointURL, EnvAccessKeyID, EnvSecretAccessKey, EnvBucketName} {
		if !strings.Contains(msg, want) {
			t.Fatalf("Validate error %q does not mention %s", msg, want)
		}
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	t.Setenv(EnvEndpointURL, "https://example.r2.cloudflarestorage.com")
	t.Setenv(EnvAccessKeyID, "access")
	t.Setenv(EnvSecretAccessKey, "secret")
	t.Setenv(EnvBucketName, "blocks")

	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("LoadConfigFromEnv returned error: %v", err)
	}

	if cfg.EndpointURL != os.Getenv(EnvEndpointURL) ||
		cfg.AccessKeyID != os.Getenv(EnvAccessKeyID) ||
		cfg.SecretAccessKey != os.Getenv(EnvSecretAccessKey) ||
		cfg.BucketName != os.Getenv(EnvBucketName) {
		t.Fatalf("LoadConfigFromEnv returned unexpected config: %+v", cfg)
	}
}

func TestPublicClientDownloadObject(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/prefix/blocks/hash.blk" {
			t.Fatalf("path = %q, want /prefix/blocks/hash.blk", r.URL.Path)
		}
		_, _ = w.Write([]byte("block"))
	}))
	defer server.Close()

	client, err := NewPublicClient(server.URL+"/prefix", nil)
	if err != nil {
		t.Fatalf("NewPublicClient returned error: %v", err)
	}
	got, err := client.DownloadObject(context.Background(), "blocks/hash.blk")
	if err != nil {
		t.Fatalf("DownloadObject returned error: %v", err)
	}
	if string(got) != "block" {
		t.Fatalf("DownloadObject = %q, want block", got)
	}
}

func TestPublicClientDownloadObjectNotFound(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	client, err := NewPublicClient(server.URL, nil)
	if err != nil {
		t.Fatalf("NewPublicClient returned error: %v", err)
	}
	_, err = client.DownloadObject(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestR2UploadIntegration(t *testing.T) {
	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Skipf("skipping R2 integration test: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	writer, err := NewWriter(ctx, cfg)
	if err != nil {
		t.Fatalf("NewWriter returned error: %v", err)
	}

	hash := "integration-test-" + strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-"))
	data := []byte("bitcoin block test payload " + time.Now().UTC().Format(time.RFC3339Nano))
	sum := sha256.Sum256([]byte(hash))
	hash = hex.EncodeToString(sum[:])

	if err := writer.UploadBlock(ctx, hash, data); err != nil {
		t.Fatalf("UploadBlock returned error: %v", err)
	}
}
