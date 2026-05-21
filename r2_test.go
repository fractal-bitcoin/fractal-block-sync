package blocksync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	for _, want := range []string{envEndpointURL, envAccessKeyID, envSecretAccessKey, envBucketName} {
		if !strings.Contains(msg, want) {
			t.Fatalf("Validate error %q does not mention %s", msg, want)
		}
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	t.Setenv(envEndpointURL, "https://example.r2.cloudflarestorage.com")
	t.Setenv(envAccessKeyID, "access")
	t.Setenv(envSecretAccessKey, "secret")
	t.Setenv(envBucketName, "blocks")

	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("LoadConfigFromEnv returned error: %v", err)
	}

	if cfg.EndpointURL != os.Getenv(envEndpointURL) ||
		cfg.AccessKeyID != os.Getenv(envAccessKeyID) ||
		cfg.SecretAccessKey != os.Getenv(envSecretAccessKey) ||
		cfg.BucketName != os.Getenv(envBucketName) {
		t.Fatalf("LoadConfigFromEnv returned unexpected config: %+v", cfg)
	}
}

func TestR2UploadIntegration(t *testing.T) {
	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Skipf("skipping R2 integration test: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := New(ctx, cfg)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	hash := "integration-test-" + strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-"))
	data := []byte("bitcoin block test payload " + time.Now().UTC().Format(time.RFC3339Nano))
	sum := sha256.Sum256([]byte(hash))
	hash = hex.EncodeToString(sum[:])

	if err := client.UploadBlock(ctx, hash, data); err != nil {
		t.Fatalf("UploadBlock returned error: %v", err)
	}
}
