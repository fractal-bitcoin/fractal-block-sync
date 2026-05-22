package rangeindex

import (
	"encoding/hex"
	"strings"
	"testing"
)

func TestObjectKeyForHeight(t *testing.T) {
	key, startHeight, err := ObjectKeyForHeight(5761, 2500)
	if err != nil {
		t.Fatalf("ObjectKeyForHeight returned error: %v", err)
	}
	if startHeight != 5000 {
		t.Fatalf("startHeight = %d, want 5000", startHeight)
	}
	wantKey := "index/range/v1/size-2500/0000005000.bin"
	if key != wantKey {
		t.Fatalf("key = %q, want %q", key, wantKey)
	}
}

func TestEncodeAndHashAt(t *testing.T) {
	hashes := []string{
		strings.Repeat("00", HashSize),
		strings.Repeat("11", HashSize),
		strings.Repeat("aa", HashSize),
	}
	bin, err := Encode(hashes, 3)
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}

	wantLen := 3 * HashSize
	if len(bin) != wantLen {
		t.Fatalf("len(bin) = %d, want %d", len(bin), wantLen)
	}

	got, err := HashAt(bin, 30, 32, 3)
	if err != nil {
		t.Fatalf("HashAt returned error: %v", err)
	}
	if got != hashes[2] {
		t.Fatalf("HashAt = %q, want %q", got, hashes[2])
	}
}

func TestHashAtRejectsInvalidBinSize(t *testing.T) {
	_, err := HashAt([]byte{1, 2, 3}, 0, 0, 1)
	if err == nil {
		t.Fatal("HashAt returned nil error")
	}
}

func TestEncodeRejectsWrongHashCount(t *testing.T) {
	_, err := Encode([]string{strings.Repeat("00", HashSize)}, 2)
	if err == nil {
		t.Fatal("Encode returned nil error")
	}
}

func TestEncodeKeepsRPCDisplayOrder(t *testing.T) {
	hash := "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	bin, err := Encode([]string{hash}, 1)
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}
	if hex.EncodeToString(bin) != hash {
		t.Fatalf("bin = %x, want %s", bin, hash)
	}
}
