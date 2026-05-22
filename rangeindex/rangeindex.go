package rangeindex

import (
	"encoding/hex"
	"errors"
	"fmt"
)

const (
	HashSize         = 32
	DefaultRangeSize = 2500
)

// StartHeight returns the first height covered by the range containing height.
func StartHeight(height uint64, rangeSize uint64) (uint64, error) {
	if rangeSize == 0 {
		return 0, errors.New("range size must be greater than zero")
	}
	return height / rangeSize * rangeSize, nil
}

// ObjectKey returns the R2 object key for a range index.
func ObjectKey(startHeight uint64, rangeSize uint64) (string, error) {
	if rangeSize == 0 {
		return "", errors.New("range size must be greater than zero")
	}
	return fmt.Sprintf("index/range/v1/size-%d/%010d.bin", rangeSize, startHeight), nil
}

// ObjectKeyForHeight returns the R2 object key for the range index containing height.
func ObjectKeyForHeight(height uint64, rangeSize uint64) (string, uint64, error) {
	startHeight, err := StartHeight(height, rangeSize)
	if err != nil {
		return "", 0, err
	}
	key, err := ObjectKey(startHeight, rangeSize)
	if err != nil {
		return "", 0, err
	}
	return key, startHeight, nil
}

// Encode converts exactly rangeSize RPC-display-order block hashes into a range bin.
func Encode(hashes []string, rangeSize uint64) ([]byte, error) {
	if rangeSize == 0 {
		return nil, errors.New("range size must be greater than zero")
	}
	if uint64(len(hashes)) != rangeSize {
		return nil, fmt.Errorf("expected %d hashes, got %d", rangeSize, len(hashes))
	}

	out := make([]byte, 0, int(rangeSize)*HashSize)
	for i, hash := range hashes {
		raw, err := DecodeHashHex(hash)
		if err != nil {
			return nil, fmt.Errorf("hash at index %d: %w", i, err)
		}
		out = append(out, raw...)
	}
	return out, nil
}

// HashAt reads one RPC-display-order block hash from a range bin.
func HashAt(bin []byte, startHeight uint64, height uint64, rangeSize uint64) (string, error) {
	if rangeSize == 0 {
		return "", errors.New("range size must be greater than zero")
	}
	if height < startHeight || height-startHeight >= rangeSize {
		return "", fmt.Errorf("height %d is outside range starting at %d with size %d", height, startHeight, rangeSize)
	}
	wantLen := int(rangeSize) * HashSize
	if len(bin) != wantLen {
		return "", fmt.Errorf("range bin has %d bytes, want %d", len(bin), wantLen)
	}

	offset := int(height-startHeight) * HashSize
	return hex.EncodeToString(bin[offset : offset+HashSize]), nil
}

// DecodeHashHex decodes an RPC display-order block hash.
func DecodeHashHex(hash string) ([]byte, error) {
	raw, err := hex.DecodeString(hash)
	if err != nil {
		return nil, err
	}
	if len(raw) != HashSize {
		return nil, fmt.Errorf("hash must be %d bytes, got %d", HashSize, len(raw))
	}
	return raw, nil
}
