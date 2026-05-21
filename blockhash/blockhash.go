package blockhash

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

const headerSize = 80

// FromRawBlock returns the RPC display-order block hash for raw block bytes.
func FromRawBlock(raw []byte) (string, error) {
	if len(raw) < headerSize {
		return "", errors.New("raw block is shorter than 80-byte header")
	}

	first := sha256.Sum256(raw[:headerSize])
	second := sha256.Sum256(first[:])
	hash := second[:]
	for i, j := 0, len(hash)-1; i < j; i, j = i+1, j-1 {
		hash[i], hash[j] = hash[j], hash[i]
	}
	return hex.EncodeToString(hash), nil
}
