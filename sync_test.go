package blocksync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

	"fractal-block-sync/btcrpc"
	"fractal-block-sync/r2store"
	"fractal-block-sync/rangeindex"
)

type fakeUploadRPC struct {
	tip    uint64
	hashes map[uint64]string
	blocks map[string]string
}

func (f *fakeUploadRPC) GetBlockCount(ctx context.Context) (uint64, error) {
	return f.tip, nil
}

func (f *fakeUploadRPC) GetBlockHash(ctx context.Context, height uint64) (string, error) {
	hash, ok := f.hashes[height]
	if !ok {
		return "", fmt.Errorf("missing hash %d", height)
	}
	return hash, nil
}

func (f *fakeUploadRPC) GetBlockRawHex(ctx context.Context, hash string) (string, error) {
	rawHex, ok := f.blocks[hash]
	if !ok {
		return "", fmt.Errorf("missing block %s", hash)
	}
	return rawHex, nil
}

type fakeWriter struct {
	blocks map[string][]byte
	index  map[string][]byte
}

func (f *fakeWriter) UploadBlock(ctx context.Context, hash string, data []byte) error {
	if f.blocks == nil {
		f.blocks = map[string][]byte{}
	}
	f.blocks[hash] = append([]byte(nil), data...)
	return nil
}

func (f *fakeWriter) UploadRangeIndex(ctx context.Context, key string, data []byte) error {
	if f.index == nil {
		f.index = map[string][]byte{}
	}
	f.index[key] = append([]byte(nil), data...)
	return nil
}

func TestUploadOnceUploadsBlocksAndStableRangeIndex(t *testing.T) {
	hashes := map[uint64]string{}
	blocks := map[string]string{}
	for height := uint64(0); height < 6; height++ {
		hash := fmt.Sprintf("%064x", height+1)
		hashes[height] = hash
		blocks[hash] = hex.EncodeToString([]byte{byte(height)})
	}
	rpc := &fakeUploadRPC{tip: 5, hashes: hashes, blocks: blocks}
	writer := &fakeWriter{}

	err := UploadOnce(context.Background(), rpc, writer, UploadConfig{RangeSize: 3, StableDelay: 2})
	if err != nil {
		t.Fatalf("UploadOnce returned error: %v", err)
	}
	if len(writer.blocks) != 6 {
		t.Fatalf("uploaded blocks = %d, want 6", len(writer.blocks))
	}

	key := "index/range/v1/size-3/0000000000.bin"
	bin := writer.index[key]
	if len(bin) != 3*rangeindex.HashSize {
		t.Fatalf("range index length = %d, want %d", len(bin), 3*rangeindex.HashSize)
	}
	got, err := rangeindex.HashAt(bin, 0, 2, 3)
	if err != nil {
		t.Fatalf("HashAt returned error: %v", err)
	}
	if got != hashes[2] {
		t.Fatalf("range hash = %q, want %q", got, hashes[2])
	}
}

func TestUploadOnceDoesNotPublishPartialStartingRange(t *testing.T) {
	hashes := map[uint64]string{}
	blocks := map[string]string{}
	for height := uint64(0); height < 6; height++ {
		hash := fmt.Sprintf("%064x", height+1)
		hashes[height] = hash
		blocks[hash] = hex.EncodeToString([]byte{byte(height)})
	}
	rpc := &fakeUploadRPC{tip: 5, hashes: hashes, blocks: blocks}
	writer := &fakeWriter{}

	err := UploadOnce(context.Background(), rpc, writer, UploadConfig{FromHeight: 1, RangeSize: 3, StableDelay: 0})
	if err != nil {
		t.Fatalf("UploadOnce returned error: %v", err)
	}
	if _, ok := writer.index["index/range/v1/size-3/0000000000.bin"]; ok {
		t.Fatal("published partial starting range")
	}
	if _, ok := writer.index["index/range/v1/size-3/0000000003.bin"]; !ok {
		t.Fatal("did not publish complete range after from-height")
	}
}

type fakeSubmitRPC struct {
	info    btcrpc.BlockchainInfo
	tips    []btcrpc.ChainTip
	headers map[string]btcrpc.BlockHeader
	submits []string
}

func (f *fakeSubmitRPC) GetBlockchainInfo(ctx context.Context) (btcrpc.BlockchainInfo, error) {
	return f.info, nil
}

func (f *fakeSubmitRPC) GetChainTips(ctx context.Context) ([]btcrpc.ChainTip, error) {
	return f.tips, nil
}

func (f *fakeSubmitRPC) GetBlockHeader(ctx context.Context, hash string) (btcrpc.BlockHeader, error) {
	header, ok := f.headers[hash]
	if !ok {
		return btcrpc.BlockHeader{}, fmt.Errorf("missing header %s", hash)
	}
	return header, nil
}

func (f *fakeSubmitRPC) SubmitBlock(ctx context.Context, blockHex string) (string, error) {
	f.submits = append(f.submits, blockHex)
	return "", nil
}

type fakeDownloader struct {
	objects map[string][]byte
	blocks  map[string][]byte
}

func (f *fakeDownloader) DownloadObject(ctx context.Context, key string) ([]byte, error) {
	data, ok := f.objects[key]
	if !ok {
		return nil, r2store.ErrNotFound
	}
	return append([]byte(nil), data...), nil
}

func (f *fakeDownloader) DownloadBlock(ctx context.Context, hash string) ([]byte, error) {
	data, ok := f.blocks[hash]
	if !ok {
		return nil, errors.New("missing block")
	}
	return append([]byte(nil), data...), nil
}

func TestSubmitNextUsesRangeIndex(t *testing.T) {
	raw, hash := testRawBlock(t, 11)
	bin, err := rangeindex.Encode([]string{fmt.Sprintf("%064x", 1), hash}, 2)
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}
	rpc := &fakeSubmitRPC{info: btcrpc.BlockchainInfo{Blocks: 10, Headers: 11}}
	downloader := &fakeDownloader{
		objects: map[string][]byte{"index/range/v1/size-2/0000000010.bin": bin},
		blocks:  map[string][]byte{hash: raw},
	}

	result, err := SubmitNext(context.Background(), rpc, downloader, SubmitConfig{RangeSize: 2})
	if err != nil {
		t.Fatalf("SubmitNext returned error: %v", err)
	}
	if !result.Submitted || result.Hash != hash || len(rpc.submits) != 1 {
		t.Fatalf("result = %+v submits=%d", result, len(rpc.submits))
	}
}

func TestSubmitNextFallsBackToHeaderWalk(t *testing.T) {
	raw, hash := testRawBlock(t, 11)
	rpc := &fakeSubmitRPC{
		info: btcrpc.BlockchainInfo{Blocks: 10, Headers: 12},
		tips: []btcrpc.ChainTip{{Height: 12, Hash: "h12", Status: "headers-only"}},
		headers: map[string]btcrpc.BlockHeader{
			"h12": {Hash: "h12", Height: 12, PreviousBlockHash: hash},
			hash:  {Hash: hash, Height: 11, PreviousBlockHash: "h10"},
			"h10": {Hash: "h10", Height: 10, PreviousBlockHash: "h9"},
		},
	}
	downloader := &fakeDownloader{blocks: map[string][]byte{hash: raw}}

	result, err := SubmitNext(context.Background(), rpc, downloader, SubmitConfig{RangeSize: 2, RecentWalkLimit: 2})
	if err != nil {
		t.Fatalf("SubmitNext returned error: %v", err)
	}
	if !result.Submitted || result.Hash != hash {
		t.Fatalf("result = %+v, want submitted hash %s", result, hash)
	}
}

func TestSubmitNextWaitsForHeaders(t *testing.T) {
	rpc := &fakeSubmitRPC{info: btcrpc.BlockchainInfo{Blocks: 10, Headers: 10}}
	result, err := SubmitNext(context.Background(), rpc, &fakeDownloader{}, SubmitConfig{})
	if err != nil {
		t.Fatalf("SubmitNext returned error: %v", err)
	}
	if !result.WaitHeaders || result.TargetHeight != 11 {
		t.Fatalf("result = %+v, want wait for height 11", result)
	}
}

func TestSubmitNextRejectsHashMismatch(t *testing.T) {
	raw, hash := testRawBlock(t, 11)
	otherRaw := append([]byte(nil), raw...)
	otherRaw[0] ^= 0xff
	bin, err := rangeindex.Encode([]string{fmt.Sprintf("%064x", 1), hash}, 2)
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}
	rpc := &fakeSubmitRPC{info: btcrpc.BlockchainInfo{Blocks: 10, Headers: 11}}
	downloader := &fakeDownloader{
		objects: map[string][]byte{"index/range/v1/size-2/0000000010.bin": bin},
		blocks:  map[string][]byte{hash: otherRaw},
	}

	_, err = SubmitNext(context.Background(), rpc, downloader, SubmitConfig{RangeSize: 2})
	if err == nil {
		t.Fatal("SubmitNext returned nil error")
	}
}

func testRawBlock(t *testing.T, nonce uint32) ([]byte, string) {
	t.Helper()
	header := make([]byte, 80)
	binary.LittleEndian.PutUint32(header[0:4], 1)
	binary.LittleEndian.PutUint32(header[76:80], nonce)
	payload := []byte("payload")
	raw := append(header, payload...)
	first := sha256.Sum256(header)
	second := sha256.Sum256(first[:])
	hashBytes := append([]byte(nil), second[:]...)
	for i, j := 0, len(hashBytes)-1; i < j; i, j = i+1, j-1 {
		hashBytes[i], hashBytes[j] = hashBytes[j], hashBytes[i]
	}
	if bytes.Equal(raw[:80], make([]byte, 80)) {
		t.Fatal("header is unexpectedly empty")
	}
	return raw, hex.EncodeToString(hashBytes)
}
