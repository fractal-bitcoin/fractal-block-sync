package blocksync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"fractal-block-sync/btcrpc"
	"fractal-block-sync/r2store"
	"fractal-block-sync/rangeindex"
)

type fakeUploadRPC struct {
	tip    uint64
	hashes map[uint64]string
	blocks map[string]string

	mu           sync.Mutex
	active       int
	maxActive    int
	rawBlockGate chan struct{}
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
	f.mu.Lock()
	f.active++
	if f.active > f.maxActive {
		f.maxActive = f.active
	}
	f.mu.Unlock()

	if f.rawBlockGate != nil {
		select {
		case <-ctx.Done():
			f.mu.Lock()
			f.active--
			f.mu.Unlock()
			return "", ctx.Err()
		case <-f.rawBlockGate:
		}
	}

	defer func() {
		f.mu.Lock()
		f.active--
		f.mu.Unlock()
	}()

	rawHex, ok := f.blocks[hash]
	if !ok {
		return "", fmt.Errorf("missing block %s", hash)
	}
	return rawHex, nil
}

type fakeWriter struct {
	mu     sync.Mutex
	blocks map[string][]byte
	index  map[string][]byte
	events []string
}

func (f *fakeWriter) UploadBlock(ctx context.Context, hash string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.blocks == nil {
		f.blocks = map[string][]byte{}
	}
	f.blocks[hash] = append([]byte(nil), data...)
	f.events = append(f.events, "block:"+hash)
	return nil
}

func (f *fakeWriter) BlockExists(ctx context.Context, hash string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.blocks[hash]
	f.events = append(f.events, "head-block:"+hash)
	return ok, nil
}

func (f *fakeWriter) ObjectExists(ctx context.Context, key string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.index[key]
	f.events = append(f.events, "head-object:"+key)
	return ok, nil
}

func (f *fakeWriter) UploadRangeIndex(ctx context.Context, key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.index == nil {
		f.index = map[string][]byte{}
	}
	f.index[key] = append([]byte(nil), data...)
	f.events = append(f.events, "index:"+key)
	return nil
}

func makeUploadFixture(t *testing.T, count uint64) (map[uint64]string, map[string]string) {
	t.Helper()
	hashes := map[uint64]string{}
	blocks := map[string]string{}
	for height := uint64(0); height < count; height++ {
		hash := fmt.Sprintf("%064x", height+1)
		hashes[height] = hash
		blocks[hash] = hex.EncodeToString([]byte{byte(height)})
	}
	return hashes, blocks
}

func TestUploadOnceUploadsBlocksAndStableRangeIndex(t *testing.T) {
	hashes, blocks := makeUploadFixture(t, 6)
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

func TestUploadOnceUploadsBlocksInParallel(t *testing.T) {
	hashes, blocks := makeUploadFixture(t, 4)
	gate := make(chan struct{})
	rpc := &fakeUploadRPC{tip: 3, hashes: hashes, blocks: blocks, rawBlockGate: gate}
	writer := &fakeWriter{}

	done := make(chan error, 1)
	go func() {
		done <- UploadOnce(context.Background(), rpc, writer, UploadConfig{RangeSize: 4, StableDelay: 4, UploadWorkers: 4})
	}()

	deadline := time.After(2 * time.Second)
	for active := 0; active < 2; {
		rpc.mu.Lock()
		active = rpc.active
		rpc.mu.Unlock()
		select {
		case <-deadline:
			close(gate)
			t.Fatalf("timed out waiting for concurrent uploads, active=%d", active)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	close(gate)

	if err := <-done; err != nil {
		t.Fatalf("UploadOnce returned error: %v", err)
	}
	if len(writer.blocks) != 4 {
		t.Fatalf("uploaded blocks = %d, want 4", len(writer.blocks))
	}
	if rpc.maxActive < 2 {
		t.Fatalf("max concurrent raw block requests = %d, want at least 2", rpc.maxActive)
	}
}

func TestUploadOnceStartsFromRangeContainingFromHeight(t *testing.T) {
	hashes, blocks := makeUploadFixture(t, 6)
	rpc := &fakeUploadRPC{tip: 5, hashes: hashes, blocks: blocks}
	writer := &fakeWriter{}

	err := UploadOnce(context.Background(), rpc, writer, UploadConfig{FromHeight: 1, RangeSize: 3, StableDelay: 0})
	if err != nil {
		t.Fatalf("UploadOnce returned error: %v", err)
	}
	if _, ok := writer.blocks[hashes[0]]; !ok {
		t.Fatal("did not upload range start block")
	}
	if _, ok := writer.index["index/range/v1/size-3/0000000000.bin"]; !ok {
		t.Fatal("did not publish starting range")
	}
	if _, ok := writer.index["index/range/v1/size-3/0000000003.bin"]; !ok {
		t.Fatal("did not publish complete range after from-height")
	}
}

func TestUploadOnceDoesNotPublishUnstablePartialStartingRange(t *testing.T) {
	hashes, blocks := makeUploadFixture(t, 6)
	rpc := &fakeUploadRPC{tip: 5, hashes: hashes, blocks: blocks}
	writer := &fakeWriter{}

	err := UploadOnce(context.Background(), rpc, writer, UploadConfig{FromHeight: 1, RangeSize: 3, StableDelay: 4})
	if err != nil {
		t.Fatalf("UploadOnce returned error: %v", err)
	}
	if _, ok := writer.index["index/range/v1/size-3/0000000000.bin"]; ok {
		t.Fatal("published unstable partial starting range")
	}
	if _, ok := writer.blocks[hashes[0]]; !ok {
		t.Fatal("did not upload starting range block")
	}
}

func TestUploadOncePublishesStableRangeIndexBeforeLaterBlocks(t *testing.T) {
	hashes, blocks := makeUploadFixture(t, 6)
	rpc := &fakeUploadRPC{tip: 5, hashes: hashes, blocks: blocks}
	writer := &fakeWriter{}

	err := UploadOnce(context.Background(), rpc, writer, UploadConfig{RangeSize: 3, StableDelay: 0, UploadWorkers: 1})
	if err != nil {
		t.Fatalf("UploadOnce returned error: %v", err)
	}

	firstIndex := indexOfEvent(writer.events, "index:index/range/v1/size-3/0000000000.bin")
	blockAfterFirstRange := indexOfEvent(writer.events, "block:"+hashes[3])
	if firstIndex == -1 {
		t.Fatalf("events = %v, missing first range index", writer.events)
	}
	if blockAfterFirstRange == -1 {
		t.Fatalf("events = %v, missing block after first range", writer.events)
	}
	if firstIndex > blockAfterFirstRange {
		t.Fatalf("events = %v, first range index was not published before later blocks", writer.events)
	}
}

func TestUploadOnceSkipsRangeWhenIndexExists(t *testing.T) {
	hashes, blocks := makeUploadFixture(t, 6)
	rpc := &fakeUploadRPC{tip: 5, hashes: hashes, blocks: blocks}
	writer := &fakeWriter{
		index: map[string][]byte{"index/range/v1/size-3/0000000000.bin": []byte("exists")},
	}

	err := UploadOnce(context.Background(), rpc, writer, UploadConfig{RangeSize: 3, StableDelay: 0, UploadWorkers: 1})
	if err != nil {
		t.Fatalf("UploadOnce returned error: %v", err)
	}
	for height := uint64(0); height < 3; height++ {
		if _, ok := writer.blocks[hashes[height]]; ok {
			t.Fatalf("uploaded block height %d in completed range", height)
		}
	}
	for height := uint64(3); height < 6; height++ {
		if _, ok := writer.blocks[hashes[height]]; !ok {
			t.Fatalf("did not upload block height %d after missing range index", height)
		}
	}
}

func TestUploadOnceSkipsExistingBlocksInMissingRange(t *testing.T) {
	hashes, blocks := makeUploadFixture(t, 3)
	rpc := &fakeUploadRPC{tip: 2, hashes: hashes, blocks: blocks}
	writer := &fakeWriter{
		blocks: map[string][]byte{hashes[1]: []byte{1}},
	}

	err := UploadOnce(context.Background(), rpc, writer, UploadConfig{RangeSize: 3, StableDelay: 0, UploadWorkers: 1})
	if err != nil {
		t.Fatalf("UploadOnce returned error: %v", err)
	}
	if count := countEvents(writer.events, "block:"+hashes[1]); count != 0 {
		t.Fatalf("uploaded existing block %d times", count)
	}
	if _, ok := writer.blocks[hashes[0]]; !ok {
		t.Fatal("did not upload missing first block")
	}
	if _, ok := writer.blocks[hashes[2]]; !ok {
		t.Fatal("did not upload missing last block")
	}
}

func TestUploadOnceHonorsToHeight(t *testing.T) {
	hashes, blocks := makeUploadFixture(t, 6)
	rpc := &fakeUploadRPC{tip: 5, hashes: hashes, blocks: blocks}
	writer := &fakeWriter{}
	toHeight := uint64(2)

	err := UploadOnce(context.Background(), rpc, writer, UploadConfig{RangeSize: 3, StableDelay: 0, ToHeight: &toHeight})
	if err != nil {
		t.Fatalf("UploadOnce returned error: %v", err)
	}
	for height := uint64(0); height <= toHeight; height++ {
		if _, ok := writer.blocks[hashes[height]]; !ok {
			t.Fatalf("did not upload block height %d", height)
		}
	}
	if _, ok := writer.blocks[hashes[3]]; ok {
		t.Fatal("uploaded block after to-height")
	}
	if _, ok := writer.index["index/range/v1/size-3/0000000000.bin"]; !ok {
		t.Fatal("did not publish complete range ending at to-height")
	}
}

func indexOfEvent(events []string, want string) int {
	for i, event := range events {
		if event == want {
			return i
		}
	}
	return -1
}

func countEvents(events []string, want string) int {
	count := 0
	for _, event := range events {
		if event == want {
			count++
		}
	}
	return count
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

func TestSubmitNextBootstrapsFromR2WithoutHeaders(t *testing.T) {
	raw, hash := testRawBlock(t, 1)
	bin, err := rangeindex.Encode([]string{fmt.Sprintf("%064x", 1), hash}, 2)
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}
	rpc := &fakeSubmitRPC{info: btcrpc.BlockchainInfo{Blocks: 0, Headers: 0}}
	downloader := &fakeDownloader{
		objects: map[string][]byte{"index/range/v1/size-2/0000000000.bin": bin},
		blocks:  map[string][]byte{hash: raw},
	}

	result, err := SubmitNext(context.Background(), rpc, downloader, SubmitConfig{RangeSize: 2, BootstrapFromR2: true})
	if err != nil {
		t.Fatalf("SubmitNext returned error: %v", err)
	}
	if !result.Submitted || result.WaitHeaders || result.Hash != hash || len(rpc.submits) != 1 {
		t.Fatalf("result = %+v submits=%d, want R2 bootstrap submit hash %s", result, len(rpc.submits), hash)
	}
}

func TestSubmitNextBootstrapWaitsForHeadersOrR2Index(t *testing.T) {
	rpc := &fakeSubmitRPC{info: btcrpc.BlockchainInfo{Blocks: 0, Headers: 0}}
	result, err := SubmitNext(context.Background(), rpc, &fakeDownloader{}, SubmitConfig{RangeSize: 2, BootstrapFromR2: true})
	if err != nil {
		t.Fatalf("SubmitNext returned error: %v", err)
	}
	if !result.WaitHeaders || !result.WaitR2Index || result.TargetHeight != 1 {
		t.Fatalf("result = %+v, want wait for headers or R2 index at height 1", result)
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
