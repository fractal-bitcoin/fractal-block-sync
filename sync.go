package blocksync

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"fractal-block-sync/blockhash"
	"fractal-block-sync/btcrpc"
	"fractal-block-sync/r2store"
	"fractal-block-sync/rangeindex"
)

const (
	DefaultRangeSize       = rangeindex.DefaultRangeSize
	DefaultStableDelay     = 2880
	DefaultRecentWalkLimit = 2880
	DefaultUploadWorkers   = 4
)

// BlockProvider is the bitcoind RPC subset required by UploadOnce.
type BlockProvider interface {
	GetBlockCount(ctx context.Context) (uint64, error)
	GetBlockHash(ctx context.Context, height uint64) (string, error)
	GetBlockRawHex(ctx context.Context, hash string) (string, error)
}

// ObjectWriter is the R2 writer subset required by UploadOnce.
type ObjectWriter interface {
	UploadBlock(ctx context.Context, hash string, data []byte) error
	UploadRangeIndex(ctx context.Context, key string, data []byte) error
}

// UploadConfig controls provider upload behavior.
type UploadConfig struct {
	FromHeight    uint64
	RangeSize     uint64
	StableDelay   uint64
	UploadWorkers uint
	Logger        *log.Logger
}

// UploadOnce uploads blocks from FromHeight to the current local tip and
// publishes complete stable range indexes.
func UploadOnce(ctx context.Context, rpc BlockProvider, writer ObjectWriter, cfg UploadConfig) error {
	rangeSize := cfg.RangeSize
	if rangeSize == 0 {
		rangeSize = DefaultRangeSize
	}
	stableDelay := cfg.StableDelay

	tip, err := rpc.GetBlockCount(ctx)
	if err != nil {
		return fmt.Errorf("get block count: %w", err)
	}
	if cfg.FromHeight > tip {
		return nil
	}

	uploadWorkers := cfg.UploadWorkers
	if uploadWorkers == 0 {
		uploadWorkers = DefaultUploadWorkers
	}

	nextHeight := cfg.FromHeight
	if tip >= stableDelay {
		stableTip := tip - stableDelay
		fromStart, err := rangeindex.StartHeight(cfg.FromHeight, rangeSize)
		if err != nil {
			return err
		}
		if fromStart < cfg.FromHeight {
			fromStart += rangeSize
		}

		if nextHeight < fromStart {
			end := fromStart - 1
			if end > tip {
				end = tip
			}
			if err := uploadBlocks(ctx, rpc, writer, nextHeight, end, uploadWorkers, cfg.Logger); err != nil {
				return err
			}
			if end == tip {
				return nil
			}
			nextHeight = end + 1
		}

		for start := fromStart; start <= stableTip && stableTip-start+1 >= rangeSize; start += rangeSize {
			end := start + rangeSize - 1
			if err := uploadBlocks(ctx, rpc, writer, start, end, uploadWorkers, cfg.Logger); err != nil {
				return err
			}
			if err := uploadRangeIndex(ctx, rpc, writer, start, rangeSize, cfg.Logger); err != nil {
				return err
			}
			nextHeight = end + 1
			if start > ^uint64(0)-rangeSize {
				break
			}
		}
	}

	if nextHeight <= tip {
		return uploadBlocks(ctx, rpc, writer, nextHeight, tip, uploadWorkers, cfg.Logger)
	}
	return nil
}

func uploadRangeIndex(ctx context.Context, rpc BlockProvider, writer ObjectWriter, start uint64, rangeSize uint64, logger *log.Logger) error {
	hashes := make([]string, 0, rangeSize)
	for height := start; height < start+rangeSize; height++ {
		hash, err := rpc.GetBlockHash(ctx, height)
		if err != nil {
			return fmt.Errorf("get range hash at height %d: %w", height, err)
		}
		hashes = append(hashes, hash)
	}
	bin, err := rangeindex.Encode(hashes, rangeSize)
	if err != nil {
		return fmt.Errorf("encode range index start %d: %w", start, err)
	}
	key, err := rangeindex.ObjectKey(start, rangeSize)
	if err != nil {
		return err
	}
	if err := writer.UploadRangeIndex(ctx, key, bin); err != nil {
		return fmt.Errorf("upload range index %s: %w", key, err)
	}
	if logger != nil {
		logger.Printf("uploaded range index start=%d key=%s", start, key)
	}
	return nil
}

func uploadBlocks(ctx context.Context, rpc BlockProvider, writer ObjectWriter, fromHeight uint64, tip uint64, workers uint, logger *log.Logger) error {
	if fromHeight > tip {
		return nil
	}
	if workers <= 1 {
		for height := fromHeight; ; height++ {
			if err := uploadBlock(ctx, rpc, writer, height, logger); err != nil {
				return err
			}
			if height == tip {
				break
			}
		}
		return nil
	}

	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan uint64)
	errs := make(chan error, 1)
	var wg sync.WaitGroup
	for worker := uint(0); worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for height := range jobs {
				if err := uploadBlock(uploadCtx, rpc, writer, height, logger); err != nil {
					select {
					case errs <- err:
						cancel()
					default:
					}
					return
				}
			}
		}()
	}

	for height := fromHeight; ; height++ {
		select {
		case <-uploadCtx.Done():
			close(jobs)
			wg.Wait()
			select {
			case err := <-errs:
				return err
			default:
				return uploadCtx.Err()
			}
		case jobs <- height:
		}
		if height == tip {
			break
		}
	}
	close(jobs)
	wg.Wait()

	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

func uploadBlock(ctx context.Context, rpc BlockProvider, writer ObjectWriter, height uint64, logger *log.Logger) error {
	hash, err := rpc.GetBlockHash(ctx, height)
	if err != nil {
		return fmt.Errorf("get block hash at height %d: %w", height, err)
	}
	rawHex, err := rpc.GetBlockRawHex(ctx, hash)
	if err != nil {
		return fmt.Errorf("get raw block %s at height %d: %w", hash, height, err)
	}
	raw, err := hex.DecodeString(strings.TrimSpace(rawHex))
	if err != nil {
		return fmt.Errorf("decode raw block %s at height %d: %w", hash, height, err)
	}
	if err := writer.UploadBlock(ctx, hash, raw); err != nil {
		return fmt.Errorf("upload block %s at height %d: %w", hash, height, err)
	}
	if logger != nil {
		logger.Printf("uploaded block height=%d hash=%s", height, hash)
	}
	return nil
}

// BlockSubmitter is the bitcoind RPC subset required by SubmitNext.
type BlockSubmitter interface {
	GetBlockchainInfo(ctx context.Context) (btcrpc.BlockchainInfo, error)
	GetChainTips(ctx context.Context) ([]btcrpc.ChainTip, error)
	GetBlockHeader(ctx context.Context, hash string) (btcrpc.BlockHeader, error)
	SubmitBlock(ctx context.Context, blockHex string) (string, error)
}

// ObjectDownloader is the public R2 HTTP reader subset required by SubmitNext.
type ObjectDownloader interface {
	DownloadObject(ctx context.Context, key string) ([]byte, error)
	DownloadBlock(ctx context.Context, hash string) ([]byte, error)
}

// SubmitConfig controls user-side block submission.
type SubmitConfig struct {
	RangeSize       uint64
	RecentWalkLimit uint64
	BootstrapFromR2 bool
	Logger          *log.Logger
}

// SubmitResult describes one submit attempt.
type SubmitResult struct {
	TargetHeight uint64
	Hash         string
	Submitted    bool
	WaitHeaders  bool
	WaitR2Index  bool
	RPCResult    string
}

// SubmitNext downloads, verifies, and submits the next missing block.
func SubmitNext(ctx context.Context, rpc BlockSubmitter, downloader ObjectDownloader, cfg SubmitConfig) (SubmitResult, error) {
	rangeSize := cfg.RangeSize
	if rangeSize == 0 {
		rangeSize = DefaultRangeSize
	}
	recentWalkLimit := cfg.RecentWalkLimit

	info, err := rpc.GetBlockchainInfo(ctx)
	if err != nil {
		return SubmitResult{}, fmt.Errorf("get blockchain info: %w", err)
	}
	target := info.Blocks + 1
	result := SubmitResult{TargetHeight: target}
	if target > info.Headers {
		if !cfg.BootstrapFromR2 {
			result.WaitHeaders = true
			return result, nil
		}
		hash, err := resolveTargetHashFromRangeIndex(ctx, downloader, target, rangeSize)
		if err != nil {
			if errors.Is(err, r2store.ErrNotFound) {
				result.WaitHeaders = true
				result.WaitR2Index = true
				return result, nil
			}
			return result, err
		}
		result.Hash = hash
	} else {
		hash, err := resolveTargetHash(ctx, rpc, downloader, target, rangeSize, recentWalkLimit)
		if err != nil {
			return result, err
		}
		result.Hash = hash
	}

	raw, err := downloader.DownloadBlock(ctx, result.Hash)
	if err != nil {
		return result, fmt.Errorf("download block %s: %w", result.Hash, err)
	}
	gotHash, err := blockhash.FromRawBlock(raw)
	if err != nil {
		return result, fmt.Errorf("calculate block hash: %w", err)
	}
	if gotHash != result.Hash {
		return result, fmt.Errorf("downloaded block hash mismatch: got %s, want %s", gotHash, result.Hash)
	}

	rpcResult, err := rpc.SubmitBlock(ctx, hex.EncodeToString(raw))
	if err != nil {
		return result, fmt.Errorf("submit block %s: %w", result.Hash, err)
	}
	result.RPCResult = rpcResult
	result.Submitted = rpcResult == "" || isAlreadyKnownSubmitResult(rpcResult)
	if !result.Submitted {
		return result, fmt.Errorf("submit block %s returned %q", result.Hash, rpcResult)
	}
	if cfg.Logger != nil {
		cfg.Logger.Printf("submitted block height=%d hash=%s", target, result.Hash)
	}
	return result, nil
}

func resolveTargetHash(ctx context.Context, rpc BlockSubmitter, downloader ObjectDownloader, target uint64, rangeSize uint64, recentWalkLimit uint64) (string, error) {
	hash, err := resolveTargetHashFromRangeIndex(ctx, downloader, target, rangeSize)
	if err == nil {
		return hash, nil
	}
	if !errors.Is(err, r2store.ErrNotFound) {
		return "", err
	}

	return resolveRecentHash(ctx, rpc, target, recentWalkLimit)
}

func resolveTargetHashFromRangeIndex(ctx context.Context, downloader ObjectDownloader, target uint64, rangeSize uint64) (string, error) {
	key, startHeight, err := rangeindex.ObjectKeyForHeight(target, rangeSize)
	if err != nil {
		return "", err
	}
	bin, err := downloader.DownloadObject(ctx, key)
	if errors.Is(err, r2store.ErrNotFound) {
		return "", err
	}
	if err != nil {
		return "", fmt.Errorf("download range index %s: %w", key, err)
	}
	hash, err := rangeindex.HashAt(bin, startHeight, target, rangeSize)
	if err != nil {
		return "", fmt.Errorf("read range index %s: %w", key, err)
	}
	return hash, nil
}

func resolveRecentHash(ctx context.Context, rpc BlockSubmitter, target uint64, recentWalkLimit uint64) (string, error) {
	tips, err := rpc.GetChainTips(ctx)
	if err != nil {
		return "", fmt.Errorf("get chain tips: %w", err)
	}
	tip, ok := bestChainTip(tips, target)
	if !ok {
		return "", fmt.Errorf("no local chain tip reaches target height %d", target)
	}

	current := tip.Hash
	for walked := uint64(0); walked <= recentWalkLimit; walked++ {
		header, err := rpc.GetBlockHeader(ctx, current)
		if err != nil {
			return "", fmt.Errorf("get block header %s: %w", current, err)
		}
		if header.Height == target {
			if header.Hash != "" {
				return header.Hash, nil
			}
			return current, nil
		}
		if header.Height < target {
			return "", fmt.Errorf("header walk passed target height %d at height %d", target, header.Height)
		}
		if header.PreviousBlockHash == "" {
			return "", fmt.Errorf("header at height %d has no previous block hash", header.Height)
		}
		current = header.PreviousBlockHash
	}
	return "", fmt.Errorf("no range index and header walk exceeds limit %d", recentWalkLimit)
}

func bestChainTip(tips []btcrpc.ChainTip, target uint64) (btcrpc.ChainTip, bool) {
	var best btcrpc.ChainTip
	found := false
	for _, tip := range tips {
		if tip.Height < target || strings.TrimSpace(tip.Hash) == "" {
			continue
		}
		if !found || tipStatusRank(tip.Status) > tipStatusRank(best.Status) || tip.Height > best.Height && tipStatusRank(tip.Status) == tipStatusRank(best.Status) {
			best = tip
			found = true
		}
	}
	return best, found
}

func tipStatusRank(status string) int {
	switch status {
	case "active":
		return 3
	case "headers-only":
		return 2
	case "valid-fork", "valid-headers":
		return 1
	default:
		return 0
	}
}

func isAlreadyKnownSubmitResult(result string) bool {
	result = strings.ToLower(strings.TrimSpace(result))
	return strings.Contains(result, "duplicate") ||
		strings.Contains(result, "already") ||
		strings.Contains(result, "known")
}

// SleepOrDone sleeps unless ctx is canceled. It is used by follow loops.
func SleepOrDone(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
