package blocksync

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fractal-block-sync/blockhash"
	"fractal-block-sync/btcrpc"
	"fractal-block-sync/r2store"
	"fractal-block-sync/rangeindex"
)

const (
	DefaultRangeSize             = rangeindex.DefaultRangeSize
	DefaultStableDelay           = 2500
	DefaultRecentWalkLimit       = 2500
	DefaultUploadWorkers         = 4
	DefaultSubmitDownloadWorkers = 4
	DefaultSubmitPrefetchWindow  = 16
	uploadMissingProbeStep       = 100
)

// BlockProvider is the bitcoind RPC subset required by UploadOnce.
type BlockProvider interface {
	GetBlockCount(ctx context.Context) (uint64, error)
	GetBlockHash(ctx context.Context, height uint64) (string, error)
	GetBlockRawHex(ctx context.Context, hash string) (string, error)
}

// ObjectWriter is the R2 writer subset required by UploadOnce.
type ObjectWriter interface {
	BlockExists(ctx context.Context, hash string) (bool, error)
	ObjectExists(ctx context.Context, key string) (bool, error)
	UploadBlock(ctx context.Context, hash string, data []byte) error
	UploadRangeIndex(ctx context.Context, key string, data []byte) error
}

// UploadConfig controls provider upload behavior.
type UploadConfig struct {
	FromHeight    uint64
	ToHeight      *uint64
	RangeSize     uint64
	StableDelay   uint64
	UploadWorkers uint
	Logger        *log.Logger
}

// ErrUploadReorgTouchesPublishedIndex indicates that a local reorg would
// invalidate an already published range index.
var ErrUploadReorgTouchesPublishedIndex = errors.New("upload reorg touches published range index")

// UploadFollowResult describes one stateful follow upload attempt.
type UploadFollowResult struct {
	Tip              uint64
	LastTip          uint64
	NextIndexStart   uint64
	UploadedBlocks   uint64
	PublishedIndexes uint64
	Waiting          bool
}

// HasWork reports whether the attempt made upload progress or observed chain
// progress that should be followed immediately.
func (r UploadFollowResult) HasWork() bool {
	return r.Tip != r.LastTip || r.UploadedBlocks > 0 || r.PublishedIndexes > 0
}

// UploadOnce uploads blocks from the range containing FromHeight to the current
// local tip or ToHeight, and publishes complete stable range indexes.
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

	endHeight := tip
	if cfg.ToHeight != nil && *cfg.ToHeight < endHeight {
		endHeight = *cfg.ToHeight
	}
	if cfg.FromHeight > endHeight {
		return nil
	}

	uploadWorkers := cfg.UploadWorkers
	if uploadWorkers == 0 {
		uploadWorkers = DefaultUploadWorkers
	}

	startHeight, err := rangeindex.StartHeight(cfg.FromHeight, rangeSize)
	if err != nil {
		return err
	}

	var stableTip uint64
	hasStableTip := tip >= stableDelay
	if tip >= stableDelay {
		stableTip = tip - stableDelay
	}

	for start := startHeight; start <= endHeight; {
		rangeEnd := endOfRange(start, rangeSize)
		end := rangeEnd
		if end > endHeight {
			end = endHeight
		}

		key, err := rangeindex.ObjectKey(start, rangeSize)
		if err != nil {
			return err
		}
		exists, err := writer.ObjectExists(ctx, key)
		if err != nil {
			return fmt.Errorf("check range index %s: %w", key, err)
		}
		if exists {
			if cfg.Logger != nil {
				cfg.Logger.Printf("skipped existing range index start=%d key=%s", start, key)
			}
			if start > ^uint64(0)-rangeSize {
				break
			}
			start += rangeSize
			continue
		}

		if _, err := uploadBlocks(ctx, rpc, writer, start, end, uploadWorkers, cfg.Logger); err != nil {
			return err
		}
		if rangeEnd <= endHeight && hasStableTip && rangeEnd <= stableTip {
			if _, err := uploadRangeIndexIfMissing(ctx, rpc, writer, start, rangeSize, cfg.Logger); err != nil {
				return err
			}
		}
		if start > ^uint64(0)-rangeSize {
			break
		}
		start += rangeSize
	}
	return nil
}

// UploadFollower keeps in-process upload state for efficient follow mode.
type UploadFollower struct {
	rpc    BlockProvider
	writer ObjectWriter
	cfg    UploadConfig

	rangeSize     uint64
	stableDelay   uint64
	uploadWorkers uint
	managedStart  uint64

	initialized    bool
	lastTip        uint64
	nextIndexStart uint64
	recentHashes   map[uint64]string
}

type uploadFollowerStats struct {
	uploadedBlocks   uint64
	publishedIndexes uint64
}

// NewUploadFollower creates a stateful uploader for follow mode.
func NewUploadFollower(rpc BlockProvider, writer ObjectWriter, cfg UploadConfig) (*UploadFollower, error) {
	rangeSize := cfg.RangeSize
	if rangeSize == 0 {
		rangeSize = DefaultRangeSize
	}
	managedStart, err := rangeindex.StartHeight(cfg.FromHeight, rangeSize)
	if err != nil {
		return nil, err
	}
	uploadWorkers := cfg.UploadWorkers
	if uploadWorkers == 0 {
		uploadWorkers = DefaultUploadWorkers
	}
	return &UploadFollower{
		rpc:            rpc,
		writer:         writer,
		cfg:            cfg,
		rangeSize:      rangeSize,
		stableDelay:    cfg.StableDelay,
		uploadWorkers:  uploadWorkers,
		managedStart:   managedStart,
		nextIndexStart: managedStart,
		recentHashes:   map[uint64]string{},
	}, nil
}

// UploadOnce uploads the current increment and advances stable range indexes.
func (f *UploadFollower) UploadOnce(ctx context.Context) (UploadFollowResult, error) {
	result := UploadFollowResult{
		LastTip:        f.lastTip,
		NextIndexStart: f.nextIndexStart,
	}
	localTip, err := f.rpc.GetBlockCount(ctx)
	if err != nil {
		return result, fmt.Errorf("get block count: %w", err)
	}
	endHeight := f.endHeight(localTip)
	result.Tip = endHeight
	if f.cfg.FromHeight > endHeight {
		return f.finishResult(result), nil
	}

	var stats uploadFollowerStats
	if !f.initialized {
		stats, err = f.initialize(ctx, localTip, endHeight)
		if err != nil {
			return result, err
		}
	} else {
		if err := f.ensurePublishedBoundaryVisible(endHeight); err != nil {
			return result, err
		}
		stats, err = f.uploadIncrement(ctx, endHeight)
		if err != nil {
			return result, err
		}
		indexStats, err := f.publishStableIndexes(ctx, localTip, endHeight)
		if err != nil {
			return result, err
		}
		stats.uploadedBlocks += indexStats.uploadedBlocks
		stats.publishedIndexes += indexStats.publishedIndexes
	}
	result.UploadedBlocks = stats.uploadedBlocks
	result.PublishedIndexes = stats.publishedIndexes
	return f.finishResult(result), nil
}

func (f *UploadFollower) finishResult(result UploadFollowResult) UploadFollowResult {
	result.NextIndexStart = f.nextIndexStart
	result.Waiting = !result.HasWork()
	return result
}

func (f *UploadFollower) endHeight(localTip uint64) uint64 {
	endHeight := localTip
	if f.cfg.ToHeight != nil && *f.cfg.ToHeight < endHeight {
		endHeight = *f.cfg.ToHeight
	}
	return endHeight
}

func (f *UploadFollower) initialize(ctx context.Context, localTip uint64, endHeight uint64) (uploadFollowerStats, error) {
	var stats uploadFollowerStats
	f.nextIndexStart = f.managedStart
	f.recentHashes = map[uint64]string{}

	uploadStart, err := f.findInitialUploadStart(ctx, endHeight)
	if err != nil {
		return stats, err
	}
	if err := f.ensurePublishedBoundaryVisible(endHeight); err != nil {
		return stats, err
	}
	if uploadStart <= endHeight {
		uploaded, err := uploadBlocks(ctx, f.rpc, f.writer, uploadStart, endHeight, f.uploadWorkers, f.cfg.Logger)
		if err != nil {
			return stats, err
		}
		stats.uploadedBlocks += uploaded
	}
	indexStats, err := f.publishStableIndexes(ctx, localTip, endHeight)
	if err != nil {
		return stats, err
	}
	stats.uploadedBlocks += indexStats.uploadedBlocks
	stats.publishedIndexes += indexStats.publishedIndexes
	if err := f.refreshRecentHashes(ctx, f.recentCacheStart(endHeight), endHeight); err != nil {
		return stats, err
	}
	f.lastTip = endHeight
	f.initialized = true
	return stats, nil
}

func (f *UploadFollower) findInitialUploadStart(ctx context.Context, endHeight uint64) (uint64, error) {
	for f.nextIndexStart <= endHeight {
		key, err := rangeindex.ObjectKey(f.nextIndexStart, f.rangeSize)
		if err != nil {
			return 0, err
		}
		exists, err := f.writer.ObjectExists(ctx, key)
		if err != nil {
			return 0, fmt.Errorf("check range index %s: %w", key, err)
		}
		if !exists {
			break
		}
		if f.cfg.Logger != nil {
			f.cfg.Logger.Printf("skipped existing range index start=%d key=%s", f.nextIndexStart, key)
		}
		next, ok := nextRangeStart(f.nextIndexStart, f.rangeSize)
		if !ok {
			return f.nextIndexStart, nil
		}
		f.nextIndexStart = next
	}
	return f.nextIndexStart, nil
}

func (f *UploadFollower) ensurePublishedBoundaryVisible(endHeight uint64) error {
	boundary, ok := f.publishedBoundary()
	if !ok || endHeight >= boundary {
		return nil
	}
	return fmt.Errorf("%w: tip_height=%d published_boundary=%d next_index_start=%d", ErrUploadReorgTouchesPublishedIndex, endHeight, boundary, f.nextIndexStart)
}

func (f *UploadFollower) publishedBoundary() (uint64, bool) {
	if f.nextIndexStart <= f.managedStart {
		return 0, false
	}
	return f.nextIndexStart - 1, true
}

func nextRangeStart(start uint64, rangeSize uint64) (uint64, bool) {
	if start > ^uint64(0)-rangeSize {
		return 0, false
	}
	return start + rangeSize, true
}

func (f *UploadFollower) uploadIncrement(ctx context.Context, endHeight uint64) (uploadFollowerStats, error) {
	var stats uploadFollowerStats
	checkHeight := endHeight
	if f.lastTip < checkHeight {
		checkHeight = f.lastTip
	}
	if checkHeight < f.managedStart {
		f.lastTip = endHeight
		f.pruneRecentHashes(endHeight)
		return stats, nil
	}

	currentHash, err := f.rpc.GetBlockHash(ctx, checkHeight)
	if err != nil {
		return stats, fmt.Errorf("get block hash at height %d: %w", checkHeight, err)
	}
	knownHash, ok := f.recentHashes[checkHeight]
	if ok && knownHash == currentHash {
		if endHeight > f.lastTip {
			start := f.lastTip + 1
			if start < f.managedStart {
				start = f.managedStart
			}
			uploaded, err := uploadBlocksFrom(ctx, f.rpc, f.writer, start, endHeight, f.uploadWorkers, f.cfg.Logger)
			if err != nil {
				return stats, err
			}
			stats.uploadedBlocks += uploaded
			if err := f.refreshRecentHashes(ctx, start, endHeight); err != nil {
				return stats, err
			}
		}
		f.lastTip = endHeight
		f.pruneRecentHashes(endHeight)
		return stats, nil
	}

	if !ok && checkHeight < f.nextIndexStart && f.nextIndexStart > f.managedStart {
		return stats, fmt.Errorf("%w: height=%d next_index_start=%d", ErrUploadReorgTouchesPublishedIndex, checkHeight, f.nextIndexStart)
	}
	return f.uploadReorg(ctx, checkHeight, endHeight)
}

func (f *UploadFollower) uploadReorg(ctx context.Context, checkHeight uint64, endHeight uint64) (uploadFollowerStats, error) {
	var stats uploadFollowerStats
	if checkHeight < f.nextIndexStart && f.nextIndexStart > f.managedStart {
		return stats, fmt.Errorf("%w: height=%d next_index_start=%d", ErrUploadReorgTouchesPublishedIndex, checkHeight, f.nextIndexStart)
	}

	ancestor, found, searchedPublishedBoundary, err := f.findCommonAncestor(ctx, checkHeight)
	if err != nil {
		return stats, err
	}

	var uploadStart uint64
	if found {
		if ancestor == ^uint64(0) {
			return stats, fmt.Errorf("common ancestor overflow at height %d", ancestor)
		}
		uploadStart = ancestor + 1
	} else {
		if searchedPublishedBoundary {
			return stats, fmt.Errorf("%w: no common ancestor before next_index_start=%d", ErrUploadReorgTouchesPublishedIndex, f.nextIndexStart)
		}
		uploadStart = f.nextIndexStart
		if uploadStart < f.managedStart {
			uploadStart = f.managedStart
		}
		if f.cfg.Logger != nil {
			f.cfg.Logger.Printf("fallback upload from unpublished tail start=%d", uploadStart)
		}
	}

	if uploadStart < f.nextIndexStart && f.nextIndexStart > f.managedStart {
		return stats, fmt.Errorf("%w: upload_start=%d next_index_start=%d", ErrUploadReorgTouchesPublishedIndex, uploadStart, f.nextIndexStart)
	}
	if uploadStart <= endHeight {
		uploaded, err := uploadBlocksFrom(ctx, f.rpc, f.writer, uploadStart, endHeight, f.uploadWorkers, f.cfg.Logger)
		if err != nil {
			return stats, err
		}
		stats.uploadedBlocks += uploaded
	}
	for height := range f.recentHashes {
		if height >= uploadStart {
			delete(f.recentHashes, height)
		}
	}
	if uploadStart <= endHeight {
		if err := f.refreshRecentHashes(ctx, uploadStart, endHeight); err != nil {
			return stats, err
		}
	}
	f.lastTip = endHeight
	f.pruneRecentHashes(endHeight)
	return stats, nil
}

func (f *UploadFollower) findCommonAncestor(ctx context.Context, startHeight uint64) (uint64, bool, bool, error) {
	lowerBound := f.recentCacheStart(f.lastTip)
	if startHeight < lowerBound {
		lowerBound = startHeight
	}
	for height := startHeight; ; height-- {
		knownHash, ok := f.recentHashes[height]
		if ok {
			currentHash, err := f.rpc.GetBlockHash(ctx, height)
			if err != nil {
				return 0, false, false, fmt.Errorf("get block hash at height %d: %w", height, err)
			}
			if currentHash == knownHash {
				return height, true, false, nil
			}
		}
		if height == lowerBound || height == 0 {
			break
		}
	}
	searchedPublishedBoundary := f.nextIndexStart > f.managedStart && lowerBound < f.nextIndexStart
	return 0, false, searchedPublishedBoundary, nil
}

func (f *UploadFollower) publishStableIndexes(ctx context.Context, localTip uint64, endHeight uint64) (uploadFollowerStats, error) {
	var stats uploadFollowerStats
	if localTip < f.stableDelay {
		return stats, nil
	}
	stableTip := localTip - f.stableDelay
	for f.nextIndexStart <= endHeight {
		rangeEnd := endOfRange(f.nextIndexStart, f.rangeSize)
		if rangeEnd > endHeight || rangeEnd > stableTip {
			break
		}
		uploaded, err := uploadBlocksFrom(ctx, f.rpc, f.writer, f.nextIndexStart, rangeEnd, f.uploadWorkers, f.cfg.Logger)
		if err != nil {
			return stats, err
		}
		stats.uploadedBlocks += uploaded
		published, err := uploadRangeIndexIfMissing(ctx, f.rpc, f.writer, f.nextIndexStart, f.rangeSize, f.cfg.Logger)
		if err != nil {
			return stats, err
		}
		if published {
			stats.publishedIndexes++
		}
		boundaryHash, err := f.rpc.GetBlockHash(ctx, rangeEnd)
		if err != nil {
			return stats, fmt.Errorf("get block hash at height %d: %w", rangeEnd, err)
		}
		f.recentHashes[rangeEnd] = boundaryHash
		next, ok := nextRangeStart(f.nextIndexStart, f.rangeSize)
		if !ok {
			break
		}
		f.nextIndexStart = next
		f.pruneRecentHashes(endHeight)
	}
	return stats, nil
}

func (f *UploadFollower) refreshRecentHashes(ctx context.Context, fromHeight uint64, toHeight uint64) error {
	if fromHeight > toHeight {
		return nil
	}
	for height := fromHeight; ; height++ {
		hash, err := f.rpc.GetBlockHash(ctx, height)
		if err != nil {
			return fmt.Errorf("get block hash at height %d: %w", height, err)
		}
		f.recentHashes[height] = hash
		if height == toHeight {
			break
		}
	}
	f.pruneRecentHashes(toHeight)
	return nil
}

func (f *UploadFollower) recentCacheStart(endHeight uint64) uint64 {
	start := f.managedStart
	if f.nextIndexStart > f.managedStart {
		start = f.nextIndexStart - 1
	}
	if endHeight < start {
		return endHeight
	}
	return start
}

func (f *UploadFollower) pruneRecentHashes(endHeight uint64) {
	start := f.recentCacheStart(endHeight)
	for height := range f.recentHashes {
		if height < start || height > endHeight {
			delete(f.recentHashes, height)
		}
	}
}

func endOfRange(start uint64, rangeSize uint64) uint64 {
	if start > ^uint64(0)-rangeSize+1 {
		return ^uint64(0)
	}
	return start + rangeSize - 1
}

func uploadRangeIndexIfMissing(ctx context.Context, rpc BlockProvider, writer ObjectWriter, start uint64, rangeSize uint64, logger *log.Logger) (bool, error) {
	key, err := rangeindex.ObjectKey(start, rangeSize)
	if err != nil {
		return false, err
	}
	exists, err := writer.ObjectExists(ctx, key)
	if err != nil {
		return false, fmt.Errorf("check range index %s: %w", key, err)
	}
	if exists {
		if logger != nil {
			logger.Printf("skipped existing range index start=%d key=%s", start, key)
		}
		return false, nil
	}
	return true, uploadRangeIndex(ctx, rpc, writer, start, rangeSize, logger)
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

func uploadBlocks(ctx context.Context, rpc BlockProvider, writer ObjectWriter, fromHeight uint64, tip uint64, workers uint, logger *log.Logger) (uint64, error) {
	if fromHeight > tip {
		return 0, nil
	}
	uploadStart, err := findUploadStart(ctx, rpc, writer, fromHeight, tip, uploadMissingProbeStep, workers, logger)
	if err != nil {
		return 0, err
	}
	return uploadBlocksFrom(ctx, rpc, writer, uploadStart, tip, workers, logger)
}

func findUploadStart(ctx context.Context, rpc BlockProvider, writer ObjectWriter, fromHeight uint64, tip uint64, probeStep uint64, workers uint, logger *log.Logger) (uint64, error) {
	if probeStep == 0 {
		return fromHeight, nil
	}
	probes := uploadProbeHeights(fromHeight, tip, probeStep)
	results, err := checkUploadProbes(ctx, rpc, writer, probes, workers, logger)
	if err != nil {
		return 0, err
	}
	for _, result := range results {
		if !result.exists {
			start := previousUploadWindowStart(fromHeight, result.height, probeStep)
			if logger != nil {
				logger.Printf("found missing block probe height=%d upload_start=%d", result.height, start)
			}
			return start, nil
		}
	}
	start := finalUploadWindowStart(fromHeight, tip, probeStep)
	if logger != nil {
		logger.Printf("no missing block probe found from=%d to=%d upload_start=%d", fromHeight, tip, start)
	}
	return start, nil
}

func uploadProbeHeights(fromHeight uint64, tip uint64, probeStep uint64) []uint64 {
	var probes []uint64
	for height := fromHeight; ; {
		probes = append(probes, height)
		if tip-height < probeStep {
			break
		}
		height += probeStep
	}
	return probes
}

type uploadProbeResult struct {
	index   int
	height  uint64
	hash    string
	exists  bool
	elapsed time.Duration
	err     error
}

func checkUploadProbes(ctx context.Context, rpc BlockProvider, writer ObjectWriter, probes []uint64, workers uint, logger *log.Logger) ([]uploadProbeResult, error) {
	if len(probes) == 0 {
		return nil, nil
	}
	workerCount := int(workers)
	if workerCount < 1 {
		workerCount = 1
	}
	if workerCount > len(probes) {
		workerCount = len(probes)
	}

	jobs := make(chan int, len(probes))
	results := make(chan uploadProbeResult, len(probes))
	for index := range probes {
		jobs <- index
	}
	close(jobs)

	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				height := probes[index]
				start := time.Now()
				hash, exists, err := getUploadBlockState(ctx, rpc, writer, height)
				result := uploadProbeResult{
					index:   index,
					height:  height,
					hash:    hash,
					exists:  exists,
					elapsed: time.Since(start),
					err:     err,
				}
				if logger != nil {
					if err != nil {
						logger.Printf("check block probe failed height=%d elapsed=%s err=%v", height, result.elapsed, err)
					} else {
						logger.Printf("checked block probe height=%d hash=%s exists=%t elapsed=%s", height, hash, exists, result.elapsed)
					}
				}
				results <- result
			}
		}()
	}
	wg.Wait()
	close(results)

	ordered := make([]uploadProbeResult, len(probes))
	var firstErr error
	for result := range results {
		ordered[result.index] = result
		if result.err != nil && firstErr == nil {
			firstErr = result.err
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return ordered, nil
}

func previousUploadWindowStart(fromHeight uint64, missingHeight uint64, probeStep uint64) uint64 {
	if missingHeight-fromHeight <= probeStep {
		return fromHeight
	}
	return missingHeight - probeStep
}

func finalUploadWindowStart(fromHeight uint64, tip uint64, window uint64) uint64 {
	if tip-fromHeight < window {
		return fromHeight
	}
	return tip - window + 1
}

func uploadBlocksFrom(ctx context.Context, rpc BlockProvider, writer ObjectWriter, fromHeight uint64, tip uint64, workers uint, logger *log.Logger) (uint64, error) {
	if workers <= 1 {
		var uploaded uint64
		for height := fromHeight; ; height++ {
			didUpload, err := uploadBlock(ctx, rpc, writer, height, logger)
			if err != nil {
				return uploaded, err
			}
			if didUpload {
				uploaded++
			}
			if height == tip {
				break
			}
		}
		return uploaded, nil
	}

	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan uint64)
	errs := make(chan error, 1)
	var uploaded atomic.Uint64
	var wg sync.WaitGroup
	for worker := uint(0); worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for height := range jobs {
				didUpload, err := uploadBlock(uploadCtx, rpc, writer, height, logger)
				if err != nil {
					select {
					case errs <- err:
						cancel()
					default:
					}
					return
				}
				if didUpload {
					uploaded.Add(1)
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
				return uploaded.Load(), err
			default:
				return uploaded.Load(), uploadCtx.Err()
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
		return uploaded.Load(), err
	default:
		return uploaded.Load(), nil
	}
}

func uploadBlock(ctx context.Context, rpc BlockProvider, writer ObjectWriter, height uint64, logger *log.Logger) (bool, error) {
	hash, exists, err := getUploadBlockState(ctx, rpc, writer, height)
	if err != nil {
		return false, err
	}
	if exists {
		if logger != nil {
			logger.Printf("skipped existing block height=%d hash=%s", height, hash)
		}
		return false, nil
	}
	rawHex, err := rpc.GetBlockRawHex(ctx, hash)
	if err != nil {
		return false, fmt.Errorf("get raw block %s at height %d: %w", hash, height, err)
	}
	raw, err := hex.DecodeString(strings.TrimSpace(rawHex))
	if err != nil {
		return false, fmt.Errorf("decode raw block %s at height %d: %w", hash, height, err)
	}
	if err := writer.UploadBlock(ctx, hash, raw); err != nil {
		return false, fmt.Errorf("upload block %s at height %d: %w", hash, height, err)
	}
	if logger != nil {
		logger.Printf("uploaded block height=%d hash=%s", height, hash)
	}
	return true, nil
}

func getUploadBlockState(ctx context.Context, rpc BlockProvider, writer ObjectWriter, height uint64) (string, bool, error) {
	hash, err := rpc.GetBlockHash(ctx, height)
	if err != nil {
		return "", false, fmt.Errorf("get block hash at height %d: %w", height, err)
	}
	exists, err := writer.BlockExists(ctx, hash)
	if err != nil {
		return "", false, fmt.Errorf("check block %s at height %d: %w", hash, height, err)
	}
	return hash, exists, nil
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
	DownloadWorkers uint
	PrefetchWindow  uint
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

// SubmitPipelineResult describes one pipelined submit run.
type SubmitPipelineResult struct {
	SubmittedBlocks uint64
	LastHeight      uint64
	TargetHeight    uint64
	WaitHeaders     bool
	WaitR2Index     bool
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

// SubmitPipeline prefetches and verifies future blocks in parallel while
// submitting blocks to the node in height order.
func SubmitPipeline(ctx context.Context, rpc BlockSubmitter, downloader ObjectDownloader, cfg SubmitConfig) (SubmitPipelineResult, error) {
	rangeSize := cfg.RangeSize
	if rangeSize == 0 {
		rangeSize = DefaultRangeSize
	}
	recentWalkLimit := cfg.RecentWalkLimit
	downloadWorkers := cfg.DownloadWorkers
	if downloadWorkers == 0 {
		downloadWorkers = DefaultSubmitDownloadWorkers
	}
	prefetchWindow := cfg.PrefetchWindow
	if prefetchWindow == 0 {
		prefetchWindow = DefaultSubmitPrefetchWindow
	}

	info, err := rpc.GetBlockchainInfo(ctx)
	if err != nil {
		return SubmitPipelineResult{}, fmt.Errorf("get blockchain info: %w", err)
	}
	target := info.Blocks + 1
	pipeline := submitPipeline{
		ctx:             ctx,
		rpc:             rpc,
		downloader:      downloader,
		cfg:             cfg,
		rangeSize:       rangeSize,
		recentWalkLimit: recentWalkLimit,
		headers:         info.Headers,
		currentHeight:   target,
		nextSchedule:    target,
		prefetchWindow:  prefetchWindow,
		downloadWorkers: downloadWorkers,
		indexCache:      newSubmitRangeIndexCache(downloader, rangeSize),
	}
	return pipeline.run()
}

type submitPipeline struct {
	ctx             context.Context
	rpc             BlockSubmitter
	downloader      ObjectDownloader
	cfg             SubmitConfig
	rangeSize       uint64
	recentWalkLimit uint64
	headers         uint64
	currentHeight   uint64
	nextSchedule    uint64
	prefetchWindow  uint
	downloadWorkers uint
	indexCache      *submitRangeIndexCache

	jobs    chan submitDownloadJob
	results chan submitDownloadResult
	ready   map[uint64]submitDownloadResult
	pending map[uint64]struct{}

	stoppedScheduling bool
	waitHeaders       bool
	waitR2Index       bool
}

type submitDownloadJob struct {
	height uint64
	hash   string
}

type submitDownloadResult struct {
	height uint64
	hash   string
	raw    []byte
	err    error
}

func (p *submitPipeline) run() (SubmitPipelineResult, error) {
	downloadCtx, cancel := context.WithCancel(p.ctx)
	defer cancel()

	p.jobs = make(chan submitDownloadJob, int(p.prefetchWindow))
	p.results = make(chan submitDownloadResult, int(p.prefetchWindow))
	p.ready = map[uint64]submitDownloadResult{}
	p.pending = map[uint64]struct{}{}

	var workerWG sync.WaitGroup
	for worker := uint(0); worker < p.downloadWorkers; worker++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			runSubmitDownloadWorker(downloadCtx, p.downloader, p.jobs, p.results)
		}()
	}
	defer func() {
		cancel()
		close(p.jobs)
		workerWG.Wait()
	}()

	result := SubmitPipelineResult{
		TargetHeight: p.currentHeight,
		LastHeight:   p.currentHeight - 1,
	}
	if err := p.fillWindow(); err != nil {
		return result, err
	}

	for {
		if ready, ok := p.ready[p.currentHeight]; ok {
			delete(p.ready, p.currentHeight)
			if ready.err != nil {
				return result, ready.err
			}
			rpcResult, err := p.rpc.SubmitBlock(p.ctx, hex.EncodeToString(ready.raw))
			if err != nil {
				if !submittedHeightAccepted(p.ctx, p.rpc, ready.height) {
					return result, fmt.Errorf("submit block %s at height %d: %w", ready.hash, ready.height, err)
				}
				if p.cfg.Logger != nil {
					p.cfg.Logger.Printf("submit block returned error but height is accepted height=%d hash=%s err=%v", ready.height, ready.hash, err)
				}
			}
			if err == nil {
				submitted := rpcResult == "" || isAlreadyKnownSubmitResult(rpcResult)
				if !submitted {
					return result, fmt.Errorf("submit block %s at height %d returned %q", ready.hash, ready.height, rpcResult)
				}
			}
			result.SubmittedBlocks++
			result.LastHeight = ready.height
			result.TargetHeight = ready.height + 1
			if p.cfg.Logger != nil {
				p.cfg.Logger.Printf("submitted block height=%d hash=%s", ready.height, ready.hash)
			}
			if p.currentHeight == ^uint64(0) {
				return result, nil
			}
			p.currentHeight++
			if err := p.fillWindow(); err != nil {
				return result, err
			}
			continue
		}

		if _, ok := p.pending[p.currentHeight]; !ok && p.stoppedScheduling {
			result.TargetHeight = p.currentHeight
			result.WaitHeaders = p.waitHeaders
			result.WaitR2Index = p.waitR2Index
			if result.WaitHeaders {
				return result, nil
			}
			return result, nil
		}

		select {
		case <-p.ctx.Done():
			return result, p.ctx.Err()
		case downloaded := <-p.results:
			delete(p.pending, downloaded.height)
			p.ready[downloaded.height] = downloaded
		}
	}
}

func (p *submitPipeline) fillWindow() error {
	for !p.stoppedScheduling && uint(len(p.ready)+len(p.pending)) < p.prefetchWindow {
		hash, waitHeaders, waitR2Index, err := p.resolveHash(p.nextSchedule)
		if err != nil {
			return err
		}
		if waitHeaders {
			p.stoppedScheduling = true
			p.waitHeaders = true
			p.waitR2Index = waitR2Index
			return nil
		}

		job := submitDownloadJob{height: p.nextSchedule, hash: hash}
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		case p.jobs <- job:
			p.pending[p.nextSchedule] = struct{}{}
		}
		if p.nextSchedule == ^uint64(0) {
			p.stoppedScheduling = true
			return nil
		}
		p.nextSchedule++
	}
	return nil
}

func (p *submitPipeline) resolveHash(height uint64) (string, bool, bool, error) {
	if height > p.headers {
		if !p.cfg.BootstrapFromR2 {
			return "", true, false, nil
		}
		hash, err := p.indexCache.hashAt(p.ctx, height)
		if err != nil {
			if errors.Is(err, r2store.ErrNotFound) {
				return "", true, true, nil
			}
			return "", false, false, err
		}
		return hash, false, false, nil
	}

	hash, err := p.indexCache.hashAt(p.ctx, height)
	if err == nil {
		return hash, false, false, nil
	}
	if !errors.Is(err, r2store.ErrNotFound) {
		return "", false, false, err
	}
	hash, err = resolveRecentHash(p.ctx, p.rpc, height, p.recentWalkLimit)
	if err != nil {
		return "", false, false, err
	}
	return hash, false, false, nil
}

func runSubmitDownloadWorker(ctx context.Context, downloader ObjectDownloader, jobs <-chan submitDownloadJob, results chan<- submitDownloadResult) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}
			result := downloadAndVerifySubmitBlock(ctx, downloader, job)
			select {
			case <-ctx.Done():
				return
			case results <- result:
			}
		}
	}
}

func downloadAndVerifySubmitBlock(ctx context.Context, downloader ObjectDownloader, job submitDownloadJob) submitDownloadResult {
	raw, err := downloader.DownloadBlock(ctx, job.hash)
	if err != nil {
		return submitDownloadResult{
			height: job.height,
			hash:   job.hash,
			err:    fmt.Errorf("download block %s at height %d: %w", job.hash, job.height, err),
		}
	}
	gotHash, err := blockhash.FromRawBlock(raw)
	if err != nil {
		return submitDownloadResult{
			height: job.height,
			hash:   job.hash,
			err:    fmt.Errorf("calculate block hash at height %d: %w", job.height, err),
		}
	}
	if gotHash != job.hash {
		return submitDownloadResult{
			height: job.height,
			hash:   job.hash,
			err:    fmt.Errorf("downloaded block hash mismatch at height %d: got %s, want %s", job.height, gotHash, job.hash),
		}
	}
	return submitDownloadResult{
		height: job.height,
		hash:   job.hash,
		raw:    raw,
	}
}

type submitRangeIndexCache struct {
	downloader ObjectDownloader
	rangeSize  uint64
	key        string
	start      uint64
	bin        []byte
	missing    map[string]struct{}
}

func newSubmitRangeIndexCache(downloader ObjectDownloader, rangeSize uint64) *submitRangeIndexCache {
	return &submitRangeIndexCache{
		downloader: downloader,
		rangeSize:  rangeSize,
		missing:    map[string]struct{}{},
	}
}

func (c *submitRangeIndexCache) hashAt(ctx context.Context, height uint64) (string, error) {
	key, startHeight, err := rangeindex.ObjectKeyForHeight(height, c.rangeSize)
	if err != nil {
		return "", err
	}
	if _, ok := c.missing[key]; ok {
		return "", r2store.ErrNotFound
	}
	if c.key != key {
		bin, err := c.downloader.DownloadObject(ctx, key)
		if errors.Is(err, r2store.ErrNotFound) {
			c.missing[key] = struct{}{}
			return "", err
		}
		if err != nil {
			return "", fmt.Errorf("download range index %s: %w", key, err)
		}
		c.key = key
		c.start = startHeight
		c.bin = bin
	}
	hash, err := rangeindex.HashAt(c.bin, c.start, height, c.rangeSize)
	if err != nil {
		return "", fmt.Errorf("read range index %s: %w", c.key, err)
	}
	return hash, nil
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

func submittedHeightAccepted(ctx context.Context, rpc BlockSubmitter, height uint64) bool {
	info, err := rpc.GetBlockchainInfo(ctx)
	if err != nil {
		return false
	}
	return info.Blocks >= height
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
