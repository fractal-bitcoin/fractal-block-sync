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

		if err := uploadBlocks(ctx, rpc, writer, start, end, uploadWorkers, cfg.Logger); err != nil {
			return err
		}
		if rangeEnd <= endHeight && hasStableTip && rangeEnd <= stableTip {
			if err := uploadRangeIndexIfMissing(ctx, rpc, writer, start, rangeSize, cfg.Logger); err != nil {
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

func endOfRange(start uint64, rangeSize uint64) uint64 {
	if start > ^uint64(0)-rangeSize+1 {
		return ^uint64(0)
	}
	return start + rangeSize - 1
}

func uploadRangeIndexIfMissing(ctx context.Context, rpc BlockProvider, writer ObjectWriter, start uint64, rangeSize uint64, logger *log.Logger) error {
	key, err := rangeindex.ObjectKey(start, rangeSize)
	if err != nil {
		return err
	}
	exists, err := writer.ObjectExists(ctx, key)
	if err != nil {
		return fmt.Errorf("check range index %s: %w", key, err)
	}
	if exists {
		if logger != nil {
			logger.Printf("skipped existing range index start=%d key=%s", start, key)
		}
		return nil
	}
	return uploadRangeIndex(ctx, rpc, writer, start, rangeSize, logger)
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
	uploadStart, err := findUploadStart(ctx, rpc, writer, fromHeight, tip, uploadMissingProbeStep, workers, logger)
	if err != nil {
		return err
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

func uploadBlocksFrom(ctx context.Context, rpc BlockProvider, writer ObjectWriter, fromHeight uint64, tip uint64, workers uint, logger *log.Logger) error {
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
	hash, exists, err := getUploadBlockState(ctx, rpc, writer, height)
	if err != nil {
		return err
	}
	if exists {
		if logger != nil {
			logger.Printf("skipped existing block height=%d hash=%s", height, hash)
		}
		return nil
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
