package blocksync

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"fractal-block-sync/r2store"
	"fractal-block-sync/rangeindex"
)

const (
	DefaultBenchDownloadWorkers        = 32
	DefaultBenchDownloadReportInterval = 5 * time.Second
)

// BenchDownloadConfig controls R2 download benchmark behavior.
type BenchDownloadConfig struct {
	FromHeight     uint64
	ToHeight       *uint64
	RangeSize      uint64
	Workers        uint
	ReportInterval time.Duration
	Logger         *log.Logger
}

// BenchDownloadResult describes one download benchmark run.
type BenchDownloadResult struct {
	FromHeight       uint64
	LastHeight       uint64
	DownloadedBlocks uint64
	FailedBlocks     uint64
	DownloadedBytes  uint64
	Duration         time.Duration
}

type benchDownloadJob struct {
	height uint64
	hash   string
}

type benchDownloadStats struct {
	mu               sync.Mutex
	fromHeight       uint64
	lastHeight       uint64
	downloadedBlocks uint64
	failedBlocks     uint64
	downloadedBytes  uint64
	start            time.Time
}

// BenchDownload downloads blocks from R2 without submitting them to a node.
func BenchDownload(ctx context.Context, downloader ObjectDownloader, cfg BenchDownloadConfig) (BenchDownloadResult, error) {
	rangeSize := cfg.RangeSize
	if rangeSize == 0 {
		rangeSize = DefaultRangeSize
	}
	workers := cfg.Workers
	if workers == 0 {
		workers = DefaultBenchDownloadWorkers
	}
	reportInterval := cfg.ReportInterval
	if reportInterval == 0 {
		reportInterval = DefaultBenchDownloadReportInterval
	}

	benchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stats := &benchDownloadStats{
		fromHeight: cfg.FromHeight,
		lastHeight: cfg.FromHeight,
		start:      time.Now(),
	}
	jobs := make(chan benchDownloadJob, int(workers)*4)

	var workerWG sync.WaitGroup
	for worker := uint(0); worker < workers; worker++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			for job := range jobs {
				raw, err := downloader.DownloadBlock(benchCtx, job.hash)
				if err != nil {
					if benchCtx.Err() != nil {
						return
					}
					stats.recordFailure(job.height)
					continue
				}
				stats.recordSuccess(job.height, uint64(len(raw)))
			}
		}()
	}

	reportDone := make(chan struct{})
	var reportWG sync.WaitGroup
	if cfg.Logger != nil {
		reportWG.Add(1)
		go func() {
			defer reportWG.Done()
			reportBenchDownloadStats(benchCtx, cfg.Logger, stats, reportInterval, reportDone)
		}()
	}

	err := enqueueBenchDownloadJobs(benchCtx, downloader, cfg.FromHeight, cfg.ToHeight, rangeSize, jobs, cfg.Logger)
	close(jobs)
	workerWG.Wait()
	close(reportDone)
	reportWG.Wait()

	result := stats.result()
	if err != nil {
		return result, err
	}
	if result.FailedBlocks > 0 {
		return result, fmt.Errorf("bench download completed with %d failed blocks", result.FailedBlocks)
	}
	return result, nil
}

func enqueueBenchDownloadJobs(ctx context.Context, downloader ObjectDownloader, fromHeight uint64, toHeight *uint64, rangeSize uint64, jobs chan<- benchDownloadJob, logger *log.Logger) error {
	if toHeight != nil && fromHeight > *toHeight {
		return nil
	}

	height := fromHeight
	for {
		key, startHeight, err := rangeindex.ObjectKeyForHeight(height, rangeSize)
		if err != nil {
			return err
		}
		bin, err := downloader.DownloadObject(ctx, key)
		if errors.Is(err, r2store.ErrNotFound) {
			if toHeight == nil {
				if logger != nil {
					logger.Printf("stopped at missing range index height=%d key=%s", height, key)
				}
				return nil
			}
			return fmt.Errorf("download range index %s: %w", key, err)
		}
		if err != nil {
			return fmt.Errorf("download range index %s: %w", key, err)
		}

		rangeEnd := endOfRange(startHeight, rangeSize)
		end := rangeEnd
		if toHeight != nil && *toHeight < end {
			end = *toHeight
		}

		for ; height <= end; height++ {
			hash, err := rangeindex.HashAt(bin, startHeight, height, rangeSize)
			if err != nil {
				return fmt.Errorf("read range index %s at height %d: %w", key, height, err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case jobs <- benchDownloadJob{height: height, hash: hash}:
			}
			if height == ^uint64(0) {
				return nil
			}
		}
		if toHeight != nil && height > *toHeight {
			return nil
		}
		if rangeEnd == ^uint64(0) {
			return nil
		}
	}
}

func reportBenchDownloadStats(ctx context.Context, logger *log.Logger, stats *benchDownloadStats, interval time.Duration, done <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	prev := stats.snapshot()
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			current := stats.snapshot()
			logBenchDownloadSnapshot(logger, prev, current)
			prev = current
		}
	}
}

type benchDownloadSnapshot struct {
	lastHeight       uint64
	downloadedBlocks uint64
	failedBlocks     uint64
	downloadedBytes  uint64
	elapsed          time.Duration
	at               time.Time
}

func (s *benchDownloadStats) recordSuccess(height uint64, bytes uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if height > s.lastHeight {
		s.lastHeight = height
	}
	s.downloadedBlocks++
	s.downloadedBytes += bytes
}

func (s *benchDownloadStats) recordFailure(height uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if height > s.lastHeight {
		s.lastHeight = height
	}
	s.failedBlocks++
}

func (s *benchDownloadStats) snapshot() benchDownloadSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	return benchDownloadSnapshot{
		lastHeight:       s.lastHeight,
		downloadedBlocks: s.downloadedBlocks,
		failedBlocks:     s.failedBlocks,
		downloadedBytes:  s.downloadedBytes,
		elapsed:          now.Sub(s.start),
		at:               now,
	}
}

func (s *benchDownloadStats) result() BenchDownloadResult {
	snapshot := s.snapshot()
	return BenchDownloadResult{
		FromHeight:       s.fromHeight,
		LastHeight:       snapshot.lastHeight,
		DownloadedBlocks: snapshot.downloadedBlocks,
		FailedBlocks:     snapshot.failedBlocks,
		DownloadedBytes:  snapshot.downloadedBytes,
		Duration:         snapshot.elapsed,
	}
}

func logBenchDownloadSnapshot(logger *log.Logger, prev benchDownloadSnapshot, current benchDownloadSnapshot) {
	elapsed := current.elapsed.Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	window := current.at.Sub(prev.at).Seconds()
	if window <= 0 {
		window = 1
	}

	bytesDelta := current.downloadedBytes - prev.downloadedBytes
	blocksDelta := current.downloadedBlocks - prev.downloadedBlocks
	logger.Printf(
		"bench download blocks=%d failed=%d bytes=%d current_mib_s=%.2f average_mib_s=%.2f current_blocks_s=%.2f average_blocks_s=%.2f last_height=%d",
		current.downloadedBlocks,
		current.failedBlocks,
		current.downloadedBytes,
		float64(bytesDelta)/window/1024/1024,
		float64(current.downloadedBytes)/elapsed/1024/1024,
		float64(blocksDelta)/window,
		float64(current.downloadedBlocks)/elapsed,
		current.lastHeight,
	)
}
