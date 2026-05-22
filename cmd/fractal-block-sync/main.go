package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"

	blocksync "fractal-block-sync"
	"fractal-block-sync/btcrpc"
	"fractal-block-sync/r2store"
)

const defaultRPCURL = "http://127.0.0.1:8332"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: fractal-block-sync <upload|submit|bench-download> [flags]")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	switch args[0] {
	case "upload":
		return runUpload(ctx, args[1:])
	case "submit":
		return runSubmit(ctx, args[1:])
	case "bench-download":
		return runBenchDownload(ctx, args[1:])
	case "-h", "--help", "help":
		printUsage()
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runUpload(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("upload", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	rpcURL := flags.String("rpc-url", defaultRPCURL, "bitcoind RPC URL")
	cookieFile := flags.String("cookie-file", "", "bitcoind cookie file")
	rpcUser := flags.String("rpc-user", "", "bitcoind RPC username")
	rpcPassword := flags.String("rpc-password", "", "bitcoind RPC password")
	fromHeight := flags.Uint64("from-height", 0, "height whose range should be checked first")
	toHeightText := flags.String("to-height", "", "last height to upload, inclusive")
	stableDelay := flags.Uint64("stable-delay", blocksync.DefaultStableDelay, "stable block delay before publishing range indexes")
	uploadWorkers := flags.Uint("upload-workers", blocksync.DefaultUploadWorkers, "parallel block upload workers")
	follow := flags.Bool("follow", false, "keep polling and uploading")
	interval := flags.Duration("interval", 30*time.Second, "follow polling interval")
	if err := flags.Parse(args); err != nil {
		return err
	}
	var toHeight *uint64
	if *toHeightText != "" {
		parsed, err := strconv.ParseUint(*toHeightText, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid --to-height %q: %w", *toHeightText, err)
		}
		toHeight = &parsed
	}

	rpcClient, err := newRPCClient(*rpcURL, *cookieFile, *rpcUser, *rpcPassword)
	if err != nil {
		return err
	}
	r2cfg, err := r2store.LoadConfigFromEnv()
	if err != nil {
		return err
	}
	writer, err := r2store.NewWriter(ctx, r2cfg)
	if err != nil {
		return err
	}

	logger := log.New(os.Stderr, "", log.LstdFlags)
	cfg := blocksync.UploadConfig{
		FromHeight:    *fromHeight,
		ToHeight:      toHeight,
		StableDelay:   *stableDelay,
		UploadWorkers: *uploadWorkers,
		Logger:        logger,
	}
	for {
		if err := blocksync.UploadOnce(ctx, rpcClient, writer, cfg); err != nil {
			if !*follow {
				return err
			}
			if ctx.Err() != nil {
				return nil
			}
			logger.Printf("upload failed: %v", err)
			if err := blocksync.SleepOrDone(ctx, *interval); err != nil {
				return nil
			}
			continue
		}
		if !*follow {
			return nil
		}
		if err := blocksync.SleepOrDone(ctx, *interval); err != nil {
			return nil
		}
	}
}

func runSubmit(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("submit", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	baseURL := flags.String("base-url", "", "public R2 download base URL")
	rpcURL := flags.String("rpc-url", defaultRPCURL, "bitcoind RPC URL")
	cookieFile := flags.String("cookie-file", "", "bitcoind cookie file")
	rpcUser := flags.String("rpc-user", "", "bitcoind RPC username")
	rpcPassword := flags.String("rpc-password", "", "bitcoind RPC password")
	recentWalkLimit := flags.Uint64("recent-walk-limit", blocksync.DefaultRecentWalkLimit, "maximum recent header walk")
	bootstrapFromR2 := flags.Bool("bootstrap-from-r2", false, "submit using R2 range indexes when local headers are unavailable")
	follow := flags.Bool("follow", false, "keep submitting as headers arrive")
	interval := flags.Duration("interval", 10*time.Second, "follow polling interval")
	if err := flags.Parse(args); err != nil {
		return err
	}

	rpcClient, err := newRPCClient(*rpcURL, *cookieFile, *rpcUser, *rpcPassword)
	if err != nil {
		return err
	}
	downloader, err := r2store.NewPublicClient(*baseURL, nil)
	if err != nil {
		return err
	}

	logger := log.New(os.Stderr, "", log.LstdFlags)
	cfg := blocksync.SubmitConfig{
		RecentWalkLimit: *recentWalkLimit,
		BootstrapFromR2: *bootstrapFromR2,
		Logger:          logger,
	}
	for {
		result, err := blocksync.SubmitNext(ctx, rpcClient, downloader, cfg)
		if err != nil {
			return err
		}
		if result.WaitHeaders {
			if result.WaitR2Index {
				logger.Printf("waiting for local headers or R2 range index target_height=%d", result.TargetHeight)
			} else {
				logger.Printf("waiting for local headers target_height=%d", result.TargetHeight)
			}
			if !*follow {
				return nil
			}
			if err := blocksync.SleepOrDone(ctx, *interval); err != nil {
				return nil
			}
			continue
		}
		if !*follow {
			return nil
		}
	}
}

func runBenchDownload(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("bench-download", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	baseURL := flags.String("base-url", "", "public R2 download base URL")
	fromHeightText := flags.String("from-height", "", "first height to download")
	toHeightText := flags.String("to-height", "", "last height to download, inclusive")
	workers := flags.Uint("workers", blocksync.DefaultBenchDownloadWorkers, "parallel block download workers")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *fromHeightText == "" {
		return errors.New("--from-height is required")
	}
	fromHeight, err := strconv.ParseUint(*fromHeightText, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid --from-height %q: %w", *fromHeightText, err)
	}
	var toHeight *uint64
	if *toHeightText != "" {
		parsed, err := strconv.ParseUint(*toHeightText, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid --to-height %q: %w", *toHeightText, err)
		}
		toHeight = &parsed
	}

	downloader, err := r2store.NewPublicClient(*baseURL, newBenchHTTPClient(*workers))
	if err != nil {
		return err
	}

	logger := log.New(os.Stderr, "", log.LstdFlags)
	result, err := blocksync.BenchDownload(ctx, downloader, blocksync.BenchDownloadConfig{
		FromHeight: fromHeight,
		ToHeight:   toHeight,
		Workers:    *workers,
		Logger:     logger,
	})
	elapsed := result.Duration.Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	logger.Printf(
		"bench download finished from_height=%d last_height=%d blocks=%d failed=%d bytes=%d average_mib_s=%.2f average_blocks_s=%.2f",
		result.FromHeight,
		result.LastHeight,
		result.DownloadedBlocks,
		result.FailedBlocks,
		result.DownloadedBytes,
		float64(result.DownloadedBytes)/elapsed/1024/1024,
		float64(result.DownloadedBlocks)/elapsed,
	)
	return err
}

func newBenchHTTPClient(workers uint) *http.Client {
	idleConns := int(workers) * 2
	if idleConns < int(blocksync.DefaultBenchDownloadWorkers) {
		idleConns = int(blocksync.DefaultBenchDownloadWorkers)
	}
	return &http.Client{
		Transport: &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			MaxIdleConns:        idleConns,
			MaxIdleConnsPerHost: idleConns,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}
}

func newRPCClient(rpcURL string, cookieFile string, rpcUser string, rpcPassword string) (*btcrpc.Client, error) {
	opts := []btcrpc.Option{}
	if cookieFile != "" {
		opts = append(opts, btcrpc.WithCookieFile(cookieFile))
	}
	if rpcUser != "" || rpcPassword != "" {
		opts = append(opts, btcrpc.WithBasicAuth(rpcUser, rpcPassword))
	}
	return btcrpc.New(rpcURL, opts...)
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "usage: fractal-block-sync <upload|submit|bench-download> [flags]")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "commands:")
	fmt.Fprintln(os.Stderr, "  upload  upload raw blocks and stable range indexes to R2")
	fmt.Fprintln(os.Stderr, "  submit  download, verify, and submit the next missing block")
	fmt.Fprintln(os.Stderr, "  bench-download  download blocks from R2 without submitting them")
}
