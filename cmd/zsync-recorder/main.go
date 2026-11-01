package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nikhilkumar1612/zsync-bench/internal/recorder"
	"github.com/nikhilkumar1612/zsync-bench/internal/zebra"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "record" {
		fmt.Fprintln(os.Stderr, "usage: zsync-recorder record [options]")
		os.Exit(2)
	}
	config := recorder.DefaultConfig()
	flags := flag.NewFlagSet("record", flag.ExitOnError)
	flags.StringVar(&config.ZebraURL, "zebra-url", config.ZebraURL, "Zebra JSON-RPC URL")
	flags.StringVar(&config.Username, "rpc-user", "", "Zebra RPC username")
	flags.StringVar(&config.OutputDir, "output", "", "dataset output directory")
	flags.BoolVar(&config.Resume, "resume", false, "resume an existing growing dataset")
	flags.Uint64Var(&config.StartHeight, "start-height", 0, "first block height to record (must be at least 1)")
	flags.Uint64Var(&config.MaxBlocks, "max-blocks", 0, "maximum blocks to record; required unless --follow")
	flags.BoolVar(&config.Follow, "follow", false, "continue following the live tip")
	flags.DurationVar(&config.PollInterval, "poll-interval", config.PollInterval, "live tip poll interval")
	flags.DurationVar(&config.RPCTimeout, "rpc-timeout", config.RPCTimeout, "per-request RPC timeout")
	flags.DurationVar(&config.RetryMin, "retry-min", config.RetryMin, "initial Zebra retry delay")
	flags.DurationVar(&config.RetryMax, "retry-max", config.RetryMax, "maximum Zebra retry delay")
	flags.Int64Var(&config.MaxResponseBytes, "max-response-bytes", config.MaxResponseBytes, "maximum Zebra RPC response size")
	shutdown := string(config.ShutdownMode)
	flags.StringVar(&shutdown, "shutdown-mode", shutdown, "pause or finalize dataset on clean stop")
	_ = flags.Parse(os.Args[2:])
	config.ShutdownMode = recorder.ShutdownMode(shutdown)
	config.Password = os.Getenv("ZSYNC_ZEBRA_RPC_PASSWORD")
	if err := config.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		os.Exit(2)
	}
	source, err := zebra.New(zebra.Config{URL: config.ZebraURL, Username: config.Username, Password: config.Password, RequestTimeout: config.RPCTimeout, MaxResponseBytes: config.MaxResponseBytes})
	if err != nil {
		fmt.Fprintf(os.Stderr, "configure Zebra client: %v\n", err)
		os.Exit(2)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	started := time.Now()
	if err := recorder.New(config, source, logger).Run(ctx); err != nil {
		logger.Error("recorder stopped with error", "error", err, "elapsed", time.Since(started))
		os.Exit(1)
	}
	logger.Info("recorder stopped", "elapsed", time.Since(started))
}
