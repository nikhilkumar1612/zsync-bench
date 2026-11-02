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

	"github.com/nikhilkumar1612/zsync-bench/internal/replay"
)

type stringList []string

func (s *stringList) String() string         { return fmt.Sprint([]string(*s)) }
func (s *stringList) Set(value string) error { *s = append(*s, value); return nil }

func main() {
	if len(os.Args) < 2 || os.Args[1] != "run" {
		fmt.Fprintln(os.Stderr, "usage: zsync-replay run [options]")
		os.Exit(2)
	}
	var config replay.Config
	var args stringList
	var startHeight uint64
	flags := flag.NewFlagSet("run", flag.ExitOnError)
	flags.StringVar(&config.DatasetRoot, "dataset", "", "finalized or growing dataset directory")
	flags.StringVar(&config.RunDir, "run-dir", "", "directory for replay progress and run manifest")
	flags.StringVar(&config.Command, "consumer", "", "indexer executable to launch")
	flags.Var(&args, "consumer-arg", "consumer argument; repeat for multiple arguments")
	mode := flags.String("timing", string(replay.TimingMaximum), "recorded, accelerated, or maximum")
	flags.Float64Var(&config.Acceleration, "acceleration", 1, "timing acceleration factor")
	flags.Uint64Var(&config.StartSequence, "start-sequence", 0, "first event sequence to deliver")
	flags.Uint64Var(&startHeight, "start-height", 0, "start at the first observed block that connects at this height")
	flags.BoolVar(&config.Resume, "resume", false, "resume after the durable acknowledged sequence")
	flags.BoolVar(&config.Follow, "follow", false, "follow newly committed events in a growing dataset")
	flags.Uint64Var(&config.MaxInFlight, "max-in-flight", 256, "maximum sent but unacknowledged events")
	flags.Uint64Var(&config.StopAfterEvents, "stop-after-events", 0, "pause cleanly after this many delivered events")
	flags.DurationVar(&config.PollInterval, "poll-interval", 250*time.Millisecond, "growing-log poll interval")
	flags.DurationVar(&config.HeartbeatInterval, "heartbeat-interval", 5*time.Second, "idle transport heartbeat interval; zero disables")
	flags.DurationVar(&config.ProgressInterval, "progress-interval", 2*time.Second, "progress log interval")
	_ = flags.Parse(os.Args[2:])
	config.Mode = replay.TimingMode(*mode)
	config.Args = args
	if startHeight > 0 {
		config.StartHeight = &startHeight
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	runner, err := replay.New(config, logger)
	if err != nil {
		logger.Error("invalid replay configuration", "error", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	result, err := runner.Run(ctx)
	if err != nil {
		logger.Error("replay failed", "error", err, "run_id", result.RunID)
		os.Exit(1)
	}
	logger.Info("replay completed", "run_id", result.RunID, "final_acknowledged", result.FinalAcknowledged)
}
