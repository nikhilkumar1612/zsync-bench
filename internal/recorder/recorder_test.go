package recorder_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nikhilkumar1612/zsync-bench/internal/dataset"
	"github.com/nikhilkumar1612/zsync-bench/internal/recorder"
	"github.com/nikhilkumar1612/zsync-bench/internal/testzebra"
	"github.com/nikhilkumar1612/zsync-bench/internal/zebra"
)

func TestRecorderCreatesValidatedFinalDataset(t *testing.T) {
	chain := testzebra.LinearChain(100, strings.Repeat("0", 64), 4, 1000)
	server := testzebra.New(chain)
	defer server.Close()
	client := newSource(t, server.URL())
	config := recorder.DefaultConfig()
	config.OutputDir = t.TempDir() + "/dataset"
	config.StartHeight = 101
	config.MaxBlocks = 3
	config.ShutdownMode = recorder.ShutdownFinalize
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := recorder.New(config, client, logger).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	report, err := dataset.Validate(config.OutputDir)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Finalized || report.BlockObjectCount != 3 || report.FinalTip.Height != 103 || report.FinalTip.Hash != chain[3].Hash {
		t.Fatalf("unexpected report: %+v", report)
	}
	if server.RequestCount("getblock") != 3 {
		t.Fatalf("getblock requests %d, want 3", server.RequestCount("getblock"))
	}
}

func TestRecorderCanPauseGrowingDataset(t *testing.T) {
	chain := testzebra.LinearChain(20, strings.Repeat("0", 64), 2, 1000)
	server := testzebra.New(chain)
	defer server.Close()
	client := newSource(t, server.URL())
	config := recorder.DefaultConfig()
	config.OutputDir = t.TempDir() + "/dataset"
	config.StartHeight = 21
	config.MaxBlocks = 1
	config.ShutdownMode = recorder.ShutdownPause
	if err := recorder.New(config, client, slog.New(slog.NewTextHandler(io.Discard, nil))).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	report, err := dataset.Validate(config.OutputDir)
	if err != nil {
		t.Fatal(err)
	}
	if report.Finalized || report.FinalTip.Height != 21 {
		t.Fatalf("unexpected report: %+v", report)
	}
}

func TestRecorderRejectsConfiguredTestChain(t *testing.T) {
	chain := testzebra.LinearChain(20, strings.Repeat("0", 64), 2, 1000)
	server := testzebra.New(chain)
	defer server.Close()
	server.SetGenesisHash(strings.Repeat("1", 64))
	config := recorder.DefaultConfig()
	config.OutputDir = t.TempDir() + "/dataset"
	config.StartHeight = 21
	config.MaxBlocks = 1
	err := recorder.New(config, newSource(t, server.URL()), discardLogger()).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "not the public Zcash Testnet genesis") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFixedCaptureAcceptsPartiallySyncedSource(t *testing.T) {
	chain := testzebra.LinearChain(100, strings.Repeat("0", 64), 3, 1000)
	server := testzebra.New(chain)
	defer server.Close()
	server.SetSyncState(1000, 0.25)
	config := recorder.DefaultConfig()
	config.OutputDir = t.TempDir() + "/dataset"
	config.StartHeight = 101
	config.MaxBlocks = 1
	if err := recorder.New(config, newSource(t, server.URL()), discardLogger()).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFollowRejectsPartiallySyncedSource(t *testing.T) {
	chain := testzebra.LinearChain(100, strings.Repeat("0", 64), 3, 1000)
	server := testzebra.New(chain)
	defer server.Close()
	server.SetSyncState(1000, 0.25)
	config := recorder.DefaultConfig()
	config.OutputDir = t.TempDir() + "/dataset"
	config.StartHeight = 101
	config.MaxBlocks = 1
	config.Follow = true
	err := recorder.New(config, newSource(t, server.URL()), discardLogger()).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "verification progress") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFixedCaptureRejectsPrunedRange(t *testing.T) {
	chain := testzebra.LinearChain(100, strings.Repeat("0", 64), 3, 1000)
	server := testzebra.New(chain)
	defer server.Close()
	server.SetPruning(102)
	config := recorder.DefaultConfig()
	config.OutputDir = t.TempDir() + "/dataset"
	config.StartHeight = 101
	config.MaxBlocks = 1
	err := recorder.New(config, newSource(t, server.URL()), discardLogger()).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "below Zebra prune height") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRecorderResumesAtCommittedBoundary(t *testing.T) {
	chain := testzebra.LinearChain(100, strings.Repeat("0", 64), 6, 1000)
	server := testzebra.New(chain)
	defer server.Close()
	root := t.TempDir() + "/dataset"
	config := recorder.DefaultConfig()
	config.OutputDir = root
	config.StartHeight = 101
	config.MaxBlocks = 2
	if err := recorder.New(config, newSource(t, server.URL()), discardLogger()).Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	config.Resume = true
	config.StartHeight = 0
	config.ShutdownMode = recorder.ShutdownFinalize
	if err := recorder.New(config, newSource(t, server.URL()), discardLogger()).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	report, err := dataset.Validate(root)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Finalized || report.FinalTip.Height != 104 || report.BlockObjectCount != 4 {
		t.Fatalf("unexpected resumed report: %+v", report)
	}
	if _, err := os.Stat(root + "/state/recorder-checkpoint.json"); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
}

func TestRecorderRecordsReorganizationOnResume(t *testing.T) {
	original := testzebra.LinearChain(100, strings.Repeat("0", 64), 4, 1000)
	server := testzebra.New(original)
	defer server.Close()
	root := t.TempDir() + "/dataset"
	config := recorder.DefaultConfig()
	config.OutputDir = root
	config.StartHeight = 101
	config.MaxBlocks = 3
	if err := recorder.New(config, newSource(t, server.URL()), discardLogger()).Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	replacement := append([]testzebra.Block{original[0]}, testzebra.Chain(101, original[0].Hash, 3, 2000, 40)...)
	server.SetChain(replacement)
	config.Resume = true
	config.StartHeight = 0
	config.ShutdownMode = recorder.ShutdownFinalize
	if err := recorder.New(config, newSource(t, server.URL()), discardLogger()).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	report, err := dataset.Validate(root)
	if err != nil {
		t.Fatal(err)
	}
	if report.FinalTip.Hash != replacement[3].Hash || report.BlockObjectCount != 6 {
		t.Fatalf("unexpected reorg report: %+v", report)
	}
	if got := countEvents(t, root, "block_disconnected"); got != 3 {
		t.Fatalf("disconnect events %d, want 3", got)
	}
}

func TestRecorderRecoversFromTemporaryRPCFailure(t *testing.T) {
	chain := testzebra.LinearChain(100, strings.Repeat("0", 64), 3, 1000)
	server := testzebra.New(chain)
	defer server.Close()
	server.FailNext("getblock", 1)
	config := recorder.DefaultConfig()
	config.OutputDir = t.TempDir() + "/dataset"
	config.StartHeight = 101
	config.MaxBlocks = 1
	config.RetryMin = time.Millisecond
	config.RetryMax = 2 * time.Millisecond
	config.ShutdownMode = recorder.ShutdownFinalize
	if err := recorder.New(config, newSource(t, server.URL()), discardLogger()).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := countEvents(t, config.OutputDir, "source_unavailable"); got != 1 {
		t.Fatalf("source_unavailable events %d, want 1", got)
	}
	if got := countEvents(t, config.OutputDir, "source_recovered"); got != 1 {
		t.Fatalf("source_recovered events %d, want 1", got)
	}
	if _, err := dataset.Validate(config.OutputDir); err != nil {
		t.Fatal(err)
	}
}

func countEvents(t *testing.T, root, kind string) int {
	t.Helper()
	file, err := os.Open(root + "/events.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	count := 0
	_, _, err = dataset.ScanFrames(file, dataset.ScanOptions{MaxPayloadBytes: 1 << 20}, func(event dataset.EventEnvelope) error {
		if event.Kind == kind {
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newSource(t *testing.T, url string) *zebra.Client {
	t.Helper()
	client, err := zebra.New(zebra.Config{URL: url, RequestTimeout: time.Second, MaxResponseBytes: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return client
}
