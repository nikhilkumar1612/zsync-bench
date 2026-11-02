package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/nikhilkumar1612/zsync-bench/internal/dataset"
)

func TestFixedMaximumReplayAndManifest(t *testing.T) {
	datasetRoot := t.TempDir() + "/dataset"
	if err := dataset.WriteFixture(datasetRoot, "linear"); err != nil {
		t.Fatal(err)
	}
	runDir := t.TempDir() + "/run"
	runner := newHelperRunner(t, datasetRoot, runDir, nil)
	result, err := runner.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := dataset.LoadManifest(datasetRoot)
	if err != nil {
		t.Fatal(err)
	}
	if result.FinalAcknowledged != manifest.Final.FinalEventSequence {
		t.Fatalf("acknowledged %d, want %d", result.FinalAcknowledged, manifest.Final.FinalEventSequence)
	}
	assertRunFile(t, runDir+"/run-manifest.json")
	assertRunFile(t, runDir+"/progress.json")
}

func TestRepeatedFixedRunsDeliverIdenticalSequences(t *testing.T) {
	datasetRoot := t.TempDir() + "/dataset"
	if err := dataset.WriteFixture(datasetRoot, "reorg"); err != nil {
		t.Fatal(err)
	}
	files := []string{t.TempDir() + "/first", t.TempDir() + "/second"}
	for i, path := range files {
		runner := newHelperRunner(t, datasetRoot, t.TempDir()+"/run", []string{"ZSYNC_TEST_SEQUENCE_FILE=" + path})
		if _, err := runner.Run(context.Background()); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}
	first, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(files[1])
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatalf("sequence deliveries differ:\n%s\n%s", first, second)
	}
}

func TestReplayResumesAfterDurableAcknowledgement(t *testing.T) {
	datasetRoot := t.TempDir() + "/dataset"
	if err := dataset.WriteFixture(datasetRoot, "linear"); err != nil {
		t.Fatal(err)
	}
	runDir := t.TempDir() + "/run"
	first := newHelperRunner(t, datasetRoot, runDir, []string{"ZSYNC_TEST_STOP_AFTER=3"})
	if _, err := first.Run(context.Background()); err == nil {
		t.Fatal("expected injected consumer exit")
	}

	config := helperConfig(datasetRoot, runDir, nil)
	config.Resume = true
	second, err := New(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := second.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.StartSequence != 4 {
		t.Fatalf("resume started at %d, want 4", result.StartSequence)
	}
}

func TestReplayStartsAtHeightBoundary(t *testing.T) {
	datasetRoot := t.TempDir() + "/dataset"
	if err := dataset.WriteFixture(datasetRoot, "linear"); err != nil {
		t.Fatal(err)
	}
	height := uint64(102)
	config := helperConfig(datasetRoot, t.TempDir()+"/run", nil)
	config.StartHeight = &height
	runner, err := New(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.StartSequence != 3 {
		t.Fatalf("height 102 resolved to %d, want 3", result.StartSequence)
	}
}

func TestGrowingReplayFollowsCommittedEventsAndHeartbeats(t *testing.T) {
	root := t.TempDir() + "/dataset"
	anchor := dataset.ChainTip{Height: 100, Hash: "0000000000000000000000000000000000000000000000000000000000000000"}
	limits := dataset.Limits{MaxEventPayloadBytes: 1 << 20, MaxRawBlockBytes: 2 << 20, MaxReorgDepth: 100}
	writer, err := dataset.Create(root, "123e4567-e89b-12d3-a456-426614174111", anchor, limits, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Append("source_started", dataset.SourceStartedEvent{SessionID: "live", RecorderVersion: "test", ZebraEndpointID: "fixture"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	heartbeatFile := t.TempDir() + "/heartbeat"
	config := helperConfig(root, t.TempDir()+"/run", []string{"ZSYNC_TEST_HEARTBEAT_FILE=" + heartbeatFile})
	config.Follow = true
	config.StopAfterEvents = 2
	config.PollInterval = 5 * time.Millisecond
	config.HeartbeatInterval = 5 * time.Millisecond
	runner, err := New(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := runner.Run(context.Background()); done <- err }()
	time.Sleep(40 * time.Millisecond)
	if _, err := writer.Append("source_stopped", dataset.SourceStoppedEvent{SessionID: "live", Reason: "test"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("live replay did not stop")
	}
	if _, err := os.Stat(heartbeatFile); err != nil {
		t.Fatalf("heartbeat not observed: %v", err)
	}
}

func TestTimingModesScaleRecordedDelays(t *testing.T) {
	root := t.TempDir() + "/dataset"
	anchor := dataset.ChainTip{Height: 100, Hash: "0000000000000000000000000000000000000000000000000000000000000000"}
	limits := dataset.Limits{MaxEventPayloadBytes: 1 << 20, MaxRawBlockBytes: 2 << 20, MaxReorgDepth: 100}
	base := time.Unix(1000, 0)
	writer, err := dataset.Create(root, "123e4567-e89b-12d3-a456-426614174222", anchor, limits, base)
	if err != nil {
		t.Fatal(err)
	}
	writer.Append("source_started", dataset.SourceStartedEvent{SessionID: "timing", RecorderVersion: "test", ZebraEndpointID: "fixture"}, base)
	writer.Append("source_stopped", dataset.SourceStoppedEvent{SessionID: "timing", Reason: "test"}, base.Add(100*time.Millisecond))
	if err := writer.Finalize(anchor, base.Add(200*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		mode  TimingMode
		accel float64
		want  time.Duration
	}{{TimingRecorded, 1, 200 * time.Millisecond}, {TimingAccelerated, 10, 20 * time.Millisecond}, {TimingMaximum, 1, 0}}
	for _, tc := range tests {
		t.Run(string(tc.mode), func(t *testing.T) {
			config := helperConfig(root, t.TempDir()+"/run", nil)
			config.Mode = tc.mode
			config.Acceleration = tc.accel
			runner, err := New(config, nil)
			if err != nil {
				t.Fatal(err)
			}
			var total time.Duration
			runner.wait = func(_ context.Context, d time.Duration) error { total += d; return nil }
			if _, err := runner.Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			if total != tc.want {
				t.Fatalf("delay %v, want %v", total, tc.want)
			}
		})
	}
}

func newHelperRunner(t *testing.T, datasetRoot, runDir string, env []string) *Runner {
	t.Helper()
	runner, err := New(helperConfig(datasetRoot, runDir, env), nil)
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func helperConfig(datasetRoot, runDir string, env []string) Config {
	return Config{DatasetRoot: datasetRoot, RunDir: runDir, Command: os.Args[0], Args: []string{"-test.run=TestReplayConsumerHelper"}, Env: append([]string{"ZSYNC_TEST_REPLAY_HELPER=1"}, env...), Mode: TimingMaximum, MaxInFlight: 1}
}

func assertRunFile(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
}

func TestReplayConsumerHelper(t *testing.T) {
	if os.Getenv("ZSYNC_TEST_REPLAY_HELPER") != "1" {
		return
	}
	decoder := NewDecoder(os.Stdin, DefaultMaxFrameBytes)
	encoder := NewEncoder(os.Stdout)
	first, err := decoder.DecodeRequest()
	if err != nil {
		os.Exit(20)
	}
	if _, ok := first.(*Handshake); !ok {
		os.Exit(21)
	}
	if err := encoder.Encode(ReadyMessage{Type: "ready", ProtocolMajor: ProtocolMajor, ProtocolMinor: ProtocolMinor}); err != nil {
		os.Exit(22)
	}
	stopAfter, _ := strconv.ParseUint(os.Getenv("ZSYNC_TEST_STOP_AFTER"), 10, 64)
	for {
		message, err := decoder.DecodeRequest()
		if err != nil {
			if err == io.EOF {
				os.Exit(0)
			}
			os.Exit(23)
		}
		switch value := message.(type) {
		case *EventMessage:
			if path := os.Getenv("ZSYNC_TEST_SEQUENCE_FILE"); path != "" {
				file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
				if err != nil {
					os.Exit(27)
				}
				_, _ = fmt.Fprintf(file, "%d\n", value.Event.Sequence)
				_ = file.Close()
			}
			if err := encoder.Encode(AckMessage{Type: "ack", Sequence: value.Event.Sequence}); err != nil {
				os.Exit(24)
			}
			if os.Getenv("ZSYNC_TEST_STOP_AFTER") != "" && value.Event.Sequence == stopAfter {
				os.Exit(25)
			}
		case *EndMessage:
			os.Exit(0)
		case *HeartbeatMessage:
			if path := os.Getenv("ZSYNC_TEST_HEARTBEAT_FILE"); path != "" {
				_ = os.WriteFile(path, []byte("heartbeat"), 0o644)
			}
			continue
		case *StopMessage:
			os.Exit(0)
		default:
			os.Exit(26)
		}
	}
}
