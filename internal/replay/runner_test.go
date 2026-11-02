package replay

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strconv"
	"testing"

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
			if err := encoder.Encode(AckMessage{Type: "ack", Sequence: value.Event.Sequence}); err != nil {
				os.Exit(24)
			}
			if os.Getenv("ZSYNC_TEST_STOP_AFTER") != "" && value.Event.Sequence == stopAfter {
				os.Exit(25)
			}
		case *EndMessage:
			os.Exit(0)
		default:
			os.Exit(26)
		}
	}
}
