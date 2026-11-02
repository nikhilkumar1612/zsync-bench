package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/nikhilkumar1612/zsync-bench/internal/dataset"
)

type TimingMode string

const (
	TimingRecorded    TimingMode = "recorded"
	TimingAccelerated TimingMode = "accelerated"
	TimingMaximum     TimingMode = "maximum"
)

type Config struct {
	DatasetRoot      string
	RunDir           string
	Command          string
	Args             []string
	Env              []string
	Mode             TimingMode
	Acceleration     float64
	StartSequence    uint64
	Resume           bool
	MaxInFlight      uint64
	MaxFrameBytes    uint32
	ProgressInterval time.Duration
}

type RunManifest struct {
	RunID               string     `json:"run_id"`
	DatasetID           string     `json:"dataset_id"`
	DatasetRoot         string     `json:"dataset_root"`
	DatasetEventsSHA256 string     `json:"dataset_events_sha256"`
	ConsumerCommand     string     `json:"consumer_command"`
	ConsumerArgs        []string   `json:"consumer_args"`
	TimingMode          TimingMode `json:"timing_mode"`
	Acceleration        float64    `json:"acceleration"`
	StartSequence       uint64     `json:"start_sequence"`
	FinalAcknowledged   uint64     `json:"final_acknowledged"`
	EventsSent          uint64     `json:"events_sent"`
	DurationNanos       int64      `json:"duration_nanos"`
	StartedAt           string     `json:"started_at"`
	CompletedAt         string     `json:"completed_at,omitempty"`
	Status              string     `json:"status"`
	Error               string     `json:"error,omitempty"`
}

type progress struct {
	DatasetID            string `json:"dataset_id"`
	AcknowledgedSequence uint64 `json:"acknowledged_sequence"`
}

type Runner struct {
	config Config
	logger *slog.Logger
}

func New(config Config, logger *slog.Logger) (*Runner, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if config.DatasetRoot == "" || config.RunDir == "" || config.Command == "" {
		return nil, fmt.Errorf("dataset, run directory, and consumer command are required")
	}
	if config.Mode == "" {
		config.Mode = TimingMaximum
	}
	if config.Mode != TimingRecorded && config.Mode != TimingAccelerated && config.Mode != TimingMaximum {
		return nil, fmt.Errorf("unsupported timing mode %q", config.Mode)
	}
	if config.Mode == TimingAccelerated && config.Acceleration <= 0 {
		return nil, fmt.Errorf("acceleration must be positive")
	}
	if config.Mode != TimingAccelerated {
		config.Acceleration = 1
	}
	if config.MaxInFlight == 0 {
		config.MaxInFlight = 256
	}
	if config.MaxFrameBytes == 0 {
		config.MaxFrameBytes = DefaultMaxFrameBytes
	}
	if config.ProgressInterval == 0 {
		config.ProgressInterval = 2 * time.Second
	}
	if config.ProgressInterval < 0 {
		return nil, fmt.Errorf("progress interval must not be negative")
	}
	return &Runner{config: config, logger: logger}, nil
}

func (r *Runner) Run(ctx context.Context) (result RunManifest, runErr error) {
	r.logger.Info("loading replay dataset", "dataset", r.config.DatasetRoot)
	manifest, err := dataset.LoadManifest(r.config.DatasetRoot)
	if err != nil {
		return result, fmt.Errorf("load dataset: %w", err)
	}
	if !manifest.Finalized {
		return result, fmt.Errorf("fixed replay requires a finalized dataset")
	}
	if manifest.Final == nil {
		return result, fmt.Errorf("finalized dataset has no final manifest")
	}
	r.logger.Info("validating replay dataset", "dataset_id", manifest.DatasetID, "event_count", manifest.Final.EventCount)
	if _, err := dataset.Validate(r.config.DatasetRoot); err != nil {
		return result, fmt.Errorf("validate dataset: %w", err)
	}
	r.logger.Info("replay dataset validated", "dataset_id", manifest.DatasetID, "event_count", manifest.Final.EventCount)
	absRoot, err := filepath.Abs(r.config.DatasetRoot)
	if err != nil {
		return result, err
	}
	if err := os.MkdirAll(r.config.RunDir, 0o755); err != nil {
		return result, err
	}
	start := r.config.StartSequence
	if r.config.Resume {
		state, err := readProgress(r.config.RunDir)
		if err != nil {
			return result, err
		}
		if state.DatasetID != manifest.DatasetID {
			return result, fmt.Errorf("resume state dataset ID mismatch")
		}
		if state.AcknowledgedSequence == ^uint64(0) {
			return result, fmt.Errorf("resume sequence overflow")
		}
		start = state.AcknowledgedSequence + 1
	}
	if start < manifest.FirstEventSequence || manifest.Final == nil || start > manifest.Final.FinalEventSequence {
		return result, fmt.Errorf("start sequence %d outside dataset range %d..%d", start, manifest.FirstEventSequence, manifest.Final.FinalEventSequence)
	}

	result = RunManifest{RunID: fmt.Sprintf("%d-%d", time.Now().UTC().UnixNano(), os.Getpid()), DatasetID: manifest.DatasetID, DatasetRoot: absRoot, DatasetEventsSHA256: manifest.Final.EventsSHA256, ConsumerCommand: r.config.Command, ConsumerArgs: append([]string(nil), r.config.Args...), TimingMode: r.config.Mode, Acceleration: r.config.Acceleration, StartSequence: start, StartedAt: time.Now().UTC().Format(time.RFC3339Nano), Status: "running"}
	defer func() {
		completed := time.Now().UTC()
		result.CompletedAt = completed.Format(time.RFC3339Nano)
		started, _ := time.Parse(time.RFC3339Nano, result.StartedAt)
		result.DurationNanos = completed.Sub(started).Nanoseconds()
		if runErr != nil {
			result.Status = "failed"
			result.Error = runErr.Error()
		} else {
			result.Status = "completed"
		}
		if err := writeJSONAtomic(filepath.Join(r.config.RunDir, "run-manifest.json"), result); runErr == nil && err != nil {
			runErr = err
		}
	}()

	command := exec.CommandContext(ctx, r.config.Command, r.config.Args...)
	command.Env = append(os.Environ(), r.config.Env...)
	stdin, err := command.StdinPipe()
	if err != nil {
		return result, err
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		return result, err
	}
	command.Stderr = os.Stderr
	r.logger.Info("starting replay consumer", "command", r.config.Command)
	if err := command.Start(); err != nil {
		return result, err
	}
	encoder, decoder := NewEncoder(stdin), NewDecoder(stdout, r.config.MaxFrameBytes)
	state := "finalized"
	handshake := Handshake{Type: "handshake", ProtocolMajor: ProtocolMajor, ProtocolMinor: ProtocolMinor, EventSchemaMajor: manifest.SchemaMajor, EventSchemaMinor: manifest.SchemaMinor, DatasetID: manifest.DatasetID, Network: manifest.Network, DatasetState: state, DatasetRoot: absRoot, FirstSequence: manifest.FirstEventSequence, RequestedSequence: start, TimingMode: string(r.config.Mode), Acceleration: r.config.Acceleration, RawBlockDelivery: "shared_dataset_content_key"}
	if err := encoder.Encode(handshake); err != nil {
		return result, r.failChild(command, stdin, err)
	}
	response, err := decoder.DecodeResponse()
	if err != nil {
		return result, r.failChild(command, stdin, fmt.Errorf("read consumer ready: %w", err))
	}
	ready, ok := response.(*ReadyMessage)
	if !ok || ready.ProtocolMajor != ProtocolMajor || ready.ProtocolMinor > ProtocolMinor {
		return result, r.failChild(command, stdin, fmt.Errorf("consumer rejected or mismatched handshake"))
	}
	r.logger.Info("replay consumer ready", "requested_sequence", start, "timing_mode", r.config.Mode, "max_in_flight", r.config.MaxInFlight)

	var sentEvents atomic.Uint64
	var acknowledgedEvents atomic.Uint64
	progressDone := make(chan struct{})
	if r.config.ProgressInterval > 0 {
		ticker := time.NewTicker(r.config.ProgressInterval)
		defer ticker.Stop()
		go func() {
			for {
				select {
				case <-progressDone:
					return
				case <-ticker.C:
					sentCount := sentEvents.Load()
					ackedCount := acknowledgedEvents.Load()
					total := manifest.Final.FinalEventSequence - start + 1
					percent := float64(ackedCount) * 100 / float64(total)
					r.logger.Info("replay progress", "sent_events", sentCount, "acknowledged_events", ackedCount, "outstanding_events", sentCount-ackedCount, "total_events", total, "percent", percent)
				}
			}
		}()
	}
	defer close(progressDone)

	ackCh := make(chan any, 1)
	go func() {
		for {
			value, err := decoder.DecodeResponse()
			if err != nil {
				ackCh <- err
				return
			}
			ackCh <- value
		}
	}()
	logFile, err := os.Open(filepath.Join(r.config.DatasetRoot, "events.bin"))
	if err != nil {
		return result, r.failChild(command, stdin, err)
	}
	defer logFile.Close()
	var sent, acked uint64
	var haveSent, haveAck bool
	var previousRecordedAt int64
	waitAck := func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case value := <-ackCh:
			switch message := value.(type) {
			case error:
				return message
			case *ErrorMessage:
				return fmt.Errorf("consumer error %s: %s", message.Code, message.Message)
			case *AckMessage:
				if !haveSent || message.Sequence > sent || haveAck && message.Sequence < acked {
					return fmt.Errorf("invalid acknowledgement %d", message.Sequence)
				}
				acked, haveAck = message.Sequence, true
				acknowledgedEvents.Store(message.Sequence - start + 1)
				result.FinalAcknowledged = acked
				return writeJSONAtomic(filepath.Join(r.config.RunDir, "progress.json"), progress{DatasetID: manifest.DatasetID, AcknowledgedSequence: acked})
			default:
				return fmt.Errorf("unexpected consumer response")
			}
		}
	}
	outstanding := func() uint64 {
		if !haveSent {
			return 0
		}
		if !haveAck || acked < start {
			return sent - start + 1
		}
		return sent - acked
	}
	_, _, err = dataset.ScanFrames(logFile, dataset.ScanOptions{MaxPayloadBytes: manifest.Limits.MaxEventPayloadBytes}, func(event dataset.EventEnvelope) error {
		if event.Sequence < start {
			return nil
		}
		if previousRecordedAt != 0 && r.config.Mode != TimingMaximum {
			delay := time.Duration(event.RecordedAtUnixNanos - previousRecordedAt)
			if delay < 0 {
				return fmt.Errorf("event %d recorded time regressed", event.Sequence)
			}
			if r.config.Mode == TimingAccelerated {
				delay = time.Duration(float64(delay) / r.config.Acceleration)
			}
			if err := waitContext(ctx, delay); err != nil {
				return err
			}
		}
		previousRecordedAt = event.RecordedAtUnixNanos
		if err := encoder.Encode(EventMessage{Type: "event", Event: event}); err != nil {
			return err
		}
		sent, haveSent = event.Sequence, true
		result.EventsSent++
		sentEvents.Store(result.EventsSent)
		for outstanding() >= r.config.MaxInFlight {
			if err := waitAck(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return result, r.failChild(command, stdin, err)
	}
	if !haveSent {
		return result, r.failChild(command, stdin, fmt.Errorf("no events at requested sequence"))
	}
	if err := encoder.Encode(EndMessage{Type: "end", FinalSequence: sent}); err != nil {
		return result, r.failChild(command, stdin, err)
	}
	for !haveAck || acked < sent {
		if err := waitAck(); err != nil {
			return result, r.failChild(command, stdin, err)
		}
	}
	if err := stdin.Close(); err != nil {
		return result, r.failChild(command, stdin, err)
	}
	if err := command.Wait(); err != nil {
		return result, err
	}
	return result, nil
}

func (r *Runner) failChild(command *exec.Cmd, stdin io.Closer, cause error) error {
	_ = stdin.Close()
	if command.Process != nil {
		_ = command.Process.Kill()
	}
	_ = command.Wait()
	return cause
}

func waitContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func readProgress(runDir string) (progress, error) {
	data, err := os.ReadFile(filepath.Join(runDir, "progress.json"))
	if err != nil {
		return progress{}, fmt.Errorf("read replay progress: %w", err)
	}
	var value progress
	if err := json.Unmarshal(data, &value); err != nil {
		return progress{}, err
	}
	return value, nil
}

func writeJSONAtomic(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".replay-")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
