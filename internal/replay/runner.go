package replay

import (
	"context"
	"encoding/json"
	"errors"
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
	DatasetRoot       string
	RunDir            string
	Command           string
	Args              []string
	Env               []string
	Mode              TimingMode
	Acceleration      float64
	StartSequence     uint64
	StartHeight       *uint64
	Resume            bool
	Follow            bool
	MaxInFlight       uint64
	MaxFrameBytes     uint32
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
	ProgressInterval  time.Duration
	StopAfterEvents   uint64
}

type RunManifest struct {
	RunID               string     `json:"run_id"`
	DatasetID           string     `json:"dataset_id"`
	DatasetRoot         string     `json:"dataset_root"`
	DatasetState        string     `json:"dataset_state"`
	DatasetEventsSHA256 string     `json:"dataset_events_sha256,omitempty"`
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
	StopReason          string     `json:"stop_reason,omitempty"`
	Error               string     `json:"error,omitempty"`
}

type progress struct {
	DatasetID            string `json:"dataset_id"`
	AcknowledgedSequence uint64 `json:"acknowledged_sequence"`
}
type Runner struct {
	config Config
	logger *slog.Logger
	wait   func(context.Context, time.Duration) error
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
	if config.StartHeight != nil && config.StartSequence != 0 {
		return nil, fmt.Errorf("start sequence and start height are mutually exclusive")
	}
	if config.Resume && config.StartHeight != nil {
		return nil, fmt.Errorf("resume and start height are mutually exclusive")
	}
	if config.MaxInFlight == 0 {
		config.MaxInFlight = 256
	}
	if config.MaxFrameBytes == 0 {
		config.MaxFrameBytes = DefaultMaxFrameBytes
	}
	if config.PollInterval == 0 {
		config.PollInterval = 250 * time.Millisecond
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 5 * time.Second
	}
	if config.ProgressInterval == 0 {
		config.ProgressInterval = 2 * time.Second
	}
	if config.PollInterval < 0 || config.HeartbeatInterval < 0 || config.ProgressInterval < 0 {
		return nil, fmt.Errorf("intervals must not be negative")
	}
	return &Runner{config: config, logger: logger, wait: waitContext}, nil
}

func (r *Runner) Run(ctx context.Context) (result RunManifest, runErr error) {
	manifest, err := dataset.LoadManifest(r.config.DatasetRoot)
	if err != nil {
		return result, fmt.Errorf("load dataset: %w", err)
	}
	if !manifest.Finalized && !r.config.Follow {
		return result, fmt.Errorf("growing dataset requires follow mode")
	}
	report, err := dataset.Validate(r.config.DatasetRoot)
	if err != nil {
		return result, fmt.Errorf("validate dataset: %w", err)
	}
	absRoot, err := filepath.Abs(r.config.DatasetRoot)
	if err != nil {
		return result, err
	}
	if err := os.MkdirAll(r.config.RunDir, 0o755); err != nil {
		return result, err
	}
	start := r.config.StartSequence
	if start == 0 {
		start = manifest.FirstEventSequence
	}
	if r.config.StartHeight != nil {
		start, err = resolveHeight(r.config.DatasetRoot, manifest, *r.config.StartHeight)
		if err != nil {
			return result, err
		}
	}
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
	if start < manifest.FirstEventSequence {
		return result, fmt.Errorf("start sequence %d precedes first sequence %d", start, manifest.FirstEventSequence)
	}
	if manifest.Finalized && (manifest.Final == nil || start > manifest.Final.FinalEventSequence) {
		return result, fmt.Errorf("start sequence %d is beyond finalized dataset", start)
	}
	if !manifest.Finalized && report.EventCount > 0 && start > report.LastSequence+1 {
		return result, fmt.Errorf("start sequence %d is beyond committed tail %d", start, report.LastSequence)
	}

	stateName := "growing"
	datasetHash := ""
	if manifest.Finalized {
		stateName = "finalized"
		datasetHash = manifest.Final.EventsSHA256
	}
	started := time.Now().UTC()
	result = RunManifest{RunID: fmt.Sprintf("%d-%d", started.UnixNano(), os.Getpid()), DatasetID: manifest.DatasetID, DatasetRoot: absRoot, DatasetState: stateName, DatasetEventsSHA256: datasetHash, ConsumerCommand: r.config.Command, ConsumerArgs: append([]string(nil), r.config.Args...), TimingMode: r.config.Mode, Acceleration: r.config.Acceleration, StartSequence: start, StartedAt: started.Format(time.RFC3339Nano), Status: "running"}
	defer func() {
		completed := time.Now().UTC()
		result.CompletedAt = completed.Format(time.RFC3339Nano)
		result.DurationNanos = completed.Sub(started).Nanoseconds()
		if runErr != nil {
			result.Status = "failed"
			result.Error = runErr.Error()
		} else if result.StopReason == "configured_pause" {
			result.Status = "paused"
		} else {
			result.Status = "completed"
		}
		if err := writeJSONAtomic(filepath.Join(r.config.RunDir, "run-manifest.json"), result); runErr == nil && err != nil {
			runErr = err
		}
	}()

	command := exec.CommandContext(ctx, r.config.Command, r.config.Args...)
	command.Env = append(os.Environ(), r.config.Env...)
	command.Stderr = os.Stderr
	stdin, err := command.StdinPipe()
	if err != nil {
		return result, err
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		return result, err
	}
	if err := command.Start(); err != nil {
		return result, err
	}
	encoder, decoder := NewEncoder(stdin), NewDecoder(stdout, r.config.MaxFrameBytes)
	handshake := Handshake{Type: "handshake", ProtocolMajor: ProtocolMajor, ProtocolMinor: ProtocolMinor, EventSchemaMajor: manifest.SchemaMajor, EventSchemaMinor: manifest.SchemaMinor, DatasetID: manifest.DatasetID, Network: manifest.Network, DatasetState: stateName, DatasetRoot: absRoot, FirstSequence: manifest.FirstEventSequence, RequestedSequence: start, TimingMode: string(r.config.Mode), Acceleration: r.config.Acceleration, RawBlockDelivery: "shared_dataset_content_key"}
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
	frames, err := dataset.NewFrameReader(logFile)
	if err != nil {
		return result, r.failChild(command, stdin, err)
	}
	var sent, acked uint64
	var haveSent, haveAck bool
	var previousRecordedAt int64
	expectedFileSequence := manifest.FirstEventSequence
	var sentMetric, ackMetric atomic.Uint64
	progressDone := make(chan struct{})
	defer close(progressDone)
	if r.config.ProgressInterval > 0 {
		go r.logProgress(progressDone, &sentMetric, &ackMetric)
	}

	processResponse := func(value any) error {
		switch message := value.(type) {
		case error:
			return message
		case *ErrorMessage:
			return fmt.Errorf("consumer error %s: %s", message.Code, message.Message)
		case *AckMessage:
			if !haveSent || message.Sequence > sent || (haveAck && message.Sequence < acked) {
				return fmt.Errorf("invalid acknowledgement %d", message.Sequence)
			}
			acked, haveAck = message.Sequence, true
			result.FinalAcknowledged = acked
			ackMetric.Store(acked - start + 1)
			return writeJSONAtomic(filepath.Join(r.config.RunDir, "progress.json"), progress{DatasetID: manifest.DatasetID, AcknowledgedSequence: acked})
		default:
			return fmt.Errorf("unexpected consumer response")
		}
	}
	waitAck := func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case value := <-ackCh:
			return processResponse(value)
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
	drainAll := func() error {
		for haveSent && (!haveAck || acked < sent) {
			if err := waitAck(); err != nil {
				return err
			}
		}
		return nil
	}
	lastHeartbeat := time.Now()

	for {
		event, readErr := frames.Next(manifest.Limits.MaxEventPayloadBytes)
		if readErr == nil {
			if event.Sequence != expectedFileSequence {
				return result, r.failChild(command, stdin, fmt.Errorf("expected file sequence %d, got %d", expectedFileSequence, event.Sequence))
			}
			expectedFileSequence++
			if event.Sequence < start {
				continue
			}
			if previousRecordedAt != 0 && r.config.Mode != TimingMaximum {
				delay := time.Duration(event.RecordedAtUnixNanos - previousRecordedAt)
				if delay < 0 {
					return result, r.failChild(command, stdin, fmt.Errorf("event %d recorded time regressed", event.Sequence))
				}
				if r.config.Mode == TimingAccelerated {
					delay = time.Duration(float64(delay) / r.config.Acceleration)
				}
				if err := r.wait(ctx, delay); err != nil {
					return result, r.failChild(command, stdin, err)
				}
			}
			previousRecordedAt = event.RecordedAtUnixNanos
			if err := encoder.Encode(EventMessage{Type: "event", Event: event}); err != nil {
				return result, r.failChild(command, stdin, err)
			}
			sent, haveSent = event.Sequence, true
			result.EventsSent++
			sentMetric.Store(result.EventsSent)
			for outstanding() >= r.config.MaxInFlight {
				if err := waitAck(); err != nil {
					return result, r.failChild(command, stdin, err)
				}
			}
			if r.config.StopAfterEvents > 0 && result.EventsSent >= r.config.StopAfterEvents {
				if err := encoder.Encode(StopMessage{Type: "stop", Reason: "configured_pause", LastSequence: sent}); err != nil {
					return result, r.failChild(command, stdin, err)
				}
				if err := drainAll(); err != nil {
					return result, r.failChild(command, stdin, err)
				}
				result.StopReason = "configured_pause"
				return result, r.closeChild(command, stdin)
			}
			continue
		}
		if !errors.Is(readErr, io.EOF) && !errors.Is(readErr, dataset.ErrIncompleteTail) {
			return result, r.failChild(command, stdin, readErr)
		}
		current, err := dataset.LoadManifest(r.config.DatasetRoot)
		if err != nil {
			return result, r.failChild(command, stdin, err)
		}
		if current.Finalized {
			if errors.Is(readErr, dataset.ErrIncompleteTail) {
				return result, r.failChild(command, stdin, fmt.Errorf("finalized dataset has incomplete tail"))
			}
			if current.Final == nil || !haveSent || sent < current.Final.FinalEventSequence {
				continue
			}
			result.DatasetEventsSHA256 = current.Final.EventsSHA256
			if err := encoder.Encode(EndMessage{Type: "end", FinalSequence: sent}); err != nil {
				return result, r.failChild(command, stdin, err)
			}
			if err := drainAll(); err != nil {
				return result, r.failChild(command, stdin, err)
			}
			result.StopReason = "dataset_finalized"
			return result, r.closeChild(command, stdin)
		}
		if !r.config.Follow {
			return result, r.failChild(command, stdin, fmt.Errorf("growing dataset requires follow mode"))
		}
		timer := time.NewTimer(r.config.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, r.failChild(command, stdin, ctx.Err())
		case value := <-ackCh:
			timer.Stop()
			if err := processResponse(value); err != nil {
				return result, r.failChild(command, stdin, err)
			}
		case <-timer.C:
			if r.config.HeartbeatInterval > 0 && time.Since(lastHeartbeat) >= r.config.HeartbeatInterval {
				if err := encoder.Encode(HeartbeatMessage{Type: "heartbeat", SentUnixNanos: time.Now().UnixNano()}); err != nil {
					return result, r.failChild(command, stdin, err)
				}
				lastHeartbeat = time.Now()
			}
		}
	}
}

func (r *Runner) logProgress(done <-chan struct{}, sent, acked *atomic.Uint64) {
	ticker := time.NewTicker(r.config.ProgressInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			s, a := sent.Load(), acked.Load()
			r.logger.Info("replay progress", "sent_events", s, "acknowledged_events", a, "outstanding_events", s-a)
		}
	}
}

func resolveHeight(root string, manifest dataset.Manifest, height uint64) (uint64, error) {
	file, err := os.Open(filepath.Join(root, "events.bin"))
	if err != nil {
		return 0, err
	}
	defer file.Close()
	reader, err := dataset.NewFrameReader(file)
	if err != nil {
		return 0, err
	}
	observed := map[string]uint64{}
	for {
		event, err := reader.Next(manifest.Limits.MaxEventPayloadBytes)
		if errors.Is(err, io.EOF) || errors.Is(err, dataset.ErrIncompleteTail) {
			break
		}
		if err != nil {
			return 0, err
		}
		switch event.Kind {
		case "block_observed":
			var p dataset.BlockEvent
			if err := json.Unmarshal(event.Payload, &p); err != nil {
				return 0, err
			}
			observed[p.Block.Hash] = event.Sequence
		case "block_connected":
			var p dataset.BlockEvent
			if err := json.Unmarshal(event.Payload, &p); err != nil {
				return 0, err
			}
			if p.Block.Height == height {
				if sequence, ok := observed[p.Block.Hash]; ok {
					return sequence, nil
				}
				return event.Sequence, nil
			}
		}
	}
	return 0, fmt.Errorf("height %d has no committed connection event", height)
}

func (r *Runner) failChild(command *exec.Cmd, stdin io.Closer, cause error) error {
	_ = stdin.Close()
	if command.Process != nil {
		_ = command.Process.Kill()
	}
	_ = command.Wait()
	return cause
}
func (r *Runner) closeChild(command *exec.Cmd, stdin io.Closer) error {
	if err := stdin.Close(); err != nil {
		return err
	}
	return command.Wait()
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
