// zsync-replay-sink is a diagnostic consumer for testing the replay transport.
// It validates event identity and durably checkpoints sequence progress, but it
// intentionally does not construct compact blocks or act as an indexer.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nikhilkumar1612/zsync-bench/internal/replay"
)

type sinkState struct {
	DatasetID string `json:"dataset_id"`
	Sequence  uint64 `json:"sequence"`
}

func main() {
	statePath := flag.String("state", "", "durable sink progress file")
	ackEvery := flag.Uint64("ack-every", 100, "durably acknowledge after this many events")
	flag.Parse()
	if *statePath == "" || *ackEvery == 0 {
		fmt.Fprintln(os.Stderr, "--state and positive --ack-every are required")
		os.Exit(2)
	}
	decoder, encoder := replay.NewDecoder(os.Stdin, replay.DefaultMaxFrameBytes), replay.NewEncoder(os.Stdout)
	message, err := decoder.DecodeRequest()
	if err != nil {
		fail(encoder, "handshake_decode", err)
	}
	handshake, ok := message.(*replay.Handshake)
	if !ok || handshake.ProtocolMajor != replay.ProtocolMajor || handshake.EventSchemaMajor != 1 || handshake.Network != "zcash-testnet" {
		fail(encoder, "handshake_unsupported", fmt.Errorf("unsupported handshake"))
	}
	if err := verifyStart(*statePath, handshake); err != nil {
		fail(encoder, "resume_mismatch", err)
	}
	if err := encoder.Encode(replay.ReadyMessage{Type: "ready", ProtocolMajor: replay.ProtocolMajor, ProtocolMinor: replay.ProtocolMinor}); err != nil {
		os.Exit(1)
	}

	expected := handshake.RequestedSequence
	var last uint64
	var pending uint64
	for {
		message, err := decoder.DecodeRequest()
		if err != nil {
			fail(encoder, "transport_read", err)
		}
		switch value := message.(type) {
		case *replay.EventMessage:
			event := value.Event
			if event.DatasetID != handshake.DatasetID || event.Network != handshake.Network || event.Sequence != expected {
				fail(encoder, "event_identity", fmt.Errorf("unexpected event sequence %d, want %d", event.Sequence, expected))
			}
			digest := sha256.Sum256(event.Payload)
			if hex.EncodeToString(digest[:]) != event.PayloadSHA256 {
				fail(encoder, "payload_checksum", fmt.Errorf("event %d payload checksum mismatch", event.Sequence))
			}
			last, expected, pending = event.Sequence, event.Sequence+1, pending+1
			if pending >= *ackEvery {
				persistAndAck(encoder, *statePath, handshake.DatasetID, last)
				pending = 0
			}
		case *replay.EndMessage:
			if expected == handshake.RequestedSequence || value.FinalSequence != last {
				fail(encoder, "end_mismatch", fmt.Errorf("unexpected final sequence"))
			}
			if pending > 0 {
				persistAndAck(encoder, *statePath, handshake.DatasetID, last)
			}
			return
		default:
			fail(encoder, "message_type", fmt.Errorf("unexpected message"))
		}
	}
}

func verifyStart(path string, handshake *replay.Handshake) error {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		if handshake.RequestedSequence != handshake.FirstSequence {
			return fmt.Errorf("missing state for non-initial sequence %d", handshake.RequestedSequence)
		}
		return nil
	}
	if err != nil {
		return err
	}
	var state sinkState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	if state.DatasetID != handshake.DatasetID || state.Sequence == ^uint64(0) || state.Sequence+1 != handshake.RequestedSequence {
		return fmt.Errorf("sink state does not precede requested sequence")
	}
	return nil
}

func persistAndAck(encoder *replay.Encoder, path, datasetID string, sequence uint64) {
	data, _ := json.MarshalIndent(sinkState{DatasetID: datasetID, Sequence: sequence}, "", "  ")
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		fail(encoder, "state_write", err)
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".sink-")
	if err != nil {
		fail(encoder, "state_write", err)
	}
	name := temp.Name()
	defer os.Remove(name)
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		fail(encoder, "state_write", err)
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		fail(encoder, "state_write", err)
	}
	if err := temp.Close(); err != nil {
		fail(encoder, "state_write", err)
	}
	if err := os.Rename(name, path); err != nil {
		fail(encoder, "state_write", err)
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		fail(encoder, "state_write", err)
	}
	if err := directory.Sync(); err != nil {
		directory.Close()
		fail(encoder, "state_write", err)
	}
	if err := directory.Close(); err != nil {
		fail(encoder, "state_write", err)
	}
	if err := encoder.Encode(replay.AckMessage{Type: "ack", Sequence: sequence}); err != nil {
		os.Exit(1)
	}
}

func fail(encoder *replay.Encoder, code string, err error) {
	_ = encoder.Encode(replay.ErrorMessage{Type: "error", Fatal: true, Code: code, Message: err.Error()})
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
