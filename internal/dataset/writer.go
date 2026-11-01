package dataset

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type Writer struct {
	root     string
	manifest Manifest
	log      *os.File
	next     uint64
	observed map[string]BlockRef
	closed   bool
}

type ResumeState struct {
	Manifest     Manifest
	Tip          ChainTip
	Canonical    map[uint64]BlockRef
	LastSequence uint64
}

func Create(root, datasetID string, anchor ChainTip, limits Limits, createdAt time.Time) (*Writer, error) {
	if err := os.MkdirAll(filepath.Join(root, "blocks"), 0o755); err != nil {
		return nil, err
	}
	manifest := Manifest{Format: "zsync-bench-dataset", SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor, DatasetID: datasetID, Network: Network, CreatedAt: createdAt.UTC().Format(time.RFC3339Nano), Finalized: false, InitialAnchor: anchor, FirstEventSequence: 0, Limits: limits}
	if err := writeJSONAtomic(filepath.Join(root, "manifest.json"), manifest); err != nil {
		return nil, err
	}
	logFile, err := os.OpenFile(filepath.Join(root, "events.bin"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	if err := WriteFileHeader(logFile); err != nil {
		logFile.Close()
		return nil, err
	}
	if err := logFile.Sync(); err != nil {
		logFile.Close()
		return nil, err
	}
	if err := syncDir(root); err != nil {
		logFile.Close()
		return nil, err
	}
	return &Writer{root: root, manifest: manifest, log: logFile, next: manifest.FirstEventSequence, observed: map[string]BlockRef{}}, nil
}

func OpenAppend(root string) (*Writer, ResumeState, error) {
	manifest, err := readManifest(filepath.Join(root, "manifest.json"))
	if err != nil {
		return nil, ResumeState{}, err
	}
	if err := validateManifest(manifest); err != nil {
		return nil, ResumeState{}, err
	}
	if manifest.Finalized {
		return nil, ResumeState{}, fmt.Errorf("cannot append to finalized dataset")
	}
	if err := rejectSymlinks(root); err != nil {
		return nil, ResumeState{}, err
	}
	logPath := filepath.Join(root, "events.bin")
	file, err := os.OpenFile(logPath, os.O_RDWR, 0)
	if err != nil {
		return nil, ResumeState{}, err
	}
	state := validationState{manifest: manifest, tip: manifest.InitialAnchor, nextSequence: manifest.FirstEventSequence, observed: map[string]BlockRef{}, canonical: map[uint64]BlockRef{}}
	_, incomplete, offset, err := scanFrames(file, ScanOptions{MaxPayloadBytes: manifest.Limits.MaxEventPayloadBytes, AllowIncompleteTail: true}, func(event EventEnvelope) error { return validateEvent(root, &state, event) })
	if err != nil {
		file.Close()
		return nil, ResumeState{}, err
	}
	if incomplete {
		if err := file.Truncate(offset); err != nil {
			file.Close()
			return nil, ResumeState{}, err
		}
		if err := file.Sync(); err != nil {
			file.Close()
			return nil, ResumeState{}, err
		}
	}
	if _, err := file.Seek(0, 2); err != nil {
		file.Close()
		return nil, ResumeState{}, err
	}
	canonical := make(map[uint64]BlockRef, len(state.canonical))
	for height, block := range state.canonical {
		canonical[height] = block
	}
	writer := &Writer{root: root, manifest: manifest, log: file, next: state.nextSequence, observed: state.observed}
	return writer, ResumeState{Manifest: manifest, Tip: state.tip, Canonical: canonical, LastSequence: state.lastSequence}, nil
}

func (w *Writer) NextSequence() uint64 { return w.next }
func (w *Writer) DatasetID() string    { return w.manifest.DatasetID }

func (w *Writer) Append(kind string, payload any, recordedAt time.Time) (EventEnvelope, error) {
	if w.closed {
		return EventEnvelope{}, fmt.Errorf("dataset writer is closed")
	}
	event, err := NewEnvelope(w.manifest.DatasetID, w.next, recordedAt.UnixNano(), kind, payload)
	if err != nil {
		return EventEnvelope{}, err
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return EventEnvelope{}, err
	}
	if len(encoded) > int(w.manifest.Limits.MaxEventPayloadBytes) {
		return EventEnvelope{}, fmt.Errorf("event payload exceeds configured limit")
	}
	if err := WriteFrame(w.log, event); err != nil {
		return EventEnvelope{}, err
	}
	if err := w.log.Sync(); err != nil {
		return EventEnvelope{}, err
	}
	w.next++
	return event, nil
}

func (w *Writer) ObserveBlock(raw []byte, height uint64, blockTime int64, recordedAt time.Time) (BlockRef, error) {
	if uint64(len(raw)) > w.manifest.Limits.MaxRawBlockBytes {
		return BlockRef{}, fmt.Errorf("raw block exceeds configured limit")
	}
	hash, parent, parsedTime, err := ParseBlockIdentity(raw)
	if err != nil {
		return BlockRef{}, err
	}
	if parsedTime != blockTime {
		return BlockRef{}, fmt.Errorf("block timestamp %d, metadata says %d", parsedTime, blockTime)
	}
	key, _ := BlockContentKey(hash)
	digest := sha256.Sum256(raw)
	ref := BlockRef{Height: height, Hash: hash, ParentHash: parent, BlockTimeUnixSeconds: blockTime, ContentKey: key, RawPayloadSHA256: hex.EncodeToString(digest[:]), RawPayloadLength: uint64(len(raw))}
	if existing, ok := w.observed[hash]; ok {
		if existing != ref {
			return BlockRef{}, fmt.Errorf("observed block %s metadata differs from existing record", hash)
		}
		return existing, nil
	}
	path := filepath.Join(w.root, filepath.FromSlash(key))
	if err := writeBytesAtomic(path, raw); err != nil {
		return BlockRef{}, err
	}
	if _, err := w.Append("block_observed", BlockEvent{Block: ref}, recordedAt); err != nil {
		return BlockRef{}, err
	}
	w.observed[hash] = ref
	return ref, nil
}

func (w *Writer) Finalize(tip ChainTip, finalizedAt time.Time) error {
	if w.closed {
		return fmt.Errorf("dataset writer is closed")
	}
	blockSet, _ := calculateBlockSet(w.observed)
	eventPayload := DatasetFinalizedEvent{FinalSequence: w.next, FinalTip: tip, BlockObjectCount: uint64(len(w.observed)), BlockSetSHA256: blockSet}
	if _, err := w.Append("dataset_finalized", eventPayload, finalizedAt); err != nil {
		return err
	}
	if err := w.log.Close(); err != nil {
		return err
	}
	w.closed = true
	logPath := filepath.Join(w.root, "events.bin")
	info, err := os.Stat(logPath)
	if err != nil {
		return err
	}
	logHash, err := hashFile(logPath)
	if err != nil {
		return err
	}
	w.manifest.Finalized = true
	w.manifest.FinalizedAt = finalizedAt.UTC().Format(time.RFC3339Nano)
	w.manifest.Final = &FinalManifest{EventCount: w.next - w.manifest.FirstEventSequence, FinalEventSequence: w.next - 1, FinalTip: tip, EventsByteLength: uint64(info.Size()), EventsSHA256: logHash, BlockObjectCount: uint64(len(w.observed)), BlockSetSHA256: blockSet}
	return writeJSONAtomic(filepath.Join(w.root, "manifest.json"), w.manifest)
}

func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	return w.log.Close()
}

func writeBytesAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".tmp-block-")
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
	if existing, err := os.ReadFile(path); err == nil {
		if !equalBytes(existing, data) {
			return fmt.Errorf("existing block object differs: %s", path)
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}
func writeJSONAtomic(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeReplaceAtomic(path, data)
}
func writeReplaceAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".tmp-manifest-")
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
	return syncDir(filepath.Dir(path))
}

func syncDir(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
