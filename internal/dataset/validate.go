package dataset

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type ValidationReport struct {
	DatasetID        string   `json:"dataset_id"`
	Finalized        bool     `json:"finalized"`
	EventCount       uint64   `json:"event_count"`
	LastSequence     uint64   `json:"last_sequence"`
	FinalTip         ChainTip `json:"final_tip"`
	BlockObjectCount uint64   `json:"block_object_count"`
	IncompleteTail   bool     `json:"incomplete_tail"`
}

type validationState struct {
	manifest        Manifest
	tip             ChainTip
	nextSequence    uint64
	lastSequence    uint64
	observed        map[string]BlockRef
	finalEvent      *DatasetFinalizedEvent
	disconnectDepth uint64
}

func Validate(root string) (ValidationReport, error) {
	manifest, err := readManifest(filepath.Join(root, "manifest.json"))
	if err != nil {
		return ValidationReport{}, err
	}
	if err := validateManifest(manifest); err != nil {
		return ValidationReport{}, err
	}
	if err := rejectSymlinks(root); err != nil {
		return ValidationReport{}, err
	}

	logPath := filepath.Join(root, "events.bin")
	logFile, err := os.Open(logPath)
	if err != nil {
		return ValidationReport{}, fmt.Errorf("open event log: %w", err)
	}
	defer logFile.Close()
	state := validationState{manifest: manifest, tip: manifest.InitialAnchor, nextSequence: manifest.FirstEventSequence, observed: map[string]BlockRef{}}
	count, incomplete, err := ScanFrames(logFile, ScanOptions{MaxPayloadBytes: manifest.Limits.MaxEventPayloadBytes, AllowIncompleteTail: !manifest.Finalized}, func(event EventEnvelope) error {
		return validateEvent(root, &state, event)
	})
	if err != nil {
		return ValidationReport{}, err
	}
	if manifest.Finalized && incomplete {
		return ValidationReport{}, fmt.Errorf("finalized dataset has incomplete trailing frame")
	}
	if manifest.Finalized {
		if state.finalEvent == nil {
			return ValidationReport{}, fmt.Errorf("finalized dataset has no dataset_finalized event")
		}
		if err := validateFinal(root, manifest, state, count); err != nil {
			return ValidationReport{}, err
		}
	}
	return ValidationReport{DatasetID: manifest.DatasetID, Finalized: manifest.Finalized, EventCount: count, LastSequence: state.lastSequence, FinalTip: state.tip, BlockObjectCount: uint64(len(state.observed)), IncompleteTail: incomplete}, nil
}

func validateEvent(root string, state *validationState, event EventEnvelope) error {
	if state.finalEvent != nil {
		return fmt.Errorf("event %d appears after dataset_finalized", event.Sequence)
	}
	if event.SchemaMajor != SchemaMajor || event.SchemaMinor > SchemaMinor {
		return fmt.Errorf("event %d uses unsupported schema %d.%d", event.Sequence, event.SchemaMajor, event.SchemaMinor)
	}
	if event.DatasetID != state.manifest.DatasetID {
		return fmt.Errorf("event %d dataset ID mismatch", event.Sequence)
	}
	if event.Network != state.manifest.Network {
		return fmt.Errorf("event %d network mismatch", event.Sequence)
	}
	if event.Sequence != state.nextSequence {
		return fmt.Errorf("expected event sequence %d, got %d", state.nextSequence, event.Sequence)
	}
	digest := sha256.Sum256(event.Payload)
	if hex.EncodeToString(digest[:]) != event.PayloadSHA256 {
		return fmt.Errorf("event %d payload digest mismatch", event.Sequence)
	}
	state.lastSequence = event.Sequence
	state.nextSequence++
	switch event.Kind {
	case "source_started", "source_stopped", "source_unavailable", "source_recovered":
		return validateSourceEvent(event)
	case "block_observed":
		var value BlockEvent
		if err := strictJSON(event.Payload, &value); err != nil {
			return fmt.Errorf("event %d block_observed: %w", event.Sequence, err)
		}
		if _, exists := state.observed[value.Block.Hash]; exists {
			return fmt.Errorf("event %d observes duplicate block %s", event.Sequence, value.Block.Hash)
		}
		if err := validateBlockObject(root, state.manifest, value.Block); err != nil {
			return fmt.Errorf("event %d: %w", event.Sequence, err)
		}
		state.observed[value.Block.Hash] = value.Block
	case "block_connected":
		var value BlockEvent
		if err := strictJSON(event.Payload, &value); err != nil {
			return fmt.Errorf("event %d block_connected: %w", event.Sequence, err)
		}
		observed, exists := state.observed[value.Block.Hash]
		if !exists {
			return fmt.Errorf("event %d connects unobserved block %s", event.Sequence, value.Block.Hash)
		}
		if observed != value.Block {
			return fmt.Errorf("event %d block reference differs from observed block", event.Sequence)
		}
		if value.Block.ParentHash != state.tip.Hash || value.Block.Height != state.tip.Height+1 {
			return fmt.Errorf("event %d cannot connect block %d:%s to tip %d:%s", event.Sequence, value.Block.Height, value.Block.Hash, state.tip.Height, state.tip.Hash)
		}
		state.tip = ChainTip{Height: value.Block.Height, Hash: value.Block.Hash}
	case "block_disconnected":
		var value BlockEvent
		if err := strictJSON(event.Payload, &value); err != nil {
			return fmt.Errorf("event %d block_disconnected: %w", event.Sequence, err)
		}
		if state.tip.Hash != value.Block.Hash || state.tip.Height != value.Block.Height {
			return fmt.Errorf("event %d disconnects non-tip block", event.Sequence)
		}
		observed, exists := state.observed[value.Block.Hash]
		if !exists || observed != value.Block {
			return fmt.Errorf("event %d disconnect reference differs from observed block", event.Sequence)
		}
		if value.Block.Height == 0 {
			return fmt.Errorf("event %d attempts to disconnect height zero", event.Sequence)
		}
		state.disconnectDepth++
		if state.disconnectDepth > state.manifest.Limits.MaxReorgDepth {
			return fmt.Errorf("event %d exceeds maximum reorganization depth %d", event.Sequence, state.manifest.Limits.MaxReorgDepth)
		}
		state.tip = ChainTip{Height: value.Block.Height - 1, Hash: value.Block.ParentHash}
	case "tip_changed":
		var value TipEvent
		if err := strictJSON(event.Payload, &value); err != nil {
			return fmt.Errorf("event %d tip_changed: %w", event.Sequence, err)
		}
		if value.Tip != state.tip {
			return fmt.Errorf("event %d declares tip %v, derived tip is %v", event.Sequence, value.Tip, state.tip)
		}
		state.disconnectDepth = 0
	case "dataset_finalized":
		if state.finalEvent != nil {
			return fmt.Errorf("event %d duplicates dataset_finalized", event.Sequence)
		}
		var value DatasetFinalizedEvent
		if err := strictJSON(event.Payload, &value); err != nil {
			return fmt.Errorf("event %d dataset_finalized: %w", event.Sequence, err)
		}
		if value.FinalSequence != event.Sequence || value.FinalTip != state.tip {
			return fmt.Errorf("event %d final state mismatch", event.Sequence)
		}
		state.finalEvent = &value
	default:
		return fmt.Errorf("event %d has unknown kind %q", event.Sequence, event.Kind)
	}
	return nil
}

func validateBlockObject(root string, manifest Manifest, block BlockRef) error {
	if err := validateHash(block.Hash); err != nil {
		return err
	}
	if err := validateHash(block.ParentHash); err != nil {
		return err
	}
	wantKey, _ := BlockContentKey(block.Hash)
	if block.ContentKey != wantKey {
		return fmt.Errorf("block %s content key %q, want %q", block.Hash, block.ContentKey, wantKey)
	}
	path := filepath.Join(root, filepath.FromSlash(wantKey))
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat block %s: %w", block.Hash, err)
	}
	if uint64(info.Size()) > manifest.Limits.MaxRawBlockBytes {
		return fmt.Errorf("block %s exceeds maximum size", block.Hash)
	}
	if uint64(info.Size()) != block.RawPayloadLength {
		return fmt.Errorf("block %s length %d, want %d", block.Hash, info.Size(), block.RawPayloadLength)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read block %s: %w", block.Hash, err)
	}
	if uint64(len(raw)) != block.RawPayloadLength {
		return fmt.Errorf("block %s length %d, want %d", block.Hash, len(raw), block.RawPayloadLength)
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != block.RawPayloadSHA256 {
		return fmt.Errorf("block %s payload digest mismatch", block.Hash)
	}
	parsedHash, parsedParent, parsedTime, err := ParseBlockIdentity(raw)
	if err != nil {
		return fmt.Errorf("parse block %s: %w", block.Hash, err)
	}
	if parsedHash != block.Hash || parsedParent != block.ParentHash || parsedTime != block.BlockTimeUnixSeconds {
		return fmt.Errorf("block %s parsed metadata mismatch: hash=%s parent=%s time=%d", block.Hash, parsedHash, parsedParent, parsedTime)
	}
	return nil
}

func validateSourceEvent(event EventEnvelope) error {
	switch event.Kind {
	case "source_started":
		var value SourceStartedEvent
		if err := strictJSON(event.Payload, &value); err != nil {
			return err
		}
		if value.SessionID == "" || value.RecorderVersion == "" || value.ZebraEndpointID == "" {
			return fmt.Errorf("source_started fields must be non-empty")
		}
	case "source_stopped":
		var value SourceStoppedEvent
		if err := strictJSON(event.Payload, &value); err != nil {
			return err
		}
		if value.SessionID == "" || value.Reason == "" {
			return fmt.Errorf("source_stopped fields must be non-empty")
		}
	case "source_unavailable":
		var value SourceUnavailableEvent
		if err := strictJSON(event.Payload, &value); err != nil {
			return err
		}
		valid := map[string]bool{"connection": true, "timeout": true, "rate_limit": true, "invalid_response": true, "source_not_ready": true}
		if !valid[value.Category] {
			return fmt.Errorf("invalid source failure category %q", value.Category)
		}
	case "source_recovered":
		var value SourceRecoveredEvent
		if err := strictJSON(event.Payload, &value); err != nil {
			return err
		}
	}
	return nil
}

func validateFinal(root string, manifest Manifest, state validationState, count uint64) error {
	final := manifest.Final
	if final == nil {
		return fmt.Errorf("finalized manifest has no final section")
	}
	if final.EventCount != count || final.FinalEventSequence != state.lastSequence || final.FinalTip != state.tip {
		return fmt.Errorf("manifest final event state does not match log")
	}
	if state.finalEvent.FinalSequence != state.lastSequence || state.finalEvent.FinalTip != state.tip {
		return fmt.Errorf("dataset_finalized event does not match derived state")
	}
	logPath := filepath.Join(root, "events.bin")
	info, err := os.Stat(logPath)
	if err != nil {
		return err
	}
	if uint64(info.Size()) != final.EventsByteLength {
		return fmt.Errorf("event log length %d, want %d", info.Size(), final.EventsByteLength)
	}
	logHash, err := hashFile(logPath)
	if err != nil {
		return err
	}
	if logHash != final.EventsSHA256 {
		return fmt.Errorf("event log SHA-256 mismatch")
	}
	blockSet, err := calculateBlockSet(state.observed)
	if err != nil {
		return err
	}
	if uint64(len(state.observed)) != final.BlockObjectCount || blockSet != final.BlockSetSHA256 {
		return fmt.Errorf("manifest block set does not match events")
	}
	if state.finalEvent.BlockObjectCount != final.BlockObjectCount || state.finalEvent.BlockSetSHA256 != final.BlockSetSHA256 {
		return fmt.Errorf("dataset_finalized block set does not match manifest")
	}
	actualObjects, err := listRawObjects(filepath.Join(root, "blocks"))
	if err != nil {
		return err
	}
	if uint64(len(actualObjects)) != final.BlockObjectCount {
		return fmt.Errorf("found %d raw block objects, want %d", len(actualObjects), final.BlockObjectCount)
	}
	for hash, ref := range state.observed {
		want, _ := BlockContentKey(hash)
		if !actualObjects[filepath.ToSlash(want)] {
			return fmt.Errorf("block object for %s is missing", ref.Hash)
		}
	}
	return nil
}

func calculateBlockSet(blocks map[string]BlockRef) (string, error) {
	lines := make([]string, 0, len(blocks))
	for hash, block := range blocks {
		lines = append(lines, fmt.Sprintf("%s %d %s\n", hash, block.RawPayloadLength, block.RawPayloadSHA256))
	}
	sort.Strings(lines)
	digest := sha256.Sum256([]byte(strings.Join(lines, "")))
	return hex.EncodeToString(digest[:]), nil
}

func readManifest(path string) (Manifest, error) {
	file, err := os.Open(path)
	if err != nil {
		return Manifest{}, fmt.Errorf("open manifest: %w", err)
	}
	defer file.Close()
	var m Manifest
	if err := strictJSONReader(file, &m); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	return m, nil
}

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func validateManifest(m Manifest) error {
	if m.Format != "zsync-bench-dataset" || m.SchemaMajor != SchemaMajor || m.SchemaMinor > SchemaMinor {
		return fmt.Errorf("unsupported manifest format or schema")
	}
	if m.Network != Network {
		return fmt.Errorf("unsupported network %q", m.Network)
	}
	if !uuidPattern.MatchString(m.DatasetID) {
		return fmt.Errorf("invalid dataset ID")
	}
	if err := validateHash(m.InitialAnchor.Hash); err != nil {
		return fmt.Errorf("invalid initial anchor: %w", err)
	}
	if m.Limits.MaxEventPayloadBytes == 0 || m.Limits.MaxRawBlockBytes == 0 || m.Limits.MaxReorgDepth == 0 {
		return fmt.Errorf("manifest limits must be positive")
	}
	if m.Limits.MaxEventPayloadBytes > 16<<20 {
		return fmt.Errorf("maximum event payload limit exceeds hard cap")
	}
	if m.Limits.MaxRawBlockBytes > 2<<20 {
		return fmt.Errorf("maximum raw block limit exceeds Zcash network maximum")
	}
	if m.Finalized != (m.Final != nil) {
		return fmt.Errorf("manifest finalized flag and final section disagree")
	}
	return nil
}
func strictJSON(data []byte, target any) error {
	return strictJSONReader(bytes.NewReader(data), target)
}
func strictJSONReader(r io.Reader, target any) error {
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("trailing JSON value")
	}
	return nil
}
func hashFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func rejectSymlinks(root string) error {
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("dataset contains symlink %s", path)
		}
		return nil
	})
}
func listRawObjects(root string) (map[string]bool, error) {
	result := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".raw" {
			return fmt.Errorf("unexpected file in blocks directory: %s", path)
		}
		relative, err := filepath.Rel(filepath.Dir(root), path)
		if err != nil {
			return err
		}
		result[filepath.ToSlash(relative)] = true
		return nil
	})
	return result, err
}
