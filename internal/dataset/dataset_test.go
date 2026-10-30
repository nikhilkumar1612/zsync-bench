package dataset

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var (
	testDatasetID = "123e4567-e89b-12d3-a456-426614174000"
	testTime      = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	testLimits    = Limits{MaxEventPayloadBytes: 1 << 20, MaxRawBlockBytes: 2 << 20, MaxReorgDepth: 100}
)

func TestValidateLinearFinalizedDataset(t *testing.T) {
	root := t.TempDir()
	anchor := ChainTip{Height: 100, Hash: zeroHash()}
	writer, err := Create(root, testDatasetID, anchor, testLimits, testTime)
	must(t, err)
	defer writer.Close()
	_, err = writer.Append("source_started", SourceStartedEvent{SessionID: "s1", RecorderVersion: "test", ZebraEndpointID: "fixture"}, testTime)
	must(t, err)
	aRaw := syntheticBlock(t, anchor.Hash, 1, testTime.Unix())
	a, err := writer.ObserveBlock(aRaw, 101, testTime.Unix(), testTime)
	must(t, err)
	_, err = writer.Append("block_connected", BlockEvent{Block: a}, testTime)
	must(t, err)
	_, err = writer.Append("tip_changed", TipEvent{Tip: ChainTip{Height: a.Height, Hash: a.Hash}}, testTime)
	must(t, err)
	bRaw := syntheticBlock(t, a.Hash, 2, testTime.Unix()+1)
	b, err := writer.ObserveBlock(bRaw, 102, testTime.Unix()+1, testTime)
	must(t, err)
	_, err = writer.Append("block_connected", BlockEvent{Block: b}, testTime)
	must(t, err)
	_, err = writer.Append("tip_changed", TipEvent{Tip: ChainTip{Height: b.Height, Hash: b.Hash}}, testTime)
	must(t, err)
	must(t, writer.Finalize(ChainTip{Height: b.Height, Hash: b.Hash}, testTime))
	report, err := Validate(root)
	must(t, err)
	if !report.Finalized || report.EventCount != 8 || report.FinalTip.Hash != b.Hash || report.BlockObjectCount != 2 {
		t.Fatalf("unexpected report: %+v", report)
	}
}

func TestValidateReorganization(t *testing.T) {
	root := t.TempDir()
	anchor := ChainTip{Height: 20, Hash: zeroHash()}
	writer, err := Create(root, testDatasetID, anchor, testLimits, testTime)
	must(t, err)
	a, err := writer.ObserveBlock(syntheticBlock(t, anchor.Hash, 10, testTime.Unix()), 21, testTime.Unix(), testTime)
	must(t, err)
	appendEvent(t, writer, "block_connected", BlockEvent{Block: a})
	b, err := writer.ObserveBlock(syntheticBlock(t, a.Hash, 11, testTime.Unix()+1), 22, testTime.Unix()+1, testTime)
	must(t, err)
	appendEvent(t, writer, "block_connected", BlockEvent{Block: b})
	x, err := writer.ObserveBlock(syntheticBlock(t, a.Hash, 12, testTime.Unix()+2), 22, testTime.Unix()+2, testTime)
	must(t, err)
	appendEvent(t, writer, "block_disconnected", BlockEvent{Block: b})
	appendEvent(t, writer, "block_connected", BlockEvent{Block: x})
	appendEvent(t, writer, "tip_changed", TipEvent{Tip: ChainTip{Height: x.Height, Hash: x.Hash}})
	must(t, writer.Finalize(ChainTip{Height: x.Height, Hash: x.Hash}, testTime))
	report, err := Validate(root)
	must(t, err)
	if report.FinalTip.Hash != x.Hash {
		t.Fatalf("tip %s, want %s", report.FinalTip.Hash, x.Hash)
	}
}

func TestLiveDatasetAllowsIncompleteTail(t *testing.T) {
	root := t.TempDir()
	writer, err := Create(root, testDatasetID, ChainTip{Height: 0, Hash: zeroHash()}, testLimits, testTime)
	must(t, err)
	appendEvent(t, writer, "source_started", SourceStartedEvent{SessionID: "s", RecorderVersion: "test", ZebraEndpointID: "fixture"})
	must(t, writer.Close())
	file, err := os.OpenFile(filepath.Join(root, "events.bin"), os.O_APPEND|os.O_WRONLY, 0)
	must(t, err)
	_, err = file.Write([]byte{1, 2, 3})
	must(t, err)
	must(t, file.Close())
	report, err := Validate(root)
	must(t, err)
	if !report.IncompleteTail {
		t.Fatal("expected incomplete tail")
	}
}

func TestFinalizedDatasetRejectsTruncation(t *testing.T) {
	root, _ := makeOneBlockDataset(t)
	path := filepath.Join(root, "events.bin")
	info, err := os.Stat(path)
	must(t, err)
	must(t, os.Truncate(path, info.Size()-1))
	if _, err := Validate(root); err == nil {
		t.Fatal("expected truncation error")
	}
}

func TestFinalizedDatasetRejectsFrameCorruption(t *testing.T) {
	root, _ := makeOneBlockDataset(t)
	path := filepath.Join(root, "events.bin")
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	must(t, err)
	_, err = file.WriteAt([]byte{0xff}, 40)
	must(t, err)
	must(t, file.Close())
	if _, err := Validate(root); err == nil {
		t.Fatal("expected frame corruption error")
	}
}

func TestDatasetRejectsCorruptBlock(t *testing.T) {
	root, block := makeOneBlockDataset(t)
	path := filepath.Join(root, filepath.FromSlash(block.ContentKey))
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	must(t, err)
	_, err = file.WriteAt([]byte{0xff}, 10)
	must(t, err)
	must(t, file.Close())
	if _, err := Validate(root); err == nil {
		t.Fatal("expected corrupt block error")
	}
}

func TestRejectsForgedDisconnectReference(t *testing.T) {
	root := t.TempDir()
	anchor := ChainTip{Height: 10, Hash: zeroHash()}
	writer, err := Create(root, testDatasetID, anchor, testLimits, testTime)
	must(t, err)
	block, err := writer.ObserveBlock(syntheticBlock(t, anchor.Hash, 1, testTime.Unix()), 11, testTime.Unix(), testTime)
	must(t, err)
	appendEvent(t, writer, "block_connected", BlockEvent{Block: block})
	forged := block
	forged.ParentHash = strings.Repeat("1", 64)
	appendEvent(t, writer, "block_disconnected", BlockEvent{Block: forged})
	must(t, writer.Close())
	if _, err := Validate(root); err == nil {
		t.Fatal("expected forged disconnect reference error")
	}
}

func TestRejectsReorganizationBeyondLimit(t *testing.T) {
	root := t.TempDir()
	limits := testLimits
	limits.MaxReorgDepth = 1
	anchor := ChainTip{Height: 10, Hash: zeroHash()}
	writer, err := Create(root, testDatasetID, anchor, limits, testTime)
	must(t, err)
	a, err := writer.ObserveBlock(syntheticBlock(t, anchor.Hash, 1, testTime.Unix()), 11, testTime.Unix(), testTime)
	must(t, err)
	appendEvent(t, writer, "block_connected", BlockEvent{Block: a})
	b, err := writer.ObserveBlock(syntheticBlock(t, a.Hash, 2, testTime.Unix()+1), 12, testTime.Unix()+1, testTime)
	must(t, err)
	appendEvent(t, writer, "block_connected", BlockEvent{Block: b})
	appendEvent(t, writer, "block_disconnected", BlockEvent{Block: b})
	appendEvent(t, writer, "block_disconnected", BlockEvent{Block: a})
	must(t, writer.Close())
	if _, err := Validate(root); err == nil {
		t.Fatal("expected maximum reorganization depth error")
	}
}

func TestRejectsInvalidSourceEvent(t *testing.T) {
	root := t.TempDir()
	writer, err := Create(root, testDatasetID, ChainTip{Height: 0, Hash: zeroHash()}, testLimits, testTime)
	must(t, err)
	appendEvent(t, writer, "source_started", SourceStartedEvent{})
	must(t, writer.Close())
	if _, err := Validate(root); err == nil {
		t.Fatal("expected invalid source event error")
	}
}

func TestRejectsInvalidEquihashSolutionLength(t *testing.T) {
	raw := syntheticBlock(t, zeroHash(), 1, testTime.Unix())
	raw[142] = 0
	if _, _, _, err := ParseBlockIdentity(raw); err == nil {
		t.Fatal("expected invalid Equihash solution length error")
	}
}

func makeOneBlockDataset(t *testing.T) (string, BlockRef) {
	t.Helper()
	root := t.TempDir()
	anchor := ChainTip{Height: 1, Hash: zeroHash()}
	writer, err := Create(root, testDatasetID, anchor, testLimits, testTime)
	must(t, err)
	block, err := writer.ObserveBlock(syntheticBlock(t, anchor.Hash, 1, testTime.Unix()), 2, testTime.Unix(), testTime)
	must(t, err)
	appendEvent(t, writer, "block_connected", BlockEvent{Block: block})
	appendEvent(t, writer, "tip_changed", TipEvent{Tip: ChainTip{Height: block.Height, Hash: block.Hash}})
	must(t, writer.Finalize(ChainTip{Height: block.Height, Hash: block.Hash}, testTime))
	return root, block
}
func appendEvent(t *testing.T, w *Writer, kind string, payload any) {
	t.Helper()
	_, err := w.Append(kind, payload, testTime)
	must(t, err)
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func zeroHash() string { return strings.Repeat("0", 64) }

func syntheticBlock(t *testing.T, parent string, nonce byte, blockTime int64) []byte {
	t.Helper()
	parentBytes, err := hex.DecodeString(parent)
	must(t, err)
	raw := make([]byte, 1488)
	raw[0] = 4
	for i := 0; i < 32; i++ {
		raw[4+i] = parentBytes[31-i]
	}
	raw[100] = byte(blockTime)
	raw[101] = byte(blockTime >> 8)
	raw[102] = byte(blockTime >> 16)
	raw[103] = byte(blockTime >> 24)
	raw[104] = 2
	raw[108] = 3
	raw[139] = nonce
	raw[140] = 0xfd
	raw[141] = 0x40
	raw[142] = 0x05
	raw[1487] = 0
	return raw
}
func expectedSyntheticHash(raw []byte) string {
	first := sha256.Sum256(raw[:1487])
	second := sha256.Sum256(first[:])
	reversed := make([]byte, 32)
	for i := range second {
		reversed[31-i] = second[i]
	}
	return hex.EncodeToString(reversed)
}
