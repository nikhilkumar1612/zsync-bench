package recorder

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/nikhilkumar1612/zsync-bench/internal/dataset"
)

type checkpoint struct {
	DatasetID    string           `json:"dataset_id"`
	NextSequence uint64           `json:"next_sequence"`
	CanonicalTip dataset.ChainTip `json:"canonical_tip"`
	UpdatedAt    string           `json:"updated_at"`
}

func writeCheckpoint(root string, writer *dataset.Writer, tip dataset.ChainTip, now time.Time) error {
	dir := filepath.Join(root, "state")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	value := checkpoint{DatasetID: writer.DatasetID(), NextSequence: writer.NextSequence(), CanonicalTip: tip, UpdatedAt: now.UTC().Format(time.RFC3339Nano)}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	temp, err := os.CreateTemp(dir, ".checkpoint-")
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
	if err := os.Rename(name, filepath.Join(dir, "recorder-checkpoint.json")); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync checkpoint directory: %w", err)
	}
	return nil
}
