package dataset

import "encoding/json"

const (
	SchemaMajor = 1
	SchemaMinor = 0
	Network     = "zcash-testnet"
)

type ChainTip struct {
	Height uint64 `json:"height"`
	Hash   string `json:"hash"`
}

type Limits struct {
	MaxEventPayloadBytes uint32 `json:"max_event_payload_bytes"`
	MaxRawBlockBytes     uint64 `json:"max_raw_block_bytes"`
	MaxReorgDepth        uint64 `json:"max_reorg_depth"`
}

type FinalManifest struct {
	EventCount         uint64   `json:"event_count"`
	FinalEventSequence uint64   `json:"final_event_sequence"`
	FinalTip           ChainTip `json:"final_tip"`
	EventsByteLength   uint64   `json:"events_byte_length"`
	EventsSHA256       string   `json:"events_sha256"`
	BlockObjectCount   uint64   `json:"block_object_count"`
	BlockSetSHA256     string   `json:"block_set_sha256"`
}

type Manifest struct {
	Format             string         `json:"format"`
	SchemaMajor        uint32         `json:"schema_major"`
	SchemaMinor        uint32         `json:"schema_minor"`
	DatasetID          string         `json:"dataset_id"`
	ParentDatasetID    string         `json:"parent_dataset_id,omitempty"`
	Network            string         `json:"network"`
	CreatedAt          string         `json:"created_at"`
	FinalizedAt        string         `json:"finalized_at,omitempty"`
	Finalized          bool           `json:"finalized"`
	InitialAnchor      ChainTip       `json:"initial_anchor"`
	FirstEventSequence uint64         `json:"first_event_sequence"`
	Limits             Limits         `json:"limits"`
	Final              *FinalManifest `json:"final,omitempty"`
}

type EventEnvelope struct {
	SchemaMajor         uint32          `json:"schema_major"`
	SchemaMinor         uint32          `json:"schema_minor"`
	DatasetID           string          `json:"dataset_id"`
	Network             string          `json:"network"`
	Sequence            uint64          `json:"sequence"`
	RecordedAtUnixNanos int64           `json:"recorded_at_unix_nanos"`
	Kind                string          `json:"kind"`
	Payload             json.RawMessage `json:"payload"`
	PayloadSHA256       string          `json:"payload_sha256"`
}

type BlockRef struct {
	Height               uint64 `json:"height"`
	Hash                 string `json:"hash"`
	ParentHash           string `json:"parent_hash"`
	BlockTimeUnixSeconds int64  `json:"block_time_unix_seconds"`
	ContentKey           string `json:"content_key"`
	RawPayloadSHA256     string `json:"raw_payload_sha256"`
	RawPayloadLength     uint64 `json:"raw_payload_length"`
}

type BlockEvent struct {
	Block BlockRef `json:"block"`
}

type TipEvent struct {
	Tip ChainTip `json:"tip"`
}

type DatasetFinalizedEvent struct {
	FinalSequence    uint64   `json:"final_sequence"`
	FinalTip         ChainTip `json:"final_tip"`
	BlockObjectCount uint64   `json:"block_object_count"`
	BlockSetSHA256   string   `json:"block_set_sha256"`
}

type SourceStartedEvent struct {
	SessionID       string `json:"session_id"`
	RecorderVersion string `json:"recorder_version"`
	ZebraEndpointID string `json:"zebra_endpoint_id"`
}

type SourceStoppedEvent struct {
	SessionID string `json:"session_id"`
	Reason    string `json:"reason"`
}

type SourceUnavailableEvent struct {
	Category   string `json:"category"`
	Diagnostic string `json:"diagnostic"`
}

type SourceRecoveredEvent struct {
	UnavailableSinceUnixNanos int64 `json:"unavailable_since_unix_nanos"`
}
