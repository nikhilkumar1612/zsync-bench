package replay

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/nikhilkumar1612/zsync-bench/internal/dataset"
)

const (
	ProtocolMajor        = 1
	ProtocolMinor        = 0
	DefaultMaxFrameBytes = 2 << 20
)

type Handshake struct {
	Type              string  `json:"type"`
	ProtocolMajor     uint32  `json:"protocol_major"`
	ProtocolMinor     uint32  `json:"protocol_minor"`
	EventSchemaMajor  uint32  `json:"event_schema_major"`
	EventSchemaMinor  uint32  `json:"event_schema_minor"`
	DatasetID         string  `json:"dataset_id"`
	Network           string  `json:"network"`
	DatasetState      string  `json:"dataset_state"`
	DatasetRoot       string  `json:"dataset_root"`
	FirstSequence     uint64  `json:"first_sequence"`
	RequestedSequence uint64  `json:"requested_sequence"`
	TimingMode        string  `json:"timing_mode"`
	Acceleration      float64 `json:"acceleration"`
	RawBlockDelivery  string  `json:"raw_block_delivery"`
}

type EventMessage struct {
	Type  string                `json:"type"`
	Event dataset.EventEnvelope `json:"event"`
}

type EndMessage struct {
	Type          string `json:"type"`
	FinalSequence uint64 `json:"final_sequence"`
}

type HeartbeatMessage struct {
	Type          string `json:"type"`
	SentUnixNanos int64  `json:"sent_unix_nanos"`
}

type StopMessage struct {
	Type         string `json:"type"`
	Reason       string `json:"reason"`
	LastSequence uint64 `json:"last_sequence"`
}

type ReadyMessage struct {
	Type          string `json:"type"`
	ProtocolMajor uint32 `json:"protocol_major"`
	ProtocolMinor uint32 `json:"protocol_minor"`
}

type AckMessage struct {
	Type     string `json:"type"`
	Sequence uint64 `json:"sequence"`
}

type ErrorMessage struct {
	Type    string `json:"type"`
	Fatal   bool   `json:"fatal"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type messageType struct {
	Type string `json:"type"`
}

type Encoder struct{ writer io.Writer }
type Decoder struct {
	reader *bufio.Reader
	max    uint32
}

func NewEncoder(writer io.Writer) *Encoder { return &Encoder{writer: writer} }
func NewDecoder(reader io.Reader, maxFrameBytes uint32) *Decoder {
	if maxFrameBytes == 0 {
		maxFrameBytes = DefaultMaxFrameBytes
	}
	return &Decoder{reader: bufio.NewReader(reader), max: maxFrameBytes}
}

func (e *Encoder) Encode(value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if uint64(len(payload)) > uint64(^uint32(0)) {
		return fmt.Errorf("transport frame exceeds uint32")
	}
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := e.writer.Write(header[:]); err != nil {
		return err
	}
	_, err = e.writer.Write(payload)
	return err
}

func (d *Decoder) Decode(value any) error {
	var header [4]byte
	if _, err := io.ReadFull(d.reader, header[:]); err != nil {
		return err
	}
	length := binary.LittleEndian.Uint32(header[:])
	if length == 0 || length > d.max {
		return fmt.Errorf("transport frame length %d exceeds limit %d", length, d.max)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(d.reader, payload); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("transport frame contains trailing JSON")
	}
	return nil
}

func (d *Decoder) DecodeResponse() (any, error) {
	var header [4]byte
	if _, err := io.ReadFull(d.reader, header[:]); err != nil {
		return nil, err
	}
	length := binary.LittleEndian.Uint32(header[:])
	if length == 0 || length > d.max {
		return nil, fmt.Errorf("transport frame length %d exceeds limit %d", length, d.max)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(d.reader, payload); err != nil {
		return nil, err
	}
	var kind messageType
	if err := json.Unmarshal(payload, &kind); err != nil {
		return nil, err
	}
	var value any
	switch kind.Type {
	case "ready":
		value = &ReadyMessage{}
	case "ack":
		value = &AckMessage{}
	case "error":
		value = &ErrorMessage{}
	default:
		return nil, fmt.Errorf("unsupported consumer message type %q", kind.Type)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return nil, err
	}
	return value, nil
}

func (d *Decoder) DecodeRequest() (any, error) {
	payload, err := d.readPayload()
	if err != nil {
		return nil, err
	}
	var kind messageType
	if err := json.Unmarshal(payload, &kind); err != nil {
		return nil, err
	}
	var value any
	switch kind.Type {
	case "handshake":
		value = &Handshake{}
	case "event":
		value = &EventMessage{}
	case "end":
		value = &EndMessage{}
	case "heartbeat":
		value = &HeartbeatMessage{}
	case "stop":
		value = &StopMessage{}
	default:
		return nil, fmt.Errorf("unsupported replay message type %q", kind.Type)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return nil, err
	}
	return value, nil
}

func (d *Decoder) readPayload() ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(d.reader, header[:]); err != nil {
		return nil, err
	}
	length := binary.LittleEndian.Uint32(header[:])
	if length == 0 || length > d.max {
		return nil, fmt.Errorf("transport frame length %d exceeds limit %d", length, d.max)
	}
	payload := make([]byte, length)
	_, err := io.ReadFull(d.reader, payload)
	return payload, err
}
