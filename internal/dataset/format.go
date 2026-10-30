package dataset

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

var fileMagic = [8]byte{'Z', 'S', 'Y', 'N', 'C', 'L', 'O', 'G'}

const (
	fileHeaderSize  = 16
	frameHeaderSize = 12
	frameDigestSize = sha256.Size
)

var ErrIncompleteTail = errors.New("incomplete trailing frame")

func WriteFileHeader(w io.Writer) error {
	var header [fileHeaderSize]byte
	copy(header[:8], fileMagic[:])
	binary.LittleEndian.PutUint16(header[8:10], SchemaMajor)
	binary.LittleEndian.PutUint16(header[10:12], SchemaMinor)
	_, err := w.Write(header[:])
	return err
}

func ReadFileHeader(r io.Reader) error {
	var header [fileHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return fmt.Errorf("read event log header: %w", err)
	}
	if string(header[:8]) != string(fileMagic[:]) {
		return fmt.Errorf("invalid event log magic %q", header[:8])
	}
	if major := binary.LittleEndian.Uint16(header[8:10]); major != SchemaMajor {
		return fmt.Errorf("unsupported event log major version %d", major)
	}
	if minor := binary.LittleEndian.Uint16(header[10:12]); minor > SchemaMinor {
		return fmt.Errorf("unsupported event log minor version %d", minor)
	}
	if flags := binary.LittleEndian.Uint32(header[12:16]); flags != 0 {
		return fmt.Errorf("unsupported event log flags %#x", flags)
	}
	return nil
}

func NewEnvelope(datasetID string, sequence uint64, recordedAt int64, kind string, payload any) (EventEnvelope, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return EventEnvelope{}, fmt.Errorf("marshal %s payload: %w", kind, err)
	}
	digest := sha256.Sum256(payloadBytes)
	return EventEnvelope{
		SchemaMajor: SchemaMajor, SchemaMinor: SchemaMinor,
		DatasetID: datasetID, Network: Network, Sequence: sequence,
		RecordedAtUnixNanos: recordedAt, Kind: kind, Payload: payloadBytes,
		PayloadSHA256: hex.EncodeToString(digest[:]),
	}, nil
}

func WriteFrame(w io.Writer, event EventEnvelope) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event %d: %w", event.Sequence, err)
	}
	if uint64(len(payload)) > uint64(^uint32(0)) {
		return fmt.Errorf("event %d is too large", event.Sequence)
	}
	var header [frameHeaderSize]byte
	binary.LittleEndian.PutUint32(header[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint64(header[4:12], event.Sequence)
	digest := sha256.New()
	digest.Write(header[:])
	digest.Write(payload)
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	if _, err := w.Write(digest.Sum(nil)); err != nil {
		return err
	}
	return nil
}

type ScanOptions struct {
	MaxPayloadBytes     uint32
	AllowIncompleteTail bool
}

func ScanFrames(r io.Reader, options ScanOptions, visit func(EventEnvelope) error) (uint64, bool, error) {
	if err := ReadFileHeader(r); err != nil {
		return 0, false, err
	}
	reader := bufio.NewReader(r)
	var count uint64
	for {
		var header [frameHeaderSize]byte
		n, err := io.ReadFull(reader, header[:])
		if errors.Is(err, io.EOF) && n == 0 {
			return count, false, nil
		}
		if err != nil {
			if options.AllowIncompleteTail && errors.Is(err, io.ErrUnexpectedEOF) {
				return count, true, nil
			}
			return count, false, fmt.Errorf("read frame header after %d events: %w", count, err)
		}
		length := binary.LittleEndian.Uint32(header[:4])
		sequence := binary.LittleEndian.Uint64(header[4:12])
		if length > options.MaxPayloadBytes {
			return count, false, fmt.Errorf("event %d payload length %d exceeds limit %d", sequence, length, options.MaxPayloadBytes)
		}
		body := make([]byte, int(length)+frameDigestSize)
		if _, err := io.ReadFull(reader, body); err != nil {
			if options.AllowIncompleteTail && errors.Is(err, io.ErrUnexpectedEOF) {
				return count, true, nil
			}
			return count, false, fmt.Errorf("read event %d body: %w", sequence, err)
		}
		payload, wantDigest := body[:length], body[length:]
		digest := sha256.New()
		digest.Write(header[:])
		digest.Write(payload)
		if !equalBytes(digest.Sum(nil), wantDigest) {
			return count, false, fmt.Errorf("event %d frame digest mismatch", sequence)
		}
		var event EventEnvelope
		if err := strictJSON(payload, &event); err != nil {
			return count, false, fmt.Errorf("decode event %d: %w", sequence, err)
		}
		if event.Sequence != sequence {
			return count, false, fmt.Errorf("frame sequence %d does not match envelope sequence %d", sequence, event.Sequence)
		}
		if err := visit(event); err != nil {
			return count, false, err
		}
		count++
	}
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var different byte
	for i := range a {
		different |= a[i] ^ b[i]
	}
	return different == 0
}
