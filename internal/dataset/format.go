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

// FrameReader incrementally reads committed event frames from a seekable log.
// If the current tail is incomplete, Next restores the frame start so a later
// call can retry after the recorder appends the remaining bytes.
type FrameReader struct {
	reader io.ReadSeeker
}

func NewFrameReader(reader io.ReadSeeker) (*FrameReader, error) {
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	if err := ReadFileHeader(reader); err != nil {
		return nil, err
	}
	return &FrameReader{reader: reader}, nil
}

func (r *FrameReader) Next(maxPayloadBytes uint32) (EventEnvelope, error) {
	start, err := r.reader.Seek(0, io.SeekCurrent)
	if err != nil {
		return EventEnvelope{}, err
	}
	var header [frameHeaderSize]byte
	n, err := io.ReadFull(r.reader, header[:])
	if errors.Is(err, io.EOF) && n == 0 {
		return EventEnvelope{}, io.EOF
	}
	if err != nil {
		_, _ = r.reader.Seek(start, io.SeekStart)
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return EventEnvelope{}, ErrIncompleteTail
		}
		return EventEnvelope{}, err
	}
	length := binary.LittleEndian.Uint32(header[:4])
	sequence := binary.LittleEndian.Uint64(header[4:12])
	if length > maxPayloadBytes {
		return EventEnvelope{}, fmt.Errorf("event %d payload length %d exceeds limit %d", sequence, length, maxPayloadBytes)
	}
	body := make([]byte, int(length)+frameDigestSize)
	if _, err := io.ReadFull(r.reader, body); err != nil {
		_, _ = r.reader.Seek(start, io.SeekStart)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return EventEnvelope{}, ErrIncompleteTail
		}
		return EventEnvelope{}, err
	}
	payload, wantDigest := body[:length], body[length:]
	digest := sha256.New()
	digest.Write(header[:])
	digest.Write(payload)
	if !equalBytes(digest.Sum(nil), wantDigest) {
		return EventEnvelope{}, fmt.Errorf("event %d frame digest mismatch", sequence)
	}
	var event EventEnvelope
	if err := strictJSON(payload, &event); err != nil {
		return EventEnvelope{}, fmt.Errorf("decode event %d: %w", sequence, err)
	}
	if event.Sequence != sequence {
		return EventEnvelope{}, fmt.Errorf("frame sequence %d does not match envelope sequence %d", sequence, event.Sequence)
	}
	return event, nil
}

func ScanFrames(r io.Reader, options ScanOptions, visit func(EventEnvelope) error) (uint64, bool, error) {
	count, incomplete, _, err := scanFrames(r, options, visit)
	return count, incomplete, err
}

func scanFrames(r io.Reader, options ScanOptions, visit func(EventEnvelope) error) (uint64, bool, int64, error) {
	if err := ReadFileHeader(r); err != nil {
		return 0, false, 0, err
	}
	reader := bufio.NewReader(r)
	var count uint64
	offset := int64(fileHeaderSize)
	for {
		var header [frameHeaderSize]byte
		n, err := io.ReadFull(reader, header[:])
		if errors.Is(err, io.EOF) && n == 0 {
			return count, false, offset, nil
		}
		if err != nil {
			if options.AllowIncompleteTail && errors.Is(err, io.ErrUnexpectedEOF) {
				return count, true, offset, nil
			}
			return count, false, offset, fmt.Errorf("read frame header after %d events: %w", count, err)
		}
		length := binary.LittleEndian.Uint32(header[:4])
		sequence := binary.LittleEndian.Uint64(header[4:12])
		if length > options.MaxPayloadBytes {
			return count, false, offset, fmt.Errorf("event %d payload length %d exceeds limit %d", sequence, length, options.MaxPayloadBytes)
		}
		body := make([]byte, int(length)+frameDigestSize)
		if _, err := io.ReadFull(reader, body); err != nil {
			if options.AllowIncompleteTail && errors.Is(err, io.ErrUnexpectedEOF) {
				return count, true, offset, nil
			}
			return count, false, offset, fmt.Errorf("read event %d body: %w", sequence, err)
		}
		payload, wantDigest := body[:length], body[length:]
		digest := sha256.New()
		digest.Write(header[:])
		digest.Write(payload)
		if !equalBytes(digest.Sum(nil), wantDigest) {
			return count, false, offset, fmt.Errorf("event %d frame digest mismatch", sequence)
		}
		var event EventEnvelope
		if err := strictJSON(payload, &event); err != nil {
			return count, false, offset, fmt.Errorf("decode event %d: %w", sequence, err)
		}
		if event.Sequence != sequence {
			return count, false, offset, fmt.Errorf("frame sequence %d does not match envelope sequence %d", sequence, event.Sequence)
		}
		if err := visit(event); err != nil {
			return count, false, offset, err
		}
		count++
		offset += frameHeaderSize + int64(length) + frameDigestSize
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
