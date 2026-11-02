package replay

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestTransportRoundTrip(t *testing.T) {
	var stream bytes.Buffer
	want := AckMessage{Type: "ack", Sequence: 42}
	if err := NewEncoder(&stream).Encode(want); err != nil {
		t.Fatal(err)
	}
	got, err := NewDecoder(&stream, 1024).DecodeResponse()
	if err != nil {
		t.Fatal(err)
	}
	ack, ok := got.(*AckMessage)
	if !ok || *ack != want {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestTransportRejectsOversizedFrame(t *testing.T) {
	var stream bytes.Buffer
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], 100)
	stream.Write(header[:])
	if _, err := NewDecoder(&stream, 10).DecodeResponse(); err == nil {
		t.Fatal("expected size error")
	}
}
