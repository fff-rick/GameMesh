package protocol

import (
	"bytes"
	"errors"
	"testing"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	want := Envelope{Version: 1, MessageType: 1000, RequestID: "r1", MessageID: "m1", Seq: 7, Ack: 5, Payload: []byte("hello"), TimestampUnixMS: 123456}
	got, err := Unmarshal(Marshal(want))
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != want.Version || got.MessageType != want.MessageType || got.RequestID != want.RequestID || got.MessageID != want.MessageID || got.Seq != want.Seq || got.Ack != want.Ack || got.TimestampUnixMS != want.TimestampUnixMS || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("round trip mismatch: %#v", got)
	}
}
func TestEnvelopeRejectsMalformed(t *testing.T) {
	if _, err := Unmarshal([]byte{0x0a, 0xff}); !errors.Is(err, ErrMalformedEnvelope) {
		t.Fatalf("got %v", err)
	}
}
func TestEnvelopeVersionAndMessageType(t *testing.T) {
	if !errors.Is(Envelope{Version: 2, MessageType: 1000}.Validate(), ErrUnsupportedVersion) {
		t.Fatal("expected version error")
	}
	if !errors.Is(Envelope{Version: 1, MessageType: 3}.Validate(), ErrUnknownMessageType) {
		t.Fatal("expected message type error")
	}
}

func TestAckIsKnownControlMessageType(t *testing.T) {
	if err := (Envelope{Version: CurrentVersion, MessageType: MessageTypeAck, Ack: 9}).Validate(); err != nil {
		t.Fatalf("ack validate: %v", err)
	}
}
