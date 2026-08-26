package backend

import "testing"

func TestBackendRequestProtobufWireRoundTrip(t *testing.T) {
	in := Request{UserID: "alice", SessionID: "s1", RoomID: "r1", MessageType: 1001, RequestID: "q1", Payload: []byte{1, 2, 3}, TimestampUnixMS: 12345}
	got, err := UnmarshalRequest(MarshalRequest(in))
	if err != nil {
		t.Fatal(err)
	}
	if got.UserID != in.UserID || got.SessionID != in.SessionID || got.RoomID != in.RoomID || got.MessageType != in.MessageType || got.RequestID != in.RequestID || string(got.Payload) != string(in.Payload) || got.TimestampUnixMS != in.TimestampUnixMS {
		t.Fatalf("got=%#v", got)
	}
}

func TestBackendResponseProtobufWireRoundTrip(t *testing.T) {
	in := Response{MessageType: 1002, Payload: []byte("ok"), ErrorCode: "room_full"}
	got, err := UnmarshalResponse(MarshalResponse(in))
	if err != nil {
		t.Fatal(err)
	}
	if got.MessageType != in.MessageType || string(got.Payload) != string(in.Payload) || got.ErrorCode != in.ErrorCode {
		t.Fatalf("got=%#v", got)
	}
}

func TestBackendWireRejectsMalformedLength(t *testing.T) {
	if _, err := UnmarshalRequest([]byte{0x0a, 0xff}); err == nil {
		t.Fatal("expected malformed request")
	}
	if _, err := UnmarshalResponse([]byte{0x12, 0xff}); err == nil {
		t.Fatal("expected malformed response")
	}
}
