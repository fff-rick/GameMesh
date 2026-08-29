package protocol

import (
	"errors"
	"testing"
)

func TestControlMessageTypesValidate(t *testing.T) {
	for _, mt := range []uint32{MessageTypeAuthRequest, MessageTypeAuthResult, MessageTypeHeartbeatRequest, MessageTypeHeartbeatResponse} {
		if err := (Envelope{Version: CurrentVersion, MessageType: mt}).Validate(); err != nil {
			t.Fatalf("message type %d rejected: %v", mt, err)
		}
	}
}

func TestAuthRequestRoundTrip(t *testing.T) {
	want := AuthRequest{Token: "user:alice"}
	got, err := UnmarshalAuthRequest(MarshalAuthRequest(want))
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestAuthResultRoundTrip(t *testing.T) {
	want := AuthResult{OK: true, UserID: "alice", SessionID: "session-1", ResumeToken: "resume-token"}
	got, err := UnmarshalAuthResult(MarshalAuthResult(want))
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %#v want %#v", got, want)
	}

	fail := AuthResult{OK: false, ErrorCode: "invalid_token"}
	got, err = UnmarshalAuthResult(MarshalAuthResult(fail))
	if err != nil {
		t.Fatal(err)
	}
	if got != fail {
		t.Fatalf("got %#v want %#v", got, fail)
	}
}

func TestResumeControlPayloadRoundTrip(t *testing.T) {
	request := ResumeRequest{ResumeToken: "resume-token"}
	gotRequest, err := UnmarshalResumeRequest(MarshalResumeRequest(request))
	if err != nil {
		t.Fatal(err)
	}
	if gotRequest != request {
		t.Fatalf("request got %#v want %#v", gotRequest, request)
	}

	result := ResumeResult{OK: true, SessionID: "session-1", ResumeToken: "fresh-token"}
	gotResult, err := UnmarshalResumeResult(MarshalResumeResult(result))
	if err != nil {
		t.Fatal(err)
	}
	if gotResult != result {
		t.Fatalf("result got %#v want %#v", gotResult, result)
	}
}

func TestResumeControlPayloadSkipsUnknownFields(t *testing.T) {
	payload := append(MarshalResumeResult(ResumeResult{OK: true, SessionID: "session-1"}), 0x28, 0x01)
	got, err := UnmarshalResumeResult(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !got.OK || got.SessionID != "session-1" {
		t.Fatalf("got %#v", got)
	}
}

func TestAuthPayloadRejectsMalformed(t *testing.T) {
	if _, err := UnmarshalAuthRequest([]byte{0x0a, 0xff}); !errors.Is(err, ErrMalformedControlPayload) {
		t.Fatalf("got %v", err)
	}
}
