package auth

import (
	"context"
	"errors"
	"testing"
)

func TestDevAuthenticatorAcceptsUserToken(t *testing.T) {
	got, err := (DevAuthenticator{}).Authenticate(context.Background(), "user:alice")
	if err != nil {
		t.Fatal(err)
	}
	if got != "alice" {
		t.Fatalf("got %q", got)
	}
}

func TestDevAuthenticatorRejectsInvalidAndExpiredToken(t *testing.T) {
	for _, token := range []string{"", "alice", "user:", "expired:alice"} {
		if _, err := (DevAuthenticator{}).Authenticate(context.Background(), token); !errors.Is(err, ErrInvalidToken) {
			t.Fatalf("token %q: got %v", token, err)
		}
	}
}
