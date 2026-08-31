package presence

import (
	"context"
	"testing"
	"time"
)

func owner(user, gateway, conn, lease string) Owner {
	return Owner{UserID: user, GatewayID: gateway, ConnID: conn, LeaseToken: lease}
}

func TestMemoryRegistryFencesDelayedReleaseAndExpires(t *testing.T) {
	r := NewMemoryRegistry(25 * time.Millisecond)
	old := owner("alice", "a", "a-1", "old")
	if _, err := r.Claim(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	current := owner("alice", "b", "b-1", "new")
	previous, err := r.Claim(context.Background(), current)
	if err != nil || previous == nil || previous.LeaseToken != old.LeaseToken {
		t.Fatalf("previous=%#v err=%v", previous, err)
	}
	if released, err := r.Release(context.Background(), old); err != nil || released {
		t.Fatalf("delayed release released=%t err=%v", released, err)
	}
	if renewed, err := r.Renew(context.Background(), current); err != nil || !renewed {
		t.Fatalf("renewed=%t err=%v", renewed, err)
	}
	time.Sleep(30 * time.Millisecond)
	if renewed, err := r.Renew(context.Background(), current); err != nil || renewed {
		t.Fatalf("expired renewed=%t err=%v", renewed, err)
	}
}

func TestMemoryRegistryUnavailable(t *testing.T) {
	r := NewMemoryRegistry(time.Second)
	r.SetAvailable(false)
	if _, err := r.Claim(context.Background(), owner("alice", "a", "c", "l")); err != ErrUnavailable {
		t.Fatalf("err=%v", err)
	}
}
