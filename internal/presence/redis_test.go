package presence

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// This integration test is opt-in so the normal unit suite has no Docker or
// external Redis dependency. Stage 8 runs it with TEST_REDIS_ADDR set.
func TestRedisRegistryContract(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR is not set")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping redis: %v", err)
	}
	r := NewRedisRegistry(client, "stage8-test:"+t.Name(), 80*time.Millisecond)
	old := Owner{UserID: "alice", GatewayID: "a", ConnID: "a-1", LeaseToken: "old"}
	if _, err := r.Claim(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	current := Owner{UserID: "alice", GatewayID: "b", ConnID: "b-1", LeaseToken: "new"}
	previous, err := r.Claim(context.Background(), current)
	if err != nil || previous == nil || previous.LeaseToken != old.LeaseToken {
		t.Fatalf("previous=%#v err=%v", previous, err)
	}
	if released, err := r.Release(context.Background(), old); err != nil || released {
		t.Fatalf("old release released=%t err=%v", released, err)
	}
	if renewed, err := r.Renew(context.Background(), current); err != nil || !renewed {
		t.Fatalf("renew current renewed=%t err=%v", renewed, err)
	}
	time.Sleep(100 * time.Millisecond)
	if renewed, err := r.Renew(context.Background(), current); err != nil || renewed {
		t.Fatalf("expired renew renewed=%t err=%v", renewed, err)
	}
}
