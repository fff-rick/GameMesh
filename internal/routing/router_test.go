package routing

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestStaticRouterResolvesMessageUserRoomAndInstance(t *testing.T) {
	r := NewStaticRouter()
	r.SetMessageBackend(1001, "room")
	r.SetUserRoom("alice", "room-1")
	r.SetRoomInstance("room-1", BackendInstance{ID: "room-a", BackendType: "room", Address: "127.0.0.1:9001"})

	route, err := r.Resolve("alice", 1001)
	if err != nil {
		t.Fatal(err)
	}
	if route.BackendType != "room" || route.RoomID != "room-1" || route.Instance.ID != "room-a" {
		t.Fatalf("route=%#v", route)
	}
}

func TestStaticRouterRejectsUnknownAndMissingRoutes(t *testing.T) {
	r := NewStaticRouter()
	if _, err := r.Resolve("alice", 1999); !errors.Is(err, ErrUnknownMessageType) {
		t.Fatalf("err=%v", err)
	}
	r.SetMessageBackend(1001, "room")
	if _, err := r.Resolve("alice", 1001); !errors.Is(err, ErrUserRoomNotFound) {
		t.Fatalf("err=%v", err)
	}
	r.SetUserRoom("alice", "room-1")
	if _, err := r.Resolve("alice", 1001); !errors.Is(err, ErrRoomInstanceNotFound) {
		t.Fatalf("err=%v", err)
	}
}

func TestStaticRouterRejectsBackendTypeMismatch(t *testing.T) {
	r := NewStaticRouter()
	r.SetMessageBackend(1001, "room")
	r.SetUserRoom("alice", "room-1")
	r.SetRoomInstance("room-1", BackendInstance{ID: "chat-a", BackendType: "chat"})
	if _, err := r.Resolve("alice", 1001); !errors.Is(err, ErrBackendTypeMismatch) {
		t.Fatalf("err=%v", err)
	}
}

func TestStaticRouterConcurrentRouteChanges(t *testing.T) {
	r := NewStaticRouter()
	r.SetMessageBackend(1001, "room")
	r.SetUserRoom("alice", "room-1")
	r.SetRoomInstance("room-1", BackendInstance{ID: "a", BackendType: "room"})
	const n = 500
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			id := "a"
			if i%2 == 1 {
				id = "b"
			}
			r.SetRoomInstance("room-1", BackendInstance{ID: id, BackendType: "room"})
		}(i)
		go func() {
			defer wg.Done()
			route, err := r.Resolve("alice", 1001)
			if err != nil {
				errCh <- err
				return
			}
			if route.Instance.ID != "a" && route.Instance.ID != "b" {
				errCh <- fmt.Errorf("id=%q", route.Instance.ID)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}
