package backend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type funcClient func(context.Context, Request) (Response, error)

func (f funcClient) Handle(ctx context.Context, req Request) (Response, error) { return f(ctx, req) }

func TestCallerSuccess(t *testing.T) {
	c := NewCaller(50 * time.Millisecond)
	got, err := c.Call(context.Background(), funcClient(func(_ context.Context, req Request) (Response, error) {
		if req.UserID != "alice" || req.RoomID != "room-1" || req.MessageType != 1001 || string(req.Payload) != "move" {
			t.Fatalf("req=%#v", req)
		}
		return Response{MessageType: 1002, Payload: []byte("ok")}, nil
	}), Request{UserID: "alice", RoomID: "room-1", MessageType: 1001, Payload: []byte("move")})
	if err != nil {
		t.Fatal(err)
	}
	if got.MessageType != 1002 || string(got.Payload) != "ok" {
		t.Fatalf("got=%#v", got)
	}
}

func TestCallerMapsTimeoutUnavailableAndBackendError(t *testing.T) {
	c := NewCaller(15 * time.Millisecond)
	_, err := c.Call(context.Background(), funcClient(func(ctx context.Context, _ Request) (Response, error) { <-ctx.Done(); return Response{}, ctx.Err() }), Request{})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("timeout err=%v", err)
	}

	_, err = c.Call(context.Background(), funcClient(func(context.Context, Request) (Response, error) { return Response{}, ErrTransportUnavailable }), Request{})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("unavailable err=%v", err)
	}

	_, err = c.Call(context.Background(), funcClient(func(context.Context, Request) (Response, error) { return Response{ErrorCode: "room_full"}, nil }), Request{})
	var be *BackendError
	if !errors.As(err, &be) || be.Code != "room_full" {
		t.Fatalf("backend err=%T %v", err, err)
	}
}

func TestCallerHandlesHighConcurrency(t *testing.T) {
	c := NewCaller(time.Second)
	client := funcClient(func(_ context.Context, req Request) (Response, error) {
		return Response{MessageType: req.MessageType + 1, Payload: req.Payload}, nil
	})
	const n = 256
	errCh := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := c.Call(context.Background(), client, Request{MessageType: 1001, Payload: []byte("x")})
			if err != nil {
				errCh <- err
				return
			}
			if resp.MessageType != 1002 {
				errCh <- fmt.Errorf("message type=%d", resp.MessageType)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

func TestRegistryConcurrentUpdatesAndReads(t *testing.T) {
	r := NewRegistry()
	client := funcClient(func(context.Context, Request) (Response, error) { return Response{}, nil })
	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func(i int) { defer wg.Done(); r.Set(fmt.Sprintf("i-%d", i%8), client) }(i)
		go func(i int) { defer wg.Done(); _, _ = r.Get(fmt.Sprintf("i-%d", i%8)) }(i)
	}
	wg.Wait()
}
