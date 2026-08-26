package backend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrTimeout              = errors.New("backend timeout")
	ErrUnavailable          = errors.New("backend unavailable")
	ErrTransportUnavailable = errors.New("backend transport unavailable")
	ErrClientNotFound       = errors.New("backend client not found")
)

type Request struct {
	UserID          string
	SessionID       string
	RoomID          string
	MessageType     uint32
	RequestID       string
	Payload         []byte
	TimestampUnixMS int64
}

type Response struct {
	MessageType uint32
	Payload     []byte
	ErrorCode   string
}

type BackendError struct{ Code string }

func (e *BackendError) Error() string { return "backend error: " + e.Code }

type Client interface {
	Handle(context.Context, Request) (Response, error)
}

type Caller struct{ timeout time.Duration }

func NewCaller(timeout time.Duration) *Caller { return &Caller{timeout: timeout} }
func (c *Caller) Call(ctx context.Context, client Client, req Request) (Response, error) {
	if client == nil {
		return Response{}, ErrUnavailable
	}
	callCtx := ctx
	cancel := func() {}
	if c.timeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, c.timeout)
	}
	defer cancel()
	resp, err := client.Handle(callCtx, req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(callCtx.Err(), context.DeadlineExceeded) {
			return Response{}, ErrTimeout
		}
		if errors.Is(err, ErrTransportUnavailable) {
			return Response{}, ErrUnavailable
		}
		return Response{}, fmt.Errorf("backend call: %w", err)
	}
	if resp.ErrorCode != "" {
		return Response{}, &BackendError{Code: resp.ErrorCode}
	}
	return resp, nil
}

type Registry struct {
	mu      sync.RWMutex
	clients map[string]Client
}

func NewRegistry() *Registry { return &Registry{clients: make(map[string]Client)} }
func (r *Registry) Set(instanceID string, client Client) {
	r.mu.Lock()
	r.clients[instanceID] = client
	r.mu.Unlock()
}
func (r *Registry) Delete(instanceID string) {
	r.mu.Lock()
	delete(r.clients, instanceID)
	r.mu.Unlock()
}
func (r *Registry) Get(instanceID string) (Client, error) {
	r.mu.RLock()
	c, ok := r.clients[instanceID]
	r.mu.RUnlock()
	if !ok || c == nil {
		return nil, ErrClientNotFound
	}
	return c, nil
}
