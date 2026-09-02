// Package statesync is GameMesh's trusted transport adapter for GSSS. It does
// not interpret game state; it only owns one ordered internal gRPC stream.
package statesync

import (
	"context"
	"errors"
	"sync"

	state_syncv1 "github.com/xin/gsss/api/state_sync/v1"
	"google.golang.org/grpc"
)

const (
	MessageTypeInput       uint32 = 2001
	MessageTypeSnapshot    uint32 = 2002
	MessageTypeSnapshotAck uint32 = 2003
	MessageTypeControl     uint32 = 2004
)

var ErrClosed = errors.New("state sync stream closed")

type Handler struct {
	Snapshot func(*state_syncv1.Snapshot)
	Control  func(*state_syncv1.ControlEvent)
	Closed   func(error)
}

type Client struct {
	stream state_syncv1.StateSyncGateway_ConnectClient
	mu     sync.RWMutex
	sendMu sync.Mutex
	h      Handler
	ctx    context.Context
	cancel context.CancelFunc
}

func New(ctx context.Context, conn grpc.ClientConnInterface, handler Handler) (*Client, error) {
	child, cancel := context.WithCancel(ctx)
	stream, err := state_syncv1.NewStateSyncGatewayClient(conn).Connect(child)
	if err != nil {
		cancel()
		return nil, err
	}
	c := &Client{stream: stream, h: handler, ctx: child, cancel: cancel}
	go c.receiveLoop()
	return c, nil
}
func (c *Client) SetHandler(handler Handler) { c.mu.Lock(); c.h = handler; c.mu.Unlock() }
func (c *Client) Close()                     { c.cancel() }

func (c *Client) SendJoin(matchID string, playerID uint64) error {
	return c.send(&state_syncv1.GatewayToStateSync{Message: &state_syncv1.GatewayToStateSync_PlayerJoin{PlayerJoin: &state_syncv1.PlayerJoin{MatchId: matchID, PlayerId: playerID}}})
}
func (c *Client) SendLeave(matchID string, playerID uint64, reason state_syncv1.LeaveReason) error {
	return c.send(&state_syncv1.GatewayToStateSync{Message: &state_syncv1.GatewayToStateSync_PlayerLeave{PlayerLeave: &state_syncv1.PlayerLeave{MatchId: matchID, PlayerId: playerID, Reason: reason}}})
}
func (c *Client) SendInput(input *state_syncv1.PlayerInput) error {
	return c.send(&state_syncv1.GatewayToStateSync{Message: &state_syncv1.GatewayToStateSync_PlayerInput{PlayerInput: input}})
}
func (c *Client) SendAck(ack *state_syncv1.SnapshotAck) error {
	return c.send(&state_syncv1.GatewayToStateSync{Message: &state_syncv1.GatewayToStateSync_SnapshotAck{SnapshotAck: ack}})
}
func (c *Client) send(message *state_syncv1.GatewayToStateSync) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if c.ctx.Err() != nil {
		return ErrClosed
	}
	return c.stream.Send(message)
}
func (c *Client) receiveLoop() {
	var final error
	defer func() {
		c.mu.RLock()
		h := c.h
		c.mu.RUnlock()
		if h.Closed != nil {
			h.Closed(final)
		}
	}()
	for {
		message, err := c.stream.Recv()
		if err != nil {
			final = err
			return
		}
		c.mu.RLock()
		h := c.h
		c.mu.RUnlock()
		switch event := message.Message.(type) {
		case *state_syncv1.StateSyncToGateway_Snapshot:
			if event.Snapshot != nil && h.Snapshot != nil {
				h.Snapshot(event.Snapshot)
			}
		case *state_syncv1.StateSyncToGateway_Control:
			if event.Control != nil && h.Control != nil {
				h.Control(event.Control)
			}
		}
	}
}
