package gateway

import (
	"context"
	"errors"
	"game-gateway/internal/metrics"
	"game-gateway/internal/protocol"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrConnectionClosed = errors.New("connection closed")
	ErrSendQueueFull    = errors.New("send queue full")
)

type transport interface {
	ReadBinary(int64) ([]byte, error)
	WriteBinary([]byte) error
	SetWriteDeadline(time.Time) error
	Close() error
}
type ConnState uint32

const (
	ConnNew ConnState = iota
	ConnOpen
	ConnClosing
	ConnClosed
)

type Connection struct {
	id, gatewayID    string
	transport        transport
	maxEnvelopeBytes int64
	writeTimeout     time.Duration
	sendQ            chan []byte
	logger           *slog.Logger
	metrics          *metrics.Metrics
	onEnvelope       func(*Connection, protocol.Envelope)
	onClosed         func(*Connection)
	state            atomic.Uint32
	ctx              context.Context
	cancel           context.CancelFunc
	closeOnce        sync.Once
	wg               sync.WaitGroup
}

func newConnection(id, gatewayID string, tr transport, maxBytes int64, queueSize int, writeTimeout time.Duration, logger *slog.Logger, m *metrics.Metrics, onEnvelope func(*Connection, protocol.Envelope), onClosed func(*Connection)) *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Connection{id: id, gatewayID: gatewayID, transport: tr, maxEnvelopeBytes: maxBytes, writeTimeout: writeTimeout, sendQ: make(chan []byte, queueSize), logger: logger.With("conn_id", id), metrics: m, onEnvelope: onEnvelope, onClosed: onClosed, ctx: ctx, cancel: cancel}
	c.state.Store(uint32(ConnNew))
	c.wg.Add(2)
	return c
}
func (c *Connection) ID() string        { return c.id }
func (c *Connection) State() ConnState  { return ConnState(c.state.Load()) }
func (c *Connection) SendQueueLen() int { return len(c.sendQ) }
func (c *Connection) SendQueueCap() int { return cap(c.sendQ) }
func (c *Connection) Start() {
	if !c.state.CompareAndSwap(uint32(ConnNew), uint32(ConnOpen)) {
		return
	}
	c.metrics.ConnectionOpened()
	c.logger.Info("connection opened")
	go c.readLoop()
	go c.writeLoop()
}
func (c *Connection) Enqueue(data []byte) error {
	if c.State() != ConnOpen {
		return ErrConnectionClosed
	}
	cp := append([]byte(nil), data...)
	select {
	case c.sendQ <- cp:
		c.metrics.ObserveQueueDepth(len(c.sendQ))
		return nil
	default:
		return ErrSendQueueFull
	}
}
func (c *Connection) Close(reason string) {
	c.closeOnce.Do(func() {
		wasOpen := c.state.Swap(uint32(ConnClosing)) == uint32(ConnOpen)
		c.cancel()
		_ = c.transport.Close()
		c.state.Store(uint32(ConnClosed))
		if wasOpen {
			c.metrics.ConnectionClosed(reason)
		}
		c.logger.Info("connection closed", "reason", reason)
		if c.onClosed != nil {
			c.onClosed(c)
		}
	})
}
func (c *Connection) Wait() { c.wg.Wait() }
func (c *Connection) readLoop() {
	defer c.wg.Done()
	for {
		data, err := c.transport.ReadBinary(c.maxEnvelopeBytes)
		if err != nil {
			c.Close(classifyReadCloseReason(err))
			return
		}
		c.metrics.Received(len(data))
		env, err := protocol.Unmarshal(data)
		if err != nil {
			c.logger.Warn("invalid envelope", "error", err)
			continue
		}
		if err := env.Validate(); err != nil {
			c.logger.Warn("rejected envelope", "error", err)
			continue
		}
		if c.onEnvelope != nil {
			c.onEnvelope(c, env)
		}
	}
}
func (c *Connection) writeLoop() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case data := <-c.sendQ:
			_ = c.transport.SetWriteDeadline(time.Now().Add(c.writeTimeout))
			if err := c.transport.WriteBinary(data); err != nil {
				c.Close("write_error")
				return
			}
			c.metrics.Sent(len(data))
		}
	}
}
func classifyReadCloseReason(error) string { return "read_error" }
