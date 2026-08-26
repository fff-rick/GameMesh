package gateway

import (
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"game-gateway/internal/config"
	"game-gateway/internal/metrics"
	"game-gateway/internal/protocol"
	"game-gateway/internal/ws"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func newTestServer(t *testing.T) (*Server, *httptest.Server, string) {
	t.Helper()
	cfg := config.Default()
	cfg.SendQueueSize = 8
	cfg.WriteTimeout = 500 * time.Millisecond
	gw := New(cfg, "test-gateway", testLogger())
	ts := httptest.NewServer(gw.Handler())
	url := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	t.Cleanup(func() { gw.Close(); ts.Close() })
	return gw, ts, url
}
func dial(t *testing.T, url string) *ws.Conn {
	t.Helper()
	c, err := ws.Dial(url, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func echo(t *testing.T, c *ws.Conn, id string, payload []byte) {
	t.Helper()
	req := protocol.Envelope{Version: 1, MessageType: protocol.MessageTypeEchoRequest, RequestID: id, Payload: payload}
	if err := c.WriteBinary(protocol.Marshal(req)); err != nil {
		t.Fatal(err)
	}
	data, err := c.ReadBinary(64 * 1024)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := protocol.Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if resp.MessageType != protocol.MessageTypeEchoResponse || resp.RequestID != id || string(resp.Payload) != string(payload) {
		t.Fatalf("bad response %#v", resp)
	}
}
func waitFor(t *testing.T, d time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

func TestSingleClientContinuousSendReceive(t *testing.T) {
	_, _, url := newTestServer(t)
	c := dial(t, url)
	defer c.Close()
	for i := 0; i < 100; i++ {
		echo(t, c, "req", []byte("payload"))
	}
}
func TestMultipleConcurrentClients(t *testing.T) {
	gw, _, url := newTestServer(t)
	const clients = 32
	var wg sync.WaitGroup
	errC := make(chan error, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := ws.Dial(url, time.Second)
			if err != nil {
				errC <- err
				return
			}
			defer c.Close()
			for j := 0; j < 20; j++ {
				req := protocol.Envelope{Version: 1, MessageType: 1, RequestID: "x", Payload: []byte("y")}
				if err = c.WriteBinary(protocol.Marshal(req)); err != nil {
					errC <- err
					return
				}
				if _, err = c.ReadBinary(64 * 1024); err != nil {
					errC <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errC)
	for err := range errC {
		t.Error(err)
	}
	waitFor(t, time.Second, func() bool { return gw.ConnectionCount() == 0 })
}
func TestInvalidPacketDoesNotPanicAndConnectionSurvives(t *testing.T) {
	_, _, url := newTestServer(t)
	c := dial(t, url)
	defer c.Close()
	if err := c.WriteBinary([]byte{0x0a, 0xff}); err != nil {
		t.Fatal(err)
	}
	echo(t, c, "after-invalid", []byte("ok"))
}
func TestOversizePacketClosesConnection(t *testing.T) {
	gw, _, url := newTestServer(t)
	c := dial(t, url)
	payload := make([]byte, config.DefaultMaxEnvelopeBytes+1)
	if err := c.WriteBinary(payload); err != nil {
		t.Fatal(err)
	}
	_, err := c.ReadBinary(64 * 1024)
	if err == nil {
		t.Fatal("expected close")
	}
	_ = c.Close()
	waitFor(t, time.Second, func() bool { return gw.ConnectionCount() == 0 })
}
func TestClientCloseReleasesConnection(t *testing.T) {
	gw, _, url := newTestServer(t)
	c := dial(t, url)
	waitFor(t, time.Second, func() bool { return gw.ConnectionCount() == 1 })
	_ = c.WriteClose()
	_ = c.Close()
	waitFor(t, time.Second, func() bool { return gw.ConnectionCount() == 0 })
}
func TestServerCloseReleasesConnections(t *testing.T) {
	gw, _, url := newTestServer(t)
	c := dial(t, url)
	defer c.Close()
	waitFor(t, time.Second, func() bool { return gw.ConnectionCount() == 1 })
	gw.Close()
	if gw.ConnectionCount() != 0 {
		t.Fatalf("count=%d", gw.ConnectionCount())
	}
	if _, err := c.ReadBinary(1024); err == nil {
		t.Fatal("client should observe closed connection")
	}
}

// blockingTransport simulates a client that never consumes outbound data.
type blockingTransport struct {
	unblock chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func newBlockingTransport() *blockingTransport {
	return &blockingTransport{unblock: make(chan struct{}), closed: make(chan struct{})}
}
func (b *blockingTransport) ReadBinary(int64) ([]byte, error) { <-b.closed; return nil, io.EOF }
func (b *blockingTransport) WriteBinary([]byte) error {
	select {
	case <-b.unblock:
		return nil
	case <-b.closed:
		return io.EOF
	}
}
func (b *blockingTransport) SetWriteDeadline(time.Time) error { return nil }
func (b *blockingTransport) Close() error                     { b.once.Do(func() { close(b.closed) }); return nil }
func TestSlowClientSendQueueIsBounded(t *testing.T) {
	m := metrics.New("test")
	tr := newBlockingTransport()
	c := newConnection("c", "test", tr, 64*1024, 2, time.Second, testLogger(), m, nil, nil)
	c.Start()
	defer func() { c.Close("test"); c.Wait() }()
	if err := c.Enqueue([]byte("one")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if err := c.Enqueue([]byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := c.Enqueue([]byte("three")); err != nil {
		t.Fatal(err)
	}
	if err := c.Enqueue([]byte("overflow")); !errors.Is(err, ErrSendQueueFull) {
		t.Fatalf("got %v", err)
	}
	if c.SendQueueLen() > c.SendQueueCap() {
		t.Fatalf("queue exceeded cap: %d>%d", c.SendQueueLen(), c.SendQueueCap())
	}
}
