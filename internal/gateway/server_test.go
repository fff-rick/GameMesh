package gateway

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"game-gateway/internal/auth"
	"game-gateway/internal/backend"
	"game-gateway/internal/config"
	"game-gateway/internal/metrics"
	"game-gateway/internal/protocol"
	"game-gateway/internal/reliability"
	"game-gateway/internal/routing"
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

type tokenAuthenticator map[string]string

func (a tokenAuthenticator) Authenticate(_ context.Context, token string) (string, error) {
	if userID, ok := a[token]; ok {
		return userID, nil
	}
	return "", auth.ErrInvalidToken
}

func sendAuth(t *testing.T, c *ws.Conn, token string) protocol.AuthResult {
	t.Helper()
	req := protocol.Envelope{
		Version:     protocol.CurrentVersion,
		MessageType: protocol.MessageTypeAuthRequest,
		RequestID:   "auth",
		Payload:     protocol.MarshalAuthRequest(protocol.AuthRequest{Token: token}),
	}
	if err := c.WriteBinary(protocol.Marshal(req)); err != nil {
		t.Fatal(err)
	}
	data, err := c.ReadBinary(64 * 1024)
	if err != nil {
		t.Fatal(err)
	}
	env, err := protocol.Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if env.MessageType != protocol.MessageTypeAuthResult {
		t.Fatalf("message_type=%d", env.MessageType)
	}
	result, err := protocol.UnmarshalAuthResult(env.Payload)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func sendResume(t *testing.T, c *ws.Conn, token string) protocol.ResumeResult {
	t.Helper()
	req := protocol.Envelope{
		Version:     protocol.CurrentVersion,
		MessageType: protocol.MessageTypeResumeRequest,
		RequestID:   "resume",
		Payload:     protocol.MarshalResumeRequest(protocol.ResumeRequest{ResumeToken: token}),
	}
	if err := c.WriteBinary(protocol.Marshal(req)); err != nil {
		t.Fatal(err)
	}
	data, err := c.ReadBinary(64 * 1024)
	if err != nil {
		t.Fatal(err)
	}
	env, err := protocol.Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if env.MessageType != protocol.MessageTypeResumeResult {
		t.Fatalf("message_type=%d", env.MessageType)
	}
	result, err := protocol.UnmarshalResumeResult(env.Payload)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestAuthenticationCreatesSession(t *testing.T) {
	cfg := config.Default()
	gw := New(cfg, "test-gateway", testLogger(), WithAuthenticator(tokenAuthenticator{"valid": "alice"}))
	ts := httptest.NewServer(gw.Handler())
	defer func() { gw.Close(); ts.Close() }()
	url := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	c := dial(t, url)
	defer c.Close()

	result := sendAuth(t, c, "valid")
	if !result.OK || result.UserID != "alice" || result.SessionID == "" || result.ResumeToken == "" {
		t.Fatalf("result=%#v", result)
	}
	if gw.ActiveSessionCount() != 1 {
		t.Fatalf("sessions=%d", gw.ActiveSessionCount())
	}
}

func TestResumeWithinGraceKeepsSessionIdentity(t *testing.T) {
	cfg := config.Default()
	cfg.SessionGracePeriod = time.Second
	gw := New(cfg, "test-gateway", testLogger(), WithAuthenticator(tokenAuthenticator{"valid": "alice"}))
	ts := httptest.NewServer(gw.Handler())
	defer func() { gw.Close(); ts.Close() }()
	url := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"

	oldConn := dial(t, url)
	authResult := sendAuth(t, oldConn, "valid")
	if !authResult.OK || authResult.ResumeToken == "" {
		t.Fatalf("auth=%#v", authResult)
	}
	_ = oldConn.WriteClose()
	_ = oldConn.Close()
	waitFor(t, time.Second, func() bool { return gw.ConnectionCount() == 0 })

	newConn := dial(t, url)
	defer newConn.Close()
	resumeResult := sendResume(t, newConn, authResult.ResumeToken)
	if !resumeResult.OK || resumeResult.SessionID != authResult.SessionID || resumeResult.ResumeToken == "" || resumeResult.ResumeToken == authResult.ResumeToken {
		t.Fatalf("auth=%#v resume=%#v", authResult, resumeResult)
	}
	if gw.ActiveSessionCount() != 1 {
		t.Fatalf("sessions=%d", gw.ActiveSessionCount())
	}
	sendHeartbeat(t, newConn)

	replayConn := dial(t, url)
	defer replayConn.Close()
	if replay := sendResume(t, replayConn, authResult.ResumeToken); replay.OK || replay.ErrorCode != "resume_token_invalid" {
		t.Fatalf("replay=%#v", replay)
	}
}

func TestExpiredResumeTokenIsRejected(t *testing.T) {
	cfg := config.Default()
	cfg.SessionGracePeriod = 20 * time.Millisecond
	gw := New(cfg, "test-gateway", testLogger(), WithAuthenticator(tokenAuthenticator{"valid": "alice"}))
	ts := httptest.NewServer(gw.Handler())
	defer func() { gw.Close(); ts.Close() }()
	url := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"

	oldConn := dial(t, url)
	authResult := sendAuth(t, oldConn, "valid")
	_ = oldConn.WriteClose()
	_ = oldConn.Close()
	waitFor(t, time.Second, func() bool { return gw.ActiveSessionCount() == 0 })

	newConn := dial(t, url)
	defer newConn.Close()
	result := sendResume(t, newConn, authResult.ResumeToken)
	if result.OK || result.ErrorCode != "resume_token_invalid" {
		t.Fatalf("result=%#v", result)
	}
}

func TestOldConnectionCloseCannotRemoveResumedSession(t *testing.T) {
	cfg := config.Default()
	cfg.SessionGracePeriod = time.Second
	gw := New(cfg, "test-gateway", testLogger(), WithAuthenticator(tokenAuthenticator{"valid": "alice"}))
	ts := httptest.NewServer(gw.Handler())
	defer func() { gw.Close(); ts.Close() }()
	url := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"

	oldClient := dial(t, url)
	authResult := sendAuth(t, oldClient, "valid")
	oldServerConn := gw.connectionBySessionID(authResult.SessionID)
	if oldServerConn == nil {
		t.Fatal("old server connection not found")
	}
	_ = oldClient.WriteClose()
	_ = oldClient.Close()
	waitFor(t, time.Second, func() bool { return gw.ConnectionCount() == 0 })

	newClient := dial(t, url)
	defer newClient.Close()
	if result := sendResume(t, newClient, authResult.ResumeToken); !result.OK {
		t.Fatalf("resume=%#v", result)
	}
	gw.removeConn(oldServerConn)
	if gw.ActiveSessionCount() != 1 {
		t.Fatalf("sessions=%d", gw.ActiveSessionCount())
	}
	sendHeartbeat(t, newClient)
}

func TestAuthenticatedResumeRequestIsRejected(t *testing.T) {
	cfg := config.Default()
	gw := New(cfg, "test-gateway", testLogger(), WithAuthenticator(tokenAuthenticator{"valid": "alice"}))
	ts := httptest.NewServer(gw.Handler())
	defer func() { gw.Close(); ts.Close() }()
	url := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	c := dial(t, url)
	defer c.Close()
	authResult := sendAuth(t, c, "valid")
	result := sendResume(t, c, authResult.ResumeToken)
	if result.OK || result.ErrorCode != "already_authenticated" {
		t.Fatalf("result=%#v", result)
	}
}

func TestInvalidAndExpiredTokenDoNotCreateSession(t *testing.T) {
	cfg := config.Default()
	gw := New(cfg, "test-gateway", testLogger(), WithAuthenticator(tokenAuthenticator{"valid": "alice"}))
	ts := httptest.NewServer(gw.Handler())
	defer func() { gw.Close(); ts.Close() }()
	url := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	for _, token := range []string{"bad", "expired"} {
		c := dial(t, url)
		result := sendAuth(t, c, token)
		if result.OK || result.ErrorCode != "invalid_token" {
			t.Fatalf("token=%s result=%#v", token, result)
		}
		_ = c.Close()
	}
	if gw.ActiveSessionCount() != 0 {
		t.Fatalf("sessions=%d", gw.ActiveSessionCount())
	}
}

func TestUnauthenticatedBusinessMessageCannotCreateBusinessEffect(t *testing.T) {
	cfg := config.Default()
	gw := New(cfg, "test-gateway", testLogger(), WithAuthenticator(tokenAuthenticator{"valid": "alice"}))
	ts := httptest.NewServer(gw.Handler())
	defer func() { gw.Close(); ts.Close() }()
	url := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	c := dial(t, url)
	defer c.Close()

	business := protocol.Envelope{Version: protocol.CurrentVersion, MessageType: protocol.BusinessMessageMin, RequestID: "business", Payload: []byte("must-not-run")}
	if err := c.WriteBinary(protocol.Marshal(business)); err != nil {
		t.Fatal(err)
	}
	result := sendAuth(t, c, "valid")
	if !result.OK {
		t.Fatalf("auth result=%#v", result)
	}
	if gw.UnauthenticatedBusinessRejected() != 1 {
		t.Fatalf("rejected=%d", gw.UnauthenticatedBusinessRejected())
	}
}

func TestDuplicateLoginNewLoginWins(t *testing.T) {
	cfg := config.Default()
	gw := New(cfg, "test-gateway", testLogger(), WithAuthenticator(tokenAuthenticator{"one": "alice", "two": "alice"}))
	ts := httptest.NewServer(gw.Handler())
	defer func() { gw.Close(); ts.Close() }()
	url := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	oldConn := dial(t, url)
	defer oldConn.Close()
	oldResult := sendAuth(t, oldConn, "one")
	if !oldResult.OK {
		t.Fatalf("old=%#v", oldResult)
	}

	newConn := dial(t, url)
	defer newConn.Close()
	newResult := sendAuth(t, newConn, "two")
	if !newResult.OK {
		t.Fatalf("new=%#v", newResult)
	}
	if newResult.SessionID == oldResult.SessionID {
		t.Fatal("new login reused old session")
	}

	if _, err := oldConn.ReadBinary(1024); err == nil {
		t.Fatal("old connection should be closed by replacement")
	}
	if gw.ActiveSessionCount() != 1 {
		t.Fatalf("sessions=%d", gw.ActiveSessionCount())
	}
}

func TestDuplicateLoginImmediatelyRemovesReplacedReliabilityState(t *testing.T) {
	cfg := config.Default()
	cfg.ReliableRetryInterval = time.Second
	r := routing.NewStaticRouter()
	r.SetMessageBackend(1001, "room")
	r.SetUserRoom("alice", "room-1")
	r.SetRoomInstance("room-1", routing.BackendInstance{ID: "room-a", BackendType: "room", Address: "inproc"})
	reg := backend.NewRegistry()
	reg.Set("room-a", fakeBackendClient{handle: func(context.Context, backend.Request) (backend.Response, error) {
		return backend.Response{MessageType: 1002, Payload: []byte("pending")}, nil
	}})
	gw := New(
		cfg,
		"test-gateway",
		testLogger(),
		WithAuthenticator(tokenAuthenticator{"one": "alice", "two": "alice"}),
		WithRouter(r),
		WithBackendRegistry(reg),
		WithReliabilityClassifier(reliability.NewStaticClassifier(1002)),
	)
	ts := httptest.NewServer(gw.Handler())
	defer func() { gw.Close(); ts.Close() }()
	url := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"

	oldConn := dial(t, url)
	defer oldConn.Close()
	if result := sendAuth(t, oldConn, "one"); !result.OK {
		t.Fatalf("old auth=%#v", result)
	}
	writeEnvelope(t, oldConn, protocol.Envelope{Version: 1, MessageType: 1001, RequestID: "pending"})
	if response := readEnvelope(t, oldConn); response.Seq == 0 {
		t.Fatalf("response=%#v", response)
	}
	waitFor(t, time.Second, func() bool { return gw.ReliablePendingCount() == 1 })

	newConn := dial(t, url)
	defer newConn.Close()
	if result := sendAuth(t, newConn, "two"); !result.OK {
		t.Fatalf("new auth=%#v", result)
	}
	waitFor(t, time.Second, func() bool { return gw.ReliablePendingCount() == 0 })
}

func TestServerShutdownImmediatelyRemovesGraceSessionState(t *testing.T) {
	cfg := config.Default()
	cfg.SessionGracePeriod = time.Hour
	cfg.ReliableRetryInterval = time.Second
	r := routing.NewStaticRouter()
	r.SetMessageBackend(1001, "room")
	r.SetUserRoom("alice", "room-1")
	r.SetRoomInstance("room-1", routing.BackendInstance{ID: "room-a", BackendType: "room", Address: "inproc"})
	reg := backend.NewRegistry()
	reg.Set("room-a", fakeBackendClient{handle: func(context.Context, backend.Request) (backend.Response, error) {
		return backend.Response{MessageType: 1002, Payload: []byte("pending")}, nil
	}})
	gw := New(
		cfg,
		"test-gateway",
		testLogger(),
		WithAuthenticator(tokenAuthenticator{"valid": "alice"}),
		WithRouter(r),
		WithBackendRegistry(reg),
		WithReliabilityClassifier(reliability.NewStaticClassifier(1002)),
	)
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()
	url := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"

	c := dial(t, url)
	if result := sendAuth(t, c, "valid"); !result.OK {
		t.Fatalf("auth=%#v", result)
	}
	writeEnvelope(t, c, protocol.Envelope{Version: 1, MessageType: 1001, RequestID: "pending"})
	if response := readEnvelope(t, c); response.Seq == 0 {
		t.Fatalf("response=%#v", response)
	}
	_ = c.WriteClose()
	_ = c.Close()
	waitFor(t, time.Second, func() bool {
		return gw.ConnectionCount() == 0 && gw.ActiveSessionCount() == 1 && gw.ReliablePendingCount() == 1
	})

	gw.Close()
	if got := gw.ActiveSessionCount(); got != 0 {
		t.Fatalf("sessions after shutdown=%d", got)
	}
	if got := gw.ReliablePendingCount(); got != 0 {
		t.Fatalf("pending after shutdown=%d", got)
	}
}

func sendHeartbeat(t *testing.T, c *ws.Conn) {
	t.Helper()
	req := protocol.Envelope{Version: protocol.CurrentVersion, MessageType: protocol.MessageTypeHeartbeatRequest, RequestID: "hb", TimestampUnixMS: time.Now().UnixMilli()}
	if err := c.WriteBinary(protocol.Marshal(req)); err != nil {
		t.Fatal(err)
	}
	data, err := c.ReadBinary(64 * 1024)
	if err != nil {
		t.Fatal(err)
	}
	env, err := protocol.Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if env.MessageType != protocol.MessageTypeHeartbeatResponse {
		t.Fatalf("message_type=%d", env.MessageType)
	}
}

func TestHeartbeatKeepsAuthenticatedConnectionAliveThenIdleTimeoutClosesIt(t *testing.T) {
	cfg := config.Default()
	cfg.IdleTimeout = 90 * time.Millisecond
	cfg.HeartbeatCheckInterval = 10 * time.Millisecond
	gw := New(cfg, "test-gateway", testLogger(), WithAuthenticator(tokenAuthenticator{"valid": "alice"}))
	ts := httptest.NewServer(gw.Handler())
	defer func() { gw.Close(); ts.Close() }()
	url := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	c := dial(t, url)
	defer c.Close()
	if result := sendAuth(t, c, "valid"); !result.OK {
		t.Fatalf("auth=%#v", result)
	}

	for i := 0; i < 4; i++ {
		time.Sleep(30 * time.Millisecond)
		sendHeartbeat(t, c)
	}
	if gw.ConnectionCount() != 1 {
		t.Fatalf("connection timed out despite heartbeat")
	}

	waitFor(t, 500*time.Millisecond, func() bool { return gw.ConnectionCount() == 0 })
	if _, err := c.ReadBinary(1024); err == nil {
		t.Fatal("idle connection should be closed")
	}
}

func TestHalfOpenConnectionIsCollectedByIdleTimeout(t *testing.T) {
	cfg := config.Default()
	cfg.IdleTimeout = 40 * time.Millisecond
	cfg.HeartbeatCheckInterval = 5 * time.Millisecond
	gw := New(cfg, "test-gateway", testLogger())
	defer gw.Close()

	tr := newBlockingTransport()
	c := newConnection("half-open", "test-gateway", tr, cfg.MaxEnvelopeBytes, cfg.SendQueueSize, cfg.WriteTimeout, testLogger(), gw.metrics, gw.handleEnvelope, gw.removeConn)
	gw.mu.Lock()
	gw.conns[c.ID()] = c
	gw.mu.Unlock()
	c.Start()
	waitFor(t, 500*time.Millisecond, func() bool { return gw.ConnectionCount() == 0 })
	c.Wait()
}

func TestCloseAndHeartbeatScanAreRaceSafe(t *testing.T) {
	cfg := config.Default()
	cfg.IdleTimeout = 20 * time.Millisecond
	cfg.HeartbeatCheckInterval = 2 * time.Millisecond
	gw := New(cfg, "test-gateway", testLogger())
	tr := newBlockingTransport()
	c := newConnection("race-close", "test-gateway", tr, cfg.MaxEnvelopeBytes, cfg.SendQueueSize, cfg.WriteTimeout, testLogger(), gw.metrics, gw.handleEnvelope, gw.removeConn)
	gw.mu.Lock()
	gw.conns[c.ID()] = c
	gw.mu.Unlock()
	c.Start()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.Close("concurrent_test") }()
	}
	wg.Wait()
	gw.Close()
	c.Wait()
	if c.State() != ConnClosed {
		t.Fatalf("state=%v", c.State())
	}
}

func TestSessionEntersGraceWhenAuthenticatedConnectionCloses(t *testing.T) {
	cfg := config.Default()
	cfg.SessionGracePeriod = time.Second
	gw := New(cfg, "test-gateway", testLogger(), WithAuthenticator(tokenAuthenticator{"valid": "alice"}))
	ts := httptest.NewServer(gw.Handler())
	defer func() { gw.Close(); ts.Close() }()
	url := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	c := dial(t, url)
	if result := sendAuth(t, c, "valid"); !result.OK {
		t.Fatalf("auth=%#v", result)
	}
	if gw.ActiveSessionCount() != 1 {
		t.Fatalf("sessions=%d", gw.ActiveSessionCount())
	}
	_ = c.WriteClose()
	_ = c.Close()
	waitFor(t, time.Second, func() bool { return gw.ActiveSessionCount() == 1 && gw.ConnectionCount() == 0 })
}

type fakeBackendClient struct {
	handle func(context.Context, backend.Request) (backend.Response, error)
}

func (f fakeBackendClient) Handle(ctx context.Context, req backend.Request) (backend.Response, error) {
	return f.handle(ctx, req)
}

func readEnvelope(t *testing.T, c *ws.Conn) protocol.Envelope {
	t.Helper()
	data, err := c.ReadBinary(64 * 1024)
	if err != nil {
		t.Fatal(err)
	}
	env, err := protocol.Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func setupRoutedServer(t *testing.T, client backend.Client) (*Server, *ws.Conn, *routing.StaticRouter, *backend.Registry) {
	t.Helper()
	cfg := config.Default()
	cfg.BackendRPCTimeout = 40 * time.Millisecond
	r := routing.NewStaticRouter()
	r.SetMessageBackend(1001, "room")
	r.SetUserRoom("alice", "room-1")
	r.SetRoomInstance("room-1", routing.BackendInstance{ID: "room-a", BackendType: "room", Address: "inproc"})
	reg := backend.NewRegistry()
	if client != nil {
		reg.Set("room-a", client)
	}
	gw := New(cfg, "test-gateway", testLogger(), WithAuthenticator(tokenAuthenticator{"valid": "alice"}), WithRouter(r), WithBackendRegistry(reg))
	ts := httptest.NewServer(gw.Handler())
	url := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	c := dial(t, url)
	if result := sendAuth(t, c, "valid"); !result.OK {
		t.Fatalf("auth=%#v", result)
	}
	t.Cleanup(func() { _ = c.Close(); gw.Close(); ts.Close() })
	return gw, c, r, reg
}

func sendBusiness(t *testing.T, c *ws.Conn, mt uint32, id string, payload []byte) protocol.Envelope {
	t.Helper()
	req := protocol.Envelope{Version: protocol.CurrentVersion, MessageType: mt, RequestID: id, Payload: payload, TimestampUnixMS: time.Now().UnixMilli()}
	if err := c.WriteBinary(protocol.Marshal(req)); err != nil {
		t.Fatal(err)
	}
	return readEnvelope(t, c)
}

func TestAuthenticatedBusinessMessageRoutesToBackend(t *testing.T) {
	var got backend.Request
	_, c, _, _ := setupRoutedServer(t, fakeBackendClient{handle: func(_ context.Context, req backend.Request) (backend.Response, error) {
		got = req
		return backend.Response{MessageType: 1002, Payload: []byte("backend-ok")}, nil
	}})
	env := sendBusiness(t, c, 1001, "req-1", []byte("move"))
	if env.MessageType != 1002 || env.RequestID != "req-1" || string(env.Payload) != "backend-ok" {
		t.Fatalf("env=%#v", env)
	}
	if got.UserID != "alice" || got.RoomID != "room-1" || got.MessageType != 1001 || string(got.Payload) != "move" {
		t.Fatalf("req=%#v", got)
	}
}

func TestUnknownBusinessMessageReturnsRoutingError(t *testing.T) {
	gw, c, _, _ := setupRoutedServer(t, fakeBackendClient{handle: func(context.Context, backend.Request) (backend.Response, error) {
		t.Fatal("backend must not be called")
		return backend.Response{}, nil
	}})
	env := sendBusiness(t, c, 1999, "unknown", nil)
	if env.MessageType != protocol.MessageTypeError {
		t.Fatalf("type=%d", env.MessageType)
	}
	er, err := protocol.UnmarshalErrorResponse(env.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if er.ErrorCode != "routing_unknown_message_type" || er.Retryable {
		t.Fatalf("error=%#v", er)
	}
	if gw.ActiveSessionCount() != 1 {
		t.Fatalf("sessions=%d", gw.ActiveSessionCount())
	}
}

func TestBackendTimeoutReturnsControlledErrorAndKeepsSession(t *testing.T) {
	gw, c, _, _ := setupRoutedServer(t, fakeBackendClient{handle: func(ctx context.Context, _ backend.Request) (backend.Response, error) {
		<-ctx.Done()
		return backend.Response{}, ctx.Err()
	}})
	env := sendBusiness(t, c, 1001, "timeout", nil)
	er, err := protocol.UnmarshalErrorResponse(env.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if env.MessageType != protocol.MessageTypeError || er.ErrorCode != "backend_timeout" || !er.Retryable {
		t.Fatalf("env=%#v err=%#v", env, er)
	}
	if gw.ActiveSessionCount() != 1 {
		t.Fatalf("sessions=%d", gw.ActiveSessionCount())
	}
}

func TestBackendUnavailableAndBackendDeclaredErrorAreMapped(t *testing.T) {
	t.Run("unavailable", func(t *testing.T) {
		gw, c, _, _ := setupRoutedServer(t, nil)
		env := sendBusiness(t, c, 1001, "unavailable", nil)
		er, err := protocol.UnmarshalErrorResponse(env.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if er.ErrorCode != "backend_unavailable" || !er.Retryable {
			t.Fatalf("error=%#v", er)
		}
		if gw.ActiveSessionCount() != 1 {
			t.Fatalf("sessions=%d", gw.ActiveSessionCount())
		}
	})
	t.Run("backend error", func(t *testing.T) {
		gw, c, _, _ := setupRoutedServer(t, fakeBackendClient{handle: func(context.Context, backend.Request) (backend.Response, error) {
			return backend.Response{ErrorCode: "room_full"}, nil
		}})
		env := sendBusiness(t, c, 1001, "business-error", nil)
		er, err := protocol.UnmarshalErrorResponse(env.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if er.ErrorCode != "room_full" || er.Retryable {
			t.Fatalf("error=%#v", er)
		}
		if gw.ActiveSessionCount() != 1 {
			t.Fatalf("sessions=%d", gw.ActiveSessionCount())
		}
	})
}

func setupReliableRoutedServer(t *testing.T, cfg config.Config, classifier reliability.Classifier, client backend.Client) (*Server, *ws.Conn) {
	t.Helper()
	r := routing.NewStaticRouter()
	r.SetMessageBackend(1001, "room")
	r.SetUserRoom("alice", "room-1")
	r.SetRoomInstance("room-1", routing.BackendInstance{ID: "room-a", BackendType: "room", Address: "inproc"})
	reg := backend.NewRegistry()
	reg.Set("room-a", client)
	gw := New(cfg, "test-gateway", testLogger(), WithAuthenticator(tokenAuthenticator{"valid": "alice"}), WithRouter(r), WithBackendRegistry(reg), WithReliabilityClassifier(classifier))
	ts := httptest.NewServer(gw.Handler())
	url := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	c := dial(t, url)
	if result := sendAuth(t, c, "valid"); !result.OK {
		t.Fatalf("auth=%#v", result)
	}
	t.Cleanup(func() { _ = c.Close(); gw.Close(); ts.Close() })
	return gw, c
}

func writeEnvelope(t *testing.T, c *ws.Conn, env protocol.Envelope) {
	t.Helper()
	if err := c.WriteBinary(protocol.Marshal(env)); err != nil {
		t.Fatal(err)
	}
}

func TestReliableInboundDuplicateDoesNotRepeatBackendEffect(t *testing.T) {
	cfg := config.Default()
	classifier := reliability.NewStaticClassifier(1001)
	var calls atomic.Int64
	gw, c := setupReliableRoutedServer(t, cfg, classifier, fakeBackendClient{handle: func(context.Context, backend.Request) (backend.Response, error) {
		calls.Add(1)
		return backend.Response{MessageType: 1002, Payload: []byte("ok")}, nil
	}})
	_ = gw
	req := protocol.Envelope{Version: protocol.CurrentVersion, MessageType: 1001, RequestID: "r1", MessageID: "m1", Seq: 1, Payload: []byte("effect")}
	writeEnvelope(t, c, req)
	ack := readEnvelope(t, c)
	if ack.MessageType != protocol.MessageTypeAck || ack.Ack != 1 {
		t.Fatalf("ack=%#v", ack)
	}
	resp := readEnvelope(t, c)
	if resp.MessageType != 1002 {
		t.Fatalf("resp=%#v", resp)
	}
	writeEnvelope(t, c, req)
	ack = readEnvelope(t, c)
	if ack.MessageType != protocol.MessageTypeAck || ack.Ack != 1 {
		t.Fatalf("duplicate ack=%#v", ack)
	}
	time.Sleep(20 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("backend calls=%d", got)
	}
}

func TestReliableInboundOutOfOrderIsRejectedBeforeBackend(t *testing.T) {
	cfg := config.Default()
	classifier := reliability.NewStaticClassifier(1001)
	var calls atomic.Int64
	_, c := setupReliableRoutedServer(t, cfg, classifier, fakeBackendClient{handle: func(context.Context, backend.Request) (backend.Response, error) {
		calls.Add(1)
		return backend.Response{MessageType: 1002}, nil
	}})
	writeEnvelope(t, c, protocol.Envelope{Version: 1, MessageType: 1001, RequestID: "r2", MessageID: "m2", Seq: 2})
	env := readEnvelope(t, c)
	if env.MessageType != protocol.MessageTypeError {
		t.Fatalf("env=%#v", env)
	}
	er, err := protocol.UnmarshalErrorResponse(env.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if er.ErrorCode != "reliable_out_of_order" || !er.Retryable {
		t.Fatalf("error=%#v", er)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("backend calls=%d", got)
	}
}

func TestReliableOutboundRetriesAfterDroppedAckAndStopsAfterDelayedAck(t *testing.T) {
	cfg := config.Default()
	cfg.ReliableRetryInterval = 20 * time.Millisecond
	cfg.ReliableMaxRetries = 3
	classifier := reliability.NewStaticClassifier(1002)
	gw, c := setupReliableRoutedServer(t, cfg, classifier, fakeBackendClient{handle: func(context.Context, backend.Request) (backend.Response, error) {
		return backend.Response{MessageType: 1002, Payload: []byte("reliable")}, nil
	}})
	writeEnvelope(t, c, protocol.Envelope{Version: 1, MessageType: 1001, RequestID: "r", Payload: []byte("x")})
	first := readEnvelope(t, c)
	if first.MessageType != 1002 || first.Seq != 1 || first.MessageID == "" {
		t.Fatalf("first=%#v", first)
	}
	retry := readEnvelope(t, c)
	if retry.Seq != first.Seq || retry.MessageID != first.MessageID || string(retry.Payload) != string(first.Payload) {
		t.Fatalf("retry=%#v first=%#v", retry, first)
	}
	writeEnvelope(t, c, protocol.Envelope{Version: 1, MessageType: protocol.MessageTypeAck, Ack: first.Seq})
	waitFor(t, 250*time.Millisecond, func() bool { return gw.ReliablePendingCount() == 0 })
	time.Sleep(60 * time.Millisecond)
	if got := gw.ReliablePendingCount(); got != 0 {
		t.Fatalf("pending=%d", got)
	}
}

func TestConnectionCloseBeforeAckRetainsReliablePendingUntilGraceExpiry(t *testing.T) {
	cfg := config.Default()
	cfg.SessionGracePeriod = 40 * time.Millisecond
	cfg.ReliableRetryInterval = time.Second
	classifier := reliability.NewStaticClassifier(1002)
	gw, c := setupReliableRoutedServer(t, cfg, classifier, fakeBackendClient{handle: func(context.Context, backend.Request) (backend.Response, error) {
		return backend.Response{MessageType: 1002}, nil
	}})
	writeEnvelope(t, c, protocol.Envelope{Version: 1, MessageType: 1001, RequestID: "r"})
	resp := readEnvelope(t, c)
	if resp.Seq == 0 {
		t.Fatalf("resp=%#v", resp)
	}
	waitFor(t, time.Second, func() bool { return gw.ReliablePendingCount() == 1 })
	_ = c.WriteClose()
	_ = c.Close()
	waitFor(t, time.Second, func() bool { return gw.ConnectionCount() == 0 })
	if got := gw.ReliablePendingCount(); got != 1 {
		t.Fatalf("pending during grace=%d", got)
	}
	if got := gw.ActiveSessionCount(); got != 1 {
		t.Fatalf("sessions during grace=%d", got)
	}
	waitFor(t, time.Second, func() bool { return gw.ReliablePendingCount() == 0 && gw.ActiveSessionCount() == 0 })
}

func TestReliablePendingOverflowClosesConnection(t *testing.T) {
	cfg := config.Default()
	cfg.ReliablePendingLimit = 1
	cfg.ReliableRetryInterval = time.Second
	classifier := reliability.NewStaticClassifier(1002)
	gw, c := setupReliableRoutedServer(t, cfg, classifier, fakeBackendClient{handle: func(context.Context, backend.Request) (backend.Response, error) {
		return backend.Response{MessageType: 1002}, nil
	}})
	writeEnvelope(t, c, protocol.Envelope{Version: 1, MessageType: 1001, RequestID: "r1"})
	_ = readEnvelope(t, c)
	writeEnvelope(t, c, protocol.Envelope{Version: 1, MessageType: 1001, RequestID: "r2"})
	waitFor(t, time.Second, func() bool { return gw.ConnectionCount() == 0 })
}

func TestReliableOutboundStopsAfterMaxRetries(t *testing.T) {
	cfg := config.Default()
	cfg.ReliableRetryInterval = 10 * time.Millisecond
	cfg.ReliableMaxRetries = 1
	classifier := reliability.NewStaticClassifier(1002)
	gw, c := setupReliableRoutedServer(t, cfg, classifier, fakeBackendClient{handle: func(context.Context, backend.Request) (backend.Response, error) {
		return backend.Response{MessageType: 1002, Payload: []byte("must-ack")}, nil
	}})
	writeEnvelope(t, c, protocol.Envelope{Version: 1, MessageType: 1001, RequestID: "r"})
	first := readEnvelope(t, c)
	if first.Seq != 1 {
		t.Fatalf("first=%#v", first)
	}
	retry := readEnvelope(t, c)
	if retry.Seq != first.Seq || retry.MessageID != first.MessageID {
		t.Fatalf("retry=%#v", retry)
	}
	waitFor(t, 500*time.Millisecond, func() bool { return gw.ConnectionCount() == 0 && gw.ReliablePendingCount() == 0 })
}
