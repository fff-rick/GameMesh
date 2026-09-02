package gateway

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"errors"
	"game-gateway/internal/auth"
	"game-gateway/internal/backend"
	"game-gateway/internal/config"
	"game-gateway/internal/metrics"
	"game-gateway/internal/presence"
	"game-gateway/internal/protocol"
	"game-gateway/internal/reliability"
	"game-gateway/internal/routing"
	"game-gateway/internal/session"
	"game-gateway/internal/statesync"
	"game-gateway/internal/ws"
	"hash/fnv"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	state_syncv1 "github.com/xin/gsss/api/state_sync/v1"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"
)

//go:embed demo.html
var demoHTML []byte

type Option func(*Server)

func WithAuthenticator(a auth.Authenticator) Option {
	return func(s *Server) {
		if a != nil {
			s.authenticator = a
		}
	}
}

func WithRouter(r routing.Resolver) Option {
	return func(s *Server) {
		if r != nil {
			s.router = r
		}
	}
}

func WithBackendRegistry(r *backend.Registry) Option {
	return func(s *Server) {
		if r != nil {
			s.backends = r
		}
	}
}

func WithReliabilityClassifier(c reliability.Classifier) Option {
	return func(s *Server) {
		if c != nil {
			s.reliabilityClassifier = c
		}
	}
}

// WithPresenceRegistry enables Stage 6 distributed user ownership. Leaving it
// unset preserves the Stage 5 single-node behaviour.
func WithPresenceRegistry(r presence.Registry) Option {
	return func(s *Server) { s.presence = r }
}

// WithStateSyncClient attaches the optional trusted GSSS stream adapter.
func WithStateSyncClient(c *statesync.Client) Option { return func(s *Server) { s.stateSync = c } }

type Server struct {
	cfg         config.Config
	gatewayID   string
	logger      *slog.Logger
	metrics     *metrics.Metrics
	mu          sync.RWMutex
	conns       map[string]*Connection
	closing     atomic.Bool
	draining    atomic.Bool
	lifecycleMu sync.Mutex

	authenticator auth.Authenticator
	sessions      *session.Manager
	unauthRejects atomic.Uint64
	graceSessions atomic.Int64
	heartbeatStop chan struct{}
	heartbeatWG   sync.WaitGroup
	sessionStop   chan struct{}
	sessionWG     sync.WaitGroup
	router        routing.Resolver
	backends      *backend.Registry
	backendCaller *backend.Caller

	reliabilityClassifier reliability.Classifier
	reliability           *reliability.Manager
	reliableStop          chan struct{}
	reliableWG            sync.WaitGroup
	presence              presence.Registry
	presenceStop          chan struct{}
	presenceWG            sync.WaitGroup
	leases                map[string]presence.Owner // conn ID -> current fenced lease; lifecycleMu protected
	inboundLimiter        *rate.Limiter
	backendSlots          chan struct{}
	stateSync             *statesync.Client
	stateSyncBindings     map[string]stateSyncBinding // session ID -> Match/Player; lifecycleMu protected
	stateSyncInputSeq     map[stateSyncBinding]uint64 // Match/Player -> Gateway-owned contiguous sequence; lifecycleMu protected
}

type stateSyncBinding struct {
	matchID  string
	playerID uint64
}

func New(cfg config.Config, gatewayID string, logger *slog.Logger, opts ...Option) *Server {
	if cfg.SessionGracePeriod <= 0 {
		cfg.SessionGracePeriod = config.DefaultSessionGracePeriod
	}
	if cfg.ConnectionRate <= 0 {
		cfg.ConnectionRate = config.DefaultConnectionRate
	}
	if cfg.ConnectionRateBurst <= 0 {
		cfg.ConnectionRateBurst = config.DefaultConnectionBurst
	}
	if cfg.GlobalRate <= 0 {
		cfg.GlobalRate = config.DefaultGlobalRate
	}
	if cfg.GlobalRateBurst <= 0 {
		cfg.GlobalRateBurst = config.DefaultGlobalBurst
	}
	if cfg.BackendMaxInFlight <= 0 {
		cfg.BackendMaxInFlight = config.DefaultBackendMaxInFlight
	}
	s := &Server{
		cfg:                   cfg,
		gatewayID:             gatewayID,
		logger:                logger.With("gateway_id", gatewayID),
		metrics:               metrics.New(gatewayID),
		conns:                 make(map[string]*Connection),
		authenticator:         auth.DevAuthenticator{},
		sessions:              session.NewManager(cfg.SessionGracePeriod),
		heartbeatStop:         make(chan struct{}),
		sessionStop:           make(chan struct{}),
		router:                routing.NewStaticRouter(),
		backends:              backend.NewRegistry(),
		backendCaller:         backend.NewCaller(cfg.BackendRPCTimeout),
		reliabilityClassifier: reliability.NewStaticClassifier(),
		reliability: reliability.NewManager(reliability.Config{
			PendingLimit: cfg.ReliablePendingLimit, DedupWindow: cfg.ReliableDedupWindow, RetryInterval: cfg.ReliableRetryInterval, MaxRetries: cfg.ReliableMaxRetries,
		}),
		reliableStop:      make(chan struct{}),
		presenceStop:      make(chan struct{}),
		leases:            make(map[string]presence.Owner),
		inboundLimiter:    rate.NewLimiter(rate.Limit(cfg.GlobalRate), cfg.GlobalRateBurst),
		backendSlots:      make(chan struct{}, cfg.BackendMaxInFlight),
		stateSyncBindings: make(map[string]stateSyncBinding),
		stateSyncInputSeq: make(map[stateSyncBinding]uint64),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.stateSync != nil {
		s.stateSync.SetHandler(statesync.Handler{Snapshot: s.handleStateSyncSnapshot, Control: s.handleStateSyncControl})
	}
	if s.cfg.HeartbeatCheckInterval > 0 && s.cfg.IdleTimeout > 0 {
		s.heartbeatWG.Add(1)
		go s.heartbeatLoop()
	}
	if s.cfg.ReliableRetryInterval > 0 {
		s.reliableWG.Add(1)
		go s.reliableLoop()
	}
	s.sessionWG.Add(1)
	go s.sessionExpiryLoop()
	if s.presence != nil {
		s.presenceWG.Add(2)
		go s.presenceRenewLoop()
		go s.presenceSubscriptionLoop()
	}
	return s
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		s.metrics.WritePrometheus(w)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/demo/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(demoHTML)
	})
	return mux
}

func (s *Server) ConnectionCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.conns)
}
func (s *Server) ActiveSessionCount() int                 { return s.sessions.ActiveCount() }
func (s *Server) UnauthenticatedBusinessRejected() uint64 { return s.unauthRejects.Load() }
func (s *Server) ReliablePendingCount() int               { return s.reliability.PendingCount() }

func (s *Server) Close() {
	if !s.closing.CompareAndSwap(false, true) {
		return
	}
	if s.stateSync != nil {
		s.stateSync.Close()
	}
	close(s.heartbeatStop)
	s.heartbeatWG.Wait()
	close(s.reliableStop)
	s.reliableWG.Wait()
	close(s.sessionStop)
	s.sessionWG.Wait()
	close(s.presenceStop)
	s.presenceWG.Wait()
	s.mu.RLock()
	list := make([]*Connection, 0, len(s.conns))
	for _, c := range s.conns {
		list = append(list, c)
	}
	s.mu.RUnlock()
	for _, c := range list {
		c.Close("server_shutdown")
	}
	for _, c := range list {
		c.Wait()
	}
	s.lifecycleMu.Lock()
	expired := s.sessions.Expire(time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC))
	for _, ended := range expired {
		s.reliability.RemoveSession(ended.ID)
	}
	if len(expired) > 0 {
		s.graceSessions.Add(-int64(len(expired)))
	}
	s.metrics.SetReliablePending(s.reliability.PendingCount())
	s.updateSessionMetricsLocked()
	s.lifecycleMu.Unlock()
}

// BeginDrain rejects new upgrades and new business work while allowing work
// already admitted to finish during Drain.
func (s *Server) BeginDrain() bool {
	if s.closing.Load() {
		return false
	}
	if !s.draining.CompareAndSwap(false, true) {
		return false
	}
	s.metrics.Shutdown("drain_started")
	return true
}

// Drain waits for existing connections to leave. At ctx expiry it force-closes
// them and returns the context error; in both cases the Server is terminal.
func (s *Server) Drain(ctx context.Context) error {
	s.BeginDrain()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if s.ConnectionCount() == 0 {
			s.metrics.Shutdown("drain_completed")
			s.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			s.metrics.Shutdown("drain_timeout")
			s.Close()
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	if s.closing.Load() || s.draining.Load() {
		http.Error(w, "gateway shutting down", http.StatusServiceUnavailable)
		return
	}
	wc, err := ws.Upgrade(w, r)
	if err != nil {
		return
	}
	if s.closing.Load() {
		_ = wc.Close()
		return
	}
	id, err := newID()
	if err != nil {
		_ = wc.Close()
		return
	}
	c := newConnection(id, s.gatewayID, wc, s.cfg.MaxEnvelopeBytes, s.cfg.SendQueueSize, s.cfg.WriteTimeout, s.logger, s.metrics, s.handleEnvelope, s.removeConn, rate.NewLimiter(rate.Limit(s.cfg.ConnectionRate), s.cfg.ConnectionRateBurst))
	s.mu.Lock()
	if s.closing.Load() {
		s.mu.Unlock()
		_ = wc.Close()
		return
	}
	s.conns[id] = c
	s.mu.Unlock()
	c.Start()
}

func (s *Server) removeConn(c *Connection) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	s.mu.Lock()
	delete(s.conns, c.ID())
	s.mu.Unlock()
	if lease, ok := s.leases[c.ID()]; ok {
		delete(s.leases, c.ID())
		s.metrics.SetPresenceLeases(len(s.leases))
		s.releaseLease(lease)
	}

	if s.closing.Load() {
		if ended := s.sessions.TerminateByConn(c.ID()); ended != nil {
			s.removeStateSyncBindingLocked(ended.ID, state_syncv1.LeaveReason_LEAVE_REASON_DISCONNECTED)
			s.reliability.RemoveSession(ended.ID)
			s.metrics.SetReliablePending(s.reliability.PendingCount())
			s.updateSessionMetricsLocked()
			s.logger.Info("session terminated", "session_id", ended.ID, "user_id", ended.UserID, "conn_id", ended.ConnID)
		}
		return
	}
	if disconnected := s.sessions.Disconnect(c.ID(), time.Now()); disconnected != nil {
		s.graceSessions.Add(1)
		s.updateSessionMetricsLocked()
		s.logger.Info("session entered grace", "session_id", disconnected.ID, "user_id", disconnected.UserID, "conn_id", disconnected.ConnID)
	}
}

func (s *Server) handleEnvelope(c *Connection, env protocol.Envelope) {
	if !s.inboundLimiter.Allow() {
		s.metrics.RateLimited("global")
		return
	}
	if env.MessageType == protocol.MessageTypeAuthRequest {
		s.handleAuth(c, env)
		return
	}
	if env.MessageType == protocol.MessageTypeResumeRequest {
		s.handleResume(c, env)
		return
	}
	if env.MessageType == protocol.MessageTypeAck {
		if c.Authenticated() {
			s.reliability.Ack(c.SessionID(), env.Ack)
			s.metrics.SetReliablePending(s.reliability.PendingCount())
		}
		return
	}
	if env.MessageType == protocol.MessageTypeHeartbeatRequest {
		if c.Authenticated() {
			resp := protocol.Envelope{Version: protocol.CurrentVersion, MessageType: protocol.MessageTypeHeartbeatResponse, RequestID: env.RequestID, TimestampUnixMS: time.Now().UnixMilli()}
			s.sendEnvelope(c, resp)
		}
		return
	}
	if env.MessageType >= protocol.BusinessMessageMin && !c.Authenticated() {
		s.unauthRejects.Add(1)
		c.logger.Warn("unauthenticated business message rejected", "message_type", env.MessageType, "request_id", env.RequestID)
		return
	}
	if c.Authenticated() && env.MessageType == statesync.MessageTypeInput {
		s.handleStateSyncInput(c, env)
		return
	}
	if c.Authenticated() && env.MessageType == statesync.MessageTypeSnapshotAck {
		s.handleStateSyncAck(c, env)
		return
	}
	if env.MessageType >= protocol.BusinessMessageMin {
		if s.draining.Load() {
			s.metrics.Shutdown("business_rejected_draining")
			s.sendError(c, env.RequestID, "server_draining", true)
			return
		}
		if s.reliabilityClassifier.Classify(env.MessageType) == reliability.DeliveryReliable {
			if !s.acceptReliableInbound(c, env) {
				return
			}
		}
		s.handleBusiness(c, env)
		return
	}
	if env.MessageType != protocol.MessageTypeEchoRequest {
		return
	}
	resp := protocol.Envelope{Version: protocol.CurrentVersion, MessageType: protocol.MessageTypeEchoResponse, RequestID: env.RequestID, Payload: env.Payload, TimestampUnixMS: time.Now().UnixMilli()}
	s.sendEnvelope(c, resp)
}

func (s *Server) handleBusiness(c *Connection, env protocol.Envelope) {
	route, err := s.router.Resolve(c.UserID(), env.MessageType)
	if err != nil {
		s.sendRoutingError(c, env.RequestID, err)
		return
	}
	s.sessions.SetRoom(c.SessionID(), route.RoomID)
	client, err := s.backends.Get(route.Instance.ID)
	if err != nil {
		s.metrics.BackendRPC(route.BackendType, "Handle", "unavailable", 0)
		s.sendError(c, env.RequestID, "backend_unavailable", true)
		return
	}
	select {
	case s.backendSlots <- struct{}{}:
		defer func() { <-s.backendSlots }()
	default:
		s.metrics.BackendRejected()
		s.sendError(c, env.RequestID, "backend_overloaded", true)
		return
	}
	started := time.Now()
	resp, err := s.backendCaller.Call(c.ctx, client, backend.Request{
		UserID: c.UserID(), SessionID: c.SessionID(), RoomID: route.RoomID, MessageType: env.MessageType, RequestID: env.RequestID, Payload: append([]byte(nil), env.Payload...), TimestampUnixMS: env.TimestampUnixMS,
	})
	if err != nil {
		if errors.Is(err, backend.ErrTimeout) {
			s.metrics.BackendRPC(route.BackendType, "Handle", "timeout", time.Since(started))
			s.sendError(c, env.RequestID, "backend_timeout", true)
			return
		}
		if errors.Is(err, backend.ErrUnavailable) {
			s.metrics.BackendRPC(route.BackendType, "Handle", "unavailable", time.Since(started))
			s.sendError(c, env.RequestID, "backend_unavailable", true)
			return
		}
		var be *backend.BackendError
		if errors.As(err, &be) {
			s.metrics.BackendRPC(route.BackendType, "Handle", "backend_error", time.Since(started))
			s.sendError(c, env.RequestID, be.Code, false)
			return
		}
		s.metrics.BackendRPC(route.BackendType, "Handle", "internal_error", time.Since(started))
		s.sendError(c, env.RequestID, "backend_internal", false)
		return
	}
	s.metrics.BackendRPC(route.BackendType, "Handle", "success", time.Since(started))
	outType := resp.MessageType
	if outType < protocol.BusinessMessageMin {
		s.sendError(c, env.RequestID, "backend_invalid_response", false)
		return
	}
	s.sendEnvelope(c, protocol.Envelope{Version: protocol.CurrentVersion, MessageType: outType, RequestID: env.RequestID, Payload: resp.Payload, TimestampUnixMS: time.Now().UnixMilli()})
}

func (s *Server) sendRoutingError(c *Connection, requestID string, err error) {
	switch {
	case errors.Is(err, routing.ErrUnknownMessageType):
		s.sendError(c, requestID, "routing_unknown_message_type", false)
	case errors.Is(err, routing.ErrUserRoomNotFound):
		s.sendError(c, requestID, "routing_user_room_not_found", true)
	case errors.Is(err, routing.ErrRoomInstanceNotFound), errors.Is(err, routing.ErrBackendTypeMismatch):
		s.sendError(c, requestID, "routing_backend_unavailable", true)
	default:
		s.sendError(c, requestID, "routing_internal", false)
	}
}

func (s *Server) sendError(c *Connection, requestID, code string, retryable bool) {
	s.sendEnvelope(c, protocol.Envelope{Version: protocol.CurrentVersion, MessageType: protocol.MessageTypeError, RequestID: requestID, Payload: protocol.MarshalErrorResponse(protocol.ErrorResponse{ErrorCode: code, Retryable: retryable}), TimestampUnixMS: time.Now().UnixMilli()})
}

func (s *Server) handleAuth(c *Connection, env protocol.Envelope) {
	if c.Authenticated() {
		s.metrics.AuthResult("already_authenticated")
		s.sendAuthResult(c, env.RequestID, protocol.AuthResult{OK: false, ErrorCode: "already_authenticated"})
		return
	}
	req, err := protocol.UnmarshalAuthRequest(env.Payload)
	if err != nil {
		s.metrics.AuthResult("invalid_auth_payload")
		s.sendAuthResult(c, env.RequestID, protocol.AuthResult{OK: false, ErrorCode: "invalid_auth_payload"})
		return
	}
	userID, err := s.authenticator.Authenticate(c.ctx, req.Token)
	if err != nil || userID == "" {
		s.metrics.AuthResult("invalid_token")
		s.sendAuthResult(c, env.RequestID, protocol.AuthResult{OK: false, ErrorCode: "invalid_token"})
		return
	}
	s.lifecycleMu.Lock()
	if s.closing.Load() || !s.currentOpenConnectionLocked(c) {
		s.lifecycleMu.Unlock()
		return
	}
	lease, previous, err := s.claimLease(c, userID)
	if err != nil {
		s.lifecycleMu.Unlock()
		s.metrics.Presence("claim_error")
		s.sendAuthResult(c, env.RequestID, protocol.AuthResult{OK: false, ErrorCode: "presence_unavailable"})
		return
	}
	current, replaced, err := s.sessions.Register(userID, c.ID())
	if err != nil {
		if lease != nil {
			s.releaseLease(*lease)
		}
		s.lifecycleMu.Unlock()
		s.metrics.AuthResult("session_create_failed")
		s.sendAuthResult(c, env.RequestID, protocol.AuthResult{OK: false, ErrorCode: "session_create_failed"})
		return
	}
	c.bindIdentity(userID, current.ID)
	if lease != nil {
		s.leases[c.ID()] = *lease
		s.metrics.SetPresenceLeases(len(s.leases))
		s.metrics.Presence("claim_success")
	}
	if replaced != nil {
		// A replacement is another connection for the same authenticated user.
		// Preserve its GSSS player and input sequence so a page reload cannot
		// cause a late Leave or reset a contiguous-input stream.
		s.detachStateSyncBindingLocked(replaced.ID)
		s.reliability.RemoveSession(replaced.ID)
		s.metrics.SetReliablePending(s.reliability.PendingCount())
		if !replaced.GraceDeadline.IsZero() {
			s.graceSessions.Add(-1)
		}
	}
	s.updateSessionMetricsLocked()
	s.lifecycleMu.Unlock()
	s.metrics.AuthResult("success")
	s.sendAuthResult(c, env.RequestID, protocol.AuthResult{OK: true, UserID: userID, SessionID: current.ID, ResumeToken: current.ResumeToken})
	if replaced != nil {
		if old := s.connectionByID(replaced.ConnID); old != nil && old.ID() != c.ID() {
			old.Close("duplicate_login_replaced")
		}
	}
	if previous != nil && previous.GatewayID != s.gatewayID {
		go s.publishEviction(*previous)
	}
}

func (s *Server) handleResume(c *Connection, env protocol.Envelope) {
	if c.Authenticated() {
		s.metrics.RecoveryResult("already_authenticated")
		s.sendResumeResult(c, env.RequestID, protocol.ResumeResult{OK: false, ErrorCode: "already_authenticated"})
		return
	}
	req, err := protocol.UnmarshalResumeRequest(env.Payload)
	if err != nil || req.ResumeToken == "" {
		s.metrics.RecoveryResult("resume_token_invalid")
		s.sendResumeResult(c, env.RequestID, protocol.ResumeResult{OK: false, ErrorCode: "resume_token_invalid"})
		return
	}
	s.lifecycleMu.Lock()
	if s.closing.Load() || !s.currentOpenConnectionLocked(c) {
		s.lifecycleMu.Unlock()
		return
	}
	// Resume remains local, but it must reacquire distributed ownership because
	// the old connection released its lease when it entered the grace period.
	userID, ok := s.resumeUserID(req.ResumeToken)
	if !ok {
		s.lifecycleMu.Unlock()
		s.metrics.RecoveryResult("resume_token_invalid")
		s.sendResumeResult(c, env.RequestID, protocol.ResumeResult{OK: false, ErrorCode: "resume_token_invalid"})
		return
	}
	lease, previous, claimErr := s.claimLease(c, userID)
	if claimErr != nil {
		s.lifecycleMu.Unlock()
		s.metrics.Presence("claim_error")
		s.metrics.RecoveryResult("presence_unavailable")
		s.sendResumeResult(c, env.RequestID, protocol.ResumeResult{OK: false, ErrorCode: "presence_unavailable"})
		return
	}
	resumed, err := s.sessions.Resume(req.ResumeToken, c.ID(), time.Now())
	if err != nil {
		if lease != nil {
			s.releaseLease(*lease)
		}
		s.lifecycleMu.Unlock()
		code := "resume_failed"
		if errors.Is(err, session.ErrInvalidResumeToken) || errors.Is(err, session.ErrSessionNotRecoverable) {
			code = "resume_token_invalid"
		}
		s.metrics.RecoveryResult(code)
		s.sendResumeResult(c, env.RequestID, protocol.ResumeResult{OK: false, ErrorCode: code})
		return
	}
	c.bindIdentity(resumed.UserID, resumed.ID)
	if lease != nil {
		s.leases[c.ID()] = *lease
		s.metrics.SetPresenceLeases(len(s.leases))
		s.metrics.Presence("claim_success")
	}
	if resumed.RoomID != "" {
		if binder, ok := s.router.(interface{ SetUserRoom(string, string) }); ok {
			binder.SetUserRoom(resumed.UserID, resumed.RoomID)
		}
	}
	s.graceSessions.Add(-1)
	s.updateSessionMetricsLocked()
	pending := s.reliability.PendingForResume(resumed.ID, req.LastAckSeq, time.Now())
	resumeEnvelope := protocol.Envelope{
		Version:         protocol.CurrentVersion,
		MessageType:     protocol.MessageTypeResumeResult,
		RequestID:       env.RequestID,
		Payload:         protocol.MarshalResumeResult(protocol.ResumeResult{OK: true, SessionID: resumed.ID, ResumeToken: resumed.ResumeToken}),
		TimestampUnixMS: time.Now().UnixMilli(),
	}
	enqueueErr := c.Enqueue(protocol.Marshal(resumeEnvelope))
	if enqueueErr == nil {
		for _, replay := range pending {
			if enqueueErr = c.Enqueue(protocol.Marshal(replay)); enqueueErr != nil {
				break
			}
		}
	}
	s.lifecycleMu.Unlock()
	s.metrics.RecoveryResult("success")
	if errors.Is(enqueueErr, ErrSendQueueFull) {
		c.Close("send_queue_full")
	}
	if previous != nil && previous.GatewayID != s.gatewayID {
		go s.publishEviction(*previous)
	}
}

func (s *Server) resumeUserID(token string) (string, bool) {
	current, ok := s.sessions.ByResumeToken(token)
	return current.UserID, ok
}

func (s *Server) sendAuthResult(c *Connection, requestID string, result protocol.AuthResult) {
	resp := protocol.Envelope{
		Version:         protocol.CurrentVersion,
		MessageType:     protocol.MessageTypeAuthResult,
		RequestID:       requestID,
		Payload:         protocol.MarshalAuthResult(result),
		TimestampUnixMS: time.Now().UnixMilli(),
	}
	s.sendEnvelope(c, resp)
}

func (s *Server) sendResumeResult(c *Connection, requestID string, result protocol.ResumeResult) {
	resp := protocol.Envelope{
		Version:         protocol.CurrentVersion,
		MessageType:     protocol.MessageTypeResumeResult,
		RequestID:       requestID,
		Payload:         protocol.MarshalResumeResult(result),
		TimestampUnixMS: time.Now().UnixMilli(),
	}
	s.sendEnvelope(c, resp)
}

func (s *Server) sendEnvelope(c *Connection, env protocol.Envelope) {
	if c.Authenticated() && s.reliabilityClassifier.Classify(env.MessageType) == reliability.DeliveryReliable {
		s.lifecycleMu.Lock()
		current, currentOK := s.sessions.ByConn(c.ID())
		if s.closing.Load() || !currentOK || current.ID != c.SessionID() || !current.GraceDeadline.IsZero() {
			s.lifecycleMu.Unlock()
			return
		}
		tracked, err := s.reliability.TrackOutbound(c.SessionID(), env, time.Now())
		if err == nil {
			s.metrics.SetReliablePending(s.reliability.PendingCount())
		}
		s.lifecycleMu.Unlock()
		if err != nil {
			if errors.Is(err, reliability.ErrPendingFull) {
				s.metrics.ReliablePendingOverflow()
				c.Close("reliable_pending_full")
				return
			}
			c.Close("reliable_sequence_error")
			return
		}
		env = tracked
	}
	droppable := env.MessageType >= protocol.BusinessMessageMin && s.reliabilityClassifier.Classify(env.MessageType) != reliability.DeliveryReliable
	s.enqueueRaw(c, protocol.Marshal(env), droppable)
}

func (s *Server) enqueueRaw(c *Connection, data []byte, droppable bool) {
	var err error
	if droppable {
		err = c.EnqueueDroppable(data)
	} else {
		err = c.Enqueue(data)
	}
	if errors.Is(err, ErrSendQueueDropped) {
		s.metrics.MessageDropped()
		return
	}
	if errors.Is(err, ErrSendQueueFull) {
		c.Close("send_queue_full")
	}
}

func (s *Server) connectionByID(id string) *Connection {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.conns[id]
}

// currentOpenConnectionLocked reports whether c is still the Gateway's active
// connection for its ID. lifecycleMu must be held so removeConn cannot delete
// the map entry between this check and a Session mutation.
func (s *Server) currentOpenConnectionLocked(c *Connection) bool {
	if c.State() != ConnOpen {
		return false
	}
	s.mu.RLock()
	current := s.conns[c.ID()]
	s.mu.RUnlock()
	return current == c
}

func (s *Server) heartbeatLoop() {
	defer s.heartbeatWG.Done()
	ticker := time.NewTicker(s.cfg.HeartbeatCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.heartbeatStop:
			return
		case now := <-ticker.C:
			s.closeIdleConnections(now)
		}
	}
}

func (s *Server) closeIdleConnections(now time.Time) {
	s.mu.RLock()
	list := make([]*Connection, 0, len(s.conns))
	for _, c := range s.conns {
		list = append(list, c)
	}
	s.mu.RUnlock()
	for _, c := range list {
		if c.State() == ConnOpen && now.Sub(c.LastSeen()) > s.cfg.IdleTimeout {
			if c.Close("heartbeat_timeout") {
				s.metrics.HeartbeatTimeout()
			}
		}
	}
}

func (s *Server) sessionExpiryLoop() {
	defer s.sessionWG.Done()
	ticker := time.NewTicker(sessionExpiryInterval(s.cfg.SessionGracePeriod))
	defer ticker.Stop()
	for {
		select {
		case <-s.sessionStop:
			return
		case now := <-ticker.C:
			s.expireSessions(now)
		}
	}
}

func sessionExpiryInterval(gracePeriod time.Duration) time.Duration {
	interval := gracePeriod / 4
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	if interval > time.Second {
		interval = time.Second
	}
	return interval
}

func (s *Server) expireSessions(now time.Time) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	expired := s.sessions.Expire(now)
	if len(expired) == 0 {
		return
	}
	for _, ended := range expired {
		s.reliability.RemoveSession(ended.ID)
		s.removeStateSyncBindingLocked(ended.ID, state_syncv1.LeaveReason_LEAVE_REASON_DISCONNECTED)
		s.metrics.GraceExpired()
	}
	s.graceSessions.Add(-int64(len(expired)))
	s.metrics.SetReliablePending(s.reliability.PendingCount())
	s.updateSessionMetricsLocked()
}

func (s *Server) handleStateSyncInput(c *Connection, env protocol.Envelope) {
	if s.stateSync == nil {
		s.sendError(c, env.RequestID, "state_sync_unavailable", true)
		return
	}
	route, err := s.router.Resolve(c.UserID(), env.MessageType)
	if err != nil {
		s.sendRoutingError(c, env.RequestID, err)
		return
	}
	var input state_syncv1.PlayerInput
	if err := proto.Unmarshal(env.Payload, &input); err != nil {
		s.sendError(c, env.RequestID, "state_sync_invalid_input", false)
		return
	}
	playerID := stateSyncPlayerID(c.UserID())
	s.lifecycleMu.Lock()
	binding, exists := s.stateSyncBindings[c.SessionID()]
	if exists && (binding.matchID != route.RoomID || binding.playerID != playerID) {
		s.removeStateSyncBindingLocked(c.SessionID(), state_syncv1.LeaveReason_LEAVE_REASON_REQUESTED)
		exists = false
	}
	if !exists {
		if err := s.stateSync.SendJoin(route.RoomID, playerID); err != nil {
			s.lifecycleMu.Unlock()
			s.sendError(c, env.RequestID, "state_sync_unavailable", true)
			return
		}
		binding = stateSyncBinding{matchID: route.RoomID, playerID: playerID}
		s.stateSyncBindings[c.SessionID()] = binding
	}
	s.stateSyncInputSeq[binding]++
	inputSeq := s.stateSyncInputSeq[binding]
	s.lifecycleMu.Unlock()
	// Gateway, never the client, owns identity, routing and input ordering. The
	// latter makes a browser reload safe for GSSS's contiguous-input contract.
	input.MatchId, input.PlayerId, input.InputSeq = route.RoomID, playerID, inputSeq
	if err := s.stateSync.SendInput(&input); err != nil {
		s.sendError(c, env.RequestID, "state_sync_unavailable", true)
	}
}

func (s *Server) handleStateSyncAck(c *Connection, env protocol.Envelope) {
	if s.stateSync == nil {
		s.sendError(c, env.RequestID, "state_sync_unavailable", true)
		return
	}
	var ack state_syncv1.SnapshotAck
	if err := proto.Unmarshal(env.Payload, &ack); err != nil {
		s.sendError(c, env.RequestID, "state_sync_invalid_ack", false)
		return
	}
	s.lifecycleMu.Lock()
	binding, ok := s.stateSyncBindings[c.SessionID()]
	s.lifecycleMu.Unlock()
	if !ok {
		s.sendError(c, env.RequestID, "state_sync_not_joined", true)
		return
	}
	ack.MatchId, ack.PlayerId = binding.matchID, binding.playerID
	if err := s.stateSync.SendAck(&ack); err != nil {
		s.sendError(c, env.RequestID, "state_sync_unavailable", true)
	}
}

func (s *Server) handleStateSyncSnapshot(snapshot *state_syncv1.Snapshot) {
	s.lifecycleMu.Lock()
	var sessionID string
	for id, binding := range s.stateSyncBindings {
		if binding.matchID == snapshot.MatchId && binding.playerID == snapshot.RecipientPlayerId {
			sessionID = id
			break
		}
	}
	s.lifecycleMu.Unlock()
	if sessionID == "" {
		return
	}
	current, ok := s.sessions.ByID(sessionID)
	if !ok {
		return
	}
	if c := s.connectionByID(current.ConnID); c != nil {
		payload, err := proto.Marshal(snapshot)
		if err == nil {
			s.sendEnvelope(c, protocol.Envelope{Version: protocol.CurrentVersion, MessageType: statesync.MessageTypeSnapshot, Payload: payload, TimestampUnixMS: time.Now().UnixMilli()})
		}
	}
}
func (s *Server) handleStateSyncControl(control *state_syncv1.ControlEvent) {
	s.lifecycleMu.Lock()
	var sessionID string
	for id, binding := range s.stateSyncBindings {
		if binding.matchID == control.MatchId && binding.playerID == control.PlayerId {
			sessionID = id
			break
		}
	}
	s.lifecycleMu.Unlock()
	if sessionID == "" {
		return
	}
	current, ok := s.sessions.ByID(sessionID)
	if !ok {
		return
	}
	if c := s.connectionByID(current.ConnID); c != nil {
		if payload, err := proto.Marshal(control); err == nil {
			s.sendEnvelope(c, protocol.Envelope{Version: protocol.CurrentVersion, MessageType: statesync.MessageTypeControl, Payload: payload, TimestampUnixMS: time.Now().UnixMilli()})
		}
	}
}
func (s *Server) removeStateSyncBindingLocked(sessionID string, reason state_syncv1.LeaveReason) {
	binding, ok := s.stateSyncBindings[sessionID]
	if !ok {
		return
	}
	delete(s.stateSyncBindings, sessionID)
	delete(s.stateSyncInputSeq, binding)
	if s.stateSync != nil {
		// Preserve stream ordering: a later Join must never be overtaken by this
		// Leave on the shared GSSS stream.
		_ = s.stateSync.SendLeave(binding.matchID, binding.playerID, reason)
	}
}

// detachStateSyncBindingLocked only removes the local session association. It
// intentionally keeps the GSSS player and its sequence during same-user
// replacement, so reconnecting a browser does not restart authoritative state.
func (s *Server) detachStateSyncBindingLocked(sessionID string) {
	delete(s.stateSyncBindings, sessionID)
}
func stateSyncPlayerID(userID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(userID))
	id := h.Sum64()
	if id == 0 {
		return 1
	}
	return id
}

func (s *Server) updateSessionMetricsLocked() {
	grace := s.graceSessions.Load()
	if grace < 0 {
		grace = 0
	}
	active := int64(s.sessions.ActiveCount()) - grace
	if active < 0 {
		active = 0
	}
	s.metrics.SetSessionCounts(int(active), int(grace))
}

func (s *Server) claimLease(c *Connection, userID string) (*presence.Owner, *presence.Owner, error) {
	if s.presence == nil {
		return nil, nil, nil
	}
	token, err := newID()
	if err != nil {
		return nil, nil, err
	}
	lease := presence.Owner{UserID: userID, GatewayID: s.gatewayID, ConnID: c.ID(), LeaseToken: token}
	ctx, cancel := s.presenceContext(c.ctx)
	defer cancel()
	previous, err := s.presence.Claim(ctx, lease)
	if err != nil {
		return nil, nil, err
	}
	return &lease, previous, nil
}

func (s *Server) releaseLease(lease presence.Owner) {
	if s.presence == nil {
		return
	}
	ctx, cancel := s.presenceContext(context.Background())
	defer cancel()
	_, err := s.presence.Release(ctx, lease)
	if err != nil {
		s.metrics.Presence("release_error")
	} else {
		s.metrics.Presence("release_success")
	}
}

func (s *Server) publishEviction(target presence.Owner) {
	if s.presence == nil {
		return
	}
	ctx, cancel := s.presenceContext(context.Background())
	defer cancel()
	if err := s.presence.PublishEviction(ctx, target); err != nil {
		s.metrics.Presence("eviction_publish_error")
	} else {
		s.metrics.Presence("eviction_published")
	}
}

func (s *Server) presenceContext(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := s.cfg.PresenceOperationTimeout
	if timeout <= 0 {
		timeout = config.DefaultPresenceTimeout
	}
	return context.WithTimeout(parent, timeout)
}

func (s *Server) presenceRenewLoop() {
	defer s.presenceWG.Done()
	interval := s.cfg.PresenceRenewInterval
	if interval <= 0 {
		interval = config.DefaultPresenceRenewEvery
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.presenceStop:
			return
		case <-ticker.C:
			s.renewLeases()
		}
	}
}

func (s *Server) renewLeases() {
	s.lifecycleMu.Lock()
	leases := make([]presence.Owner, 0, len(s.leases))
	for _, lease := range s.leases {
		leases = append(leases, lease)
	}
	s.lifecycleMu.Unlock()
	for _, lease := range leases {
		ctx, cancel := s.presenceContext(context.Background())
		renewed, err := s.presence.Renew(ctx, lease)
		cancel()
		if err != nil {
			s.metrics.Presence("renew_error")
			continue
		}
		if renewed {
			s.metrics.Presence("renew_success")
			continue
		}
		s.metrics.Presence("lease_fenced")
		if c := s.connectionByID(lease.ConnID); c != nil {
			c.Close("presence_lease_fenced")
		}
	}
}

func (s *Server) presenceSubscriptionLoop() {
	defer s.presenceWG.Done()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { <-s.presenceStop; cancel() }()
	for ctx.Err() == nil {
		err := s.presence.Subscribe(ctx, func(target presence.Owner) {
			if target.GatewayID != s.gatewayID {
				return
			}
			if c := s.connectionByID(target.ConnID); c != nil {
				s.metrics.Presence("eviction_received")
				c.Close("duplicate_login_replaced")
			}
		})
		if err != nil && ctx.Err() == nil {
			s.metrics.Presence("subscription_error")
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func (s *Server) acceptReliableInbound(c *Connection, env protocol.Envelope) bool {
	decision, err := s.reliability.AcceptInbound(c.SessionID(), env.MessageID, env.Seq)
	if err == nil {
		if decision == reliability.InboundDuplicate {
			s.metrics.ReliableDedup()
			s.sendAck(c, s.reliability.LastRecvSeq(c.SessionID()))
			return false
		}
		s.sendAck(c, env.Seq)
		return true
	}
	switch {
	case errors.Is(err, reliability.ErrStaleSequence):
		s.metrics.ReliableDedup()
		s.sendAck(c, s.reliability.LastRecvSeq(c.SessionID()))
	case errors.Is(err, reliability.ErrOutOfOrder):
		s.metrics.ReliableOutOfOrder()
		s.sendError(c, env.RequestID, "reliable_out_of_order", true)
	case errors.Is(err, reliability.ErrInvalidMessageID), errors.Is(err, reliability.ErrInvalidSequence):
		s.sendError(c, env.RequestID, "reliable_invalid_envelope", false)
	case errors.Is(err, reliability.ErrMessageIDConflict), errors.Is(err, reliability.ErrSeqConflict):
		s.sendError(c, env.RequestID, "reliable_sequence_conflict", false)
	case errors.Is(err, reliability.ErrSeqExhausted):
		s.sendError(c, env.RequestID, "reliable_sequence_exhausted", false)
	default:
		s.sendError(c, env.RequestID, "reliable_internal", false)
	}
	return false
}

func (s *Server) sendAck(c *Connection, ack uint64) {
	if ack == 0 {
		return
	}
	s.sendEnvelope(c, protocol.Envelope{Version: protocol.CurrentVersion, MessageType: protocol.MessageTypeAck, Ack: ack, TimestampUnixMS: time.Now().UnixMilli()})
}

func (s *Server) connectionBySessionID(sessionID string) *Connection {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.conns {
		if c.SessionID() == sessionID && c.State() == ConnOpen {
			return c
		}
	}
	return nil
}

func (s *Server) reliableLoop() {
	defer s.reliableWG.Done()
	ticker := time.NewTicker(s.cfg.ReliableRetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.reliableStop:
			return
		case now := <-ticker.C:
			s.lifecycleMu.Lock()
			s.mu.RLock()
			active := make(map[string]*Connection)
			for _, c := range s.conns {
				if !c.Authenticated() || c.State() != ConnOpen {
					continue
				}
				current, ok := s.sessions.ByConn(c.ID())
				if ok && current.ID == c.SessionID() && current.GraceDeadline.IsZero() {
					active[current.ID] = c
				}
			}
			s.mu.RUnlock()
			activeSessionIDs := make([]string, 0, len(active))
			for sessionID := range active {
				activeSessionIDs = append(activeSessionIDs, sessionID)
			}
			due, exhausted := s.reliability.CollectDueForSessions(now, activeSessionIDs)
			s.metrics.SetReliablePending(s.reliability.PendingCount())
			var closeFull []*Connection
			for _, item := range due {
				if !s.reliability.IsPending(item.SessionID, item.Envelope.Seq) {
					continue
				}
				if c := active[item.SessionID]; c != nil {
					s.metrics.ReliableRetry()
					if err := c.Enqueue(protocol.Marshal(item.Envelope)); errors.Is(err, ErrSendQueueFull) {
						closeFull = append(closeFull, c)
					}
				}
			}
			var closeExhausted []*Connection
			for _, item := range exhausted {
				s.metrics.ReliableRetryExhausted()
				if c := active[item.SessionID]; c != nil {
					closeExhausted = append(closeExhausted, c)
				}
			}
			s.lifecycleMu.Unlock()
			for _, c := range closeFull {
				c.Close("send_queue_full")
			}
			for _, c := range closeExhausted {
				c.Close("reliable_retry_exhausted")
			}
		}
	}
}

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
