package gateway

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"game-gateway/internal/auth"
	"game-gateway/internal/backend"
	"game-gateway/internal/config"
	"game-gateway/internal/metrics"
	"game-gateway/internal/protocol"
	"game-gateway/internal/reliability"
	"game-gateway/internal/routing"
	"game-gateway/internal/session"
	"game-gateway/internal/ws"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

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

type Server struct {
	cfg         config.Config
	gatewayID   string
	logger      *slog.Logger
	metrics     *metrics.Metrics
	mu          sync.RWMutex
	conns       map[string]*Connection
	closing     atomic.Bool
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
}

func New(cfg config.Config, gatewayID string, logger *slog.Logger, opts ...Option) *Server {
	if cfg.SessionGracePeriod <= 0 {
		cfg.SessionGracePeriod = config.DefaultSessionGracePeriod
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
		reliableStop: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
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
	close(s.heartbeatStop)
	s.heartbeatWG.Wait()
	close(s.reliableStop)
	s.reliableWG.Wait()
	close(s.sessionStop)
	s.sessionWG.Wait()
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

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	if s.closing.Load() {
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
	c := newConnection(id, s.gatewayID, wc, s.cfg.MaxEnvelopeBytes, s.cfg.SendQueueSize, s.cfg.WriteTimeout, s.logger, s.metrics, s.handleEnvelope, s.removeConn)
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

	if s.closing.Load() {
		if ended := s.sessions.TerminateByConn(c.ID()); ended != nil {
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
	if env.MessageType >= protocol.BusinessMessageMin {
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
	current, replaced, err := s.sessions.Register(userID, c.ID())
	if err != nil {
		s.lifecycleMu.Unlock()
		s.metrics.AuthResult("session_create_failed")
		s.sendAuthResult(c, env.RequestID, protocol.AuthResult{OK: false, ErrorCode: "session_create_failed"})
		return
	}
	c.bindIdentity(userID, current.ID)
	if replaced != nil {
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
	resumed, err := s.sessions.Resume(req.ResumeToken, c.ID(), time.Now())
	if err != nil {
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
	s.enqueueRawOrClose(c, protocol.Marshal(env))
}

func (s *Server) enqueueRawOrClose(c *Connection, data []byte) {
	if err := c.Enqueue(data); err != nil && errors.Is(err, ErrSendQueueFull) {
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
		s.metrics.GraceExpired()
	}
	s.graceSessions.Add(-int64(len(expired)))
	s.metrics.SetReliablePending(s.reliability.PendingCount())
	s.updateSessionMetricsLocked()
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
