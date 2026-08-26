package gateway

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"game-gateway/internal/config"
	"game-gateway/internal/metrics"
	"game-gateway/internal/protocol"
	"game-gateway/internal/ws"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Server struct {
	cfg       config.Config
	gatewayID string
	logger    *slog.Logger
	metrics   *metrics.Metrics
	mu        sync.RWMutex
	conns     map[string]*Connection
	closing   atomic.Bool
}

func New(cfg config.Config, gatewayID string, logger *slog.Logger) *Server {
	return &Server{cfg: cfg, gatewayID: gatewayID, logger: logger.With("gateway_id", gatewayID), metrics: metrics.New(gatewayID), conns: make(map[string]*Connection)}
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
func (s *Server) ConnectionCount() int { s.mu.RLock(); defer s.mu.RUnlock(); return len(s.conns) }
func (s *Server) Close() {
	if !s.closing.CompareAndSwap(false, true) {
		return
	}
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
	s.conns[id] = c
	s.mu.Unlock()
	c.Start()
}
func (s *Server) removeConn(c *Connection) { s.mu.Lock(); delete(s.conns, c.ID()); s.mu.Unlock() }
func (s *Server) handleEnvelope(c *Connection, env protocol.Envelope) {
	if env.MessageType != protocol.MessageTypeEchoRequest {
		return
	}
	resp := protocol.Envelope{Version: protocol.CurrentVersion, MessageType: protocol.MessageTypeEchoResponse, RequestID: env.RequestID, Payload: env.Payload, TimestampUnixMS: time.Now().UnixMilli()}
	if err := c.Enqueue(protocol.Marshal(resp)); err != nil && errors.Is(err, ErrSendQueueFull) {
		c.Close("send_queue_full")
	}
}
func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
