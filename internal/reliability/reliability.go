package reliability

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	"game-gateway/internal/protocol"
)

var (
	ErrInvalidSessionID  = errors.New("invalid session id")
	ErrInvalidMessageID  = errors.New("invalid message id")
	ErrInvalidSequence   = errors.New("invalid sequence")
	ErrOutOfOrder        = errors.New("out of order sequence")
	ErrMessageIDConflict = errors.New("message id conflict")
	ErrSeqConflict       = errors.New("sequence conflict")
	ErrStaleSequence     = errors.New("stale sequence")
	ErrPendingFull       = errors.New("pending queue full")
	ErrSeqExhausted      = errors.New("sequence exhausted")
)

type DeliveryClass uint8

const (
	DeliveryUnreliable DeliveryClass = iota
	DeliveryReliable
)

type Classifier interface {
	Classify(messageType uint32) DeliveryClass
}

type StaticClassifier struct {
	mu       sync.RWMutex
	reliable map[uint32]struct{}
}

func NewStaticClassifier(types ...uint32) *StaticClassifier {
	c := &StaticClassifier{reliable: make(map[uint32]struct{}, len(types))}
	for _, mt := range types {
		c.reliable[mt] = struct{}{}
	}
	return c
}

func (c *StaticClassifier) SetReliable(messageType uint32, reliable bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if reliable {
		c.reliable[messageType] = struct{}{}
	} else {
		delete(c.reliable, messageType)
	}
}

func (c *StaticClassifier) Classify(messageType uint32) DeliveryClass {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if _, ok := c.reliable[messageType]; ok {
		return DeliveryReliable
	}
	return DeliveryUnreliable
}

type Config struct {
	PendingLimit  int
	DedupWindow   int
	RetryInterval time.Duration
	MaxRetries    int
}

type InboundDecision uint8

const (
	InboundAccepted InboundDecision = iota + 1
	InboundDuplicate
)

type dedupEntry struct {
	MessageID string
	Seq       uint64
}
type pendingEntry struct {
	Envelope   protocol.Envelope
	NextRetry  time.Time
	RetryCount int
}
type sessionState struct {
	lastRecvSeq   uint64
	nextSendSeq   uint64
	sendExhausted bool
	dedupByID     map[string]uint64
	dedupBySeq    map[uint64]string
	dedupOrder    []dedupEntry
	pending       map[uint64]pendingEntry
}

type Retransmission struct {
	SessionID  string
	Envelope   protocol.Envelope
	RetryCount int
}

type Manager struct {
	mu       sync.Mutex
	cfg      Config
	sessions map[string]*sessionState
}

func NewManager(cfg Config) *Manager {
	if cfg.PendingLimit <= 0 {
		cfg.PendingLimit = 1
	}
	if cfg.DedupWindow <= 0 {
		cfg.DedupWindow = 1
	}
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = time.Second
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	return &Manager{cfg: cfg, sessions: make(map[string]*sessionState)}
}

func (m *Manager) stateLocked(sessionID string) *sessionState {
	st := m.sessions[sessionID]
	if st == nil {
		st = &sessionState{nextSendSeq: 1, dedupByID: map[string]uint64{}, dedupBySeq: map[uint64]string{}, pending: map[uint64]pendingEntry{}}
		m.sessions[sessionID] = st
	}
	return st
}

// state is intentionally package-private and only used by focused boundary tests.
func (m *Manager) state(sessionID string) *sessionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stateLocked(sessionID)
}

func (m *Manager) AcceptInbound(sessionID, messageID string, seq uint64) (InboundDecision, error) {
	if sessionID == "" {
		return 0, ErrInvalidSessionID
	}
	if messageID == "" {
		return 0, ErrInvalidMessageID
	}
	if seq == 0 {
		return 0, ErrInvalidSequence
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.stateLocked(sessionID)

	if prior, ok := st.dedupByID[messageID]; ok {
		if prior == seq {
			return InboundDuplicate, nil
		}
		return 0, ErrMessageIDConflict
	}
	if priorID, ok := st.dedupBySeq[seq]; ok {
		if priorID == messageID {
			return InboundDuplicate, nil
		}
		return 0, ErrSeqConflict
	}
	if seq <= st.lastRecvSeq {
		return 0, ErrStaleSequence
	}
	if st.lastRecvSeq == math.MaxUint64 {
		return 0, ErrSeqExhausted
	}
	if seq != st.lastRecvSeq+1 {
		return 0, ErrOutOfOrder
	}

	st.lastRecvSeq = seq
	st.dedupByID[messageID] = seq
	st.dedupBySeq[seq] = messageID
	st.dedupOrder = append(st.dedupOrder, dedupEntry{MessageID: messageID, Seq: seq})
	for len(st.dedupOrder) > m.cfg.DedupWindow {
		old := st.dedupOrder[0]
		st.dedupOrder = st.dedupOrder[1:]
		delete(st.dedupByID, old.MessageID)
		delete(st.dedupBySeq, old.Seq)
	}
	return InboundAccepted, nil
}

func (m *Manager) LastRecvSeq(sessionID string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if st := m.sessions[sessionID]; st != nil {
		return st.lastRecvSeq
	}
	return 0
}

func (m *Manager) DedupEntryCount(sessionID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if st := m.sessions[sessionID]; st != nil {
		return len(st.dedupOrder)
	}
	return 0
}

func (m *Manager) TrackOutbound(sessionID string, env protocol.Envelope, now time.Time) (protocol.Envelope, error) {
	if sessionID == "" {
		return protocol.Envelope{}, ErrInvalidSessionID
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.stateLocked(sessionID)
	if len(st.pending) >= m.cfg.PendingLimit {
		return protocol.Envelope{}, ErrPendingFull
	}
	if st.sendExhausted {
		return protocol.Envelope{}, ErrSeqExhausted
	}
	if st.nextSendSeq == 0 {
		return protocol.Envelope{}, ErrSeqExhausted
	}
	seq := st.nextSendSeq
	if seq == math.MaxUint64 {
		st.sendExhausted = true
	} else {
		st.nextSendSeq++
	}
	if env.MessageID == "" {
		id, err := newMessageID()
		if err != nil {
			return protocol.Envelope{}, err
		}
		env.MessageID = id
	}
	env.Seq = seq
	st.pending[seq] = pendingEntry{Envelope: cloneEnvelope(env), NextRetry: now.Add(m.cfg.RetryInterval)}
	return env, nil
}

func (m *Manager) Ack(sessionID string, ack uint64) int {
	if sessionID == "" || ack == 0 {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.sessions[sessionID]
	if st == nil {
		return 0
	}
	removed := 0
	for seq := range st.pending {
		if seq <= ack {
			delete(st.pending, seq)
			removed++
		}
	}
	return removed
}

func (m *Manager) CollectDue(now time.Time) (due []Retransmission, exhausted []Retransmission) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for sessionID, st := range m.sessions {
		due, exhausted = m.collectDueLocked(sessionID, st, now, due, exhausted)
	}
	return due, exhausted
}

func (m *Manager) CollectDueForSessions(now time.Time, sessionIDs []string) (due []Retransmission, exhausted []Retransmission) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := make(map[string]struct{}, len(sessionIDs))
	for _, sessionID := range sessionIDs {
		if _, ok := seen[sessionID]; ok {
			continue
		}
		seen[sessionID] = struct{}{}
		if st := m.sessions[sessionID]; st != nil {
			due, exhausted = m.collectDueLocked(sessionID, st, now, due, exhausted)
		}
	}
	return due, exhausted
}

func (m *Manager) collectDueLocked(sessionID string, st *sessionState, now time.Time, due, exhausted []Retransmission) ([]Retransmission, []Retransmission) {
	for seq, p := range st.pending {
		if now.Before(p.NextRetry) {
			continue
		}
		if p.RetryCount >= m.cfg.MaxRetries {
			exhausted = append(exhausted, Retransmission{SessionID: sessionID, Envelope: cloneEnvelope(p.Envelope), RetryCount: p.RetryCount})
			delete(st.pending, seq)
			continue
		}
		p.RetryCount++
		p.NextRetry = now.Add(m.cfg.RetryInterval)
		st.pending[seq] = p
		due = append(due, Retransmission{SessionID: sessionID, Envelope: cloneEnvelope(p.Envelope), RetryCount: p.RetryCount})
	}
	return due, exhausted
}

func (m *Manager) Pending(sessionID string) []protocol.Envelope {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.sessions[sessionID]
	if st == nil {
		return nil
	}
	pending := make([]protocol.Envelope, 0, len(st.pending))
	for _, entry := range st.pending {
		pending = append(pending, cloneEnvelope(entry.Envelope))
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].Seq < pending[j].Seq })
	return pending
}

func (m *Manager) PendingCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0
	for _, st := range m.sessions {
		total += len(st.pending)
	}
	return total
}

func (m *Manager) SessionPendingCount(sessionID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if st := m.sessions[sessionID]; st != nil {
		return len(st.pending)
	}
	return 0
}

func (m *Manager) IsPending(sessionID string, seq uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.sessions[sessionID]
	if st == nil {
		return false
	}
	_, ok := st.pending[seq]
	return ok
}

func (m *Manager) RemoveSession(sessionID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.sessions[sessionID]
	if st == nil {
		return 0
	}
	n := len(st.pending)
	delete(m.sessions, sessionID)
	return n
}

func cloneEnvelope(e protocol.Envelope) protocol.Envelope {
	e.Payload = append([]byte(nil), e.Payload...)
	return e
}
func newMessageID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
