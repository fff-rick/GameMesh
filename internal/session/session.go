package session

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

var (
	ErrInvalidUserID         = errors.New("invalid user id")
	ErrInvalidConnID         = errors.New("invalid conn id")
	ErrInvalidResumeToken    = errors.New("invalid resume token")
	ErrSessionNotRecoverable = errors.New("session not recoverable")
)

type Session struct {
	ID            string
	UserID        string
	ConnID        string
	RoomID        string
	CreatedAt     time.Time
	ResumeToken   string
	GraceDeadline time.Time
}

type Manager struct {
	mu            sync.RWMutex
	gracePeriod   time.Duration
	byUser        map[string]Session
	byConn        map[string]Session
	byResumeToken map[string]Session
}

func NewManager(gracePeriod ...time.Duration) *Manager {
	period := time.Minute
	if len(gracePeriod) > 0 {
		period = gracePeriod[0]
	}
	return &Manager{
		gracePeriod:   period,
		byUser:        make(map[string]Session),
		byConn:        make(map[string]Session),
		byResumeToken: make(map[string]Session),
	}
}

func (m *Manager) Register(userID, connID string) (Session, *Session, error) {
	if userID == "" {
		return Session{}, nil, ErrInvalidUserID
	}
	if connID == "" {
		return Session{}, nil, ErrInvalidConnID
	}
	id, err := newID()
	if err != nil {
		return Session{}, nil, err
	}
	resumeToken, err := newToken()
	if err != nil {
		return Session{}, nil, err
	}
	current := Session{ID: id, UserID: userID, ConnID: connID, CreatedAt: time.Now(), ResumeToken: resumeToken}

	m.mu.Lock()
	defer m.mu.Unlock()
	var replaced *Session
	if old, ok := m.byUser[userID]; ok {
		copyOld := old
		replaced = &copyOld
		delete(m.byConn, old.ConnID)
		delete(m.byResumeToken, old.ResumeToken)
	}
	m.byUser[userID] = current
	m.byConn[connID] = current
	m.byResumeToken[current.ResumeToken] = current
	return current, replaced, nil
}

func (m *Manager) TerminateByConn(connID string) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.byConn[connID]
	if !ok {
		return nil
	}
	delete(m.byConn, connID)
	if current, ok := m.byUser[s.UserID]; ok && current.ID == s.ID {
		delete(m.byUser, s.UserID)
		delete(m.byResumeToken, s.ResumeToken)
	}
	copyS := s
	return &copyS
}

func (m *Manager) Disconnect(connID string, now time.Time) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.byConn[connID]
	if !ok || !s.GraceDeadline.IsZero() {
		return nil
	}
	current, ok := m.byUser[s.UserID]
	if !ok || current.ID != s.ID || current.ConnID != connID {
		return nil
	}
	s.GraceDeadline = now.Add(m.gracePeriod)
	m.byUser[s.UserID] = s
	m.byConn[connID] = s
	m.byResumeToken[s.ResumeToken] = s
	copyS := s
	return &copyS
}

func (m *Manager) Resume(token, connID string, now time.Time) (Session, error) {
	if connID == "" {
		return Session{}, ErrInvalidConnID
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.byResumeToken[token]
	if !ok {
		return Session{}, ErrInvalidResumeToken
	}
	if s.GraceDeadline.IsZero() {
		return Session{}, ErrSessionNotRecoverable
	}
	if !now.Before(s.GraceDeadline) {
		return Session{}, ErrInvalidResumeToken
	}
	newResumeToken, err := newToken()
	if err != nil {
		return Session{}, err
	}
	delete(m.byConn, s.ConnID)
	delete(m.byResumeToken, s.ResumeToken)
	s.ConnID = connID
	s.GraceDeadline = time.Time{}
	s.ResumeToken = newResumeToken
	m.byUser[s.UserID] = s
	m.byConn[connID] = s
	m.byResumeToken[s.ResumeToken] = s
	return s, nil
}

func (m *Manager) Expire(now time.Time) []Session {
	m.mu.Lock()
	defer m.mu.Unlock()

	var expired []Session
	for userID, s := range m.byUser {
		if s.GraceDeadline.IsZero() || now.Before(s.GraceDeadline) {
			continue
		}
		delete(m.byUser, userID)
		if byConn, ok := m.byConn[s.ConnID]; ok && byConn.ID == s.ID {
			delete(m.byConn, s.ConnID)
		}
		if byToken, ok := m.byResumeToken[s.ResumeToken]; ok && byToken.ID == s.ID {
			delete(m.byResumeToken, s.ResumeToken)
		}
		expired = append(expired, s)
	}
	return expired
}

func (m *Manager) ByConn(connID string) (Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.byConn[connID]
	return s, ok
}

func (m *Manager) ByUser(userID string) (Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.byUser[userID]
	return s, ok
}

// SetRoom records the last successfully resolved room for later in-process
// routing recovery. It only updates an existing Session.
func (m *Manager) SetRoom(sessionID, roomID string) bool {
	if sessionID == "" || roomID == "" {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for userID, s := range m.byUser {
		if s.ID != sessionID {
			continue
		}
		s.RoomID = roomID
		m.byUser[userID] = s
		m.byConn[s.ConnID] = s
		m.byResumeToken[s.ResumeToken] = s
		return true
	}
	return false
}

func (m *Manager) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.byUser)
}

func newID() (string, error) {
	return newToken()
}

func newToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
