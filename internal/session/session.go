package session

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

var (
	ErrInvalidUserID = errors.New("invalid user id")
	ErrInvalidConnID = errors.New("invalid conn id")
)

type Session struct {
	ID        string
	UserID    string
	ConnID    string
	CreatedAt time.Time
}

type Manager struct {
	mu     sync.RWMutex
	byUser map[string]Session
	byConn map[string]Session
}

func NewManager() *Manager {
	return &Manager{byUser: make(map[string]Session), byConn: make(map[string]Session)}
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
	current := Session{ID: id, UserID: userID, ConnID: connID, CreatedAt: time.Now()}

	m.mu.Lock()
	defer m.mu.Unlock()
	var replaced *Session
	if old, ok := m.byUser[userID]; ok {
		copyOld := old
		replaced = &copyOld
		delete(m.byConn, old.ConnID)
	}
	m.byUser[userID] = current
	m.byConn[connID] = current
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
	}
	copyS := s
	return &copyS
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

func (m *Manager) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.byUser)
}

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
