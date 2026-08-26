package routing

import (
	"errors"
	"sync"
)

var (
	ErrUnknownMessageType   = errors.New("unknown message type route")
	ErrUserRoomNotFound     = errors.New("user room route not found")
	ErrRoomInstanceNotFound = errors.New("room backend instance route not found")
	ErrBackendTypeMismatch  = errors.New("backend type mismatch")
)

type BackendInstance struct {
	ID          string
	BackendType string
	Address     string
}

type Route struct {
	BackendType string
	RoomID      string
	Instance    BackendInstance
}

type Resolver interface {
	Resolve(userID string, messageType uint32) (Route, error)
}

type StaticRouter struct {
	mu             sync.RWMutex
	messageBackend map[uint32]string
	userRoom       map[string]string
	roomInstance   map[string]BackendInstance
}

func NewStaticRouter() *StaticRouter {
	return &StaticRouter{
		messageBackend: make(map[uint32]string),
		userRoom:       make(map[string]string),
		roomInstance:   make(map[string]BackendInstance),
	}
}

func (r *StaticRouter) SetMessageBackend(messageType uint32, backendType string) {
	r.mu.Lock()
	r.messageBackend[messageType] = backendType
	r.mu.Unlock()
}
func (r *StaticRouter) SetUserRoom(userID, roomID string) {
	r.mu.Lock()
	r.userRoom[userID] = roomID
	r.mu.Unlock()
}
func (r *StaticRouter) SetRoomInstance(roomID string, instance BackendInstance) {
	r.mu.Lock()
	r.roomInstance[roomID] = instance
	r.mu.Unlock()
}
func (r *StaticRouter) Resolve(userID string, messageType uint32) (Route, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	backendType, ok := r.messageBackend[messageType]
	if !ok || backendType == "" {
		return Route{}, ErrUnknownMessageType
	}
	roomID, ok := r.userRoom[userID]
	if !ok || roomID == "" {
		return Route{}, ErrUserRoomNotFound
	}
	inst, ok := r.roomInstance[roomID]
	if !ok || inst.ID == "" {
		return Route{}, ErrRoomInstanceNotFound
	}
	if inst.BackendType != backendType {
		return Route{}, ErrBackendTypeMismatch
	}
	return Route{BackendType: backendType, RoomID: roomID, Instance: inst}, nil
}
