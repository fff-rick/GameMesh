package session

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestRegisterAndTerminateByConn(t *testing.T) {
	m := NewManager()
	s, replaced, err := m.Register("alice", "conn-1")
	if err != nil {
		t.Fatal(err)
	}
	if replaced != nil {
		t.Fatalf("unexpected replacement %#v", replaced)
	}
	if s.UserID != "alice" || s.ConnID != "conn-1" || s.ID == "" {
		t.Fatalf("bad session %#v", s)
	}
	if got, ok := m.ByConn("conn-1"); !ok || got.ID != s.ID {
		t.Fatalf("by conn %#v %v", got, ok)
	}
	if got, ok := m.ByUser("alice"); !ok || got.ID != s.ID {
		t.Fatalf("by user %#v %v", got, ok)
	}
	if m.ActiveCount() != 1 {
		t.Fatalf("count=%d", m.ActiveCount())
	}
	ended := m.TerminateByConn("conn-1")
	if ended == nil || ended.ID != s.ID {
		t.Fatalf("ended %#v", ended)
	}
	if m.ActiveCount() != 0 {
		t.Fatalf("count=%d", m.ActiveCount())
	}
}

func TestRegisterSameUserReplacesOldSession(t *testing.T) {
	m := NewManager()
	old, _, err := m.Register("alice", "old")
	if err != nil {
		t.Fatal(err)
	}
	cur, replaced, err := m.Register("alice", "new")
	if err != nil {
		t.Fatal(err)
	}
	if replaced == nil || replaced.ID != old.ID {
		t.Fatalf("replaced %#v", replaced)
	}
	if cur.ID == old.ID || cur.ConnID != "new" {
		t.Fatalf("current %#v old %#v", cur, old)
	}
	if _, ok := m.ByConn("old"); ok {
		t.Fatal("old conn still active")
	}
	if got, ok := m.ByUser("alice"); !ok || got.ID != cur.ID {
		t.Fatalf("winner %#v %v", got, ok)
	}
}

func TestConcurrentSameUserHasExactlyOneActiveWinner(t *testing.T) {
	m := NewManager()
	const n = 64
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			connID := string(rune('A'+i%26)) + string(rune('a'+i/26))
			if _, _, err := m.Register("alice", connID); err != nil {
				t.Errorf("register: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if m.ActiveCount() != 1 {
		t.Fatalf("count=%d", m.ActiveCount())
	}
	winner, ok := m.ByUser("alice")
	if !ok || winner.ConnID == "" {
		t.Fatalf("winner %#v %v", winner, ok)
	}
	if got, ok := m.ByConn(winner.ConnID); !ok || got.ID != winner.ID {
		t.Fatalf("by conn %#v %v", got, ok)
	}
}

func TestDisconnectThenResumeKeepsSessionAndRotatesToken(t *testing.T) {
	now := time.Date(2026, 8, 29, 0, 0, 0, 0, time.UTC)
	m := NewManager(time.Minute)
	original, _, err := m.Register("alice", "old")
	if err != nil {
		t.Fatal(err)
	}
	if original.ResumeToken == "" {
		t.Fatal("register did not issue a resume token")
	}
	if ended := m.Disconnect("old", now); ended == nil {
		t.Fatal("disconnect did not retain the session")
	}

	resumed, err := m.Resume(original.ResumeToken, "new", now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if resumed.ID != original.ID || resumed.ConnID != "new" || resumed.ResumeToken == original.ResumeToken {
		t.Fatalf("resumed=%#v", resumed)
	}
	if _, err := m.Resume(original.ResumeToken, "replay", now.Add(time.Second)); !errors.Is(err, ErrInvalidResumeToken) {
		t.Fatalf("err=%v", err)
	}
}

func TestExpireRemovesOnlyGraceSessionsPastDeadline(t *testing.T) {
	now := time.Date(2026, 8, 29, 0, 0, 0, 0, time.UTC)
	m := NewManager(time.Minute)
	grace, _, err := m.Register("grace", "old")
	if err != nil {
		t.Fatal(err)
	}
	active, _, err := m.Register("active", "current")
	if err != nil {
		t.Fatal(err)
	}
	if ended := m.Disconnect(grace.ConnID, now); ended == nil {
		t.Fatal("disconnect did not retain grace session")
	}

	expired := m.Expire(now.Add(time.Minute))
	if len(expired) != 1 || expired[0].ID != grace.ID {
		t.Fatalf("expired=%#v", expired)
	}
	if got, ok := m.ByUser(active.UserID); !ok || got.ID != active.ID {
		t.Fatal("active session removed")
	}
	if _, ok := m.ByUser(grace.UserID); ok {
		t.Fatal("expired session retained")
	}
}

func TestOldConnectionDisconnectDoesNotRemoveResumedSession(t *testing.T) {
	now := time.Date(2026, 8, 29, 0, 0, 0, 0, time.UTC)
	m := NewManager(time.Minute)
	s, _, err := m.Register("alice", "old")
	if err != nil {
		t.Fatal(err)
	}
	if ended := m.Disconnect("old", now); ended == nil {
		t.Fatal("disconnect did not retain the session")
	}
	resumed, err := m.Resume(s.ResumeToken, "new", now)
	if err != nil {
		t.Fatal(err)
	}

	if ended := m.Disconnect("old", now); ended != nil {
		t.Fatalf("ended=%#v", ended)
	}
	if got, ok := m.ByUser("alice"); !ok || got.ConnID != "new" || got.ID != resumed.ID {
		t.Fatalf("got=%#v", got)
	}
}

func TestResumeRejectsEmptyConnectionID(t *testing.T) {
	now := time.Date(2026, 8, 29, 0, 0, 0, 0, time.UTC)
	m := NewManager(time.Minute)
	s, _, err := m.Register("alice", "old")
	if err != nil {
		t.Fatal(err)
	}
	if ended := m.Disconnect(s.ConnID, now); ended == nil {
		t.Fatal("disconnect did not retain the session")
	}

	if _, err := m.Resume(s.ResumeToken, "", now); !errors.Is(err, ErrInvalidConnID) {
		t.Fatalf("err=%v", err)
	}
}
