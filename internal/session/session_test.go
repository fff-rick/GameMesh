package session

import (
	"sync"
	"testing"
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
