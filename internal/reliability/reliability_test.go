package reliability

import (
	"errors"
	"math"
	"testing"
	"time"

	"game-gateway/internal/protocol"
)

func TestAcceptInboundInOrderThenDuplicate(t *testing.T) {
	m := NewManager(Config{PendingLimit: 4, DedupWindow: 4, RetryInterval: time.Second, MaxRetries: 2})
	d, err := m.AcceptInbound("s1", "m1", 1)
	if err != nil || d != InboundAccepted {
		t.Fatalf("decision=%v err=%v", d, err)
	}
	d, err = m.AcceptInbound("s1", "m1", 1)
	if err != nil || d != InboundDuplicate {
		t.Fatalf("decision=%v err=%v", d, err)
	}
	if got := m.LastRecvSeq("s1"); got != 1 {
		t.Fatalf("last_recv=%d", got)
	}
}

func TestAcceptInboundRejectsOutOfOrderAndMessageIDConflict(t *testing.T) {
	m := NewManager(Config{PendingLimit: 4, DedupWindow: 4, RetryInterval: time.Second, MaxRetries: 2})
	if _, err := m.AcceptInbound("s1", "m2", 2); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("err=%v", err)
	}
	if _, err := m.AcceptInbound("s1", "m1", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := m.AcceptInbound("s1", "m1", 2); !errors.Is(err, ErrMessageIDConflict) {
		t.Fatalf("err=%v", err)
	}
	if _, err := m.AcceptInbound("s1", "other", 1); !errors.Is(err, ErrSeqConflict) {
		t.Fatalf("err=%v", err)
	}
}

func TestTrackOutboundAndCumulativeAck(t *testing.T) {
	now := time.Unix(100, 0)
	m := NewManager(Config{PendingLimit: 4, DedupWindow: 4, RetryInterval: time.Second, MaxRetries: 2})
	e1, err := m.TrackOutbound("s1", protocol.Envelope{Version: 1, MessageType: 1002}, now)
	if err != nil {
		t.Fatal(err)
	}
	e2, err := m.TrackOutbound("s1", protocol.Envelope{Version: 1, MessageType: 1002}, now)
	if err != nil {
		t.Fatal(err)
	}
	if e1.Seq != 1 || e2.Seq != 2 || e1.MessageID == "" || e2.MessageID == "" || e1.MessageID == e2.MessageID {
		t.Fatalf("e1=%#v e2=%#v", e1, e2)
	}
	if got := m.PendingCount(); got != 2 {
		t.Fatalf("pending=%d", got)
	}
	if removed := m.Ack("s1", 1); removed != 1 {
		t.Fatalf("removed=%d", removed)
	}
	if got := m.PendingCount(); got != 1 {
		t.Fatalf("pending=%d", got)
	}
	if removed := m.Ack("s1", 2); removed != 1 {
		t.Fatalf("removed=%d", removed)
	}
	if got := m.PendingCount(); got != 0 {
		t.Fatalf("pending=%d", got)
	}
}

func TestPendingReturnsClonedEnvelopesInSequenceOrder(t *testing.T) {
	base := time.Unix(100, 0)
	m := NewManager(Config{PendingLimit: 4, DedupWindow: 4, RetryInterval: time.Second, MaxRetries: 2})
	first, err := m.TrackOutbound("s1", protocol.Envelope{Version: 1, MessageType: 1002, RequestID: "first", Payload: []byte("one")}, base)
	if err != nil {
		t.Fatal(err)
	}
	second, err := m.TrackOutbound("s1", protocol.Envelope{Version: 1, MessageType: 1002, RequestID: "second", Payload: []byte("two")}, base)
	if err != nil {
		t.Fatal(err)
	}

	pending := m.Pending("s1")
	if len(pending) != 2 || pending[0].Seq != first.Seq || pending[1].Seq != second.Seq {
		t.Fatalf("pending=%#v", pending)
	}
	pending[0].Payload[0] = 'X'

	fresh := m.Pending("s1")
	if got := string(fresh[0].Payload); got != "one" {
		t.Fatalf("fresh payload=%q", got)
	}
}

func TestPendingLimitIsStrict(t *testing.T) {
	m := NewManager(Config{PendingLimit: 1, DedupWindow: 4, RetryInterval: time.Second, MaxRetries: 2})
	if _, err := m.TrackOutbound("s1", protocol.Envelope{Version: 1, MessageType: 1002}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := m.TrackOutbound("s1", protocol.Envelope{Version: 1, MessageType: 1002}, time.Now()); !errors.Is(err, ErrPendingFull) {
		t.Fatalf("err=%v", err)
	}
}

func TestCollectDueRetriesThenExhausts(t *testing.T) {
	base := time.Unix(100, 0)
	m := NewManager(Config{PendingLimit: 4, DedupWindow: 4, RetryInterval: time.Second, MaxRetries: 2})
	tracked, err := m.TrackOutbound("s1", protocol.Envelope{Version: 1, MessageType: 1002}, base)
	if err != nil {
		t.Fatal(err)
	}
	due, exhausted := m.CollectDue(base.Add(time.Second))
	if len(due) != 1 || len(exhausted) != 0 || due[0].Envelope.Seq != tracked.Seq || due[0].RetryCount != 1 {
		t.Fatalf("due=%#v exhausted=%#v", due, exhausted)
	}
	due, exhausted = m.CollectDue(base.Add(2 * time.Second))
	if len(due) != 1 || len(exhausted) != 0 || due[0].RetryCount != 2 {
		t.Fatalf("due=%#v exhausted=%#v", due, exhausted)
	}
	due, exhausted = m.CollectDue(base.Add(3 * time.Second))
	if len(due) != 0 || len(exhausted) != 1 || exhausted[0].Envelope.Seq != tracked.Seq {
		t.Fatalf("due=%#v exhausted=%#v", due, exhausted)
	}
	if got := m.PendingCount(); got != 0 {
		t.Fatalf("pending=%d", got)
	}
}

func TestCollectDueDoesNotConsumeRetriesWhileSessionIsDisconnected(t *testing.T) {
	base := time.Unix(100, 0)
	m := NewManager(Config{PendingLimit: 4, DedupWindow: 4, RetryInterval: time.Second, MaxRetries: 1})
	tracked, err := m.TrackOutbound("s1", protocol.Envelope{Version: 1, MessageType: 1002, RequestID: "original", Payload: []byte("payload")}, base)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 3; i++ {
		due, exhausted := m.CollectDueForSessions(base.Add(time.Duration(i)*time.Second), nil)
		if len(due) != 0 || len(exhausted) != 0 {
			t.Fatalf("disconnected scan %d: due=%#v exhausted=%#v", i, due, exhausted)
		}
	}
	if got := m.PendingCount(); got != 1 {
		t.Fatalf("pending while disconnected=%d", got)
	}

	due, exhausted := m.CollectDueForSessions(base.Add(4*time.Second), []string{"s1"})
	if len(due) != 1 || len(exhausted) != 0 || due[0].RetryCount != 1 || due[0].Envelope.MessageID != tracked.MessageID || due[0].Envelope.Seq != tracked.Seq {
		t.Fatalf("due=%#v exhausted=%#v tracked=%#v", due, exhausted, tracked)
	}
}

func TestDedupWindowRemainsBounded(t *testing.T) {
	m := NewManager(Config{PendingLimit: 4, DedupWindow: 2, RetryInterval: time.Second, MaxRetries: 2})
	for i, id := range []string{"m1", "m2", "m3"} {
		if _, err := m.AcceptInbound("s1", id, uint64(i+1)); err != nil {
			t.Fatal(err)
		}
	}
	if got := m.DedupEntryCount("s1"); got != 2 {
		t.Fatalf("dedup=%d", got)
	}
	if _, err := m.AcceptInbound("s1", "m1", 1); !errors.Is(err, ErrStaleSequence) {
		t.Fatalf("err=%v", err)
	}
}

func TestSequenceExhaustionDoesNotWrap(t *testing.T) {
	m := NewManager(Config{PendingLimit: 4, DedupWindow: 4, RetryInterval: time.Second, MaxRetries: 2})
	st := m.state("s1")
	st.nextSendSeq = math.MaxUint64
	first, err := m.TrackOutbound("s1", protocol.Envelope{Version: 1, MessageType: 1002}, time.Now())
	if err != nil || first.Seq != math.MaxUint64 {
		t.Fatalf("env=%#v err=%v", first, err)
	}
	if _, err := m.TrackOutbound("s1", protocol.Envelope{Version: 1, MessageType: 1002}, time.Now()); !errors.Is(err, ErrSeqExhausted) {
		t.Fatalf("err=%v", err)
	}

	st.lastRecvSeq = math.MaxUint64
	if _, err := m.AcceptInbound("s1", "after-max", math.MaxUint64); !errors.Is(err, ErrStaleSequence) {
		t.Fatalf("err=%v", err)
	}
}

func TestAckConcurrentWithCollectDueLeavesNoPending(t *testing.T) {
	base := time.Unix(100, 0)
	m := NewManager(Config{PendingLimit: 4, DedupWindow: 4, RetryInterval: time.Millisecond, MaxRetries: 3})
	tracked, err := m.TrackOutbound("s1", protocol.Envelope{Version: 1, MessageType: 1002}, base)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	done := make(chan struct{}, 2)
	go func() { <-start; m.CollectDue(base.Add(time.Millisecond)); done <- struct{}{} }()
	go func() { <-start; m.Ack("s1", tracked.Seq); done <- struct{}{} }()
	close(start)
	<-done
	<-done
	if got := m.PendingCount(); got != 0 {
		t.Fatalf("pending=%d", got)
	}
	if due, exhausted := m.CollectDue(base.Add(10 * time.Millisecond)); len(due) != 0 || len(exhausted) != 0 {
		t.Fatalf("due=%#v exhausted=%#v", due, exhausted)
	}
}

func TestStaticClassifierDefaultsUnreliableAndCanMarkReliable(t *testing.T) {
	c := NewStaticClassifier(1001)
	if got := c.Classify(1001); got != DeliveryReliable {
		t.Fatalf("1001=%v", got)
	}
	if got := c.Classify(1002); got != DeliveryUnreliable {
		t.Fatalf("1002=%v", got)
	}
	c.SetReliable(1001, false)
	c.SetReliable(1002, true)
	if got := c.Classify(1001); got != DeliveryUnreliable {
		t.Fatalf("1001=%v", got)
	}
	if got := c.Classify(1002); got != DeliveryReliable {
		t.Fatalf("1002=%v", got)
	}
}
