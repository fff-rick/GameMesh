package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestStage2MetricsExposeSessionsAuthAndHeartbeat(t *testing.T) {
	m := New("gw-1")
	m.SetActiveSessions(2)
	m.AuthResult("success")
	m.AuthResult("invalid_token")
	m.HeartbeatTimeout()
	var b strings.Builder
	m.WritePrometheus(&b)
	got := b.String()
	for _, want := range []string{
		`game_gateway_sessions{gateway_id="gw-1",state="active"} 2`,
		`game_gateway_auth_total{gateway_id="gw-1",result="success"} 1`,
		`game_gateway_auth_total{gateway_id="gw-1",result="invalid_token"} 1`,
		`game_gateway_heartbeat_timeouts_total{gateway_id="gw-1"} 1`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
}

func TestStage3MetricsExposeBackendRPCResultAndLatency(t *testing.T) {
	m := New("gw-1")
	m.BackendRPC("room", "Handle", "success", 12*time.Millisecond)
	m.BackendRPC("room", "Handle", "timeout", 75*time.Millisecond)
	var b strings.Builder
	m.WritePrometheus(&b)
	got := b.String()
	for _, want := range []string{
		`game_gateway_backend_rpc_total{backend_type="room",method="Handle",result="success"} 1`,
		`game_gateway_backend_rpc_total{backend_type="room",method="Handle",result="timeout"} 1`,
		`game_gateway_backend_rpc_seconds_count{backend_type="room",method="Handle"} 2`,
		`game_gateway_backend_rpc_seconds_sum{backend_type="room",method="Handle"} `,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
}

func TestStage4MetricsExposeReliabilityState(t *testing.T) {
	m := New("gw-1")
	m.SetReliablePending(3)
	m.ReliableRetry()
	m.ReliableDedup()
	m.ReliableOutOfOrder()
	m.ReliableRetryExhausted()
	m.ReliablePendingOverflow()
	var b strings.Builder
	m.WritePrometheus(&b)
	got := b.String()
	for _, want := range []string{
		`game_gateway_reliable_pending{gateway_id="gw-1"} 3`,
		`game_gateway_reliable_retries_total{gateway_id="gw-1"} 1`,
		`game_gateway_reliable_dedup_total{gateway_id="gw-1"} 1`,
		`game_gateway_reliable_out_of_order_total{gateway_id="gw-1"} 1`,
		`game_gateway_reliable_retry_exhausted_total{gateway_id="gw-1"} 1`,
		`game_gateway_reliable_pending_overflow_total{gateway_id="gw-1"} 1`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
}
