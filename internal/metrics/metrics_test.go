package metrics

import (
	"strings"
	"testing"
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
