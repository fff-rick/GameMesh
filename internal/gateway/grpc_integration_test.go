package gateway

import (
	"bufio"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"game-gateway/internal/backend"
	"game-gateway/internal/protocol"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type fakeBackendProcess struct {
	cmd *exec.Cmd
}

func startFakeBackendProcess(t *testing.T, mode string) (*fakeBackendProcess, *grpc.ClientConn) {
	t.Helper()
	root := repositoryRoot(t)
	binary := filepath.Join(t.TempDir(), "fake-backend")
	build := exec.Command("go", "build", "-o", binary, "./cmd/fake_backend")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build fake backend: %v\n%s", err, output)
	}

	cmd := exec.Command(binary, "-mode", mode)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	address := readBackendAddress(t, stdout, cmd)
	conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		t.Fatal(err)
	}
	process := &fakeBackendProcess{cmd: cmd}
	t.Cleanup(func() {
		_ = conn.Close()
		process.stop()
	})
	return process, conn
}

func (p *fakeBackendProcess) stop() {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	_, _ = p.cmd.Process.Wait()
	p.cmd = nil
}

func readBackendAddress(t *testing.T, stdout io.Reader, cmd *exec.Cmd) string {
	t.Helper()
	addressCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		line, err := bufio.NewReader(stdout).ReadString('\n')
		if err != nil {
			errCh <- err
			return
		}
		addressCh <- strings.TrimSpace(line)
	}()
	select {
	case address := <-addressCh:
		if address == "" {
			t.Fatal("fake backend returned an empty address")
		}
		return address
	case err := <-errCh:
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		t.Fatalf("read fake backend address: %v", err)
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		t.Fatal("fake backend did not become ready")
	}
	return ""
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve repository root")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func TestGatewayRoutesThroughIndependentGRPCBackend(t *testing.T) {
	_, conn := startFakeBackendProcess(t, "success")
	gw, client, _, _ := setupRoutedServer(t, backend.NewGRPCClient(conn))

	env := sendBusiness(t, client, 1001, "grpc-success", []byte("move"))
	if env.MessageType != 1002 || env.RequestID != "grpc-success" || string(env.Payload) != "move" {
		t.Fatalf("response = %#v", env)
	}
	assertBackendMetric(t, gw, `result="success"`)
}

func TestGatewayMapsIndependentGRPCBackendTimeout(t *testing.T) {
	_, conn := startFakeBackendProcess(t, "delay")
	gw, client, _, _ := setupRoutedServer(t, backend.NewGRPCClient(conn))

	env := sendBusiness(t, client, 1001, "grpc-timeout", nil)
	assertErrorEnvelope(t, env, "backend_timeout", true)
	if gw.ActiveSessionCount() != 1 {
		t.Fatalf("sessions = %d, want 1", gw.ActiveSessionCount())
	}
	assertBackendMetric(t, gw, `result="timeout"`)
}

func TestGatewayMapsIndependentGRPCBackendBusinessError(t *testing.T) {
	_, conn := startFakeBackendProcess(t, "business-error")
	gw, client, _, _ := setupRoutedServer(t, backend.NewGRPCClient(conn))

	env := sendBusiness(t, client, 1001, "grpc-business-error", nil)
	assertErrorEnvelope(t, env, "room_full", false)
	assertBackendMetric(t, gw, `result="backend_error"`)
}

func TestGatewayMapsStoppedGRPCBackendToUnavailable(t *testing.T) {
	process, conn := startFakeBackendProcess(t, "success")
	process.stop()
	gw, client, _, _ := setupRoutedServer(t, backend.NewGRPCClient(conn))

	env := sendBusiness(t, client, 1001, "grpc-unavailable", nil)
	assertErrorEnvelope(t, env, "backend_unavailable", true)
	if gw.ActiveSessionCount() != 1 {
		t.Fatalf("sessions = %d, want 1", gw.ActiveSessionCount())
	}
	assertBackendMetric(t, gw, `result="unavailable"`)
}

func assertErrorEnvelope(t *testing.T, env protocol.Envelope, code string, retryable bool) {
	t.Helper()
	if env.MessageType != protocol.MessageTypeError {
		t.Fatalf("message type = %d, want error", env.MessageType)
	}
	errResp, err := protocol.UnmarshalErrorResponse(env.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if errResp.ErrorCode != code || errResp.Retryable != retryable {
		t.Fatalf("error response = %#v, want code=%q retryable=%t", errResp, code, retryable)
	}
}

func assertBackendMetric(t *testing.T, gw *Server, fragment string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/metrics", nil)
	gw.Handler().ServeHTTP(recorder, request)
	if !strings.Contains(recorder.Body.String(), "game_gateway_backend_rpc_total") || !strings.Contains(recorder.Body.String(), fragment) {
		t.Fatalf("backend metric %q missing from:\n%s", fragment, recorder.Body.String())
	}
}
