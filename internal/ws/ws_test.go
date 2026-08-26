package ws

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestBinaryRoundTrip(t *testing.T) {
	ts := httptest.NewServer(httpHandler(func(c *Conn) {
		p, err := c.ReadBinary(1024)
		if err != nil {
			return
		}
		_ = c.WriteBinary(p)
		_ = c.Close()
	}))
	defer ts.Close()
	c, err := Dial(strings.Replace(ts.URL, "http://", "ws://", 1), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.WriteBinary([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	p, err := c.ReadBinary(1024)
	if err != nil {
		t.Fatal(err)
	}
	if string(p) != "abc" {
		t.Fatalf("got %q", p)
	}
}

type httpHandler func(*Conn)

func (h httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := Upgrade(w, r)
	if err != nil {
		return
	}
	h(c)
}
