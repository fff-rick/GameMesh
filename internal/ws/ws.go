package ws

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

var (
	ErrProtocol = errors.New("websocket protocol error")
	ErrTooLarge = errors.New("websocket message too large")
)

const magicGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
const (
	opcodeText   = 1
	opcodeBinary = 2
	opcodeClose  = 8
	opcodePing   = 9
	opcodePong   = 10
)

type Conn struct {
	c         net.Conn
	r         *bufio.Reader
	client    bool
	writeMu   sync.Mutex
	closeOnce sync.Once
}

func Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") || !headerHasToken(r.Header.Get("Connection"), "upgrade") || r.Header.Get("Sec-WebSocket-Version") != "13" {
		return nil, ErrProtocol
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return nil, ErrProtocol
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("hijacking unsupported")
	}
	nc, rw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}
	h := sha1.Sum([]byte(key + magicGUID))
	accept := base64.StdEncoding.EncodeToString(h[:])
	_, err = fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", accept)
	if err != nil {
		_ = nc.Close()
		return nil, err
	}
	if err = rw.Flush(); err != nil {
		_ = nc.Close()
		return nil, err
	}
	return &Conn{c: nc, r: rw.Reader, client: false}, nil
}

func Dial(rawurl string, timeout time.Duration) (*Conn, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "ws" {
		return nil, fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	host := u.Host
	if !strings.Contains(host, ":") {
		host += ":80"
	}
	nc, err := net.DialTimeout("tcp", host, timeout)
	if err != nil {
		return nil, err
	}
	var keyBytes [16]byte
	if _, err = rand.Read(keyBytes[:]); err != nil {
		_ = nc.Close()
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes[:])
	path := u.RequestURI()
	if path == "" {
		path = "/"
	}
	_, err = fmt.Fprintf(nc, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", path, u.Host, key)
	if err != nil {
		_ = nc.Close()
		return nil, err
	}
	br := bufio.NewReader(nc)
	resp, err := http.ReadResponse(br, &http.Request{Method: "GET"})
	if err != nil {
		_ = nc.Close()
		return nil, err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 101 {
		_ = nc.Close()
		return nil, fmt.Errorf("upgrade status %s", resp.Status)
	}
	h := sha1.Sum([]byte(key + magicGUID))
	want := base64.StdEncoding.EncodeToString(h[:])
	if resp.Header.Get("Sec-WebSocket-Accept") != want {
		_ = nc.Close()
		return nil, ErrProtocol
	}
	return &Conn{c: nc, r: br, client: true}, nil
}

func (c *Conn) ReadBinary(max int64) ([]byte, error) {
	for {
		op, p, err := c.readFrame(max)
		if err != nil {
			return nil, err
		}
		switch op {
		case opcodeBinary:
			return p, nil
		case opcodePing:
			_ = c.writeFrame(opcodePong, p)
		case opcodePong:
			continue
		case opcodeClose:
			return nil, io.EOF
		case opcodeText:
			return nil, ErrProtocol
		default:
			return nil, ErrProtocol
		}
	}
}
func (c *Conn) WriteBinary(p []byte) error         { return c.writeFrame(opcodeBinary, p) }
func (c *Conn) WriteClose() error                  { return c.writeFrame(opcodeClose, nil) }
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.c.SetWriteDeadline(t) }
func (c *Conn) Close() error                       { var err error; c.closeOnce.Do(func() { err = c.c.Close() }); return err }

func (c *Conn) readFrame(max int64) (byte, []byte, error) {
	h := make([]byte, 2)
	if _, err := io.ReadFull(c.r, h); err != nil {
		return 0, nil, err
	}
	fin := h[0]&0x80 != 0
	op := h[0] & 0x0f
	if !fin {
		return 0, nil, ErrProtocol
	}
	masked := h[1]&0x80 != 0
	if c.client && masked {
		return 0, nil, ErrProtocol
	}
	if !c.client && !masked {
		return 0, nil, ErrProtocol
	}
	ln := int64(h[1] & 0x7f)
	if ln == 126 {
		var x uint16
		if err := binary.Read(c.r, binary.BigEndian, &x); err != nil {
			return 0, nil, err
		}
		ln = int64(x)
	} else if ln == 127 {
		var x uint64
		if err := binary.Read(c.r, binary.BigEndian, &x); err != nil {
			return 0, nil, err
		}
		if x > 1<<63-1 {
			return 0, nil, ErrTooLarge
		}
		ln = int64(x)
	}
	if max > 0 && ln > max {
		return 0, nil, ErrTooLarge
	}
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(c.r, mask[:]); err != nil {
			return 0, nil, err
		}
	}
	p := make([]byte, ln)
	if _, err := io.ReadFull(c.r, p); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := range p {
			p[i] ^= mask[i%4]
		}
	}
	return op, p, nil
}
func (c *Conn) writeFrame(op byte, p []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	var h []byte
	h = append(h, 0x80|op)
	maskBit := byte(0)
	if c.client {
		maskBit = 0x80
	}
	ln := len(p)
	switch {
	case ln < 126:
		h = append(h, maskBit|byte(ln))
	case ln <= 65535:
		h = append(h, maskBit|126, byte(ln>>8), byte(ln))
	default:
		h = append(h, maskBit|127)
		var x [8]byte
		binary.BigEndian.PutUint64(x[:], uint64(ln))
		h = append(h, x[:]...)
	}
	data := p
	if c.client {
		var m [4]byte
		if _, err := rand.Read(m[:]); err != nil {
			return err
		}
		h = append(h, m[:]...)
		data = append([]byte(nil), p...)
		for i := range data {
			data[i] ^= m[i%4]
		}
	}
	if _, err := c.c.Write(h); err != nil {
		return err
	}
	_, err := c.c.Write(data)
	return err
}
func headerHasToken(v, t string) bool {
	for _, p := range strings.Split(v, ",") {
		if strings.EqualFold(strings.TrimSpace(p), t) {
			return true
		}
	}
	return false
}
