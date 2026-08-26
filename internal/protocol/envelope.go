package protocol

import (
	"errors"
	"fmt"
)

const CurrentVersion uint32 = 1

var (
	ErrMalformedEnvelope  = errors.New("malformed envelope")
	ErrUnsupportedVersion = errors.New("unsupported protocol version")
	ErrUnknownMessageType = errors.New("unknown message type")
)

const (
	MessageTypeEchoRequest  uint32 = 1
	MessageTypeEchoResponse uint32 = 2
	BusinessMessageMin      uint32 = 1000
)

type Envelope struct {
	Version         uint32
	MessageType     uint32
	RequestID       string
	MessageID       string
	Seq             uint64
	Ack             uint64
	Payload         []byte
	TimestampUnixMS int64
}

func (e Envelope) Validate() error {
	if e.Version != CurrentVersion {
		return fmt.Errorf("%w: got=%d want=%d", ErrUnsupportedVersion, e.Version, CurrentVersion)
	}
	if e.MessageType == 0 || (e.MessageType < BusinessMessageMin && e.MessageType != MessageTypeEchoRequest && e.MessageType != MessageTypeEchoResponse) {
		return fmt.Errorf("%w: %d", ErrUnknownMessageType, e.MessageType)
	}
	return nil
}

func Marshal(e Envelope) []byte {
	b := make([]byte, 0, len(e.Payload)+64)
	b = appendVarintField(b, 1, uint64(e.Version))
	b = appendVarintField(b, 2, uint64(e.MessageType))
	if e.RequestID != "" {
		b = appendBytesField(b, 3, []byte(e.RequestID))
	}
	if e.MessageID != "" {
		b = appendBytesField(b, 4, []byte(e.MessageID))
	}
	if e.Seq != 0 {
		b = appendVarintField(b, 5, e.Seq)
	}
	if e.Ack != 0 {
		b = appendVarintField(b, 6, e.Ack)
	}
	if len(e.Payload) != 0 {
		b = appendBytesField(b, 7, e.Payload)
	}
	if e.TimestampUnixMS != 0 {
		b = appendVarintField(b, 8, uint64(e.TimestampUnixMS))
	}
	return b
}

func Unmarshal(b []byte) (Envelope, error) {
	var e Envelope
	for len(b) > 0 {
		key, n := consumeVarint(b)
		if n <= 0 {
			return Envelope{}, ErrMalformedEnvelope
		}
		b = b[n:]
		field, wire := int(key>>3), int(key&7)
		switch field {
		case 1, 2, 5, 6, 8:
			if wire != 0 {
				return Envelope{}, ErrMalformedEnvelope
			}
			v, n := consumeVarint(b)
			if n <= 0 {
				return Envelope{}, ErrMalformedEnvelope
			}
			b = b[n:]
			switch field {
			case 1:
				e.Version = uint32(v)
			case 2:
				e.MessageType = uint32(v)
			case 5:
				e.Seq = v
			case 6:
				e.Ack = v
			case 8:
				e.TimestampUnixMS = int64(v)
			}
		case 3, 4, 7:
			if wire != 2 {
				return Envelope{}, ErrMalformedEnvelope
			}
			ln, n := consumeVarint(b)
			if n <= 0 || ln > uint64(len(b)-n) {
				return Envelope{}, ErrMalformedEnvelope
			}
			b = b[n:]
			v := b[:int(ln)]
			b = b[int(ln):]
			switch field {
			case 3:
				e.RequestID = string(v)
			case 4:
				e.MessageID = string(v)
			case 7:
				e.Payload = append([]byte(nil), v...)
			}
		default:
			var ok bool
			b, ok = skipField(wire, b)
			if !ok {
				return Envelope{}, ErrMalformedEnvelope
			}
		}
	}
	return e, nil
}

func appendVarintField(b []byte, field int, v uint64) []byte {
	b = appendVarint(b, uint64(field<<3))
	return appendVarint(b, v)
}
func appendBytesField(b []byte, field int, v []byte) []byte {
	b = appendVarint(b, uint64(field<<3|2))
	b = appendVarint(b, uint64(len(v)))
	return append(b, v...)
}
func appendVarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}
func consumeVarint(b []byte) (uint64, int) {
	var v uint64
	for i := 0; i < len(b) && i < 10; i++ {
		c := b[i]
		if i == 9 && c > 1 {
			return 0, -1
		}
		v |= uint64(c&0x7f) << uint(7*i)
		if c < 0x80 {
			return v, i + 1
		}
	}
	return 0, -1
}
func skipField(wire int, b []byte) ([]byte, bool) {
	switch wire {
	case 0:
		_, n := consumeVarint(b)
		if n <= 0 {
			return nil, false
		}
		return b[n:], true
	case 1:
		if len(b) < 8 {
			return nil, false
		}
		return b[8:], true
	case 2:
		ln, n := consumeVarint(b)
		if n <= 0 || ln > uint64(len(b)-n) {
			return nil, false
		}
		return b[n+int(ln):], true
	case 5:
		if len(b) < 4 {
			return nil, false
		}
		return b[4:], true
	default:
		return nil, false
	}
}
