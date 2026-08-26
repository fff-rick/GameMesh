package backend

import "errors"

var ErrMalformedWire = errors.New("malformed backend protobuf wire")

func MarshalRequest(v Request) []byte {
	var b []byte
	if v.UserID != "" {
		b = appendBytesField(b, 1, []byte(v.UserID))
	}
	if v.SessionID != "" {
		b = appendBytesField(b, 2, []byte(v.SessionID))
	}
	if v.RoomID != "" {
		b = appendBytesField(b, 3, []byte(v.RoomID))
	}
	if v.MessageType != 0 {
		b = appendVarintField(b, 4, uint64(v.MessageType))
	}
	if v.RequestID != "" {
		b = appendBytesField(b, 5, []byte(v.RequestID))
	}
	if len(v.Payload) != 0 {
		b = appendBytesField(b, 6, v.Payload)
	}
	if v.TimestampUnixMS != 0 {
		b = appendVarintField(b, 7, uint64(v.TimestampUnixMS))
	}
	return b
}

func UnmarshalRequest(b []byte) (Request, error) {
	var out Request
	for len(b) > 0 {
		field, wire, rest, ok := consumeFieldKey(b)
		if !ok {
			return Request{}, ErrMalformedWire
		}
		b = rest
		switch field {
		case 1, 2, 3, 5, 6:
			if wire != 2 {
				return Request{}, ErrMalformedWire
			}
			v, rest, ok := consumeBytes(b)
			if !ok {
				return Request{}, ErrMalformedWire
			}
			b = rest
			switch field {
			case 1:
				out.UserID = string(v)
			case 2:
				out.SessionID = string(v)
			case 3:
				out.RoomID = string(v)
			case 5:
				out.RequestID = string(v)
			case 6:
				out.Payload = append([]byte(nil), v...)
			}
		case 4, 7:
			if wire != 0 {
				return Request{}, ErrMalformedWire
			}
			v, n := consumeVarint(b)
			if n <= 0 {
				return Request{}, ErrMalformedWire
			}
			b = b[n:]
			if field == 4 {
				out.MessageType = uint32(v)
			} else {
				out.TimestampUnixMS = int64(v)
			}
		default:
			var ok bool
			b, ok = skipField(wire, b)
			if !ok {
				return Request{}, ErrMalformedWire
			}
		}
	}
	return out, nil
}

func MarshalResponse(v Response) []byte {
	var b []byte
	if v.MessageType != 0 {
		b = appendVarintField(b, 1, uint64(v.MessageType))
	}
	if len(v.Payload) != 0 {
		b = appendBytesField(b, 2, v.Payload)
	}
	if v.ErrorCode != "" {
		b = appendBytesField(b, 3, []byte(v.ErrorCode))
	}
	return b
}

func UnmarshalResponse(b []byte) (Response, error) {
	var out Response
	for len(b) > 0 {
		field, wire, rest, ok := consumeFieldKey(b)
		if !ok {
			return Response{}, ErrMalformedWire
		}
		b = rest
		switch field {
		case 1:
			if wire != 0 {
				return Response{}, ErrMalformedWire
			}
			v, n := consumeVarint(b)
			if n <= 0 {
				return Response{}, ErrMalformedWire
			}
			out.MessageType = uint32(v)
			b = b[n:]
		case 2, 3:
			if wire != 2 {
				return Response{}, ErrMalformedWire
			}
			v, rest, ok := consumeBytes(b)
			if !ok {
				return Response{}, ErrMalformedWire
			}
			b = rest
			if field == 2 {
				out.Payload = append([]byte(nil), v...)
			} else {
				out.ErrorCode = string(v)
			}
		default:
			var ok bool
			b, ok = skipField(wire, b)
			if !ok {
				return Response{}, ErrMalformedWire
			}
		}
	}
	return out, nil
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
func consumeFieldKey(b []byte) (int, int, []byte, bool) {
	key, n := consumeVarint(b)
	if n <= 0 {
		return 0, 0, nil, false
	}
	return int(key >> 3), int(key & 7), b[n:], true
}
func consumeBytes(b []byte) ([]byte, []byte, bool) {
	ln, n := consumeVarint(b)
	if n <= 0 || ln > uint64(len(b)-n) {
		return nil, nil, false
	}
	b = b[n:]
	return b[:int(ln)], b[int(ln):], true
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
