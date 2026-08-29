package protocol

import "errors"

var ErrMalformedControlPayload = errors.New("malformed control payload")

type AuthRequest struct {
	Token string
}

type AuthResult struct {
	OK          bool
	UserID      string
	SessionID   string
	ResumeToken string
	ErrorCode   string
}

func MarshalAuthRequest(v AuthRequest) []byte {
	var b []byte
	if v.Token != "" {
		b = appendBytesField(b, 1, []byte(v.Token))
	}
	return b
}

func UnmarshalAuthRequest(b []byte) (AuthRequest, error) {
	var out AuthRequest
	for len(b) > 0 {
		field, wire, rest, ok := consumeFieldKey(b)
		if !ok {
			return AuthRequest{}, ErrMalformedControlPayload
		}
		b = rest
		if field == 1 {
			if wire != 2 {
				return AuthRequest{}, ErrMalformedControlPayload
			}
			v, rest, ok := consumeBytes(b)
			if !ok {
				return AuthRequest{}, ErrMalformedControlPayload
			}
			out.Token = string(v)
			b = rest
			continue
		}
		var skipped bool
		b, skipped = skipField(wire, b)
		if !skipped {
			return AuthRequest{}, ErrMalformedControlPayload
		}
	}
	return out, nil
}

func MarshalAuthResult(v AuthResult) []byte {
	var b []byte
	if v.OK {
		b = appendVarintField(b, 1, 1)
	}
	if v.UserID != "" {
		b = appendBytesField(b, 2, []byte(v.UserID))
	}
	if v.SessionID != "" {
		b = appendBytesField(b, 3, []byte(v.SessionID))
	}
	if v.ErrorCode != "" {
		b = appendBytesField(b, 4, []byte(v.ErrorCode))
	}
	if v.ResumeToken != "" {
		b = appendBytesField(b, 5, []byte(v.ResumeToken))
	}
	return b
}

func UnmarshalAuthResult(b []byte) (AuthResult, error) {
	var out AuthResult
	for len(b) > 0 {
		field, wire, rest, ok := consumeFieldKey(b)
		if !ok {
			return AuthResult{}, ErrMalformedControlPayload
		}
		b = rest
		switch field {
		case 1:
			if wire != 0 {
				return AuthResult{}, ErrMalformedControlPayload
			}
			v, n := consumeVarint(b)
			if n <= 0 {
				return AuthResult{}, ErrMalformedControlPayload
			}
			out.OK = v != 0
			b = b[n:]
		case 2, 3, 4, 5:
			if wire != 2 {
				return AuthResult{}, ErrMalformedControlPayload
			}
			v, rest, ok := consumeBytes(b)
			if !ok {
				return AuthResult{}, ErrMalformedControlPayload
			}
			b = rest
			switch field {
			case 2:
				out.UserID = string(v)
			case 3:
				out.SessionID = string(v)
			case 4:
				out.ErrorCode = string(v)
			case 5:
				out.ResumeToken = string(v)
			}
		default:
			var skipped bool
			b, skipped = skipField(wire, b)
			if !skipped {
				return AuthResult{}, ErrMalformedControlPayload
			}
		}
	}
	return out, nil
}

type ResumeRequest struct {
	ResumeToken string
}

func MarshalResumeRequest(v ResumeRequest) []byte {
	var b []byte
	if v.ResumeToken != "" {
		b = appendBytesField(b, 1, []byte(v.ResumeToken))
	}
	return b
}

func UnmarshalResumeRequest(b []byte) (ResumeRequest, error) {
	var out ResumeRequest
	for len(b) > 0 {
		field, wire, rest, ok := consumeFieldKey(b)
		if !ok {
			return ResumeRequest{}, ErrMalformedControlPayload
		}
		b = rest
		if field == 1 {
			if wire != 2 {
				return ResumeRequest{}, ErrMalformedControlPayload
			}
			v, rest, ok := consumeBytes(b)
			if !ok {
				return ResumeRequest{}, ErrMalformedControlPayload
			}
			out.ResumeToken = string(v)
			b = rest
			continue
		}
		var skipped bool
		b, skipped = skipField(wire, b)
		if !skipped {
			return ResumeRequest{}, ErrMalformedControlPayload
		}
	}
	return out, nil
}

type ResumeResult struct {
	OK          bool
	SessionID   string
	ResumeToken string
	ErrorCode   string
}

func MarshalResumeResult(v ResumeResult) []byte {
	var b []byte
	if v.OK {
		b = appendVarintField(b, 1, 1)
	}
	if v.SessionID != "" {
		b = appendBytesField(b, 2, []byte(v.SessionID))
	}
	if v.ResumeToken != "" {
		b = appendBytesField(b, 3, []byte(v.ResumeToken))
	}
	if v.ErrorCode != "" {
		b = appendBytesField(b, 4, []byte(v.ErrorCode))
	}
	return b
}

func UnmarshalResumeResult(b []byte) (ResumeResult, error) {
	var out ResumeResult
	for len(b) > 0 {
		field, wire, rest, ok := consumeFieldKey(b)
		if !ok {
			return ResumeResult{}, ErrMalformedControlPayload
		}
		b = rest
		switch field {
		case 1:
			if wire != 0 {
				return ResumeResult{}, ErrMalformedControlPayload
			}
			v, n := consumeVarint(b)
			if n <= 0 {
				return ResumeResult{}, ErrMalformedControlPayload
			}
			out.OK = v != 0
			b = b[n:]
		case 2, 3, 4:
			if wire != 2 {
				return ResumeResult{}, ErrMalformedControlPayload
			}
			v, rest, ok := consumeBytes(b)
			if !ok {
				return ResumeResult{}, ErrMalformedControlPayload
			}
			b = rest
			switch field {
			case 2:
				out.SessionID = string(v)
			case 3:
				out.ResumeToken = string(v)
			case 4:
				out.ErrorCode = string(v)
			}
		default:
			var skipped bool
			b, skipped = skipField(wire, b)
			if !skipped {
				return ResumeResult{}, ErrMalformedControlPayload
			}
		}
	}
	return out, nil
}

func consumeFieldKey(b []byte) (field int, wire int, rest []byte, ok bool) {
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

type ErrorResponse struct {
	ErrorCode string
	Retryable bool
}

func MarshalErrorResponse(v ErrorResponse) []byte {
	var b []byte
	if v.ErrorCode != "" {
		b = appendBytesField(b, 1, []byte(v.ErrorCode))
	}
	if v.Retryable {
		b = appendVarintField(b, 2, 1)
	}
	return b
}

func UnmarshalErrorResponse(b []byte) (ErrorResponse, error) {
	var out ErrorResponse
	for len(b) > 0 {
		field, wire, rest, ok := consumeFieldKey(b)
		if !ok {
			return ErrorResponse{}, ErrMalformedControlPayload
		}
		b = rest
		switch field {
		case 1:
			if wire != 2 {
				return ErrorResponse{}, ErrMalformedControlPayload
			}
			v, rest, ok := consumeBytes(b)
			if !ok {
				return ErrorResponse{}, ErrMalformedControlPayload
			}
			out.ErrorCode = string(v)
			b = rest
		case 2:
			if wire != 0 {
				return ErrorResponse{}, ErrMalformedControlPayload
			}
			v, n := consumeVarint(b)
			if n <= 0 {
				return ErrorResponse{}, ErrMalformedControlPayload
			}
			out.Retryable = v != 0
			b = b[n:]
		default:
			var skipped bool
			b, skipped = skipField(wire, b)
			if !skipped {
				return ErrorResponse{}, ErrMalformedControlPayload
			}
		}
	}
	return out, nil
}
