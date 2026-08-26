package auth

import (
	"context"
	"errors"
	"strings"
)

var ErrInvalidToken = errors.New("invalid token")

type Authenticator interface {
	Authenticate(context.Context, string) (string, error)
}

type DevAuthenticator struct{}

func (DevAuthenticator) Authenticate(_ context.Context, token string) (string, error) {
	const prefix = "user:"
	if !strings.HasPrefix(token, prefix) {
		return "", ErrInvalidToken
	}
	userID := strings.TrimSpace(strings.TrimPrefix(token, prefix))
	if userID == "" {
		return "", ErrInvalidToken
	}
	return userID, nil
}
