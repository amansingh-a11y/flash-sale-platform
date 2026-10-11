// Package admission verifies the short-lived token the waiting room issues when
// a user reaches the front of the queue. Only token holders may hit the
// reservation path, so origin load is bounded by the waiting room's release rate.
package admission

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"
)

var (
	ErrMalformed = errors.New("admission: malformed token")
	ErrSignature = errors.New("admission: bad signature")
	ErrExpired   = errors.New("admission: token expired")
)

type Claims struct {
	UserID  string
	EventID string
	Expires time.Time
}

var enc = base64.RawURLEncoding

func sign(secret []byte, payload string) []byte {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(payload))
	return m.Sum(nil)
}

// Mint is used by the waiting room (and by the load generator / tests).
func Mint(secret []byte, c Claims) (string, error) {
	if c.UserID == "" || strings.Contains(c.UserID, "|") || strings.Contains(c.EventID, "|") {
		return "", ErrMalformed
	}
	payload := c.UserID + "|" + c.EventID + "|" + strconv.FormatInt(c.Expires.Unix(), 10)
	return enc.EncodeToString([]byte(payload)) + "." + enc.EncodeToString(sign(secret, payload)), nil
}

func Verify(secret []byte, token string, now time.Time) (Claims, error) {
	p, s, ok := strings.Cut(token, ".")
	if !ok {
		return Claims{}, ErrMalformed
	}
	payload, err := enc.DecodeString(p)
	if err != nil {
		return Claims{}, ErrMalformed
	}
	sig, err := enc.DecodeString(s)
	if err != nil {
		return Claims{}, ErrMalformed
	}
	if !hmac.Equal(sig, sign(secret, string(payload))) {
		return Claims{}, ErrSignature
	}
	parts := strings.Split(string(payload), "|")
	if len(parts) != 3 || parts[0] == "" {
		return Claims{}, ErrMalformed
	}
	exp, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return Claims{}, ErrMalformed
	}
	c := Claims{UserID: parts[0], EventID: parts[1], Expires: time.Unix(exp, 0)}
	if !now.Before(c.Expires) {
		return Claims{}, ErrExpired
	}
	return c, nil
}
