package admission

import (
	"errors"
	"testing"
	"time"
)

func TestMintVerify(t *testing.T) {
	secret := []byte("test-secret")
	now := time.Unix(1_700_000_000, 0)
	tok, err := Mint(secret, Claims{UserID: "u1", EventID: "drop-1", Expires: now.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}

	c, err := Verify(secret, tok, now)
	if err != nil || c.UserID != "u1" || c.EventID != "drop-1" {
		t.Fatalf("verify: %+v %v", c, err)
	}
	if _, err := Verify([]byte("other"), tok, now); !errors.Is(err, ErrSignature) {
		t.Fatalf("want ErrSignature, got %v", err)
	}
	if _, err := Verify(secret, tok, now.Add(2*time.Minute)); !errors.Is(err, ErrExpired) {
		t.Fatalf("want ErrExpired, got %v", err)
	}
	if _, err := Verify(secret, tok[:len(tok)-2]+"xx", now); err == nil {
		t.Fatal("tampered token accepted")
	}
}
