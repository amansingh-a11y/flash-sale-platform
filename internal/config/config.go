package config

import (
	"errors"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr        string
	RedisAddr       string
	RedisPassword   string
	AdmissionSecret []byte
	AdminToken      string

	HoldTTL         time.Duration
	PaymentDeadline time.Duration

	MaxInflightRead    int
	MaxInflightReserve int
	MaxInflightPay     int

	PSPAttemptTimeout time.Duration
	PSPMaxConcurrency int
	PSPLatency        time.Duration
	PSPTransientRate  float64
	PSPAmbiguousRate  float64
	PSPDeclineRate    float64

	ReconcileInterval time.Duration
}

func Load() (Config, error) {
	c := Config{
		HTTPAddr:           env("HTTP_ADDR", ":8080"),
		RedisAddr:          env("REDIS_ADDR", "localhost:6379"),
		RedisPassword:      os.Getenv("REDIS_PASSWORD"),
		AdmissionSecret:    []byte(os.Getenv("ADMISSION_SECRET")),
		AdminToken:         os.Getenv("ADMIN_TOKEN"),
		HoldTTL:            dur("HOLD_TTL", 10*time.Minute),
		PaymentDeadline:    dur("PAYMENT_DEADLINE", 2*time.Minute),
		MaxInflightRead:    num("MAX_INFLIGHT_READ", 2000),
		MaxInflightReserve: num("MAX_INFLIGHT_RESERVE", 500),
		MaxInflightPay:     num("MAX_INFLIGHT_PAY", 200),
		PSPAttemptTimeout:  dur("PSP_ATTEMPT_TIMEOUT", 3*time.Second),
		PSPMaxConcurrency:  num("PSP_MAX_CONCURRENCY", 100),
		PSPLatency:         dur("PSP_FAKE_LATENCY", 80*time.Millisecond),
		PSPTransientRate:   flt("PSP_FAKE_TRANSIENT_RATE", 0.02),
		PSPAmbiguousRate:   flt("PSP_FAKE_AMBIGUOUS_RATE", 0.01),
		PSPDeclineRate:     flt("PSP_FAKE_DECLINE_RATE", 0.02),
		ReconcileInterval:  dur("RECONCILE_INTERVAL", time.Second),
	}
	if len(c.AdmissionSecret) < 16 {
		return c, errors.New("ADMISSION_SECRET must be set (>= 16 bytes); see .env.example")
	}
	if len(c.AdminToken) < 16 {
		return c, errors.New("ADMIN_TOKEN must be set (>= 16 bytes); see .env.example")
	}
	return c, nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func dur(k string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(k)); err == nil {
		return d
	}
	return def
}

func num(k string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return n
	}
	return def
}

func flt(k string, def float64) float64 {
	if f, err := strconv.ParseFloat(os.Getenv(k), 64); err == nil {
		return f
	}
	return def
}
