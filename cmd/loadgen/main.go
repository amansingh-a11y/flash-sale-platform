// Command loadgen replays a drop against a running reservation-api: seeds a
// SKU, sends a burst of admitted users through reserve -> pay, then checks the
// oversell invariant and prints latency percentiles. Exits non-zero on oversell.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amansingh-a11y/flash-sale-platform/internal/admission"
)

func main() {
	base := flag.String("base", "http://localhost:8080", "reservation-api base URL")
	sku := flag.String("sku", fmt.Sprintf("drop-%d", time.Now().Unix()), "SKU to seed")
	stock := flag.Int("stock", 1000, "units")
	shards := flag.Int("shards", 16, "inventory shards")
	users := flag.Int("users", 20000, "simulated users")
	conc := flag.Int("concurrency", 400, "concurrent users")
	payRatio := flag.Float64("pay", 0.9, "fraction of successful holds that pay")
	flag.Parse()

	secret, admin := []byte(os.Getenv("ADMISSION_SECRET")), os.Getenv("ADMIN_TOKEN")
	if len(secret) == 0 || admin == "" {
		fmt.Fprintln(os.Stderr, "ADMISSION_SECRET and ADMIN_TOKEN must be set (same values as the server)")
		os.Exit(2)
	}
	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: *conc, MaxConnsPerHost: *conc},
	}
	call := func(method, path, user, key string, body any) (int, map[string]any, time.Duration) {
		var buf bytes.Buffer
		if body != nil {
			_ = json.NewEncoder(&buf).Encode(body)
		}
		req, _ := http.NewRequest(method, *base+path, &buf)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Admin-Token", admin)
		if user != "" {
			tok, _ := admission.Mint(secret, admission.Claims{UserID: user, EventID: *sku, Expires: time.Now().Add(15 * time.Minute)})
			req.Header.Set("X-Admission-Token", tok)
		}
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		start := time.Now()
		resp, err := client.Do(req)
		lat := time.Since(start)
		if err != nil {
			return 0, nil, lat
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out, lat
	}

	if code, body, _ := call("POST", "/admin/skus", "", "", map[string]any{
		"sku": *sku, "total": *stock, "shards": *shards, "price_minor": 4999, "currency": "USD",
	}); code != http.StatusCreated {
		fmt.Fprintf(os.Stderr, "seed failed: %d %v\n", code, body)
		os.Exit(1)
	}

	var (
		mu       sync.Mutex
		reserveL []time.Duration
		payL     []time.Duration
		codes    = map[string]int{}
		reserved atomic.Int64
		wg       sync.WaitGroup
		sem      = make(chan struct{}, *conc)
	)
	record := func(kind string, code int, lat time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		codes[fmt.Sprintf("%s_%d", kind, code)]++
		if kind == "reserve" {
			reserveL = append(reserveL, lat)
		} else {
			payL = append(payL, lat)
		}
	}

	start := time.Now()
	for i := range *users {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			user := fmt.Sprintf("%s-u%d", *sku, i)
			code, body, lat := call("POST", "/v1/skus/"+*sku+"/holds", user, "r-"+user, nil)
			record("reserve", code, lat)
			if code != http.StatusCreated {
				return
			}
			reserved.Add(1)
			if float64(i%100)/100 >= *payRatio {
				return
			}
			// A well-behaved client: same idempotency key, jittered backoff on 503/409.
			for attempt := 0; attempt < 5; attempt++ {
				code, _, lat = call("POST", "/v1/holds/"+body["hold_id"].(string)+"/pay", user, "p-"+user, nil)
				record("pay", code, lat)
				if code != http.StatusServiceUnavailable && code != http.StatusConflict {
					return
				}
				time.Sleep(time.Duration(500+rand.IntN(1500)) * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	time.Sleep(2 * time.Second) // let the order consumer drain
	_, stats, _ := call("GET", "/admin/skus/"+*sku+"/stats", "", "", nil)

	fmt.Printf("\nusers=%d concurrency=%d elapsed=%s throughput=%.0f req/s\n",
		*users, *conc, elapsed.Round(time.Millisecond), float64(len(reserveL)+len(payL))/elapsed.Seconds())
	fmt.Printf("reserve latency %s\npay latency     %s\n", pct(reserveL), pct(payL))
	keys := make([]string, 0, len(codes))
	for k := range codes {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		fmt.Printf("  %-14s %d\n", k, codes[k])
	}
	fmt.Printf("stats: %v\n", stats)

	orders, _ := stats["orders"].(float64)
	if reserved.Load() == 0 {
		fmt.Println("FAIL: no holds succeeded; check the status codes above")
		os.Exit(1)
	}
	if reserved.Load() > int64(*stock) || int(orders) > *stock {
		fmt.Printf("FAIL: oversell (holds=%d orders=%d stock=%d)\n", reserved.Load(), int(orders), *stock)
		os.Exit(1)
	}
	fmt.Printf("OK: holds=%d orders=%d stock=%d, no oversell\n", reserved.Load(), int(orders), *stock)
}

func pct(d []time.Duration) string {
	if len(d) == 0 {
		return "n/a"
	}
	slices.Sort(d)
	at := func(p float64) time.Duration {
		return d[min(len(d)-1, int(p*float64(len(d))))].Round(100 * time.Microsecond)
	}
	return fmt.Sprintf("p50=%s p95=%s p99=%s max=%s", at(.50), at(.95), at(.99), d[len(d)-1].Round(100*time.Microsecond))
}
