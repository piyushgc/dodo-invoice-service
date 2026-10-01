// Command webhooksink is a tiny demo receiver. It verifies each webhook's signature exactly
// as a business would and logs the result, so deliveries are visible in
// `docker compose logs -f webhook-sink`.
//
// POST /webhooks        → verifies and returns 200
// POST /webhooks/flaky  → returns 500 for the first 2 deliveries of each event, then 200,
//
//	to demonstrate retries with backoff
package main

import (
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"invoicesvc/internal/webhook"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	secret := os.Getenv("WEBHOOK_SECRET")
	addr := ":9000"
	if v := os.Getenv("LISTEN_ADDR"); v != "" {
		addr = v
	}

	var mu sync.Mutex
	seen := map[string]int{}

	handle := func(flaky bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			eventID := r.Header.Get(webhook.HeaderEventID)

			verified := "skipped (no WEBHOOK_SECRET)"
			if secret != "" {
				if err := webhook.Verify(secret, r.Header.Get(webhook.HeaderSignature), body, webhook.DefaultTolerance, time.Now()); err != nil {
					slog.Warn("webhook rejected", "event_id", eventID, "err", err)
					http.Error(w, "invalid signature", http.StatusBadRequest)
					return
				}
				verified = "valid"
			}

			mu.Lock()
			seen[eventID]++
			n := seen[eventID]
			mu.Unlock()

			if flaky && n <= 2 {
				slog.Warn("webhook received, simulating failure", "event_id", eventID, "delivery_number", n)
				http.Error(w, "simulated outage", http.StatusInternalServerError)
				return
			}
			slog.Info("webhook received",
				"type", r.Header.Get(webhook.HeaderEventType), "event_id", eventID,
				"signature", verified, "delivery_number", n, "body", string(body))
			w.WriteHeader(http.StatusOK)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhooks", handle(false))
	mux.HandleFunc("POST /webhooks/flaky", handle(true))
	slog.Info("webhook sink listening", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		slog.Error("webhook sink", "err", err)
		os.Exit(1)
	}
}
