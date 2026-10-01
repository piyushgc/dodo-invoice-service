// Command mockpsp runs the mock payment processor as a separate service, so the invoice
// service talks to it over a real network hop.
package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"invoicesvc/internal/psp/mock"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	addr := ":8081"
	if v := os.Getenv("LISTEN_ADDR"); v != "" {
		addr = v
	}
	timeoutDelay := 30 * time.Second
	if v, err := time.ParseDuration(os.Getenv("TIMEOUT_DELAY")); err == nil {
		timeoutDelay = v
	}
	routes := mock.New(100*time.Millisecond, timeoutDelay).Handler()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slog.Info("psp request", "method", r.Method, "path", r.URL.Path, "idempotency_key", r.Header.Get("Idempotency-Key"))
		routes.ServeHTTP(w, r)
	})
	slog.Info("mock psp listening", "addr", addr, "timeout_delay", timeoutDelay.String())
	if err := http.ListenAndServe(addr, handler); err != nil {
		slog.Error("mock psp", "err", err)
		os.Exit(1)
	}
}
