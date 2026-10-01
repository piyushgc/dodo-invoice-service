// Package mock is the mock payment processor. It runs as its own binary (cmd/mockpsp) in
// docker compose, and in-process inside the integration tests.
//
// Outcomes are chosen by card token:
//
//	tok_success             -> succeeded after Delay (~100ms)
//	tok_insufficient_funds  -> failed/insufficient_funds after Delay
//	tok_card_declined       -> failed/card_declined after Delay
//	tok_timeout             -> sleeps TimeoutDelay (30s) then succeeds
//	tok_network_error       -> drops the TCP connection without answering
//	anything else           -> failed/invalid_card_token
//
// Like a real PSP it is idempotent on the Idempotency-Key header and lets callers look a
// charge up later by that key. A charge keeps processing even if the caller disconnects,
// which is exactly the situation the invoice service has to reconcile.
package mock

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type charge struct {
	Status      string `json:"status"`
	PSPRef      string `json:"psp_ref,omitempty"`
	Code        string `json:"code,omitempty"`
	AmountCents int64  `json:"amount_cents"`
	CardToken   string `json:"-"`
}

// Server is the mock PSP.
type Server struct {
	Delay        time.Duration
	TimeoutDelay time.Duration

	mu      sync.Mutex
	charges map[string]*charge

	chargeRequests atomic.Int64
}

// New returns a mock PSP with the given delays.
func New(delay, timeoutDelay time.Duration) *Server {
	return &Server{Delay: delay, TimeoutDelay: timeoutDelay, charges: map[string]*charge{}}
}

// ChargeRequests is the number of POST /v1/charges requests received (tests use it to
// prove an idempotent replay did not call the PSP again).
func (s *Server) ChargeRequests() int64 { return s.chargeRequests.Load() }

// SucceededCharges is the number of distinct charges that actually moved money.
func (s *Server) SucceededCharges() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.charges {
		if c.Status == "succeeded" {
			n++
		}
	}
	return n
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/charges", s.createCharge)
	mux.HandleFunc("GET /v1/charges/{key}", s.getCharge)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

func (s *Server) createCharge(w http.ResponseWriter, r *http.Request) {
	s.chargeRequests.Add(1)
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"status": "failed", "code": "missing_idempotency_key"})
		return
	}
	var req struct {
		AmountCents int64  `json:"amount_cents"`
		CardToken   string `json:"card_token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.AmountCents <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"status": "failed", "code": "invalid_request"})
		return
	}

	if req.CardToken == "tok_network_error" {
		// Simulate the connection dying mid-request. Nothing is recorded.
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				conn.Close()
				return
			}
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"status": "error"})
		return
	}

	s.mu.Lock()
	if existing, ok := s.charges[key]; ok {
		// Idempotent replay: report the current state of the original charge.
		snapshot := *existing
		s.mu.Unlock()
		status := http.StatusOK
		if snapshot.Status == "processing" {
			status = http.StatusAccepted
		}
		writeJSON(w, status, snapshot)
		return
	}
	c := &charge{Status: "processing", AmountCents: req.AmountCents, CardToken: req.CardToken}
	s.charges[key] = c
	s.mu.Unlock()

	delay := s.Delay
	if req.CardToken == "tok_timeout" {
		delay = s.TimeoutDelay
	}

	// The charge completes on its own schedule, whether or not the caller is still there.
	done := make(chan struct{})
	go func() {
		time.Sleep(delay)
		s.mu.Lock()
		switch req.CardToken {
		case "tok_success", "tok_timeout":
			c.Status, c.PSPRef = "succeeded", uuid.NewString()
		case "tok_insufficient_funds":
			c.Status, c.Code = "failed", "insufficient_funds"
		case "tok_card_declined":
			c.Status, c.Code = "failed", "card_declined"
		default:
			c.Status, c.Code = "failed", "invalid_card_token"
		}
		s.mu.Unlock()
		close(done)
	}()

	select {
	case <-done:
		s.mu.Lock()
		snapshot := *c
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, snapshot)
	case <-r.Context().Done():
		// Caller gave up; the charge still completes in the background.
	}
}

func (s *Server) getCharge(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	c, ok := s.charges[r.PathValue("key")]
	var snapshot charge
	if ok {
		snapshot = *c
	}
	s.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"code": "charge_not_found"})
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}
