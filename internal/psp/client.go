// Package psp is the HTTP client for the payment service provider. The invoice service
// treats the PSP as a real external dependency: every call has a deadline, and anything
// other than a definitive answer is reported as ErrUnknown, never as success or failure.
package psp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Charge statuses returned by the PSP.
const (
	StatusSucceeded  = "succeeded"
	StatusFailed     = "failed"
	StatusProcessing = "processing"
)

var (
	// ErrUnknown means we do not know whether the card was charged: timeout, dropped
	// connection, or a 5xx. The caller must NOT assume failure; it must reconcile later.
	ErrUnknown = errors.New("psp: outcome unknown")
	// ErrNotFound means the PSP has no record of a charge with that idempotency key.
	ErrNotFound = errors.New("psp: charge not found")
)

// Result is a PSP answer.
type Result struct {
	Status string `json:"status"`
	PSPRef string `json:"psp_ref,omitempty"`
	Code   string `json:"code,omitempty"`
}

// Final reports whether the result is a definitive outcome.
func (r Result) Final() bool { return r.Status == StatusSucceeded || r.Status == StatusFailed }

// Client calls the PSP over HTTP.
type Client struct {
	BaseURL string
	Timeout time.Duration // per-call deadline; the /pay endpoint never waits longer than this
	HTTP    *http.Client
}

// NewClient returns a client with the given per-call timeout.
func NewClient(baseURL string, timeout time.Duration) *Client {
	return &Client{BaseURL: baseURL, Timeout: timeout, HTTP: &http.Client{}}
}

// Charge asks the PSP to charge the card. idempotencyKey is our payment attempt ID, so
// retrying the same attempt can never create a second charge at the PSP.
func (c *Client) Charge(ctx context.Context, idempotencyKey string, amountCents int64, cardToken string) (Result, error) {
	body, _ := json.Marshal(map[string]any{
		"amount_cents": amountCents,
		"currency":     "USD",
		"card_token":   cardToken,
	})
	return c.do(ctx, http.MethodPost, "/v1/charges", idempotencyKey, body)
}

// GetCharge looks up a charge by idempotency key (used by the reconciler).
func (c *Client) GetCharge(ctx context.Context, idempotencyKey string) (Result, error) {
	return c.do(ctx, http.MethodGet, "/v1/charges/"+idempotencyKey, "", nil)
}

func (c *Client) do(ctx context.Context, method, path, idemKey string, body []byte) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return Result{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		// Timeout, refused or dropped connection: the charge may or may not have happened.
		return Result{}, fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return Result{}, fmt.Errorf("%w: reading body: %v", ErrUnknown, err)
	}

	switch {
	case resp.StatusCode == http.StatusNotFound && method == http.MethodGet:
		return Result{}, ErrNotFound
	case resp.StatusCode >= 500:
		return Result{}, fmt.Errorf("%w: psp responded %d", ErrUnknown, resp.StatusCode)
	}

	var res Result
	if err := json.Unmarshal(raw, &res); err != nil {
		return Result{}, fmt.Errorf("%w: undecodable response (%d)", ErrUnknown, resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		// The PSP rejected the request outright (4xx): nothing was charged.
		if res.Code == "" {
			res.Code = "psp_rejected"
		}
		return Result{Status: StatusFailed, Code: res.Code}, nil
	}
	switch res.Status {
	case StatusSucceeded, StatusFailed, StatusProcessing:
		return res, nil
	}
	return Result{}, fmt.Errorf("%w: unexpected status %q", ErrUnknown, res.Status)
}
