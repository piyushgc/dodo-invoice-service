package tests

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

// Required test 1: N concurrent POST /pay for the same invoice (different idempotency
// keys, so they are genuinely different requests). At most one may succeed, the card is
// charged exactly once, and the invoice ends paid with exactly one succeeded attempt.
func TestConcurrentPaymentsChargeOnce(t *testing.T) {
	e := setup(t)
	invoiceID := e.createInvoice()

	const n = 25
	statuses := make([]int, n)
	codes := make([]string, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			r := e.pay(invoiceID, "tok_success", fmt.Sprintf("concurrent-%d", i))
			statuses[i] = r.status
			if r.status != http.StatusOK {
				codes[i] = errorCode(t, r)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	succeeded := 0
	for i, s := range statuses {
		switch {
		case s == http.StatusOK:
			succeeded++
		case s == http.StatusConflict && (codes[i] == "payment_in_progress" || codes[i] == "invoice_already_paid"):
		default:
			t.Errorf("request %d: unexpected status %d code %q", i, s, codes[i])
		}
	}
	if succeeded != 1 {
		t.Fatalf("expected exactly 1 successful payment, got %d (statuses %v)", succeeded, statuses)
	}
	if got := e.psp.ChargeRequests(); got != 1 {
		t.Fatalf("expected the PSP to be called once, got %d", got)
	}
	if got := e.psp.SucceededCharges(); got != 1 {
		t.Fatalf("expected exactly 1 charge at the PSP, got %d", got)
	}
	if s := e.invoiceStatus(invoiceID); s != "paid" {
		t.Fatalf("invoice status = %s, want paid", s)
	}
	counts := e.attemptCounts(invoiceID)
	if counts["succeeded"] != 1 || counts["pending"] != 0 {
		t.Fatalf("attempt counts = %v, want exactly 1 succeeded and none pending", counts)
	}
}

// Required test 2: retrying with the same Idempotency-Key returns the identical response
// and does not call the PSP again. Also covers: same key + different body → 422, a
// declined payment replays as the same decline, and concurrent duplicates of one key.
func TestIdempotentRetryDoesNotRecharge(t *testing.T) {
	e := setup(t)

	t.Run("sequential retry replays the stored response", func(t *testing.T) {
		invoiceID := e.createInvoice()
		before := e.psp.ChargeRequests()

		first := e.pay(invoiceID, "tok_success", "idem-seq")
		second := e.pay(invoiceID, "tok_success", "idem-seq")

		if first.status != http.StatusOK {
			t.Fatalf("first: %d %s", first.status, first.body)
		}
		if second.status != first.status || !bytes.Equal(second.body, first.body) {
			t.Fatalf("replay differs:\nfirst  %d %s\nsecond %d %s", first.status, first.body, second.status, second.body)
		}
		if second.header.Get("Idempotent-Replayed") != "true" {
			t.Fatalf("expected Idempotent-Replayed header on the replay")
		}
		if got := e.psp.ChargeRequests() - before; got != 1 {
			t.Fatalf("PSP called %d times, want 1", got)
		}
	})

	t.Run("same key with a different body is rejected", func(t *testing.T) {
		invoiceID := e.createInvoice()
		e.pay(invoiceID, "tok_card_declined", "idem-mismatch")
		before := e.psp.ChargeRequests()
		r := e.pay(invoiceID, "tok_success", "idem-mismatch")
		if r.status != http.StatusUnprocessableEntity || errorCode(t, r) != "idempotency_key_reused" {
			t.Fatalf("got %d %s, want 422 idempotency_key_reused", r.status, r.body)
		}
		if e.psp.ChargeRequests() != before {
			t.Fatalf("PSP must not be called for a rejected key reuse")
		}
	})

	t.Run("a decline replays as the same decline", func(t *testing.T) {
		invoiceID := e.createInvoice()
		before := e.psp.ChargeRequests()
		first := e.pay(invoiceID, "tok_card_declined", "idem-decline")
		second := e.pay(invoiceID, "tok_card_declined", "idem-decline")
		if first.status != http.StatusPaymentRequired || !bytes.Equal(first.body, second.body) {
			t.Fatalf("got %d %s then %d %s", first.status, first.body, second.status, second.body)
		}
		if got := e.psp.ChargeRequests() - before; got != 1 {
			t.Fatalf("PSP called %d times, want 1", got)
		}
		if s := e.invoiceStatus(invoiceID); s != "open" {
			t.Fatalf("declined invoice should stay open, got %s", s)
		}
	})

	t.Run("concurrent duplicates of one key charge once", func(t *testing.T) {
		invoiceID := e.createInvoice()
		before := e.psp.ChargeRequests()
		const n = 10
		results := make([]response, n)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				results[i] = e.pay(invoiceID, "tok_success", "idem-concurrent")
			}(i)
		}
		wg.Wait()
		// A duplicate that arrives while the original is still talking to the PSP sees the
		// attempt in flight (202); later ones get the stored 200. All must describe the
		// same single payment attempt.
		var attemptID any
		for i, r := range results {
			if r.status != http.StatusOK && r.status != http.StatusAccepted {
				t.Fatalf("request %d: %d %s", i, r.status, r.body)
			}
			id := r.json(t)["payment_attempt"].(map[string]any)["id"]
			if attemptID == nil {
				attemptID = id
			} else if id != attemptID {
				t.Fatalf("request %d saw attempt %v, others saw %v", i, id, attemptID)
			}
		}
		if got := e.psp.ChargeRequests() - before; got != 1 {
			t.Fatalf("PSP called %d times, want 1", got)
		}
	})
}

// Required test 3: PSP failures must not leave the invoice in a bad state.
func TestPSPFailuresDoNotCorruptInvoice(t *testing.T) {
	e := setup(t)
	ctx := context.Background()

	// reconcileUntil runs the reconciler until the invoice has no pending attempt.
	reconcileUntil := func(t *testing.T, invoiceID string) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := e.app.Payments.ReconcileOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if e.attemptCounts(invoiceID)["pending"] == 0 {
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatalf("attempt still pending after 15s")
	}

	t.Run("tok_timeout: endpoint returns quickly, invoice is locked, reconciler settles it", func(t *testing.T) {
		invoiceID := e.createInvoice()

		start := time.Now()
		r := e.pay(invoiceID, "tok_timeout", "timeout-1")
		if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
			t.Fatalf("endpoint hung for %s; PSP timeout is 500ms", elapsed)
		}
		if r.status != http.StatusAccepted {
			t.Fatalf("got %d %s, want 202 pending", r.status, r.body)
		}
		if s := e.invoiceStatus(invoiceID); s != "open" {
			t.Fatalf("invoice must stay open while the outcome is unknown, got %s", s)
		}

		// While the outcome is unknown, no second charge and no void may happen.
		if r := e.pay(invoiceID, "tok_success", "timeout-2"); r.status != http.StatusConflict || errorCode(t, r) != "payment_in_progress" {
			t.Fatalf("second payment: got %d %s, want 409 payment_in_progress", r.status, r.body)
		}
		if r := e.do(http.MethodPost, "/v1/invoices/"+invoiceID+"/void", nil, nil); r.status != http.StatusConflict {
			t.Fatalf("void during pending payment: got %d %s, want 409", r.status, r.body)
		}

		// The PSP finishes the charge after 2s; the reconciler picks that up.
		reconcileUntil(t, invoiceID)
		if s := e.invoiceStatus(invoiceID); s != "paid" {
			t.Fatalf("invoice status = %s, want paid", s)
		}
		if c := e.attemptCounts(invoiceID); c["succeeded"] != 1 {
			t.Fatalf("attempts = %v", c)
		}
		if got := e.psp.SucceededCharges(); got != 1 {
			t.Fatalf("PSP charges = %d, want 1", got)
		}
		// Retrying the original request now returns the final outcome.
		if r := e.pay(invoiceID, "tok_timeout", "timeout-1"); r.status != http.StatusOK {
			t.Fatalf("replay after reconcile: got %d %s, want 200", r.status, r.body)
		}
	})

	t.Run("tok_network_error: attempt fails safely and the invoice can be paid again", func(t *testing.T) {
		invoiceID := e.createInvoice()
		chargesBefore := e.psp.SucceededCharges()

		r := e.pay(invoiceID, "tok_network_error", "neterr-1")
		if r.status != http.StatusAccepted {
			t.Fatalf("got %d %s, want 202 pending", r.status, r.body)
		}
		reconcileUntil(t, invoiceID)

		if c := e.attemptCounts(invoiceID); c["failed"] != 1 {
			t.Fatalf("attempts = %v, want 1 failed", c)
		}
		if s := e.invoiceStatus(invoiceID); s != "open" {
			t.Fatalf("invoice status = %s, want open", s)
		}
		if r := e.pay(invoiceID, "tok_success", "neterr-2"); r.status != http.StatusOK {
			t.Fatalf("retry with a good card: got %d %s", r.status, r.body)
		}
		if got := e.psp.SucceededCharges() - chargesBefore; got != 1 {
			t.Fatalf("PSP charges = %d, want 1", got)
		}
	})
}

// Not one of the three required tests, but cheap and covers the state machine and the
// "never trust a client total" rule through the real API.
func TestInvoiceRulesAtTheAPI(t *testing.T) {
	e := setup(t)
	c := e.do(http.MethodPost, "/v1/customers", map[string]string{"name": "Bob", "email": "bob@example.com"}, nil)
	customerID := c.json(t)["id"]

	r := e.do(http.MethodPost, "/v1/invoices", map[string]any{
		"customer_id": customerID, "due_date": "2030-01-01", "total_cents": 1,
		"line_items": []map[string]any{{"description": "x", "quantity": 1, "unit_amount_cents": 500}},
	}, nil)
	if r.status != http.StatusBadRequest {
		t.Fatalf("client-supplied total: got %d %s, want 400", r.status, r.body)
	}

	r = e.do(http.MethodPost, "/v1/invoices", map[string]any{
		"customer_id": customerID, "due_date": "2030-01-01",
		"line_items": []map[string]any{{"description": "x", "quantity": 1, "unit_amount_cents": 10.5}},
	}, nil)
	if r.status != http.StatusBadRequest {
		t.Fatalf("fractional cents: got %d %s, want 400", r.status, r.body)
	}

	invoiceID := e.createInvoice()
	if r := e.pay(invoiceID, "tok_success", "rules-1"); r.status != http.StatusOK {
		t.Fatalf("pay: %d %s", r.status, r.body)
	}
	if r := e.pay(invoiceID, "tok_success", "rules-2"); r.status != http.StatusConflict || errorCode(t, r) != "invoice_already_paid" {
		t.Fatalf("pay paid invoice: got %d %s", r.status, r.body)
	}
	r = e.do(http.MethodPost, "/v1/invoices/"+invoiceID+"/void", nil, nil)
	if r.status != http.StatusConflict || errorCode(t, r) != "invalid_state_transition" {
		t.Fatalf("void paid invoice: got %d %s", r.status, r.body)
	}
}
