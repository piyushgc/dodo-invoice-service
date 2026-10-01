// Package payment implements POST /v1/invoices/{id}/pay and the reconciler that settles
// payment attempts whose PSP outcome was unknown at request time.
//
// The flow is split into three short steps so no database transaction is ever held open
// across a network call to the PSP:
//
//  1. Reserve (one transaction): claim the idempotency key, lock the invoice row
//     (SELECT ... FOR UPDATE), check it is payable and has no in-flight attempt, insert a
//     `pending` payment attempt. Commit. From here on the attempt is durable: a crash at
//     any later point is recovered by the reconciler.
//  2. Charge (no transaction): call the PSP with a hard timeout, passing the attempt ID as
//     the PSP idempotency key.
//  3. Apply (one transaction): if the PSP gave a definitive answer, lock the invoice and
//     attempt, record the outcome, transition the invoice, emit the webhook event and
//     store the response against the idempotency key. If the answer is unknown (timeout,
//     dropped connection, 5xx) nothing is written: the attempt stays pending and the
//     endpoint returns 202; the reconciler resolves it.
package payment

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"invoicesvc/internal/auth"
	"invoicesvc/internal/db"
	"invoicesvc/internal/httpx"
	"invoicesvc/internal/invoice"
	"invoicesvc/internal/psp"
	"invoicesvc/internal/webhook"
)

// Service owns the payment flow.
type Service struct {
	DB  *pgxpool.Pool
	PSP *psp.Client
	// ReconcileAfter is when the reconciler first looks at a still-pending attempt. It must
	// be longer than the PSP timeout so the reconciler never races the original request.
	ReconcileAfter time.Duration
	// MaxReconcileBackoff caps the exponential backoff between reconciler checks.
	MaxReconcileBackoff time.Duration
}

// Response is a fully rendered HTTP response, as returned live or replayed from storage.
type Response struct {
	Status   int
	Body     []byte
	Replayed bool
}

type payRequest struct {
	CardToken string `json:"card_token"`
}

type payResult struct {
	PaymentAttempt *invoice.PaymentAttempt `json:"payment_attempt"`
	Invoice        *invoice.Invoice        `json:"invoice"`
}

// Handler serves POST /v1/invoices/{id}/pay.
func (s *Service) Handler(w http.ResponseWriter, r *http.Request) error {
	invoiceID, err := httpx.PathUUID(r, "id", "invoice")
	if err != nil {
		return err
	}
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" || len(key) > 255 {
		return httpx.NewError(http.StatusBadRequest, "idempotency_key_required",
			"an Idempotency-Key header (1-255 characters) is required for payments")
	}
	var req payRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		return err
	}
	if strings.TrimSpace(req.CardToken) == "" {
		return httpx.NewError(http.StatusUnprocessableEntity, "validation_error", "card_token is required").
			WithDetails(map[string]any{"field": "card_token"})
	}

	resp, err := s.Pay(r.Context(), auth.BusinessID(r.Context()), invoiceID, key, requestHash(invoiceID, req), req.CardToken)
	if err != nil {
		return err
	}
	if resp.Replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	httpx.WriteRaw(w, resp.Status, resp.Body)
	return nil
}

// requestHash fingerprints what the key is being used for: the target invoice plus the
// re-encoded (so whitespace/ordering independent) body.
func requestHash(invoiceID uuid.UUID, req payRequest) []byte {
	canonical, _ := json.Marshal(req)
	h := sha256.New()
	h.Write([]byte("pay:" + invoiceID.String() + ":"))
	h.Write(canonical)
	return h.Sum(nil)
}

// Pay runs the reserve → charge → apply flow described in the package comment.
func (s *Service) Pay(ctx context.Context, businessID, invoiceID uuid.UUID, idemKey string, reqHash []byte, cardToken string) (Response, error) {
	var (
		inv         *invoice.Invoice
		attempt     *invoice.PaymentAttempt
		decided     *Response // an error decided during reserve, stored against the key
		keyConflict bool
	)

	// ---- 1. Reserve ----
	err := db.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		// If another request already holds this key, the insert waits for it to commit,
		// then does nothing: concurrent duplicates are serialised on the key itself.
		tag, err := tx.Exec(ctx,
			`INSERT INTO idempotency_keys (business_id, key, request_hash) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
			businessID, idemKey, reqHash)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			keyConflict = true
			return nil
		}

		// The row lock: every payment and state change on this invoice queues here.
		inv, err = invoice.Get(ctx, tx, businessID, invoiceID, true)
		var reject *httpx.Error
		switch {
		case errors.Is(err, invoice.ErrNotFound):
			reject = httpx.ErrNotFound("invoice")
		case err != nil:
			return err
		default:
			if reject, err = checkPayable(ctx, tx, inv); err != nil {
				return err
			}
		}
		if reject != nil {
			decided = &Response{Status: reject.Status, Body: httpx.ErrorBody(ctx, reject)}
			_, err := tx.Exec(ctx,
				`UPDATE idempotency_keys SET response_status = $3, response_body = $4 WHERE business_id = $1 AND key = $2`,
				businessID, idemKey, decided.Status, decided.Body)
			return err
		}

		attempt = &invoice.PaymentAttempt{InvoiceID: inv.ID, Status: invoice.AttemptPending, AmountCents: inv.TotalCents}
		attempt.ID, _ = uuid.NewV7()
		err = tx.QueryRow(ctx, `
			INSERT INTO payment_attempts (id, invoice_id, business_id, status, amount_cents, card_token, next_check_at)
			VALUES ($1, $2, $3, 'pending', $4, $5, now() + ($6::bigint || ' milliseconds')::interval)
			RETURNING created_at`,
			attempt.ID, inv.ID, businessID, attempt.AmountCents, cardToken, s.ReconcileAfter.Milliseconds(),
		).Scan(&attempt.CreatedAt)
		if db.IsUniqueViolation(err) {
			// Unreachable while the row lock is held; the partial unique index is the backstop.
			return errPaymentInProgress(nil)
		}
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx,
			`UPDATE idempotency_keys SET payment_attempt_id = $3 WHERE business_id = $1 AND key = $2`,
			businessID, idemKey, attempt.ID)
		return err
	})
	if err != nil {
		return Response{}, err
	}
	if keyConflict {
		return s.replay(ctx, businessID, idemKey, reqHash)
	}
	if decided != nil {
		return *decided, nil
	}

	// ---- 2. Charge ----
	// WithoutCancel: if the client disconnects we still want to record the PSP's answer.
	// The PSP client applies its own hard timeout, so this can never hang.
	pspCtx := context.WithoutCancel(ctx)
	res, err := s.PSP.Charge(pspCtx, attempt.ID.String(), attempt.AmountCents, cardToken)
	if err != nil || !res.Final() {
		slog.WarnContext(ctx, "psp outcome unknown; attempt left pending for reconciler",
			"payment_attempt_id", attempt.ID, "invoice_id", inv.ID, "err", err, "psp_status", res.Status,
			"request_id", httpx.RequestID(ctx))
		return render(ctx, inv, attempt), nil
	}

	// ---- 3. Apply ----
	return s.ApplyResult(pspCtx, attempt.ID, res)
}

// checkPayable returns a rejection if the (locked) invoice cannot take a new payment.
func checkPayable(ctx context.Context, tx pgx.Tx, inv *invoice.Invoice) (*httpx.Error, error) {
	if inv.Status == invoice.StatusPaid {
		return httpx.NewError(http.StatusConflict, "invoice_already_paid", "this invoice has already been paid").
			WithDetails(map[string]any{"current_status": inv.Status}), nil
	}
	if !invoice.AcceptsPayment(inv.Status) {
		_, err := invoice.Transition(inv.Status, invoice.ActionPaymentSucceeded)
		var apiErr *httpx.Error
		errors.As(err, &apiErr)
		return apiErr, nil
	}
	pending, err := invoice.PendingAttemptID(ctx, tx, inv.ID)
	if err != nil {
		return nil, err
	}
	if pending != nil {
		return errPaymentInProgress(pending), nil
	}
	return nil, nil
}

func errPaymentInProgress(attemptID *uuid.UUID) *httpx.Error {
	e := httpx.NewError(http.StatusConflict, "payment_in_progress",
		"another payment attempt for this invoice is still being processed")
	if attemptID != nil {
		e.WithDetails(map[string]any{"payment_attempt_id": attemptID})
	}
	return e
}

// replay answers a request whose idempotency key was already used.
func (s *Service) replay(ctx context.Context, businessID uuid.UUID, idemKey string, reqHash []byte) (Response, error) {
	var (
		storedHash []byte
		attemptID  *uuid.UUID
		status     *int
		body       []byte
	)
	err := s.DB.QueryRow(ctx, `
		SELECT request_hash, payment_attempt_id, response_status, response_body
		FROM idempotency_keys WHERE business_id = $1 AND key = $2`,
		businessID, idemKey).Scan(&storedHash, &attemptID, &status, &body)
	if err != nil {
		return Response{}, err
	}
	if !bytes.Equal(storedHash, reqHash) {
		return Response{}, httpx.NewError(http.StatusUnprocessableEntity, "idempotency_key_reused",
			"this Idempotency-Key was already used with a different request; use a new key for a new request")
	}
	if status != nil {
		return Response{Status: *status, Body: body, Replayed: true}, nil
	}
	if attemptID == nil {
		// Cannot happen: the key and its attempt/response are written in one transaction.
		return Response{}, httpx.NewError(http.StatusConflict, "idempotency_key_in_use", "a request with this key is in progress")
	}
	// The original request is still unresolved (PSP outcome unknown). Report current state.
	attempt, err := invoice.GetAttempt(ctx, s.DB, *attemptID, false)
	if err != nil {
		return Response{}, err
	}
	inv, err := invoice.Get(ctx, s.DB, businessID, attempt.InvoiceID, false)
	if err != nil {
		return Response{}, err
	}
	resp := render(ctx, inv, attempt)
	resp.Replayed = true
	return resp, nil
}

// ApplyResult records a definitive PSP outcome. It is shared by the request path and the
// reconciler and is safe to call twice: whoever gets the locks second sees the attempt is
// no longer pending and just renders the existing outcome.
func (s *Service) ApplyResult(ctx context.Context, attemptID uuid.UUID, res psp.Result) (Response, error) {
	var resp Response
	err := db.InTx(ctx, s.DB, func(tx pgx.Tx) error {
		var invoiceID, businessID uuid.UUID
		if err := tx.QueryRow(ctx, `SELECT invoice_id, business_id FROM payment_attempts WHERE id = $1`, attemptID).
			Scan(&invoiceID, &businessID); err != nil {
			return err
		}
		// Lock order is always invoice, then attempt, everywhere, so these cannot deadlock.
		inv, err := invoice.Get(ctx, tx, businessID, invoiceID, true)
		if err != nil {
			return err
		}
		attempt, err := invoice.GetAttempt(ctx, tx, attemptID, true)
		if err != nil {
			return err
		}
		if attempt.Status != invoice.AttemptPending {
			resp = render(ctx, inv, attempt)
			return nil
		}

		switch res.Status {
		case psp.StatusSucceeded:
			attempt, err = invoice.ScanAttempt(tx.QueryRow(ctx, `
				UPDATE payment_attempts
				SET status = 'succeeded', psp_ref = $2, next_check_at = NULL, resolved_at = now(), updated_at = now()
				WHERE id = $1 RETURNING `+attemptColumns, attemptID, res.PSPRef))
			if err != nil {
				return err
			}
			to, terr := invoice.Transition(inv.Status, invoice.ActionPaymentSucceeded)
			if terr != nil {
				// Cannot happen while void/uncollectible are blocked during pending attempts.
				// If it ever does, money moved for an invoice we cannot mark paid: page a human.
				slog.ErrorContext(ctx, "charge succeeded for an invoice that cannot be paid; manual refund required",
					"invoice_id", inv.ID, "status", inv.Status, "payment_attempt_id", attemptID)
			} else {
				if err := invoice.SetStatus(ctx, tx, inv, to); err != nil {
					return err
				}
				if err := webhook.Emit(ctx, tx, businessID, webhook.EventInvoicePaid,
					map[string]any{"invoice": inv, "payment_attempt": attempt}); err != nil {
					return err
				}
			}
		default:
			code := res.Code
			if code == "" {
				code = "payment_failed"
			}
			attempt, err = invoice.ScanAttempt(tx.QueryRow(ctx, `
				UPDATE payment_attempts
				SET status = 'failed', failure_code = $2, next_check_at = NULL, resolved_at = now(), updated_at = now()
				WHERE id = $1 RETURNING `+attemptColumns, attemptID, code))
			if err != nil {
				return err
			}
			if err := webhook.Emit(ctx, tx, businessID, webhook.EventInvoicePaymentFailed,
				map[string]any{"invoice": inv, "payment_attempt": attempt}); err != nil {
				return err
			}
		}

		resp = render(ctx, inv, attempt)
		_, err = tx.Exec(ctx,
			`UPDATE idempotency_keys SET response_status = $2, response_body = $3 WHERE payment_attempt_id = $1`,
			attemptID, resp.Status, resp.Body)
		return err
	})
	if err == nil {
		slog.InfoContext(ctx, "payment attempt resolved", "payment_attempt_id", attemptID, "psp_status", res.Status, "code", res.Code)
	}
	return resp, err
}

const attemptColumns = `id, invoice_id, status, amount_cents, psp_ref, failure_code, created_at, resolved_at`

// render produces the HTTP response for an attempt:
//
//	succeeded → 200 {payment_attempt, invoice}
//	pending   → 202 {payment_attempt, invoice}   (outcome unknown; poll GET /v1/invoices/{id} or wait for a webhook)
//	failed    → 402 error envelope, code "payment_failed", details.decline_code + payment_attempt
func render(ctx context.Context, inv *invoice.Invoice, attempt *invoice.PaymentAttempt) Response {
	switch attempt.Status {
	case invoice.AttemptSucceeded:
		b, _ := json.Marshal(payResult{PaymentAttempt: attempt, Invoice: inv})
		return Response{Status: http.StatusOK, Body: b}
	case invoice.AttemptPending:
		b, _ := json.Marshal(payResult{PaymentAttempt: attempt, Invoice: inv})
		return Response{Status: http.StatusAccepted, Body: b}
	default:
		code := ""
		if attempt.FailureCode != nil {
			code = *attempt.FailureCode
		}
		e := httpx.NewError(http.StatusPaymentRequired, "payment_failed", "the payment was not successful: "+code).
			WithDetails(map[string]any{"decline_code": code, "payment_attempt": attempt, "invoice_status": inv.Status})
		return Response{Status: e.Status, Body: httpx.ErrorBody(ctx, e)}
	}
}
