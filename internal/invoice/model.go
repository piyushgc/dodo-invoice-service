// Package invoice implements invoices, their line items and state machine.
package invoice

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"invoicesvc/internal/db"
)

// LineItem is one priced line on an invoice. AmountCents is computed by the server.
type LineItem struct {
	Description     string `json:"description"`
	Quantity        int64  `json:"quantity"`
	UnitAmountCents int64  `json:"unit_amount_cents"`
	AmountCents     int64  `json:"amount_cents"`
}

// Invoice is the API and storage representation of an invoice.
type Invoice struct {
	ID                    uuid.UUID        `json:"id"`
	BusinessID            uuid.UUID        `json:"-"`
	CustomerID            uuid.UUID        `json:"customer_id"`
	Status                Status           `json:"status"`
	Currency              string           `json:"currency"`
	TotalCents            int64            `json:"total_cents"`
	DueDate               string           `json:"due_date"`
	LineItems             []LineItem       `json:"line_items,omitempty"`
	PaymentAttempts       []PaymentAttempt `json:"payment_attempts,omitempty"`
	CreatedAt             time.Time        `json:"created_at"`
	UpdatedAt             time.Time        `json:"updated_at"`
	PaidAt                *time.Time       `json:"paid_at"`
	VoidedAt              *time.Time       `json:"voided_at"`
	MarkedUncollectibleAt *time.Time       `json:"marked_uncollectible_at"`
}

// PaymentAttempt status values.
const (
	AttemptPending   = "pending"
	AttemptSucceeded = "succeeded"
	AttemptFailed    = "failed"
)

// PaymentAttempt records one try at paying an invoice. It lives in this package because
// an invoice owns its attempts; the payment package drives their lifecycle.
type PaymentAttempt struct {
	ID          uuid.UUID  `json:"id"`
	InvoiceID   uuid.UUID  `json:"invoice_id"`
	Status      string     `json:"status"`
	AmountCents int64      `json:"amount_cents"`
	PSPRef      *string    `json:"psp_ref"`
	FailureCode *string    `json:"failure_code"`
	CreatedAt   time.Time  `json:"created_at"`
	ResolvedAt  *time.Time `json:"resolved_at"`
}

// ErrNotFound is returned when an invoice does not exist for the business.
var ErrNotFound = errors.New("invoice not found")

const invoiceColumns = `id, business_id, customer_id, status, currency, total_cents, due_date,
	created_at, updated_at, paid_at, voided_at, marked_uncollectible_at`

func scanInvoice(row pgx.Row) (*Invoice, error) {
	var inv Invoice
	var due time.Time
	err := row.Scan(&inv.ID, &inv.BusinessID, &inv.CustomerID, &inv.Status, &inv.Currency, &inv.TotalCents, &due,
		&inv.CreatedAt, &inv.UpdatedAt, &inv.PaidAt, &inv.VoidedAt, &inv.MarkedUncollectibleAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	inv.DueDate = due.Format(time.DateOnly)
	return &inv, nil
}

// Get loads an invoice scoped to a business. With forUpdate it takes a row lock
// (SELECT ... FOR UPDATE) that serialises every state change and payment on the invoice.
func Get(ctx context.Context, q db.Querier, businessID, id uuid.UUID, forUpdate bool) (*Invoice, error) {
	sql := `SELECT ` + invoiceColumns + ` FROM invoices WHERE id = $1 AND business_id = $2`
	if forUpdate {
		sql += ` FOR UPDATE`
	}
	return scanInvoice(q.QueryRow(ctx, sql, id, businessID))
}

// SetStatus persists a transition already validated by Transition, stamping the matching
// timestamp column.
func SetStatus(ctx context.Context, tx pgx.Tx, inv *Invoice, to Status) error {
	err := tx.QueryRow(ctx, `
		UPDATE invoices SET status = $2, updated_at = now(),
			paid_at                 = CASE WHEN $2 = 'paid'          THEN now() ELSE paid_at END,
			voided_at               = CASE WHEN $2 = 'void'          THEN now() ELSE voided_at END,
			marked_uncollectible_at = CASE WHEN $2 = 'uncollectible' THEN now() ELSE marked_uncollectible_at END
		WHERE id = $1
		RETURNING updated_at, paid_at, voided_at, marked_uncollectible_at`,
		inv.ID, to).Scan(&inv.UpdatedAt, &inv.PaidAt, &inv.VoidedAt, &inv.MarkedUncollectibleAt)
	if err != nil {
		return err
	}
	inv.Status = to
	return nil
}

// LoadLineItems fills inv.LineItems.
func LoadLineItems(ctx context.Context, q db.Querier, inv *Invoice) error {
	rows, err := q.Query(ctx, `
		SELECT description, quantity, unit_amount_cents, amount_cents
		FROM invoice_line_items WHERE invoice_id = $1 ORDER BY position`, inv.ID)
	if err != nil {
		return err
	}
	inv.LineItems, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (LineItem, error) {
		var li LineItem
		return li, row.Scan(&li.Description, &li.Quantity, &li.UnitAmountCents, &li.AmountCents)
	})
	return err
}

const attemptColumns = `id, invoice_id, status, amount_cents, psp_ref, failure_code, created_at, resolved_at`

// ScanAttempt scans a row selected with attemptColumns.
func ScanAttempt(row pgx.Row) (*PaymentAttempt, error) {
	var a PaymentAttempt
	if err := row.Scan(&a.ID, &a.InvoiceID, &a.Status, &a.AmountCents, &a.PSPRef, &a.FailureCode,
		&a.CreatedAt, &a.ResolvedAt); err != nil {
		return nil, err
	}
	return &a, nil
}

// GetAttempt loads one payment attempt, optionally locking it.
func GetAttempt(ctx context.Context, q db.Querier, id uuid.UUID, forUpdate bool) (*PaymentAttempt, error) {
	sql := `SELECT ` + attemptColumns + ` FROM payment_attempts WHERE id = $1`
	if forUpdate {
		sql += ` FOR UPDATE`
	}
	return ScanAttempt(q.QueryRow(ctx, sql, id))
}

// LoadAttempts fills inv.PaymentAttempts, oldest first.
func LoadAttempts(ctx context.Context, q db.Querier, inv *Invoice) error {
	rows, err := q.Query(ctx, `SELECT `+attemptColumns+` FROM payment_attempts WHERE invoice_id = $1 ORDER BY created_at`, inv.ID)
	if err != nil {
		return err
	}
	attempts, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (PaymentAttempt, error) {
		a, err := ScanAttempt(row)
		if err != nil {
			return PaymentAttempt{}, err
		}
		return *a, nil
	})
	inv.PaymentAttempts = attempts
	return err
}

// PendingAttemptID returns the ID of the in-flight attempt for the invoice, if any.
func PendingAttemptID(ctx context.Context, q db.Querier, invoiceID uuid.UUID) (*uuid.UUID, error) {
	var id uuid.UUID
	err := q.QueryRow(ctx, `SELECT id FROM payment_attempts WHERE invoice_id = $1 AND status = 'pending'`, invoiceID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &id, nil
}
