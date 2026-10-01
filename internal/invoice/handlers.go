package invoice

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"invoicesvc/internal/auth"
	"invoicesvc/internal/db"
	"invoicesvc/internal/httpx"
	"invoicesvc/internal/money"
	"invoicesvc/internal/webhook"
)

// Handlers serves /v1/invoices (everything except /pay, which lives in package payment).
type Handlers struct {
	DB *pgxpool.Pool
}

const maxLineItems = 100

type lineItemRequest struct {
	Description     string `json:"description"`
	Quantity        *int64 `json:"quantity"`
	UnitAmountCents *int64 `json:"unit_amount_cents"`
}

// createRequest deliberately has no total field. Because DecodeJSON rejects unknown
// fields, a client that sends "total_cents" gets a 400 rather than having it ignored.
type createRequest struct {
	CustomerID uuid.UUID         `json:"customer_id"`
	DueDate    string            `json:"due_date"`
	LineItems  []lineItemRequest `json:"line_items"`
}

func validation(field, msg string) *httpx.Error {
	return httpx.NewError(http.StatusUnprocessableEntity, "validation_error", msg).
		WithDetails(map[string]any{"field": field})
}

// buildLineItems validates the request and computes every amount and the total on the
// server, with overflow checks, in integer cents.
func buildLineItems(req []lineItemRequest) ([]LineItem, int64, error) {
	if len(req) == 0 || len(req) > maxLineItems {
		return nil, 0, validation("line_items", fmt.Sprintf("between 1 and %d line items are required", maxLineItems))
	}
	items := make([]LineItem, len(req))
	var total int64
	for i, li := range req {
		field := fmt.Sprintf("line_items[%d]", i)
		desc := strings.TrimSpace(li.Description)
		if desc == "" || len(desc) > 500 {
			return nil, 0, validation(field+".description", "description is required and must be at most 500 characters")
		}
		if li.Quantity == nil || *li.Quantity < 1 {
			return nil, 0, validation(field+".quantity", "quantity must be a positive integer")
		}
		if li.UnitAmountCents == nil || *li.UnitAmountCents < 0 {
			return nil, 0, validation(field+".unit_amount_cents", "unit_amount_cents must be a non-negative integer (cents)")
		}
		amount, ok := money.Mul(*li.Quantity, *li.UnitAmountCents)
		if !ok || amount > money.MaxInvoiceTotalCents {
			return nil, 0, validation(field, "line amount is too large")
		}
		if total, ok = money.Add(total, amount); !ok || total > money.MaxInvoiceTotalCents {
			return nil, 0, validation("line_items", "invoice total exceeds the maximum of 9999999999 cents")
		}
		items[i] = LineItem{Description: desc, Quantity: *li.Quantity, UnitAmountCents: *li.UnitAmountCents, AmountCents: amount}
	}
	if total == 0 {
		return nil, 0, validation("line_items", "invoice total must be greater than zero")
	}
	return items, total, nil
}

// Create creates an open invoice and emits invoice.created in the same transaction.
func (h *Handlers) Create(w http.ResponseWriter, r *http.Request) error {
	var req createRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		return err
	}
	if req.CustomerID == uuid.Nil {
		return validation("customer_id", "customer_id is required")
	}
	due, err := time.Parse(time.DateOnly, req.DueDate)
	if err != nil {
		return validation("due_date", "due_date must be a date in YYYY-MM-DD format")
	}
	items, total, err := buildLineItems(req.LineItems)
	if err != nil {
		return err
	}

	businessID := auth.BusinessID(r.Context())
	inv := &Invoice{
		BusinessID: businessID,
		CustomerID: req.CustomerID,
		Status:     StatusOpen,
		Currency:   "USD",
		TotalCents: total,
		DueDate:    due.Format(time.DateOnly),
		LineItems:  items,
	}
	inv.ID, _ = uuid.NewV7()

	err = db.InTx(r.Context(), h.DB, func(tx pgx.Tx) error {
		var exists bool
		if err := tx.QueryRow(r.Context(),
			`SELECT EXISTS (SELECT 1 FROM customers WHERE id = $1 AND business_id = $2)`,
			req.CustomerID, businessID).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return validation("customer_id", "customer not found")
		}
		if err := tx.QueryRow(r.Context(), `
			INSERT INTO invoices (id, business_id, customer_id, status, currency, total_cents, due_date)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			RETURNING created_at, updated_at`,
			inv.ID, businessID, inv.CustomerID, inv.Status, inv.Currency, inv.TotalCents, due,
		).Scan(&inv.CreatedAt, &inv.UpdatedAt); err != nil {
			return err
		}
		for i, li := range items {
			if _, err := tx.Exec(r.Context(), `
				INSERT INTO invoice_line_items (invoice_id, position, description, quantity, unit_amount_cents, amount_cents)
				VALUES ($1, $2, $3, $4, $5, $6)`,
				inv.ID, i, li.Description, li.Quantity, li.UnitAmountCents, li.AmountCents); err != nil {
				return err
			}
		}
		return webhook.Emit(r.Context(), tx, businessID, webhook.EventInvoiceCreated, map[string]any{"invoice": inv})
	})
	if err != nil {
		return err
	}
	httpx.WriteJSON(w, http.StatusCreated, inv)
	return nil
}

// Get returns an invoice with its line items and payment attempts.
func (h *Handlers) Get(w http.ResponseWriter, r *http.Request) error {
	id, err := httpx.PathUUID(r, "id", "invoice")
	if err != nil {
		return err
	}
	inv, err := Get(r.Context(), h.DB, auth.BusinessID(r.Context()), id, false)
	if errors.Is(err, ErrNotFound) {
		return httpx.ErrNotFound("invoice")
	}
	if err != nil {
		return err
	}
	if err := LoadLineItems(r.Context(), h.DB, inv); err != nil {
		return err
	}
	if err := LoadAttempts(r.Context(), h.DB, inv); err != nil {
		return err
	}
	httpx.WriteJSON(w, http.StatusOK, inv)
	return nil
}

// List returns invoices newest first, optionally filtered by ?status=.
func (h *Handlers) List(w http.ResponseWriter, r *http.Request) error {
	page, err := httpx.ParsePage(r)
	if err != nil {
		return err
	}
	var status *Status
	if v := r.URL.Query().Get("status"); v != "" {
		s := Status(v)
		if !s.Valid() {
			return httpx.NewError(http.StatusBadRequest, "invalid_parameter",
				"status must be one of open, paid, void, uncollectible")
		}
		status = &s
	}
	businessID := auth.BusinessID(r.Context())
	rows, err := h.DB.Query(r.Context(), `
		SELECT `+invoiceColumns+` FROM invoices
		WHERE business_id = $1
		  AND ($2::text IS NULL OR status = $2)
		  AND ($3::uuid IS NULL OR (created_at, id) <
		       (SELECT created_at, id FROM invoices WHERE id = $3 AND business_id = $1))
		ORDER BY created_at DESC, id DESC
		LIMIT $4`,
		businessID, status, page.StartingAfter, page.Limit+1)
	if err != nil {
		return err
	}
	list, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Invoice, error) {
		inv, err := scanInvoice(row)
		if err != nil {
			return Invoice{}, err
		}
		return *inv, nil
	})
	if err != nil {
		return err
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.NewList(list, page.Limit))
	return nil
}

// Void moves an invoice to void.
func (h *Handlers) Void(w http.ResponseWriter, r *http.Request) error {
	return h.adminTransition(w, r, ActionVoid, webhook.EventInvoiceVoided)
}

// MarkUncollectible moves an invoice to uncollectible.
func (h *Handlers) MarkUncollectible(w http.ResponseWriter, r *http.Request) error {
	return h.adminTransition(w, r, ActionMarkUncollectible, webhook.EventInvoiceMarkedUncollectible)
}

// adminTransition applies a business-initiated transition under the invoice row lock.
// It is refused while a payment attempt is pending: otherwise an invoice could be voided
// while the PSP is still charging the card.
func (h *Handlers) adminTransition(w http.ResponseWriter, r *http.Request, action Action, event string) error {
	id, err := httpx.PathUUID(r, "id", "invoice")
	if err != nil {
		return err
	}
	businessID := auth.BusinessID(r.Context())
	var inv *Invoice
	err = db.InTx(r.Context(), h.DB, func(tx pgx.Tx) error {
		inv, err = Get(r.Context(), tx, businessID, id, true)
		if errors.Is(err, ErrNotFound) {
			return httpx.ErrNotFound("invoice")
		}
		if err != nil {
			return err
		}
		to, err := Transition(inv.Status, action)
		if err != nil {
			return err
		}
		pending, err := PendingAttemptID(r.Context(), tx, inv.ID)
		if err != nil {
			return err
		}
		if pending != nil {
			return httpx.NewError(http.StatusConflict, "payment_in_progress",
				"a payment attempt for this invoice is still being processed; retry once it resolves").
				WithDetails(map[string]any{"payment_attempt_id": pending})
		}
		if err := SetStatus(r.Context(), tx, inv, to); err != nil {
			return err
		}
		return webhook.Emit(r.Context(), tx, businessID, event, map[string]any{"invoice": inv})
	})
	if err != nil {
		return err
	}
	httpx.WriteJSON(w, http.StatusOK, inv)
	return nil
}
