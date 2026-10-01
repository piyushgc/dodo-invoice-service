// Package webhook implements the event outbox, HMAC signing, the background delivery
// worker with retries, and the endpoint/event APIs.
package webhook

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Event types.
const (
	EventInvoiceCreated             = "invoice.created"
	EventInvoicePaid                = "invoice.paid"
	EventInvoicePaymentFailed       = "invoice.payment_failed"
	EventInvoiceVoided              = "invoice.voided"
	EventInvoiceMarkedUncollectible = "invoice.marked_uncollectible"
)

// Event is both the webhook payload and the GET /v1/events resource.
type Event struct {
	ID         uuid.UUID       `json:"id"`
	Type       string          `json:"type"`
	CreatedAt  time.Time       `json:"created_at"`
	Data       json.RawMessage `json:"data"`
	Deliveries []Delivery      `json:"deliveries,omitempty"`
}

// Emit records an event and fans it out into one pending delivery per active endpoint.
//
// It must be called with the same transaction that performs the state change. That is the
// transactional-outbox guarantee: an event exists if and only if the change committed, and
// no network I/O happens inside the API request.
func Emit(ctx context.Context, tx pgx.Tx, businessID uuid.UUID, eventType string, data any) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	eventID, _ := uuid.NewV7()
	if _, err := tx.Exec(ctx,
		`INSERT INTO events (id, business_id, type, data) VALUES ($1, $2, $3, $4)`,
		eventID, businessID, eventType, payload); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO webhook_deliveries (event_id, endpoint_id)
		SELECT $1, id FROM webhook_endpoints WHERE business_id = $2 AND disabled_at IS NULL`,
		eventID, businessID)
	return err
}
