package webhook

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"invoicesvc/internal/auth"
	"invoicesvc/internal/httpx"
)

// Endpoint is a registered webhook URL. Secret is only returned on creation.
type Endpoint struct {
	ID         uuid.UUID  `json:"id"`
	URL        string     `json:"url"`
	Secret     string     `json:"secret,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	DisabledAt *time.Time `json:"disabled_at"`
}

// Delivery is the per-endpoint delivery state of an event.
type Delivery struct {
	ID                 uuid.UUID  `json:"id"`
	EndpointID         uuid.UUID  `json:"endpoint_id"`
	Status             string     `json:"status"`
	AttemptCount       int        `json:"attempt_count"`
	NextAttemptAt      *time.Time `json:"next_attempt_at"`
	LastAttemptAt      *time.Time `json:"last_attempt_at"`
	LastResponseStatus *int       `json:"last_response_status"`
	LastError          *string    `json:"last_error"`
}

// Handlers serves /v1/webhook-endpoints and /v1/events.
type Handlers struct {
	DB *pgxpool.Pool
}

func newSecret() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "whsec_" + base64.RawURLEncoding.EncodeToString(b), nil
}

func validURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != "" && len(raw) <= 2048
}

// CreateEndpoint registers a webhook URL and returns its signing secret (once).
func (h *Handlers) CreateEndpoint(w http.ResponseWriter, r *http.Request) error {
	var req struct {
		URL string `json:"url"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		return err
	}
	if !validURL(req.URL) {
		return httpx.NewError(http.StatusUnprocessableEntity, "validation_error", "url must be an absolute http(s) URL").
			WithDetails(map[string]any{"field": "url"})
	}
	secret, err := newSecret()
	if err != nil {
		return err
	}
	ep := Endpoint{URL: req.URL, Secret: secret}
	ep.ID, _ = uuid.NewV7()
	err = h.DB.QueryRow(r.Context(),
		`INSERT INTO webhook_endpoints (id, business_id, url, secret) VALUES ($1, $2, $3, $4) RETURNING created_at`,
		ep.ID, auth.BusinessID(r.Context()), ep.URL, ep.Secret).Scan(&ep.CreatedAt)
	if err != nil {
		return err
	}
	httpx.WriteJSON(w, http.StatusCreated, ep)
	return nil
}

// ListEndpoints lists the business's endpoints (without secrets).
func (h *Handlers) ListEndpoints(w http.ResponseWriter, r *http.Request) error {
	rows, err := h.DB.Query(r.Context(),
		`SELECT id, url, created_at, disabled_at FROM webhook_endpoints WHERE business_id = $1 ORDER BY created_at DESC`,
		auth.BusinessID(r.Context()))
	if err != nil {
		return err
	}
	eps, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Endpoint, error) {
		var e Endpoint
		return e, row.Scan(&e.ID, &e.URL, &e.CreatedAt, &e.DisabledAt)
	})
	if err != nil {
		return err
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.NewList(eps, len(eps)))
	return nil
}

// DeleteEndpoint disables an endpoint. Rows are kept so delivery history stays intact.
func (h *Handlers) DeleteEndpoint(w http.ResponseWriter, r *http.Request) error {
	id, err := httpx.PathUUID(r, "id", "webhook endpoint")
	if err != nil {
		return err
	}
	var e Endpoint
	err = h.DB.QueryRow(r.Context(), `
		UPDATE webhook_endpoints SET disabled_at = COALESCE(disabled_at, now())
		WHERE id = $1 AND business_id = $2
		RETURNING id, url, created_at, disabled_at`,
		id, auth.BusinessID(r.Context())).Scan(&e.ID, &e.URL, &e.CreatedAt, &e.DisabledAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return httpx.ErrNotFound("webhook endpoint")
	}
	if err != nil {
		return err
	}
	httpx.WriteJSON(w, http.StatusOK, e)
	return nil
}

// ListEvents is the reconciliation API: a business that missed webhooks pages through
// this (optionally filtered by ?type=) to catch up. Each event carries its delivery state.
func (h *Handlers) ListEvents(w http.ResponseWriter, r *http.Request) error {
	page, err := httpx.ParsePage(r)
	if err != nil {
		return err
	}
	var typ *string
	if v := r.URL.Query().Get("type"); v != "" {
		typ = &v
	}
	businessID := auth.BusinessID(r.Context())
	rows, err := h.DB.Query(r.Context(), `
		SELECT id, type, created_at, data FROM events
		WHERE business_id = $1
		  AND ($2::text IS NULL OR type = $2)
		  AND ($3::uuid IS NULL OR (created_at, id) <
		       (SELECT created_at, id FROM events WHERE id = $3 AND business_id = $1))
		ORDER BY created_at DESC, id DESC
		LIMIT $4`,
		businessID, typ, page.StartingAfter, page.Limit+1)
	if err != nil {
		return err
	}
	events, err := pgx.CollectRows(rows, scanEvent)
	if err != nil {
		return err
	}
	list := httpx.NewList(events, page.Limit)
	if err := h.attachDeliveries(r.Context(), list.Data); err != nil {
		return err
	}
	httpx.WriteJSON(w, http.StatusOK, list)
	return nil
}

// GetEvent returns one event with its deliveries.
func (h *Handlers) GetEvent(w http.ResponseWriter, r *http.Request) error {
	id, err := httpx.PathUUID(r, "id", "event")
	if err != nil {
		return err
	}
	rows, err := h.DB.Query(r.Context(),
		`SELECT id, type, created_at, data FROM events WHERE id = $1 AND business_id = $2`,
		id, auth.BusinessID(r.Context()))
	if err != nil {
		return err
	}
	ev, err := pgx.CollectExactlyOneRow(rows, scanEvent)
	if errors.Is(err, pgx.ErrNoRows) {
		return httpx.ErrNotFound("event")
	}
	if err != nil {
		return err
	}
	events := []Event{ev}
	if err := h.attachDeliveries(r.Context(), events); err != nil {
		return err
	}
	httpx.WriteJSON(w, http.StatusOK, events[0])
	return nil
}

func scanEvent(row pgx.CollectableRow) (Event, error) {
	var e Event
	return e, row.Scan(&e.ID, &e.Type, &e.CreatedAt, &e.Data)
}

func (h *Handlers) attachDeliveries(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	ids := make([]uuid.UUID, len(events))
	idx := make(map[uuid.UUID]int, len(events))
	for i, e := range events {
		ids[i] = e.ID
		idx[e.ID] = i
	}
	rows, err := h.DB.Query(ctx, `
		SELECT event_id, id, endpoint_id, status, attempt_count,
		       CASE WHEN status = 'pending' THEN next_attempt_at END,
		       last_attempt_at, last_response_status, last_error
		FROM webhook_deliveries WHERE event_id = ANY($1) ORDER BY created_at`, ids)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var eventID uuid.UUID
		var d Delivery
		if err := rows.Scan(&eventID, &d.ID, &d.EndpointID, &d.Status, &d.AttemptCount,
			&d.NextAttemptAt, &d.LastAttemptAt, &d.LastResponseStatus, &d.LastError); err != nil {
			return err
		}
		i := idx[eventID]
		events[i].Deliveries = append(events[i].Deliveries, d)
	}
	return rows.Err()
}

// SeedEndpoint registers url for the business with a fixed secret if it is not registered
// yet. Used only for the docker-compose demo so the webhook sink can verify signatures.
func SeedEndpoint(ctx context.Context, pool *pgxpool.Pool, businessID uuid.UUID, url, secret string) error {
	id, _ := uuid.NewV7()
	_, err := pool.Exec(ctx, `
		INSERT INTO webhook_endpoints (id, business_id, url, secret)
		SELECT $1, $2, $3, $4
		WHERE NOT EXISTS (SELECT 1 FROM webhook_endpoints WHERE business_id = $2 AND url = $3 AND disabled_at IS NULL)`,
		id, businessID, url, secret)
	return err
}
