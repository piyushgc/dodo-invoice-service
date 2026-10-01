// Package customer implements create/get/list of customers, scoped to a business.
package customer

import (
	"errors"
	"net/http"
	"net/mail"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"invoicesvc/internal/auth"
	"invoicesvc/internal/httpx"
)

// Customer is a business's customer.
type Customer struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
}

// Handlers serves /v1/customers.
type Handlers struct {
	DB *pgxpool.Pool
}

type createRequest struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

// Create adds a customer to the authenticated business.
func (h *Handlers) Create(w http.ResponseWriter, r *http.Request) error {
	var req createRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		return err
	}
	req.Name = strings.TrimSpace(req.Name)
	req.Email = strings.TrimSpace(req.Email)
	if req.Name == "" || len(req.Name) > 200 {
		return validation("name", "name is required and must be at most 200 characters")
	}
	if addr, err := mail.ParseAddress(req.Email); err != nil || addr.Address != req.Email || len(req.Email) > 320 {
		return validation("email", "email must be a valid address like jane@example.com")
	}

	c := Customer{Name: req.Name, Email: req.Email}
	c.ID, _ = uuid.NewV7()
	err := h.DB.QueryRow(r.Context(),
		`INSERT INTO customers (id, business_id, name, email) VALUES ($1, $2, $3, $4) RETURNING created_at`,
		c.ID, auth.BusinessID(r.Context()), c.Name, c.Email).Scan(&c.CreatedAt)
	if err != nil {
		return err
	}
	httpx.WriteJSON(w, http.StatusCreated, c)
	return nil
}

// Get returns one customer. Another business's customer is indistinguishable from a
// missing one (404), so IDs cannot be probed across tenants.
func (h *Handlers) Get(w http.ResponseWriter, r *http.Request) error {
	id, err := httpx.PathUUID(r, "id", "customer")
	if err != nil {
		return err
	}
	var c Customer
	err = h.DB.QueryRow(r.Context(),
		`SELECT id, name, email, created_at FROM customers WHERE id = $1 AND business_id = $2`,
		id, auth.BusinessID(r.Context())).Scan(&c.ID, &c.Name, &c.Email, &c.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return httpx.ErrNotFound("customer")
	}
	if err != nil {
		return err
	}
	httpx.WriteJSON(w, http.StatusOK, c)
	return nil
}

// List returns customers newest first with keyset pagination.
func (h *Handlers) List(w http.ResponseWriter, r *http.Request) error {
	page, err := httpx.ParsePage(r)
	if err != nil {
		return err
	}
	rows, err := h.DB.Query(r.Context(), `
		SELECT id, name, email, created_at FROM customers
		WHERE business_id = $1
		  AND ($2::uuid IS NULL OR (created_at, id) <
		       (SELECT created_at, id FROM customers WHERE id = $2 AND business_id = $1))
		ORDER BY created_at DESC, id DESC
		LIMIT $3`,
		auth.BusinessID(r.Context()), page.StartingAfter, page.Limit+1)
	if err != nil {
		return err
	}
	list, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Customer, error) {
		var c Customer
		return c, row.Scan(&c.ID, &c.Name, &c.Email, &c.CreatedAt)
	})
	if err != nil {
		return err
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.NewList(list, page.Limit))
	return nil
}

func validation(field, msg string) error {
	return httpx.NewError(http.StatusUnprocessableEntity, "validation_error", msg).
		WithDetails(map[string]any{"field": field})
}
