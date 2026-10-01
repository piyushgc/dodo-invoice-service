// Package app wires handlers, middleware and background workers together. It is used by
// cmd/api and by the integration tests, so tests exercise the real router.
package app

import (
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"invoicesvc/internal/auth"
	"invoicesvc/internal/customer"
	"invoicesvc/internal/httpx"
	"invoicesvc/internal/invoice"
	"invoicesvc/internal/payment"
	"invoicesvc/internal/psp"
	"invoicesvc/internal/webhook"
)

// Config holds the tunables of the service.
type Config struct {
	PSPBaseURL     string
	PSPTimeout     time.Duration
	ReconcileAfter time.Duration
}

// App is the assembled service.
type App struct {
	Handler  http.Handler
	Payments *payment.Service
	Webhooks *webhook.Worker
}

// New builds the service.
func New(pool *pgxpool.Pool, cfg Config) *App {
	if cfg.ReconcileAfter <= cfg.PSPTimeout {
		// The reconciler must never look at an attempt whose original PSP call may still
		// be in flight, or a "not found" could be misread as "never charged".
		cfg.ReconcileAfter = 2 * cfg.PSPTimeout
	}
	payments := &payment.Service{
		DB:                  pool,
		PSP:                 psp.NewClient(cfg.PSPBaseURL, cfg.PSPTimeout),
		ReconcileAfter:      cfg.ReconcileAfter,
		MaxReconcileBackoff: 5 * time.Minute,
	}
	keys := &auth.Handlers{DB: pool}
	customers := &customer.Handlers{DB: pool}
	invoices := &invoice.Handlers{DB: pool}
	hooks := &webhook.Handlers{DB: pool}

	api := http.NewServeMux()
	api.Handle("POST /v1/api-keys", httpx.Handle(keys.Create))
	api.Handle("GET /v1/api-keys", httpx.Handle(keys.List))
	api.Handle("DELETE /v1/api-keys/{id}", httpx.Handle(keys.Revoke))

	api.Handle("POST /v1/customers", httpx.Handle(customers.Create))
	api.Handle("GET /v1/customers", httpx.Handle(customers.List))
	api.Handle("GET /v1/customers/{id}", httpx.Handle(customers.Get))

	api.Handle("POST /v1/invoices", httpx.Handle(invoices.Create))
	api.Handle("GET /v1/invoices", httpx.Handle(invoices.List))
	api.Handle("GET /v1/invoices/{id}", httpx.Handle(invoices.Get))
	api.Handle("POST /v1/invoices/{id}/pay", httpx.Handle(payments.Handler))
	api.Handle("POST /v1/invoices/{id}/void", httpx.Handle(invoices.Void))
	api.Handle("POST /v1/invoices/{id}/mark-uncollectible", httpx.Handle(invoices.MarkUncollectible))

	api.Handle("POST /v1/webhook-endpoints", httpx.Handle(hooks.CreateEndpoint))
	api.Handle("GET /v1/webhook-endpoints", httpx.Handle(hooks.ListEndpoints))
	api.Handle("DELETE /v1/webhook-endpoints/{id}", httpx.Handle(hooks.DeleteEndpoint))
	api.Handle("GET /v1/events", httpx.Handle(hooks.ListEvents))
	api.Handle("GET /v1/events/{id}", httpx.Handle(hooks.GetEvent))

	root := http.NewServeMux()
	root.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Ping(r.Context()); err != nil {
			httpx.WriteError(w, r, httpx.NewError(http.StatusServiceUnavailable, "unhealthy", "database unreachable"))
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	root.Handle("/v1/", auth.Middleware(pool)(api))
	root.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteError(w, r, httpx.NewError(http.StatusNotFound, "route_not_found", "no route for "+r.Method+" "+r.URL.Path))
	})

	return &App{
		Handler:  httpx.Middleware(root),
		Payments: payments,
		Webhooks: webhook.NewWorker(pool),
	}
}
