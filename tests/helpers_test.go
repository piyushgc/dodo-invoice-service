// Package tests holds integration tests that run the real router against a real Postgres
// and an in-process mock PSP. Run them with:
//
//	docker compose --profile test run --rm tests
//
// or, with Postgres from docker compose on localhost:
//
//	TEST_DATABASE_URL=postgres://invoices:invoices@localhost:5432/invoices?sslmode=disable go test ./tests/ -v
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"invoicesvc/internal/app"
	"invoicesvc/internal/auth"
	"invoicesvc/internal/db"
	"invoicesvc/internal/psp/mock"
	"invoicesvc/migrations"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		fmt.Println("TEST_DATABASE_URL not set; skipping integration tests")
		os.Exit(0)
	}
	ctx := context.Background()
	var err error
	if pool, err = db.Connect(ctx, url); err != nil {
		fmt.Println("connect:", err)
		os.Exit(1)
	}
	if err := db.Migrate(ctx, pool, migrations.FS); err != nil {
		fmt.Println("migrate:", err)
		os.Exit(1)
	}
	code := m.Run()
	pool.Close()
	os.Exit(code)
}

// env is one isolated tenant (fresh business + API key) with its own mock PSP.
type env struct {
	t      *testing.T
	app    *app.App
	psp    *mock.Server
	base   string
	apiKey string
}

func setup(t *testing.T) *env {
	t.Helper()
	// Short delays keep the suite fast; the semantics are the same as in compose
	// (PSP timeout 5s, tok_timeout 30s, first reconcile after 10s).
	psp := mock.New(20*time.Millisecond, 2*time.Second)
	pspSrv := httptest.NewServer(psp.Handler())
	t.Cleanup(pspSrv.Close)

	a := app.New(pool, app.Config{
		PSPBaseURL:     pspSrv.URL,
		PSPTimeout:     500 * time.Millisecond,
		ReconcileAfter: time.Second,
	})
	apiSrv := httptest.NewServer(a.Handler)
	t.Cleanup(apiSrv.Close)

	key, _, _, err := auth.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := auth.CreateBusinessWithKey(context.Background(), pool, "Test "+t.Name(), key); err != nil {
		t.Fatal(err)
	}
	return &env{t: t, app: a, psp: psp, base: apiSrv.URL, apiKey: key}
}

type response struct {
	status int
	body   []byte
	header http.Header
}

func (r response) json(t *testing.T) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(r.body, &m); err != nil {
		t.Fatalf("decode %s: %v", r.body, err)
	}
	return m
}

func (e *env) do(method, path string, body any, headers map[string]string) response {
	e.t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, e.base+path, rd)
	req.Header.Set("Authorization", "Bearer "+e.apiKey)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return response{status: resp.StatusCode, body: b, header: resp.Header}
}

func (e *env) pay(invoiceID, token, idemKey string) response {
	return e.do(http.MethodPost, "/v1/invoices/"+invoiceID+"/pay",
		map[string]string{"card_token": token}, map[string]string{"Idempotency-Key": idemKey})
}

// createInvoice creates a customer and an open invoice for 2 x 1250 = 2500 cents.
func (e *env) createInvoice() string {
	e.t.Helper()
	c := e.do(http.MethodPost, "/v1/customers", map[string]string{"name": "Ada", "email": "ada@example.com"}, nil)
	if c.status != http.StatusCreated {
		e.t.Fatalf("create customer: %d %s", c.status, c.body)
	}
	inv := e.do(http.MethodPost, "/v1/invoices", map[string]any{
		"customer_id": c.json(e.t)["id"],
		"due_date":    "2030-01-31",
		"line_items":  []map[string]any{{"description": "Widget", "quantity": 2, "unit_amount_cents": 1250}},
	}, nil)
	if inv.status != http.StatusCreated {
		e.t.Fatalf("create invoice: %d %s", inv.status, inv.body)
	}
	m := inv.json(e.t)
	if m["total_cents"] != float64(2500) || m["status"] != "open" {
		e.t.Fatalf("unexpected invoice: %s", inv.body)
	}
	return m["id"].(string)
}

func (e *env) invoiceStatus(id string) string {
	e.t.Helper()
	var s string
	if err := pool.QueryRow(context.Background(), `SELECT status FROM invoices WHERE id = $1`, uuid.MustParse(id)).Scan(&s); err != nil {
		e.t.Fatal(err)
	}
	return s
}

// attemptCounts returns how many attempts the invoice has in each status.
func (e *env) attemptCounts(id string) map[string]int {
	e.t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT status, count(*) FROM payment_attempts WHERE invoice_id = $1 GROUP BY status`, uuid.MustParse(id))
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var s string
		var n int
		if err := rows.Scan(&s, &n); err != nil {
			e.t.Fatal(err)
		}
		out[s] = n
	}
	return out
}

func errorCode(t *testing.T, r response) string {
	t.Helper()
	m := r.json(t)
	errObj, _ := m["error"].(map[string]any)
	code, _ := errObj["code"].(string)
	return code
}
