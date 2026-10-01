# Invoice & Payment Service

A small billing backend:
- businesses create customers and invoices,
- customers pay invoices through a (mock) payment processor,
- businesses receive signed webhooks when an invoice changes state.

- **[DESIGN.md](DESIGN.md)** covers the data model, state machine, failure modes, webhooks and the API key model.
- **[openapi.yaml](openapi.yaml)** is the API reference.
- **[AI_USAGE.md](AI_USAGE.md)** describes how AI tools were used.

## Demo Video

> **TODO:** add the Loom / Drive link here. It must open without a login.

## Why Go instead of Rust

The brief prefers Rust. I used Go because it's the language I can reason about most fluently under a 4–6 hour budget. The parts of this problem that matter map directly onto Go's standard library:
- per-call deadlines with `context`,
- goroutines for the webhook worker and the reconciler,
- `net/http`, whose 1.22+ router needs no framework,
- `crypto/hmac` for webhook signatures.

There are only two third-party dependencies: `jackc/pgx` (Postgres driver) and `google/uuid` (UUIDv7).

## Running it

```bash
docker compose up --build
```

This starts four services. Migrations run automatically when the API starts, and a demo business is seeded:

| Service | Host port | Purpose |
|---|---|---|
| `api` | 8080 | The invoice service, webhook worker and payment reconciler |
| `mockpsp` | 8081 | Mock payment processor (`tok_*` tokens) |
| `webhook-sink` | 9000 | Demo receiver that verifies webhook signatures and logs them |
| `db` | 5433 | Postgres 16 |

- **Demo API key:** `sk_test_demo_key_local_only`. It is seeded only via `DEMO_API_KEY` in docker-compose.
- **Webhooks:** a webhook endpoint pointing at the sink is registered automatically. Watch deliveries with:

  ```bash
  docker compose logs -f webhook-sink
  ```

- **Port conflicts:** if 8080 or 5433 is taken on your machine, override them: `API_PORT=8090 DB_PORT=5434 docker compose up --build`.

## curl examples

```bash
export API=http://localhost:8080
export KEY="Authorization: Bearer sk_test_demo_key_local_only"
```

**1. Create a customer**

```bash
curl -s -X POST $API/v1/customers -H "$KEY" -H 'Content-Type: application/json' \
  -d '{"name":"Jane Doe","email":"jane@example.com"}'
# 201 {"id":"<customer_id>","name":"Jane Doe","email":"jane@example.com","created_at":"..."}
```

**2. Create an invoice.** The server computes all amounts. Sending `total_cents` is rejected with a 400.

```bash
curl -s -X POST $API/v1/invoices -H "$KEY" -H 'Content-Type: application/json' -d '{
  "customer_id": "<customer_id>",
  "due_date": "2026-12-31",
  "line_items": [
    {"description": "Consulting", "quantity": 3, "unit_amount_cents": 15000},
    {"description": "Hosting",    "quantity": 1, "unit_amount_cents": 2999}
  ]}'
# 201 {"id":"<invoice_id>","status":"open","total_cents":47999,"line_items":[...], ...}
```

**3. Pay it (success).** The `Idempotency-Key` header is required.

```bash
curl -s -X POST $API/v1/invoices/<invoice_id>/pay -H "$KEY" \
  -H 'Idempotency-Key: pay-001' -d '{"card_token":"tok_success"}'
# 200 {"payment_attempt":{"status":"succeeded","psp_ref":"..."},"invoice":{"status":"paid", ...}}
```

Running the same command again returns the same body, with the header `Idempotent-Replayed: true`, and the PSP is not called a second time.

**4. Pay a different invoice with a declined card**

```bash
curl -s -X POST $API/v1/invoices/<other_invoice_id>/pay -H "$KEY" \
  -H 'Idempotency-Key: pay-002' -d '{"card_token":"tok_card_declined"}'
# 402 {"error":{"code":"payment_failed","message":"the payment was not successful: card_declined",
#      "details":{"decline_code":"card_declined","invoice_status":"open","payment_attempt":{...}}}}
```

**More examples**
- **PSP timeout:**
  - Pay with `tok_timeout`. You get **202 after 5s** (no hang), with the attempt `pending`.
  - About 30s later, the reconciler marks the invoice `paid` and `invoice.paid` is delivered.
  - Repeating the request with the same key returns the final 200.
- **Other endpoints:**
  - `GET /v1/invoices?status=paid`
  - `POST /v1/invoices/{id}/void` (returns 409 `invalid_state_transition` once the invoice is paid)
  - `GET /v1/events` lists events and their delivery status, so a business can catch up on missed webhooks.

## Tests

The three required tests live in [tests/payment_test.go](tests/payment_test.go). They run the real router against real Postgres and an in-process mock PSP:

| Test | What it proves |
|---|---|
| `TestConcurrentPaymentsChargeOnce` | 25 parallel `/pay` calls give exactly one 200, one PSP call, one succeeded attempt, and an invoice in `paid` |
| `TestIdempotentRetryDoesNotRecharge` | The same key returns a byte-identical response with no second PSP call. The same key with a different body gives 422. Concurrent duplicates of one key still charge once. |
| `TestPSPFailuresDoNotCorruptInvoice` | `tok_timeout` returns 202 quickly, blocks both a second payment and void while pending, then the reconciler marks the invoice `paid`. With `tok_network_error` the attempt fails safely and the invoice can be paid again. |

```bash
# inside Docker (no local Go needed)
docker compose --profile test run --rm tests

# or with local Go 1.24+, against the compose database
docker compose up -d db
TEST_DATABASE_URL="postgres://invoices:invoices@localhost:5433/invoices?sslmode=disable" go test ./tests/ -v -count=1
```

The tests shorten timings to keep the suite fast. The semantics are the same:

| Setting | Tests | Compose |
|---|---|---|
| PSP timeout | 500ms | 5s |
| `tok_timeout` delay | 2s | 30s |
| First reconcile | 1s | 10s |

## Layout

```
cmd/api            HTTP API + webhook worker + payment reconciler
cmd/mockpsp        mock payment processor (separate service)
cmd/webhooksink    demo webhook receiver (verifies signatures)
internal/payment   POST /pay: reserve → charge → apply, idempotency, reconciler   ← the core
internal/invoice   invoices, line items, state machine (statemachine.go)
internal/webhook   outbox (Emit), HMAC signing, delivery worker, endpoints/events API
internal/auth      API keys: generation, hashing, middleware, rotation/revocation
internal/money     int64 cents arithmetic with overflow checks
internal/httpx     error envelope, JSON decoding, pagination, request-ID/logging middleware
migrations/        SQL migrations (embedded, applied on start-up)
tests/             integration tests
```

## Configuration (api)

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | local compose DB | Postgres connection string |
| `PSP_BASE_URL` | `http://localhost:8081` | Mock PSP base URL |
| `PSP_TIMEOUT` | `5s` | Hard deadline on each PSP call |
| `RECONCILE_AFTER` | `10s` | When the reconciler first checks an unresolved attempt. Must be longer than `PSP_TIMEOUT`. |
| `DEMO_API_KEY`, `DEMO_WEBHOOK_URL`, `DEMO_WEBHOOK_SECRET` | unset | Local demo seeding only |
