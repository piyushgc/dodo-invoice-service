-- Initial schema for the invoice & payment service.
-- All money columns are BIGINT minor units (USD cents). There is no NUMERIC/REAL anywhere.

CREATE TABLE businesses (
    id          uuid        PRIMARY KEY,
    name        text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- Only a SHA-256 of the key is stored. `prefix` is the first few characters, kept so a
-- human can recognise a key in a list ("sk_AbC123…") without us being able to use it.
CREATE TABLE api_keys (
    id           uuid        PRIMARY KEY,
    business_id  uuid        NOT NULL REFERENCES businesses(id),
    prefix       text        NOT NULL,
    key_hash     bytea       NOT NULL UNIQUE,
    created_at   timestamptz NOT NULL DEFAULT now(),
    revoked_at   timestamptz
);
CREATE INDEX api_keys_business_idx ON api_keys (business_id);

CREATE TABLE customers (
    id           uuid        PRIMARY KEY,
    business_id  uuid        NOT NULL REFERENCES businesses(id),
    name         text        NOT NULL,
    email        text        NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX customers_business_created_idx ON customers (business_id, created_at DESC, id DESC);

CREATE TABLE invoices (
    id                       uuid        PRIMARY KEY,
    business_id              uuid        NOT NULL REFERENCES businesses(id),
    customer_id              uuid        NOT NULL REFERENCES customers(id),
    status                   text        NOT NULL CHECK (status IN ('open', 'paid', 'void', 'uncollectible')),
    currency                 text        NOT NULL DEFAULT 'USD' CHECK (currency = 'USD'),
    total_cents              bigint      NOT NULL CHECK (total_cents > 0),
    due_date                 date        NOT NULL,
    created_at               timestamptz NOT NULL DEFAULT now(),
    updated_at               timestamptz NOT NULL DEFAULT now(),
    paid_at                  timestamptz,
    voided_at                timestamptz,
    marked_uncollectible_at  timestamptz
);
CREATE INDEX invoices_business_created_idx        ON invoices (business_id, created_at DESC, id DESC);
CREATE INDEX invoices_business_status_created_idx ON invoices (business_id, status, created_at DESC, id DESC);
CREATE INDEX invoices_customer_idx                ON invoices (customer_id);

-- Line items are immutable once written; amount_cents is derived and the CHECK makes the
-- database refuse any row where it does not equal quantity * unit_amount_cents.
CREATE TABLE invoice_line_items (
    invoice_id         uuid    NOT NULL REFERENCES invoices(id),
    position           int     NOT NULL,
    description        text    NOT NULL,
    quantity           bigint  NOT NULL CHECK (quantity > 0),
    unit_amount_cents  bigint  NOT NULL CHECK (unit_amount_cents >= 0),
    amount_cents       bigint  NOT NULL CHECK (amount_cents = quantity * unit_amount_cents),
    PRIMARY KEY (invoice_id, position)
);

CREATE TABLE payment_attempts (
    id             uuid        PRIMARY KEY,  -- also sent to the PSP as its idempotency key
    invoice_id     uuid        NOT NULL REFERENCES invoices(id),
    business_id    uuid        NOT NULL REFERENCES businesses(id),
    status         text        NOT NULL CHECK (status IN ('pending', 'succeeded', 'failed')),
    amount_cents   bigint      NOT NULL CHECK (amount_cents > 0),
    card_token     text        NOT NULL,
    psp_ref        text,
    failure_code   text,
    check_count    int         NOT NULL DEFAULT 0,
    next_check_at  timestamptz,           -- when the reconciler should ask the PSP about a pending attempt
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    resolved_at    timestamptz
);
CREATE INDEX payment_attempts_invoice_idx ON payment_attempts (invoice_id, created_at);
CREATE INDEX payment_attempts_reconcile_idx ON payment_attempts (next_check_at) WHERE status = 'pending';
-- Backstops for the row lock in the /pay path: the database itself refuses a second
-- in-flight attempt or a second successful charge for the same invoice.
CREATE UNIQUE INDEX payment_attempts_one_pending_per_invoice ON payment_attempts (invoice_id) WHERE status = 'pending';
CREATE UNIQUE INDEX payment_attempts_one_success_per_invoice ON payment_attempts (invoice_id) WHERE status = 'succeeded';

-- response_body is bytea, not jsonb, so a replay returns byte-for-byte what was first sent.
CREATE TABLE idempotency_keys (
    business_id         uuid        NOT NULL REFERENCES businesses(id),
    key                 text        NOT NULL,
    request_hash        bytea       NOT NULL,
    payment_attempt_id  uuid        REFERENCES payment_attempts(id),
    response_status     int,
    response_body       bytea,
    created_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (business_id, key)
);
CREATE INDEX idempotency_keys_attempt_idx ON idempotency_keys (payment_attempt_id);

CREATE TABLE webhook_endpoints (
    id           uuid        PRIMARY KEY,
    business_id  uuid        NOT NULL REFERENCES businesses(id),
    url          text        NOT NULL,
    secret       text        NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    disabled_at  timestamptz
);
CREATE INDEX webhook_endpoints_business_idx ON webhook_endpoints (business_id);

-- The event log doubles as the transactional outbox: events are inserted in the same
-- transaction as the state change they describe, and are what GET /v1/events serves.
CREATE TABLE events (
    id           uuid        PRIMARY KEY,
    business_id  uuid        NOT NULL REFERENCES businesses(id),
    type         text        NOT NULL,
    data         jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX events_business_created_idx ON events (business_id, created_at DESC, id DESC);

CREATE TABLE webhook_deliveries (
    id                    uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id              uuid        NOT NULL REFERENCES events(id),
    endpoint_id           uuid        NOT NULL REFERENCES webhook_endpoints(id),
    status                text        NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'succeeded', 'failed')),
    attempt_count         int         NOT NULL DEFAULT 0,
    next_attempt_at       timestamptz NOT NULL DEFAULT now(),
    last_attempt_at       timestamptz,
    last_response_status  int,
    last_error            text,
    created_at            timestamptz NOT NULL DEFAULT now(),
    UNIQUE (event_id, endpoint_id)
);
CREATE INDEX webhook_deliveries_due_idx ON webhook_deliveries (next_attempt_at) WHERE status = 'pending';
