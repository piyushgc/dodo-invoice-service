# AI Usage

> **Note to self before submitting:** this file is graded on honesty and specificity.
> The factual record below is accurate. The sections marked ✍️ must be rewritten in **my
> own words**, about decisions I actually made or checked. Delete this note afterwards.

## Tools used and for what

- **Claude Code (Claude Opus, in VS Code)** was the main tool. I gave it the assignment brief and asked for a structured breakdown of what was required and how it would be graded. Then I asked it to build the service in **Go** (it had first proposed Rust/Axum, the brief's preference; I overrode that). It wrote most of the code, the migrations, docker-compose, the tests and the first drafts of DESIGN.md, README.md and openapi.yaml.
- It also ran the verification loop in my terminal:
  - `go build`, `go vet`, and the integration tests against the compose Postgres (repeated 5 times to check for flakiness),
  - a live `docker compose up` session with curl for every scenario, including waiting out a real 30s `tok_timeout` to watch the reconciler settle it and the webhook sink verify the signatures.

## Three decisions I made myself ✍️

Rewrite each one: what the AI proposed (if anything), what I chose, and why. Candidates from this session, keeping only the ones that are genuinely mine:

1. **Go instead of Rust.** The AI's first plan was Rust/Axum because the brief prefers it. I chose Go because … *(my reason)*.
2. ✍️ *(e.g. a pending payment returns 202 rather than 504 or a failure; `uncollectible` stays payable; no `draft` state; declines return 402 in the error envelope.)* Pick the ones I deliberated on and explain my reasoning.
3. ✍️ *(…)*

## Things the AI got wrong or I had to correct

These are real, from this session:

1. **Dependency/toolchain mismatch.** It first pulled `pgx v5.11`, which silently raised `go.mod` to `go 1.25` while my machine has Go 1.24.6. It was caught on review and pinned to `pgx v5.7.5`.
2. **A wrong test expectation.** The first version of the "concurrent duplicates of one idempotency key" test asserted that every response was 200. That's wrong for this design: a duplicate that arrives while the original is still waiting on the PSP correctly gets **202** for the same in-flight attempt. The test now asserts 200 or 202 and that every response refers to the **same payment attempt**, with exactly one PSP call.
3. **Environment assumptions.** The first compose file hard-coded host ports 5432 and 8080, which were already taken on my machine (a local Postgres and a RethinkDB on 8080, which answered the first curl calls with 403/405). Host ports are now overridable (`API_PORT`, `DB_PORT`), and the database defaults to 5433.

✍️ Add anything I corrected myself while reviewing the code.

## How I verified correctness

- The three required integration tests run against real Postgres (not mocks of the database) and passed 5 runs in a row.
- In a manual run on `docker compose up`:
  - success gave 200 and the invoice became `paid`,
  - `tok_card_declined` gave 402 and the invoice stayed `open`,
  - `tok_timeout` gave 202 after 5s; about 30s later the reconciler marked it `paid`, and replaying the key returned 200 with `Idempotent-Replayed: true`,
  - voiding a paid invoice gave 409 `invalid_state_transition`,
  - a request with no key gave 401,
  - the webhook sink logged `signature: valid` for `invoice.created`, `invoice.paid` and `invoice.payment_failed`.
- ✍️ What I personally read line by line: at least `internal/payment/payment.go`, `reconciler.go` and `internal/invoice/statemachine.go`, since those are the walkthroughs in the video.
