# AI Usage

## Tools used and for what

**Claude Code (Claude Opus, in VS Code)** was my main tool, and it did most of the typing.

- **Reading the brief.** I gave it the assignment document and asked for a structured breakdown of what had to be built and how it is graded.
- **Code.** It wrote most of the code, the SQL migration, docker-compose, the integration tests and a PowerShell demo script (`scripts/demo.ps1`).
- **Docs.** It wrote the first drafts of DESIGN.md, the README and openapi.yaml.
- **Verification.** It ran `go build`, `go vet` and the integration tests against the compose Postgres. It repeated the tests 5 times to check for flaky results, and ran a live `docker compose up` session with curl for every scenario.
- **Video prep.** It helped me prepare: it explained each demo step and each part of `payment.go` in simple terms, and produced a step-by-step guide of what to show on screen.

I used no other AI tools.

## Three decisions

Only the first decision went against what the AI suggested. For the other two, the AI proposed the approach. I'm listing them because I reviewed them, understood them well enough to explain them on camera, and chose to keep them.

1. **Go instead of Rust: my decision, against the AI's first plan.**
   - **AI proposed:** Rust with Axum, because the brief prefers Rust.
   - **I chose:** Go.
   - **Why:** I work as a Go backend developer at my current company and have 2 years of Go experience. Go is simple and fast. Within a 4–6 hour budget, I can reason about correctness much better in Go than in Rust. I would rather be sure the payment logic is right than fight the language.

2. **On a PSP timeout, return 202 "pending" and don't guess: AI proposal, which I kept.**
   - **AI proposed:** leave the attempt `pending`, return 202, and let a background reconciler ask the PSP for the real result.
   - **I chose:** to keep it after walking through the code.
   - **Why:** when the PSP times out, the card may already have been charged. Marking it "failed" would let the customer pay again and be charged twice. Marking it "paid" would be a guess with no proof. Pending, plus blocking new payments while pending, is the only safe answer. This is the failure mode I explain in the video.

3. **Lock the invoice row with `SELECT … FOR UPDATE`: AI proposal, which I kept.**
   - **AI proposed:** lock the invoice row before creating a payment attempt, with a partial unique index (one pending attempt per invoice) as a backstop. The alternatives were advisory locks or optimistic concurrency.
   - **I chose:** to keep it.
   - **Why:** it's the simplest mechanism to explain and to trust. Payments on the same invoice simply run one at a time. The lock is held only for a few milliseconds, never during the PSP call. The concurrency test (25 parallel requests, exactly one charge) confirmed it.

## Things the AI got wrong or that had to be corrected

These all happened in this session:

1. **Dependency and toolchain mismatch.** The AI first pulled `pgx v5.11`, which silently raised `go.mod` to `go 1.25`. My machine has Go 1.24.6. This was caught and `pgx` was pinned to v5.7.5.
2. **A wrong test expectation.** The first version of the test "concurrent duplicates of one idempotency key" expected every response to be 200. That's wrong for this design: a duplicate that arrives while the original is still waiting on the PSP correctly gets 202 for the same in-flight attempt. The test now accepts 200 or 202, and checks that all responses refer to the same payment attempt with exactly one PSP call.
3. **Assumptions about my environment.** The first compose file hard-coded host ports 5432 and 8080. On my machine both were already in use, by a local Postgres and a RethinkDB on 8080. RethinkDB answered the first curl calls with 403 and 405, which looked like bugs in the app. Host ports are now overridable (`API_PORT`, `DB_PORT`), and the database defaults to 5433.

## How I verified correctness

- **Tests.** The three required integration tests run against real Postgres, not a mocked database. They passed 5 runs in a row, and also inside Docker (`docker compose --profile test run --rm tests`).
- **The live demo.** I ran the whole demo myself step by step before recording (`scripts/demo.ps1`). I checked each result:

  | Scenario | Result |
  |---|---|
  | Pay with `tok_success` | 200, and the invoice becomes `paid` |
  | Retry with the same key | Same response, with `Idempotent-Replayed: true` |
  | Pay with `tok_card_declined` | 402, and the invoice stays `open` |
  | Void a paid invoice | 409 `invalid_state_transition` |
  | Pay with `tok_timeout` | 202 after 5 s |
  | Second payment while pending | 409 `payment_in_progress` |
  | About 45 s later | The reconciler marked the invoice `paid` |
  | All webhooks | Arrived at the sink with valid signatures |

- **Reading the code.** I read `internal/payment/payment.go` (the reserve → charge → apply flow) and `reconciler.go` before recording, so I could walk through them line by line in the video.
