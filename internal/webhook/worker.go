package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultBackoff is the wait after each failed attempt. Attempt 1 is immediate, so a
// delivery gets 1 + len(backoff) = 7 attempts spread over ~31h42m before it is marked
// failed. Long enough to ride out a receiver's overnight outage.
var DefaultBackoff = []time.Duration{
	30 * time.Second,
	2 * time.Minute,
	10 * time.Minute,
	1 * time.Hour,
	6 * time.Hour,
	24 * time.Hour,
}

const (
	deliveryTimeout = 10 * time.Second
	// lease is how long a claimed delivery is hidden from other workers. If a worker dies
	// mid-delivery the row becomes due again after the lease: at-least-once delivery.
	lease     = 60 * time.Second
	batchSize = 20
)

// Worker delivers pending webhooks. Any number of workers can run; claims use
// FOR UPDATE SKIP LOCKED so they never deliver the same row concurrently.
type Worker struct {
	DB       *pgxpool.Pool
	Client   *http.Client
	Backoff  []time.Duration
	Interval time.Duration
	Now      func() time.Time
}

// NewWorker returns a worker with production defaults.
func NewWorker(pool *pgxpool.Pool) *Worker {
	return &Worker{
		DB:       pool,
		Client:   &http.Client{Timeout: deliveryTimeout},
		Backoff:  DefaultBackoff,
		Interval: time.Second,
		Now:      time.Now,
	}
}

// Run polls for due deliveries until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	t := time.NewTicker(w.Interval)
	defer t.Stop()
	for {
		if _, err := w.RunOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Error("webhook worker", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

type claimed struct {
	id          uuid.UUID
	attempt     int
	url, secret string
	event       Event
}

// RunOnce claims and delivers one batch, returning how many deliveries were attempted.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	rows, err := w.DB.Query(ctx, `
		WITH due AS (
			SELECT id FROM webhook_deliveries
			WHERE status = 'pending' AND next_attempt_at <= now()
			ORDER BY next_attempt_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE webhook_deliveries d
		SET attempt_count = d.attempt_count + 1,
		    last_attempt_at = now(),
		    next_attempt_at = now() + $2::interval
		FROM due, events e, webhook_endpoints ep
		WHERE d.id = due.id AND e.id = d.event_id AND ep.id = d.endpoint_id
		RETURNING d.id, d.attempt_count, ep.url, ep.secret, ep.disabled_at IS NOT NULL,
		          e.id, e.type, e.created_at, e.data`,
		batchSize, fmt.Sprintf("%d seconds", int(lease.Seconds())))
	if err != nil {
		return 0, err
	}
	var batch []claimed
	var disabled []uuid.UUID
	for rows.Next() {
		var c claimed
		var isDisabled bool
		if err := rows.Scan(&c.id, &c.attempt, &c.url, &c.secret, &isDisabled,
			&c.event.ID, &c.event.Type, &c.event.CreatedAt, &c.event.Data); err != nil {
			rows.Close()
			return 0, err
		}
		if isDisabled {
			disabled = append(disabled, c.id)
			continue
		}
		batch = append(batch, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	for _, id := range disabled {
		w.finish(ctx, id, "failed", nil, "endpoint disabled")
	}

	// Deliver concurrently so one slow receiver does not hold up the batch.
	var wg sync.WaitGroup
	for _, c := range batch {
		wg.Add(1)
		go func(c claimed) {
			defer wg.Done()
			w.deliver(ctx, c)
		}(c)
	}
	wg.Wait()
	return len(batch) + len(disabled), nil
}

func (w *Worker) deliver(ctx context.Context, c claimed) {
	body, _ := json.Marshal(c.event)
	ts := w.Now().Unix()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		w.fail(ctx, c, nil, err.Error())
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "invoicesvc-webhooks/1")
	req.Header.Set(HeaderEventID, c.event.ID.String())
	req.Header.Set(HeaderEventType, c.event.Type)
	req.Header.Set(HeaderSignature, Sign(c.secret, ts, body))

	resp, err := w.Client.Do(req)
	if err != nil {
		w.fail(ctx, c, nil, err.Error())
		return
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10)) //nolint:errcheck
	resp.Body.Close()

	status := resp.StatusCode
	if status >= 200 && status < 300 {
		w.finish(ctx, c.id, "succeeded", &status, "")
		slog.Info("webhook delivered", "delivery_id", c.id, "event_id", c.event.ID, "type", c.event.Type, "attempt", c.attempt)
		return
	}
	w.fail(ctx, c, &status, fmt.Sprintf("receiver responded %d", status))
}

// fail schedules the next retry, or marks the delivery failed once the budget is spent.
func (w *Worker) fail(ctx context.Context, c claimed, status *int, msg string) {
	if c.attempt > len(w.Backoff) {
		w.finish(ctx, c.id, "failed", status, msg)
		slog.Warn("webhook delivery exhausted retries", "delivery_id", c.id, "event_id", c.event.ID, "attempts", c.attempt, "err", msg)
		return
	}
	wait := w.Backoff[c.attempt-1]
	_, err := w.DB.Exec(ctx, `
		UPDATE webhook_deliveries
		SET next_attempt_at = now() + $2::interval, last_response_status = $3, last_error = $4
		WHERE id = $1`,
		c.id, fmt.Sprintf("%d milliseconds", wait.Milliseconds()), status, msg)
	if err != nil {
		slog.Error("schedule webhook retry", "delivery_id", c.id, "err", err)
	}
	slog.Warn("webhook delivery failed, will retry", "delivery_id", c.id, "event_id", c.event.ID,
		"attempt", c.attempt, "retry_in", wait.String(), "err", msg)
}

func (w *Worker) finish(ctx context.Context, id uuid.UUID, status string, code *int, msg string) {
	_, err := w.DB.Exec(ctx, `
		UPDATE webhook_deliveries
		SET status = $2, last_response_status = $3, last_error = NULLIF($4, '')
		WHERE id = $1`, id, status, code, msg)
	if err != nil && err != pgx.ErrNoRows {
		slog.Error("finish webhook delivery", "delivery_id", id, "err", err)
	}
}
