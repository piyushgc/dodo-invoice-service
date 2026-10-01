package payment

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"invoicesvc/internal/psp"
)

// reconcileLease hides a claimed attempt from other reconciler instances while it is
// being checked. If the instance dies, the attempt becomes due again after the lease.
const reconcileLease = 30 * time.Second

// RunReconciler periodically settles pending payment attempts until ctx is cancelled.
func (s *Service) RunReconciler(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if _, err := s.ReconcileOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Error("reconciler", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// ReconcileOnce checks every due pending attempt against the PSP (by its idempotency key,
// which is the attempt ID) and applies whatever the PSP knows:
//
//   - succeeded / failed → apply it (invoice → paid, or attempt → failed)
//   - still processing, or PSP unreachable → check again later with exponential backoff
//   - PSP has no record → the charge never reached the PSP, no money moved; mark the
//     attempt failed so the invoice can be paid again
func (s *Service) ReconcileOnce(ctx context.Context) (int, error) {
	rows, err := s.DB.Query(ctx, `
		UPDATE payment_attempts
		SET check_count = check_count + 1, next_check_at = now() + ($2::bigint || ' milliseconds')::interval
		WHERE id IN (
			SELECT id FROM payment_attempts
			WHERE status = 'pending' AND next_check_at <= now()
			ORDER BY next_check_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED)
		RETURNING id, check_count`,
		20, reconcileLease.Milliseconds())
	if err != nil {
		return 0, err
	}
	type due struct {
		id    uuid.UUID
		count int
	}
	var batch []due
	for rows.Next() {
		var d due
		if err := rows.Scan(&d.id, &d.count); err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	for _, d := range batch {
		res, err := s.PSP.GetCharge(ctx, d.id.String())
		switch {
		case errors.Is(err, psp.ErrNotFound):
			res = psp.Result{Status: psp.StatusFailed, Code: "processor_unavailable"}
		case err != nil || !res.Final():
			s.reschedule(ctx, d.id, d.count, err, res.Status)
			continue
		}
		if _, err := s.ApplyResult(ctx, d.id, res); err != nil {
			slog.Error("reconciler apply", "payment_attempt_id", d.id, "err", err)
		}
	}
	return len(batch), nil
}

func (s *Service) reschedule(ctx context.Context, id uuid.UUID, checks int, cause error, pspStatus string) {
	wait := s.ReconcileAfter
	for i := 1; i < checks && wait < s.MaxReconcileBackoff; i++ {
		wait *= 2
	}
	if wait > s.MaxReconcileBackoff {
		wait = s.MaxReconcileBackoff
	}
	if _, err := s.DB.Exec(ctx,
		`UPDATE payment_attempts SET next_check_at = now() + ($2::bigint || ' milliseconds')::interval WHERE id = $1 AND status = 'pending'`,
		id, wait.Milliseconds()); err != nil {
		slog.Error("reconciler reschedule", "payment_attempt_id", id, "err", err)
		return
	}
	slog.Warn("payment attempt still unresolved", "payment_attempt_id", id, "checks", checks,
		"psp_status", pspStatus, "err", cause, "next_check_in", wait.String())
}
