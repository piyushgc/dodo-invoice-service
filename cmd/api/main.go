// Command api runs the invoice & payment service: HTTP API, webhook delivery worker and
// payment reconciler in one process.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"invoicesvc/internal/app"
	"invoicesvc/internal/auth"
	"invoicesvc/internal/db"
	"invoicesvc/internal/webhook"
	"invoicesvc/migrations"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, env("DATABASE_URL", "postgres://invoices:invoices@localhost:5432/invoices?sslmode=disable"))
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := db.Migrate(ctx, pool, migrations.FS); err != nil {
		return err
	}

	if key := os.Getenv("DEMO_API_KEY"); key != "" {
		businessID, err := auth.SeedDemoBusiness(ctx, pool, key)
		if err != nil {
			return err
		}
		slog.Info("demo business ready", "business_id", businessID, "api_key_prefix", key[:11])
		if url, secret := os.Getenv("DEMO_WEBHOOK_URL"), os.Getenv("DEMO_WEBHOOK_SECRET"); url != "" && secret != "" {
			if err := webhook.SeedEndpoint(ctx, pool, businessID, url, secret); err != nil {
				return err
			}
			slog.Info("demo webhook endpoint ready", "url", url)
		}
	}

	a := app.New(pool, app.Config{
		PSPBaseURL:     env("PSP_BASE_URL", "http://localhost:8081"),
		PSPTimeout:     duration("PSP_TIMEOUT", 5*time.Second),
		ReconcileAfter: duration("RECONCILE_AFTER", 10*time.Second),
	})

	go a.Webhooks.Run(ctx)
	go a.Payments.RunReconciler(ctx, 2*time.Second)

	srv := &http.Server{
		Addr:              env("LISTEN_ADDR", ":8080"),
		Handler:           a.Handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		slog.Info("api listening", "addr", srv.Addr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		slog.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
	return nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func duration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		slog.Warn("invalid duration, using default", "key", key, "value", v)
	}
	return def
}
