package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"controlplane/internal/api"
	"controlplane/internal/broker"
	"controlplane/internal/config"
	"controlplane/internal/consumer"
	"controlplane/internal/outbox"
	"controlplane/internal/store"

	"github.com/jackc/pgx/v5/pgxpool"
)

// main creates the JSON logger, starts the control plane, and exits with an error code if
// startup or serving fails.
func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)
	if err := run(log); err != nil {
		log.Error("control plane exited", "err", err)
		os.Exit(1)
	}
}

// run loads configuration, prepares PostgreSQL and RabbitMQ, starts the relay and progress
// consumer, and serves HTTP until shutdown or a server error.
func run(log *slog.Logger) error {
	cfg, err := config.LoadControlPlane()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	st := store.New(pool)
	if err := store.Migrate(ctx, st); err != nil {
		return err
	}

	bus, err := broker.Connect(ctx, cfg.RabbitMQURL)
	if err != nil {
		return err
	}
	defer bus.Close()

	relay := outbox.New(st, bus, log)
	var wg sync.WaitGroup
	wg.Add(2)
	defer func() { stop(); wg.Wait() }()
	go func() { defer wg.Done(); relay.Run(ctx) }()

	cons := consumer.New(st, log)
	go func() {
		defer wg.Done()
		_ = bus.ConsumeProgress(ctx, cons.Handle)
	}()

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.New(st).WithBrokerReadiness(bus.Ready).Router(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.HTTPAddr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdown)
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}
