package main

import (
	"context"
	"encoding/json"
	"flag"
	"hash/fnv"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"controlplane/internal/broker"
	"controlplane/internal/config"
	"controlplane/internal/domain"
)

// main parses worker flags, applies environment overrides, and runs the simulator with
// JSON logging.
func main() {
	failRate := flag.Float64("fail-rate", -1, "probability of failing a task (0-1)")
	minDelay := flag.Int("min-delay-ms", -1, "minimum simulated work delay in milliseconds")
	maxDelay := flag.Int("max-delay-ms", -1, "maximum simulated work delay in milliseconds")
	flag.Parse()

	if *failRate >= 0 {
		_ = os.Setenv("FAIL_RATE", strconv.FormatFloat(*failRate, 'f', -1, 64))
	}
	if *minDelay >= 0 {
		_ = os.Setenv("MIN_DELAY_MS", strconv.Itoa(*minDelay))
	}
	if *maxDelay >= 0 {
		_ = os.Setenv("MAX_DELAY_MS", strconv.Itoa(*maxDelay))
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)
	if err := run(log); err != nil {
		log.Error("worker exited", "err", err)
		os.Exit(1)
	}
}

// run consumes outbound tasks, simulates delays and outcomes, publishes stable progress
// updates, and reconnects after consumer failures until shutdown.
func run(log *slog.Logger) error {
	cfg, err := config.LoadWorker()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	bus, err := broker.Connect(ctx, cfg.RabbitMQURL)
	if err != nil {
		return err
	}
	defer bus.Close()

	log.Info("worker started", "fail_rate", cfg.FailRate, "min_delay", cfg.MinDelay, "max_delay", cfg.MaxDelay)

	return bus.ConsumeOutbound(ctx, func(data []byte) error {
		return handleTask(ctx, bus, cfg, data, log)
	})
}

// handleTask validates one task, simulates its two progress steps, and publishes
// stable update IDs. Failure sampling stays consistent across task redelivery.
func handleTask(ctx context.Context, bus *broker.Bus, cfg config.Worker, data []byte, log *slog.Logger) error {
	var task domain.TaskEvent
	if err := json.Unmarshal(data, &task); err != nil {
		log.Error("poison outbound task, skipping")
		return nil
	}
	if task.ID == "" || task.TenantID == "" || task.Status != domain.TaskAccepted ||
		(task.Type != domain.TaskDeploy && task.Type != domain.TaskUpdate && task.Type != domain.TaskDestroy) {
		log.Error("invalid outbound task, skipping")
		return nil
	}

	log.Info("accepted task", "task_id", task.ID, "type", task.Type, "tenant_id", task.TenantID)
	final := domain.TaskDone
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(task.ID))
	if float64(hash.Sum32())/(1<<32) < cfg.FailRate {
		final = domain.TaskFailed
	}

	for _, status := range []domain.TaskStatus{domain.TaskInProgress, final} {
		if err := sleepJitter(ctx, cfg.MinDelay, cfg.MaxDelay); err != nil {
			return err
		}
		stage := "in_progress"
		if status.Terminal() {
			stage = "terminal"
		}
		update := domain.WorkerUpdate{UpdateID: task.ID + ":" + stage, TaskID: task.ID, Status: status}
		if err := publishWithRetry(ctx, bus, update, log); err != nil {
			return err
		}
	}
	log.Info("finished task", "task_id", task.ID, "status", final)
	return nil
}

// publishWithRetry publishes the same progress update until confirmed or cancelled, using
// bounded attempts and increasing retry delays.
func publishWithRetry(ctx context.Context, bus *broker.Bus, u domain.WorkerUpdate, log *slog.Logger) error {
	backoff := time.Second
	for ctx.Err() == nil {
		err := bus.PublishProgress(ctx, u)
		if err == nil {
			return nil
		}
		log.Warn("publish progress failed, retrying", "err", err, "update_id", u.UpdateID)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 16*time.Second)
	}
	return ctx.Err()
}

// sleepJitter waits for a random duration between the configured limits, returning early
// if the context is cancelled.
func sleepJitter(ctx context.Context, minDelay, maxDelay time.Duration) error {
	delay := minDelay
	if maxDelay > minDelay {
		delay += time.Duration(rand.Int64N(int64(maxDelay-minDelay) + 1))
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
