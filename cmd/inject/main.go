package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"controlplane/internal/broker"
	"controlplane/internal/domain"
)

// main validates CLI arguments, connects to RabbitMQ, publishes one progress update for a
// specified task, and exits.
func main() {
	taskID := flag.String("task-id", "", "task UUID to update")
	status := flag.String("status", "done", "in_progress | done | failed")
	updateID := flag.String("update-id", "", "idempotency key; generated if empty")
	rabbitMQURL := flag.String("rabbitmq-url", getenv("RABBITMQ_URL", "amqp://tenant:tenant@127.0.0.1:5672/"), "RabbitMQ AMQP URL")
	flag.Parse()

	if *taskID == "" {
		fmt.Fprintln(os.Stderr, "--task-id is required")
		os.Exit(1)
	}
	if *updateID == "" {
		*updateID = fmt.Sprintf("inject-%d", time.Now().UnixNano())
	}

	u := domain.WorkerUpdate{
		UpdateID: *updateID,
		TaskID:   *taskID,
		Status:   domain.TaskStatus(*status),
	}
	if err := u.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bus, err := broker.Connect(ctx, *rabbitMQURL)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer bus.Close()

	if err := bus.PublishProgress(ctx, u); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("published update_id=%s task_id=%s status=%s\n", u.UpdateID, u.TaskID, u.Status)
}

// getenv returns a non-empty environment value or the supplied fallback.
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
