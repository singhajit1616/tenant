package broker

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"controlplane/internal/domain"
	"github.com/google/uuid"
)

// testBus opens a RabbitMQ bus with a bounded test context and cleanup, skipping or
// failing when required broker configuration is missing.
func testBus(t *testing.T) (*Bus, context.Context) {
	t.Helper()
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		if os.Getenv("REQUIRE_INTEGRATION") == "1" {
			t.Fatal("RABBITMQ_URL required")
		}
		t.Skip("run Compose quality gate for RabbitMQ integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	bus, err := Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bus.Close)
	return bus, ctx
}

// TestPublishUnroutableFailsDespiteConfirmation verifies that a mandatory return is
// treated as publication failure without closing the connection.
func TestPublishUnroutableFailsDespiteConfirmation(t *testing.T) {
	bus, ctx := testBus(t)
	if err := bus.Publish(ctx, "missing-"+uuid.NewString(), "unroutable", []byte(`{}`)); err == nil {
		t.Fatal("an unroutable publication must never mark an outbox row published")
	}
	if !bus.Ready() {
		t.Fatal("mandatory return should not close connection")
	}
}

// TestDuplicateMessageIDsAndTransientHandlerFailure verifies that RabbitMQ delivers
// duplicate IDs and that a transient handler error is retried before acknowledgement.
func TestDuplicateMessageIDsAndTransientHandlerFailure(t *testing.T) {
	bus, ctx := testBus(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	u := domain.WorkerUpdate{UpdateID: uuid.NewString(), TaskID: uuid.NewString(), Status: domain.TaskDone}
	for i := 0; i < 2; i++ {
		if err := bus.PublishProgress(ctx, u); err != nil {
			t.Fatal(err)
		}
	}
	var attempts, handled atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- bus.ConsumeProgress(ctx, func(data []byte) error {
			if attempts.Add(1) == 1 {
				return errors.New("transient database failure")
			}
			if handled.Add(1) == 2 {
				cancel()
			}
			return nil
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("handler retry or duplicate delivery missing")
	}
	if attempts.Load() != 3 || handled.Load() != 2 {
		t.Fatalf("attempts=%d handled=%d", attempts.Load(), handled.Load())
	}
}

// TestUnacknowledgedDeliveryRequeuesOnConsumerExit verifies that unfinished work is
// delivered to a replacement consumer after the first exits.
func TestUnacknowledgedDeliveryRequeuesOnConsumerExit(t *testing.T) {
	bus, ctx := testBus(t)
	if err := bus.Publish(ctx, QueueOutbound, uuid.NewString(), []byte(`{"redelivery":true}`)); err != nil {
		t.Fatal(err)
	}
	first, cancelFirst := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- bus.ConsumeOutbound(first, func([]byte) error {
			cancelFirst()
			return errors.New("worker exited before finishing")
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("first consumer did not exit")
	}
	second, cancelSecond := context.WithCancel(ctx)
	defer cancelSecond()
	var received atomic.Bool
	go func() {
		done <- bus.ConsumeOutbound(second, func([]byte) error { received.Store(true); cancelSecond(); return nil })
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("unacknowledged task was lost")
	}
	if !received.Load() {
		t.Fatal("replacement consumer did not receive requeued task")
	}
}

// TestConnectCancellationAndBackoff checks cancelled startup and the retry-delay growth
// and cap without a broker.
func TestConnectCancellationAndBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Connect(ctx, "amqp://localhost:1/"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if nextBackoff(time.Second) != 2*time.Second || nextBackoff(16*time.Second) != 30*time.Second || nextBackoff(time.Minute) != 30*time.Second {
		t.Fatal("backoff must grow and remain capped")
	}
}
