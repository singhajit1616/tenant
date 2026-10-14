package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"controlplane/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	QueueOutbound = "tasks.outbound"
	QueueProgress = "tasks.progress"
)

// Bus reconnects lazily after connection failures. Each operation has its own
// channel, so publisher confirmations cannot be confused between publishers.
type Bus struct {
	url      string
	mu       sync.Mutex
	conn     *amqp.Connection
	socket   net.Conn
	closed   bool
	dialGate chan struct{}
}

// Connect prepares a RabbitMQ connection and durable queues, retrying startup failures
// with backoff for up to 30 seconds or until cancellation.
func Connect(ctx context.Context, url string) (*Bus, error) {
	b := &Bus{url: url, dialGate: make(chan struct{}, 1)}
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	delay := 100 * time.Millisecond
	for {
		attempt, stop := context.WithTimeout(startup, 5*time.Second)
		ch, finish, err := b.channel(attempt)
		if err == nil {
			err = declareQueues(ch)
			_ = ch.Close()
			finish()
		}
		stop()
		if err == nil {
			return b, nil
		}
		if err := wait(startup, delay); err != nil {
			b.Close()
			return nil, fmt.Errorf("RabbitMQ startup: %w", err)
		}
		delay = nextBackoff(delay)
	}
}

// connection reuses an open AMQP connection or serializes a new dial, enforcing connection
// deadlines and rejecting use after the bus is closed.
func (b *Bus) connection(ctx context.Context) (*amqp.Connection, net.Conn, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case b.dialGate <- struct{}{}:
	}
	defer func() { <-b.dialGate }()
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, nil, errors.New("broker closed")
	}
	if b.conn != nil && !b.conn.IsClosed() {
		conn, socket := b.conn, b.socket
		b.mu.Unlock()
		return conn, socket, nil
	}
	b.mu.Unlock()
	var socket net.Conn
	conn, err := amqp.DialConfig(b.url, amqp.Config{
		Heartbeat: 5 * time.Second,
		Dial: func(network, address string) (net.Conn, error) {
			var err error
			socket, err = (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			deadline := time.Now().Add(5 * time.Second)
			if limit, ok := ctx.Deadline(); ok && limit.Before(deadline) {
				deadline = limit
			}
			if err := socket.SetDeadline(deadline); err != nil {
				_ = socket.Close()
				return nil, err
			}
			return socket, nil
		},
	})
	if err != nil {
		if socket != nil {
			_ = socket.Close()
		}
		return nil, nil, fmt.Errorf("RabbitMQ connect: %w", err)
	}
	if err := socket.SetDeadline(time.Time{}); err != nil {
		_ = socket.Close()
		return nil, nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		_ = socket.Close()
		return nil, nil, errors.New("broker closed")
	}
	b.conn, b.socket = conn, socket
	return conn, socket, nil
}

// Closing the transport on cancellation also interrupts AMQP RPCs, whose
// library APIs do not all accept contexts. Any uncertain publication is retried
// by the outbox or worker; duplicate application is prevented by the database.
func (b *Bus) channel(ctx context.Context) (*amqp.Channel, func(), error) {
	conn, socket, err := b.connection(ctx)
	if err != nil {
		return nil, nil, err
	}
	finished := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		select {
		case <-ctx.Done():
			_ = socket.Close()
		case <-finished:
		}
	}()
	finish := func() { close(finished); <-stopped }
	ch, err := conn.Channel()
	if err != nil {
		finish()
		return nil, nil, err
	}
	return ch, finish, nil
}

// declareQueues creates the durable classic outbound and progress queues if they do not
// already exist with matching settings.
func declareQueues(ch *amqp.Channel) error {
	for _, queue := range []string{QueueOutbound, QueueProgress} {
		if _, err := ch.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-queue-type": "classic"}); err != nil {
			return err
		}
	}
	return nil
}

// Close marks the bus closed and closes its transport, ending active AMQP operations and
// preventing reconnection.
func (b *Bus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	if b.socket != nil {
		_ = b.socket.Close()
	}
}

// Ready reports whether the bus has an open connection and has not been explicitly closed.
func (b *Bus) Ready() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.closed && b.conn != nil && !b.conn.IsClosed()
}

// PublishOutbound extracts the task ID from its JSON event and publishes the event to the
// outbound queue using that ID as message metadata.
func (b *Bus) PublishOutbound(ctx context.Context, payload []byte) error {
	var event domain.TaskEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return err
	}
	return b.Publish(ctx, QueueOutbound, event.ID, payload)
}

// PublishProgress encodes a worker update as JSON and publishes it to the progress queue
// using its update ID as message metadata.
func (b *Bus) PublishProgress(ctx context.Context, update domain.WorkerUpdate) error {
	payload, err := json.Marshal(update)
	if err != nil {
		return err
	}
	return b.Publish(ctx, QueueProgress, update.UpdateID, payload)
}

// Publish returns success only after a positive publisher confirmation and no
// mandatory return. RabbitMQ MessageId is metadata, not broker deduplication.
func (b *Bus) Publish(ctx context.Context, queue, messageID string, payload []byte) error {
	attempt, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ch, finish, err := b.channel(attempt)
	if err != nil {
		return err
	}
	defer finish()
	defer ch.Close()
	if err := declareQueues(ch); err != nil {
		return err
	}
	if err := ch.Confirm(false); err != nil {
		return err
	}
	returned := ch.NotifyReturn(make(chan amqp.Return, 1))
	confirmation, err := ch.PublishWithDeferredConfirmWithContext(attempt, "", queue, true, false, amqp.Publishing{
		ContentType: "application/json", DeliveryMode: amqp.Persistent,
		MessageId: messageID, Timestamp: time.Now().UTC(), Body: payload,
	})
	if err != nil {
		return err
	}
	acked, err := confirmation.WaitContext(attempt)
	if err != nil {
		return err
	}
	if !acked {
		return errors.New("RabbitMQ did not confirm publication")
	}
	select {
	case message := <-returned:
		return fmt.Errorf("RabbitMQ returned publication: %s", message.ReplyText)
	default:
		return nil
	}
}

// ConsumeOutbound processes outbound task deliveries with the supplied handler and manual
// acknowledgements.
func (b *Bus) ConsumeOutbound(ctx context.Context, handler func([]byte) error) error {
	return b.consume(ctx, QueueOutbound, handler)
}

// ConsumeProgress processes worker progress deliveries with the supplied handler and
// manual acknowledgements.
func (b *Bus) ConsumeProgress(ctx context.Context, handler func([]byte) error) error {
	return b.consume(ctx, QueueProgress, handler)
}

// consume restarts a failed consumer with backoff until cancellation. Both the API
// and worker use this loop instead of implementing their own reconnection loops.
func (b *Bus) consume(ctx context.Context, queue string, handler func([]byte) error) error {
	delay := time.Second
	for ctx.Err() == nil {
		err := b.consumeSession(ctx, queue, handler)
		if ctx.Err() != nil {
			return nil
		}
		slog.Warn("consumer stopped, retrying", "queue", queue, "err", err)
		if wait(ctx, delay) != nil {
			return nil
		}
		delay = nextBackoff(delay)
	}
	return nil
}

// subscribe declares queues and opens a consumer with prefetch one and manual
// acknowledgements. Its setup timeout ends before message processing starts.
func (b *Bus) subscribe(ctx context.Context, queue string) (*amqp.Channel, <-chan amqp.Delivery, <-chan *amqp.Error, error) {
	setup, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ch, finish, err := b.channel(setup)
	if err != nil {
		return nil, nil, nil, err
	}
	defer finish()
	if err := declareQueues(ch); err != nil {
		_ = ch.Close()
		return nil, nil, nil, err
	}
	if err := ch.Qos(1, 0, false); err != nil {
		_ = ch.Close()
		return nil, nil, nil, err
	}
	closed := ch.NotifyClose(make(chan *amqp.Error, 1))
	messages, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return nil, nil, nil, err
	}
	return ch, messages, closed, nil
}

// consumeSession retries transient handler failures while a delivery remains
// unacknowledged, then acknowledges success. Connection loss requeues unfinished work.
func (b *Bus) consumeSession(ctx context.Context, queue string, handler func([]byte) error) error {
	ch, messages, closed, err := b.subscribe(ctx, queue)
	if err != nil {
		return err
	}
	defer ch.Close()
	for {
		select {
		case <-ctx.Done():
			return nil
		case message, ok := <-messages:
			if !ok {
				return errors.New("RabbitMQ delivery channel closed")
			}
			delay := time.Second
			for {
				if err := handler(message.Body); err == nil {
					break
				}
				// Keep the delivery unacknowledged while retrying with backoff.
				// Connection loss or process exit requeues it automatically.
				select {
				case <-ctx.Done():
					return nil
				case <-closed:
					return errors.New("RabbitMQ consumer channel closed")
				case <-time.After(delay):
					delay = nextBackoff(delay)
				}
			}
			if err := message.Ack(false); err != nil {
				return err
			}
		}
	}
}

// nextBackoff doubles the retry delay and caps it at 30 seconds.
func nextBackoff(delay time.Duration) time.Duration {
	return min(delay*2, 30*time.Second)
}

// wait pauses for the requested duration or returns the context error when cancelled.
func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
