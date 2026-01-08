package natsapp

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestJetStream_Redelivery_WhenNotAcked(t *testing.T) {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = "nats://127.0.0.1:4222"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	c, err := Connect(ctx, ConnectOptions{
		URL:            url,
		Name:           "js-redelivery-test",
		AllowReconnect: true,
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	js, err := c.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	stream := "S_REDELIVER"
	subject := "events.redeliver"
	durable := "C_REDELIVER"

	if err := js.EnsureStream(ctx, StreamConfig{
		Name:     stream,
		Subjects: []string{subject},
		Storage:  "memory",
		MaxAge:   5 * time.Minute,
	}); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}

	// short ack wait so redelivery happens quickly
	if err := js.EnsureDurablePullConsumer(ctx, stream, ConsumerConfig{
		Durable:       durable,
		FilterSubject: subject,
		AckPolicy:     "explicit",
		MaxAckPending: 256,
		MaxDeliver:    5,
		AckWait:       300 * time.Millisecond,
		DeliverPolicy: "all",
		ReplayPolicy:  "instant",
	}); err != nil {
		t.Fatalf("EnsureDurablePullConsumer: %v", err)
	}

	if err := js.Publish(ctx, subject, []byte("x")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// First fetch: do NOT ack
	msgs1, err := js.Fetch(ctx, stream, durable, subject, 1, 2*time.Second)
	if err != nil {
		t.Fatalf("Fetch1: %v", err)
	}
	if len(msgs1) != 1 {
		t.Fatalf("expected 1 msg, got %d", len(msgs1))
	}

	// Wait past AckWait so it becomes eligible for redelivery
	time.Sleep(500 * time.Millisecond)

	// Second fetch: should receive again, then ack
	msgs2, err := js.Fetch(ctx, stream, durable, subject, 1, 2*time.Second)
	if err != nil {
		t.Fatalf("Fetch2: %v", err)
	}
	if len(msgs2) != 1 {
		t.Fatalf("expected 1 redelivered msg, got %d", len(msgs2))
	}
	if err := msgs2[0].Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// After ack, should not receive again (best-effort check)
	msgs3, err := js.Fetch(ctx, stream, durable, subject, 1, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("Fetch3: %v", err)
	}
	if len(msgs3) != 0 {
		t.Fatalf("expected 0 msgs after ack, got %d", len(msgs3))
	}
}
