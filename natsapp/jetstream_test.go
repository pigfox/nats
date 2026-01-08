package natsapp

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestJetStream_Stream_Publish_Consume_Ack_AndData(t *testing.T) {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = "nats://127.0.0.1:4222"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	c, err := Connect(ctx, ConnectOptions{
		URL:            url,
		Name:           "js-test",
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

	stream := "S_TEST"
	subject := "events.test"
	durable := "C_TEST"

	if err := js.EnsureStream(ctx, StreamConfig{
		Name:     stream,
		Subjects: []string{subject},
		Storage:  "memory",
		MaxAge:   10 * time.Minute,
	}); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}

	// Call twice to cover "already exists" branch inside EnsureDurablePullConsumer
	for i := 0; i < 2; i++ {
		if err := js.EnsureDurablePullConsumer(ctx, stream, ConsumerConfig{
			Durable:       durable,
			FilterSubject: subject,
			AckPolicy:     "explicit",
			MaxAckPending: 256,
			MaxDeliver:    3,
			AckWait:       5 * time.Second,
			DeliverPolicy: "all",
			ReplayPolicy:  "instant",
		}); err != nil {
			t.Fatalf("EnsureDurablePullConsumer: %v", err)
		}
	}

	for i := 0; i < 5; i++ {
		if err := js.Publish(ctx, subject, []byte{byte('0' + i)}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	msgs, err := js.Fetch(ctx, stream, durable, subject, 5, 2*time.Second)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(msgs) == 0 {
		t.Fatalf("expected messages")
	}

	for _, m := range msgs {
		_ = m.Data()
		if err := m.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
}

func TestValidateConsumerConfig_AllBranches(t *testing.T) {
	if err := validateConsumerConfig(ConsumerConfig{}); err == nil {
		t.Fatalf("expected error (missing durable)")
	}
	if err := validateConsumerConfig(ConsumerConfig{Durable: "D", AckPolicy: "none"}); err == nil {
		t.Fatalf("expected error (bad ack policy)")
	}
	if err := validateConsumerConfig(ConsumerConfig{Durable: "D", DeliverPolicy: "new"}); err == nil {
		t.Fatalf("expected error (bad deliver policy)")
	}
	if err := validateConsumerConfig(ConsumerConfig{Durable: "D", ReplayPolicy: "original"}); err == nil {
		t.Fatalf("expected error (bad replay policy)")
	}
	if err := validateConsumerConfig(ConsumerConfig{
		Durable:       "D",
		AckPolicy:     "explicit",
		DeliverPolicy: "all",
		ReplayPolicy:  "instant",
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestJetStream_Fetch_RequiresFilterSubject(t *testing.T) {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = "nats://127.0.0.1:4222"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := Connect(ctx, ConnectOptions{
		URL:            url,
		Name:           "js-fetch-filter-required",
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

	if _, err := js.Fetch(ctx, "S_ANY", "C_ANY", "", 0, 0); err == nil {
		t.Fatalf("expected error")
	}
}

func TestJetStream_Fetch_DefaultBatchAndWait_NoMessages(t *testing.T) {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = "nats://127.0.0.1:4222"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	c, err := Connect(ctx, ConnectOptions{
		URL:            url,
		Name:           "js-fetch-defaults",
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

	stream := "S_DEFAULTS"
	subject := "events.defaults"
	durable := "C_DEFAULTS"

	if err := js.EnsureStream(ctx, StreamConfig{
		Name:     stream,
		Subjects: []string{subject},
		Storage:  "memory",
		MaxAge:   5 * time.Minute,
	}); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}

	if err := js.EnsureDurablePullConsumer(ctx, stream, ConsumerConfig{
		Durable:       durable,
		FilterSubject: subject,
		AckPolicy:     "explicit",
		MaxAckPending: 256,
		MaxDeliver:    3,
		AckWait:       2 * time.Second,
		DeliverPolicy: "all",
		ReplayPolicy:  "instant",
	}); err != nil {
		t.Fatalf("EnsureDurablePullConsumer: %v", err)
	}

	// batch<=0 and maxWait<=0 hit defaulting branches; no messages published => expect 0
	msgs, err := js.Fetch(ctx, stream, durable, subject, 0, 0)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected 0 msgs, got %d", len(msgs))
	}
}
