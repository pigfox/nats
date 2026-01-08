package natsapp

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func natsURL() string {
	if v := os.Getenv("NATS_URL"); v != "" {
		return v
	}
	return "nats://127.0.0.1:4222"
}

func TestNATSClient_PubSub_PublishMsg_RequestReply_QueueGroup_Drain_FlushDefaults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	c, err := Connect(ctx, ConnectOptions{
		URL:            natsURL(),
		Name:           "client-test",
		AllowReconnect: true,
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	// -------------------------
	// Pub/Sub (Publish)
	// -------------------------
	got := make(chan Msg, 8)

	sub, err := c.Subscribe("t.pubsub", func(m Msg) { got <- m })
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	if err := c.FlushTimeout(2 * time.Second); err != nil {
		t.Fatalf("FlushTimeout after Subscribe: %v", err)
	}

	if err := c.Publish("t.pubsub", []byte("hello")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Cover FlushTimeout default branch by passing <=0
	if err := c.FlushTimeout(0); err != nil {
		t.Fatalf("FlushTimeout(0): %v", err)
	}

	select {
	case m := <-got:
		if string(m.Data) != "hello" {
			t.Fatalf("got=%q want=%q", string(m.Data), "hello")
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for pubsub message")
	}

	// -------------------------
	// Pub/Sub (PublishMsg) + headers
	// -------------------------
	if err := c.PublishMsg(Msg{
		Subject: "t.pubsub",
		Data:    []byte("hi"),
		Header:  map[string][]string{"X-Test": {"1"}},
	}); err != nil {
		t.Fatalf("PublishMsg: %v", err)
	}

	if err := c.FlushTimeout(2 * time.Second); err != nil {
		t.Fatalf("FlushTimeout after PublishMsg: %v", err)
	}

	select {
	case m := <-got:
		if string(m.Data) != "hi" {
			t.Fatalf("got=%q want=%q", string(m.Data), "hi")
		}
		if m.Header == nil || len(m.Header["X-Test"]) == 0 || m.Header["X-Test"][0] != "1" {
			t.Fatalf("expected header X-Test=1, got: %+v", m.Header)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for publishmsg message")
	}

	// -------------------------
	// Request/Reply (timeout<=0 covers default timeout branch)
	// -------------------------
	replySub, err := c.Subscribe("t.reply", func(m Msg) {
		_ = c.Publish(m.Reply, []byte("ok:"+string(m.Data)))
	})
	if err != nil {
		t.Fatalf("Subscribe reply: %v", err)
	}
	defer replySub.Unsubscribe()

	if err := c.FlushTimeout(2 * time.Second); err != nil {
		t.Fatalf("FlushTimeout after reply Subscribe: %v", err)
	}

	resp, err := c.Request("t.reply", []byte("ping"), 0)
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if string(resp.Data) != "ok:ping" {
		t.Fatalf("resp=%q want=%q", string(resp.Data), "ok:ping")
	}

	// -------------------------
	// Queue group
	// -------------------------
	var aCount int32
	var bCount int32

	qa, err := c.QueueSubscribe("t.q", "workers", func(Msg) { atomic.AddInt32(&aCount, 1) })
	if err != nil {
		t.Fatalf("QueueSubscribe A: %v", err)
	}
	defer qa.Unsubscribe()

	qb, err := c.QueueSubscribe("t.q", "workers", func(Msg) { atomic.AddInt32(&bCount, 1) })
	if err != nil {
		t.Fatalf("QueueSubscribe B: %v", err)
	}
	defer qb.Unsubscribe()

	if err := c.FlushTimeout(2 * time.Second); err != nil {
		t.Fatalf("FlushTimeout after QueueSubscribe: %v", err)
	}

	const N = 50
	for i := 0; i < N; i++ {
		if err := c.Publish("t.q", []byte("job")); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if err := c.FlushTimeout(3 * time.Second); err != nil {
		t.Fatalf("FlushTimeout after queue publishes: %v", err)
	}

	deadline := time.Now().Add(4 * time.Second)
	for {
		total := atomic.LoadInt32(&aCount) + atomic.LoadInt32(&bCount)
		if int(total) == N {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue total=%d want=%d (a=%d b=%d)", total, N, aCount, bCount)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// -------------------------
	// Subscription.Drain wrapper
	// -------------------------
	drainSub, err := c.Subscribe("t.drain", func(Msg) {})
	if err != nil {
		t.Fatalf("Subscribe drain: %v", err)
	}
	if err := c.FlushTimeout(2 * time.Second); err != nil {
		t.Fatalf("FlushTimeout after drain Subscribe: %v", err)
	}
	if err := drainSub.Drain(); err != nil {
		t.Fatalf("Drain: %v", err)
	}
}

func TestNATSClient_CloseAndIsClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := Connect(ctx, ConnectOptions{
		URL:            natsURL(),
		Name:           "close-test",
		AllowReconnect: false,
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}

	if c.IsClosed() {
		t.Fatalf("expected open")
	}

	c.Close()

	if !c.IsClosed() {
		t.Fatalf("expected closed")
	}

	// These should all error when closed; covers more Close-path fallout.
	if err := c.Publish("x", []byte("y")); err == nil {
		t.Fatalf("expected error")
	}
	if err := c.PublishMsg(Msg{Subject: "x", Data: []byte("y")}); err == nil {
		t.Fatalf("expected error")
	}
	if _, err := c.Subscribe("x", func(Msg) {}); err == nil {
		t.Fatalf("expected error")
	}
	if _, err := c.QueueSubscribe("x", "q", func(Msg) {}); err == nil {
		t.Fatalf("expected error")
	}
	if _, err := c.Request("x", []byte("y"), time.Second); err == nil {
		t.Fatalf("expected error")
	}
	if err := c.FlushTimeout(time.Second); err == nil {
		t.Fatalf("expected error")
	}
	if _, err := c.JetStream(); err == nil {
		t.Fatalf("expected error")
	}
}

func TestNATSClient_ClosedErrors_NilClient(t *testing.T) {
	c := &NATSClient{nc: nil}

	if err := c.Publish("x", []byte("y")); err == nil {
		t.Fatalf("expected error")
	}
	if err := c.PublishMsg(Msg{Subject: "x", Data: []byte("y")}); err == nil {
		t.Fatalf("expected error")
	}
	if _, err := c.Subscribe("x", func(Msg) {}); err == nil {
		t.Fatalf("expected error")
	}
	if _, err := c.QueueSubscribe("x", "q", func(Msg) {}); err == nil {
		t.Fatalf("expected error")
	}
	if _, err := c.Request("x", []byte("y"), time.Second); err == nil {
		t.Fatalf("expected error")
	}
	if err := c.FlushTimeout(time.Second); err == nil {
		t.Fatalf("expected error")
	}
	if _, err := c.JetStream(); err == nil {
		t.Fatalf("expected error")
	}
}
