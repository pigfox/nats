package natsapp

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
)

var (
	ErrNoStream = errors.New("stream does not exist")
)

type JetStream interface {
	EnsureStream(ctx context.Context, cfg StreamConfig) error
	Publish(ctx context.Context, subject string, data []byte) error
	EnsureDurablePullConsumer(ctx context.Context, stream string, cfg ConsumerConfig) error
	Fetch(ctx context.Context, stream, durable, filterSubject string, batch int, maxWait time.Duration) ([]JetStreamMsg, error)
}

type StreamConfig struct {
	Name     string
	Subjects []string
	Storage  string // "file" or "memory"
	Replicas int
	MaxAge   time.Duration
}

type ConsumerConfig struct {
	Durable       string
	FilterSubject string
	AckPolicy     string // "explicit"
	MaxAckPending int
	MaxDeliver    int
	AckWait       time.Duration
	DeliverPolicy string // "all"
	ReplayPolicy  string // "instant"
}

type JetStreamMsg interface {
	Data() []byte
	Ack() error
}

type natsJS struct {
	js nats.JetStreamContext
}

type natsJSM struct {
	m *nats.Msg
}

func (m natsJSM) Data() []byte { return m.m.Data }
func (m natsJSM) Ack() error   { return m.m.Ack() }

func validateConsumerConfig(cfg ConsumerConfig) error {
	if cfg.Durable == "" {
		return errors.New("durable required")
	}
	if cfg.AckPolicy != "" && cfg.AckPolicy != "explicit" {
		return errors.New("only explicit ack supported")
	}
	if cfg.DeliverPolicy != "" && cfg.DeliverPolicy != "all" {
		return errors.New("only deliver all supported")
	}
	if cfg.ReplayPolicy != "" && cfg.ReplayPolicy != "instant" {
		return errors.New("only instant replay supported")
	}
	return nil
}

func (j *natsJS) EnsureStream(ctx context.Context, cfg StreamConfig) error {
	if cfg.Replicas <= 0 {
		cfg.Replicas = 1
	}

	storage := nats.FileStorage
	if cfg.Storage == "memory" {
		storage = nats.MemoryStorage
	}

	if _, err := j.js.StreamInfo(cfg.Name); err == nil {
		return nil
	}

	_, err := j.js.AddStream(&nats.StreamConfig{
		Name:     cfg.Name,
		Subjects: cfg.Subjects,
		Storage:  storage,
		Replicas: cfg.Replicas,
		MaxAge:   cfg.MaxAge,
	})
	return err
}

func (j *natsJS) Publish(ctx context.Context, subject string, data []byte) error {
	_, err := j.js.Publish(subject, data)
	return err
}

func (j *natsJS) EnsureDurablePullConsumer(ctx context.Context, stream string, cfg ConsumerConfig) error {
	if err := validateConsumerConfig(cfg); err != nil {
		return err
	}

	if _, err := j.js.ConsumerInfo(stream, cfg.Durable); err == nil {
		return nil
	}

	_, err := j.js.AddConsumer(stream, &nats.ConsumerConfig{
		Durable:       cfg.Durable,
		FilterSubject: cfg.FilterSubject,
		AckPolicy:     nats.AckExplicitPolicy,
		MaxAckPending: cfg.MaxAckPending,
		MaxDeliver:    cfg.MaxDeliver,
		AckWait:       cfg.AckWait,
		DeliverPolicy: nats.DeliverAllPolicy,
		ReplayPolicy:  nats.ReplayInstantPolicy,
	})
	return err
}

func (j *natsJS) Fetch(
	ctx context.Context,
	stream string,
	durable string,
	filterSubject string,
	batch int,
	maxWait time.Duration,
) ([]JetStreamMsg, error) {

	if filterSubject == "" {
		return nil, errors.New("filterSubject required")
	}
	if batch <= 0 {
		batch = 1
	}
	if maxWait <= 0 {
		maxWait = time.Second
	}

	sub, err := j.js.PullSubscribe(
		filterSubject,
		durable,
		nats.Bind(stream, durable),
	)
	if err != nil {
		return nil, err
	}

	msgs, err := sub.Fetch(batch, nats.MaxWait(maxWait))
	if err != nil && !errors.Is(err, nats.ErrTimeout) {
		return nil, err
	}

	out := make([]JetStreamMsg, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, natsJSM{m: m})
	}
	return out, nil
}
