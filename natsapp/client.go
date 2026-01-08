// natsapp/client.go
package natsapp

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
)

var ErrClosed = errors.New("nats client closed")

type Msg struct {
	Subject string
	Data    []byte
	Reply   string
	Header  map[string][]string
}

type Subscription interface {
	Unsubscribe() error
	Drain() error
}

type Client interface {
	Publish(subject string, data []byte) error
	PublishMsg(m Msg) error
	Request(subject string, data []byte, timeout time.Duration) (Msg, error)
	Subscribe(subject string, handler func(Msg)) (Subscription, error)
	QueueSubscribe(subject, queue string, handler func(Msg)) (Subscription, error)
	FlushTimeout(d time.Duration) error
	Close()
	IsClosed() bool
	JetStream() (JetStream, error)
}

type natsSub struct{ s *nats.Subscription }

func (ns natsSub) Unsubscribe() error { return ns.s.Unsubscribe() }
func (ns natsSub) Drain() error       { return ns.s.Drain() }

type NATSClient struct {
	nc *nats.Conn
}

type ConnectOptions struct {
	URL            string
	Name           string
	Timeout        time.Duration
	PingInterval   time.Duration
	MaxReconnects  int
	ReconnectWait  time.Duration
	DrainTimeout   time.Duration
	AllowReconnect bool
}

func Connect(ctx context.Context, opt ConnectOptions) (*NATSClient, error) {
	if opt.Timeout <= 0 {
		opt.Timeout = 3 * time.Second
	}
	if opt.PingInterval <= 0 {
		opt.PingInterval = 20 * time.Second
	}
	if opt.ReconnectWait <= 0 {
		opt.ReconnectWait = 250 * time.Millisecond
	}
	if opt.MaxReconnects == 0 && opt.AllowReconnect {
		opt.MaxReconnects = 60
	}

	nopts := []nats.Option{
		nats.Name(opt.Name),
		nats.Timeout(opt.Timeout),
		nats.PingInterval(opt.PingInterval),
		nats.ReconnectWait(opt.ReconnectWait),
	}
	if opt.AllowReconnect {
		nopts = append(nopts, nats.MaxReconnects(opt.MaxReconnects))
	} else {
		nopts = append(nopts, nats.NoReconnect())
	}

	type result struct {
		nc  *nats.Conn
		err error
	}
	ch := make(chan result, 1)
	go func() {
		nc, err := nats.Connect(opt.URL, nopts...)
		ch <- result{nc: nc, err: err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ch:
		if r.err != nil {
			return nil, r.err
		}
		return &NATSClient{nc: r.nc}, nil
	}
}

func (c *NATSClient) IsClosed() bool {
	if c.nc == nil {
		return true
	}
	return c.nc.IsClosed()
}

func (c *NATSClient) Close() {
	if c.nc == nil || c.nc.IsClosed() {
		return
	}
	_ = c.nc.Drain()
	c.nc.Close()
}

func (c *NATSClient) Publish(subject string, data []byte) error {
	if c.IsClosed() {
		return ErrClosed
	}
	return c.nc.Publish(subject, data)
}

func (c *NATSClient) PublishMsg(m Msg) error {
	if c.IsClosed() {
		return ErrClosed
	}
	nm := &nats.Msg{Subject: m.Subject, Data: m.Data, Reply: m.Reply, Header: nats.Header(m.Header)}
	return c.nc.PublishMsg(nm)
}

func (c *NATSClient) Request(subject string, data []byte, timeout time.Duration) (Msg, error) {
	if c.IsClosed() {
		return Msg{}, ErrClosed
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	r, err := c.nc.Request(subject, data, timeout)
	if err != nil {
		return Msg{}, err
	}
	return Msg{
		Subject: r.Subject,
		Data:    r.Data,
		Reply:   r.Reply,
		Header:  map[string][]string(r.Header),
	}, nil
}

func (c *NATSClient) Subscribe(subject string, handler func(Msg)) (Subscription, error) {
	if c.IsClosed() {
		return nil, ErrClosed
	}
	s, err := c.nc.Subscribe(subject, func(m *nats.Msg) {
		handler(Msg{Subject: m.Subject, Data: m.Data, Reply: m.Reply, Header: map[string][]string(m.Header)})
	})
	if err != nil {
		return nil, err
	}
	return natsSub{s: s}, nil
}

func (c *NATSClient) QueueSubscribe(subject, queue string, handler func(Msg)) (Subscription, error) {
	if c.IsClosed() {
		return nil, ErrClosed
	}
	s, err := c.nc.QueueSubscribe(subject, queue, func(m *nats.Msg) {
		handler(Msg{Subject: m.Subject, Data: m.Data, Reply: m.Reply, Header: map[string][]string(m.Header)})
	})
	if err != nil {
		return nil, err
	}
	return natsSub{s: s}, nil
}

func (c *NATSClient) FlushTimeout(d time.Duration) error {
	if c.IsClosed() {
		return ErrClosed
	}
	if d <= 0 {
		d = time.Second
	}
	return c.nc.FlushTimeout(d)
}

func (c *NATSClient) JetStream() (JetStream, error) {
	if c.IsClosed() {
		return nil, ErrClosed
	}
	js, err := c.nc.JetStream()
	if err != nil {
		return nil, err
	}
	return &natsJS{js: js}, nil
}
