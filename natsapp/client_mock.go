// natsapp/client_mock.go
package natsapp

import (
	"errors"
	"sync"
	"time"
)

type mockSub struct {
	unsub func()
}

func (s mockSub) Unsubscribe() error { s.unsub(); return nil }
func (s mockSub) Drain() error       { s.unsub(); return nil }

type MockClient struct {
	mu        sync.RWMutex
	closed    bool
	subs      map[string][]func(Msg)
	qsubs     map[string]map[string][]func(Msg) // subject -> queue -> handlers
	publishes []Msg
}

func NewMockClient() *MockClient {
	return &MockClient{
		subs:  map[string][]func(Msg){},
		qsubs: map[string]map[string][]func(Msg){},
	}
}

func (m *MockClient) IsClosed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.closed
}

func (m *MockClient) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
}

func (m *MockClient) Publish(subject string, data []byte) error {
	return m.PublishMsg(Msg{Subject: subject, Data: data})
}

func (m *MockClient) PublishMsg(msg Msg) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	m.publishes = append(m.publishes, msg)

	handlers := append([]func(Msg){}, m.subs[msg.Subject]...)
	// queue groups: deliver to exactly 1 handler per queue (round-robin not implemented; pick first)
	qm := m.qsubs[msg.Subject]
	qhandlers := make([]func(Msg), 0)
	for _, hs := range qm {
		if len(hs) > 0 {
			qhandlers = append(qhandlers, hs[0])
		}
	}
	m.mu.Unlock()

	for _, h := range handlers {
		h(msg)
	}
	for _, h := range qhandlers {
		h(msg)
	}
	return nil
}

func (m *MockClient) Request(subject string, data []byte, timeout time.Duration) (Msg, error) {
	// simple in-mem request/reply: publish to subject with Reply "_INBOX"
	reply := "_INBOX"
	ch := make(chan Msg, 1)

	sub, _ := m.Subscribe(reply, func(mm Msg) { ch <- mm })
	_ = m.PublishMsg(Msg{Subject: subject, Data: data, Reply: reply})
	_ = sub.Unsubscribe()

	if timeout <= 0 {
		timeout = time.Second
	}
	select {
	case r := <-ch:
		return r, nil
	case <-time.After(timeout):
		return Msg{}, errors.New("timeout")
	}
}

func (m *MockClient) Subscribe(subject string, handler func(Msg)) (Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	m.subs[subject] = append(m.subs[subject], handler)
	idx := len(m.subs[subject]) - 1
	return mockSub{unsub: func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		hs := m.subs[subject]
		if idx >= 0 && idx < len(hs) {
			hs[idx] = func(Msg) {}
			m.subs[subject] = hs
		}
	}}, nil
}

func (m *MockClient) QueueSubscribe(subject, queue string, handler func(Msg)) (Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	if _, ok := m.qsubs[subject]; !ok {
		m.qsubs[subject] = map[string][]func(Msg){}
	}
	m.qsubs[subject][queue] = append(m.qsubs[subject][queue], handler)
	idx := len(m.qsubs[subject][queue]) - 1
	return mockSub{unsub: func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		hs := m.qsubs[subject][queue]
		if idx >= 0 && idx < len(hs) {
			hs[idx] = func(Msg) {}
			m.qsubs[subject][queue] = hs
		}
	}}, nil
}

func (m *MockClient) FlushTimeout(d time.Duration) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return ErrClosed
	}
	return nil
}

func (m *MockClient) JetStream() (JetStream, error) {
	return nil, errors.New("not implemented in mock")
}

func (m *MockClient) Publishes() []Msg {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Msg, len(m.publishes))
	copy(out, m.publishes)
	return out
}
