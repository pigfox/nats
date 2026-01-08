// natsapp/service.go
package natsapp

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

type UserCreated struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

type Service struct {
	C Client
}

func (s Service) PublishUserCreated(ctx context.Context, u UserCreated) error {
	if s.C == nil {
		return errors.New("client required")
	}
	b, err := json.Marshal(u)
	if err != nil {
		return err
	}
	if err := s.C.Publish("users.created", b); err != nil {
		return err
	}
	return s.C.FlushTimeout(2 * time.Second)
}

func (s Service) StartUserCreatedHandler(ctx context.Context, handler func(UserCreated) error) (Subscription, error) {
	if s.C == nil {
		return nil, errors.New("client required")
	}
	return s.C.Subscribe("users.created", func(m Msg) {
		var u UserCreated
		if err := json.Unmarshal(m.Data, &u); err != nil {
			return
		}
		_ = handler(u)
	})
}

func (s Service) StartAdder(ctx context.Context) (Subscription, error) {
	if s.C == nil {
		return nil, errors.New("client required")
	}
	return s.C.Subscribe("math.add", func(m Msg) {
		// request payload: {"a":1,"b":2}
		var in struct {
			A int `json:"a"`
			B int `json:"b"`
		}
		if err := json.Unmarshal(m.Data, &in); err != nil || m.Reply == "" {
			return
		}
		out := struct {
			Sum int `json:"sum"`
		}{Sum: in.A + in.B}
		b, _ := json.Marshal(out)
		_ = s.C.Publish(m.Reply, b)
	})
}
