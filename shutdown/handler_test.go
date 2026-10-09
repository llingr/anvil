// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package shutdown_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/llingr/anvil/shutdown"
)

func TestIgnoreContextCallsTheFunction(t *testing.T) {
	failed := errors.New("close failed")
	err := shutdown.IgnoreContext(func() error {
		return failed
	})(context.Background())
	if !errors.Is(err, failed) {
		t.Fatalf("error %v, want the function's", err)
	}
}

// Close runs the close function and reports no error
func TestClose(t *testing.T) {
	closed := false
	err := shutdown.Close(func() {
		closed = true
	})(context.Background())
	if err != nil || !closed {
		t.Fatalf("error %v, closed %v, want nil and closed", err, closed)
	}
}

// A HandlerFunc's Shutdown calls the function with the ctx it is given and returns its error
func TestHandlerFuncCallsTheFunction(t *testing.T) {
	type key struct{}
	failed := errors.New("flush failed")
	var seen any
	var handler shutdown.Handler = shutdown.HandlerFunc(func(ctx context.Context) error {
		seen = ctx.Value(key{})
		return failed
	})
	err := handler.Shutdown(context.WithValue(context.Background(), key{}, "ctx"))
	if err != failed || seen != "ctx" {
		t.Fatalf("error %v, ctx value %v, want %v and the ctx passed", err, seen, failed)
	}
}

// Named keeps the handler's Shutdown and gives the name back, through the method anvil reads
func TestNamedKeepsTheHandler(t *testing.T) {
	called := false
	named := shutdown.Named("outbox", shutdown.HandlerFunc(func(context.Context) error {
		called = true
		return nil
	}))
	if err := named.Shutdown(context.Background()); err != nil || !called {
		t.Fatalf("error %v, called %v, want nil and the handler called", err, called)
	}
	name, ok := named.(interface{ ShutdownName() string })
	if !ok || name.ShutdownName() != "outbox" {
		t.Fatalf("Named gives no name back, want outbox")
	}
}

// Handlers of every kind that can be nil, for Named's nil check
type (
	pointerHandler struct{}
	mapHandler     map[string]int
	sliceHandler   []int
	chanHandler    chan int
	valueHandler   struct{}
)

func (*pointerHandler) Shutdown(context.Context) error {
	return nil
}

func (mapHandler) Shutdown(context.Context) error {
	return nil
}

func (sliceHandler) Shutdown(context.Context) error {
	return nil
}

func (chanHandler) Shutdown(context.Context) error {
	return nil
}

func (valueHandler) Shutdown(context.Context) error {
	return nil
}

// Named given a nil handler panics at once, naming the name, rather than hiding it in a wrapper
// until the shutdown calls it: a nil interface, or a nil pointer, map, slice, func or chan in one
func TestNamedNilPanics(t *testing.T) {
	cases := []struct {
		name    string
		handler shutdown.Handler
	}{
		{"nil interface", nil},
		{"nil pointer", (*pointerHandler)(nil)},
		{"nil map", mapHandler(nil)},
		{"nil slice", sliceHandler(nil)},
		{"nil func", shutdown.HandlerFunc(nil)},
		{"nil chan", chanHandler(nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				message := fmt.Sprint(recover())
				if !strings.HasPrefix(message, "shutdown: ") || !strings.Contains(message, `"outbox"`) {
					t.Fatalf("panic %q, want a shutdown: message naming outbox", message)
				}
			}()
			shutdown.Named("outbox", tc.handler)
		})
	}
}

// Named accepts every non-nil handler, including the kinds that could be nil and a plain value
func TestNamedAcceptsNonNilHandlers(t *testing.T) {
	handlers := []shutdown.Handler{
		&pointerHandler{},
		mapHandler{},
		sliceHandler{},
		make(chanHandler),
		valueHandler{},
		shutdown.HandlerFunc(func(context.Context) error {
			return nil
		}),
	}
	for _, handler := range handlers {
		if named := shutdown.Named("outbox", handler); named == nil {
			t.Errorf("Named(%T) returned nil, want the named handler", handler)
		}
	}
}
