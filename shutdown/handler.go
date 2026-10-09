// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package shutdown

import (
	"context"
	"fmt"
	"reflect"
)

// Handler shuts down one component within ctx's deadline, as http.Server does
type Handler interface {
	Shutdown(ctx context.Context) error
}

// HandlerFunc is a function used as a Handler. The logs show it as shutdown.HandlerFunc unless it
// is wrapped in Named.
type HandlerFunc func(ctx context.Context) error

// Shutdown calls f
func (f HandlerFunc) Shutdown(ctx context.Context) error {
	return f(ctx)
}

// IgnoreContext adapts a shutdown function that takes no context, such as a server's Close
func IgnoreContext(handler func() error) HandlerFunc {
	return func(_ context.Context) error {
		return handler()
	}
}

// Close adapts a close function that takes no context and returns no error, such as a pool's Close
func Close(closer func()) HandlerFunc {
	return func(_ context.Context) error {
		closer()
		return nil
	}
}

// Named gives handler the name the log lines show for it, in place of its type's. It panics on a
// nil handler, including a nil pointer, map, slice, func or chan held in the interface, which the
// wrapper would otherwise hide from anvil until the shutdown called it.
func Named(name string, handler Handler) Handler {
	isNil := handler == nil
	if !isNil {
		switch value := reflect.ValueOf(handler); value.Kind() {
		case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
			isNil = value.IsNil()
		}
	}
	if isNil {
		panic(fmt.Sprintf("shutdown: Named(%q) given a nil handler", name))
	}
	return named{name: name, Handler: handler}
}

// named is a Handler with the name Named gave it
type named struct {
	name string
	Handler
}

// ShutdownName is the name Named gave
func (n named) ShutdownName() string {
	return n.name
}
