// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/llingr/anvil/shutdown"
)

// ShutdownGroup is handlers that stop together. Groups stop one after another, in the order they
// were added; each method may only be called during wiring.
type ShutdownGroup interface {
	// SetName names the group in the log lines, in place of its position
	SetName(name string) ShutdownGroup

	// Add adds handlers, each named by its type or by shutdown.Named
	Add(handlers ...shutdown.Handler) ShutdownGroup

	// Go runs fn on its own goroutine until the group stops, when fn's ctx is cancelled and the
	// group waits for fn to return. An error or panic before then stops the service.
	Go(fn func(ctx context.Context) error) ShutdownGroup
}

// lifecycleLogger is LoggerProvider's lifecycle lines, without the logger's type parameter
type lifecycleLogger interface {
	LifecycleInfo(ctx context.Context, msg string)
	LifecycleError(ctx context.Context, msg string, err error)
}

// shutdownGroup is one of the service's groups with its handlers, in the order they were added.
// The handlers stop changing when registration closes, so the shutdown reads them without the lock.
type shutdownGroup struct {
	mu          *sync.Mutex             // the shell's: guards every group while wiring
	started     *bool                   // the shell's: true once wire has returned, refusing every change
	logger      lifecycleLogger         // the shell's LoggerProvider
	runCtx      context.Context         // the shell's, for logging
	goCtx       context.Context         // the shell's: the parent of every goroutine's ctx
	stopped     <-chan struct{}         // the shell's: closed when the shutdown begins
	requestStop func(reason error) bool // the shell's: begins the shutdown, false if it had begun
	order       int                     // position among the groups, from 1
	name        string                  // set by SetName
	handlers    []*shutdownHandler
	stopping    atomic.Bool // set before any handler is called; then a Go function's return does not count
}

// label is the group as the log lines show it: its position, so the order is visible, then its name
// if it has one, as "group 2 (payments)"
func (sg *shutdownGroup) label() string {
	if sg.name == "" {
		return fmt.Sprintf("group %d", sg.order)
	}
	return fmt.Sprintf("group %d (%s)", sg.order, sg.name)
}

func (sg *shutdownGroup) SetName(name string) ShutdownGroup {
	sg.mu.Lock()
	if *sg.started {
		sg.mu.Unlock()
		sg.logger.LifecycleError(sg.runCtx, fmt.Sprintf("shutdown group %q not named", name), errAddedLate)
		return sg
	}
	defer sg.mu.Unlock()
	if sg.name != "" {
		panic(fmt.Sprintf("anvil: shutdown group %q named again as %q", sg.name, name))
	}
	sg.name = name
	return sg
}

func (sg *shutdownGroup) Add(handlers ...shutdown.Handler) ShutdownGroup {
	if len(handlers) == 0 {
		return sg
	}
	sg.mu.Lock()
	if *sg.started {
		sg.mu.Unlock()
		sg.logger.LifecycleError(sg.runCtx, "shutdown handler not added", errAddedLate)
		return sg
	}
	defer sg.mu.Unlock()
	for _, handler := range handlers {
		// a panic here, during wiring, becomes wire's error; on any other goroutine it crashes the process
		if isNil(handler) {
			panic(fmt.Sprintf("anvil: nil shutdown handler in shutdown %s", sg.label()))
		}
		name := reflect.TypeOf(handler).String() // as *http.Server
		if named, ok := handler.(interface{ ShutdownName() string }); ok {
			name = named.ShutdownName()
		}
		sg.handlers = append(sg.handlers, &shutdownHandler{
			group: sg,
			name:  name,
			fn:    handler,
		})
	}
	return sg
}

func (sg *shutdownGroup) Go(fn func(ctx context.Context) error) ShutdownGroup {
	sg.mu.Lock()
	if *sg.started {
		sg.mu.Unlock()
		sg.logger.LifecycleError(sg.runCtx, "goroutine not started", errAddedLate)
		return sg
	}
	defer sg.mu.Unlock()
	if fn == nil {
		panic(fmt.Sprintf("anvil: nil function passed to Go in shutdown %s", sg.label()))
	}
	ctx, cancel := context.WithCancelCause(sg.goCtx)
	g := &goroutine{
		group:  sg,
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan error, 1),
	}
	sg.handlers = append(sg.handlers, &shutdownHandler{
		group: sg,
		name:  "go",
		fn:    g,
	})
	g.run(fn) // only once its handler is added, so shutdown never misses it
	return sg
}

// runHandlers calls the group's handlers together
func (sg *shutdownGroup) runHandlers(ctx context.Context) {
	// before any handler, as a server's Shutdown can end its Go function before the
	// function's own handler runs, and what the function returns then is a clean stop
	sg.stopping.Store(true)
	var wg sync.WaitGroup
	for _, handler := range sg.handlers {
		wg.Go(func() {
			handler.call(ctx)
		})
	}
	wg.Wait()
}
