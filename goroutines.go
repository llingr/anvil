// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// goroutine is a function started by a group's Go, and the shutdown handler that stops it
type goroutine struct {
	group  *shutdownGroup          // its group, for the label in its errors and whether it is stopping
	ctx    context.Context         // fn's ctx, derived from the shell's goCtx
	cancel context.CancelCauseFunc // cancels ctx when the group stops
	done   chan error              // fn's result, buffered so the goroutine always exits
}

// run calls fn on its own goroutine. An error or panic before its group stops stops the service;
// nil before then is logged, and the service runs on without it.
func (g *goroutine) run(fn func(ctx context.Context) error) {
	go func() {
		err := catchPanic(func() error {
			return fn(g.ctx)
		})
		if g.group.stopping.Load() {
			err = nil // its group stopped it
		}

		g.group.mu.Lock() // wiring may still be naming the group
		label := g.group.label()
		g.group.mu.Unlock()

		select {
		case <-g.group.stopped: // the service is already stopping
		default:
			if err == nil {
				g.group.logger.LifecycleError(g.group.runCtx, label, errReturnedEarly)
			}
		}
		if err != nil && g.group.requestStop(fmt.Errorf("%s: %w", label, err)) {
			err = nil // the stop's reason, so not reported again by the group
		}
		g.done <- err // not reached on runtime.Goexit, so Shutdown waits for the deadline
	}()
}

// Shutdown cancels fn's ctx and waits for fn to return, or for the deadline
func (g *goroutine) Shutdown(ctx context.Context) error {
	deadline, _ := ctx.Deadline()
	g.cancel(stopCause{deadline})
	select {
	case err := <-g.done:
		return err
	case <-ctx.Done(): // the deadline
		return ctx.Err()
	}
}

// errReturnedEarly reports a goroutine Go started that ended without being told to stop
var errReturnedEarly = errors.New("returned nil before any stop")

// stopCause is the cancellation cause of a Go function's ctx, carrying the shutdown deadline
type stopCause struct {
	deadline time.Time
}

func (stopCause) Error() string {
	return "told to stop"
}

// Unwrap makes a function that returns context.Cause(ctx) stop as cleanly as one returning ctx.Err()
func (stopCause) Unwrap() error {
	return context.Canceled
}

// StopContext returns a ctx for the stopping work of a function started with a group's Go, whose
// own ctx has just been cancelled. Given that ctx, it returns one with the same values that the stop
// does not cancel and whose deadline is the shutdown deadline. Given any other ctx, or one not yet
// told to stop, it returns a ctx with the same values and no deadline. Pass the ctx Go gave fn: a
// ctx derived from it that ended first, with a cause of its own, has no shutdown deadline. Call
// cancel when the stopping work is done.
func StopContext(ctx context.Context) (context.Context, context.CancelFunc) {
	cause, ok := context.Cause(ctx).(stopCause)
	if !ok {
		return context.WithCancel(context.WithoutCancel(ctx))
	}
	return context.WithDeadline(context.WithoutCancel(ctx), cause.deadline)
}
