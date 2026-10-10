// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
	"time"
)

// awaitDrainDelay waits out the drain delay while the service keeps serving
func (s *shell[C, L]) awaitDrainDelay() {
	if stoppedBySignal(s.stopReason) != kubernetesTermSignal {
		return
	}
	drainLeft := time.Until(s.stoppedTime.Add(s.options.drainDelay))
	if drainLeft > 0 {
		const pauseMessage = "pausing for %s before shutdown"
		s.loggerProvider.LifecycleInfo(s.runCtx, fmt.Sprintf(pauseMessage, drainLeft.Round(time.Millisecond)))
		time.Sleep(drainLeft) // a second signal will exit immediately
	}
}

// deadline is the time the shutdown ends, counted from the stop
func (s *shell[C, L]) deadline() time.Time {
	return s.stoppedTime.Add(s.options.shutdownDeadline)
}

// isExpired reports whether the deadline had been reached at the time at
func isExpired(deadline, at time.Time) bool {
	return !at.Before(deadline)
}

// errRunCtxCancelled is the reason when the ctx passed to Run is cancelled
var errRunCtxCancelled = errors.New("the ctx passed to Run was cancelled")

// requestStop initiates the graceful shut-down process
func (s *shell[C, L]) requestStop(reason error) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-s.stopped:
		return false
	default:
	}
	s.stopReason = reason
	s.stoppedTime = time.Now()
	if !s.started {
		// logged under the lock, so it comes before anything run logs once wire returns, and a wire
		// that never returns still shows why the process went quiet
		s.loggerProvider.LifecycleInfo(s.runCtx, stoppingLine(reason)+", waiting for wire to return")
		s.wireCtxCancel() // a stop before or during wiring: wire returns rather than finishing a start nobody wants
	}
	close(s.stopped) // after wire's ctx, so nothing sees Stopping() while wire's ctx is still live
	return true
}

// stoppingLine is the line logged as the shutdown begins, naming its reason
func stoppingLine(reason error) string {
	if reason == nil {
		return "stopping: Stop called"
	}
	return "stopping: " + reason.Error()
}

// shutdown runs the groups, the last added first; each handler's call refuses it once the deadline
// has passed
func (s *shell[C, L]) shutdown(ctx context.Context) {
	for _, group := range slices.Backward(s.groups) {
		if !isExpired(s.deadline(), time.Now()) {
			names := "no handlers"
			if len(group.handlers) > 0 {
				names = group.handlers[0].name
				for _, handler := range group.handlers[1:] {
					names += ", " + handler.name
				}
			}
			s.loggerProvider.LifecycleInfo(s.runCtx, fmt.Sprintf("shutdown %s with %s", group.label(), names))
		}
		group.runHandlers(ctx) // each handler logs its own line as it returns
	}
}

// catchPanic calls fn, returning a panic as "panic: <value>" and the stack. On runtime.Goexit it
// does not return.
func catchPanic(fn func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic: %v\n%s", recovered, debug.Stack())
		}
	}()
	return fn()
}

// failures joins, in append order, the stop reason when it is an error, wire's error unless it is
// the reason, the handlers' errors in the order they stopped, and the deadline error
func (s *shell[C, L]) failures(reason, wireErr error) error {
	var failures []error
	// a signal, Stop(nil) and cancelling Run's ctx without a cause are clean stops; any other reason failed
	if reason != nil && reason != errRunCtxCancelled && stoppedBySignal(reason) == nil {
		failures = append(failures, reason)
	}
	if wireErr != nil && wireErr != reason {
		failures = append(failures, wireErr)
	}
	unfinished := false
	for _, group := range slices.Backward(s.groups) {
		for _, handler := range group.handlers {
			if handler.err != nil {
				failures = append(failures, fmt.Errorf("%s %s: %w", group.label(), handler.name, handler.err))
			}
			if handler.returned.IsZero() { // never called, or still running at the deadline
				unfinished = true
			}
		}
	}
	if unfinished {
		failures = append(failures, deadlineError(s.options.shutdownDeadline, s.groups))
	}
	return errors.Join(failures...)
}
