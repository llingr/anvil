// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"context"
	"fmt"
	"time"

	"github.com/llingr/anvil/shutdown"
)

// shutdownHandler is one shutdown handler and the record of its call, written only by call
type shutdownHandler struct {
	group    *shutdownGroup // its group, for the label and logger in its line
	name     string
	fn       shutdown.Handler
	called   time.Time // zero until called
	returned time.Time // zero unless it returned before the deadline
	err      error
}

// isNeverCalled reports a handler the deadline passed before
func (sh *shutdownHandler) isNeverCalled() bool {
	return sh.called.IsZero()
}

// isRunning reports a handler called but not returned before the deadline
func (sh *shutdownHandler) isRunning() bool {
	return !sh.called.IsZero() && sh.returned.IsZero()
}

// call calls the handler unless the deadline has expired, recording when it was called and, if it
// returned before the deadline, when it returned and its error. A handler still running at the
// deadline is left running. Whenever the handler returns, even long after the deadline, its line
// is logged, with its error if it failed.
func (sh *shutdownHandler) call(ctx context.Context) {
	deadline, _ := ctx.Deadline()
	called := time.Now()
	if isExpired(deadline, called) {
		return
	}
	sh.called = called
	results := make(chan result, 1) // a handler returning after call has gone still sends and exits
	go func() {
		err := catchPanic(func() error {
			return sh.fn.Shutdown(ctx)
		})
		at := time.Now()

		logger, runCtx := sh.group.logger, sh.group.runCtx
		line := fmt.Sprintf("shutdown %s: %s", sh.group.label(), sh.name)
		switch {
		case isExpired(deadline, at) && err == nil:
			late := at.Sub(deadline).Round(time.Millisecond)
			logger.LifecycleInfo(runCtx, fmt.Sprintf("%s completed %s after the deadline", line, late))
		case isExpired(deadline, at):
			late := at.Sub(deadline).Round(time.Millisecond)
			logger.LifecycleError(runCtx, fmt.Sprintf("%s errored %s after the deadline", line, late), err)
		case err == nil:
			took := at.Sub(called).Round(time.Millisecond)
			logger.LifecycleInfo(runCtx, fmt.Sprintf("%s done in %s", line, took))
		default:
			took := at.Sub(called).Round(time.Millisecond)
			logger.LifecycleError(runCtx, fmt.Sprintf("%s failed after %s", line, took), err)
		}

		results <- result{err, at} // not reached on runtime.Goexit
	}()
	returned, ok := await(ctx, results)
	if ok && !isExpired(deadline, returned.at) {
		sh.returned, sh.err = returned.at, returned.err
	}
}

// result is what a handler returned, and when
type result struct {
	err error
	at  time.Time
}

// await waits for the handler's result or for ctx to end, still taking a result sent as ctx ended
// so the record depends on the return time alone; false means the handler has not returned
func await(ctx context.Context, results <-chan result) (result, bool) {
	select {
	case returned := <-results:
		return returned, true
	case <-ctx.Done():
	}
	select {
	case returned := <-results:
		return returned, true
	default:
		return result{}, false
	}
}
