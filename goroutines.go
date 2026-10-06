// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/llingr/anvil/shutdown"
)

// Go registers the wait for fn in phase, under the lock that closes registration, and starts fn
// only once that wait is registered, so shutdown never misses a goroutine Go started
func (s *shell[C, L]) Go(phase shutdown.Phase, name string, fn func(ctx context.Context) error) {
	if fn == nil {
		panic(fmt.Sprintf("anvil: %s has a nil func", name))
	}
	done := make(chan error, 1)
	if !s.register(phase, name, waitFor(done)) {
		s.refused(name)
		return
	}
	go func() {
		var err error
		defer func() {
			reason := err
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("panicked: %v\n%s", recovered, debug.Stack())
				reason = fmt.Errorf("%s %w", name, err)
			} else if err != nil {
				reason = fmt.Errorf("%s: %w", name, err)
			} else if !s.Stopping() {
				s.loggerProvider.LifecycleError(s.detached, name, errReturnedEarly) // the service runs on without it
			}
			if err != nil && s.requestStop(reason) {
				err = nil // the stop's reason, so not reported again by the phase
			}
			done <- err
		}()
		err = fn(s.phaseCtx[phase])
	}()
}

// errReturnedEarly reports a goroutine Go started that ended without being told to stop
var errReturnedEarly = errors.New("returned nil before any stop")

// waitFor is the shutdown handler that waits for a goroutine Go started, returning its failure once
// the shutdown had begun; an error wrapping context.Canceled is the goroutine told to stop
func waitFor(done <-chan error) shutdown.Handler {
	return func(ctx context.Context) error {
		select {
		case err := <-done:
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
