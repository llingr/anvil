// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/llingr/anvil/lifecycle/shutdown"
)

// shutdown waits for a trigger, then runs the phases within their budgets
func (a *application) shutdown() error {
	<-a.stopping.Done()
	// the signal package's cause reports errors.Is(context.Canceled), so only identity separates
	// a signal's stop from a reason that merely wraps the sentinel
	signalled := context.Cause(a.signalled)
	var failures []error
	if cause := context.Cause(a.stopping); cause != context.Canceled && cause != signalled {
		failures = append(failures, cause) // a signal and Stop(nil) both stop cleanly
	}

	// trap before release: a signal falling through to the default disposition kills the process
	forced := make(chan os.Signal, len(a.shutdownSignals))
	signal.Notify(forced, a.shutdownSignals...)
	defer signal.Stop(forced)
	// no signal asked for this shutdown, so the first one is the asking and not the second ask
	absorb := 0
	if signalled == nil {
		absorb = 1
	}
	a.releaseSignals()
	finished := make(chan struct{})
	defer close(finished)
	go exitOnSignal(forced, finished, absorb)

	return errors.Join(append(failures, a.runPhases()...)...)
}

// runPhases ends each phase at the running total of budgets, so unused time carries forward
func (a *application) runPhases() []error {
	var failures []error
	deadline := time.Now()
	for _, phase := range shutdown.Phases() {
		deadline = deadline.Add(a.shutdownBudgets[phase])
		failures = append(failures, a.shutdownPhase(phase, deadline)...)
	}
	return failures
}

// exitOnSignal quits on the first signal past the absorb allowance, leaving the handlers behind
func exitOnSignal(signals <-chan os.Signal, finished <-chan struct{}, absorb int) {
	for {
		select {
		case received := <-signals:
			if absorb > 0 {
				absorb--
				continue
			}
			code := 1
			if number, ok := received.(syscall.Signal); ok {
				code = 128 + int(number) // uses Unix convention
			}
			os.Exit(code)
		case <-finished:
			return
		}
	}
}

func (a *application) shutdownPhase(phase shutdown.Phase, deadline time.Time) []error {
	registeredHandlers := func() []registeredHandler {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.shutdownHandlers[phase]
	}()

	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	failures := make([]error, len(registeredHandlers))
	if phase == shutdown.Default {
		for index, registered := range registeredHandlers {
			failures[index] = invoke(ctx, phase, registered)
		}
		return failures
	}
	var pending sync.WaitGroup
	for index, registered := range registeredHandlers {
		pending.Go(func() {
			failures[index] = invoke(ctx, phase, registered)
		})
	}
	pending.Wait()
	return failures
}

// invoke waits for the handler or the budget, whichever ends first; a handler
// still running at the deadline is left to finish on its own
func invoke(ctx context.Context, phase shutdown.Phase, registered registeredHandler) error {
	done := make(chan error, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				done <- fmt.Errorf("panic: %v", recovered)
			}
		}()
		done <- registered.shutdownHandler(ctx)
	}()
	var err error
	select {
	case err = <-done:
	case <-ctx.Done():
		select {
		case err = <-done:
		default:
			err = ctx.Err()
		}
	}
	if err != nil {
		return fmt.Errorf("%s %s: %w", phase, registered.name, err)
	}
	return nil
}
