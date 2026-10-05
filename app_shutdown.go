// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/llingr/anvil/shutdown"
)

// shutdown waits for a stop, drains if a SIGTERM asked, cancels wire's ctx, then runs the
// phases within the shutdown deadline
func (s *shell[C, L]) shutdown(cancelWire context.CancelFunc) error {
	<-s.stopped
	cause := s.reason()
	stopping := "Stop called"
	if cause != nil {
		stopping = cause.Error()
	}
	s.loggerProvider.LifecycleInfo(s.detached, "stopping: "+stopping)
	var failures []error
	if !cleanStop(cause) {
		failures = append(failures, cause)
	}

	// SIGTERM is Kubernetes taking the pod out of its endpoints, so keep serving meanwhile. Ctrl+C
	// (SIGINT), Stop and a cancelled ctx skip only the wait: the phases still run, so servers and
	// pools close properly on a developer's machine too, and the drain's time goes to the phases.
	drainEnd := s.shutdownStart().Add(s.options.drainDelay)
	if drainLeft := time.Until(drainEnd); stoppedBySignal(cause) == syscall.SIGTERM && drainLeft > 0 {
		s.loggerProvider.LifecycleInfo(s.detached, fmt.Sprintf("draining for %s before %s", drainLeft.Round(time.Millisecond), shutdown.Ingress))
		time.Sleep(drainLeft) // a second signal will exit immediately
	}
	cancelWire() // wire's loops stop before the handlers close what they use
	return errors.Join(append(failures, s.runPhases(drainEnd)...)...)
}

// cleanStop is a stop asked for by a signal, Stop(nil), or cancelling Run's ctx without a cause
func cleanStop(cause error) bool {
	return cause == nil || cause == errRunCtxCancelled || stoppedBySignal(cause) != nil
}

// errRunCtxCancelled is the reason when the ctx passed to Run is cancelled
var errRunCtxCancelled = errors.New("the ctx passed to Run was cancelled")

// requestStop begins shutdown for reason, which says what asked for it, when no stop came before it,
// and reports whether it did
func (s *shell[C, L]) requestStop(reason error) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.stopped:
		return false // subsequent stops/reasons are ignored
	default:
	}
	s.stoppedAt = time.Now()
	s.stopReason = reason
	close(s.stopped)
	return true
}

// reason is why the application stopped: nil for a clean Stop(nil)
func (s *shell[C, L]) reason() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopReason
}

// shutdownStart is when the stop that won arrived: the drain and every phase deadline count from it,
// so a wire returning late takes its time out of the shutdown rather than adding to it
func (s *shell[C, L]) shutdownStart() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stoppedAt
}

// runPhases ends each phase at the running total of budgets from the drain's end, so time a phase
// leaves unused, and a drain that did not run, carries forward to the next
func (s *shell[C, L]) runPhases(drainEnd time.Time) []error {
	var failures []error
	deadline := drainEnd
	for _, phase := range shutdown.Phases() {
		deadline = deadline.Add(s.options.shutdownBudgets[phase])
		s.endPhaseCtx[phase]() // Go's loops for this phase stop taking work
		failures = append(failures, s.shutdownPhase(phase, deadline)...)
	}
	return failures
}

func (s *shell[C, L]) shutdownPhase(phase shutdown.Phase, deadline time.Time) []error {
	registeredHandlers := func() []registeredHandler {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.shutdownHandlers[phase]
	}()

	ctx, cancel := context.WithDeadline(s.detached, deadline)
	defer cancel()
	start := time.Now()
	failures := make([]error, len(registeredHandlers))
	took := make([]time.Duration, len(registeredHandlers))
	timed := func(index int) {
		if ctx.Err() != nil { // an overrunning handler or a late wire used the phase's time
			failures[index] = fmt.Errorf("%s %s: %w", phase, registeredHandlers[index].name, errSkipped)
			return
		}
		handlerStart := time.Now()
		failures[index] = invoke(ctx, phase, registeredHandlers[index])
		took[index] = time.Since(handlerStart)
	}
	if phase == shutdown.Core {
		for index := len(registeredHandlers) - 1; index >= 0; index-- { // reverse, as defer runs
			timed(index)
		}
	} else {
		var pending sync.WaitGroup
		for index := range registeredHandlers {
			pending.Go(func() {
				timed(index)
			})
		}
		pending.Wait()
	}
	if len(registeredHandlers) > 0 {
		s.loggerProvider.LifecycleInfo(s.detached, phaseSummary(phase, time.Since(start), registeredHandlers, took, failures))
	}
	return failures
}

// errSkipped fails a handler never started, so that no two Core handlers ever run at once and
// none is left running behind the next phase
var errSkipped = errors.New("skipped, the phase's deadline had passed")

// phaseSummary reads "CORE done in 5.7s: orders skipped, ledger failed after 5.7s, outbox 2ms"
func phaseSummary(phase shutdown.Phase, elapsed time.Duration, handlers []registeredHandler, took []time.Duration, failures []error) string {
	parts := make([]string, len(handlers))
	for index, registered := range handlers {
		switch {
		case errors.Is(failures[index], errSkipped):
			parts[index] = registered.name + " skipped"
		case failures[index] != nil:
			parts[index] = fmt.Sprintf("%s failed after %s", registered.name, took[index].Truncate(time.Microsecond))
		default:
			parts[index] = fmt.Sprintf("%s %s", registered.name, took[index].Truncate(time.Microsecond))
		}
	}
	return fmt.Sprintf("%s done in %s: %s", phase, elapsed.Truncate(time.Microsecond), strings.Join(parts, ", "))
}

// invoke waits for the handler or the budget, whichever ends first; a handler
// still running at the deadline is left to finish on its own
func invoke(ctx context.Context, phase shutdown.Phase, registered registeredHandler) error {
	done := make(chan error, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				done <- fmt.Errorf("panic: %v\n%s", recovered, debug.Stack())
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
