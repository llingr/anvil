// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/llingr/anvil/shutdown"
)

// A bubbled clock advances only once every goroutine is blocked, so these tests assert the real
// 23s of phase budgets exactly, in no wall clock time, and fail if a goroutine outlives the shutdown.
// They drive runPhases rather than Run because os/signal cannot be called inside a bubble.

// silentLogger stands in for the logger provider, the phase lines being Run's concern, not these tests
type silentLogger struct{}

func (silentLogger) Logger() struct{} {
	return struct{}{}
}

func (silentLogger) LifecycleInfo(context.Context, string) {
}

func (silentLogger) LifecycleError(context.Context, string, error) {
}

func (silentLogger) Flush() {
}

// phased is a shell with the default budgets and no signals trapped
func phased(t *testing.T, opts ...Option) *shell[struct{}, struct{}] {
	t.Helper()
	s := &shell[struct{}, struct{}]{
		detached:         context.Background(),
		loggerProvider:   silentLogger{},
		stopped:          make(chan struct{}),
		options:          processOptions(opts...),
		phaseCtx:         make(map[shutdown.Phase]context.Context),
		endPhaseCtx:      make(map[shutdown.Phase]context.CancelFunc),
		shutdownHandlers: make(map[shutdown.Phase][]registeredHandler),
	}
	for _, phase := range shutdown.Phases() {
		s.phaseCtx[phase], s.endPhaseCtx[phase] = context.WithCancel(context.Background())
	}
	return s
}

// remaining records the time a handler has left when it starts
func remaining(into *time.Duration) shutdown.Handler {
	return func(ctx context.Context) error {
		deadline, _ := ctx.Deadline()
		*into = time.Until(deadline)
		return nil
	}
}

// occupancy counts how many handlers are inside a phase at once
type occupancy struct {
	mu      sync.Mutex
	inside  int
	most    int
	entered []string
}

func (o *occupancy) handler(name string, delay time.Duration) shutdown.Handler {
	return func(context.Context) error {
		o.enter(name)
		time.Sleep(delay)
		o.leave()
		return nil
	}
}

func (o *occupancy) enter(name string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.inside++
	o.entered = append(o.entered, name)
	o.most = max(o.most, o.inside)
}

// order is the handlers entered so far, safe while an abandoned one still runs
func (o *occupancy) order() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return strings.Join(o.entered, " ")
}

func (o *occupancy) leave() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.inside--
}

func TestPhaseDeadlinesAreExact(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var first, other, last time.Duration
		s := phased(t)
		s.RegisterShutdownHandler(shutdown.Ingress, "first", remaining(&first))
		s.RegisterShutdownHandler(shutdown.Core, "default", remaining(&other))
		s.RegisterShutdownHandler(shutdown.Egress, "last", remaining(&last))

		if err := errors.Join(s.runPhases(time.Now())...); err != nil {
			t.Fatal(err)
		}
		budgets := []struct {
			phase     shutdown.Phase
			got, want time.Duration
		}{
			{shutdown.Ingress, first, 11500 * time.Millisecond},
			{shutdown.Core, other, 17250 * time.Millisecond},
			{shutdown.Egress, last, 23 * time.Second},
		}
		for _, budget := range budgets {
			if budget.got != budget.want {
				t.Errorf("%s had %s left, want %s", budget.phase, budget.got, budget.want)
			}
		}
	})
}

// Time Ingress leaves unspent reaches the phases after it
func TestUnspentBudgetRollsForward(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var other, last time.Duration
		slow := &occupancy{}
		s := phased(t)
		s.RegisterShutdownHandler(shutdown.Ingress, "slow", slow.handler("slow", 5*time.Second))
		s.RegisterShutdownHandler(shutdown.Core, "default", remaining(&other))
		s.RegisterShutdownHandler(shutdown.Egress, "last", remaining(&last))

		start := time.Now()
		if err := errors.Join(s.runPhases(time.Now())...); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed != 5*time.Second {
			t.Fatalf("shutdown took %s, want the 5s the one slow handler used", elapsed)
		}
		if other != 12250*time.Millisecond {
			t.Errorf("CORE had %s left, want 12.25s", other)
		}
		if last != 18*time.Second {
			t.Errorf("EGRESS had %s left, want 18s", last)
		}
	})
}

// A handler ignoring its context is abandoned on its own deadline and the phases after it keep
// their budgets, so shutdown ends at the total however badly every handler behaves
func TestShutdownEndsAtTheTotalBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		defer synctest.Wait() // the abandoned handlers leave before the bubble does
		defer close(release)
		stuck := func(context.Context) error {
			<-release
			return nil
		}

		s := phased(t)
		s.RegisterShutdownHandler(shutdown.Ingress, "ingress", stuck)
		s.RegisterShutdownHandler(shutdown.Core, "service", stuck)
		s.RegisterShutdownHandler(shutdown.Egress, "egress", stuck)

		start := time.Now()
		err := errors.Join(s.runPhases(time.Now())...)
		if elapsed := time.Since(start); elapsed != 23*time.Second {
			t.Fatalf("shutdown took %s, want 23s", elapsed)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error %v, want deadline exceeded", err)
		}
		for _, abandoned := range []string{"INGRESS ingress", "CORE service", "EGRESS egress"} {
			if !strings.Contains(err.Error(), abandoned) {
				t.Errorf("error %v, want %s named", err, abandoned)
			}
		}
	})
}

// A phase that overruns leaves the next one its own budget and no more
func TestOverrunLeavesTheNextPhaseItsOwnBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		defer synctest.Wait()
		defer close(release)

		var other time.Duration
		s := phased(t)
		s.RegisterShutdownHandler(shutdown.Ingress, "ingress", func(context.Context) error {
			<-release
			return nil
		})
		s.RegisterShutdownHandler(shutdown.Core, "default", remaining(&other))

		start := time.Now()
		_ = errors.Join(s.runPhases(time.Now())...)
		if elapsed := time.Since(start); elapsed != 11500*time.Millisecond {
			t.Fatalf("INGRESS ran for %s, want its 11.5s budget", elapsed)
		}
		if other != 5750*time.Millisecond {
			t.Errorf("CORE had %s left, want its own 5.75s", other)
		}
	})
}

// A Core handler that overruns leaves the ones after it skipped, never launched, so no two
// services ever run at once
func TestCoreHandlersAfterAnOverrunAreSkipped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		defer synctest.Wait()
		defer close(release)

		ran := &occupancy{}
		s := phased(t)
		for _, name := range []string{"orders", "ledger"} {
			s.RegisterShutdownHandler(shutdown.Core, name, ran.handler(name, 0))
		}
		s.RegisterShutdownHandler(shutdown.Core, "outbox", func(context.Context) error {
			ran.enter("outbox")
			<-release
			return nil
		})

		err := errors.Join(s.runPhases(time.Now())...)
		if order := ran.order(); order != "outbox" {
			t.Errorf("handlers ran %q, want only the overrunning outbox", order)
		}
		for _, skipped := range []string{"CORE ledger: skipped", "CORE orders: skipped"} {
			if err == nil || !strings.Contains(err.Error(), skipped) {
				t.Errorf("error %v, want %q", err, skipped)
			}
		}
	})
}

// Ingress and Egress hold every handler at once, so the phase costs the slowest of them
func TestConcurrentPhasesCostTheSlowestHandler(t *testing.T) {
	for _, phase := range []shutdown.Phase{shutdown.Ingress, shutdown.Egress} {
		t.Run(string(phase), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				together := &occupancy{}
				s := phased(t)
				for _, delay := range []time.Duration{time.Second, 3 * time.Second, 5 * time.Second} {
					s.RegisterShutdownHandler(phase, delay.String(), together.handler(delay.String(), delay))
				}

				start := time.Now()
				if err := errors.Join(s.runPhases(time.Now())...); err != nil {
					t.Fatal(err)
				}
				if elapsed := time.Since(start); elapsed != 5*time.Second {
					t.Errorf("%s took %s, want 5s", phase, elapsed)
				}
				if together.most != 3 {
					t.Errorf("%s held %d handlers at once, want 3", phase, together.most)
				}
			})
		})
	}
}

// Core holds one handler at a time, in reverse registration order, so the phase costs their sum
func TestCorePhaseCostsTheSumOfItsHandlers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sequential := &occupancy{}
		s := phased(t)
		for _, name := range []string{"orders", "ledger", "outbox"} {
			s.RegisterShutdownHandler(shutdown.Core, name, sequential.handler(name, 2*time.Second))
		}

		start := time.Now()
		if err := errors.Join(s.runPhases(time.Now())...); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed != 6*time.Second {
			t.Errorf("CORE took %s, want 6s", elapsed)
		}
		if sequential.most != 1 {
			t.Errorf("CORE held %d handlers at once, want 1", sequential.most)
		}
		if order := strings.Join(sequential.entered, " "); order != "outbox ledger orders" {
			t.Errorf("handlers ran %q, want reverse registration order", order)
		}
	})
}

// Registering from many goroutines while the phases run never loses a handler or races
func TestRegisterRacesWithShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := phased(t, WithShutdownPhaseBudget(shutdown.Ingress, 20*time.Second))
		ran := &occupancy{}
		var registering sync.WaitGroup
		for index := range 16 {
			registering.Go(func() {
				s.RegisterShutdownHandler(shutdown.Ingress, strings.Repeat("h", index+1), ran.handler("h", time.Second))
			})
		}
		registering.Wait()

		if err := errors.Join(s.runPhases(time.Now())...); err != nil {
			t.Fatal(err)
		}
		if len(ran.entered) != 16 {
			t.Fatalf("%d handlers ran, want 16", len(ran.entered))
		}
		if ran.most != 16 {
			t.Errorf("INGRESS held %d handlers at once, want 16", ran.most)
		}
	})
}
