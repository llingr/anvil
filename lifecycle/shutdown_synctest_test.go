// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/llingr/anvil/lifecycle/shutdown"
)

// A bubbled clock advances only once every goroutine is blocked, so these tests assert the real
// 28s of budgets exactly, in no wall clock time, and fail if a goroutine outlives the shutdown.
// They drive runPhases rather than Run because os/signal cannot be called inside a bubble.

// phased is an application with the default budgets and no signals trapped
func phased(t *testing.T, opts ...Option) *application {
	t.Helper()
	stopping, stop := context.WithCancelCause(context.Background())
	t.Cleanup(func() {
		stop(nil)
	})
	return &application{
		signalled:        context.Background(), // no signals trapped, so one never arrives
		stopping:         stopping,
		stop:             stop,
		shutdownBudgets:  processOptions(opts...).shutdownBudgets,
		shutdownHandlers: make(map[shutdown.Phase][]registeredHandler),
	}
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

func (o *occupancy) leave() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.inside--
}

func TestPhaseDeadlinesAreExact(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var first, other, last time.Duration
		app := phased(t)
		app.RegisterShutdownHandler("first", remaining(&first), shutdown.First)
		app.RegisterShutdownHandler("default", remaining(&other), shutdown.Default)
		app.RegisterShutdownHandler("last", remaining(&last), shutdown.Last)

		if err := errors.Join(app.runPhases()...); err != nil {
			t.Fatal(err)
		}
		budgets := []struct {
			phase     shutdown.Phase
			got, want time.Duration
		}{
			{shutdown.First, first, 14 * time.Second},
			{shutdown.Default, other, 21 * time.Second},
			{shutdown.Last, last, 28 * time.Second},
		}
		for _, budget := range budgets {
			if budget.got != budget.want {
				t.Errorf("%s had %s left, want %s", budget.phase, budget.got, budget.want)
			}
		}
	})
}

// Time First leaves unspent reaches the phases after it
func TestUnspentBudgetRollsForward(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var other, last time.Duration
		slow := &occupancy{}
		app := phased(t)
		app.RegisterShutdownHandler("slow", slow.handler("slow", 10*time.Second), shutdown.First)
		app.RegisterShutdownHandler("default", remaining(&other), shutdown.Default)
		app.RegisterShutdownHandler("last", remaining(&last), shutdown.Last)

		start := time.Now()
		if err := errors.Join(app.runPhases()...); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed != 10*time.Second {
			t.Fatalf("shutdown took %s, want the 10s the one slow handler used", elapsed)
		}
		if other != 11*time.Second {
			t.Errorf("DEFAULT had %s left, want 11s", other)
		}
		if last != 18*time.Second {
			t.Errorf("LAST had %s left, want 18s", last)
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

		app := phased(t)
		app.RegisterShutdownHandler("intake", stuck, shutdown.First)
		app.RegisterShutdownHandler("service", stuck, shutdown.Default)
		app.RegisterShutdownHandler("egress", stuck, shutdown.Last)

		start := time.Now()
		err := errors.Join(app.runPhases()...)
		if elapsed := time.Since(start); elapsed != 28*time.Second {
			t.Fatalf("shutdown took %s, want 28s", elapsed)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error %v, want deadline exceeded", err)
		}
		for _, abandoned := range []string{"FIRST intake", "DEFAULT service", "LAST egress"} {
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
		app := phased(t)
		app.RegisterShutdownHandler("intake", func(context.Context) error {
			<-release
			return nil
		}, shutdown.First)
		app.RegisterShutdownHandler("default", remaining(&other), shutdown.Default)

		start := time.Now()
		_ = errors.Join(app.runPhases()...)
		if elapsed := time.Since(start); elapsed != 14*time.Second {
			t.Fatalf("FIRST ran for %s, want its 14s budget", elapsed)
		}
		if other != 7*time.Second {
			t.Errorf("DEFAULT had %s left, want its own 7s", other)
		}
	})
}

// First and Last hold every handler at once, so the phase costs the slowest of them
func TestConcurrentPhasesCostTheSlowestHandler(t *testing.T) {
	for _, phase := range []shutdown.Phase{shutdown.First, shutdown.Last} {
		t.Run(string(phase), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				together := &occupancy{}
				app := phased(t)
				for _, delay := range []time.Duration{time.Second, 3 * time.Second, 5 * time.Second} {
					app.RegisterShutdownHandler(delay.String(), together.handler(delay.String(), delay), phase)
				}

				start := time.Now()
				if err := errors.Join(app.runPhases()...); err != nil {
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

// Default holds one handler at a time, in registration order, so the phase costs their sum
func TestDefaultPhaseCostsTheSumOfItsHandlers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sequential := &occupancy{}
		app := phased(t)
		for _, name := range []string{"orders", "ledger", "outbox"} {
			app.RegisterShutdownHandler(name, sequential.handler(name, 2*time.Second), shutdown.Default)
		}

		start := time.Now()
		if err := errors.Join(app.runPhases()...); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed != 6*time.Second {
			t.Errorf("DEFAULT took %s, want 6s", elapsed)
		}
		if sequential.most != 1 {
			t.Errorf("DEFAULT held %d handlers at once, want 1", sequential.most)
		}
		if order := strings.Join(sequential.entered, " "); order != "orders ledger outbox" {
			t.Errorf("handlers ran %q, want registration order", order)
		}
	})
}

// Registering from many goroutines while the phases run never loses a handler or races
func TestRegisterRacesWithShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		app := phased(t, WithShutdownPhaseBudget(shutdown.First, time.Minute))
		ran := &occupancy{}
		var registering sync.WaitGroup
		for index := range 16 {
			registering.Go(func() {
				app.RegisterShutdownHandler(strings.Repeat("h", index+1), ran.handler("h", time.Second), shutdown.First)
			})
		}
		registering.Wait()

		if err := errors.Join(app.runPhases()...); err != nil {
			t.Fatal(err)
		}
		if len(ran.entered) != 16 {
			t.Fatalf("%d handlers ran, want 16", len(ran.entered))
		}
		if ran.most != 16 {
			t.Errorf("FIRST held %d handlers at once, want 16", ran.most)
		}
	})
}
