// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package lifecycle_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/llingr/anvil/lifecycle"
	"github.com/llingr/anvil/lifecycle/shutdown"
)

// recorder keeps the start and end time of every handler that ran
type recorder struct {
	mu    sync.Mutex
	spans map[string][2]time.Time
}

func newRecorder() *recorder {
	return &recorder{
		spans: map[string][2]time.Time{},
	}
}

func (r *recorder) handler(name string, delay time.Duration) shutdown.Handler {
	return func(context.Context) error {
		r.mark(name, delay)
		return nil
	}
}

func (r *recorder) mark(name string, delay time.Duration) {
	start := time.Now()
	time.Sleep(delay)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spans[name] = [2]time.Time{start, time.Now()}
}

func (r *recorder) startOf(name string) time.Time {
	return r.spans[name][0]
}

func (r *recorder) endOf(name string) time.Time {
	return r.spans[name][1]
}

type blocking struct {
	release chan struct{}
}

func (b *blocking) Shutdown() error {
	<-b.release
	return nil
}

type contextless struct {
	called bool
}

func (c *contextless) Shutdown() error {
	c.called = true
	return nil
}

// shutdownWithin splits total 50/25/25 across the phases, as the defaults do
func shutdownWithin(total time.Duration) []lifecycle.Option {
	return []lifecycle.Option{
		lifecycle.WithShutdownPhaseBudget(shutdown.First, total*50/100),
		lifecycle.WithShutdownPhaseBudget(shutdown.Default, total*25/100),
		lifecycle.WithShutdownPhaseBudget(shutdown.Last, total*25/100),
	}
}

// run wires, stops at once, and returns the outcome
func run(wiring func(app lifecycle.Application), options ...lifecycle.Option) error {
	return lifecycle.New(options...).Run(func(ctx context.Context, app lifecycle.Application) error {
		wiring(app)
		app.Stop(nil)
		return nil
	})
}

// First and Last run concurrently, Default runs in registration order, phases run in order
func TestPhaseOrderAndConcurrency(t *testing.T) {
	rec := newRecorder()
	err := run(func(app lifecycle.Application) {
		app.RegisterShutdownHandler("l1", rec.handler("l1", 50*time.Millisecond), shutdown.Last)
		app.RegisterShutdownHandler("l2", rec.handler("l2", 50*time.Millisecond), shutdown.Last)
		app.RegisterShutdownHandler("n1", rec.handler("n1", 30*time.Millisecond), shutdown.Default)
		app.RegisterShutdownHandler("n2", rec.handler("n2", 30*time.Millisecond), shutdown.Default)
		app.RegisterShutdownHandler("e1", rec.handler("e1", 50*time.Millisecond), shutdown.First)
		app.RegisterShutdownHandler("e2", rec.handler("e2", 50*time.Millisecond), shutdown.First)
	}, shutdownWithin(5*time.Second)...)

	if err != nil {
		t.Fatalf("clean shutdownHandler returned %v", err)
	}
	if len(rec.spans) != 6 {
		t.Fatalf("%d handlers ran, want 6", len(rec.spans))
	}
	if gap := rec.startOf("e2").Sub(rec.startOf("e1")).Abs(); gap > 20*time.Millisecond {
		t.Errorf("early handlers started %s apart, want concurrent", gap)
	}
	if gap := rec.startOf("l2").Sub(rec.startOf("l1")).Abs(); gap > 20*time.Millisecond {
		t.Errorf("late handlers started %s apart, want concurrent", gap)
	}
	if rec.startOf("n1").Before(rec.endOf("e1")) || rec.startOf("n1").Before(rec.endOf("e2")) {
		t.Error("normal started before early finished")
	}
	if rec.startOf("n2").Before(rec.endOf("n1")) {
		t.Error("n2 started before n1 finished, want sequential")
	}
	if rec.startOf("l1").Before(rec.endOf("n2")) {
		t.Error("late started before normal finished")
	}
}

// Each phase ends at the running total of budgets, so time First does not use reaches Default and Last
func TestBudgetRollsForward(t *testing.T) {
	var defaultBudget, lastBudget time.Duration
	capture := func(into *time.Duration) shutdown.Handler {
		return func(ctx context.Context) error {
			deadline, _ := ctx.Deadline()
			*into = time.Until(deadline)
			return nil
		}
	}
	err := run(func(app lifecycle.Application) {
		app.RegisterShutdownHandler("early", newRecorder().handler("early", 300*time.Millisecond), shutdown.First)
		app.RegisterShutdownHandler("default budget", capture(&defaultBudget), shutdown.Default)
		app.RegisterShutdownHandler("last budget", capture(&lastBudget), shutdown.Last)
	}, shutdownWithin(3*time.Second)...)
	if err != nil {
		t.Fatal(err)
	}

	within := func(name string, got, want time.Duration) {
		t.Helper()
		if (got - want).Abs() > 150*time.Millisecond {
			t.Errorf("%s budget %s, want about %s", name, got, want)
		}
	}
	within("default", defaultBudget, 1950*time.Millisecond) // 1.5s first + 750ms default, less 300ms used
	within("last", lastBudget, 2700*time.Millisecond)       // all 3s less 300ms, Default spent none
}

// A contextless handler still running at the deadline fails and is left behind
func TestDetachesAtDeadline(t *testing.T) {
	stuck := &blocking{
		release: make(chan struct{}),
	}
	defer close(stuck.release)

	start := time.Now()
	err := run(func(app lifecycle.Application) {
		app.RegisterShutdownHandler("stuck", shutdown.IgnoreContext(stuck.Shutdown), shutdown.Default)
	}, shutdownWithin(300*time.Millisecond)...)

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Run took %s, want return at the deadline", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "DEFAULT stuck") {
		t.Fatalf("error %v, want deadline exceeded naming the handler", err)
	}
}

// A panicking handler becomes an error and the next one still runs
func TestPanicRecovered(t *testing.T) {
	ran := false
	err := run(func(app lifecycle.Application) {
		app.RegisterShutdownHandler("bad", func(context.Context) error {
			panic("kaboom")
		}, shutdown.Default)
		app.RegisterShutdownHandler("good", func(context.Context) error {
			ran = true
			return nil
		}, shutdown.Default)
	})

	if err == nil || !strings.Contains(err.Error(), "DEFAULT bad: panic: kaboom") {
		t.Fatalf("panic reported as %v", err)
	}
	if !ran {
		t.Fatal("handler after the panic did not run")
	}
}

// Stop's reason is delivered; the first call wins and nil is clean
func TestStop(t *testing.T) {
	boom := errors.New("boom")
	err := run(func(app lifecycle.Application) {
		app.Stop(boom)
		app.Stop(errors.New("late"))
	})
	if !errors.Is(err, boom) || strings.Contains(err.Error(), "late") {
		t.Fatalf("error %v, want boom alone", err)
	}

	if err = run(func(lifecycle.Application) {}); err != nil {
		t.Fatalf("Stop(nil) returned %v", err)
	}
}

// A reason that wraps context.Canceled is still a reason: a consumer loop handing its own
// cancellation back must not be read as a clean stop
func TestStopReasonWrappingCancelledIsReported(t *testing.T) {
	reason := fmt.Errorf("consumer loop: %w", context.Canceled)
	err := run(func(app lifecycle.Application) {
		app.Stop(reason)
	})
	if !errors.Is(err, reason) {
		t.Fatalf("error %v, want the wrapped reason", err)
	}
	if !strings.Contains(err.Error(), "consumer loop") {
		t.Fatalf("error %v, want the reason's own text", err)
	}
}

// Only the sentinel itself is the clean stop, as Stop(nil) and a signal both raise
func TestStopReasonCancelledSentinelIsClean(t *testing.T) {
	err := run(func(app lifecycle.Application) {
		app.Stop(context.Canceled)
	})
	if err != nil {
		t.Fatalf("Stop(context.Canceled) returned %v, want a clean stop", err)
	}
}

// Stop never blocks its caller, whether shutdownHandler has yet to start or has already finished
func TestStopDoesNotBlock(t *testing.T) {
	app := lifecycle.New()
	err := app.Run(func(ctx context.Context, app lifecycle.Application) error {
		return errors.New("wiring failed") // Run's own Stop, before shutdownHandler receives
	})
	if err == nil {
		t.Fatal("wiring error not delivered")
	}
	returned := make(chan struct{})
	go func() {
		app.Stop(nil)
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("Stop after shutdownHandler blocked")
	}
}

// SIGINT and SIGTERM stay trapped when WithSignals adds another
func TestDefaultSignalsSurviveWithSignals(t *testing.T) {
	plain := &contextless{}
	app := lifecycle.New(lifecycle.WithSignals(syscall.SIGUSR1))
	err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
	if err != nil {
		t.Fatal(err)
	}
	err = app.Run(func(ctx context.Context, app lifecycle.Application) error {
		app.RegisterShutdownHandler("plain", shutdown.IgnoreContext(plain.Shutdown), shutdown.Default)
		return nil
	})
	if err != nil || !plain.called {
		t.Fatalf("error %v, called %v", err, plain.called)
	}
}

// A wiring error starts shutdownHandler, runs what was registered, and is delivered
func TestWiringError(t *testing.T) {
	boom := errors.New("boom")
	plain := &contextless{}
	err := lifecycle.New().Run(func(ctx context.Context, app lifecycle.Application) error {
		app.RegisterShutdownHandler("plain", shutdown.IgnoreContext(plain.Shutdown), shutdown.Default)
		return boom
	})
	if !errors.Is(err, boom) || !plain.called {
		t.Fatalf("error %v, handler called %v", err, plain.called)
	}
}

// The wiring's context outlives the handlers and is cancelled once they are done
func TestWiringContext(t *testing.T) {
	var wiringCtx context.Context
	var duringShutdown error
	err := lifecycle.New().Run(func(ctx context.Context, app lifecycle.Application) error {
		wiringCtx = ctx
		app.RegisterShutdownHandler("observe", func(context.Context) error {
			duringShutdown = ctx.Err()
			return nil
		}, shutdown.Default)
		app.Stop(nil)
		return nil
	})
	if err != nil || duringShutdown != nil {
		t.Fatalf("error %v, ctx during shutdownHandler %v", err, duringShutdown)
	}
	if wiringCtx.Err() == nil {
		t.Fatal("wiring context not cancelled after shutdownHandler")
	}
}

// A trapped signal triggers shutdownHandler, and one arriving before Run is not lost
func TestSignalTrigger(t *testing.T) {
	plain := &contextless{}
	app := lifecycle.New(lifecycle.WithSignals(syscall.SIGUSR1))
	err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
	if err != nil {
		t.Fatal(err)
	}
	err = app.Run(func(ctx context.Context, app lifecycle.Application) error {
		app.RegisterShutdownHandler("plain", shutdown.IgnoreContext(plain.Shutdown), shutdown.Default)
		return nil
	})
	if err != nil || !plain.called {
		t.Fatalf("error %v, called %v", err, plain.called)
	}
}

// A context-free Shutdown runs through IgnoreContext, and a closure is a Handler as it is
func TestShapes(t *testing.T) {
	plain := &contextless{}
	ran := false
	err := run(func(app lifecycle.Application) {
		app.RegisterShutdownHandler("plain", shutdown.IgnoreContext(plain.Shutdown), shutdown.Default)
		app.RegisterShutdownHandler("flush", func(context.Context) error {
			ran = true
			return nil
		}, shutdown.Default)
	})
	if err != nil || !plain.called || !ran {
		t.Fatalf("error %v, contextless %v, func %v", err, plain.called, ran)
	}
}

// Wiring mistakes fail at the point of the mistake
func TestRegisterPanics(t *testing.T) {
	expectPanic := func(name string, do func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s did not panic", name)
			}
		}()
		do()
	}
	var wired lifecycle.Application
	app := lifecycle.New()
	err := app.Run(func(ctx context.Context, app lifecycle.Application) error {
		wired = app
		expectPanic("empty name", func() {
			app.RegisterShutdownHandler("", shutdown.IgnoreContext((&contextless{}).Shutdown), shutdown.Default)
		})
		expectPanic("nil handler", func() {
			app.RegisterShutdownHandler("nil", nil, shutdown.Default)
		})
		expectPanic("unknown phase", func() {
			app.RegisterShutdownHandler("contextless", shutdown.IgnoreContext((&contextless{}).Shutdown), shutdown.Phase("early"))
		})
		app.Stop(nil)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	expectPanic("after wiring", func() {
		wired.RegisterShutdownHandler("contextless", shutdown.IgnoreContext((&contextless{}).Shutdown), shutdown.Default)
	})
	expectPanic("Run twice", func() {
		_ = app.Run(func(context.Context, lifecycle.Application) error {
			return nil
		})
	})
}

// Concurrent Run calls race for the running flag: exactly one wins, the other panics
func TestRunConcurrentlyPanicsOnce(t *testing.T) {
	app := lifecycle.New()
	start := make(chan struct{})
	outcomes := make(chan error, 2)
	var panics atomic.Int32
	var callers sync.WaitGroup
	for range 2 {
		callers.Go(func() {
			defer func() {
				if recover() != nil {
					panics.Add(1)
				}
			}()
			<-start
			outcomes <- app.Run(func(ctx context.Context, app lifecycle.Application) error {
				app.Stop(nil)
				return nil
			})
		})
	}
	close(start)
	callers.Wait()
	close(outcomes)
	if panics.Load() != 1 {
		t.Fatalf("%d Run calls panicked, want 1", panics.Load())
	}
	for err := range outcomes {
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Registration from another goroutine sees wiring return, without synchronising with Run
func TestRegisterFromAnotherGoroutinePanicsOnceWiringReturns(t *testing.T) {
	registerPanics := func(app lifecycle.Application) (panicked bool) {
		defer func() {
			panicked = recover() != nil
		}()
		app.RegisterShutdownHandler("late", func(context.Context) error {
			return nil
		}, shutdown.Default)
		return false
	}
	app := lifecycle.New()
	closed := make(chan bool, 1)
	outcome := make(chan error, 1)
	go func() {
		outcome <- app.Run(func(ctx context.Context, app lifecycle.Application) error {
			go func() {
				giveUp := time.Now().Add(time.Second)
				for time.Now().Before(giveUp) {
					if registerPanics(app) {
						closed <- true
						return
					}
				}
				closed <- false
			}()
			return nil
		})
	}()
	if !<-closed {
		t.Fatal("registration never closed after wiring returned")
	}
	app.Stop(nil)
	if err := <-outcome; err != nil {
		t.Fatal(err)
	}
}
