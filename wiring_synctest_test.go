// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/llingr/anvil/shutdown"
)

// These tests pin §Wiring and the Stop. Wire's ctx is made with the shell, so a stop before wire is
// called, or while it runs, cancels it at once, and a stop after wire returns cancels it once the
// shutdown has run. After a stop during wiring an error from wire that is a clean stop is clean, any
// other is a failure, and no "started" line is logged. Each wire adds one quick handler, consumer,
// to a group named INGRESS.

var (
	sigint = signalStop{signal: syscall.SIGINT}
	sighup = signalStop{signal: syscall.SIGHUP}
)

// errLeaseLost is the error a stop with a reason gives
var errLeaseLost = errors.New("lease lost")

// waitingForWire ends the stopping line of a stop that came before wire returned
const waitingForWire = ", waiting for wire to return"

// wiredLines are the lines from the stopping line on of a shutdown that began with stopping, drained
// first when draining is set, and logged failure when it is set
func wiredLines(stopping, draining, failure string) []string {
	lines := []string{"stopping: " + stopping}
	if draining != "" {
		lines = append(lines, draining)
	}
	lines = append(lines,
		"shutdown group 1 (INGRESS) with consumer",
		"shutdown group 1 (INGRESS): consumer done in 0s",
	)
	if failure != "" {
		lines = append(lines, errorLine("stopped with an error", failure))
	}
	return append(lines, "exiting orders")
}

// wiringStop is a stop, with its stopping line, the drain it gives when it comes as wire returns,
// and the exit code of a shutdown after it in which nothing fails
type wiringStop struct {
	name     string
	reason   error
	stopping string
	draining string
	drain    time.Duration
	code     int
}

// cleanStops are the stops that are clean
var cleanStops = []wiringStop{
	{"Stop(nil)", nil, "Stop called", "", 0, 0},
	{"SIGTERM", sigterm, "terminated signal received", "pausing for 5s before shutdown", 5 * time.Second, 0},
	{"SIGINT", sigint, "interrupt signal received", "", 0, 130},
	{"SIGHUP", sighup, "hangup signal received", "", 0, 129},
	{"Run's ctx cancelled", errRunCtxCancelled, "the ctx passed to Run was cancelled", "", 0, 0},
}

// assertCalledAt checks that the handler was called exactly want after the stop
func assertCalledAt(t *testing.T, called *calls, handler string, stopAt time.Time, want time.Duration) {
	t.Helper()
	calledAt, ok := called.snapshot()[handler]
	switch {
	case !ok:
		t.Errorf("%s never called, want it called %s after the stop", handler, want)
	case calledAt.Sub(stopAt) != want:
		t.Errorf("%s called %s after the stop, want %s", handler, calledAt.Sub(stopAt), want)
	}
}

// assertNotStarted checks that no "started" line was logged
func assertNotStarted(t *testing.T, result ran) {
	t.Helper()
	for _, logged := range result.lines {
		if strings.HasPrefix(logged.msg, "started ") {
			t.Errorf("logged %q after a stop during wiring, want no started line", logged.msg)
		}
	}
}

// A stop that arrives after the shell is built and before wire is called, as a signal or the
// cancellation of Run's ctx can, leaves wire a ctx already cancelled that still carries Run's
// values, and the shutdown runs the handlers wire adds
func TestStopBeforeWireCancelsItsCtx(t *testing.T) {
	for _, stop := range cleanStops {
		t.Run(stop.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				called := newCalls()
				var stopAt time.Time
				var onEntry error
				var value any
				result := bubbledBefore(t, &lifecycle{}, func(s *shell[struct{}, struct{}]) {
					stopAt = time.Now()
					s.requestStop(stop.reason)
				}, func(ctx context.Context, sh bubbleShell) error {
					onEntry, value = ctx.Err(), ctx.Value(runValue{})
					called.add(sh.AddShutdownGroup().SetName("INGRESS"), "consumer", quick)
					<-ctx.Done() // returns at once, or the bubble deadlocks
					return ctx.Err()
				})

				if onEntry != context.Canceled {
					t.Errorf("wire's ctx on entry %v, want %v", onEntry, context.Canceled)
				}
				if value != "run" {
					t.Errorf("wire's ctx carries %v, want Run's value", value)
				}
				if result.code != stop.code {
					t.Errorf("exit code %d, want %d", result.code, stop.code)
				}
				assertLines(t, from(result.lines, "stopping:"), wiredLines(stop.stopping+waitingForWire, stop.draining, "")...)
				assertCalledAt(t, called, "INGRESS consumer", stopAt, stop.drain)
			})
		})
	}
}

// A stop while wire runs cancels wire's ctx before the stop's call returns, so a wire that checks
// its ctx after starting something sees it, and a wire waiting on it wakes at the stop's instant;
// the handler wire added still gets a live ctx of its own
func TestStopDuringWiringCancelsWireCtxAtOnce(t *testing.T) {
	stops := append(slices.Clone(cleanStops),
		wiringStop{"Stop with a reason", fmt.Errorf("Stop called: %w", errLeaseLost), "Stop called: lease lost", "", 0, 1})
	for _, stop := range stops {
		t.Run(stop.name+", checked as the stop returns", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var afterStop, inHandler, handlerOwn error
				result := bubbled(t, func(ctx context.Context, sh bubbleShell) error {
					sh.AddShutdownGroup(shutdown.Named("consumer", shutdown.HandlerFunc(func(handlerCtx context.Context) error {
						inHandler, handlerOwn = ctx.Err(), handlerCtx.Err()
						return nil
					}))).SetName("INGRESS")
					stopNow(sh, stop.reason)
					afterStop = ctx.Err()
					return nil
				})

				if afterStop != context.Canceled {
					t.Errorf("wire's ctx as the stop returned %v, want %v", afterStop, context.Canceled)
				}
				if inHandler != context.Canceled || handlerOwn != nil {
					t.Errorf("in the handler, wire's ctx %v and the handler's %v, want cancelled and live", inHandler, handlerOwn)
				}
				if result.code != stop.code {
					t.Errorf("exit code %d, want %d", result.code, stop.code)
				}
			})
		})
		t.Run(stop.name+", waited on 2s into wiring", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				began := time.Now()
				var woke time.Time
				var stopped <-chan time.Time
				result := bubbled(t, func(ctx context.Context, sh bubbleShell) error {
					sh.AddShutdownGroup(shutdown.Named("consumer", shutdown.HandlerFunc(quick))).SetName("INGRESS")
					stopped = stopAfter(sh, 2*time.Second, stop.reason)
					<-ctx.Done() // a stop that does not cancel it deadlocks the bubble
					woke = time.Now()
					return ctx.Err()
				})

				stopAt := <-stopped
				if stopAt.Sub(began) != 2*time.Second || !woke.Equal(stopAt) {
					t.Errorf("stopped %s and wire woke %s into wiring, want both at 2s", stopAt.Sub(began), woke.Sub(began))
				}
				if result.code != stop.code {
					t.Errorf("exit code %d, want %d", result.code, stop.code)
				}
			})
		})
	}
}

// After a stop during wiring, an error from wire that is a clean stop, by the rule for Go
// functions, is clean: the exit code is the stop's own and no error is logged, so a pod terminated
// during a slow start exits 0
func TestCleanWireErrorAfterAStopIsClean(t *testing.T) {
	returns := []struct {
		name string
		err  func(ctx context.Context) error
	}{
		{"nil", func(context.Context) error {
			return nil
		}},
		{"ctx.Err()", func(ctx context.Context) error {
			return ctx.Err()
		}},
		{"context.Cause", func(ctx context.Context) error {
			return context.Cause(ctx)
		}},
		{"context.Canceled", func(context.Context) error {
			return context.Canceled
		}},
		{"wrapped once", func(ctx context.Context) error {
			return fmt.Errorf("db: %w", ctx.Err())
		}},
		{"wrapped twice", func(ctx context.Context) error {
			return fmt.Errorf("db: %w", fmt.Errorf("dial: %w", ctx.Err()))
		}},
	}
	for _, stop := range cleanStops {
		for _, returned := range returns {
			t.Run(stop.name+", wire returns "+returned.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					result := bubbled(t, func(ctx context.Context, sh bubbleShell) error {
						sh.AddShutdownGroup(shutdown.Named("consumer", shutdown.HandlerFunc(quick))).SetName("INGRESS")
						stopNow(sh, stop.reason)
						return returned.err(ctx)
					})

					if result.code != stop.code {
						t.Errorf("exit code %d, want %d", result.code, stop.code)
					}
					assertLines(t, from(result.lines, "stopping:"), wiredLines(stop.stopping+waitingForWire, stop.draining, "")...)
				})
			})
		}
	}
}

// After a stop during wiring, any other error from wire, and any panic, is a failure, logged after
// the reason of a stop that is itself a failure. A stop with a reason fails on the reason alone, and
// a clean-stop error from wire after it is not reported again.
func TestFailingWireErrorAfterAStopFails(t *testing.T) {
	stopWithReason := fmt.Errorf("Stop called: %w", errLeaseLost)
	cases := []struct {
		name     string
		reason   error
		stopping string
		draining string
		returns  func(ctx context.Context) error
		failure  string // with a panic's stack cut
	}{
		{
			name: "Stop(nil), the cancellation joined with a failure", stopping: "Stop called",
			returns: func(ctx context.Context) error {
				return errors.Join(errors.New("commit failed"), ctx.Err())
			},
			failure: "wiring failed: commit failed\ncontext canceled",
		},
		{
			name: "Stop(nil), an error of its own", stopping: "Stop called",
			returns: func(context.Context) error {
				return errors.New("boom")
			},
			failure: "wiring failed: boom",
		},
		{
			name: "Stop(nil), its own timeout", stopping: "Stop called",
			returns: func(context.Context) error {
				return fmt.Errorf("db: %w", context.DeadlineExceeded)
			},
			failure: "wiring failed: db: context deadline exceeded",
		},
		{
			name: "Stop(nil), an error that is Canceled only by its Is", stopping: "Stop called",
			returns: func(context.Context) error {
				return errMatchesCanceled{}
			},
			failure: "wiring failed: matches context canceled",
		},
		{
			name: "Stop(nil), an error that unwraps to nil", stopping: "Stop called",
			returns: func(context.Context) error {
				return errUnwrapsToNil{}
			},
			failure: "wiring failed: unwraps to nil",
		},
		{
			name: "Stop(nil), a panic", stopping: "Stop called",
			returns: func(context.Context) error {
				panic("kaboom")
			},
			failure: "wiring failed: panic: kaboom",
		},
		{
			name: "Stop(nil), a panic with the ctx's error", stopping: "Stop called",
			returns: func(ctx context.Context) error {
				panic(ctx.Err())
			},
			failure: "wiring failed: panic: context canceled",
		},
		{
			name: "SIGINT, an error of its own", reason: sigint, stopping: "interrupt signal received",
			returns: func(context.Context) error {
				return errors.New("boom")
			},
			failure: "wiring failed: boom",
		},
		{
			name: "SIGTERM, an error of its own, still drains", reason: sigterm, stopping: "terminated signal received",
			draining: "pausing for 5s before shutdown",
			returns: func(context.Context) error {
				return errors.New("boom")
			},
			failure: "wiring failed: boom",
		},
		{
			name: "Stop with a reason, wire's ctx.Err()", reason: stopWithReason, stopping: "Stop called: lease lost",
			returns: func(ctx context.Context) error {
				return ctx.Err()
			},
			failure: "Stop called: lease lost",
		},
		{
			name: "Stop with a reason, an error of its own", reason: stopWithReason, stopping: "Stop called: lease lost",
			returns: func(context.Context) error {
				return errors.New("boom")
			},
			failure: "Stop called: lease lost\nwiring failed: boom",
		},
		{
			name:     "Stop with a reason wrapping Canceled, wire's ctx.Err()",
			reason:   fmt.Errorf("Stop called: %w", fmt.Errorf("consumer loop: %w", context.Canceled)),
			stopping: "Stop called: consumer loop: context canceled",
			returns: func(ctx context.Context) error {
				return ctx.Err()
			},
			failure: "Stop called: consumer loop: context canceled",
		},
		{
			name:     "Run's ctx cancelled with a cause, wire's ctx.Err()",
			reason:   fmt.Errorf("%w: %w", errRunCtxCancelled, errLeaseLost),
			stopping: "the ctx passed to Run was cancelled: lease lost",
			returns: func(ctx context.Context) error {
				return ctx.Err()
			},
			failure: "the ctx passed to Run was cancelled: lease lost",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				result := bubbled(t, func(ctx context.Context, sh bubbleShell) error {
					sh.AddShutdownGroup(shutdown.Named("consumer", shutdown.HandlerFunc(quick))).SetName("INGRESS")
					stopNow(sh, tc.reason)
					return tc.returns(ctx)
				})

				if result.code != 1 {
					t.Errorf("exit code %d, want 1", result.code)
				}
				assertLines(t, from(result.lines, "stopping:"), wiredLines(tc.stopping+waitingForWire, tc.draining, tc.failure)...)
			})
		})
	}
}

// With no stop before wire returns, an error from wire that wraps context.Canceled is a failure
// like any other: it is the stop reason, reported once
func TestCleanWireErrorWithoutAStopFails(t *testing.T) {
	cases := []struct {
		name    string
		returns error
		failure string
	}{
		{"wrapping Canceled", fmt.Errorf("db: %w", context.Canceled), "wiring failed: db: context canceled"},
		{"Canceled", context.Canceled, "wiring failed: context canceled"},
		{"an error of its own", errors.New("boom"), "wiring failed: boom"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					sh.AddShutdownGroup(shutdown.Named("consumer", shutdown.HandlerFunc(quick))).SetName("INGRESS")
					return tc.returns
				})

				if result.code != 1 {
					t.Errorf("exit code %d, want 1", result.code)
				}
				// no stop came during wiring, so wire's ctx is still live and is cancelled before exiting
				want := wiredLines(tc.failure, "", tc.failure)
				want = slices.Insert(want, len(want)-2, cancellingWireCtx)
				assertLines(t, from(result.lines, "stopping:"), want...)
			})
		})
	}
}

// A SIGTERM during wiring drains only what is left of the drain once wire returns: the line names
// the remainder rounded to the millisecond, while the first group begins exactly as the drain ends
func TestSigtermDuringWiringDrainsTheRest(t *testing.T) {
	cases := []struct {
		name     string
		returns  time.Duration // after the stop
		draining string
	}{
		{"2s after the stop", 2 * time.Second, "pausing for 3s before shutdown"},
		{"0.6ms before the drain ends", 5*time.Second - 600*time.Microsecond, "pausing for 1ms before shutdown"},
		{"0.4ms before the drain ends", 5*time.Second - 400*time.Microsecond, "pausing for 0s before shutdown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				called := newCalls()
				var stopped <-chan time.Time
				result := bubbled(t, func(ctx context.Context, sh bubbleShell) error {
					called.add(sh.AddShutdownGroup().SetName("INGRESS"), "consumer", quick)
					stopped = stopAfter(sh, 2*time.Second, sigterm)
					<-ctx.Done()
					time.Sleep(tc.returns)
					return ctx.Err()
				})

				stopAt := <-stopped
				if result.code != 0 {
					t.Errorf("exit code %d, want 0", result.code)
				}
				assertLines(t, from(result.lines, "stopping:"), wiredLines("terminated signal received"+waitingForWire, tc.draining, "")...)
				if tc.draining != "" {
					if at := loggedAt(t, result.lines, tc.draining, stopAt); at != tc.returns {
						t.Errorf("%q logged %s after the stop, want %s, as wire returned", tc.draining, at, tc.returns)
					}
				}
				assertCalledAt(t, called, "INGRESS consumer", stopAt, 5*time.Second)
			})
		})
	}
}

// A wire that returns as the drain ends, or after it, leaves no drain: no line, and the first group
// begins as wire returns. One that returns after the deadline in a service with nothing to run
// leaves nothing running and nothing never called, so the shutdown is clean.
func TestWireReturningAfterTheDrainLeavesNone(t *testing.T) {
	for _, returns := range []time.Duration{5 * time.Second, 6 * time.Second, 20 * time.Second} {
		t.Run(fmt.Sprintf("%s after the stop", returns), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				called := newCalls()
				var stopped <-chan time.Time
				result := bubbled(t, func(ctx context.Context, sh bubbleShell) error {
					called.add(sh.AddShutdownGroup().SetName("INGRESS"), "consumer", quick)
					stopped = stopAfter(sh, 2*time.Second, sigterm)
					<-ctx.Done()
					time.Sleep(returns)
					return ctx.Err()
				})

				stopAt := <-stopped
				if result.code != 0 {
					t.Errorf("exit code %d, want 0", result.code)
				}
				assertLines(t, from(result.lines, "stopping:"), wiredLines("terminated signal received"+waitingForWire, "", "")...)
				assertCalledAt(t, called, "INGRESS consumer", stopAt, returns)
			})
		})
	}
	t.Run("after the deadline, with nothing to run", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var stopped <-chan time.Time
			result := bubbled(t, func(ctx context.Context, sh bubbleShell) error {
				stopped = stopAfter(sh, 2*time.Second, sigterm)
				<-ctx.Done()
				time.Sleep(30 * time.Second)
				return ctx.Err()
			})

			stopAt := <-stopped
			if result.code != 0 {
				t.Errorf("exit code %d, want 0", result.code)
			}
			if ended := result.ended.Sub(stopAt); ended != 30*time.Second {
				t.Errorf("run returned %s after the stop, want 30s, as wire returned", ended)
			}
			assertLines(t, from(result.lines, "stopping:"),
				"stopping: terminated signal received"+waitingForWire,
				"no components are registered for graceful shutdown",
				"exiting orders",
			)
		})
	})
}

// "started" is logged only when wire returned nil and no stop came while it ran, by
// any trigger, at once or while wire waited on its ctx
func TestStartedNotLoggedAfterAStopDuringWiring(t *testing.T) {
	for _, stop := range cleanStops {
		t.Run(stop.name+" as wire runs", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					sh.AddShutdownGroup(shutdown.Named("consumer", shutdown.HandlerFunc(quick))).SetName("INGRESS")
					stopNow(sh, stop.reason)
					return nil
				})
				assertNotStarted(t, result)
			})
		})
		t.Run(stop.name+" as wire waits", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				result := bubbled(t, func(ctx context.Context, sh bubbleShell) error {
					sh.AddShutdownGroup(shutdown.Named("consumer", shutdown.HandlerFunc(quick))).SetName("INGRESS")
					stopAfter(sh, time.Second, stop.reason)
					<-ctx.Done()
					return nil
				})
				assertNotStarted(t, result)
			})
		})
		t.Run(stop.name+" once wire returned", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					sh.AddShutdownGroup(shutdown.Named("consumer", shutdown.HandlerFunc(quick))).SetName("INGRESS")
					stopWhenIdle(sh, stop.reason)
					return nil
				})
				opening := golden(result.lines)
				if len(opening) < 2 || opening[0] != "started orders" || opening[1] != "stopping: "+stop.stopping {
					t.Errorf("lines begin %q, want started orders, then the stop", opening)
				}
				if count := strings.Count(strings.Join(opening, "\n"), "started "); count != 1 {
					t.Errorf("%d started lines, want 1", count)
				}
			})
		})
	}
}

// A stop that comes once wire has returned leaves wire's ctx live through the drain and the groups,
// so what wire built on it keeps working until the handlers have stopped what they own, and cancels
// it once the groups have run
func TestStopJustAfterWireReturnsKeepsWireCtxThroughTheShutdown(t *testing.T) {
	cases := []struct {
		name   string
		reason error
		opts   []Option
		drain  time.Duration
	}{
		{"SIGTERM", sigterm, nil, 5 * time.Second},
		{"SIGTERM with a 3s drain", sigterm, []Option{WithDrainDelay(3 * time.Second)}, 3 * time.Second},
		{"SIGTERM with no drain", sigterm, []Option{WithDrainDelay(0)}, 0},
		{"Stop(nil)", nil, nil, 0},
		{"SIGINT", sigint, nil, 0},
		{"Run's ctx cancelled", errRunCtxCancelled, nil, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cancelled := make(chan time.Time, 1)
				var inHandler, handlerOwn error
				var stopped <-chan time.Time
				result := bubbled(t, func(ctx context.Context, sh bubbleShell) error {
					go func() { // a loop wire built on its ctx
						<-ctx.Done()
						cancelled <- time.Now()
					}()
					sh.AddShutdownGroup(shutdown.Named("consumer", shutdown.HandlerFunc(func(handlerCtx context.Context) error {
						inHandler, handlerOwn = ctx.Err(), handlerCtx.Err()
						time.Sleep(time.Second)
						return nil
					}))).SetName("INGRESS")
					stopped = stopWhenIdle(sh, tc.reason)
					return nil
				}, tc.opts...)

				stopAt := <-stopped
				if at, want := (<-cancelled).Sub(stopAt), tc.drain+time.Second; at != want {
					t.Errorf("wire's ctx cancelled %s after the stop, want %s, once the 1s group has run", at, want)
				}
				if inHandler != nil || handlerOwn != nil {
					t.Errorf("in the handler, wire's ctx %v and the handler's %v, want both live", inHandler, handlerOwn)
				}
				if got := golden(result.lines); len(got) < 1 || got[0] != "started orders" {
					t.Errorf("lines %q, want started orders first", got)
				}
			})
		})
	}
}
