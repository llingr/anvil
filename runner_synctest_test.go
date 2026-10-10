// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/llingr/anvil/shutdown"
)

// These tests pin how call records each handler and what the records decide: who is called once
// the deadline has expired, the deadline error, the order of the joined error and the exit code. In
// a bubble a handler that returns at the deadline wakes with the ctx's timer, in either order, and
// every test here holds in both.

// sleepsThen is a handler that ignores its ctx, sleeps for delay, then returns err
func sleepsThen(delay time.Duration, err error) shutdown.HandlerFunc {
	return func(context.Context) error {
		time.Sleep(delay)
		return err
	}
}

// assertDeadlineLast checks that the last line of the shutdown's error is the deadline error want,
// and that no handler is reported as failing with its ctx's error
func assertDeadlineLast(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Errorf("no error, want it to end with %q", want)
		return
	}
	lines := strings.Split(err.Error(), "\n")
	if last := lines[len(lines)-1]; last != want {
		t.Errorf("error ends with %q, want %q:\n%v", last, want, err)
	}
	if strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Errorf("error %q reports a ctx's deadline as a failure, want the handler named as running", err)
	}
}

// assertCalledOnly checks that the handlers called are exactly want, each before the deadline
func assertCalledOnly(t *testing.T, called *calls, stopAt time.Time, deadline time.Duration, want ...string) {
	t.Helper()
	at := called.snapshot()
	for handler, calledAt := range at {
		if !slices.Contains(want, handler) {
			t.Errorf("%s called %s after the stop, want it never called", handler, calledAt.Sub(stopAt))
		}
		if !calledAt.Before(stopAt.Add(deadline)) {
			t.Errorf("%s called %s after the stop, at or after the %s deadline", handler, calledAt.Sub(stopAt), deadline)
		}
	}
	for _, handler := range want {
		if _, ok := at[handler]; !ok {
			t.Errorf("%s never called, want it called", handler)
		}
	}
}

// assertFailure checks the error stopped with an error carried, or that none was logged for ""
func assertFailure(t *testing.T, result ran, want string) {
	t.Helper()
	err := result.failure()
	switch {
	case want == "" && err != nil:
		t.Errorf("stopped with an error %q, want a clean shutdown", err)
	case want != "" && err == nil:
		t.Errorf("no error, want %q: %q", want, result.messages())
	case want != "" && err.Error() != want:
		t.Errorf("stopped with an error\n%v\nwant\n%s", err, want)
	}
}

// A handler still running at the deadline keeps its group from finishing, so no group due to stop
// after it begins: their handlers are never called and they log no begin line, while the hung
// group's other handlers are called
func TestHandlersAfterAHangAreNeverCalled(t *testing.T) {
	cases := []struct {
		name    string
		add     func(sh bubbleShell, called *calls, release <-chan struct{})
		called  []string
		hung    string // the label of the group that hangs, the only one to log a begin line
		running string
	}{
		{
			name: "groups of one",
			add: func(sh bubbleShell, called *calls, release <-chan struct{}) {
				called.add(sh.AddShutdownGroup().SetName("EGRESS"), "pool", quick)
				called.add(sh.AddShutdownGroup().SetName("LEDGER"), "ledger", quick)
				called.add(sh.AddShutdownGroup().SetName("ORDERS"), "orders", quick)
				called.add(sh.AddShutdownGroup().SetName("OUTBOX"), "outbox", ignoring(release))
			},
			called:  []string{"OUTBOX outbox"},
			hung:    "group 4 (OUTBOX)",
			running: "group 4 (OUTBOX) outbox",
		},
		{
			name: "a hang beside a handler that returns",
			add: func(sh bubbleShell, called *calls, release <-chan struct{}) {
				core := sh.AddShutdownGroup().SetName("CORE")
				called.add(core, "ledger", quick)
				called.add(core, "outbox", quick)
				ingress := sh.AddShutdownGroup().SetName("INGRESS")
				called.add(ingress, "stuck", ignoring(release))
				called.add(ingress, "listener", quick)
			},
			called:  []string{"INGRESS stuck", "INGRESS listener"},
			hung:    "group 2 (INGRESS)",
			running: "group 2 (INGRESS) stuck",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				defer synctest.Wait()
				defer close(release)

				called := newCalls()
				var stopped <-chan time.Time
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					tc.add(sh, called, release)
					stopped = stopWhenIdle(sh, nil)
					return nil
				})

				stopAt := <-stopped
				assertCalledOnly(t, called, stopAt, 28*time.Second, tc.called...)
				if elapsed := result.ended.Sub(stopAt); elapsed != 28*time.Second {
					t.Errorf("run returned %s after the stop, want the 28s deadline", elapsed)
				}
				if result.code != 1 {
					t.Errorf("exit code %d, want 1", result.code)
				}
				assertFailure(t, result, "shutdown deadline 28s passed: "+tc.running)
				for _, logged := range result.lines {
					if strings.HasPrefix(logged.msg, "shutdown group ") && strings.Contains(logged.msg, " with ") &&
						!strings.HasPrefix(logged.msg, "shutdown "+tc.hung+" ") {
						t.Errorf("logged %q, want no group begun after the one that hung", logged.msg)
					}
				}
			})
		})
	}
}

// A handler that returns exactly at the deadline is recorded as running, whatever it returned and
// whichever of its return and the ctx's end the runner saw first: the deadline has expired at that
// instant, so it did not return before it. In the last group, nothing else is unfinished, so only
// its record makes the shutdown fail.
func TestHandlerReturningAtTheDeadlineIsRunning(t *testing.T) {
	late := errors.New("closed late")
	cases := []struct {
		name   string
		reason error
		add    func(sh bubbleShell, called *calls)
		want   string
	}{
		{
			name: "alone, returning nil",
			add: func(sh bubbleShell, called *calls) {
				called.add(sh.AddShutdownGroup().SetName("CORE"), "ledger", sleepsThen(28*time.Second, nil))
			},
			want: "shutdown deadline 28s passed: group 1 (CORE) ledger",
		},
		{
			name: "alone, returning an error",
			add: func(sh bubbleShell, called *calls) {
				called.add(sh.AddShutdownGroup().SetName("CORE"), "ledger", sleepsThen(28*time.Second, late))
			},
			want: "shutdown deadline 28s passed: group 1 (CORE) ledger",
		},
		{
			name: "beside one that finished",
			add: func(sh bubbleShell, called *calls) {
				egress := sh.AddShutdownGroup().SetName("EGRESS")
				called.add(egress, "kafka", sleepsThen(28*time.Second, late))
				called.add(egress, "pool", quick)
			},
			want: "shutdown deadline 28s passed: group 1 (EGRESS) kafka",
		},
		{
			name:   "called as the drain ends",
			reason: sigterm,
			add: func(sh bubbleShell, called *calls) {
				called.add(sh.AddShutdownGroup().SetName("EGRESS"), "kafka", sleepsThen(23*time.Second, nil))
			},
			want: "shutdown deadline 28s passed: group 1 (EGRESS) kafka",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				called := newCalls()
				var stopped <-chan time.Time
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					tc.add(sh, called)
					stopped = stopWhenIdle(sh, tc.reason)
					return nil
				})

				stopAt := <-stopped
				if elapsed := result.ended.Sub(stopAt); elapsed != 28*time.Second {
					t.Errorf("run returned %s after the stop, want the 28s deadline", elapsed)
				}
				if result.code != 1 {
					t.Errorf("exit code %d, want 1", result.code)
				}
				assertFailure(t, result, tc.want)
			})
		})
	}
}

// A handler that returns exactly at the deadline leaves the handlers of the group due to stop next
// never called, whether it was alone in its group or beside another: the deadline has expired by
// the time the runner reaches that group
func TestNextGroupIsNotCalledAtTheDeadline(t *testing.T) {
	cases := []struct {
		name    string
		add     func(sh bubbleShell, called *calls)
		called  []string
		running string
	}{
		{
			name: "after a group of one",
			add: func(sh bubbleShell, called *calls) {
				egress := sh.AddShutdownGroup().SetName("EGRESS")
				called.add(egress, "kafka", quick)
				called.add(egress, "pool", quick)
				called.add(sh.AddShutdownGroup().SetName("CORE"), "ledger", sleepsThen(28*time.Second, nil))
			},
			called:  []string{"CORE ledger"},
			running: "group 2 (CORE) ledger",
		},
		{
			name: "after a group of two",
			add: func(sh bubbleShell, called *calls) {
				called.add(sh.AddShutdownGroup().SetName("CORE"), "ledger", quick)
				ingress := sh.AddShutdownGroup().SetName("INGRESS")
				called.add(ingress, "consumer", sleepsThen(28*time.Second, nil))
				called.add(ingress, "listener", quick)
			},
			called:  []string{"INGRESS consumer", "INGRESS listener"},
			running: "group 2 (INGRESS) consumer",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				called := newCalls()
				var stopped <-chan time.Time
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					tc.add(sh, called)
					stopped = stopWhenIdle(sh, nil)
					return nil
				})

				stopAt := <-stopped
				assertCalledOnly(t, called, stopAt, 28*time.Second, tc.called...)
				if result.code != 1 {
					t.Errorf("exit code %d, want 1", result.code)
				}
				assertFailure(t, result, "shutdown deadline 28s passed: "+tc.running)
			})
		})
	}
}

// A handler that returns a nanosecond before the deadline is recorded as finished, with its error,
// and the group due to stop next is still called; with no error the shutdown is clean
func TestHandlerReturningBeforeTheDeadlineIsRecorded(t *testing.T) {
	const justBefore = 28*time.Second - time.Nanosecond
	cases := []struct {
		name     string
		returned error
		code     int
		want     string
	}{
		{"with an error", errors.New("flush failed"), 1, "group 2 (CORE) ledger: flush failed"},
		{"with no error", nil, 0, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				called := newCalls()
				var stopped <-chan time.Time
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					called.add(sh.AddShutdownGroup().SetName("OUTBOX"), "outbox", quick)
					called.add(sh.AddShutdownGroup().SetName("CORE"), "ledger", sleepsThen(justBefore, tc.returned))
					stopped = stopWhenIdle(sh, nil)
					return nil
				})

				stopAt := <-stopped
				assertCalledOnly(t, called, stopAt, 28*time.Second, "CORE ledger", "OUTBOX outbox")
				if at := called.snapshot()["OUTBOX outbox"]; at.Sub(stopAt) != justBefore {
					t.Errorf("OUTBOX outbox called %s after the stop, want %s", at.Sub(stopAt), justBefore)
				}
				if elapsed := result.ended.Sub(stopAt); elapsed != justBefore {
					t.Errorf("run returned %s after the stop, want %s", elapsed, justBefore)
				}
				if result.code != tc.code {
					t.Errorf("exit code %d, want %d", result.code, tc.code)
				}
				assertFailure(t, result, tc.want)
			})
		})
	}
}

// A handler that respects its ctx returns its ctx's error as the deadline expires it, and is named
// as the handler running at the deadline, never reported as failing with that error: plainly,
// wrapped, or as the handler Go adds for a function that ignores its ctx
func TestHandlerHonouringItsCtxIsNamedNotFailed(t *testing.T) {
	cases := []struct {
		name    string
		handler string // as calls records it
		add     func(group ShutdownGroup, called *calls, release <-chan struct{})
	}{
		{
			name:    "returning its ctx's error",
			handler: "consumer",
			add: func(group ShutdownGroup, called *calls, _ <-chan struct{}) {
				called.add(group, "consumer", patient)
			},
		},
		{
			name:    "returning its ctx's error wrapped",
			handler: "consumer",
			add: func(group ShutdownGroup, called *calls, _ <-chan struct{}) {
				called.add(group, "consumer", func(ctx context.Context) error {
					<-ctx.Done()
					return fmt.Errorf("commit: %w", ctx.Err())
				})
			},
		},
		{
			name:    "the handler Go adds",
			handler: goName,
			add: func(group ShutdownGroup, called *calls, release <-chan struct{}) {
				called.goIgnoring(group, release)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				defer synctest.Wait()
				defer close(release)

				called := newCalls()
				var stopped <-chan time.Time
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					called.add(sh.AddShutdownGroup().SetName("CORE"), "ledger", quick)
					tc.add(sh.AddShutdownGroup().SetName("INGRESS"), called, release)
					stopped = stopWhenIdle(sh, sigterm)
					return nil
				})
				synctest.Wait() // the Go function told to stop has recorded it

				stopAt := <-stopped
				assertCalledOnly(t, called, stopAt, 28*time.Second, "INGRESS "+tc.handler)
				if result.code != 1 {
					t.Errorf("exit code %d, want 1", result.code)
				}
				assertFailure(t, result, "shutdown deadline 28s passed: group 2 (INGRESS) "+tc.handler)
			})
		})
	}
}

// Every handler returning before the deadline is a clean shutdown even when the logger, writing a
// line once the handlers have returned, holds Run past the deadline: the records decide, not the
// clock
func TestSlowLoggerDoesNotFailACleanShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := &lifecycle{slow: func(msg string) {
			if msg == cancellingWireCtx {
				time.Sleep(30 * time.Second)
			}
		}}
		var stopped <-chan time.Time
		result := bubbledWith(t, logger, func(_ context.Context, sh bubbleShell) error {
			sh.AddShutdownGroup(shutdown.Named("consumer", shutdown.HandlerFunc(quick))).SetName("INGRESS")
			sh.AddShutdownGroup(shutdown.Named("ledger", shutdown.HandlerFunc(quick))).SetName("CORE")
			sh.AddShutdownGroup(shutdown.Named("pool", shutdown.HandlerFunc(quick))).SetName("EGRESS")
			stopped = stopWhenIdle(sh, nil)
			return nil
		})

		if elapsed := result.ended.Sub(<-stopped); elapsed != 30*time.Second {
			t.Errorf("run returned %s after the stop, want 30s, held by the logger", elapsed)
		}
		if result.code != 0 {
			t.Errorf("exit code %d, want 0", result.code)
		}
		assertFailure(t, result, "")
		for _, logged := range result.lines {
			if logged.err != nil {
				t.Errorf("logged %q: %v, want no error", logged.msg, logged.err)
			}
		}
	})
}

// A logger that holds a group's begin line, logged a nanosecond before the deadline, until the
// deadline leaves that group's handlers never called, with every handler called finished: the
// deadline error names the first never called
func TestSlowLoggerBetweenGroupsLeavesTheNextNeverCalled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := &lifecycle{slow: func(msg string) {
			if msg == "shutdown group 2 (CORE) with ledger" {
				time.Sleep(time.Nanosecond)
			}
		}}
		called := newCalls()
		var stopped <-chan time.Time
		result := bubbledWith(t, logger, func(_ context.Context, sh bubbleShell) error {
			called.add(sh.AddShutdownGroup().SetName("EGRESS"), "pool", quick)
			called.add(sh.AddShutdownGroup().SetName("CORE"), "ledger", quick)
			called.add(sh.AddShutdownGroup().SetName("INGRESS"), "consumer", sleepsThen(28*time.Second-time.Nanosecond, nil))
			stopped = stopWhenIdle(sh, nil)
			return nil
		})

		stopAt := <-stopped
		assertCalledOnly(t, called, stopAt, 28*time.Second, "INGRESS consumer")
		if elapsed := result.ended.Sub(stopAt); elapsed != 28*time.Second {
			t.Errorf("run returned %s after the stop, want the 28s deadline", elapsed)
		}
		if result.code != 1 {
			t.Errorf("exit code %d, want 1", result.code)
		}
		assertFailure(t, result, "shutdown deadline 28s passed before group 2 (CORE) ledger")
	})
}

// The shutdown's error is joined in time order: the stop reason when it is an error, then wire's
// error unless it is the reason, then the handlers' errors in the order their groups stop and,
// within a group, the order added, whenever they failed, then the deadline error
func TestErrorsJoinInTimeOrder(t *testing.T) {
	handlers := []string{
		"group 3 (INGRESS) first: first broke",   // fails 2s in
		"group 3 (INGRESS) second: second broke", // fails 1s in, before first
		"group 2 (CORE) ledger: ledger broke",
		"shutdown deadline 28s passed: group 2 (CORE) outbox",
	}
	cases := []struct {
		name string
		stop func(sh bubbleShell) // during wiring, nil for a SIGTERM once idle
		wire error
		want []string
	}{
		{
			name: "Stop with a reason, then wire fails",
			stop: func(sh bubbleShell) {
				sh.Stop(errors.New("lease lost"))
			},
			wire: errors.New("boom"),
			want: append([]string{"Stop called: lease lost", "wiring failed: boom"}, handlers...),
		},
		{
			name: "wire fails, its error the reason",
			wire: errors.New("boom"),
			want: append([]string{"wiring failed: boom"}, handlers...),
		},
		{
			name: "a clean Stop, then wire fails",
			stop: func(sh bubbleShell) {
				sh.Stop(nil)
			},
			wire: errors.New("boom"),
			want: append([]string{"wiring failed: boom"}, handlers...),
		},
		{
			name: "a SIGTERM",
			want: handlers,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				defer synctest.Wait()
				defer close(release)

				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					if tc.stop != nil {
						tc.stop(sh)
					}
					sh.AddShutdownGroup(shutdown.Named("pool", shutdown.HandlerFunc(quick))).SetName("EGRESS")
					sh.AddShutdownGroup(
						shutdown.Named("ledger", sleepsThen(0, errors.New("ledger broke"))),
						shutdown.Named("outbox", ignoring(release)),
					).SetName("CORE")
					sh.AddShutdownGroup(
						shutdown.Named("first", sleepsThen(2*time.Second, errors.New("first broke"))),
						shutdown.Named("second", sleepsThen(time.Second, errors.New("second broke"))),
					).SetName("INGRESS")
					if tc.stop == nil && tc.wire == nil {
						stopWhenIdle(sh, sigterm)
					}
					return tc.wire
				})

				if result.code != 1 {
					t.Errorf("exit code %d, want 1", result.code)
				}
				assertFailure(t, result, strings.Join(tc.want, "\n"))
			})
		})
	}
}

// A panic anvil recovers reads "panic: <value>" and then the stack, from one helper, wherever it
// is recovered: a handler, a Go function before any stop and after the stop before its group
// begins, and wire. Each is reported once.
func TestPanicsUseOneForm(t *testing.T) {
	nilPanic := "panic: " + new(runtime.PanicNilError).Error() // the value panic(nil) raises
	cases := []struct {
		name string
		wire func(sh bubbleShell) error
		want string // the panic's report, followed by the stack
	}{
		{
			name: "a handler",
			wire: func(sh bubbleShell) error {
				sh.AddShutdownGroup(shutdown.Named("ledger", shutdown.HandlerFunc(func(context.Context) error {
					panic("kaboom")
				}))).SetName("CORE")
				stopWhenIdle(sh, nil)
				return nil
			},
			want: "group 1 (CORE) ledger: panic: kaboom",
		},
		{
			name: "a handler panicking with nil",
			wire: func(sh bubbleShell) error {
				sh.AddShutdownGroup(shutdown.Named("ledger", shutdown.HandlerFunc(func(context.Context) error {
					panic(nil)
				}))).SetName("CORE")
				stopWhenIdle(sh, nil)
				return nil
			},
			want: "group 1 (CORE) ledger: " + nilPanic,
		},
		{
			name: "a Go function before any stop",
			wire: func(sh bubbleShell) error {
				sh.AddShutdownGroup().SetName("INGRESS").Go(func(context.Context) error {
					panic("kaboom")
				})
				return nil
			},
			want: "group 1 (INGRESS): panic: kaboom",
		},
		{
			name: "a Go function after the stop, before its group begins",
			wire: func(sh bubbleShell) error {
				sh.AddShutdownGroup().SetName("INGRESS").Go(func(context.Context) error {
					for !sh.Stopping() {
						time.Sleep(time.Second)
					}
					panic("kaboom")
				})
				stopWhenIdle(sh, sigterm)
				return nil
			},
			want: "group 1 (INGRESS) " + goName + ": panic: kaboom",
		},
		{
			name: "wire",
			wire: func(bubbleShell) error {
				panic("kaboom")
			},
			want: "wiring failed: panic: kaboom",
		},
		{
			name: "wire panicking with nil",
			wire: func(bubbleShell) error {
				panic(nil)
			},
			want: "wiring failed: " + nilPanic,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					return tc.wire(sh)
				})

				if result.code != 1 {
					t.Errorf("exit code %d, want 1", result.code)
				}
				err := result.failure()
				if err == nil {
					t.Fatalf("no error, want %q", tc.want)
				}
				text := err.Error()
				if !strings.HasPrefix(text, tc.want+"\ngoroutine ") {
					t.Errorf("error\n%s\nwant it to begin %q then the stack", text, tc.want)
				}
				if count := strings.Count(text, tc.want); count != 1 {
					t.Errorf("error\n%s\nreports the panic %d times, want once", text, count)
				}
				if strings.Contains(text, "panicked") {
					t.Errorf("error\n%s\nwant the one form, not panicked", text)
				}
			})
		})
	}
}

// A handler that calls runtime.Goexit, as t.FailNow does, sends no result, so it is recorded as
// running until the deadline and the groups due to stop after it are never called
func TestGoexitIsRunningUntilTheDeadline(t *testing.T) {
	quits := func(context.Context) error {
		runtime.Goexit()
		return nil
	}
	cases := []struct {
		name    string
		add     func(sh bubbleShell, called *calls)
		reached []string // every handler called
		running string
	}{
		{
			name: "alone in its group",
			add: func(sh bubbleShell, called *calls) {
				called.add(sh.AddShutdownGroup().SetName("AFTER"), "after", quick)
				called.add(sh.AddShutdownGroup().SetName("CORE"), "quits", quits)
			},
			reached: []string{"CORE quits"},
			running: "group 3 (CORE) quits",
		},
		{
			name: "beside another",
			add: func(sh bubbleShell, called *calls) {
				called.add(sh.AddShutdownGroup().SetName("CORE"), "after", quick)
				ingress := sh.AddShutdownGroup().SetName("INGRESS")
				called.add(ingress, "quits", quits)
				called.add(ingress, "beside", quick)
			},
			reached: []string{"INGRESS quits", "INGRESS beside"},
			running: "group 3 (INGRESS) quits",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				called := newCalls()
				var stopped <-chan time.Time
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					called.add(sh.AddShutdownGroup().SetName("EGRESS"), "pool", quick)
					tc.add(sh, called)
					stopped = stopWhenIdle(sh, nil)
					return nil
				})

				stopAt := <-stopped
				assertCalledOnly(t, called, stopAt, 28*time.Second, tc.reached...)
				if elapsed := result.ended.Sub(stopAt); elapsed != 28*time.Second {
					t.Errorf("run returned %s after the stop, want the 28s deadline", elapsed)
				}
				if result.code != 1 {
					t.Errorf("exit code %d, want 1", result.code)
				}
				assertFailure(t, result, "shutdown deadline 28s passed: "+tc.running)
			})
		})
	}
}

// When wire returns after the deadline has passed no handler is called, and the deadline error names
// the first handler never called in the order the groups stop, skipping groups with no handlers; a
// wire returning a nanosecond before the deadline leaves time to call the first group to stop, and
// a service with no handlers at all leaves nothing unfinished
func TestLateWireNamesTheFirstHandlerNeverCalled(t *testing.T) {
	cases := []struct {
		name   string
		late   time.Duration // how long after the stop wire returns
		add    func(sh bubbleShell, called *calls)
		called []string
		code   int
		want   string
	}{
		{
			name: "the first three groups to stop empty",
			late: 29 * time.Second,
			add: func(sh bubbleShell, called *calls) {
				ledger := sh.AddShutdownGroup().SetName("LEDGER")
				called.add(ledger, "outbox", quick)
				called.add(ledger, "ledger", quick)
				sh.AddShutdownGroup().SetName("DB_POOLS")
				sh.AddShutdownGroup().SetName("PAYMENTS")
				sh.AddShutdownGroup().SetName("INGRESS_HTTP")
			},
			code: 1,
			want: "shutdown deadline 28s passed before group 1 (LEDGER) outbox",
		},
		{
			name: "the first group to stop empty, returning at the deadline",
			late: 28 * time.Second,
			add: func(sh bubbleShell, called *calls) {
				called.add(sh.AddShutdownGroup().SetName("EGRESS"), "pool", quick)
				called.add(sh.AddShutdownGroup().SetName("CORE"), "ledger", quick)
				sh.AddShutdownGroup().SetName("INGRESS")
			},
			code: 1,
			want: "shutdown deadline 28s passed before group 2 (CORE) ledger",
		},
		{
			name: "returning a nanosecond before the deadline",
			late: 28*time.Second - time.Nanosecond,
			add: func(sh bubbleShell, called *calls) {
				called.add(sh.AddShutdownGroup().SetName("CORE"), "ledger", quick)
				ingress := sh.AddShutdownGroup().SetName("INGRESS")
				called.add(ingress, "consumer", patient)
				called.add(ingress, "listener", quick)
			},
			called: []string{"INGRESS consumer", "INGRESS listener"},
			code:   1,
			want:   "shutdown deadline 28s passed: group 2 (INGRESS) consumer",
		},
		{
			name: "no handlers",
			late: 29 * time.Second,
			add: func(bubbleShell, *calls) {
			},
			code: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				called := newCalls()
				var stopAt time.Time
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					tc.add(sh, called)
					stopAt = <-stopNow(sh, nil)
					time.Sleep(tc.late)
					return nil
				})

				assertCalledOnly(t, called, stopAt, 28*time.Second, tc.called...)
				if result.code != tc.code {
					t.Errorf("exit code %d, want %d", result.code, tc.code)
				}
				assertFailure(t, result, tc.want)
			})
		})
	}
}

// A handler left running at the deadline can still return after Run has: its result goes into the
// buffer nobody reads, without blocking, and its record stays running. The bubble ends only once
// every goroutine in it has exited, so a send that blocked would fail the test.
func TestLeftRunningHandlerCanStillSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var returned atomic.Bool
		var s *shell[struct{}, struct{}]
		var stopped <-chan time.Time
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			s = sh.(*shell[struct{}, struct{}])
			sh.AddShutdownGroup(shutdown.Named("stuck", shutdown.HandlerFunc(func(context.Context) error {
				<-release
				returned.Store(true)
				return errors.New("returned after Run")
			}))).SetName("INGRESS")
			stopped = stopWhenIdle(sh, nil)
			return nil
		})

		if elapsed := result.ended.Sub(<-stopped); elapsed != 28*time.Second {
			t.Errorf("run returned %s after the stop, want the 28s deadline", elapsed)
		}
		if result.code != 1 {
			t.Errorf("exit code %d, want 1", result.code)
		}
		assertFailure(t, result, "shutdown deadline 28s passed: group 1 (INGRESS) stuck")

		close(release)
		synctest.Wait() // the handler has returned and its goroutine sent and exited
		if !returned.Load() {
			t.Error("the handler never returned once released")
		}
		stuck := s.groups[0].handlers[0]
		if state, err := stateOf(stuck); state != stateRunning {
			t.Errorf("record %s (%v) after the late return, want it still running", state, err)
		}
	})
}

// endedWithAResult is a ctx that has already ended, before the deadline it reports, and whose Done
// waits until every other goroutine in the bubble is blocked or gone. When call reaches its wait,
// the handler's goroutine has therefore sent its result and exited, and both the result and the
// ctx's end are ready.
type endedWithAResult struct {
	deadline time.Time
	done     chan struct{}
}

func (c endedWithAResult) Deadline() (time.Time, bool) {
	return c.deadline, true
}

func (c endedWithAResult) Done() <-chan struct{} {
	synctest.Wait()
	return c.done
}

func (endedWithAResult) Err() error {
	return context.Canceled
}

func (endedWithAResult) Value(any) any {
	return nil
}

// When the ctx has ended and the handler's result is already waiting, call takes the result, so the
// record depends on when the handler returned and not on which of the two the wait saw first. Go
// picks at random between ready cases, so 100 calls take the ctx's side about half the time.
func TestCallTakesAResultWaitingAsTheCtxEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failed := errors.New("flush failed")
		for attempt := range 100 {
			done := make(chan struct{})
			close(done)
			ctx := endedWithAResult{deadline: time.Now().Add(28 * time.Second), done: done}
			record := &shutdownHandler{
				group: &shutdownGroup{
					order:  1,
					logger: &lifecycle{},
					runCtx: context.Background(),
				},
				name: "ledger",
				fn: shutdown.HandlerFunc(func(context.Context) error {
					return failed
				}),
			}
			record.call(ctx)
			if state, err := stateOf(record); state != stateFinished || err != failed {
				t.Fatalf("call %d recorded %s (%v), want finished with %v, returned well before the deadline", attempt, state, err, failed)
			}
		}
	})
}
