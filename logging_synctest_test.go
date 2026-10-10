// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/llingr/anvil/shutdown"
)

// These tests pin the lifecycle lines word for word. Every line is compared whole, in the order
// logged, so a changed separator, label or rounding fails here before it reaches anyone's log
// search. A handler that returns exactly at the deadline logs its line as Run ends, in either
// order, so a test comparing every line holds a hanging handler with release instead.

// errSMS is the sms handler's error in the example service
var errSMS = errors.New("gateway unavailable")

// cancellingWireCtx is logged just before exiting when wire returned before the stop
const cancellingWireCtx = "cancelling wire's ctx"

// errorLine is a LifecycleError line as golden renders it
func errorLine(msg, err string) string {
	return msg + "\n\terror: " + err
}

// golden renders the lines for comparison: the message, and the error under it for a
// LifecycleError line. A panic's stack is cut, leaving its "panic: <value>" line.
func golden(lines []line) []string {
	rendered := make([]string, len(lines))
	for index, logged := range lines {
		rendered[index] = logged.msg
		if logged.err != nil {
			rendered[index] = errorLine(logged.msg, logged.err.Error())
		}
		if stack := strings.Index(rendered[index], "\ngoroutine "); stack >= 0 {
			rendered[index] = rendered[index][:stack]
		}
	}
	return rendered
}

// from is the lines from the first whose message starts with prefix, for a test whose earlier
// lines are pinned elsewhere
func from(lines []line, prefix string) []line {
	for index, logged := range lines {
		if strings.HasPrefix(logged.msg, prefix) {
			return lines[index:]
		}
	}
	return nil
}

// assertLines checks that the lines are exactly want, in order
func assertLines(t *testing.T, lines []line, want ...string) {
	t.Helper()
	got := golden(lines)
	if !slices.Equal(got, want) {
		t.Errorf("lines\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// loggedAt is when the line with message msg was logged, counted from the stop
func loggedAt(t *testing.T, lines []line, msg string, stopAt time.Time) time.Duration {
	t.Helper()
	for _, logged := range lines {
		if logged.msg == msg {
			return logged.at.Sub(stopAt)
		}
	}
	t.Errorf("no line %q", msg)
	return -1
}

// wireTheExample adds the example service's seven groups, each handler taking its time from the
// example, with replace overriding any by name, and stops with a SIGTERM once wire has returned.
// The groups are added in the order the service opens what they stop, so they stop in reverse:
// INGRESS_HTTP, added last, stops first. The http server is a Go function that, told to stop, takes
// 312ms to stop serving.
func wireTheExample(replace map[string]shutdown.HandlerFunc, stopped *<-chan time.Time) Wiring[struct{}, struct{}] {
	handler := func(name string, delay time.Duration, err error) shutdown.Handler {
		if replaced, ok := replace[name]; ok {
			return shutdown.Named(name, replaced)
		}
		return shutdown.Named(name, sleepsThen(delay, err))
	}
	return func(_ context.Context, sh bubbleShell) error {
		sh.AddShutdownGroup(handler("linkerd", 1100*time.Millisecond, nil)).SetName("SERVICE_MESH")
		sh.AddShutdownGroup(
			handler("postgres", 12*time.Millisecond, nil),
			handler("dynamo", 15*time.Millisecond, nil),
		).SetName("DB_POOLS")
		sh.AddShutdownGroup(handler("kafka", 80*time.Millisecond, nil)).SetName("EGRESS_PRODUCERS_FLUSH")
		sh.AddShutdownGroup(
			handler("email", 2020*time.Millisecond, nil),
			handler("sms", 40*time.Millisecond, errSMS),
		).SetName("NOTIFICATIONS")
		sh.AddShutdownGroup(
			handler("payments", 1300*time.Millisecond, nil),
			handler("refunds", 300*time.Millisecond, nil),
		).SetName("PAYMENT_PROCESSING")
		sh.AddShutdownGroup(
			handler("orders consumer", 1104*time.Millisecond, nil),
			handler("refunds consumer", 870*time.Millisecond, nil),
		).SetName("INGRESS_CONSUMERS")
		sh.AddShutdownGroup().SetName("INGRESS_HTTP").Go(func(ctx context.Context) error {
			<-ctx.Done()
			time.Sleep(312 * time.Millisecond)
			return ctx.Err()
		})
		*stopped = stopWhenIdle(sh, sigterm)
		return nil
	}
}

// The lines the example service logs up to the third group to stop. Each handler logs as it
// returns, so refunds consumer, added second, logs first.
var exampleOpening = []string{
	"started orders",
	"stopping: terminated signal received",
	"pausing for 5s before shutdown",
	"shutdown group 7 (INGRESS_HTTP) with " + goName,
	"shutdown group 7 (INGRESS_HTTP): " + goName + " done in 312ms",
	"shutdown group 6 (INGRESS_CONSUMERS) with orders consumer, refunds consumer",
	"shutdown group 6 (INGRESS_CONSUMERS): refunds consumer done in 870ms",
	"shutdown group 6 (INGRESS_CONSUMERS): orders consumer done in 1.104s",
	"shutdown group 5 (PAYMENT_PROCESSING) with payments, refunds",
}

// The example service logs exactly these lines, each group beginning as the one before it ends:
// a begin line for each group, then each handler's line as it returns, a failure with its error
func TestLinesMatchTheExample(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var stopped <-chan time.Time
		result := bubbled(t, wireTheExample(nil, &stopped))

		stopAt := <-stopped
		assertLines(t, result.lines, append(slices.Clone(exampleOpening),
			"shutdown group 5 (PAYMENT_PROCESSING): refunds done in 300ms",
			"shutdown group 5 (PAYMENT_PROCESSING): payments done in 1.3s",
			"shutdown group 4 (NOTIFICATIONS) with email, sms",
			errorLine("shutdown group 4 (NOTIFICATIONS): sms failed after 40ms", "gateway unavailable"),
			"shutdown group 4 (NOTIFICATIONS): email done in 2.02s",
			"shutdown group 3 (EGRESS_PRODUCERS_FLUSH) with kafka",
			"shutdown group 3 (EGRESS_PRODUCERS_FLUSH): kafka done in 80ms",
			"shutdown group 2 (DB_POOLS) with postgres, dynamo",
			"shutdown group 2 (DB_POOLS): postgres done in 12ms",
			"shutdown group 2 (DB_POOLS): dynamo done in 15ms",
			"shutdown group 1 (SERVICE_MESH) with linkerd",
			"shutdown group 1 (SERVICE_MESH): linkerd done in 1.1s",
			cancellingWireCtx,
			errorLine("stopped with an error", "group 4 (NOTIFICATIONS) sms: gateway unavailable"),
			"exiting orders",
		)...)
		// the drain ends at 5s, then each group begins as the one before it ends
		begins := []struct {
			msg string
			at  time.Duration
		}{
			{"shutdown group 7 (INGRESS_HTTP) with " + goName, 5 * time.Second},
			{"shutdown group 6 (INGRESS_CONSUMERS) with orders consumer, refunds consumer", 5312 * time.Millisecond},
			{"shutdown group 5 (PAYMENT_PROCESSING) with payments, refunds", 6416 * time.Millisecond},
			{"shutdown group 4 (NOTIFICATIONS) with email, sms", 7716 * time.Millisecond},
			{"shutdown group 3 (EGRESS_PRODUCERS_FLUSH) with kafka", 9736 * time.Millisecond},
			{"shutdown group 2 (DB_POOLS) with postgres, dynamo", 9816 * time.Millisecond},
			{"shutdown group 1 (SERVICE_MESH) with linkerd", 9831 * time.Millisecond},
		}
		for _, begin := range begins {
			if at := loggedAt(t, result.lines, begin.msg, stopAt); at != begin.at {
				t.Errorf("%q logged %s after the stop, want %s", begin.msg, at, begin.at)
			}
		}
		if elapsed := result.ended.Sub(stopAt); elapsed != 10931*time.Millisecond {
			t.Errorf("run returned %s after the stop, want 10.931s", elapsed)
		}
		if result.code != 1 {
			t.Errorf("exit code %d, want 1 for the sms failure", result.code)
		}
	})
}

// With payments hanging in PAYMENT_PROCESSING, refunds beside it still logs as it returns, payments
// logs nothing before the deadline, and no group due to stop after it begins: the deadline error
// names payments
func TestHangLogsNoLineBeforeTheDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		defer synctest.Wait() // payments, left running, returns before the bubble ends
		defer close(release)

		var stopped <-chan time.Time
		result := bubbled(t, wireTheExample(map[string]shutdown.HandlerFunc{"payments": ignoring(release)}, &stopped))

		const deadlineErr = "shutdown deadline 28s passed: group 5 (PAYMENT_PROCESSING) payments"
		assertLines(t, result.lines, append(slices.Clone(exampleOpening),
			"shutdown group 5 (PAYMENT_PROCESSING): refunds done in 300ms",
			cancellingWireCtx,
			errorLine("stopped with an error", deadlineErr),
			"exiting orders",
		)...)
		if elapsed := result.ended.Sub(<-stopped); elapsed != 28*time.Second {
			t.Errorf("run returned %s after the stop, want the 28s deadline", elapsed)
		}
		if result.code != 1 {
			t.Errorf("exit code %d, want 1", result.code)
		}
	})
}

// With email hanging in NOTIFICATIONS, sms beside it logs its own line, finished or failed, and a
// failure is reported again in the shutdown's error before the deadline error
func TestHangBesideAFailure(t *testing.T) {
	cases := []struct {
		name    string
		sms     error
		smsLine string
		failure string
	}{
		{
			name:    "sms finished",
			smsLine: "shutdown group 4 (NOTIFICATIONS): sms done in 40ms",
			failure: "shutdown deadline 28s passed: group 4 (NOTIFICATIONS) email",
		},
		{
			name:    "sms failed",
			sms:     errSMS,
			smsLine: errorLine("shutdown group 4 (NOTIFICATIONS): sms failed after 40ms", "gateway unavailable"),
			failure: "group 4 (NOTIFICATIONS) sms: gateway unavailable\nshutdown deadline 28s passed: group 4 (NOTIFICATIONS) email",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				defer synctest.Wait() // email, left running, returns before the bubble ends
				defer close(release)

				var stopped <-chan time.Time
				result := bubbled(t, wireTheExample(map[string]shutdown.HandlerFunc{
					"email": ignoring(release),
					"sms":   sleepsThen(40*time.Millisecond, tc.sms),
				}, &stopped))

				assertLines(t, result.lines, append(slices.Clone(exampleOpening),
					"shutdown group 5 (PAYMENT_PROCESSING): refunds done in 300ms",
					"shutdown group 5 (PAYMENT_PROCESSING): payments done in 1.3s",
					"shutdown group 4 (NOTIFICATIONS) with email, sms",
					tc.smsLine,
					cancellingWireCtx,
					errorLine("stopped with an error", tc.failure),
					"exiting orders",
				)...)
				if result.code != 1 {
					t.Errorf("exit code %d, want 1", result.code)
				}
			})
		})
	}
}

// When the deadline passes before any group begins, no group logs a line and no handler is called
func TestDeadlineBeforeAnyGroupLogsNoGroup(t *testing.T) {
	const deadlineErr = "shutdown deadline 28s passed before group 2 (CORE) ledger"
	addHandlers := func(sh bubbleShell, called *calls) {
		called.add(sh.AddShutdownGroup().SetName("EGRESS"), "pool", quick)
		core := sh.AddShutdownGroup().SetName("CORE")
		called.add(core, "ledger", quick)
		called.add(core, "outbox", quick)
		sh.AddShutdownGroup().SetName("INGRESS")
	}

	t.Run("a wire returning after the deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			called := newCalls()
			result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
				addHandlers(sh, called)
				stopNow(sh, nil)
				time.Sleep(29 * time.Second) // ignores its ctx
				return nil
			})

			assertLines(t, from(result.lines, "stopping:"),
				"stopping: Stop called"+waitingForWire,
				errorLine("stopped with an error", deadlineErr),
				"exiting orders",
			)
			if at := called.snapshot(); len(at) > 0 {
				t.Errorf("handlers called %v, want none", at)
			}
		})
	})

	t.Run("a logger holding the stopping line to the deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			logger := &lifecycle{slow: func(msg string) {
				if strings.HasPrefix(msg, "stopping:") {
					time.Sleep(28 * time.Second)
				}
			}}
			called := newCalls()
			result := bubbledWith(t, logger, func(_ context.Context, sh bubbleShell) error {
				addHandlers(sh, called)
				stopWhenIdle(sh, nil)
				return nil
			})

			assertLines(t, result.lines,
				"started orders",
				"stopping: Stop called",
				cancellingWireCtx,
				errorLine("stopped with an error", deadlineErr),
				"exiting orders",
			)
			if at := called.snapshot(); len(at) > 0 {
				t.Errorf("handlers called %v, want none", at)
			}
		})
	})
}

// A group with no handlers logs `with no handlers` when it begins. Having begun, it counts as
// finished, so the deadline error skips it; one never begun logs nothing.
func TestEmptyGroupLogsNoHandlers(t *testing.T) {
	cases := []struct {
		name     string
		http     bool   // http hangs
		postgres bool   // postgres hangs
		slowOn   string // a line the logger holds to the deadline
		code     int
		want     []string
	}{
		{
			name: "every handler returns",
			want: []string{
				"started orders",
				"stopping: Stop called",
				"shutdown group 4 (INGRESS_HTTP) with http",
				"shutdown group 4 (INGRESS_HTTP): http done in 0s",
				"shutdown group 3 (NOTIFICATIONS) with no handlers",
				"shutdown group 2 (LEDGER) with no handlers",
				"shutdown group 1 (DB_POOLS) with postgres",
				"shutdown group 1 (DB_POOLS): postgres done in 0s",
				cancellingWireCtx,
				"exiting orders",
			},
		},
		{
			name: "no group begins after a hang",
			http: true,
			code: 1,
			want: []string{
				"started orders",
				"stopping: Stop called",
				"shutdown group 4 (INGRESS_HTTP) with http",
				cancellingWireCtx,
				errorLine("stopped with an error", "shutdown deadline 28s passed: group 4 (INGRESS_HTTP) http"),
				"exiting orders",
			},
		},
		{
			name:     "empty groups that began before a hang are finished",
			postgres: true,
			code:     1,
			want: []string{
				"started orders",
				"stopping: Stop called",
				"shutdown group 4 (INGRESS_HTTP) with http",
				"shutdown group 4 (INGRESS_HTTP): http done in 0s",
				"shutdown group 3 (NOTIFICATIONS) with no handlers",
				"shutdown group 2 (LEDGER) with no handlers",
				"shutdown group 1 (DB_POOLS) with postgres",
				cancellingWireCtx,
				errorLine("stopped with an error", "shutdown deadline 28s passed: group 1 (DB_POOLS) postgres"),
				"exiting orders",
			},
		},
		{
			name:   "an empty group's begin line held to the deadline",
			slowOn: "shutdown group 3 (NOTIFICATIONS) with",
			code:   1,
			want: []string{
				"started orders",
				"stopping: Stop called",
				"shutdown group 4 (INGRESS_HTTP) with http",
				"shutdown group 4 (INGRESS_HTTP): http done in 0s",
				"shutdown group 3 (NOTIFICATIONS) with no handlers",
				cancellingWireCtx,
				errorLine("stopped with an error", "shutdown deadline 28s passed before group 1 (DB_POOLS) postgres"),
				"exiting orders",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				defer synctest.Wait()
				defer close(release)

				logger := &lifecycle{slow: func(msg string) {
					if tc.slowOn != "" && strings.HasPrefix(msg, tc.slowOn) {
						time.Sleep(28 * time.Second)
					}
				}}
				handler := func(hangs bool) shutdown.HandlerFunc {
					if hangs {
						return ignoring(release)
					}
					return quick
				}
				result := bubbledWith(t, logger, func(_ context.Context, sh bubbleShell) error {
					sh.AddShutdownGroup(shutdown.Named("postgres", handler(tc.postgres))).SetName("DB_POOLS")
					sh.AddShutdownGroup().SetName("LEDGER")
					sh.AddShutdownGroup().SetName("NOTIFICATIONS")
					sh.AddShutdownGroup(shutdown.Named("http", handler(tc.http))).SetName("INGRESS_HTTP")
					stopWhenIdle(sh, nil)
					return nil
				})

				assertLines(t, result.lines, tc.want...)
				if result.code != tc.code {
					t.Errorf("exit code %d, want %d", result.code, tc.code)
				}
			})
		})
	}
}

// Every duration in a handler's line rounds to the millisecond, half a millisecond up, so under half
// a millisecond reads 0s
func TestDurationsRoundToTheMillisecond(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		defer synctest.Wait()
		defer close(release)

		var stopped <-chan time.Time
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			sh.AddShutdownGroup(shutdown.Named("pool", ignoring(release))).SetName("EGRESS")
			sh.AddShutdownGroup(
				shutdown.Named("outbox", sleepsThen(1000500*time.Microsecond, errors.New("outbox broke"))),
				shutdown.Named("ledger", sleepsThen(200*time.Microsecond, nil)),
			).SetName("CORE")
			sh.AddShutdownGroup(
				shutdown.Named("slow", sleepsThen(312600*time.Microsecond, nil)),
				shutdown.Named("half", sleepsThen(500*time.Microsecond, nil)),
				shutdown.Named("fast", sleepsThen(400*time.Microsecond, nil)),
			).SetName("INGRESS")
			stopped = stopWhenIdle(sh, nil)
			return nil
		})

		// CORE takes 1.0005s from 312.6ms, so EGRESS begins at 1.3131s
		const deadlineErr = "shutdown deadline 28s passed: group 1 (EGRESS) pool"
		assertLines(t, result.lines,
			"started orders",
			"stopping: Stop called",
			"shutdown group 3 (INGRESS) with slow, half, fast",
			"shutdown group 3 (INGRESS): fast done in 0s",
			"shutdown group 3 (INGRESS): half done in 1ms",
			"shutdown group 3 (INGRESS): slow done in 313ms",
			"shutdown group 2 (CORE) with outbox, ledger",
			"shutdown group 2 (CORE): ledger done in 0s",
			errorLine("shutdown group 2 (CORE): outbox failed after 1.001s", "outbox broke"),
			"shutdown group 1 (EGRESS) with pool",
			cancellingWireCtx,
			errorLine("stopped with an error", "group 2 (CORE) outbox: outbox broke\n"+deadlineErr),
			"exiting orders",
		)
		if at := loggedAt(t, result.lines, "shutdown group 1 (EGRESS) with pool", <-stopped); at != 1313100*time.Microsecond {
			t.Errorf("EGRESS began %s after the stop, want 1.3131s, unrounded", at)
		}
	})
}

// A handler's time runs from its call, after its group's begin line is logged, so a logger slow to
// write that line delays the call and does not count in the handler's time
func TestHandlerTimeCountsFromItsCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := &lifecycle{slow: func(msg string) {
			if msg == "shutdown group 1 (INGRESS) with consumer" {
				time.Sleep(100 * time.Millisecond)
			}
		}}
		var stopped <-chan time.Time
		result := bubbledWith(t, logger, func(_ context.Context, sh bubbleShell) error {
			sh.AddShutdownGroup(shutdown.Named("consumer", sleepsThen(time.Second, nil))).SetName("INGRESS")
			stopped = stopWhenIdle(sh, nil)
			return nil
		})

		assertLines(t, from(result.lines, "shutdown group 1 (INGRESS) with"),
			"shutdown group 1 (INGRESS) with consumer",
			"shutdown group 1 (INGRESS): consumer done in 1s",
			cancellingWireCtx,
			"exiting orders",
		)
		if elapsed := result.ended.Sub(<-stopped); elapsed != 1100*time.Millisecond {
			t.Errorf("run returned %s after the stop, want 1.1s", elapsed)
		}
	})
}

// The drain waits the exact time left and its line rounds that to the millisecond, so a wire
// returning under half a millisecond before the drain ends logs 0s; with no time left there is no
// line, and either way the first group begins exactly as the drain ends
func TestDrainLineRoundsTheTimeLeft(t *testing.T) {
	cases := []struct {
		name  string
		left  time.Duration // of the 5s drain when wire returns
		drain string        // the drain line, "" for none
	}{
		{"0.4ms left", 400 * time.Microsecond, "pausing for 0s before shutdown"},
		{"none left", 0, ""},
		{"0.5ms left", 500 * time.Microsecond, "pausing for 1ms before shutdown"},
		{"312.6ms left", 312600 * time.Microsecond, "pausing for 313ms before shutdown"},
		{"3s left", 3 * time.Second, "pausing for 3s before shutdown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				called := newCalls()
				var stopped <-chan time.Time
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					called.add(sh.AddShutdownGroup().SetName("INGRESS"), "consumer", quick)
					stopped = stopNow(sh, sigterm)
					time.Sleep(5*time.Second - tc.left) // ignores its ctx
					return nil
				})

				stopAt := <-stopped
				want := []string{"stopping: terminated signal received" + waitingForWire}
				if tc.drain != "" {
					want = append(want, tc.drain)
				}
				want = append(want,
					"shutdown group 1 (INGRESS) with consumer",
					"shutdown group 1 (INGRESS): consumer done in 0s",
					"exiting orders",
				)
				assertLines(t, from(result.lines, "stopping:"), want...)
				if tc.drain != "" {
					if at := loggedAt(t, result.lines, tc.drain, stopAt); at != 5*time.Second-tc.left {
						t.Errorf("drain line logged %s after the stop, want %s, as wire returned", at, 5*time.Second-tc.left)
					}
				}
				if calledAt, ok := called.snapshot()["INGRESS consumer"]; !ok || calledAt.Sub(stopAt) != 5*time.Second {
					t.Errorf("consumer called %v %s after the stop, want exactly as the 5s drain ends", ok, calledAt.Sub(stopAt))
				}
				if result.code != 0 {
					t.Errorf("exit code %d, want 0: %v", result.code, result.failure())
				}
			})
		})
	}
}

// A service with no handlers stops cleanly and says so, whether it added no groups or only empty
// ones, which still log their begin lines
func TestNoHandlersLogsNothingRegistered(t *testing.T) {
	cases := []struct {
		name   string
		groups int
		want   []string
	}{
		{"no groups", 0, nil},
		{"two empty groups", 2, []string{"shutdown group 2 with no handlers", "shutdown group 1 with no handlers"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					for range tc.groups {
						sh.AddShutdownGroup()
					}
					stopWhenIdle(sh, nil)
					return nil
				})

				if result.code != 0 {
					t.Errorf("exit code %d, want 0", result.code)
				}
				want := []string{"stopping: Stop called", "no components are registered for graceful shutdown"}
				want = append(want, tc.want...)
				want = append(want, cancellingWireCtx, "exiting orders")
				assertLines(t, from(result.lines, "stopping:"), want...)
			})
		})
	}
}

// The groups wire added run whatever wire returns, so a failing wire still logs them; started is
// not logged
func TestShutdownRunsAfterAFailingWire(t *testing.T) {
	addBoth := func(sh bubbleShell) {
		sh.AddShutdownGroup(
			shutdown.Named("outbox", shutdown.HandlerFunc(quick)),
			shutdown.Named("ledger", sleepsThen(time.Millisecond, nil)),
		).SetName("CORE")
		sh.AddShutdownGroup(shutdown.Named("consumer", shutdown.HandlerFunc(quick))).SetName("INGRESS")
	}
	ran := []string{
		"shutdown group 2 (INGRESS) with consumer",
		"shutdown group 2 (INGRESS): consumer done in 0s",
		"shutdown group 1 (CORE) with outbox, ledger",
		"shutdown group 1 (CORE): outbox done in 0s",
		"shutdown group 1 (CORE): ledger done in 1ms",
	}
	cases := []struct {
		name   string
		wire   Wiring[struct{}, struct{}]
		reason string
		want   []string
	}{
		{
			name: "an error after adding",
			wire: func(_ context.Context, sh bubbleShell) error {
				addBoth(sh)
				return errors.New("boom")
			},
			reason: "wiring failed: boom",
			want:   ran,
		},
		{
			name: "a panic after adding",
			wire: func(_ context.Context, sh bubbleShell) error {
				addBoth(sh)
				panic("kaboom")
			},
			reason: "wiring failed: panic: kaboom",
			want:   ran,
		},
		{
			name: "an error before adding anything",
			wire: func(context.Context, bubbleShell) error {
				return errors.New("boom")
			},
			reason: "wiring failed: boom",
			want:   []string{"no components are registered for graceful shutdown"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				result := bubbled(t, tc.wire)

				// the stop wire's failure asked for, then the groups
				want := append([]string{"stopping: " + tc.reason}, tc.want...)
				want = append(want, cancellingWireCtx, errorLine("stopped with an error", tc.reason), "exiting orders")
				assertLines(t, result.lines, want...)
				if result.code != 1 {
					t.Errorf("exit code %d, want 1", result.code)
				}
			})
		})
	}
}

// The begin line is logged only before the deadline: a logger holding the begin line of the first
// group to stop past it leaves that group's handlers never called and the group due to stop next
// with no line of its own
func TestNoBeginLineAfterTheDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := &lifecycle{slow: func(msg string) {
			if msg == "shutdown group 2 (INGRESS) with consumer" {
				time.Sleep(28 * time.Second)
			}
		}}
		result := bubbledWith(t, logger, func(_ context.Context, sh bubbleShell) error {
			sh.AddShutdownGroup(shutdown.Named("ledger", shutdown.HandlerFunc(quick))).SetName("CORE")
			sh.AddShutdownGroup(shutdown.Named("consumer", shutdown.HandlerFunc(quick))).SetName("INGRESS")
			stopWhenIdle(sh, nil)
			return nil
		})

		const deadlineErr = "shutdown deadline 28s passed before group 2 (INGRESS) consumer"
		assertLines(t, result.lines,
			"started orders",
			"stopping: Stop called",
			"shutdown group 2 (INGRESS) with consumer",
			cancellingWireCtx,
			errorLine("stopped with an error", deadlineErr),
			"exiting orders",
		)
	})
}
