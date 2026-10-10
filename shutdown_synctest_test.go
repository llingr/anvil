// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/llingr/anvil/shutdown"
)

// These tests run the shell in a synctest bubble, whose clock advances only once every goroutine in
// it is blocked, so they assert the shutdown's times exactly, in no wall clock time. bubbled builds
// the shell with initShell and runs s.run(wire), as Run does once the configuration has loaded. What
// Run does that bubbled does not, so any change to one is weighed against the other:
//
//   - validating the providers and wire, and logging "starting", "loading config" and
//     "configuration loaded in";
//   - loading the configuration (bubbled passes struct{});
//   - trapping signals with os/signal, which cannot run in a bubble: a SIGTERM here is
//     s.requestStop(signalStop{...}), the call watchSignals makes on the first signal, and real
//     signals are tested in tests/;
//   - the context.AfterFunc on Run's ctx that stops the shell when it is cancelled;
//   - flushing the logger, and logging a panic before re-raising it.

// bubbleShell is the shell bubbled passes to wire
type bubbleShell = Shell[struct{}, struct{}]

// runValue keys a value on Run's ctx, which every handler's ctx carries
type runValue struct{}

// sigterm is the stop watchSignals requests on a SIGTERM
var sigterm = signalStop{signal: syscall.SIGTERM}

// goName is the name a group's Go gives its record in the log lines, kept here alone as it may change
const goName = "go"

// line is one lifecycle line, with the bubble's time when it was logged
type line struct {
	at  time.Time
	msg string
	err error // nil for LifecycleInfo
}

// lifecycle records every lifecycle line, locked because handlers and goroutines log concurrently
type lifecycle struct {
	mu    sync.Mutex
	lines []line
	slow  func(msg string) // when set, called before an Info line is recorded, as a slow logger
}

func (l *lifecycle) Logger() struct{} {
	return struct{}{}
}

func (l *lifecycle) LifecycleInfo(_ context.Context, msg string) {
	if l.slow != nil {
		l.slow(msg)
	}
	l.add(msg, nil)
}

// errNilError is recorded for a LifecycleError given a nil error, so an error line never passes
// for an info line
var errNilError = errors.New("LifecycleError given a nil error")

func (l *lifecycle) LifecycleError(_ context.Context, msg string, err error) {
	if err == nil {
		err = errNilError
	}
	l.add(msg, err)
}

func (l *lifecycle) Flush() {
}

func (l *lifecycle) add(msg string, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, line{at: time.Now(), msg: msg, err: err})
}

func (l *lifecycle) recorded() []line {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.lines)
}

// ran is what one bubbled run did
type ran struct {
	code  int
	lines []line    // every lifecycle line, in the order logged
	ended time.Time // when run returned
}

// failure is the error "stopped with an error" carried, or nil when that line was not logged
func (r ran) failure() error {
	for _, logged := range r.lines {
		if logged.msg == "stopped with an error" {
			return logged.err
		}
	}
	return nil
}

// messages are the lines' messages, for a failure report
func (r ran) messages() []string {
	messages := make([]string, len(r.lines))
	for index, logged := range r.lines {
		messages[index] = logged.msg
	}
	return messages
}

// bubbled runs wire in a shell named "orders" built from opts, and returns once run does. It must be
// called inside synctest.Test, so that the shell, its contexts and its channels belong to the bubble.
func bubbled(t *testing.T, wire Wiring[struct{}, struct{}], opts ...Option) ran {
	t.Helper()
	return bubbledWith(t, &lifecycle{}, wire, opts...)
}

// bubbledWith is bubbled logging to logger
func bubbledWith(t *testing.T, logger *lifecycle, wire Wiring[struct{}, struct{}], opts ...Option) ran {
	t.Helper()
	return bubbledBefore(t, logger, nil, wire, opts...)
}

// bubbledBefore is bubbledWith calling before, when set, on the built shell before wire is called,
// as a signal or the cancellation of Run's ctx can arrive between Run building the shell and
// calling wire
func bubbledBefore(t *testing.T, logger *lifecycle, before func(*shell[struct{}, struct{}]), wire Wiring[struct{}, struct{}], opts ...Option) ran {
	t.Helper()
	runCtx := context.WithValue(t.Context(), runValue{}, "run")
	s := initShell[struct{}, struct{}]("orders", context.WithoutCancel(runCtx), logger, struct{}{}, processOptions(opts...))
	if before != nil {
		before(s)
	}
	code := s.run(wire)
	return ran{code: code, lines: logger.recorded(), ended: time.Now()}
}

// stopNow stops the shell at once, as a stop during wiring, and returns the instant of the stop
func stopNow(sh bubbleShell, reason error) <-chan time.Time {
	stopped := make(chan time.Time, 1)
	stopped <- time.Now()
	sh.(*shell[struct{}, struct{}]).requestStop(reason)
	return stopped
}

// stopAfter stops the shell from a goroutine of its own delay after the call, as a signal arriving
// while wire still runs, and sends the instant of the stop
func stopAfter(sh bubbleShell, delay time.Duration, reason error) <-chan time.Time {
	stopped := make(chan time.Time, 1)
	go func() {
		time.Sleep(delay)
		stopped <- time.Now()
		sh.(*shell[struct{}, struct{}]).requestStop(reason)
	}()
	return stopped
}

// stopWhenIdle stops the shell from a goroutine of its own once wire has returned and every other
// goroutine in the bubble is blocked, as a signal or Stop would, and sends the instant of the stop
func stopWhenIdle(sh bubbleShell, reason error) <-chan time.Time {
	stopped := make(chan time.Time, 1)
	go func() {
		synctest.Wait()
		stopped <- time.Now()
		sh.(*shell[struct{}, struct{}]).requestStop(reason)
	}()
	return stopped
}

// calls records when each handler was called, as "<group name> <name>", locked because a handler
// left running at the deadline records from its own goroutine
type calls struct {
	mu sync.Mutex
	at map[string]time.Time
}

func newCalls() *calls {
	return &calls{at: map[string]time.Time{}}
}

// add adds handler to group under name, recording when it is called. The group's name is read as
// it is added, so a test names the group first.
func (c *calls) add(group ShutdownGroup, name string, handler shutdown.HandlerFunc) {
	key := group.(*shutdownGroup).name + " " + name
	group.Add(shutdown.Named(name, shutdown.HandlerFunc(func(ctx context.Context) error {
		c.record(key)
		return handler(ctx)
	})))
}

// goIgnoring starts a Go function in group that ignores its ctx, returning only once release is
// closed. It records as called, under the name its record has, when it is told to stop, which is
// when its group stops.
func (c *calls) goIgnoring(group ShutdownGroup, release <-chan struct{}) {
	key := group.(*shutdownGroup).name + " " + goName
	group.Go(func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			c.record(key)
		case <-release:
		}
		<-release
		return nil
	})
}

func (c *calls) record(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at[name] = time.Now()
}

func (c *calls) snapshot() map[string]time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.at)
}

// quick is a handler that returns nil at once
func quick(context.Context) error {
	return nil
}

// ignoring is a handler that ignores its ctx and returns only once release is closed
func ignoring(release <-chan struct{}) shutdown.HandlerFunc {
	return func(context.Context) error {
		<-release
		return nil
	}
}

// patient is a handler that respects its ctx and returns its error when it ends
func patient(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// unruly adds to group a handler for each way a handler can misbehave, and a Go function that
// ignores its ctx, recorded in told as "<group name> loop" when it is told to stop. They are all
// called together as the group stops.
func unruly(group ShutdownGroup, called *calls, told *stops, release <-chan struct{}) {
	called.add(group, "panics", func(context.Context) error {
		panic("kaboom")
	})
	called.add(group, "fails", func(context.Context) error {
		return errors.New("flush failed")
	})
	called.add(group, "on-time", patient)
	told.add(group, "loop", holds, release)
	called.add(group, "late", func(ctx context.Context) error {
		<-ctx.Done()
		<-release // after the deadline, as the test ends
		return ctx.Err()
	})
	called.add(group, "ignores", ignoring(release))
}

// unrulyCalled is the handlers unruly added to the group named group, as calls records them, every
// one of them called as the group stops
func unrulyCalled(group string) []string {
	names := []string{"panics", "fails", "on-time", "late", "ignores"}
	called := make([]string, len(names))
	for index, name := range names {
		called[index] = group + " " + name
	}
	return called
}

// unrulyDeadline is the deadline error when unruly's handlers in the group labelled label are the
// first not to finish: it names those still running at the deadline, in the order added, which is
// every one but those that panicked and failed
func unrulyDeadline(deadline time.Duration, label string) string {
	names := []string{"on-time", goName, "late", "ignores"}
	running := make([]string, len(names))
	for index, name := range names {
		running[index] = label + " " + name
	}
	return fmt.Sprintf("shutdown deadline %s passed: %s", deadline, strings.Join(running, ", "))
}

// shutdownStops are the stops the deadline is counted from, with and without a drain
var shutdownStops = []struct {
	name      string
	reason    error
	opts      []Option
	deadline  time.Duration
	cleanCode int // the exit code of a clean shutdown after this stop
}{
	{"SIGTERM drains first", sigterm, nil, 28 * time.Second, 0},
	{"Stop has no drain", nil, nil, 28 * time.Second, 0},
	{"SIGINT has no drain", signalStop{signal: syscall.SIGINT}, nil, 28 * time.Second, 130},
	{"SIGTERM with a 10s grace period", sigterm, []Option{WithShutdownGracePeriod(10 * time.Second), WithDrainDelay(3 * time.Second)}, 10 * time.Second, 0},
}

// However badly the handlers of any group behave, the shutdown ends exactly at the deadline counted
// from the stop, drain included, and Run returns 1 without waiting for the handlers left running
func TestShutdownEndsAtTheDeadline(t *testing.T) {
	everyGroup := []string{"INGRESS", "CORE", "EGRESS"} // in the order they stop, so added in reverse
	unrulyGroups := []struct {
		names []string
		first string // the label of the first unruly group to stop, as the log lines and errors show it
	}{
		{[]string{"INGRESS"}, "group 3 (INGRESS)"},
		{[]string{"CORE"}, "group 2 (CORE)"},
		{[]string{"EGRESS"}, "group 1 (EGRESS)"},
		{everyGroup, "group 3 (INGRESS)"},
	}
	for _, stop := range shutdownStops {
		for _, unrulyGroup := range unrulyGroups {
			misbehaving := unrulyGroup.names
			name := fmt.Sprintf("%s/unruly %v", stop.name, misbehaving)
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					release := make(chan struct{})
					defer synctest.Wait() // the handlers left running return before the bubble ends
					defer close(release)

					called := newCalls()
					told := newStops()
					var stopped <-chan time.Time
					result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
						for _, groupName := range slices.Backward(everyGroup) {
							group := sh.AddShutdownGroup().SetName(groupName)
							if slices.Contains(misbehaving, groupName) {
								unruly(group, called, told, release)
							} else {
								called.add(group, "quick", quick)
							}
						}
						stopped = stopWhenIdle(sh, stop.reason)
						return nil
					}, stop.opts...)

					stopAt := <-stopped
					if elapsed := result.ended.Sub(stopAt); elapsed != stop.deadline {
						t.Errorf("run returned %s after the stop, want the %s deadline", elapsed, stop.deadline)
					}
					if result.code != 1 {
						t.Errorf("exit code %d, want 1", result.code)
					}
					// a handler returning at the deadline logs its late line as run ends, in either order
					var messages []string
					for _, msg := range result.messages() {
						if !strings.HasSuffix(msg, " after the deadline") {
							messages = append(messages, msg)
						}
					}
					if len(messages) < 2 || messages[len(messages)-1] != "exiting orders" ||
						messages[len(messages)-2] != "stopped with an error" || result.failure() == nil {
						t.Errorf("lines %q, want an error ending in stopped with an error, then exiting orders", messages)
					}
					// the quick handlers before the first unruly group, then those it calls, then nothing
					var want []string
					for _, groupName := range everyGroup {
						if slices.Contains(misbehaving, groupName) {
							want = append(want, unrulyCalled(groupName)...)
							break
						}
						want = append(want, groupName+" quick")
					}
					at := called.snapshot()
					for handler, calledAt := range at {
						if !slices.Contains(want, handler) {
							t.Errorf("%s called %s after the stop, want it never called", handler, calledAt.Sub(stopAt))
						}
						if !calledAt.Before(stopAt.Add(stop.deadline)) {
							t.Errorf("%s called %s after the stop, at or after the %s deadline", handler, calledAt.Sub(stopAt), stop.deadline)
						}
					}
					for _, handler := range want {
						if _, ok := at[handler]; !ok {
							t.Errorf("%s never called, want it called before the deadline", handler)
						}
					}
					first := unrulyGroup.first
					for _, reported := range []string{first + " panics: panic: kaboom", first + " fails: flush failed"} {
						if err := result.failure(); err == nil || !strings.Contains(err.Error(), reported) {
							t.Errorf("error %v, want %q", err, reported)
						}
					}
					assertDeadlineLast(t, result.failure(), unrulyDeadline(stop.deadline, first))
					// each loop is told to stop when its group stops, which only the first unruly
					// group reaches, else as the shutdown ends
					synctest.Wait()
					drain := time.Duration(0)
					if stop.reason == sigterm {
						drain = processOptions(stop.opts...).drainDelay
					}
					toldAt := told.snapshot()
					for index, groupName := range misbehaving {
						after := stop.deadline
						if index == 0 {
							after = drain
						}
						loop := groupName + " loop"
						if got, ok := toldAt[loop]; !ok || got.at.Sub(stopAt) != after {
							t.Errorf("%s told to stop %v %s after the stop, want %s", loop, ok, got.at.Sub(stopAt), after)
						}
					}
				})
			})
		}
	}
}

// Every handler called is called before the deadline, and a handler in a group due to stop after one
// still running at the deadline, or behind a wire that returned too late, is never called. Each case
// adds its groups in reverse of the order they stop.
func TestNoHandlerCalledAfterTheDeadline(t *testing.T) {
	cases := []struct {
		name   string
		wire   func(sh bubbleShell, called *calls, release <-chan struct{}) <-chan time.Time
		called []string // every handler called; any other is never called
		failed string   // the deadline error, the last line of the shutdown's error
	}{
		{
			name: "a hang in INGRESS",
			wire: func(sh bubbleShell, called *calls, release <-chan struct{}) <-chan time.Time {
				egress := sh.AddShutdownGroup().SetName("EGRESS")
				core := sh.AddShutdownGroup().SetName("CORE")
				ingress := sh.AddShutdownGroup().SetName("INGRESS")
				called.add(ingress, "consumer", quick)
				called.add(ingress, "stuck", ignoring(release))
				called.add(core, "ledger", quick)
				called.add(core, "outbox", quick)
				called.add(egress, "pool", quick)
				return stopWhenIdle(sh, nil)
			},
			called: []string{"INGRESS consumer", "INGRESS stuck"},
			failed: "shutdown deadline 28s passed: group 3 (INGRESS) stuck",
		},
		{
			name: "a hang alone in CORE",
			wire: func(sh bubbleShell, called *calls, release <-chan struct{}) <-chan time.Time {
				egress := sh.AddShutdownGroup().SetName("EGRESS")
				core := sh.AddShutdownGroup().SetName("CORE")
				ingress := sh.AddShutdownGroup().SetName("INGRESS")
				called.add(ingress, "consumer", quick)
				called.add(core, "stuck", ignoring(release))
				called.add(egress, "producer", quick)
				called.add(egress, "pool", quick)
				return stopWhenIdle(sh, sigterm)
			},
			called: []string{"INGRESS consumer", "CORE stuck"},
			failed: "shutdown deadline 28s passed: group 2 (CORE) stuck",
		},
		{
			name: "a hang in a group between two others",
			wire: func(sh bubbleShell, called *calls, release <-chan struct{}) <-chan time.Time {
				egress := sh.AddShutdownGroup().SetName("EGRESS")
				ledger := sh.AddShutdownGroup().SetName("LEDGER")
				hang := sh.AddShutdownGroup().SetName("HANG")
				outbox := sh.AddShutdownGroup().SetName("OUTBOX")
				ingress := sh.AddShutdownGroup().SetName("INGRESS")
				called.add(ingress, "consumer", quick)
				called.add(outbox, "outbox", quick)
				called.add(hang, "stuck", ignoring(release))
				called.add(ledger, "ledger", quick)
				called.add(egress, "pool", quick)
				return stopWhenIdle(sh, nil)
			},
			called: []string{"INGRESS consumer", "OUTBOX outbox", "HANG stuck"},
			failed: "shutdown deadline 28s passed: group 3 (HANG) stuck",
		},
		{
			name: "a hang in EGRESS",
			wire: func(sh bubbleShell, called *calls, release <-chan struct{}) <-chan time.Time {
				egress := sh.AddShutdownGroup().SetName("EGRESS")
				core := sh.AddShutdownGroup().SetName("CORE")
				ingress := sh.AddShutdownGroup().SetName("INGRESS")
				called.add(ingress, "consumer", quick)
				called.add(core, "ledger", quick)
				called.add(egress, "stuck", ignoring(release))
				called.add(egress, "pool", quick)
				return stopWhenIdle(sh, nil)
			},
			called: []string{"INGRESS consumer", "CORE ledger", "EGRESS stuck", "EGRESS pool"},
			failed: "shutdown deadline 28s passed: group 1 (EGRESS) stuck",
		},
		{
			name: "a Go function that ignores its ctx, in a group between two others",
			wire: func(sh bubbleShell, called *calls, release <-chan struct{}) <-chan time.Time {
				egress := sh.AddShutdownGroup().SetName("EGRESS")
				ledger := sh.AddShutdownGroup().SetName("LEDGER")
				relay := sh.AddShutdownGroup().SetName("RELAY")
				outbox := sh.AddShutdownGroup().SetName("OUTBOX")
				ingress := sh.AddShutdownGroup().SetName("INGRESS")
				called.add(ingress, "consumer", quick)
				called.add(outbox, "outbox", quick)
				called.goIgnoring(relay, release)
				called.add(ledger, "ledger", quick)
				called.add(egress, "pool", quick)
				return stopWhenIdle(sh, nil)
			},
			called: []string{"INGRESS consumer", "OUTBOX outbox", "RELAY " + goName},
			failed: "shutdown deadline 28s passed: group 3 (RELAY) " + goName,
		},
		{
			name: "a Go function in INGRESS that ignores its ctx, after a drain",
			wire: func(sh bubbleShell, called *calls, release <-chan struct{}) <-chan time.Time {
				egress := sh.AddShutdownGroup().SetName("EGRESS")
				core := sh.AddShutdownGroup().SetName("CORE")
				ingress := sh.AddShutdownGroup().SetName("INGRESS")
				called.goIgnoring(ingress, release)
				called.add(ingress, "listener", quick)
				called.add(core, "ledger", quick)
				called.add(egress, "pool", quick)
				return stopWhenIdle(sh, sigterm)
			},
			called: []string{"INGRESS " + goName, "INGRESS listener"},
			failed: "shutdown deadline 28s passed: group 3 (INGRESS) " + goName,
		},
		{
			name: "a handler in INGRESS that returns as its ctx ends, after a drain",
			wire: func(sh bubbleShell, called *calls, _ <-chan struct{}) <-chan time.Time {
				egress := sh.AddShutdownGroup().SetName("EGRESS")
				core := sh.AddShutdownGroup().SetName("CORE")
				ingress := sh.AddShutdownGroup().SetName("INGRESS")
				called.add(ingress, "consumer", patient)
				called.add(core, "ledger", quick)
				called.add(egress, "pool", quick)
				return stopWhenIdle(sh, sigterm)
			},
			called: []string{"INGRESS consumer"},
			failed: "shutdown deadline 28s passed: group 3 (INGRESS) consumer",
		},
		{
			name: "wire returns after the deadline",
			wire: func(sh bubbleShell, called *calls, _ <-chan struct{}) <-chan time.Time {
				egress := sh.AddShutdownGroup().SetName("EGRESS")
				core := sh.AddShutdownGroup().SetName("CORE")
				ingress := sh.AddShutdownGroup().SetName("INGRESS")
				called.add(ingress, "consumer", quick)
				called.add(core, "ledger", quick)
				called.add(egress, "pool", quick)
				stopped := stopNow(sh, sigterm)
				time.Sleep(29 * time.Second)
				return stopped
			},
			called: nil,
			failed: "shutdown deadline 28s passed before group 3 (INGRESS) consumer",
		},
		{
			name: "wire returns at the deadline",
			wire: func(sh bubbleShell, called *calls, _ <-chan struct{}) <-chan time.Time {
				egress := sh.AddShutdownGroup().SetName("EGRESS")
				core := sh.AddShutdownGroup().SetName("CORE")
				ingress := sh.AddShutdownGroup().SetName("INGRESS")
				called.add(ingress, "consumer", quick)
				called.add(core, "ledger", quick)
				called.add(egress, "pool", quick)
				stopped := stopNow(sh, nil)
				time.Sleep(28 * time.Second)
				return stopped
			},
			called: nil,
			failed: "shutdown deadline 28s passed before group 3 (INGRESS) consumer",
		},
		{
			name: "wire returns a second before the deadline",
			wire: func(sh bubbleShell, called *calls, _ <-chan struct{}) <-chan time.Time {
				egress := sh.AddShutdownGroup().SetName("EGRESS")
				core := sh.AddShutdownGroup().SetName("CORE")
				ingress := sh.AddShutdownGroup().SetName("INGRESS")
				called.add(ingress, "consumer", patient)
				called.add(ingress, "listener", quick)
				called.add(core, "ledger", quick)
				called.add(egress, "pool", quick)
				stopped := stopNow(sh, sigterm)
				time.Sleep(27 * time.Second)
				return stopped
			},
			called: []string{"INGRESS consumer", "INGRESS listener"},
			failed: "shutdown deadline 28s passed: group 3 (INGRESS) consumer",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				defer synctest.Wait()
				defer close(release)

				const deadline = 28 * time.Second
				called := newCalls()
				var stopped <-chan time.Time
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					stopped = tc.wire(sh, called, release)
					return nil
				})
				synctest.Wait() // a Go function told to stop as run returned has recorded it

				stopAt := <-stopped
				at := called.snapshot()
				for handler, calledAt := range at {
					if !slices.Contains(tc.called, handler) {
						t.Errorf("%s called %s after the stop, want it never called", handler, calledAt.Sub(stopAt))
					}
					if !calledAt.Before(stopAt.Add(deadline)) {
						t.Errorf("%s called %s after the stop, at or after the deadline", handler, calledAt.Sub(stopAt))
					}
				}
				for _, handler := range tc.called {
					if _, ok := at[handler]; !ok {
						t.Errorf("%s never called, want it called before the deadline", handler)
					}
				}
				if result.code != 1 {
					t.Errorf("exit code %d, want 1", result.code)
				}
				assertDeadlineLast(t, result.failure(), tc.failed)
			})
		})
	}
}

// Every handler's ctx carries Run's values and ends at the shutdown deadline, counted from the stop,
// so a handler called later has only what is left of it
func TestEveryHandlerHasTheShutdownDeadline(t *testing.T) {
	for _, stop := range shutdownStops {
		t.Run(stop.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// the drain, then INGRESS for 3s, then CORE for 2s, then EGRESS
				drain := time.Duration(0)
				if stop.reason == sigterm {
					drain = processOptions(stop.opts...).drainDelay
				}
				calledAfter := map[string]time.Duration{
					"INGRESS consumer": drain,
					"INGRESS listener": drain,
					"CORE ledger":      drain + 3*time.Second,
					"EGRESS pool":      drain + 5*time.Second,
				}

				var mu sync.Mutex
				type seen struct {
					called, deadline time.Time
					hasDeadline      bool
					value            any
					err              error // the ctx's error on entry
				}
				saw := map[string]seen{}
				observing := func(name string, delay time.Duration) shutdown.HandlerFunc {
					return func(ctx context.Context) error {
						deadline, ok := ctx.Deadline()
						mu.Lock()
						saw[name] = seen{
							called:      time.Now(),
							deadline:    deadline,
							hasDeadline: ok,
							value:       ctx.Value(runValue{}),
							err:         ctx.Err(),
						}
						mu.Unlock()
						time.Sleep(delay)
						return nil
					}
				}

				var stopped <-chan time.Time
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					sh.AddShutdownGroup(shutdown.Named("pool", observing("EGRESS pool", 0))).SetName("EGRESS")
					sh.AddShutdownGroup(shutdown.Named("ledger", observing("CORE ledger", 2*time.Second))).SetName("CORE")
					sh.AddShutdownGroup(
						shutdown.Named("consumer", observing("INGRESS consumer", 0)),
						shutdown.Named("listener", observing("INGRESS listener", 3*time.Second)),
					).SetName("INGRESS")
					stopped = stopWhenIdle(sh, stop.reason)
					return nil
				}, stop.opts...)

				stopAt := <-stopped
				if result.code != stop.cleanCode {
					t.Errorf("exit code %d, want %d for a clean shutdown", result.code, stop.cleanCode)
				}
				for name, after := range calledAfter {
					got, ok := saw[name]
					switch {
					case !ok:
						t.Errorf("%s never called", name)
					case !got.hasDeadline:
						t.Errorf("%s had no deadline, want the shutdown's", name)
					case got.called.Sub(stopAt) != after:
						t.Errorf("%s called %s after the stop, want %s", name, got.called.Sub(stopAt), after)
					case got.deadline.Sub(got.called) != stop.deadline-after:
						t.Errorf("%s had %s left, want %s, the %s deadline less %s", name,
							got.deadline.Sub(got.called), stop.deadline-after, stop.deadline, after)
					case !got.deadline.Equal(stopAt.Add(stop.deadline)):
						t.Errorf("%s had a deadline %s after the stop, want %s", name, got.deadline.Sub(stopAt), stop.deadline)
					}
					if ok && got.value != "run" {
						t.Errorf("%s ctx carried %v, want Run's value", name, got.value)
					}
					if ok && got.err != nil {
						t.Errorf("%s ctx ended with %v as it was called, want it live until the deadline", name, got.err)
					}
				}
			})
		})
	}
}

// occupancy counts how many handlers are stopping at once
type occupancy struct {
	mu      sync.Mutex
	inside  int
	most    int
	entered []string
}

// handler is a handler named name that sleeps for delay, counted while it runs
func (o *occupancy) handler(name string, delay time.Duration) shutdown.Handler {
	return shutdown.Named(name, shutdown.HandlerFunc(func(context.Context) error {
		o.enter(name)
		time.Sleep(delay)
		o.leave()
		return nil
	}))
}

func (o *occupancy) enter(name string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.inside++
	o.entered = append(o.entered, name)
	o.most = max(o.most, o.inside)
}

// order is the handlers entered so far, safe while a handler left running still runs
func (o *occupancy) order() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return strings.Join(o.entered, " ")
}

// peak is the most handlers inside at once so far
func (o *occupancy) peak() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.most
}

func (o *occupancy) leave() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.inside--
}

// A group calls every handler at once, so the group costs the slowest of them, named or not
func TestGroupCostsItsSlowestHandler(t *testing.T) {
	for _, named := range []bool{true, false} {
		t.Run(fmt.Sprintf("named %v", named), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				together := &occupancy{}
				var stopped <-chan time.Time
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					group := sh.AddShutdownGroup()
					if named {
						group.SetName("POOLS")
					}
					for _, delay := range []time.Duration{time.Second, 3 * time.Second, 5 * time.Second} {
						group.Add(together.handler(delay.String(), delay))
					}
					stopped = stopWhenIdle(sh, nil)
					return nil
				})

				if result.code != 0 {
					t.Fatalf("exit code %d, want 0: %v", result.code, result.failure())
				}
				if elapsed := result.ended.Sub(<-stopped); elapsed != 5*time.Second {
					t.Errorf("the group took %s, want 5s", elapsed)
				}
				if most := together.peak(); most != 3 {
					t.Errorf("the group held %d handlers at once, want 3", most)
				}
			})
		})
	}
}

// Groups of one handler each run one at a time, the last added first, so together they cost the
// sum of their handlers
func TestGroupsOfOneRunOneAtATime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sequential := &occupancy{}
		var stopped <-chan time.Time
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			for _, name := range []string{"outbox", "ledger", "orders"} {
				sh.AddShutdownGroup(sequential.handler(name, 2*time.Second))
			}
			stopped = stopWhenIdle(sh, nil)
			return nil
		})

		if result.code != 0 {
			t.Fatalf("exit code %d, want 0: %v", result.code, result.failure())
		}
		if elapsed := result.ended.Sub(<-stopped); elapsed != 6*time.Second {
			t.Errorf("the groups took %s, want 6s", elapsed)
		}
		if most := sequential.peak(); most != 1 {
			t.Errorf("held %d handlers at once, want 1", most)
		}
		if order := sequential.order(); order != "orders ledger outbox" {
			t.Errorf("handlers ran %q, want the last added first", order)
		}
	})
}

// Groups run in reverse of the order AddShutdownGroup added them, the last added first, whatever
// order their handlers were added in, each starting as the one before it ends and costing its
// slowest handler
func TestGroupsRunLastAddedFirst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		called := newCalls()
		sleeping := func(delay time.Duration) shutdown.HandlerFunc {
			return func(context.Context) error {
				time.Sleep(delay)
				return nil
			}
		}
		var stopped <-chan time.Time
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			ledger := sh.AddShutdownGroup().SetName("LEDGER")
			pools := sh.AddShutdownGroup().SetName("DB_POOLS")
			payments := sh.AddShutdownGroup().SetName("PAYMENT_PROCESSING")
			ingress := sh.AddShutdownGroup().SetName("INGRESS_HTTP")
			called.add(ledger, "outbox", sleeping(time.Second))
			called.add(pools, "postgres", sleeping(2*time.Second))
			called.add(payments, "payments", sleeping(2*time.Second))
			called.add(ingress, "http", sleeping(time.Second))
			called.add(ledger, "ledger", sleeping(2*time.Second))
			called.add(pools, "dynamo", sleeping(4*time.Second))
			called.add(payments, "refunds", sleeping(time.Second))
			called.add(ingress, "grpc", sleeping(3*time.Second))
			stopped = stopWhenIdle(sh, nil)
			return nil
		})

		stopAt := <-stopped
		if result.code != 0 {
			t.Fatalf("exit code %d, want 0: %v", result.code, result.failure())
		}
		// INGRESS_HTTP for 3s, PAYMENT_PROCESSING for 2s, DB_POOLS for 4s, LEDGER for 2s
		want := map[string]time.Duration{
			"INGRESS_HTTP http":           0,
			"INGRESS_HTTP grpc":           0,
			"PAYMENT_PROCESSING payments": 3 * time.Second,
			"PAYMENT_PROCESSING refunds":  3 * time.Second,
			"DB_POOLS postgres":           5 * time.Second,
			"DB_POOLS dynamo":             5 * time.Second,
			"LEDGER outbox":               9 * time.Second,
			"LEDGER ledger":               9 * time.Second,
		}
		at := called.snapshot()
		for handler, after := range want {
			calledAt, ok := at[handler]
			switch {
			case !ok:
				t.Errorf("%s never called", handler)
			case calledAt.Sub(stopAt) != after:
				t.Errorf("%s called %s after the stop, want %s", handler, calledAt.Sub(stopAt), after)
			}
		}
		if len(at) != len(want) {
			t.Errorf("%d handlers called, want %d", len(at), len(want))
		}
		if elapsed := result.ended.Sub(stopAt); elapsed != 11*time.Second {
			t.Errorf("shutdown took %s, want 11s", elapsed)
		}
	})
}

// Handlers and Go functions added to a group, at AddShutdownGroup or later by Add and Go, all join
// that one group, called together, and are listed in the order added
func TestHandlersAndGoJoinTheirGroup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		together := &occupancy{}
		loopStopped := make(chan time.Time, 1)
		var stopped <-chan time.Time
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			sh.AddShutdownGroup(together.handler("outbox", time.Second)).
				SetName("CORE").
				Add(together.handler("refunds", time.Second)).
				Go(func(ctx context.Context) error {
					<-ctx.Done()
					loopStopped <- time.Now()
					return ctx.Err()
				}).
				Add(together.handler("ledger", time.Second))
			stopped = stopWhenIdle(sh, nil)
			return nil
		})

		stopAt := <-stopped
		if result.code != 0 {
			t.Fatalf("exit code %d, want 0: %v", result.code, result.failure())
		}
		if most := together.peak(); most != 3 {
			t.Errorf("CORE held %d handlers at once, want 3", most)
		}
		if elapsed := result.ended.Sub(stopAt); elapsed != time.Second {
			t.Errorf("CORE took %s, want 1s", elapsed)
		}
		select {
		case at := <-loopStopped:
			if at.Sub(stopAt) != 0 {
				t.Errorf("the Go function was told to stop %s after the stop, want as CORE began", at.Sub(stopAt))
			}
		default:
			t.Error("the Go function added to the group was never told to stop")
		}
		order := from(result.lines, "shutdown group 1 (CORE) with")
		if want := "shutdown group 1 (CORE) with outbox, refunds, " + goName + ", ledger"; len(order) == 0 || order[0].msg != want {
			t.Errorf("lines %q, want %q", result.messages(), want)
		}
	})
}

// After a SIGTERM the pause is logged at once, and the first group to stop begins exactly when it
// ends
func TestFirstGroupBeginsAsThePauseEnds(t *testing.T) {
	cases := []struct {
		name string
		opts []Option
	}{
		{"the default drain", nil},
		{"a 3s drain", []Option{WithDrainDelay(3 * time.Second)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				drain := processOptions(tc.opts...).drainDelay
				called := newCalls()
				var stopped <-chan time.Time
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					called.add(sh.AddShutdownGroup().SetName("POOLS"), "second", quick)
					called.add(sh.AddShutdownGroup().SetName("PAYMENTS"), "first", quick)
					stopped = stopWhenIdle(sh, sigterm)
					return nil
				}, tc.opts...)

				stopAt := <-stopped
				want := fmt.Sprintf("pausing for %s before shutdown", drain)
				var pauses []string
				for _, logged := range result.lines {
					if strings.HasPrefix(logged.msg, "pausing") {
						pauses = append(pauses, logged.msg)
						if !logged.at.Equal(stopAt) {
							t.Errorf("pause line logged %s after the stop, want at once", logged.at.Sub(stopAt))
						}
					}
				}
				if !slices.Equal(pauses, []string{want}) {
					t.Errorf("pause lines %q, want %q", pauses, want)
				}
				if calledAt, ok := called.snapshot()["PAYMENTS first"]; !ok || calledAt.Sub(stopAt) != drain {
					t.Errorf("PAYMENTS first called %v %s after the stop, want called as the %s drain ends", ok, calledAt.Sub(stopAt), drain)
				}
				if result.code != 0 {
					t.Errorf("exit code %d, want 0: %v", result.code, result.failure())
				}
			})
		})
	}
}

// Adding to a group from many goroutines while wire returns into a stop already requested never
// loses a handler: each is added and runs, or is refused and logged once
func TestAddRacesWithShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ran := &occupancy{}
		names := make([]string, 16)
		var stopped <-chan time.Time
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			group := sh.AddShutdownGroup().SetName("INGRESS")
			var adding sync.WaitGroup
			for index := range names {
				names[index] = fmt.Sprintf("racer%02d", index)
				add := func() {
					group.Add(ran.handler(names[index], time.Second))
				}
				if index < len(names)/2 {
					adding.Go(add) // added before wire returns
				} else {
					go add() // races wire's return and the shutdown
				}
			}
			adding.Wait()
			stopped = stopNow(sh, nil)
			return nil
		})

		if result.code != 0 {
			t.Fatalf("exit code %d, want 0: %v", result.code, result.failure())
		}
		entered := strings.Fields(ran.order())
		for index, name := range names {
			kept := slices.Contains(entered, name)
			if index < len(names)/2 && !kept {
				t.Errorf("%s was added during wiring but never ran", name)
			}
		}
		refusals := 0
		for _, logged := range result.lines {
			if logged.msg == "shutdown handler not added" && errors.Is(logged.err, errAddedLate) {
				refusals++
			}
		}
		if len(entered)+refusals != len(names) {
			t.Errorf("%d ran and %d were refused, want each of the %d either run or refused", len(entered), refusals, len(names))
		}
		if most := ran.peak(); most != len(entered) {
			t.Errorf("INGRESS held %d handlers at once, want all %d it ran", most, len(entered))
		}
		if elapsed := result.ended.Sub(<-stopped); elapsed != time.Second {
			t.Errorf("shutdown took %s, want the 1s every handler takes together", elapsed)
		}
	})
}

// A Go with a nil function once wire has returned is refused and logged once, and starts no
// goroutine: nothing else is logged and nothing stops the shell until a Stop issued once the bubble
// is idle, and Run returns 0
func TestGoNilFuncAddedLateStartsNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stoppingBeforeStop := make(chan bool, 1)
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			group := sh.AddShutdownGroup().SetName("INGRESS")
			go func() {
				synctest.Wait() // wire has returned and run waits for a stop
				group.Go(nil)
				synctest.Wait() // a goroutine Go started would have called nil and stopped the shell
				stoppingBeforeStop <- sh.Stopping()
				sh.Stop(nil)
			}()
			return nil
		})

		if result.code != 0 {
			t.Errorf("exit code %d, want 0: %v", result.code, result.failure())
		}
		if <-stoppingBeforeStop {
			t.Error("the shell was stopping before Stop, want nothing started by the late Go")
		}
		const refused = "goroutine not started"
		var beforeStop []string
		for _, logged := range result.lines {
			if strings.HasPrefix(logged.msg, "stopping:") {
				break
			}
			beforeStop = append(beforeStop, logged.msg)
		}
		if want := []string{"started orders", refused}; !slices.Equal(beforeStop, want) {
			t.Errorf("lines before the stop %q, want %q", beforeStop, want)
		}
		refusals := 0
		for _, logged := range result.lines {
			if logged.msg == refused {
				refusals++
				if !errors.Is(logged.err, errAddedLate) {
					t.Errorf("refusal error %v, want %v", logged.err, errAddedLate)
				}
			}
		}
		if refusals != 1 {
			t.Errorf("late Go refused %d times, want once: %q", refusals, result.messages())
		}
	})
}

// A Go function's ctx carries the values of Run's ctx, as every handler's does
func TestGoCtxCarriesRunsValues(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		carried := make(chan any, 1)
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			sh.AddShutdownGroup().Go(func(ctx context.Context) error {
				carried <- ctx.Value(runValue{})
				<-ctx.Done()
				return ctx.Err()
			})
			stopWhenIdle(sh, nil)
			return nil
		})

		if result.code != 0 {
			t.Errorf("exit code %d, want 0: %v", result.code, result.failure())
		}
		if value := <-carried; value != "run" {
			t.Errorf("Go function's ctx carried %v, want Run's value", value)
		}
	})
}
