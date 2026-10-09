// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/llingr/anvil/shutdown"
)

// toldStop is how one Go function was told to stop
type toldStop struct {
	at    time.Time
	err   error // its ctx's error
	cause error // context.Cause of its ctx
}

// stops records when each Go function was told to stop, as "<group name> <name>", where name is the
// test's own name for the function, and which of them had returned, locked because the functions
// record from their own goroutines
type stops struct {
	mu       sync.Mutex
	told     map[string]toldStop
	returned map[string]bool
}

func newStops() *stops {
	return &stops{told: map[string]toldStop{}, returned: map[string]bool{}}
}

// add starts a Go function in group that waits for its ctx to end, records the stop, then returns
// what then returns. A function never told to stop returns nil once release is closed, so the bubble
// can end even when nothing cancels its ctx. The group's name is read as it is added, so a test
// names the group first.
func (s *stops) add(group ShutdownGroup, name string, then work, release <-chan struct{}) {
	key := group.(*shutdownGroup).name + " " + name
	group.Go(func(ctx context.Context) error {
		defer s.locked(func() {
			s.returned[key] = true
		})
		select {
		case <-ctx.Done():
			s.locked(func() {
				s.told[key] = toldStop{at: time.Now(), err: ctx.Err(), cause: context.Cause(ctx)}
			})
			return then(ctx, release)
		case <-release:
			return nil
		}
	})
}

func (s *stops) locked(change func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	change()
}

func (s *stops) snapshot() map[string]toldStop {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Clone(s.told)
}

func (s *stops) hasReturned(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.returned[key]
}

// work is what a Go function does once told to stop. Closing release ends it early, so a test that
// fails, leaving it behind, still ends its bubble.
type work func(ctx context.Context, release <-chan struct{}) error

// returnsAfter finishes for delay, then returns its ctx's error, a clean stop
func returnsAfter(delay time.Duration) work {
	return func(ctx context.Context, release <-chan struct{}) error {
		select {
		case <-time.After(delay):
		case <-release:
		}
		return ctx.Err()
	}
}

// holds ignores the signal and returns only once release is closed
func holds(_ context.Context, release <-chan struct{}) error {
	<-release
	return nil
}

// A Go function is told to stop as its group begins, together with the group's handlers, and the
// group waits for it to return before the next group begins
func TestGoToldToStopAsItsGroupBegins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		defer synctest.Wait()
		defer close(release)

		called := newCalls()
		told := newStops()
		var stopped <-chan time.Time
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			// INGRESS for 3s, held by listener
			ingress := sh.AddShutdownGroup().SetName("INGRESS")
			called.add(ingress, "listener", func(context.Context) error {
				time.Sleep(3 * time.Second)
				return nil
			})
			told.add(ingress, "consumer", returnsAfter(0), release)
			told.add(ingress, "poller", returnsAfter(2*time.Second), release)
			// CORE for 4s, held by relay once told
			core := sh.AddShutdownGroup().SetName("CORE")
			called.add(core, "outbox", func(context.Context) error {
				time.Sleep(2 * time.Second)
				return nil
			})
			told.add(core, "relay", returnsAfter(4*time.Second), release)
			// EGRESS
			egress := sh.AddShutdownGroup().SetName("EGRESS")
			told.add(egress, "publisher", returnsAfter(0), release)
			called.add(egress, "pool", quick)
			stopped = stopWhenIdle(sh, nil)
			return nil
		})
		synctest.Wait()

		stopAt := <-stopped
		if result.code != 0 {
			t.Fatalf("exit code %d, want 0: %v", result.code, result.failure())
		}
		if elapsed := result.ended.Sub(stopAt); elapsed != 7*time.Second {
			t.Errorf("shutdown took %s, want 7s", elapsed)
		}
		wantTold := map[string]time.Duration{
			"INGRESS consumer": 0,
			"INGRESS poller":   0,
			"CORE relay":       3 * time.Second,
			"EGRESS publisher": 7 * time.Second,
		}
		toldAt := told.snapshot()
		for name, after := range wantTold {
			got, ok := toldAt[name]
			switch {
			case !ok:
				t.Errorf("%s never told to stop", name)
			case got.at.Sub(stopAt) != after:
				t.Errorf("%s told to stop %s after the stop, want %s", name, got.at.Sub(stopAt), after)
			case got.err != context.Canceled:
				t.Errorf("%s ctx ended with %v, want %v", name, got.err, context.Canceled)
			}
		}
		wantCalled := map[string]time.Duration{
			"INGRESS listener": 0,
			"CORE outbox":      3 * time.Second,
			"EGRESS pool":      7 * time.Second,
		}
		calledAt := called.snapshot()
		for name, after := range wantCalled {
			if at, ok := calledAt[name]; !ok || at.Sub(stopAt) != after {
				t.Errorf("%s called %v %s after the stop, want %s", name, ok, at.Sub(stopAt), after)
			}
		}
	})
}

// After a SIGTERM a Go function keeps running through the drain, serving the requests still routed
// to the pod, and is told to stop only when its group begins as the drain ends
func TestGoNotToldToStopDuringTheDrain(t *testing.T) {
	for _, stopping := range shutdownStops {
		t.Run(stopping.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				defer synctest.Wait()
				defer close(release)

				drain := time.Duration(0)
				if stopping.reason == sigterm {
					drain = processOptions(stopping.opts...).drainDelay
				}
				told := newStops()
				var stopped <-chan time.Time
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					told.add(sh.AddShutdownGroup().SetName("INGRESS"), "http", returnsAfter(0), release)
					told.add(sh.AddShutdownGroup().SetName("EGRESS"), "publisher", returnsAfter(0), release)
					stopped = stopWhenIdle(sh, stopping.reason)
					return nil
				}, stopping.opts...)
				synctest.Wait()

				stopAt := <-stopped
				if result.code != stopping.cleanCode {
					t.Errorf("exit code %d, want %d: %v", result.code, stopping.cleanCode, result.failure())
				}
				toldAt := told.snapshot()
				for _, name := range []string{"INGRESS http", "EGRESS publisher"} {
					if got, ok := toldAt[name]; !ok || got.at.Sub(stopAt) != drain {
						t.Errorf("%s told to stop %v %s after the stop, want as the %s drain ends", name, ok, got.at.Sub(stopAt), drain)
					}
				}
			})
		})
	}
}

// A Go function in a group the shutdown never reaches, behind a handler still running at the
// deadline, is told to stop when the shutdown ends, and Run returns without waiting for it
func TestUnreachedGoFunctionsToldToStopAtTheEnd(t *testing.T) {
	cases := []struct {
		name     string
		reason   error
		opts     []Option
		deadline time.Duration
		drain    time.Duration
	}{
		{"Stop", nil, nil, 28 * time.Second, 0},
		{"SIGTERM with a 10s grace period", sigterm, []Option{WithShutdownGracePeriod(10 * time.Second), WithDrainDelay(3 * time.Second)}, 10 * time.Second, 3 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				defer synctest.Wait()
				defer close(release)

				called := newCalls()
				told := newStops()
				var stopped <-chan time.Time
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					told.add(sh.AddShutdownGroup().SetName("INGRESS"), "consumer", returnsAfter(0), release)
					called.add(sh.AddShutdownGroup().SetName("CORE"), "outbox", ignoring(release))
					told.add(sh.AddShutdownGroup().SetName("RELAY"), "relay", holds, release)
					egress := sh.AddShutdownGroup().SetName("EGRESS")
					told.add(egress, "publisher", holds, release)
					called.add(egress, "pool", quick)
					stopped = stopWhenIdle(sh, tc.reason)
					return nil
				}, tc.opts...)

				stopAt := <-stopped
				if elapsed := result.ended.Sub(stopAt); elapsed != tc.deadline {
					t.Errorf("run returned %s after the stop, want the %s deadline", elapsed, tc.deadline)
				}
				if result.code != 1 {
					t.Errorf("exit code %d, want 1", result.code)
				}
				synctest.Wait() // the functions told to stop as run returned have recorded it
				wantTold := map[string]time.Duration{
					"INGRESS consumer": tc.drain,
					"RELAY relay":      tc.deadline,
					"EGRESS publisher": tc.deadline,
				}
				toldAt := told.snapshot()
				for name, after := range wantTold {
					if got, ok := toldAt[name]; !ok || got.at.Sub(stopAt) != after {
						t.Errorf("%s told to stop %v %s after the stop, want %s", name, ok, got.at.Sub(stopAt), after)
					}
				}
				for _, name := range []string{"RELAY relay", "EGRESS publisher"} {
					if told.hasReturned(name) {
						t.Errorf("%s returned, want it still running after run returned", name)
					}
				}
				if at, ok := called.snapshot()["EGRESS pool"]; ok {
					t.Errorf("EGRESS pool called %s after the stop, want it never called", at.Sub(stopAt))
				}
			})
		})
	}
}

// The cause of a Go function's cancellation carries the shutdown deadline, counted from the stop
// with the drain inside it, whether its group told it to stop or the shutdown's end did
func TestGoCauseCarriesTheDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		defer synctest.Wait()
		defer close(release)

		told := newStops()
		var stopped <-chan time.Time
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			told.add(sh.AddShutdownGroup().SetName("INGRESS"), "consumer", returnsAfter(0), release) // by its group
			sh.AddShutdownGroup(shutdown.Named("outbox", ignoring(release))).SetName("CORE")
			told.add(sh.AddShutdownGroup().SetName("EGRESS"), "publisher", returnsAfter(0), release) // by the shutdown's end
			stopped = stopWhenIdle(sh, sigterm)
			return nil
		}, WithShutdownGracePeriod(10*time.Second), WithDrainDelay(3*time.Second))
		synctest.Wait()

		stopAt := <-stopped
		if result.code != 1 {
			t.Errorf("exit code %d, want 1", result.code)
		}
		deadline := stopAt.Add(10 * time.Second)
		toldAt := told.snapshot()
		for name, after := range map[string]time.Duration{"INGRESS consumer": 3 * time.Second, "EGRESS publisher": 10 * time.Second} {
			got, ok := toldAt[name]
			if !ok {
				t.Errorf("%s never told to stop", name)
				continue
			}
			if got.at.Sub(stopAt) != after {
				t.Errorf("%s told to stop %s after the stop, want %s", name, got.at.Sub(stopAt), after)
			}
			if got.cause == nil || got.cause == context.Canceled {
				t.Errorf("%s cause %v, want one carrying the deadline", name, got.cause)
				continue
			}
			var carried stopCause
			if !errors.As(got.cause, &carried) {
				t.Errorf("%s cause %v (%T), want a stopCause", name, got.cause, got.cause)
				continue
			}
			if !carried.deadline.Equal(deadline) {
				t.Errorf("%s cause carried a deadline %s after the stop, want 10s", name, carried.deadline.Sub(stopAt))
			}
		}
	})
}

// A Go function that returns only when a separate handler shuts its server down works when both are
// in one group, called together, but with the handler in a later group the Go function's group waits
// for a return only the later handler can cause, so the shutdown waits for the deadline
func TestGoServeWithItsShutdownInALaterGroupWaitsForTheDeadline(t *testing.T) {
	cases := []struct {
		name     string
		apart    bool // whether the shutdown handler is in a group of its own, after the Go function's
		code     int
		took     time.Duration
		called   string // the shutdown handler as calls records it
		shutdown bool   // whether the separate shutdown handler is called
	}{
		{"one group", false, 0, 0, "SERVER shutdown", true},
		{"a later group", true, 1, 28 * time.Second, "LATER shutdown", false},
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
					serving := make(chan struct{}) // closed by server.Shutdown
					group := sh.AddShutdownGroup().SetName("SERVER")
					group.Go(func(context.Context) error {
						select {
						case <-serving: // Serve returned http.ErrServerClosed
						case <-release:
						}
						return http.ErrServerClosed
					})
					if tc.apart {
						group = sh.AddShutdownGroup().SetName("LATER")
					}
					called.add(group, "shutdown", func(context.Context) error {
						close(serving)
						return nil
					})
					stopped = stopWhenIdle(sh, nil)
					return nil
				})

				stopAt := <-stopped
				if result.code != tc.code {
					t.Errorf("exit code %d, want %d: %v", result.code, tc.code, result.failure())
				}
				if elapsed := result.ended.Sub(stopAt); elapsed != tc.took {
					t.Errorf("shutdown took %s, want %s", elapsed, tc.took)
				}
				if _, ok := called.snapshot()[tc.called]; ok != tc.shutdown {
					t.Errorf("%s called %v, want %v", tc.called, ok, tc.shutdown)
				}
			})
		})
	}
}

// A Go function that returns during the drain, after the stop and before its group begins, is not
// a stop reason: an error waits for its group, which reports it once under the group's label and
// the go record, and nil is a quiet end
func TestGoReturningAfterTheStopBeforeItsGroup(t *testing.T) {
	cases := []struct {
		name     string
		returned error
		code     int
		reported string // the error stopped with an error carries, "" for none
	}{
		{"an error", errors.New("commit failed"), 1, "group 1 (EGRESS) " + goName + ": commit failed"},
		{"nil", nil, 0, ""},
	}
	// the go record's own line, logged as an error when the function's error is reported
	const failedLine = "shutdown group 1 (EGRESS): " + goName + " failed after 0s"
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var returnedAt time.Time
				var stopped <-chan time.Time
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					sh.AddShutdownGroup().SetName("EGRESS").Go(func(context.Context) error {
						for !sh.Stopping() { // returns on its own, never reading its ctx
							time.Sleep(time.Second)
						}
						returnedAt = time.Now()
						return tc.returned
					})
					stopped = stopWhenIdle(sh, sigterm)
					return nil
				})

				stopAt := <-stopped
				if after := returnedAt.Sub(stopAt); after != time.Second {
					t.Errorf("publisher returned %s after the stop, want 1s, inside the 5s drain", after)
				}
				if result.code != tc.code {
					t.Errorf("exit code %d, want %d: %v", result.code, tc.code, result.failure())
				}
				for _, logged := range result.lines {
					switch {
					case strings.HasPrefix(logged.msg, "stopping:") && logged.msg != "stopping: "+sigterm.Error():
						t.Errorf("stop line %q, want the SIGTERM as the reason", logged.msg)
					case logged.msg == failedLine && logged.err != tc.returned:
						t.Errorf("logged %q: %v, want the function's error %v", logged.msg, logged.err, tc.returned)
					case logged.err != nil && logged.msg != "stopped with an error" && logged.msg != failedLine:
						t.Errorf("logged %q: %v, want nothing but the go record's line and the shutdown's error", logged.msg, logged.err)
					}
				}
				assertFailure(t, result, tc.reported)
			})
		})
	}
}

// The handler Go adds returns at its ctx's deadline when the function never returns, so the
// goroutine waiting for the function can exit wherever the process carries on after Run. The
// handler is called directly, on a shell that never runs, so that nothing else waits for the
// function's result.
func TestGoHandlerExitsAtTheDeadlineIfFnNeverReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		defer synctest.Wait()
		defer close(release)

		s := initShell[struct{}, struct{}]("orders", context.Background(), &lifecycle{}, struct{}{}, processOptions())
		s.AddShutdownGroup().Go(func(context.Context) error {
			<-release
			return nil
		})

		handler := s.groups[0].handlers[0].fn
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		returned := make(chan error, 1)
		go func() {
			returned <- handler.Shutdown(ctx)
		}()
		time.Sleep(5*time.Second - time.Nanosecond)
		synctest.Wait()
		if len(returned) != 0 {
			t.Fatal("handler returned before its deadline, want it waiting for the function until then")
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		select {
		case err := <-returned:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("handler returned %v, want %v", err, context.DeadlineExceeded)
			}
		default:
			t.Error("handler still waiting at its deadline for a function that never returns")
		}
	})
}

// handlerSaw is what a server's shutdown handler saw when it was called
type handlerSaw struct {
	at          time.Time
	deadline    time.Time // its own ctx's deadline
	hasDeadline bool
	err         error // its own ctx's error
}

// A server's shutdown handler and the Go function serving it, in one group, are called together as
// the group begins: the handler with a live ctx carrying the shutdown deadline, and the function
// told to stop at the same instant. The group waits for the function, whose http.ErrServerClosed
// after the stop is clean, before the next group begins.
func TestGoAndItsServersShutdownAreCalledTogether(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		defer synctest.Wait()
		defer close(release)

		saw := make(chan handlerSaw, 1)
		toldAt := make(chan time.Time, 1)
		var returnedAt time.Time
		called := newCalls()
		var stopped <-chan time.Time
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			serving := make(chan struct{}) // closed by the handler, as server.Shutdown ends Serve
			server := shutdown.Named("server", shutdown.HandlerFunc(func(ctx context.Context) error {
				deadline, ok := ctx.Deadline()
				saw <- handlerSaw{at: time.Now(), deadline: deadline, hasDeadline: ok, err: ctx.Err()}
				close(serving)
				return nil
			}))
			sh.AddShutdownGroup(server).SetName("INGRESS").Go(func(ctx context.Context) error {
				<-ctx.Done()
				toldAt <- time.Now()
				select {
				case <-serving:
				case <-release:
				}
				time.Sleep(2 * time.Second) // the requests in flight finish
				returnedAt = time.Now()
				return http.ErrServerClosed
			})
			called.add(sh.AddShutdownGroup().SetName("EGRESS"), "pool", quick)
			stopped = stopWhenIdle(sh, sigterm)
			return nil
		}, WithShutdownGracePeriod(10*time.Second), WithDrainDelay(3*time.Second))

		stopAt := <-stopped
		if result.code != 0 {
			t.Errorf("exit code %d, want 0: %v", result.code, result.failure())
		}
		var got handlerSaw
		select {
		case got = <-saw:
		default:
			t.Fatal("the server's handler never called")
		}
		if after := got.at.Sub(stopAt); after != 3*time.Second {
			t.Errorf("handler called %s after the stop, want 3s, as the drain ends", after)
		}
		if !got.hasDeadline || !got.deadline.Equal(stopAt.Add(10*time.Second)) {
			t.Errorf("handler ctx deadline %v %s after the stop, want 10s", got.hasDeadline, got.deadline.Sub(stopAt))
		}
		if got.err != nil {
			t.Errorf("handler ctx ended with %v, want it live", got.err)
		}
		if after := (<-toldAt).Sub(stopAt); after != 3*time.Second {
			t.Errorf("fn told to stop %s after the stop, want 3s, with the handler", after)
		}
		if after := returnedAt.Sub(stopAt); after != 5*time.Second {
			t.Errorf("fn returned %s after the stop, want 5s", after)
		}
		if at, ok := called.snapshot()["EGRESS pool"]; !ok || at.Sub(stopAt) != 5*time.Second {
			t.Errorf("EGRESS pool called %v %s after the stop, want 5s, once fn returned", ok, at.Sub(stopAt))
		}
	})
}

// A Go function's return once its group has started stopping does not count, whatever it is, as
// http.ErrServerClosed after server.Shutdown. Before that, an error or a cancellation is a failure:
// the stop's reason under the group's label when it comes first, otherwise reported by the group.
// A failing handler beside it is reported by the group, and a function that never ends is left
// running at the deadline.
func TestGoBesideItsServersShutdown(t *testing.T) {
	cases := []struct {
		name     string
		reason   error
		group    string
		failsAt  time.Duration // when set, fn fails this long after wire returns with failure
		failure  error
		stopErr  error
		ignored  bool // whether fn ignores the handler that would end it
		code     int
		reported string // the error stopped with an error carries, "" for none
	}{
		{name: "returns ErrServerClosed once stopped", group: "INGRESS"},
		{name: "fails before any stop", group: "INGRESS", failsAt: time.Second, failure: errors.New("listener closed"), code: 1, reported: "group 1 (INGRESS): listener closed"},
		{name: "fails during the drain", reason: sigterm, group: "INGRESS", failsAt: 3 * time.Second, failure: errors.New("listener closed"), code: 1, reported: "group 1 (INGRESS) " + goName + ": listener closed"},
		{name: "cancelled during the drain", reason: sigterm, group: "INGRESS", failsAt: 3 * time.Second, failure: fmt.Errorf("accept: %w", context.Canceled), code: 1, reported: "group 1 (INGRESS) " + goName + ": accept: context canceled"},
		{name: "fails while an earlier group runs", group: "EGRESS", failsAt: 3 * time.Second, failure: errors.New("listener closed"), code: 1, reported: "group 2 (EGRESS) " + goName + ": listener closed"},
		{name: "the server's shutdown fails", group: "INGRESS", stopErr: errors.New("shutdown failed"), code: 1, reported: "group 1 (INGRESS) server: shutdown failed"},
		{name: "the server's shutdown does not end fn", group: "INGRESS", ignored: true, code: 1, reported: "shutdown deadline 28s passed: group 1 (INGRESS) " + goName},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				defer synctest.Wait()
				defer close(release)

				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					serving := make(chan struct{}) // closed by the handler, as server.Shutdown ends Serve
					server := shutdown.Named("server", shutdown.HandlerFunc(func(context.Context) error {
						if !tc.ignored {
							close(serving)
						}
						return tc.stopErr
					}))
					serve := func(context.Context) error {
						if tc.failsAt > 0 {
							time.Sleep(tc.failsAt)
							return tc.failure
						}
						select {
						case <-serving:
							return http.ErrServerClosed
						case <-release:
							return nil
						}
					}
					outbox := shutdown.Named("outbox", sleepsThen(2*time.Second, nil))
					if tc.group == "INGRESS" {
						sh.AddShutdownGroup(server).SetName("INGRESS").Go(serve)
						sh.AddShutdownGroup(outbox).SetName("CORE")
					} else {
						sh.AddShutdownGroup(outbox).SetName("CORE")
						sh.AddShutdownGroup(server).SetName("EGRESS").Go(serve)
					}
					stopAfter(sh, 2*time.Second, tc.reason)
					return nil
				})

				if result.code != tc.code {
					t.Errorf("exit code %d, want %d: %v", result.code, tc.code, result.failure())
				}
				assertFailure(t, result, tc.reported)
				for _, logged := range result.lines {
					failedLine := strings.Contains(logged.msg, " failed after ")
					if logged.err != nil && logged.msg != "stopped with an error" && !failedLine && !tc.ignored {
						t.Errorf("logged %q: %v, want nothing but failed handlers' lines and the shutdown's error", logged.msg, logged.err)
					}
				}
			})
		})
	}
}

// Whatever a Go function returns once its group has started stopping does not count: an error, a
// panic, or nil before the ctx was even read, so a function that fails as it is stopped does not
// fail the shutdown
func TestGoReturnOnceItsGroupStopsDoesNotCount(t *testing.T) {
	cases := []struct {
		name string
		fn   func(ctx context.Context) error
	}{
		{"an error", func(ctx context.Context) error {
			<-ctx.Done()
			return errors.New("commit failed")
		}},
		{"a panic", func(ctx context.Context) error {
			<-ctx.Done()
			panic("kaboom")
		}},
		{"an error joined with the cancellation", func(ctx context.Context) error {
			<-ctx.Done()
			return errors.Join(errors.New("commit failed"), ctx.Err())
		}},
		{"nil", func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					sh.AddShutdownGroup().SetName("INGRESS").Go(tc.fn)
					stopWhenIdle(sh, nil)
					return nil
				})

				if result.code != 0 {
					t.Errorf("exit code %d, want 0: %v", result.code, result.failure())
				}
				assertLines(t, from(result.lines, "shutdown group 1 (INGRESS):"),
					"shutdown group 1 (INGRESS): "+goName+" done in 0s",
					cancellingWireCtx,
					"exiting orders",
				)
			})
		})
	}
}

// errMatchesCanceled says it is context.Canceled through its Is method, without wrapping it
type errMatchesCanceled struct{}

func (errMatchesCanceled) Error() string {
	return "matches context canceled"
}

func (errMatchesCanceled) Is(target error) bool {
	return target == context.Canceled
}

// errUnwrapsToNil has an Unwrap method that returns nil, which ends the chain
type errUnwrapsToNil struct{}

func (errUnwrapsToNil) Error() string {
	return "unwraps to nil"
}

func (errUnwrapsToNil) Unwrap() error {
	return nil
}

// An error has stopped cleanly when it is nil, or its chain of single wraps reaches
// context.Canceled. An error wrapping several at once is a failure even when one of them is the
// cancellation, so a real failure reported together with it is not hidden.
func TestStoppedCleanly(t *testing.T) {
	failed := errors.New("commit failed")
	cases := []struct {
		name  string
		err   error
		clean bool
	}{
		{"nil", nil, true},
		{"Canceled", context.Canceled, true},
		{"wrapped once", fmt.Errorf("orders consumer: %w", context.Canceled), true},
		{"wrapped twice", fmt.Errorf("orders consumer: %w", fmt.Errorf("fetch: %w", context.Canceled)), true},
		{"joined alone", errors.Join(context.Canceled), false},
		{"joined with a failure", errors.Join(failed, context.Canceled), false},
		{"joined, then wrapped once", fmt.Errorf("orders consumer: %w", errors.Join(context.Canceled)), false},
		{"two %w with a failure", fmt.Errorf("%w: %w", failed, context.Canceled), false},
		{"two %w of the cancellation", fmt.Errorf("%w: %w", context.Canceled, context.Canceled), false},
		{"DeadlineExceeded", context.DeadlineExceeded, false},
		{"DeadlineExceeded wrapped once", fmt.Errorf("commit: %w", context.DeadlineExceeded), false},
		{"the same text, not wrapped", errors.New(context.Canceled.Error()), false},
		{"Is matches Canceled", errMatchesCanceled{}, false},
		{"Unwrap returns nil", errUnwrapsToNil{}, false},
		{"Unwrap returns nil, wrapped once", fmt.Errorf("orders consumer: %w", errUnwrapsToNil{}), false},
		{"the stop's cause", stopCause{deadline: time.Now()}, true},
		{"the stop's cause, wrapped once", fmt.Errorf("orders consumer: %w", stopCause{}), true},
		{"the stop's cause, joined", errors.Join(stopCause{}), false},
		{"a failure", failed, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isStoppedCleanly(tc.err); got != tc.clean {
				t.Errorf("stoppedCleanly(%v) = %v, want %v", tc.err, got, tc.clean)
			}
		})
	}
}

// stopSeen is what StopContext gave a Go function
type stopSeen struct {
	at     time.Time       // when the function called StopContext
	ctx    context.Context // the ctx StopContext returned
	cancel context.CancelFunc
	err    error // that ctx's error as StopContext returned it
}

// seesStopContext is a Go function that, once told to stop, calls StopContext on its ctx, sends
// what it got on seen, and returns its ctx's error, a clean stop. It leaves the ctx StopContext
// returned to the test, which cancels it. Closing release ends it if it is never told to stop.
func seesStopContext(seen chan<- stopSeen, release <-chan struct{}) func(context.Context) error {
	return func(ctx context.Context) error {
		select {
		case <-ctx.Done():
		case <-release:
			return nil
		}
		stopCtx, cancel := StopContext(ctx)
		seen <- stopSeen{at: time.Now(), ctx: stopCtx, cancel: cancel, err: stopCtx.Err()}
		return ctx.Err()
	}
}

// received is what was sent on seen, failing the test if nothing was
func received(t *testing.T, seen <-chan stopSeen) stopSeen {
	t.Helper()
	select {
	case got := <-seen:
		return got
	default:
		t.Fatal("the Go function never called StopContext")
		return stopSeen{}
	}
}

// The ctx StopContext returns once a Go function is told to stop has the shutdown deadline,
// counted from the stop with the drain inside it, and ends at that instant
func TestStopContextHasTheShutdownDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		defer synctest.Wait()
		defer close(release)

		seen := make(chan stopSeen, 1)
		var stopped <-chan time.Time
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			sh.AddShutdownGroup().Go(seesStopContext(seen, release))
			stopped = stopWhenIdle(sh, sigterm)
			return nil
		}, WithShutdownGracePeriod(10*time.Second), WithDrainDelay(3*time.Second))

		stopAt := <-stopped
		if result.code != 0 {
			t.Errorf("exit code %d, want 0: %v", result.code, result.failure())
		}
		got := received(t, seen)
		defer got.cancel()
		if after := got.at.Sub(stopAt); after != 3*time.Second {
			t.Errorf("told to stop %s after the stop, want 3s, as the drain ends", after)
		}
		if got.err != nil {
			t.Errorf("stop ctx ended with %v as it was made, want it live", got.err)
		}
		deadline, ok := got.ctx.Deadline()
		if !ok || !deadline.Equal(stopAt.Add(10*time.Second)) {
			t.Fatalf("stop ctx deadline %v %s after the stop, want 10s", ok, deadline.Sub(stopAt))
		}

		time.Sleep(time.Until(deadline) - time.Nanosecond)
		synctest.Wait()
		if err := got.ctx.Err(); err != nil {
			t.Errorf("stop ctx ended with %v a nanosecond before the deadline, want it live", err)
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if err := got.ctx.Err(); err != context.DeadlineExceeded {
			t.Errorf("stop ctx error %v at the deadline, want %v", err, context.DeadlineExceeded)
		}
	})
}

// The ctx StopContext returns carries the values of the ctx passed to Run
func TestStopContextKeepsValues(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		defer synctest.Wait()
		defer close(release)

		seen := make(chan stopSeen, 1)
		bubbled(t, func(_ context.Context, sh bubbleShell) error {
			sh.AddShutdownGroup().Go(seesStopContext(seen, release))
			stopWhenIdle(sh, nil)
			return nil
		})

		got := received(t, seen)
		defer got.cancel()
		if value := got.ctx.Value(runValue{}); value != "run" {
			t.Errorf("stop ctx value %v, want run, from the ctx passed to Run", value)
		}
	})
}

// The ctx StopContext returns is not cancelled by the stop that told the function to stop, nor by
// the end of the shutdown, only by its own cancel or its deadline
func TestStopContextIsNotCancelledByTheStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		defer synctest.Wait()
		defer close(release)

		seen := make(chan stopSeen, 1)
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			sh.AddShutdownGroup().Go(seesStopContext(seen, release))
			stopWhenIdle(sh, nil)
			return nil
		})
		synctest.Wait() // anything the end of the shutdown cancels has been cancelled

		if result.code != 0 {
			t.Errorf("exit code %d, want 0: %v", result.code, result.failure())
		}
		got := received(t, seen)
		if got.err != nil {
			t.Errorf("stop ctx ended with %v as it was made, want it live", got.err)
		}
		select {
		case <-got.ctx.Done():
			t.Errorf("stop ctx ended with %v once the shutdown ended, want it live", got.ctx.Err())
		default:
		}
		got.cancel()
		if err := got.ctx.Err(); err != context.Canceled {
			t.Errorf("stop ctx error %v once cancelled, want %v", err, context.Canceled)
		}
	})
}

// liveWithoutDeadline checks that ctx, from StopContext, has no deadline, is live, carries the
// value runValue keys as want (nil for none), and ends when cancel is called
func liveWithoutDeadline(t *testing.T, name string, ctx context.Context, cancel context.CancelFunc, want any) {
	t.Helper()
	if deadline, ok := ctx.Deadline(); ok {
		t.Errorf("%s: stop ctx deadline %s, want none", name, deadline)
	}
	if err := ctx.Err(); err != nil {
		t.Errorf("%s: stop ctx ended with %v, want it live", name, err)
	}
	if value := ctx.Value(runValue{}); value != want {
		t.Errorf("%s: stop ctx value %v, want %v", name, value, want)
	}
	cancel()
	if err := ctx.Err(); err != context.Canceled {
		t.Errorf("%s: stop ctx error %v once cancelled, want %v", name, err, context.Canceled)
	}
}

// A ctx Go did not make, or a Go function's ctx before it is told to stop, gives a ctx with the
// same values, live and with no deadline, whatever the ctx's own deadline or cancellation
func TestStopContextWithoutGoOrBeforeTheStopHasNoDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		valued := context.WithValue(context.Background(), runValue{}, "host")
		cancelled, cancel := context.WithCancelCause(valued)
		cancel(errors.New("lease lost"))
		expired, cancelExpired := context.WithTimeout(valued, time.Second)
		defer cancelExpired()
		foreign := []struct {
			name string
			ctx  context.Context
			want any
		}{
			{"Background", context.Background(), nil},
			{"a ctx with a value", valued, "host"},
			{"a ctx cancelled with its own cause", cancelled, "host"},
			{"a ctx with its own deadline", expired, "host"},
		}
		for _, tc := range foreign {
			stopCtx, cancel := StopContext(tc.ctx)
			time.Sleep(2 * time.Second) // past the deadline of the ctx that has one
			synctest.Wait()
			liveWithoutDeadline(t, tc.name, stopCtx, cancel, tc.want)
		}

		release := make(chan struct{})
		defer synctest.Wait()
		defer close(release)
		before := make(chan stopSeen, 1)
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			sh.AddShutdownGroup().Go(func(ctx context.Context) error {
				stopCtx, cancel := StopContext(ctx)
				before <- stopSeen{at: time.Now(), ctx: stopCtx, cancel: cancel, err: stopCtx.Err()}
				<-ctx.Done()
				return ctx.Err()
			})
			stopWhenIdle(sh, nil)
			return nil
		})
		synctest.Wait()

		if result.code != 0 {
			t.Errorf("exit code %d, want 0: %v", result.code, result.failure())
		}
		got := received(t, before)
		if got.err != nil {
			t.Errorf("before the stop: stop ctx ended with %v as it was made, want it live", got.err)
		}
		liveWithoutDeadline(t, "before the stop, checked after it", got.ctx, got.cancel, "run")
	})
}

// derivedValue keys a value a Go function adds to its own ctx
type derivedValue struct{}

// derivation is a ctx a Go function derives from its own before the stop
type derivation struct {
	name     string
	derive   func(ctx context.Context) (context.Context, context.CancelFunc)
	valued   bool // whether the derived ctx carries derivedValue
	deadline bool // whether StopContext on it, after the stop, gives the shutdown deadline
}

// endedWithItsOwnCause derives a ctx from ctx that has already ended, before any stop, with a
// cause of its own
func endedWithItsOwnCause(ctx context.Context) (context.Context, context.CancelFunc) {
	derived, cancel := context.WithCancelCause(ctx)
	cancel(errors.New("lease lost"))
	return derived, func() {
	}
}

func withDerivedValue(ctx context.Context) context.Context {
	return context.WithValue(ctx, derivedValue{}, "derived")
}

// stopContextsOfDerived runs a Go function that derives each ctx from its own before the stop,
// and once told to stop calls StopContext on each. It checks that the derivations not ended
// first get the shutdown deadline, those ended first with their own cause get none, and that
// every one is live and keeps its values.
func stopContextsOfDerived(t *testing.T, derivations []derivation) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		defer synctest.Wait()
		defer close(release)

		seen := make(chan map[string]stopSeen, 1)
		var stopped <-chan time.Time
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			sh.AddShutdownGroup().Go(func(ctx context.Context) error {
				derived := map[string]context.Context{}
				for _, d := range derivations {
					ctx, cancel := d.derive(ctx)
					defer cancel()
					derived[d.name] = ctx
				}
				select {
				case <-ctx.Done():
				case <-release:
					return nil
				}
				got := map[string]stopSeen{}
				for name, ctx := range derived {
					// a cancelled ctx closes its Done before it cancels its children, so a function
					// passing on a derived ctx waits for that ctx to end, as one selecting on it would
					<-ctx.Done()
					stopCtx, cancel := StopContext(ctx)
					got[name] = stopSeen{at: time.Now(), ctx: stopCtx, cancel: cancel, err: stopCtx.Err()}
				}
				seen <- got
				return ctx.Err()
			})
			stopped = stopWhenIdle(sh, nil)
			return nil
		})

		stopAt := <-stopped
		if result.code != 0 {
			t.Errorf("exit code %d, want 0: %v", result.code, result.failure())
		}
		var got map[string]stopSeen
		select {
		case got = <-seen:
		default:
			t.Fatal("the Go function never called StopContext")
		}
		for _, d := range derivations {
			stop := got[d.name]
			defer stop.cancel()
			deadline, ok := stop.ctx.Deadline()
			switch {
			case d.deadline && (!ok || !deadline.Equal(stopAt.Add(28*time.Second))):
				t.Errorf("%s: stop ctx deadline %v %s after the stop, want 28s", d.name, ok, deadline.Sub(stopAt))
			case !d.deadline && ok:
				t.Errorf("%s: stop ctx deadline %s after the stop, want none", d.name, deadline.Sub(stopAt))
			}
			if stop.err != nil {
				t.Errorf("%s: stop ctx ended with %v as it was made, want it live", d.name, stop.err)
			}
			if value := stop.ctx.Value(runValue{}); value != "run" {
				t.Errorf("%s: stop ctx value %v, want run, from the ctx passed to Run", d.name, value)
			}
			if value, want := stop.ctx.Value(derivedValue{}), d.valued; (value == "derived") != want {
				t.Errorf("%s: stop ctx derived value %v, want it carried %v", d.name, value, want)
			}
		}
	})
}

// A ctx derived once from a Go function's ctx, and not ended before the stop, carries the stop's
// cause and so gets the shutdown deadline; one that ended first with a cause of its own gets none
func TestStopContextFromADerivedCtx(t *testing.T) {
	stopContextsOfDerived(t, []derivation{
		{"WithValue", func(ctx context.Context) (context.Context, context.CancelFunc) {
			return withDerivedValue(ctx), func() {
			}
		}, true, true},
		{"WithCancel", func(ctx context.Context) (context.Context, context.CancelFunc) {
			return context.WithCancel(ctx)
		}, false, true},
		{"WithTimeout longer than the shutdown", func(ctx context.Context) (context.Context, context.CancelFunc) {
			return context.WithTimeout(ctx, time.Hour)
		}, false, true},
		{"ended first with its own cause", endedWithItsOwnCause, false, false},
	})
}

// The same holds for a ctx derived twice, whichever derivation comes first
func TestStopContextFromADoublyDerivedCtx(t *testing.T) {
	stopContextsOfDerived(t, []derivation{
		{"WithValue of WithCancel", func(ctx context.Context) (context.Context, context.CancelFunc) {
			derived, cancel := context.WithCancel(ctx)
			return withDerivedValue(derived), cancel
		}, true, true},
		{"WithCancel of WithValue", func(ctx context.Context) (context.Context, context.CancelFunc) {
			return context.WithCancel(withDerivedValue(ctx))
		}, true, true},
		{"WithValue of one ended first with its own cause", func(ctx context.Context) (context.Context, context.CancelFunc) {
			derived, cancel := endedWithItsOwnCause(ctx)
			return withDerivedValue(derived), cancel
		}, true, false},
		{"WithCancel of one ended first with its own cause", func(ctx context.Context) (context.Context, context.CancelFunc) {
			derived, _ := endedWithItsOwnCause(ctx)
			return context.WithCancel(derived)
		}, false, false},
	})
}

// A Go function in a group the shutdown never reaches is told to stop as the shutdown ends, by the
// parent of every Go function's ctx, and StopContext gives it the shutdown deadline, already passed
func TestStopContextForAnUnreachedFunction(t *testing.T) {
	cases := []struct {
		name     string
		reason   error
		opts     []Option
		deadline time.Duration
		lateWire time.Duration // when set, wire stops the service itself and returns this long after
	}{
		{"Stop", nil, nil, 28 * time.Second, 0},
		{"SIGTERM with a 10s grace period", sigterm, []Option{WithShutdownGracePeriod(10 * time.Second), WithDrainDelay(3 * time.Second)}, 10 * time.Second, 0},
		{"Stop during a wire returning after the deadline", nil, nil, 28 * time.Second, 30 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				defer synctest.Wait()
				defer close(release)

				seen := make(chan stopSeen, 1)
				var stopped <-chan time.Time
				result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
					sh.AddShutdownGroup(shutdown.Named("outbox", ignoring(release)))
					sh.AddShutdownGroup().Go(seesStopContext(seen, release))
					if tc.lateWire == 0 {
						stopped = stopWhenIdle(sh, tc.reason)
						return nil
					}
					stopped = stopNow(sh, tc.reason)
					time.Sleep(tc.lateWire) // ignoring its ctx, so the shutdown ends as it returns
					return nil
				}, tc.opts...)
				synctest.Wait() // the function told to stop as run returned has called StopContext

				stopAt := <-stopped
				if result.code != 1 {
					t.Errorf("exit code %d, want 1", result.code)
				}
				got := received(t, seen)
				defer got.cancel()
				if after, want := got.at.Sub(stopAt), max(tc.deadline, tc.lateWire); after != want {
					t.Errorf("told to stop %s after the stop, want %s, as the shutdown ended", after, want)
				}
				deadline, ok := got.ctx.Deadline()
				if !ok || !deadline.Equal(stopAt.Add(tc.deadline)) {
					t.Errorf("stop ctx deadline %v %s after the stop, want %s", ok, deadline.Sub(stopAt), tc.deadline)
				}
				if got.err != context.DeadlineExceeded {
					t.Errorf("stop ctx error %v as it was made, want %v, its deadline passed", got.err, context.DeadlineExceeded)
				}
			})
		})
	}
}
