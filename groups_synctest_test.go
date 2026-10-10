// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/llingr/anvil/shutdown"
)

// fakeServer is a handler with a pointer receiver, as *http.Server is
type fakeServer struct {
	called *atomic.Int32
}

func (f *fakeServer) Shutdown(context.Context) error {
	f.called.Add(1)
	return nil
}

// valueServer is a handler with a value receiver
type valueServer struct {
	called *atomic.Int32
}

func (v valueServer) Shutdown(context.Context) error {
	v.called.Add(1)
	return nil
}

// cache is a generic handler, whose type name carries its type argument
type cache[K comparable] struct {
	called *atomic.Int32
}

func (c *cache[K]) Shutdown(context.Context) error {
	c.called.Add(1)
	return nil
}

// A handler is named by shutdown.Named, else by its type as Go writes it: a pointer keeps its star,
// a value type has none, a generic type keeps its type argument, and a function or an adapter is
// shutdown.HandlerFunc. Go's record has its own name. Two handlers of one name in a group are both
// kept, and every one is called.
func TestHandlerNamesInTheLines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var called atomic.Int32
		count := shutdown.HandlerFunc(func(context.Context) error {
			called.Add(1)
			return nil
		})
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			sh.AddShutdownGroup(
				&fakeServer{called: &called},
				&fakeServer{called: &called},
				valueServer{called: &called},
				&cache[string]{called: &called},
				count,
				shutdown.IgnoreContext(func() error {
					return count(context.Background())
				}),
				shutdown.Named("outbox", count),
			).Go(func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			})
			stopWhenIdle(sh, nil)
			return nil
		})

		const begin = "shutdown group 1 with *anvil.fakeServer, *anvil.fakeServer, anvil.valueServer, *anvil.cache[string], shutdown.HandlerFunc, shutdown.HandlerFunc, outbox, " + goName
		if !slices.Contains(result.messages(), begin) {
			t.Errorf("lines %q, want %q", result.messages(), begin)
		}
		// every handler returns at the same instant, so their lines come in no set order
		var done []string
		for _, logged := range result.lines {
			if strings.HasPrefix(logged.msg, "shutdown group 1: ") {
				done = append(done, logged.msg)
			}
		}
		slices.Sort(done)
		want := []string{
			"shutdown group 1: *anvil.cache[string] done in 0s",
			"shutdown group 1: *anvil.fakeServer done in 0s",
			"shutdown group 1: *anvil.fakeServer done in 0s",
			"shutdown group 1: anvil.valueServer done in 0s",
			"shutdown group 1: " + goName + " done in 0s",
			"shutdown group 1: outbox done in 0s",
			"shutdown group 1: shutdown.HandlerFunc done in 0s",
			"shutdown group 1: shutdown.HandlerFunc done in 0s",
		}
		if !slices.Equal(done, want) {
			t.Errorf("handler lines\n%s\nwant\n%s", strings.Join(done, "\n"), strings.Join(want, "\n"))
		}
		if called.Load() != 7 {
			t.Errorf("%d handlers called, want all 7", called.Load())
		}
		if result.code != 0 {
			t.Errorf("exit code %d, want 0: %v", result.code, result.failure())
		}
	})
}

// A group is labelled by its position in the order the groups were added, from 1, not by when it
// stops, with the name SetName gave it in brackets, in every line and error; SetName may come after
// handlers are added
func TestGroupsAreLabelledByPosition(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			sh.AddShutdownGroup(shutdown.Named("postgres", shutdown.HandlerFunc(quick)))
			sh.AddShutdownGroup(shutdown.Named("payments", sleepsThen(time.Second, errors.New("refund lost")))).SetName("PAYMENTS")
			sh.AddShutdownGroup(shutdown.Named("http", shutdown.HandlerFunc(quick)))
			stopWhenIdle(sh, nil)
			return nil
		})

		assertLines(t, from(result.lines, "shutdown group 3 with"),
			"shutdown group 3 with http",
			"shutdown group 3: http done in 0s",
			"shutdown group 2 (PAYMENTS) with payments",
			errorLine("shutdown group 2 (PAYMENTS): payments failed after 1s", "refund lost"),
			"shutdown group 1 with postgres",
			"shutdown group 1: postgres done in 0s",
			cancellingWireCtx,
			errorLine("stopped with an error", "group 2 (PAYMENTS) payments: refund lost"),
			"exiting orders",
		)
	})
}

// Groups stop in reverse of the order they were added, as deferred calls run, so a service that adds
// each group as it opens what the group stops (a pool, then a worker using the pool, then a server
// feeding the worker) stops the server first and the pool last
func TestGroupsStopInReverseOfTheOrderAdded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			sh.AddShutdownGroup(shutdown.Named("postgres", shutdown.HandlerFunc(quick))).SetName("POOL")
			sh.AddShutdownGroup(shutdown.Named("consumer", shutdown.HandlerFunc(quick))).SetName("WORKER")
			sh.AddShutdownGroup(shutdown.Named("http", shutdown.HandlerFunc(quick))).SetName("SERVER")
			stopWhenIdle(sh, nil)
			return nil
		})

		assertLines(t, result.lines,
			"started orders",
			"stopping: Stop called",
			"shutdown group 3 (SERVER) with http",
			"shutdown group 3 (SERVER): http done in 0s",
			"shutdown group 2 (WORKER) with consumer",
			"shutdown group 2 (WORKER): consumer done in 0s",
			"shutdown group 1 (POOL) with postgres",
			"shutdown group 1 (POOL): postgres done in 0s",
			cancellingWireCtx,
			"exiting orders",
		)
		if result.code != 0 {
			t.Errorf("exit code %d, want 0: %v", result.code, result.failure())
		}
	})
}

// No name is checked: a group or a handler may have any name, empty or holding the characters the
// lines put around names, and two handlers of one name in one group are both called and logged
func TestNamesAreNotChecked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			sh.AddShutdownGroup().SetName("")
			sh.AddShutdownGroup(
				shutdown.Named("a->b; [x], y", sleepsThen(time.Millisecond, nil)),
				shutdown.Named("", sleepsThen(2*time.Millisecond, nil)),
				shutdown.Named("twice", sleepsThen(3*time.Millisecond, nil)),
				shutdown.Named("twice", sleepsThen(4*time.Millisecond, nil)),
			).SetName("DB POOLS: (x)")
			stopWhenIdle(sh, nil)
			return nil
		})

		assertLines(t, from(result.lines, "shutdown group 2"),
			"shutdown group 2 (DB POOLS: (x)) with a->b; [x], y, , twice, twice",
			"shutdown group 2 (DB POOLS: (x)): a->b; [x], y done in 1ms",
			"shutdown group 2 (DB POOLS: (x)):  done in 2ms",
			"shutdown group 2 (DB POOLS: (x)): twice done in 3ms",
			"shutdown group 2 (DB POOLS: (x)): twice done in 4ms",
			"shutdown group 1 with no handlers",
			cancellingWireCtx,
			"exiting orders",
		)
		if result.code != 0 {
			t.Errorf("exit code %d, want 0: %v", result.code, result.failure())
		}
	})
}

// Each handler logs its own line the moment it returns, so the handlers of one group log in the order
// they finish, not the order added, and a handler's time runs from its call: done in for one that
// returns nil, failed after, with its error, for one that fails
func TestHandlersLogAsTheyFinish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var stopped <-chan time.Time
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			sh.AddShutdownGroup(
				shutdown.Named("slow", sleepsThen(3*time.Second, nil)),
				shutdown.Named("fails", sleepsThen(2*time.Second, errors.New("flush failed"))),
				shutdown.Named("fast", sleepsThen(time.Second, nil)),
			).SetName("CORE")
			stopped = stopWhenIdle(sh, nil)
			return nil
		})

		stopAt := <-stopped
		assertLines(t, from(result.lines, "shutdown group 1 (CORE) with"),
			"shutdown group 1 (CORE) with slow, fails, fast",
			"shutdown group 1 (CORE): fast done in 1s",
			errorLine("shutdown group 1 (CORE): fails failed after 2s", "flush failed"),
			"shutdown group 1 (CORE): slow done in 3s",
			cancellingWireCtx,
			errorLine("stopped with an error", "group 1 (CORE) fails: flush failed"),
			"exiting orders",
		)
		for msg, after := range map[string]time.Duration{
			"shutdown group 1 (CORE): fast done in 1s":       time.Second,
			"shutdown group 1 (CORE): fails failed after 2s": 2 * time.Second,
			"shutdown group 1 (CORE): slow done in 3s":       3 * time.Second,
		} {
			if at := loggedAt(t, result.lines, msg, stopAt); at != after {
				t.Errorf("%q logged %s after the stop, want %s, as it returned", msg, at, after)
			}
		}
	})
}

// A handler still running at the deadline is left running, and logs its line whenever it returns,
// even after Run has returned: completed for nil, errored with its error, each with the time since
// the deadline rounded to the millisecond. Its record stays running, so the deadline error names it
// and the exit code is 1.
func TestLateHandlersLogAfterTheDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := &lifecycle{}
		var stopped <-chan time.Time
		result := bubbledWith(t, logger, func(_ context.Context, sh bubbleShell) error {
			sh.AddShutdownGroup(
				shutdown.Named("kafka", sleepsThen(28*time.Second+1200600*time.Microsecond, nil)),
				shutdown.Named("payments", sleepsThen(30500*time.Millisecond, errors.New("refund lost"))),
			).SetName("EGRESS")
			stopped = stopWhenIdle(sh, nil)
			return nil
		})

		stopAt := <-stopped
		if elapsed := result.ended.Sub(stopAt); elapsed != 28*time.Second {
			t.Errorf("run returned %s after the stop, want the 28s deadline", elapsed)
		}
		if result.code != 1 {
			t.Errorf("exit code %d, want 1", result.code)
		}
		assertFailure(t, result, "shutdown deadline 28s passed: group 1 (EGRESS) kafka, group 1 (EGRESS) payments")

		time.Sleep(3 * time.Second) // the host carries on after Run
		synctest.Wait()
		late := from(logger.recorded(), "exiting orders")
		assertLines(t, late,
			"exiting orders",
			"shutdown group 1 (EGRESS): kafka completed 1.201s after the deadline",
			errorLine("shutdown group 1 (EGRESS): payments errored 2.5s after the deadline", "refund lost"),
		)
		if len(late) == 3 {
			if at := late[1].at.Sub(stopAt); at != 28*time.Second+1200600*time.Microsecond {
				t.Errorf("kafka's line logged %s after the stop, want as it returned", at)
			}
		}
	})
}

// Once wire has returned, a group refuses every change, each logged once, and a refused Go never
// calls its function; an Add of no handlers changes nothing and so logs nothing
func TestRefusedChangesStartNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var ran atomic.Bool
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			group := sh.AddShutdownGroup()
			go func() {
				synctest.Wait() // wire has returned and run waits for a stop
				group.Add()
				group.Go(func(context.Context) error {
					ran.Store(true)
					return nil
				})
				late := sh.AddShutdownGroup()
				late.Go(func(context.Context) error {
					ran.Store(true)
					return nil
				})
				late.SetName("LATE")
				synctest.Wait() // a goroutine Go started would have run by now
				sh.Stop(nil)
			}()
			return nil
		})

		if ran.Load() {
			t.Error("a refused Go called its function")
		}
		if result.code != 0 {
			t.Errorf("exit code %d, want 0: %v", result.code, result.failure())
		}
		const refused = "shutdown groups can only be changed during wiring"
		assertLines(t, result.lines,
			"started orders",
			errorLine("goroutine not started", refused),
			errorLine("shutdown group not added", refused),
			errorLine("goroutine not started", refused),
			errorLine(`shutdown group "LATE" not named`, refused),
			"stopping: Stop called",
			"no components are registered for graceful shutdown",
			"shutdown group 1 with no handlers",
			cancellingWireCtx,
			"exiting orders",
		)
	})
}

// A Go function that returns nil before any stop is logged under its group's label as an error,
// once, and the service runs on until something else stops it
func TestGoNilBeforeAnyStopIsLogged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		result := bubbled(t, func(_ context.Context, sh bubbleShell) error {
			sh.AddShutdownGroup().SetName("ONE-OFF").Go(func(context.Context) error {
				time.Sleep(time.Millisecond) // once wire has returned
				return nil
			})
			stopAfter(sh, time.Second, nil)
			return nil
		})

		assertLines(t, result.lines,
			"started orders",
			errorLine("group 1 (ONE-OFF)", "returned nil before any stop"),
			"stopping: Stop called",
			"shutdown group 1 (ONE-OFF) with "+goName,
			"shutdown group 1 (ONE-OFF): "+goName+" done in 0s",
			cancellingWireCtx,
			"exiting orders",
		)
		if result.code != 0 {
			t.Errorf("exit code %d, want 0: %v", result.code, result.failure())
		}
	})
}
