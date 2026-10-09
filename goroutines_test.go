// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/anvil"
	"github.com/llingr/anvil/shutdown"
)

// An error from a goroutine Go started, before any stop, stops the service under its group's
// label, and Run returns 1
func TestGoErrorStops(t *testing.T) {
	logger := newRecordingLogger()
	code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
		shell.AddShutdownGroup().SetName("INGRESS").Go(func(context.Context) error {
			return errors.New("broker unreachable")
		})
		return nil
	})
	if lines := logger.errorLines(); code != 1 || len(lines) != 1 || lines[0] != "stopped with an error: group 1 (INGRESS): broker unreachable" {
		t.Fatalf("exit %d, errors %q, want 1 and the consumer's error", code, lines)
	}
}

// A loop returning a cancellation before any stop has failed, as a client library whose own ctx was
// cancelled does, so it is reported and Run returns 1
func TestGoCancelledBeforeStopFails(t *testing.T) {
	logger := newRecordingLogger()
	code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
		shell.AddShutdownGroup().Go(func(context.Context) error {
			return fmt.Errorf("fetch: %w", context.Canceled)
		})
		return nil
	})
	if lines := logger.errorLines(); code != 1 || len(lines) != 1 || lines[0] != "stopped with an error: group 1: fetch: context canceled" {
		t.Fatalf("exit %d, errors %q, want 1 and the consumer's cancellation", code, lines)
	}
}

// A panic in a goroutine Go started stops the service through the usual shutdown, so the handlers
// added in wire still run, where a bare go statement would crash the process
func TestGoPanicStopsAndHandlersRun(t *testing.T) {
	logger := newRecordingLogger()
	var flushed atomic.Bool
	code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
		shell.AddShutdownGroup().SetName("INGRESS").Go(func(context.Context) error {
			panic("kaboom")
		})
		shell.AddShutdownGroup(shutdown.HandlerFunc(func(context.Context) error {
			flushed.Store(true)
			return nil
		}))
		return nil
	})
	lines := logger.errorLines()
	if code != 1 || !flushed.Load() || len(lines) != 1 || !strings.HasPrefix(lines[0], "stopped with an error: group 1 (INGRESS): panic: kaboom\n") {
		t.Fatalf("exit %d, outbox flushed %v, errors %q, want 1, true and the panic", code, flushed.Load(), lines)
	}
}

// A goroutine that returns nil before any stop is logged under its group's label but does not stop
// the service: the stop comes from elsewhere, with its own reason
func TestGoNilReturnDoesNotStop(t *testing.T) {
	logger := newRecordingLogger()
	code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
		shell.AddShutdownGroup().SetName("ONE-OFF").Go(func(context.Context) error {
			return nil
		})
		go func() {
			time.Sleep(100 * time.Millisecond)
			shell.Stop(errors.New("later"))
		}()
		return nil
	})
	want := []string{"group 1 (ONE-OFF): returned nil before any stop", "stopped with an error: Stop called: later"}
	if lines := logger.errorLines(); code != 1 || !slices.Equal(lines, want) {
		t.Fatalf("exit %d, errors %q, want 1 and %q", code, lines, want)
	}
}

// A goroutine returning nil once told to stop has ended as it should, so nothing is logged
func TestGoNilReturnOnceStoppingIsQuiet(t *testing.T) {
	logger := newRecordingLogger()
	code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
		shell.AddShutdownGroup().Go(func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		})
		shell.Stop(nil)
		return nil
	}, anvil.WithDrainDelay(0))
	if lines := logger.errorLines(); code != 0 || len(lines) != 0 {
		t.Fatalf("exit %d, errors %q, want 0 and none", code, lines)
	}
}

// The goroutine's group waits for it to return, so the group after it starts only once it has, and
// a goroutine returning its ctx's error once told to stop is a clean end
func TestGoGroupWaitsForTheGoroutine(t *testing.T) {
	var finished, finishedBeforeNext atomic.Bool
	code := anvil.Run(context.Background(), "test", quietConfig{}, quietLogger{}, func(ctx context.Context, shell quietShell) error {
		shell.AddShutdownGroup().Go(func(ctx context.Context) error {
			<-ctx.Done()
			time.Sleep(100 * time.Millisecond) // finishing the message in hand
			finished.Store(true)
			return ctx.Err()
		})
		shell.AddShutdownGroup(shutdown.HandlerFunc(func(context.Context) error {
			finishedBeforeNext.Store(finished.Load())
			return nil
		}))
		shell.Stop(nil)
		return nil
	}, anvil.WithDrainDelay(0))
	if code != 0 || !finishedBeforeNext.Load() {
		t.Fatalf("exit %d, consumer finished before the next group %v, want 0 and true", code, finishedBeforeNext.Load())
	}
}

// A goroutine still running at the shutdown deadline is named in the deadline error and left running;
// its record's wait ends at the deadline, logged late with the ctx's error, in either order with
// Run's own error line
func TestGoOverrunIsReported(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	logger := newRecordingLogger()
	code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
		shell.AddShutdownGroup().SetName("INGRESS").Go(func(context.Context) error {
			<-release
			return nil
		})
		shell.AddShutdownGroup()
		shell.Stop(nil)
		return nil
	}, shutdownWithin(300*time.Millisecond)...)
	const stopped = "stopped with an error: shutdown deadline 300ms passed: group 1 (INGRESS) " + goName
	lines := logger.errorLines()
	if code != 1 || !slices.Contains(lines, stopped) {
		t.Fatalf("exit %d, errors %q, want 1 and the overrun reported", code, lines)
	}
	// the late line's time is measured in real time here; the synctest tests pin it exactly
	for _, line := range lines {
		late := strings.HasPrefix(line, "shutdown group 1 (INGRESS): "+goName+" errored ") &&
			strings.HasSuffix(line, " after the deadline: context deadline exceeded")
		if line != stopped && !late {
			t.Errorf("error line %q, want only the overrun and the go record's late line", line)
		}
	}
}

// A nil func is a mistake in wire, which Run reports as wire's error
func TestGoNilFuncFailsWiring(t *testing.T) {
	logger := newRecordingLogger()
	code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
		shell.AddShutdownGroup().SetName("INGRESS").Go(nil)
		return nil
	})
	lines := logger.errorLines()
	if code != 1 || len(lines) != 1 {
		t.Fatalf("exit %d, errors %q, want 1 and the nil func reported once", code, lines)
	}
	for _, named := range []string{"wiring failed: panic: anvil: ", "group 1 (INGRESS)", "nil function"} {
		if !strings.Contains(lines[0], named) {
			t.Errorf("error %q, want it to name %s", lines[0], named)
		}
	}
}

// A nil func passed once wire has returned is refused and logged like any late Go, and never panics
func TestGoNilFuncRefusedOnceWireReturns(t *testing.T) {
	logger := newRecordingLogger()
	var group anvil.ShutdownGroup
	finished := make(chan int)
	go func() {
		finished <- anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
			group = shell.AddShutdownGroup()
			go func() {
				<-logger.started
				group.Go(nil)
				shell.Stop(nil)
			}()
			return nil
		})
	}()

	const refused = "goroutine not started: shutdown groups can only be changed during wiring"
	if code := <-finished; code != 0 {
		t.Fatalf("exit %d, want 0", code)
	}
	if lines := logger.errorLines(); len(lines) != 1 || lines[0] != refused {
		t.Fatalf("errors %q, want the refusal of the late Go", lines)
	}
}

// A loop that fails once the service is stopping, before its group begins, fails the shutdown,
// reported by its group as a handler is
func TestGoErrorBeforeItsGroupBeginsIsReported(t *testing.T) {
	logger := newRecordingLogger()
	code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
		shell.AddShutdownGroup(shutdown.HandlerFunc(func(context.Context) error {
			time.Sleep(100 * time.Millisecond)
			return nil
		}))
		shell.AddShutdownGroup().SetName("INGRESS").Go(func(context.Context) error {
			for !shell.Stopping() {
				time.Sleep(time.Millisecond)
			}
			return errors.New("commit failed")
		})
		shell.Stop(nil)
		return nil
	}, anvil.WithDrainDelay(0))
	// real time, so the handler's own line is checked without its duration, which -race can push
	// from 0s to 1ms
	lines := logger.errorLines()
	if code != 1 || len(lines) != 2 ||
		!strings.HasPrefix(lines[0], "shutdown group 2 (INGRESS): "+goName+" failed after ") ||
		!strings.HasSuffix(lines[0], ": commit failed") ||
		lines[1] != "stopped with an error: group 2 (INGRESS) "+goName+": commit failed" {
		t.Fatalf("exit %d, errors %q, want 1, the go function's failed line and the final error", code, lines)
	}
}

// Whatever a loop returns once its group has told it to stop is a clean end, a failure joined with
// its ctx's cancellation included, as http.ErrServerClosed is after server.Shutdown
func TestGoReturnOnceToldToStopIsClean(t *testing.T) {
	cases := []struct {
		name     string
		returned func(ctx context.Context) error
	}{
		{"joined", func(ctx context.Context) error {
			return errors.Join(errors.New("commit failed"), ctx.Err())
		}},
		{"wrapped alone", func(ctx context.Context) error {
			return fmt.Errorf("orders consumer: %w", ctx.Err())
		}},
		{"the cause", context.Cause},
		{"a failure of its own", func(context.Context) error {
			return errors.New("commit failed")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger := newRecordingLogger()
			code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
				shell.AddShutdownGroup().Go(func(ctx context.Context) error {
					<-ctx.Done()
					return tc.returned(ctx)
				})
				shell.Stop(nil)
				return nil
			}, anvil.WithDrainDelay(0))
			if lines := logger.errorLines(); code != 0 || len(lines) != 0 {
				t.Fatalf("exit %d, errors %q, want 0 and none", code, lines)
			}
		})
	}
}
