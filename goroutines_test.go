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

// An error from a goroutine Go started stops the service under its name, and Run returns 1
func TestGoErrorStops(t *testing.T) {
	logger := newRecordingLogger()
	code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
		shell.Go(shutdown.Ingress, "consumer", func(context.Context) error {
			return errors.New("broker unreachable")
		})
		return nil
	})
	if lines := logger.errorLines(); code != 1 || len(lines) != 1 || lines[0] != "stopped with an error: consumer: broker unreachable" {
		t.Fatalf("exit %d, errors %q, want 1 and the consumer's error", code, lines)
	}
}

// A loop returning a cancellation before any stop has failed, as a client library whose own ctx was
// cancelled does, so it is reported and Run returns 1
func TestGoCancelledBeforeStopFails(t *testing.T) {
	logger := newRecordingLogger()
	code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
		shell.Go(shutdown.Ingress, "consumer", func(context.Context) error {
			return fmt.Errorf("fetch: %w", context.Canceled)
		})
		return nil
	})
	if lines := logger.errorLines(); code != 1 || len(lines) != 1 || lines[0] != "stopped with an error: consumer: fetch: context canceled" {
		t.Fatalf("exit %d, errors %q, want 1 and the consumer's cancellation", code, lines)
	}
}

// A panic in a goroutine Go started stops the service through the usual shutdown, so the handlers
// registered in wire still run, where a bare go statement would crash the process
func TestGoPanicStopsAndHandlersRun(t *testing.T) {
	logger := newRecordingLogger()
	var flushed atomic.Bool
	code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
		shell.RegisterShutdownHandler(shutdown.Egress, "outbox", func(context.Context) error {
			flushed.Store(true)
			return nil
		})
		shell.Go(shutdown.Ingress, "consumer", func(context.Context) error {
			panic("kaboom")
		})
		return nil
	})
	lines := logger.errorLines()
	if code != 1 || !flushed.Load() || len(lines) != 1 || !strings.Contains(lines[0], "consumer panicked: kaboom") {
		t.Fatalf("exit %d, outbox flushed %v, errors %q, want 1, true and the panic", code, flushed.Load(), lines)
	}
}

// A goroutine that returns nil before any stop is logged as an error but does not stop the service:
// the stop comes from elsewhere, with its own reason
func TestGoNilReturnDoesNotStop(t *testing.T) {
	logger := newRecordingLogger()
	code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
		shell.Go(shutdown.Ingress, "one-off", func(context.Context) error {
			return nil
		})
		go func() {
			time.Sleep(100 * time.Millisecond)
			shell.Stop(errors.New("later"))
		}()
		return nil
	})
	want := []string{"one-off: returned nil before any stop", "stopped with an error: Stop called: later"}
	if lines := logger.errorLines(); code != 1 || !slices.Equal(lines, want) {
		t.Fatalf("exit %d, errors %q, want 1 and %q", code, lines, want)
	}
}

// A goroutine returning nil once told to stop has ended as it should, so nothing is logged
func TestGoNilReturnOnceStoppingIsQuiet(t *testing.T) {
	logger := newRecordingLogger()
	code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
		shell.Go(shutdown.Ingress, "consumer", func(ctx context.Context) error {
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

// The goroutine's phase waits for it to return, so the phase after it starts only once it has, and
// a goroutine returning its ctx's error once told to stop is a clean end
func TestGoPhaseWaitsForTheGoroutine(t *testing.T) {
	var finished, finishedBeforeEgress atomic.Bool
	code := anvil.Run(context.Background(), "test", quietConfig{}, quietLogger{}, func(ctx context.Context, shell quietShell) error {
		shell.Go(shutdown.Ingress, "consumer", func(ctx context.Context) error {
			<-ctx.Done()
			time.Sleep(100 * time.Millisecond) // finishing the message in hand
			finished.Store(true)
			return ctx.Err()
		})
		shell.RegisterShutdownHandler(shutdown.Egress, "pool", func(context.Context) error {
			finishedBeforeEgress.Store(finished.Load())
			return nil
		})
		shell.Stop(nil)
		return nil
	}, anvil.WithDrainDelay(0))
	if code != 0 || !finishedBeforeEgress.Load() {
		t.Fatalf("exit %d, consumer finished before EGRESS %v, want 0 and true", code, finishedBeforeEgress.Load())
	}
}

// A goroutine still running at its phase's deadline is reported as failed and left behind
func TestGoOverrunIsReported(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	logger := newRecordingLogger()
	code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
		shell.Go(shutdown.Ingress, "stuck", func(context.Context) error {
			<-release
			return nil
		})
		shell.Stop(nil)
		return nil
	}, shutdownWithin(300*time.Millisecond)...)
	lines := logger.errorLines()
	if code != 1 || len(lines) != 1 || !strings.Contains(lines[0], "INGRESS stuck: context deadline exceeded") {
		t.Fatalf("exit %d, errors %q, want 1 and the overrun reported", code, lines)
	}
}

// Go once wire has returned is refused as a late registration is, and its func never runs
func TestGoRefusedOnceWireReturns(t *testing.T) {
	logger := newRecordingLogger()
	var shell recordedShell
	var ran atomic.Bool
	finished := make(chan int)
	go func() {
		finished <- anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, wired recordedShell) error {
			shell = wired
			return nil
		})
	}()
	<-logger.started
	shell.Go(shutdown.Ingress, "late", func(context.Context) error {
		ran.Store(true)
		return nil
	})
	shell.Stop(nil)

	const refused = `shutdown handler "late" not registered: registrations are only permitted during startup`
	if code := <-finished; code != 0 || ran.Load() {
		t.Fatalf("exit %d, late func ran %v, want 0 and never run", code, ran.Load())
	}
	if lines := logger.errorLines(); len(lines) != 1 || !strings.HasPrefix(lines[0], refused) {
		t.Fatalf("errors %q, want the refusal of late", lines)
	}
}

// A nil func is a mistake in wire, which Run reports as wire's error
func TestGoNilFuncFailsWiring(t *testing.T) {
	logger := newRecordingLogger()
	code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
		shell.Go(shutdown.Ingress, "consumer", nil)
		return nil
	})
	if lines := logger.errorLines(); code != 1 || len(lines) != 1 || !strings.Contains(lines[0], "consumer has a nil func") {
		t.Fatalf("exit %d, errors %q, want 1 and the nil func reported", code, lines)
	}
}

// A loop is told to stop as its own phase begins, so an Egress publisher keeps running while
// Ingress and Core still produce work for it
func TestGoCtxEndsAsItsPhaseBegins(t *testing.T) {
	var ingressDone, ingressDoneAtStop atomic.Bool
	code := anvil.Run(context.Background(), "test", quietConfig{}, quietLogger{}, func(ctx context.Context, shell quietShell) error {
		shell.RegisterShutdownHandler(shutdown.Ingress, "consumer", func(context.Context) error {
			time.Sleep(50 * time.Millisecond)
			ingressDone.Store(true)
			return nil
		})
		shell.Go(shutdown.Egress, "publisher", func(ctx context.Context) error {
			<-ctx.Done()
			ingressDoneAtStop.Store(ingressDone.Load())
			return nil
		})
		shell.Stop(nil)
		return nil
	}, anvil.WithDrainDelay(0))
	if code != 0 || !ingressDoneAtStop.Load() {
		t.Fatalf("exit %d, INGRESS done when the publisher was told to stop %v, want 0 and true", code, ingressDoneAtStop.Load())
	}
}

// A loop that fails once told to stop fails the shutdown, reported in its phase as a handler is
func TestGoErrorDuringShutdownIsReported(t *testing.T) {
	logger := newRecordingLogger()
	code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
		shell.Go(shutdown.Ingress, "consumer", func(ctx context.Context) error {
			<-ctx.Done()
			return errors.New("commit failed")
		})
		shell.Stop(nil)
		return nil
	}, anvil.WithDrainDelay(0))
	if lines := logger.errorLines(); code != 1 || len(lines) != 1 || !strings.Contains(lines[0], "INGRESS consumer: commit failed") {
		t.Fatalf("exit %d, errors %q, want 1 and the failed commit", code, lines)
	}
}
