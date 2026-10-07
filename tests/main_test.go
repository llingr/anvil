// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/llingr/anvil"
	"github.com/llingr/anvil/shutdown"
)

// stderrLogging stands in for an application's logger provider as a real one behaves: Error
// writes to stderr, so the exit code tests see what a host process reports
type stderrLogging struct{}

func (s stderrLogging) Logger() stderrLogging {
	return s
}

// LifecycleInfo is silent: stdout carries only the ready line the exit code tests wait for, and a clean
// exit must leave stderr empty
func (stderrLogging) LifecycleInfo(context.Context, string) {
}

func (stderrLogging) LifecycleError(_ context.Context, msg string, err error) {
	log.Print(msg, ": ", err)
}

func (stderrLogging) Flush() {
}

// noConfig stands in for an application's config provider
type noConfig struct{}

func (n noConfig) Load(context.Context) (noConfig, error) {
	return n, nil
}

// chattyLogging also writes Info lines to stderr
type chattyLogging struct {
	stderrLogging
}

func (chattyLogging) LifecycleInfo(_ context.Context, msg string) {
	log.Print(msg)
}

// brokenLogging panics as Run logs "exiting", outside wire, and reports its Flush on stderr
type brokenLogging struct {
	stderrLogging
}

func (brokenLogging) LifecycleInfo(_ context.Context, msg string) {
	if strings.HasPrefix(msg, "exiting") {
		panic("logger broke")
	}
}

func (brokenLogging) Flush() {
	log.Print("flushed")
}

// explodingConfig is a config provider that panics
type explodingConfig struct{}

func (explodingConfig) Load(context.Context) (noConfig, error) {
	panic("config exploded")
}

// readyHangingConfig announces it is loading, then hangs, so a signal arrives mid-load
type readyHangingConfig struct{}

func (readyHangingConfig) Load(context.Context) (noConfig, error) {
	fmt.Println("ready")
	time.Sleep(time.Hour) // a stalled read, which the runtime does not mistake for a deadlock as it would select {}
	return noConfig{}, nil
}

// unloadableConfig is a config provider whose source cannot be read
type unloadableConfig struct{}

func (unloadableConfig) Load(context.Context) (noConfig, error) {
	return noConfig{}, errors.New("config unreadable")
}

// hangingConfig is a config provider whose source never answers, giving up only as its ctx ends
type hangingConfig struct{}

func (hangingConfig) Load(ctx context.Context) (noConfig, error) {
	<-ctx.Done()
	return noConfig{}, ctx.Err()
}

// testShell is the shell those providers make
type testShell = anvil.Shell[noConfig, stderrLogging]

// hostMain is the main a host application writes around Run
func hostMain(wire anvil.Wiring[noConfig, stderrLogging], opts ...anvil.Option) {
	exitCode := anvil.Run(context.Background(), "test", noConfig{}, stderrLogging{}, wire, opts...)
	os.Exit(exitCode)
}

// neverStarted is the wiring for a helper whose Run returns before calling it
func neverStarted(context.Context, testShell) error {
	log.Print("started")
	return nil
}

// TestMain doubles as the child process for the exit code tests, selected by LIFECYCLE_HELPER
func TestMain(m *testing.M) {
	switch os.Getenv("LIFECYCLE_HELPER") {
	case "":
		os.Exit(m.Run())
	case "clean":
		hostMain(func(ctx context.Context, shell testShell) error {
			shell.Stop(nil)
			return nil
		})
	case "setup-error":
		hostMain(func(ctx context.Context, shell testShell) error {
			shell.RegisterShutdownHandler(shutdown.Core, "plain", func(context.Context) error {
				log.Print("plain ran")
				return nil
			})
			return errors.New("boom")
		})
	case "wiring-panic":
		hostMain(func(ctx context.Context, shell testShell) error {
			shell.RegisterShutdownHandler(shutdown.Core, "plain", func(context.Context) error {
				log.Print("plain ran")
				return nil
			})
			panic("kaboom")
		})
	case "run-panic":
		os.Exit(anvil.Run(context.Background(), "test", noConfig{}, brokenLogging{}, func(ctx context.Context, shell testShell) error {
			shell.Stop(nil)
			return nil
		}))
	case "load-panic":
		anvil.Run(context.Background(), "test", explodingConfig{}, brokenLogging{}, neverStarted)
	case "load-hangs":
		root, cancel := context.WithTimeoutCause(context.Background(), 200*time.Millisecond, errors.New("host gave up"))
		defer cancel()
		os.Exit(anvil.Run(root, "test", hangingConfig{}, stderrLogging{}, neverStarted))
	case "done-ctx":
		root, cancel := context.WithCancelCause(context.Background())
		cancel(errors.New("lease lost"))
		os.Exit(anvil.Run(root, "test", hangingConfig{}, chattyLogging{}, neverStarted))
	case "config-error":
		os.Exit(anvil.Run(context.Background(), "test", unloadableConfig{}, stderrLogging{}, neverStarted))
	case "detach":
		hostMain(func(ctx context.Context, shell testShell) error {
			shell.RegisterShutdownHandler(shutdown.Core, "stuck", shutdown.IgnoreContext(func() error {
				select {}
			}))
			shell.Stop(nil)
			return nil
		},
			anvil.WithDrainDelay(0),
			anvil.WithShutdownPhaseBudget(shutdown.Ingress, 100*time.Millisecond),
			anvil.WithShutdownPhaseBudget(shutdown.Core, 100*time.Millisecond),
			anvil.WithShutdownPhaseBudget(shutdown.Egress, 100*time.Millisecond))
	case "panic":
		hostMain(func(ctx context.Context, shell testShell) error {
			shell.RegisterShutdownHandler(shutdown.Core, "bad", func(context.Context) error {
				panic("kaboom")
			})
			shell.RegisterShutdownHandler(shutdown.Core, "good", func(context.Context) error {
				log.Print("good ran")
				return nil
			})
			shell.Stop(nil)
			return nil
		})
	case "stop-then-wiring-error":
		hostMain(func(ctx context.Context, shell testShell) error {
			shell.Stop(nil)
			return errors.New("boom")
		})
	case "stop-then-wiring-error-logged":
		os.Exit(anvil.Run(context.Background(), "test", noConfig{}, chattyLogging{}, func(ctx context.Context, shell testShell) error {
			shell.Stop(nil)
			return errors.New("boom")
		}))
	case "cancel-then-wiring-error-logged":
		root, cancel := context.WithCancelCause(context.Background())
		os.Exit(anvil.Run(root, "test", noConfig{}, chattyLogging{}, func(ctx context.Context, shell testShell) error {
			cancel(errors.New("lease lost"))
			for !shell.Stopping() { // the ctx's stop arrives from its own goroutine
				time.Sleep(time.Millisecond)
			}
			return errors.New("boom")
		}))
	case "late-wiring":
		// Ingress's 3s ends 3s after the Stop, so wire returning 2.5s late leaves it half a second
		os.Exit(anvil.Run(context.Background(), "test", noConfig{}, chattyLogging{}, func(_ context.Context, shell testShell) error {
			shell.RegisterShutdownHandler(shutdown.Ingress, "consumer", func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			})
			shell.Stop(nil)
			time.Sleep(2500 * time.Millisecond)
			return nil
		}, anvil.WithDrainDelay(0), anvil.WithShutdownDeadline(6*time.Second)))
	case "stop-then-wiring-panic":
		hostMain(func(ctx context.Context, shell testShell) error {
			shell.Stop(nil)
			panic("kaboom")
		})
	case "stop-twice":
		hostMain(func(ctx context.Context, shell testShell) error {
			shell.Stop(errors.New("boom"))
			shell.Stop(errors.New("late"))
			return nil
		})
	case "lifecycle-lines":
		os.Exit(anvil.Run(context.Background(), "test", noConfig{}, chattyLogging{}, func(ctx context.Context, shell testShell) error {
			shell.RegisterShutdownHandler(shutdown.Core, "orders", func(context.Context) error {
				return nil
			})
			shell.Stop(nil)
			return nil
		}))
	case "root-cancel-cause":
		root, cancel := context.WithCancelCause(context.Background())
		os.Exit(anvil.Run(root, "test", noConfig{}, chattyLogging{}, func(ctx context.Context, shell testShell) error {
			cancel(errors.New("lease lost"))
			return nil
		}))
	case "block":
		hostMain(func(ctx context.Context, shell testShell) error {
			shell.RegisterShutdownHandler(shutdown.Core, "block", func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			})
			fmt.Println("ready")
			return nil
		},
			anvil.WithDrainDelay(0),
			anvil.WithShutdownPhaseBudget(shutdown.Ingress, 200*time.Millisecond),
			anvil.WithShutdownPhaseBudget(shutdown.Core, 100*time.Millisecond),
			anvil.WithShutdownPhaseBudget(shutdown.Egress, 100*time.Millisecond))
	case "signal-during-load":
		os.Exit(anvil.Run(context.Background(), "test", readyHangingConfig{}, chattyLogging{}, neverStarted))
	case "blocking-wiring":
		os.Exit(anvil.Run(context.Background(), "test", noConfig{}, chattyLogging{}, func(context.Context, testShell) error {
			fmt.Println("ready")
			select {}
		}))
	case "drain", "drain-forced", "drain-sigint":
		exitCode := anvil.Run(context.Background(), "test", noConfig{}, chattyLogging{}, func(ctx context.Context, shell testShell) error {
			shell.RegisterShutdownHandler(shutdown.Ingress, "http server", func(context.Context) error {
				log.Printf("ingress ran, wire ctx %v", ctx.Err())
				return nil
			})
			go func() {
				for !shell.Stopping() { // a readiness probe's view
					time.Sleep(10 * time.Millisecond)
				}
				log.Printf("not ready, wire ctx %v", ctx.Err())
			}()
			fmt.Println("ready")
			return nil
		}, anvil.WithDrainDelay(300*time.Millisecond))
		os.Exit(exitCode)
	case "stop-block":
		// Stop starts the shutdown, so a signal arrives with the operator yet to ask once, and
		// ready prints from inside the handler to put it squarely in the phase
		hostMain(func(ctx context.Context, shell testShell) error {
			shell.RegisterShutdownHandler(shutdown.Core, "block", func(ctx context.Context) error {
				fmt.Println("ready")
				<-ctx.Done()
				return ctx.Err()
			})
			shell.Stop(nil)
			return nil
		},
			anvil.WithDrainDelay(0),
			anvil.WithShutdownPhaseBudget(shutdown.Ingress, 200*time.Millisecond),
			anvil.WithShutdownPhaseBudget(shutdown.Core, time.Second),
			anvil.WithShutdownPhaseBudget(shutdown.Egress, 100*time.Millisecond))
	}
}

// runMain runs this test binary as the named helper, sending signals once it prints ready
func runMain(t *testing.T, helper string, signals ...syscall.Signal) (int, string) {
	t.Helper()
	child := exec.Command(os.Args[0])
	child.Env = append(os.Environ(), "LIFECYCLE_HELPER="+helper)
	var stderr bytes.Buffer
	child.Stderr = &stderr
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	if len(signals) > 0 {
		if line, _ := bufio.NewReader(stdout).ReadString('\n'); line != "ready\n" {
			t.Fatalf("child printed %q before ready", line)
		}
		for _, sig := range signals {
			if err = child.Process.Signal(sig); err != nil {
				t.Fatal(err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	err = child.Wait()
	var exit *exec.ExitError
	if err != nil && !errors.As(err, &exit) {
		t.Fatal(err)
	}
	if status, ok := child.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		return 128 + int(status.Signal()), stderr.String() // bash reports a signal's kill the same way
	}
	return child.ProcessState.ExitCode(), stderr.String()
}

// Main exits 0 after a clean Stop, and 1 when wire fails, after running what was registered
func TestMainExitCodes(t *testing.T) {
	if code, output := runMain(t, "clean"); code != 0 || strings.Contains(output, "stopped with an error") {
		t.Fatalf("clean exit %d: %s", code, output)
	}
	code, output := runMain(t, "setup-error")
	if code != 1 || !strings.Contains(output, "boom") || !strings.Contains(output, "plain ran") {
		t.Fatalf("setup error exit %d: %s", code, output)
	}
}

// A panicking wire is logged as an error, after running what it registered, and exits 1
func TestMainWiringPanic(t *testing.T) {
	code, output := runMain(t, "wiring-panic")
	if code != 1 || !strings.Contains(output, "wiring panicked: kaboom") || !strings.Contains(output, "plain ran") {
		t.Fatalf("wiring panic exit %d: %s", code, output)
	}
}

// A panic escaping Run outside wire is logged and the logger flushed before it crashes the
// process with Go's exit code 2
func TestMainRunPanicFlushes(t *testing.T) {
	code, output := runMain(t, "run-panic")
	if code != 2 || !inOrder(output, "panicked: logger broke", "flushed") {
		t.Fatalf("run panic exit %d: %s", code, output)
	}
}

// A panic in the config provider is logged and the logger flushed before it crashes the process
// with Go's exit code 2
func TestMainLoadPanicFlushes(t *testing.T) {
	code, output := runMain(t, "load-panic")
	if code != 2 || !inOrder(output, "panicked: config exploded", "flushed") {
		t.Fatalf("load panic exit %d: %s", code, output)
	}
}

// A config provider that hangs gives up as the ctx passed to Run ends, and the load's failure is
// logged as any other
func TestMainLoadHangsUntilRunsCtxEnds(t *testing.T) {
	start := time.Now()
	code, output := runMain(t, "load-hangs")
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("exit took %s, want exit at the deadline", elapsed)
	}
	if code != 1 || !strings.Contains(output, "failed to load config: context deadline exceeded") {
		t.Fatalf("hanging load exit %d: %s", code, output)
	}
}

// A ctx already done reaches the config provider like any other, and one that honours it fails the
// load, so wire is never called
func TestMainDoneCtxFailsTheLoad(t *testing.T) {
	code, output := runMain(t, "done-ctx")
	if code != 1 || strings.Contains(output, "started") ||
		!inOrder(output, "starting test", "failed to load config: context canceled", "exiting test") {
		t.Fatalf("done ctx exit %d: %s", code, output)
	}
}

// A configuration that cannot be loaded makes Run return 1 without calling wire
func TestMainConfigError(t *testing.T) {
	code, output := runMain(t, "config-error")
	if code != 1 || !strings.Contains(output, "config unreadable") || strings.Contains(output, "started") {
		t.Fatalf("config error exit %d: %s", code, output)
	}
}

// A contextless handler still running at the deadline fails and is left behind
func TestMainDetachesAtDeadline(t *testing.T) {
	start := time.Now()
	code, output := runMain(t, "detach")
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("exit took %s, want exit at the deadline", elapsed)
	}
	if code != 1 || !strings.Contains(output, "CORE stuck: context deadline exceeded") {
		t.Fatalf("detach exit %d: %s", code, output)
	}
}

// A panicking handler becomes an error and the next one still runs
func TestMainPanicRecovered(t *testing.T) {
	code, output := runMain(t, "panic")
	if code != 1 || !strings.Contains(output, "CORE bad: panic: kaboom") || !strings.Contains(output, "good ran") {
		t.Fatalf("panic exit %d: %s", code, output)
	}
}

// A wiring error or panic is reported even when a stop was already under way and took the reason
func TestMainWiringFailureAfterStopIsReported(t *testing.T) {
	code, output := runMain(t, "stop-then-wiring-error")
	if code != 1 || !strings.Contains(output, "stopped with an error: boom") {
		t.Fatalf("wiring error after stop exit %d: %s", code, output)
	}
	code, output = runMain(t, "stop-then-wiring-panic")
	if code != 1 || !strings.Contains(output, "wiring panicked: kaboom") {
		t.Fatalf("wiring panic after stop exit %d: %s", code, output)
	}
}

// The "stopping:" line names the stop that won, not wiring failing after it
func TestMainStoppingLineNamesTheFirstTrigger(t *testing.T) {
	code, output := runMain(t, "stop-then-wiring-error-logged")
	if code != 1 || !strings.Contains(output, "stopping: Stop called\n") || strings.Contains(output, "wiring failed") {
		t.Fatalf("Stop then wiring error exit %d: %s", code, output)
	}
	code, output = runMain(t, "cancel-then-wiring-error-logged")
	if code != 1 || !strings.Contains(output, "stopping: the ctx passed to Run was cancelled: lease lost") ||
		strings.Contains(output, "wiring failed") {
		t.Fatalf("cancel then wiring error exit %d: %s", code, output)
	}
}

// The shutdown deadline runs from the stop, so a wire that returns late leaves the phases less time
// rather than pushing the shutdown past the grace period
func TestMainLateWiringKeepsTheDeadline(t *testing.T) {
	start := time.Now()
	code, output := runMain(t, "late-wiring")
	if elapsed := time.Since(start); elapsed > 4500*time.Millisecond {
		t.Fatalf("exit took %s, want Ingress ended 3s after the stop, not 3s after wire returned", elapsed)
	}
	if code != 1 || !strings.Contains(output, "INGRESS consumer: context deadline exceeded") {
		t.Fatalf("late wiring exit %d: %s", code, output)
	}
}

// Stop's first reason is the one reported
func TestMainStopFirstReasonWins(t *testing.T) {
	code, output := runMain(t, "stop-twice")
	if code != 1 || !strings.Contains(output, "boom") || strings.Contains(output, "late") {
		t.Fatalf("stop twice exit %d: %s", code, output)
	}
}

// Cancelling Run's ctx with a cause stops as Stop(cause) does: reported, exit 1
func TestMainRootCancelCauseIsReported(t *testing.T) {
	code, output := runMain(t, "root-cancel-cause")
	if code != 1 || !inOrder(output, "stopping: the ctx passed to Run was cancelled: lease lost",
		"stopped with an error: the ctx passed to Run was cancelled: lease lost", "exiting test") {
		t.Fatalf("root cancel exit %d: %s", code, output)
	}
}

// anvil logs its lifecycle in order: starting, the configuration's load time, what stopped it, each
// phase that ran with its handlers' times, then exiting
func TestMainLifecycleLines(t *testing.T) {
	code, output := runMain(t, "lifecycle-lines")
	if code != 0 || !inOrder(output, "starting test", "loading config", "configuration loaded in ", "started test", "stopping: Stop called",
		"CORE done in ", ": orders ", "exiting test") {
		t.Fatalf("lifecycle exit %d: %s", code, output)
	}
}

// A signal starts the drain: wire's ctx stays live and Ingress waits until the delay ends
func TestMainDrainDelay(t *testing.T) {
	start := time.Now()
	code, output := runMain(t, "drain", syscall.SIGTERM)
	if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
		t.Fatalf("exit after %s, want the 300ms drain first", elapsed)
	}
	if code != 0 || !inOrder(output, "stopping: terminated signal received", "draining for 300ms before INGRESS",
		"ingress ran, wire ctx context canceled", "INGRESS done in ") || !strings.Contains(output, "not ready, wire ctx <nil>") {
		t.Fatalf("drain exit %d: %s", code, output)
	}
}

// Ctrl+C runs the shutdown as Stop does, phases and all, but skips the drain's wait, and exits 130
// as the calling process expects of an interrupt
func TestMainSigintSkipsTheDrain(t *testing.T) {
	code, output := runMain(t, "drain-sigint", syscall.SIGINT)
	if code != 130 || strings.Contains(output, "draining") ||
		!inOrder(output, "stopping: interrupt signal received", "ingress ran", "INGRESS done in ") {
		t.Fatalf("sigint exit %d: %s", code, output)
	}
}

// Signals are trapped only once the configuration has loaded, so one during the load ends the
// process at once, as it would any program, and the last line says what was hanging
func TestMainSignalDuringLoad(t *testing.T) {
	for _, received := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT} {
		code, output := runMain(t, "signal-during-load", received)
		if code != 128+int(received) || !strings.HasSuffix(output, "loading config\n") {
			t.Fatalf("%s during load exit %d: %s", received, code, output)
		}
	}
}

// A wire that blocks cannot hold the process: the second signal ends it
func TestMainBlockingWiringIsKilled(t *testing.T) {
	code, output := runMain(t, "blocking-wiring", syscall.SIGTERM, syscall.SIGTERM)
	if code != 143 || strings.Contains(output, "exiting test") {
		t.Fatalf("blocking wiring exit %d: %s", code, output)
	}
}

// A second signal during the drain ends the process, as it does during the phases
func TestMainDrainDelaySecondSignalKills(t *testing.T) {
	code, output := runMain(t, "drain-forced", syscall.SIGTERM, syscall.SIGTERM)
	if code != 143 || strings.Contains(output, "ingress ran") || strings.Contains(output, "exiting test") {
		t.Fatalf("forced drain exit %d: %s", code, output)
	}
}

// inOrder reports whether each fragment appears in output after the one before it
func inOrder(output string, fragments ...string) bool {
	from := 0
	for _, fragment := range fragments {
		index := strings.Index(output[from:], fragment)
		if index < 0 {
			return false
		}
		from += index + len(fragment)
	}
	return true
}

// One SIGTERM shuts down through the phases; a second ends the process at once, 128+15 as bash reports it
func TestMainSignals(t *testing.T) {
	code, output := runMain(t, "block", syscall.SIGTERM)
	if code != 1 || !strings.Contains(output, "deadline exceeded") {
		t.Fatalf("single SIGTERM exit %d: %s", code, output)
	}
	code, output = runMain(t, "block", syscall.SIGTERM, syscall.SIGTERM)
	if code != 143 {
		t.Fatalf("double SIGTERM exit %d: %s", code, output)
	}
}

// A Stop already shutting down takes the first SIGTERM as joining it, so a pod that stops itself as the
// kubelet's SIGTERM arrives still gets its phases; a second SIGTERM ends the process
func TestMainSignalsDuringStopTriggeredShutdown(t *testing.T) {
	code, output := runMain(t, "stop-block", syscall.SIGTERM)
	if code != 1 || !strings.Contains(output, "deadline exceeded") {
		t.Fatalf("single SIGTERM after Stop exit %d: %s", code, output)
	}
	code, output = runMain(t, "stop-block", syscall.SIGTERM, syscall.SIGTERM)
	if code != 143 {
		t.Fatalf("double SIGTERM after Stop exit %d: %s", code, output)
	}
}
