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

	"github.com/llingr/anvil/lifecycle"
	"github.com/llingr/anvil/lifecycle/shutdown"
)

// hostMain is the main a host application writes around Run
func hostMain(wiring lifecycle.Wiring, opts ...lifecycle.Option) {
	if err := lifecycle.New(opts...).Run(wiring); err != nil {
		log.Print("shutdown: ", err)
		os.Exit(1)
	}
}

// TestMain doubles as the child process for the exit code tests, selected by LIFECYCLE_HELPER
func TestMain(m *testing.M) {
	switch os.Getenv("LIFECYCLE_HELPER") {
	case "":
		os.Exit(m.Run())
	case "clean":
		hostMain(func(ctx context.Context, app lifecycle.Application) error {
			app.Stop(nil)
			return nil
		})
	case "setup-error":
		hostMain(func(ctx context.Context, app lifecycle.Application) error {
			return errors.New("boom")
		})
	case "block":
		hostMain(func(ctx context.Context, app lifecycle.Application) error {
			app.RegisterShutdownHandler("block", func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			}, shutdown.Default)
			fmt.Println("ready")
			return nil
		},
			lifecycle.WithShutdownPhaseBudget(shutdown.First, 200*time.Millisecond),
			lifecycle.WithShutdownPhaseBudget(shutdown.Default, 100*time.Millisecond),
			lifecycle.WithShutdownPhaseBudget(shutdown.Last, 100*time.Millisecond))
	case "stop-block":
		// Stop starts the shutdown, so a signal arrives with the operator yet to ask once, and
		// ready prints from inside the handler to put it squarely in the phase
		hostMain(func(ctx context.Context, app lifecycle.Application) error {
			app.RegisterShutdownHandler("block", func(ctx context.Context) error {
				fmt.Println("ready")
				<-ctx.Done()
				return ctx.Err()
			}, shutdown.Default)
			app.Stop(nil)
			return nil
		},
			lifecycle.WithShutdownPhaseBudget(shutdown.First, 200*time.Millisecond),
			lifecycle.WithShutdownPhaseBudget(shutdown.Default, time.Second),
			lifecycle.WithShutdownPhaseBudget(shutdown.Last, 100*time.Millisecond))
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
	return child.ProcessState.ExitCode(), stderr.String()
}

// Main exits 0 after a clean Stop and 1 when setup fails
func TestMainExitCodes(t *testing.T) {
	if code, output := runMain(t, "clean"); code != 0 || output != "" {
		t.Fatalf("clean exit %d: %s", code, output)
	}
	code, output := runMain(t, "setup-error")
	if code != 1 || !strings.Contains(output, "boom") {
		t.Fatalf("setup error exit %d: %s", code, output)
	}
}

// One SIGTERM shuts down through the phases; a second exits at once with 128+15
func TestMainSignals(t *testing.T) {
	code, output := runMain(t, "block", syscall.SIGTERM)
	if code != 1 || !strings.Contains(output, "deadline exceeded") {
		t.Fatalf("single SIGTERM exit %d: %s", code, output)
	}
	if code, output = runMain(t, "block", syscall.SIGTERM, syscall.SIGTERM); code != 143 {
		t.Fatalf("double SIGTERM exit %d: %s", code, output)
	}
}

// A Stop already shutting down absorbs the operator's first SIGTERM: a pod that stops itself as
// the kubelet's SIGTERM arrives still gets its phases
func TestMainSignalsDuringStopTriggeredShutdown(t *testing.T) {
	code, output := runMain(t, "stop-block", syscall.SIGTERM)
	if code != 1 || !strings.Contains(output, "deadline exceeded") {
		t.Fatalf("single SIGTERM after Stop exit %d: %s", code, output)
	}
	if code, output = runMain(t, "stop-block", syscall.SIGTERM, syscall.SIGTERM); code != 143 {
		t.Fatalf("double SIGTERM after Stop exit %d: %s", code, output)
	}
}
