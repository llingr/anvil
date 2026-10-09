// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
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

// readyLogging is chattyLogging that prints ready once Run logs started, so a signal sent on ready
// arrives after wire has returned
type readyLogging struct {
	chattyLogging
}

func (r readyLogging) LifecycleInfo(ctx context.Context, msg string) {
	r.chattyLogging.LifecycleInfo(ctx, msg)
	if msg == "started test" {
		fmt.Println("ready")
	}
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

// slowRequest is how long the http-server helper takes to answer a request
const slowRequest = 500 * time.Millisecond

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
			shell.AddShutdownGroup(shutdown.HandlerFunc(func(context.Context) error {
				log.Print("plain ran")
				return nil
			}))
			return errors.New("boom")
		})
	case "wiring-panic":
		hostMain(func(ctx context.Context, shell testShell) error {
			shell.AddShutdownGroup(shutdown.HandlerFunc(func(context.Context) error {
				log.Print("plain ran")
				return nil
			}))
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
			shell.AddShutdownGroup(shutdown.Named("stuck", shutdown.IgnoreContext(func() error {
				select {}
			}))).SetName("CORE")
			shell.Stop(nil)
			return nil
		},
			anvil.WithDrainDelay(0),
			anvil.WithShutdownGracePeriod(300*time.Millisecond))
	case "panic":
		hostMain(func(ctx context.Context, shell testShell) error {
			shell.AddShutdownGroup(
				shutdown.Named("bad", shutdown.HandlerFunc(func(context.Context) error {
					panic("kaboom")
				})),
				shutdown.Named("good", shutdown.HandlerFunc(func(context.Context) error {
					log.Print("good ran")
					return nil
				})),
			).SetName("CORE")
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
		// The 1s deadline ends 1s after the Stop, so wire returning 1.5s late finds it already spent
		os.Exit(anvil.Run(context.Background(), "test", noConfig{}, chattyLogging{}, func(_ context.Context, shell testShell) error {
			shell.AddShutdownGroup(shutdown.Named("consumer", shutdown.HandlerFunc(func(ctx context.Context) error {
				log.Print("consumer ran")
				<-ctx.Done()
				return ctx.Err()
			}))).SetName("INGRESS")
			shell.Stop(nil)
			time.Sleep(1500 * time.Millisecond)
			return nil
		}, anvil.WithDrainDelay(0), anvil.WithShutdownGracePeriod(time.Second)))
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
			shell.AddShutdownGroup(shutdown.Named("orders", shutdown.HandlerFunc(func(context.Context) error {
				return nil
			}))).SetName("CORE")
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
			shell.AddShutdownGroup(shutdown.Named("block", shutdown.HandlerFunc(func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			}))).SetName("CORE")
			shell.AddShutdownGroup().SetName("EGRESS")
			fmt.Println("ready")
			return nil
		},
			anvil.WithDrainDelay(0),
			anvil.WithShutdownGracePeriod(400*time.Millisecond))
	case "signal-during-load":
		os.Exit(anvil.Run(context.Background(), "test", readyHangingConfig{}, chattyLogging{}, neverStarted))
	case "blocking-wiring":
		os.Exit(anvil.Run(context.Background(), "test", noConfig{}, chattyLogging{}, func(context.Context, testShell) error {
			fmt.Println("ready")
			select {}
		}))
	case "drain", "drain-forced", "drain-sigint":
		exitCode := anvil.Run(context.Background(), "test", noConfig{}, readyLogging{}, func(ctx context.Context, shell testShell) error {
			shell.AddShutdownGroup(shutdown.Named("http server", shutdown.HandlerFunc(func(context.Context) error {
				log.Printf("ingress ran, wire ctx %v", ctx.Err())
				return nil
			}))).SetName("INGRESS")
			go func() {
				for !shell.Stopping() { // a readiness probe's view
					time.Sleep(10 * time.Millisecond)
				}
				log.Printf("not ready, wire ctx %v", ctx.Err())
			}()
			return nil
		}, anvil.WithDrainDelay(300*time.Millisecond))
		os.Exit(exitCode)
	case "sigterm-during-wiring", "sigint-during-wiring":
		// a slow start: wire waits on its ctx, as a dial or a migration would, so the signal arrives
		// while it runs
		os.Exit(anvil.Run(context.Background(), "test", noConfig{}, chattyLogging{}, func(ctx context.Context, shell testShell) error {
			shell.AddShutdownGroup(shutdown.Named("consumer", shutdown.HandlerFunc(func(context.Context) error {
				log.Print("consumer ran")
				return nil
			}))).SetName("INGRESS")
			fmt.Println("ready")
			<-ctx.Done()
			log.Print("wire saw its ctx cancelled")
			return ctx.Err()
		}, anvil.WithDrainDelay(time.Second), anvil.WithShutdownGracePeriod(3*time.Second)))
	case "stop-block":
		// Stop starts the shutdown, so a signal arrives with the operator yet to ask once, and
		// ready prints from inside the handler to put it squarely in the shutdown
		hostMain(func(ctx context.Context, shell testShell) error {
			shell.AddShutdownGroup(shutdown.Named("block", shutdown.HandlerFunc(func(ctx context.Context) error {
				fmt.Println("ready")
				<-ctx.Done()
				return ctx.Err()
			}))).SetName("CORE")
			shell.Stop(nil)
			return nil
		},
			anvil.WithDrainDelay(0),
			anvil.WithShutdownGracePeriod(1300*time.Millisecond))
	case "http-server":
		// prints its address, then ready from inside a request, so the SIGTERM finds it in flight; no
		// drain, so the server is stopped at once, by server.Shutdown in the group Serve's Go is in,
		// which lets the request finish while Serve returns http.ErrServerClosed
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			log.Fatal(err)
		}
		server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			fmt.Println("ready")
			time.Sleep(slowRequest)
			_, _ = fmt.Fprint(w, "finished") // a failed write shows as the test's wrong body
		})}
		fmt.Println(listener.Addr())
		hostMain(func(ctx context.Context, shell testShell) error {
			shell.AddShutdownGroup(server).Go(func(context.Context) error {
				return server.Serve(listener)
			})
			return nil
		},
			anvil.WithDrainDelay(0),
			anvil.WithShutdownGracePeriod(3*time.Second))
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
	hung := time.AfterFunc(30*time.Second, func() {
		_ = child.Process.Kill()
	}) // a hung child fails, as 137
	defer hung.Stop()
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
	return waitExit(t, child), stderr.String()
}

// waitExit waits for child and returns its exit code
func waitExit(t *testing.T, child *exec.Cmd) int {
	t.Helper()
	err := child.Wait()
	var exit *exec.ExitError
	if err != nil && !errors.As(err, &exit) {
		t.Fatal(err)
	}
	if status, ok := child.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		return 128 + int(status.Signal()) // bash reports a signal's kill the same way
	}
	return child.ProcessState.ExitCode()
}

// An HTTP server run by Go with server.Shutdown as its stop finishes the request in flight at
// SIGTERM, and its Serve returning http.ErrServerClosed is a clean stop, so the process exits 0
func TestMainHTTPServerFinishesInFlightRequest(t *testing.T) {
	child := exec.Command(os.Args[0])
	child.Env = append(os.Environ(), "LIFECYCLE_HELPER=http-server")
	var stderr bytes.Buffer
	child.Stderr = &stderr
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = child.Process.Kill()
	}() // a no-op once the child has exited

	lines := bufio.NewReader(stdout)
	addr, err := lines.ReadString('\n')
	if err != nil {
		t.Fatalf("child printed %q before its address: %v", addr, err)
	}
	type response struct {
		status int
		body   string
		err    error
	}
	responded := make(chan response, 1)
	go func() {
		client := http.Client{Timeout: 10 * time.Second}
		resp, err := client.Get("http://" + strings.TrimSpace(addr) + "/")
		if err != nil {
			responded <- response{err: err}
			return
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close() // read to the end, so nothing is left to report
		responded <- response{status: resp.StatusCode, body: string(body), err: err}
	}()
	if line, err := lines.ReadString('\n'); line != "ready\n" {
		t.Fatalf("child printed %q before the request was in flight: %v", line, err)
	}
	signalled := time.Now()
	if err = child.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}

	got := <-responded
	code := waitExit(t, child)
	if elapsed := time.Since(signalled); elapsed > 6*time.Second {
		t.Errorf("exit %s after the SIGTERM, want within twice the 3s deadline", elapsed)
	}
	if got.err != nil || got.status != http.StatusOK || got.body != "finished" {
		t.Errorf("in-flight request got %d %q, error %v, want 200 finished", got.status, got.body, got.err)
	}
	if code != 0 || stderr.Len() != 0 {
		t.Errorf("exit %d: %s, want 0 and nothing reported", code, stderr.String())
	}
}

// Main exits 0 after a clean Stop, and 1 when wire fails, after running what was added
func TestMainExitCodes(t *testing.T) {
	if code, output := runMain(t, "clean"); code != 0 || strings.Contains(output, "stopped with an error") {
		t.Fatalf("clean exit %d: %s", code, output)
	}
	code, output := runMain(t, "setup-error")
	if code != 1 || !strings.Contains(output, "stopped with an error: wiring failed: boom\n") || !strings.Contains(output, "plain ran") {
		t.Fatalf("setup error exit %d: %s", code, output)
	}
}

// A panicking wire is logged as an error, after running what it added, and exits 1
func TestMainWiringPanic(t *testing.T) {
	code, output := runMain(t, "wiring-panic")
	if code != 1 || !strings.Contains(output, "stopped with an error: wiring failed: panic: kaboom\ngoroutine ") ||
		!strings.Contains(output, "plain ran") || strings.Contains(output, "panicked") {
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

// A contextless handler still running at the shutdown deadline is named in the deadline error and
// left running
func TestMainDetachesAtDeadline(t *testing.T) {
	start := time.Now()
	code, output := runMain(t, "detach")
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("exit took %s, want exit at the deadline", elapsed)
	}
	if code != 1 || !strings.Contains(output, "stopped with an error: shutdown deadline 300ms passed: group 1 (CORE) stuck\n") {
		t.Fatalf("detach exit %d: %s", code, output)
	}
}

// A panicking handler becomes an error and the one beside it still runs
func TestMainPanicRecovered(t *testing.T) {
	code, output := runMain(t, "panic")
	if code != 1 || !strings.Contains(output, "group 1 (CORE) bad: panic: kaboom") || !strings.Contains(output, "good ran") {
		t.Fatalf("panic exit %d: %s", code, output)
	}
}

// A wiring error or panic is reported even when a stop was already under way and took the reason
func TestMainWiringFailureAfterStopIsReported(t *testing.T) {
	code, output := runMain(t, "stop-then-wiring-error")
	if code != 1 || !strings.Contains(output, "stopped with an error: wiring failed: boom\n") {
		t.Fatalf("wiring error after stop exit %d: %s", code, output)
	}
	code, output = runMain(t, "stop-then-wiring-panic")
	if code != 1 || !strings.Contains(output, "stopped with an error: wiring failed: panic: kaboom\ngoroutine ") {
		t.Fatalf("wiring panic after stop exit %d: %s", code, output)
	}
}

// The "stopping:" line names the stop that won, not wiring failing after it, which the error reports
// after the stop's own reason
func TestMainStoppingLineNamesTheFirstTrigger(t *testing.T) {
	code, output := runMain(t, "stop-then-wiring-error-logged")
	if code != 1 || stoppingLine(output) != "stopping: Stop called, waiting for wire to return" ||
		!strings.Contains(output, "stopped with an error: wiring failed: boom\n") {
		t.Fatalf("Stop then wiring error exit %d: %s", code, output)
	}
	code, output = runMain(t, "cancel-then-wiring-error-logged")
	if code != 1 || stoppingLine(output) != "stopping: the ctx passed to Run was cancelled: lease lost, waiting for wire to return" ||
		!strings.Contains(output, "stopped with an error: the ctx passed to Run was cancelled: lease lost\nwiring failed: boom\n") {
		t.Fatalf("cancel then wiring error exit %d: %s", code, output)
	}
}

// stoppingLine is the output's one line starting "stopping:", without the log's timestamp, or a
// note of how many there were
func stoppingLine(output string) string {
	var found []string
	for _, logged := range strings.Split(output, "\n") {
		if _, after, ok := strings.Cut(logged, " stopping: "); ok {
			found = append(found, "stopping: "+after)
		}
	}
	if len(found) != 1 {
		return fmt.Sprintf("%d stopping lines", len(found))
	}
	return found[0]
}

// The shutdown deadline runs from the stop, so a wire that returns after it has passed leaves the
// groups no time: no handler is called, and the shutdown does not run past the grace period
func TestMainLateWiringKeepsTheDeadline(t *testing.T) {
	start := time.Now()
	code, output := runMain(t, "late-wiring")
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("exit took %s, want the deadline kept from the stop, not restarted when wire returned", elapsed)
	}
	if code != 1 || strings.Contains(output, "consumer ran") || strings.Contains(output, "shutdown group") ||
		!strings.Contains(output, "stopped with an error: shutdown deadline 1s passed before group 1 (INGRESS) consumer\n") {
		t.Fatalf("late wiring exit %d, want 1 with no handler called: %s", code, output)
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

// anvil logs its lifecycle in order: starting, the configuration's load time, what stopped it,
// each group as it begins, each handler with its time as it returns, then exiting. Wire stops the
// application itself, so no started line is logged.
func TestMainLifecycleLines(t *testing.T) {
	code, output := runMain(t, "lifecycle-lines")
	if code != 0 || strings.Contains(output, "pausing") || strings.Contains(output, "started test") ||
		!inOrder(output, "starting test", "loading config", "configuration loaded in ",
			"stopping: Stop called", "shutdown group 1 (CORE) with orders\n", "shutdown group 1 (CORE): orders done in ",
			"exiting test") {
		t.Fatalf("lifecycle exit %d: %s", code, output)
	}
}

// A signal starts the drain: Ingress waits until the delay ends, and wire's ctx stays live through it
func TestMainDrainDelay(t *testing.T) {
	start := time.Now()
	code, output := runMain(t, "drain", syscall.SIGTERM)
	if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
		t.Fatalf("exit after %s, want the 300ms drain first", elapsed)
	}
	if code != 0 || !inOrder(output, "stopping: terminated signal received", "pausing for ", "ms before shutdown\n",
		"shutdown group 1 (INGRESS) with http server\n", "ingress ran, wire ctx <nil>",
		"shutdown group 1 (INGRESS): http server done in ") || !strings.Contains(output, "not ready, wire ctx <nil>") {
		t.Fatalf("drain exit %d: %s", code, output)
	}
}

// Ctrl+C runs the shutdown as Stop does, groups and all, but skips the drain's wait, and exits 130
// as the calling process expects of an interrupt
func TestMainSigintSkipsTheDrain(t *testing.T) {
	code, output := runMain(t, "drain-sigint", syscall.SIGINT)
	if code != 130 || strings.Contains(output, "pausing") ||
		!inOrder(output, "stopping: interrupt signal received", "shutdown group 1 (INGRESS) with http server\n", "ingress ran",
			"shutdown group 1 (INGRESS): http server done in ") {
		t.Fatalf("sigint exit %d: %s", code, output)
	}
}

// A SIGTERM during a slow start cancels wire's ctx at once, so wire returns its ctx's error, a
// clean stop: the rest of the drain runs, then the handlers wire added, and the process exits 0
// with no started line and nothing reported
func TestMainSigtermDuringWiringExitsClean(t *testing.T) {
	start := time.Now()
	code, output := runMain(t, "sigterm-during-wiring", syscall.SIGTERM)
	if elapsed := time.Since(start); elapsed > 8*time.Second {
		t.Errorf("exit took %s, want well within three times the 3s deadline", elapsed)
	}
	if code != 0 || strings.Contains(output, "started test") || strings.Contains(output, "stopped with an error") ||
		!inOrder(output, "stopping: terminated signal received, waiting for wire to return\n", "wire saw its ctx cancelled",
			"pausing for ", " before shutdown\n",
			"shutdown group 1 (INGRESS) with consumer\n", "consumer ran", "shutdown group 1 (INGRESS): consumer done in ",
			"exiting test") {
		t.Fatalf("sigterm during wiring exit %d: %s", code, output)
	}
	if !inOrder(output, "wire saw its ctx cancelled", "pausing for ") {
		t.Fatalf("wire returned before seeing its ctx cancelled: %s", output)
	}
}

// Ctrl+C during a slow start is as clean, with no drain, and exits 130
func TestMainSigintDuringWiringExits130(t *testing.T) {
	code, output := runMain(t, "sigint-during-wiring", syscall.SIGINT)
	if code != 130 || strings.Contains(output, "pausing") || strings.Contains(output, "started test") ||
		strings.Contains(output, "stopped with an error") ||
		!inOrder(output, "stopping: interrupt signal received, waiting for wire to return\n", "wire saw its ctx cancelled",
			"shutdown group 1 (INGRESS) with consumer\n", "consumer ran", "exiting test") {
		t.Fatalf("sigint during wiring exit %d: %s", code, output)
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

// A wire that blocks cannot hold the process: the stopping line says the shutdown is waiting for
// wire, and the second signal ends it
func TestMainBlockingWiringIsKilled(t *testing.T) {
	code, output := runMain(t, "blocking-wiring", syscall.SIGTERM, syscall.SIGTERM)
	if code != 143 || strings.Contains(output, "exiting test") ||
		!strings.Contains(output, "stopping: terminated signal received, waiting for wire to return\n") {
		t.Fatalf("blocking wiring exit %d: %s", code, output)
	}
}

// A second signal during the drain ends the process, as it does during the groups
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

// One SIGTERM shuts down through the groups; a second ends the process at once, 128+15 as bash reports it
func TestMainSignals(t *testing.T) {
	code, output := runMain(t, "block", syscall.SIGTERM)
	// the whole error is the deadline's, so block is not reported failing with its ctx's error
	if code != 1 || !strings.Contains(output, "stopped with an error: shutdown deadline 400ms passed: group 1 (CORE) block\n") {
		t.Fatalf("single SIGTERM exit %d: %s", code, output)
	}
	code, output = runMain(t, "block", syscall.SIGTERM, syscall.SIGTERM)
	if code != 143 {
		t.Fatalf("double SIGTERM exit %d: %s", code, output)
	}
}

// A Stop already shutting down takes the first SIGTERM as joining it, so a pod that stops itself as the
// kubelet's SIGTERM arrives still gets its groups; a second SIGTERM ends the process
func TestMainSignalsDuringStopTriggeredShutdown(t *testing.T) {
	code, output := runMain(t, "stop-block", syscall.SIGTERM)
	if code != 1 || !strings.Contains(output, "stopped with an error: shutdown deadline 1.3s passed: group 1 (CORE) block\n") {
		t.Fatalf("single SIGTERM after Stop exit %d: %s", code, output)
	}
	code, output = runMain(t, "stop-block", syscall.SIGTERM, syscall.SIGTERM)
	if code != 143 {
		t.Fatalf("double SIGTERM after Stop exit %d: %s", code, output)
	}
}
