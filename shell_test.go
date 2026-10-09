// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/llingr/anvil"
	"github.com/llingr/anvil/shutdown"
)

// panickingLogger stands in for an application's logger provider: Error panics, so a reported
// failure fails the test at once instead of exiting the test binary; tests/ covers the error paths
type panickingLogger struct{}

func (p panickingLogger) Logger() panickingLogger {
	return p
}

func (panickingLogger) LifecycleInfo(context.Context, string) {
}

func (panickingLogger) LifecycleError(_ context.Context, msg string, err error) {
	panic(fmt.Sprintf("%s: %v", msg, err))
}

func (panickingLogger) Flush() {
}

// noConfig stands in for an application's config provider
type noConfig struct{}

func (n noConfig) Load(context.Context) (noConfig, error) {
	return n, nil
}

// testShell is the shell those providers make
type testShell = anvil.Shell[noConfig, panickingLogger]

type contextless struct {
	called bool
}

func (c *contextless) Shutdown() error {
	c.called = true
	return nil
}

// shutdownWithin gives the shutdown one deadline of total, with no drain taking any of it
func shutdownWithin(total time.Duration) []anvil.Option {
	return []anvil.Option{
		anvil.WithDrainDelay(0),
		anvil.WithShutdownGracePeriod(total),
	}
}

// goName is the name a group's Go gives its record in the log lines, kept here alone as it may change
const goName = "go"

// quick is a handler that returns nil at once
func quick(context.Context) error {
	return nil
}

// run wires and stops at once; a reported failure panics through panickingLogger
func run(wire func(shell testShell), options ...anvil.Option) {
	anvil.Run(context.Background(), "test", noConfig{}, panickingLogger{}, func(ctx context.Context, shell testShell) error {
		wire(shell)
		shell.Stop(nil)
		return nil
	}, options...)
}

// A Stop reason wrapping context.Canceled is a failure, as any other reason is
func TestStopReasonCancelledFails(t *testing.T) {
	code := anvil.Run(context.Background(), "test", quietConfig{}, quietLogger{}, func(ctx context.Context, shell quietShell) error {
		shell.Stop(fmt.Errorf("consumer loop: %w", context.Canceled))
		return nil
	})
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
}

// Stop never blocks its caller, whether shutdown has yet to start or has already finished
func TestStopDoesNotBlock(t *testing.T) {
	var shell testShell
	anvil.Run(context.Background(), "test", noConfig{}, panickingLogger{}, func(ctx context.Context, started testShell) error {
		shell = started
		shell.Stop(nil) // before shutdown receives
		return nil
	})
	returned := make(chan struct{})
	go func() {
		shell.Stop(nil)
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("Stop after shutdown blocked")
	}
}

// SIGINT and SIGTERM stay trapped when WithStopSignals adds another
func TestDefaultSignalsSurviveWithStopSignals(t *testing.T) {
	plain := &contextless{}
	anvil.Run(context.Background(), "test", noConfig{}, panickingLogger{}, func(ctx context.Context, shell testShell) error {
		shell.AddShutdownGroup(shutdown.IgnoreContext(plain.Shutdown))
		return raise(syscall.SIGTERM)
	}, anvil.WithStopSignals(syscall.SIGHUP), anvil.WithDrainDelay(0))
	if !plain.called {
		t.Fatal("handler not called")
	}
}

// Wire's context is cancelled as shutdown starts, so its loops stop before any handler runs,
// while each handler gets a live context of its own. A Stop while wire runs cancels it before Stop
// returns.
func TestWiringContext(t *testing.T) {
	var afterStop, startDuringShutdown, handlerDuringShutdown error
	anvil.Run(context.Background(), "test", noConfig{}, panickingLogger{}, func(ctx context.Context, shell testShell) error {
		shell.AddShutdownGroup(shutdown.HandlerFunc(func(handlerCtx context.Context) error {
			startDuringShutdown = ctx.Err()
			handlerDuringShutdown = handlerCtx.Err()
			return nil
		}))
		shell.Stop(nil)
		afterStop = ctx.Err()
		return nil
	})

	if afterStop != context.Canceled {
		t.Fatalf("wire ctx as Stop returned %v, want cancelled", afterStop)
	}
	if !errors.Is(startDuringShutdown, context.Canceled) {
		t.Fatalf("wire ctx in the first handler %v, want cancelled", startDuringShutdown)
	}
	if handlerDuringShutdown != nil {
		t.Fatalf("handler ctx %v, want live", handlerDuringShutdown)
	}
}

type traceKey struct{}

// Cancelling Run's ctx stops cleanly, and its values reach wire and handlers without its
// cancellation, so a handler's ctx is live after the root's cancel
func TestRootCancelStops(t *testing.T) {
	root, cancel := context.WithCancel(context.WithValue(context.Background(), traceKey{}, "trace-1"))
	var startTrace, handlerTrace any
	var handlerDuringShutdown error
	anvil.Run(root, "test", noConfig{}, panickingLogger{}, func(ctx context.Context, shell testShell) error {
		startTrace = ctx.Value(traceKey{})
		shell.AddShutdownGroup(shutdown.HandlerFunc(func(handlerCtx context.Context) error {
			handlerTrace = handlerCtx.Value(traceKey{})
			handlerDuringShutdown = handlerCtx.Err()
			return nil
		}))
		cancel()
		return nil
	})

	if startTrace != "trace-1" || handlerTrace != "trace-1" {
		t.Fatalf("wire saw %v, handler saw %v, want trace-1", startTrace, handlerTrace)
	}
	if handlerDuringShutdown != nil {
		t.Fatalf("handler ctx %v, want live", handlerDuringShutdown)
	}
}

// raise sends this process sig
func raise(sig os.Signal) error {
	process, err := os.FindProcess(os.Getpid())
	if err != nil {
		return err
	}
	return process.Signal(sig)
}

// A trapped signal triggers shutdown, even one arriving while wire still runs
func TestSignalTrigger(t *testing.T) {
	plain := &contextless{}
	anvil.Run(context.Background(), "test", noConfig{}, panickingLogger{}, func(ctx context.Context, shell testShell) error {
		err := raise(syscall.SIGHUP)
		shell.AddShutdownGroup(shutdown.IgnoreContext(plain.Shutdown))
		return err
	}, anvil.WithStopSignals(syscall.SIGHUP), anvil.WithDrainDelay(0))
	if !plain.called {
		t.Fatal("handler not called")
	}
}

// A context-free Shutdown runs through IgnoreContext, a close function through Close, and a closure
// through HandlerFunc, all in one group without names of their own
func TestShapes(t *testing.T) {
	plain := &contextless{}
	closed := false
	ran := false
	run(func(shell testShell) {
		shell.AddShutdownGroup(
			shutdown.IgnoreContext(plain.Shutdown),
			shutdown.Close(func() {
				closed = true
			}),
			shutdown.HandlerFunc(func(context.Context) error {
				ran = true
				return nil
			}),
		)
	})
	if !plain.called || !closed || !ran {
		t.Fatalf("contextless %v, close %v, func %v, want all called", plain.called, closed, ran)
	}
}

// fakeServer is a handler with a pointer receiver, as *http.Server is
type fakeServer struct{}

func (*fakeServer) Shutdown(context.Context) error {
	return nil
}

// names reports whether a panic message names value, as written or quoted
func names(message, value string) bool {
	return strings.Contains(message, value) || strings.Contains(message, strconv.Quote(value))
}

// Wiring mistakes panic at the call with the anvil: prefix, naming what is wrong and where, and
// change nothing more: the faulty handler or function is not added. The group named PAYMENTS
// already holds a valid handler, which still runs.
func TestAddPanics(t *testing.T) {
	const label = "shutdown group 1 (PAYMENTS)"
	type addCase struct {
		name     string
		mistake  func(shell testShell, group anvil.ShutdownGroup)
		mentions []string
	}
	cases := []addCase{
		{"a nil handler", func(_ testShell, group anvil.ShutdownGroup) {
			group.Add(nil)
		}, []string{"nil shutdown handler", label}},
		{"a nil pointer handler", func(_ testShell, group anvil.ShutdownGroup) {
			group.Add((*fakeServer)(nil))
		}, []string{"nil shutdown handler", label}},
		{"a nil HandlerFunc", func(_ testShell, group anvil.ShutdownGroup) {
			group.Add(shutdown.HandlerFunc(nil))
		}, []string{"nil shutdown handler", label}},
		{"a nil handler at AddShutdownGroup", func(shell testShell, _ anvil.ShutdownGroup) {
			shell.AddShutdownGroup(nil)
		}, []string{"nil shutdown handler", "shutdown group 2"}},
		{"a nil function to Go", func(_ testShell, group anvil.ShutdownGroup) {
			group.Go(nil)
		}, []string{"nil function", label}},
		{"a group named twice", func(_ testShell, group anvil.ShutdownGroup) {
			group.SetName("REFUNDS")
		}, []string{"PAYMENTS", "REFUNDS"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ran atomic.Bool
			var recovered any
			code := anvil.Run(context.Background(), "test", noConfig{}, panickingLogger{}, func(ctx context.Context, shell testShell) error {
				defer shell.Stop(nil)
				group := shell.AddShutdownGroup(shutdown.Named("outbox", shutdown.HandlerFunc(func(context.Context) error {
					ran.Store(true)
					return nil
				}))).SetName("PAYMENTS")
				defer func() {
					recovered = recover()
				}()
				tc.mistake(shell, group)
				return nil
			}, anvil.WithDrainDelay(0))

			if code != 0 {
				t.Errorf("exit code %d, want 0", code)
			}
			if !ran.Load() {
				t.Error("the valid handler added first never ran")
			}
			if recovered == nil {
				t.Fatal("did not panic")
			}
			message := fmt.Sprint(recovered)
			if !strings.HasPrefix(message, "anvil: ") {
				t.Errorf("panic %q, want it to start %q", message, "anvil: ")
			}
			for _, named := range tc.mentions {
				if !names(message, named) {
					t.Errorf("panic %q, want it to name %q", message, named)
				}
			}
		})
	}
}

// No name is checked: shutdown.Named and SetName accept any text, empty, with spaces, the characters
// the lines put around names, control characters or bad UTF-8. Each handler is added and run, and two
// handlers of one name in one group both run.
func TestAnyNameIsAccepted(t *testing.T) {
	lineSeparator := string(rune(0x2028))
	noBreakSpace := string(rune(0xa0))
	odd := []string{"http server", "out-box", "café", "(x)", " ", "", "a->b", "[x]", "a;b", "a,b", "a\nb", "a\tb", lineSeparator, noBreakSpace, "\x00", "\x7f", "\xff"}
	for _, name := range odd {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			var ran atomic.Int32
			counted := shutdown.Named(name, shutdown.HandlerFunc(func(context.Context) error {
				ran.Add(1)
				return nil
			}))
			run(func(shell testShell) {
				shell.AddShutdownGroup(counted, counted).SetName(name)
			})
			if ran.Load() != 2 {
				t.Errorf("ran %d times, want both handlers named %q run", ran.Load(), name)
			}
		})
	}
}

// recordingLogger keeps the error lines and reports "started", for tests that act once wire returns
type recordingLogger struct {
	mu      sync.Mutex
	errors  []string
	started chan struct{}
}

func newRecordingLogger() *recordingLogger {
	return &recordingLogger{
		started: make(chan struct{}),
	}
}

func (r *recordingLogger) Logger() *recordingLogger {
	return r
}

func (r *recordingLogger) LifecycleInfo(_ context.Context, msg string) {
	if strings.HasPrefix(msg, "started ") {
		close(r.started)
	}
}

func (r *recordingLogger) LifecycleError(_ context.Context, msg string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors = append(r.errors, fmt.Sprintf("%s: %v", msg, err))
}

func (r *recordingLogger) Flush() {
}

func (r *recordingLogger) errorLines() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.errors)
}

type recordedShell = anvil.Shell[noConfig, *recordingLogger]

// panickingConfig panics while loading, on Run's own goroutine, where only Run's deferred flush sees it
type panickingConfig struct{}

func (panickingConfig) Load(context.Context) (noConfig, error) {
	panic("kaboom")
}

// flushCountingLogger records error lines and counts flushes
type flushCountingLogger struct {
	recordingLogger
	flushes int
}

func (f *flushCountingLogger) Flush() {
	f.flushes++
}

// A panic on Run's goroutine is logged and the logger flushed, then the panic carries on unchanged
func TestPanicIsLoggedAndFlushedBeforeItCarriesOn(t *testing.T) {
	logger := &flushCountingLogger{recordingLogger: *newRecordingLogger()}
	defer func() {
		recovered := recover()
		if recovered != "kaboom" {
			t.Fatalf("recovered %v, want the original panic", recovered)
		}
		if lines := logger.errorLines(); len(lines) != 1 || lines[0] != "panicked: kaboom" || logger.flushes != 1 {
			t.Fatalf("errors %q, flushes %d, want the panic logged once and one flush", lines, logger.flushes)
		}
	}()
	anvil.Run(context.Background(), "test", panickingConfig{}, logger,
		func(ctx context.Context, shell anvil.Shell[noConfig, *recordingLogger]) error {
			return nil
		})
}

// Shell.Logger is the logger provider's own logger
func TestLoggerIsTheProvidersLogger(t *testing.T) {
	logger := newRecordingLogger()
	var got *recordingLogger
	code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
		got = shell.Logger()
		shell.Stop(nil)
		return nil
	})
	if code != 0 || got != logger {
		t.Fatalf("exit %d, logger %p, want 0 and the provider's %p", code, got, logger)
	}
}

// Once wire has returned, every change to the shutdown groups is refused: logged once each, with
// the process carrying on, nothing added, named or started, and the handler added in time still run.
// A group AddShutdownGroup refuses refuses every change made to it in the same way.
func TestChangesAfterWiringAreRefused(t *testing.T) {
	logger := newRecordingLogger()
	var shell recordedShell
	var early anvil.ShutdownGroup
	var ranEarly, ranLate atomic.Bool
	finished := make(chan int)
	go func() {
		finished <- anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, started recordedShell) error {
			shell = started
			early = shell.AddShutdownGroup(shutdown.HandlerFunc(func(context.Context) error {
				ranEarly.Store(true)
				return nil
			}))
			return nil
		})
	}()
	<-logger.started
	late := shutdown.HandlerFunc(func(context.Context) error {
		ranLate.Store(true)
		return nil
	})
	lateFn := func(context.Context) error {
		ranLate.Store(true)
		return nil
	}
	refusedGroup := shell.AddShutdownGroup(late)
	refusedGroup.Add(late)
	refusedGroup.Go(lateFn)
	refusedGroup.SetName("LATE")
	early.Add(late)
	early.Go(lateFn)
	early.SetName("EARLY")
	shell.Stop(nil)

	if code := <-finished; code != 0 || !ranEarly.Load() || ranLate.Load() {
		t.Fatalf("exit %d, early ran %v, late ran %v, want 0, true, false", code, ranEarly.Load(), ranLate.Load())
	}
	const reason = ": shutdown groups can only be changed during wiring"
	want := []string{
		"shutdown group not added" + reason,
		"shutdown handler not added" + reason,
		"goroutine not started" + reason,
		`shutdown group "LATE" not named` + reason,
		"shutdown handler not added" + reason,
		"goroutine not started" + reason,
		`shutdown group "EARLY" not named` + reason,
	}
	if lines := logger.errorLines(); !slices.Equal(lines, want) {
		t.Fatalf("errors\n%q\nwant\n%q", lines, want)
	}
}

// Adding from a goroutine wire launched is refused once wire returns, without that goroutine
// synchronising with Run
func TestAddFromAnotherGoroutineIsRefusedOnceWiringReturns(t *testing.T) {
	logger := newRecordingLogger()
	refused := make(chan bool, 1)
	code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
		group := shell.AddShutdownGroup()
		go func() {
			defer shell.Stop(nil)
			giveUp := time.Now().Add(time.Second)
			for time.Now().Before(giveUp) {
				group.Add(shutdown.HandlerFunc(quick))
				if len(logger.errorLines()) > 0 {
					refused <- true
					return
				}
			}
			refused <- false
		}()
		return nil
	})
	if !<-refused {
		t.Fatal("add never refused after wire returned")
	}
	if code != 0 {
		t.Fatalf("exit %d, want 0", code)
	}
}

// quietLogger stands in for a logger provider on the failure paths, where Run must return its exit
// code to the caller rather than end the process
type quietLogger struct{}

func (q quietLogger) Logger() quietLogger {
	return q
}

func (quietLogger) LifecycleInfo(context.Context, string) {
}

func (quietLogger) LifecycleError(context.Context, string, error) {
}

func (quietLogger) Flush() {
}

// quietConfig is a config provider for quietLogger, failing when it holds an error
type quietConfig struct {
	err error
}

func (q quietConfig) Load(context.Context) (quietConfig, error) {
	return q, q.err
}

type quietShell = anvil.Shell[quietConfig, quietLogger]

// Run returns the exit code to its caller on every path, so the test carries on past it
func TestRunReturnsExitCodes(t *testing.T) {
	clean := anvil.Run(context.Background(), "test", quietConfig{}, quietLogger{}, func(ctx context.Context, shell quietShell) error {
		shell.Stop(nil)
		return nil
	})

	failed := anvil.Run(context.Background(), "test", quietConfig{}, quietLogger{}, func(context.Context, quietShell) error {
		return errors.New("boom")
	})

	started := false
	notStarted := func(context.Context, quietShell) error {
		started = true
		return nil
	}
	unloaded := anvil.Run(context.Background(), "test", quietConfig{err: errors.New("unreadable")}, quietLogger{}, notStarted)

	// with no stop, a wire error wrapping context.Canceled is still a failure
	startCancelled := anvil.Run(context.Background(), "test", quietConfig{}, quietLogger{}, func(context.Context, quietShell) error {
		return fmt.Errorf("db: %w", context.Canceled)
	})

	// a provider's own cancellation is a failed load, not a clean stop
	loadCancelled := quietConfig{err: fmt.Errorf("vault: %w", context.Canceled)}
	cancelledLoad := anvil.Run(context.Background(), "test", loadCancelled, quietLogger{}, notStarted)

	if clean != 0 || failed != 1 || startCancelled != 1 || unloaded != 1 || cancelledLoad != 1 || started {
		t.Fatalf("clean %d, failed %d, wire cancelled %d, unloaded %d, cancelled load %d (started %v), want 0, 1, 1, 1, 1 and no wiring",
			clean, failed, startCancelled, unloaded, cancelledLoad, started)
	}
}

// configLogging is a provider implementing anvil.ConfigLogger, recording every lifecycle line
type configLogging struct {
	quietLogger
	info    []string
	configs []any
}

func (c *configLogging) LifecycleInfo(_ context.Context, msg string) {
	c.info = append(c.info, msg)
}

func (c *configLogging) LogConfig(_ context.Context, msg string, config any) {
	c.info = append(c.info, msg)
	c.configs = append(c.configs, config)
}

var _ anvil.ConfigLogger = (*configLogging)(nil)

// portConfig is a config provider whose loaded value can be recognised
type portConfig struct {
	port int
	err  error
}

func (p portConfig) Load(context.Context) (portConfig, error) {
	return p, p.err
}

// A provider implementing ConfigLogger is handed the loaded configuration once, on the
// "configuration loaded" line, which is not also logged through LifecycleInfo
func TestConfigLoggerReceivesTheLoadedConfig(t *testing.T) {
	logger := &configLogging{}
	code := anvil.Run(context.Background(), "test", portConfig{port: 8080}, logger,
		func(ctx context.Context, shell anvil.Shell[portConfig, quietLogger]) error {
			shell.Stop(nil)
			return nil
		})
	loadedLines := 0
	for _, msg := range logger.info {
		if strings.HasPrefix(msg, "configuration loaded in ") {
			loadedLines++
		}
	}
	if code != 0 || loadedLines != 1 || len(logger.configs) != 1 || logger.configs[0] != (portConfig{port: 8080}) {
		t.Fatalf("exit %d, configs %v, lines %q, want 0 and one loaded line carrying {8080}", code, logger.configs, logger.info)
	}
}

// A load that fails hands the provider no configuration
func TestConfigLoggerNotCalledWhenTheLoadFails(t *testing.T) {
	logger := &configLogging{}
	code := anvil.Run(context.Background(), "test", portConfig{err: errors.New("unreadable")}, logger,
		func(context.Context, anvil.Shell[portConfig, quietLogger]) error {
			return nil
		})
	if code != 1 || len(logger.configs) != 0 {
		t.Fatalf("exit %d, configs %v, want 1 and none", code, logger.configs)
	}
}

// A ctx already done when Run is called stops the application as soon as it starts, through the
// usual shutdown, once a provider that ignores the ctx has loaded the configuration anyway. The
// stop cancels wire's ctx, so a wire waiting on it returns its error at once, which is clean, and
// no started line is logged.
func TestDoneCtxStopsAtOnce(t *testing.T) {
	done, cancel := context.WithCancel(context.Background())
	cancel()
	flushed := false
	code := anvil.Run(done, "test", quietConfig{}, quietLogger{}, func(ctx context.Context, shell quietShell) error {
		shell.AddShutdownGroup(shutdown.HandlerFunc(func(context.Context) error {
			flushed = true
			return nil
		}))
		return nil
	})
	if code != 0 || !flushed {
		t.Fatalf("exit %d, outbox flushed %v, want 0 and the shutdown run", code, flushed)
	}

	logger := newRecordingLogger()
	finished := make(chan int, 1)
	go func() {
		finished <- anvil.Run(done, "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
			<-ctx.Done() // the stop lands from its own goroutine, before or after wire is called
			return ctx.Err()
		})
	}()
	select {
	case code = <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("wire waiting on its ctx never returned: the done ctx's stop did not cancel it")
	}
	if code != 0 || len(logger.errorLines()) != 0 {
		t.Fatalf("waiting wire exit %d, errors %q, want 0 and none", code, logger.errorLines())
	}
	select {
	case <-logger.started:
		t.Fatal("started logged, want none after a stop during wiring")
	default:
	}
}

// A Stop racing wire's return gets one rule or the other, so a wire that returns its ctx's error
// always stops cleanly, and a wire that saw its ctx cancelled is never reported started. The
// synctest tests pin each rule; this runs the race on real goroutines for -race to check the lock.
// Wire spins a little longer each iteration, so the Stop lands before, around and after its return.
func TestStopRacingWireReturn(t *testing.T) {
	for iteration := range 200 {
		logger := newRecordingLogger()
		var sawStop bool
		code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
			go shell.Stop(nil)
			for range iteration * iteration {
				if ctx.Err() != nil {
					break
				}
			}
			err := ctx.Err()
			sawStop = err != nil
			if err != nil && iteration%2 == 1 {
				return fmt.Errorf("db: %w", err)
			}
			return err
		}, anvil.WithDrainDelay(0))

		if code != 0 || len(logger.errorLines()) != 0 {
			t.Fatalf("iteration %d: exit %d, errors %q, want 0 and none", iteration, code, logger.errorLines())
		}
		select {
		case <-logger.started:
			if sawStop {
				t.Fatalf("iteration %d: started logged after wire saw its ctx cancelled", iteration)
			}
		default:
		}
	}
}

// A clean stop a signal other than SIGTERM asked for exits 128 plus the signal, as Ctrl+C's 130 does;
// SIGTERM's clean stop exits 0; and a failure exits 1 whichever signal asked
func TestCleanStopExitCodeFollowsTheSignal(t *testing.T) {
	exitCodeAfter := func(received syscall.Signal, handler shutdown.Handler) int {
		return anvil.Run(context.Background(), "test", quietConfig{}, quietLogger{}, func(ctx context.Context, shell quietShell) error {
			shell.AddShutdownGroup(handler)
			return raise(received)
		}, anvil.WithStopSignals(syscall.SIGHUP), anvil.WithDrainDelay(0))
	}
	clean := shutdown.HandlerFunc(func(context.Context) error {
		return nil
	})
	failing := shutdown.HandlerFunc(func(context.Context) error {
		return errors.New("flush failed")
	})
	if code, want := exitCodeAfter(syscall.SIGHUP, clean), 128+int(syscall.SIGHUP); code != want {
		t.Errorf("SIGHUP clean stop exit %d, want %d", code, want)
	}
	if code := exitCodeAfter(syscall.SIGTERM, clean); code != 0 {
		t.Errorf("SIGTERM clean stop exit %d, want 0", code)
	}
	if code := exitCodeAfter(syscall.SIGHUP, failing); code != 1 {
		t.Errorf("SIGHUP stop with a failing handler exit %d, want 1", code)
	}
}

// Run given a nil provider or wire panics naming the types, an interface type included, rather than
// dereferencing nil later
func TestNewNilProviderPanics(t *testing.T) {
	expectPanic := func(want string, do func()) {
		t.Helper()
		defer func() {
			if recovered := recover(); fmt.Sprint(recovered) != want {
				t.Errorf("recovered %v, want %q", recovered, want)
			}
		}()
		do()
	}
	expectPanic("anvil: nil ConfigProvider[anvil_test.noConfig], cannot start", func() {
		anvil.Run[noConfig, panickingLogger](context.Background(), "test", nil, panickingLogger{}, stopAtOnce)
	})
	expectPanic("anvil: nil LoggerProvider[anvil_test.panickingLogger], cannot start", func() {
		anvil.Run[noConfig, panickingLogger](context.Background(), "test", noConfig{}, nil, stopAtOnce)
	})
	expectPanic("anvil: nil LoggerProvider[fmt.Stringer], cannot start", func() {
		anvil.Run[noConfig, fmt.Stringer](context.Background(), "test", noConfig{}, nil, nil)
	})
	expectPanic("anvil: nil ConfigProvider[anvil_test.noConfig], cannot start", func() {
		anvil.Run[noConfig, panickingLogger](context.Background(), "test", (*pointerConfig)(nil), panickingLogger{}, stopAtOnce)
	})
	expectPanic("anvil: nil Wiring[anvil_test.noConfig, anvil_test.panickingLogger] function, cannot start", func() {
		anvil.Run[noConfig, panickingLogger](context.Background(), "test", noConfig{}, panickingLogger{}, nil)
	})
}

// Stopping is false while wire runs and true once shutdown has begun, for a readiness probe
func TestStopping(t *testing.T) {
	var whileStarting, duringShutdown bool
	anvil.Run(context.Background(), "test", noConfig{}, panickingLogger{}, func(ctx context.Context, shell testShell) error {
		whileStarting = shell.Stopping()
		shell.AddShutdownGroup(shutdown.HandlerFunc(func(context.Context) error {
			duringShutdown = shell.Stopping()
			return nil
		}))
		shell.Stop(nil)
		return nil
	})

	if whileStarting || !duringShutdown {
		t.Fatalf("Stopping while starting %v, during shutdown %v, want false then true", whileStarting, duringShutdown)
	}
}

// stopAtOnce is a wiring that stops the application straight away
func stopAtOnce(_ context.Context, shell testShell) error {
	shell.Stop(nil)
	return nil
}

// pointerConfig is a config provider with a pointer receiver, so a nil *pointerConfig is a provider
type pointerConfig struct{}

func (*pointerConfig) Load(context.Context) (noConfig, error) {
	return noConfig{}, nil
}
