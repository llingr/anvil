// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
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

// recorder keeps the start and end time of every handler that ran
type recorder struct {
	mu    sync.Mutex
	spans map[string][2]time.Time
}

func newRecorder() *recorder {
	return &recorder{
		spans: map[string][2]time.Time{},
	}
}

func (r *recorder) handler(name string, delay time.Duration) shutdown.Handler {
	return func(context.Context) error {
		r.mark(name, delay)
		return nil
	}
}

func (r *recorder) mark(name string, delay time.Duration) {
	start := time.Now()
	time.Sleep(delay)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spans[name] = [2]time.Time{start, time.Now()}
}

func (r *recorder) startOf(name string) time.Time {
	return r.spans[name][0]
}

func (r *recorder) endOf(name string) time.Time {
	return r.spans[name][1]
}

type contextless struct {
	called bool
}

func (c *contextless) Shutdown() error {
	c.called = true
	return nil
}

// shutdownWithin splits total 50/25/25 across the phases with no drain, whose unused time would
// otherwise reach the phases
func shutdownWithin(total time.Duration) []anvil.Option {
	return []anvil.Option{
		anvil.WithDrainDelay(0),
		anvil.WithShutdownPhaseBudget(shutdown.Ingress, total*50/100),
		anvil.WithShutdownPhaseBudget(shutdown.Core, total*25/100),
		anvil.WithShutdownPhaseBudget(shutdown.Egress, total*25/100),
	}
}

// run wires and stops at once; a reported failure panics through panickingLogger
func run(wire func(shell testShell), options ...anvil.Option) {
	anvil.Run(context.Background(), "test", noConfig{}, panickingLogger{}, func(ctx context.Context, shell testShell) error {
		wire(shell)
		shell.Stop(nil)
		return nil
	}, options...)
}

// Ingress and Egress run concurrently, Core runs in reverse registration order, phases run in order
func TestPhaseOrderAndConcurrency(t *testing.T) {
	rec := newRecorder()
	run(func(shell testShell) {
		shell.RegisterShutdownHandler(shutdown.Egress, "l1", rec.handler("l1", 50*time.Millisecond))
		shell.RegisterShutdownHandler(shutdown.Egress, "l2", rec.handler("l2", 50*time.Millisecond))
		shell.RegisterShutdownHandler(shutdown.Core, "n1", rec.handler("n1", 30*time.Millisecond))
		shell.RegisterShutdownHandler(shutdown.Core, "n2", rec.handler("n2", 30*time.Millisecond))
		shell.RegisterShutdownHandler(shutdown.Ingress, "e1", rec.handler("e1", 50*time.Millisecond))
		shell.RegisterShutdownHandler(shutdown.Ingress, "e2", rec.handler("e2", 50*time.Millisecond))
	}, shutdownWithin(5*time.Second)...)

	if len(rec.spans) != 6 {
		t.Fatalf("%d handlers ran, want 6", len(rec.spans))
	}
	if gap := rec.startOf("e2").Sub(rec.startOf("e1")).Abs(); gap > 20*time.Millisecond {
		t.Errorf("early handlers started %s apart, want concurrent", gap)
	}
	if gap := rec.startOf("l2").Sub(rec.startOf("l1")).Abs(); gap > 20*time.Millisecond {
		t.Errorf("late handlers started %s apart, want concurrent", gap)
	}
	if rec.startOf("n2").Before(rec.endOf("e1")) || rec.startOf("n2").Before(rec.endOf("e2")) {
		t.Error("core started before ingress finished")
	}
	if rec.startOf("n1").Before(rec.endOf("n2")) {
		t.Error("n1 started before n2 finished, want reverse registration order")
	}
	if rec.startOf("l1").Before(rec.endOf("n1")) {
		t.Error("egress started before core finished")
	}
}

// Each phase ends at the running total of budgets, so time Ingress does not use reaches Core and Egress
func TestBudgetRollsForward(t *testing.T) {
	var defaultBudget, lastBudget time.Duration
	capture := func(into *time.Duration) shutdown.Handler {
		return func(ctx context.Context) error {
			deadline, _ := ctx.Deadline()
			*into = time.Until(deadline)
			return nil
		}
	}
	run(func(shell testShell) {
		shell.RegisterShutdownHandler(shutdown.Ingress, "early", newRecorder().handler("early", 300*time.Millisecond))
		shell.RegisterShutdownHandler(shutdown.Core, "default budget", capture(&defaultBudget))
		shell.RegisterShutdownHandler(shutdown.Egress, "last budget", capture(&lastBudget))
	}, shutdownWithin(3*time.Second)...)

	within := func(name string, got, want time.Duration) {
		t.Helper()
		if (got - want).Abs() > 150*time.Millisecond {
			t.Errorf("%s budget %s, want about %s", name, got, want)
		}
	}
	within("default", defaultBudget, 1950*time.Millisecond) // 1.5s first + 750ms default, less 300ms used
	within("last", lastBudget, 2700*time.Millisecond)       // all 3s less 300ms, Core spent none
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
		shell.RegisterShutdownHandler(shutdown.Core, "plain", shutdown.IgnoreContext(plain.Shutdown))
		return raise(syscall.SIGTERM)
	}, anvil.WithStopSignals(syscall.SIGHUP), anvil.WithDrainDelay(0))
	if !plain.called {
		t.Fatal("handler not called")
	}
}

// Wire's context is cancelled as shutdown starts, so its loops stop before any handler runs,
// while each handler gets a live context of its own
func TestWiringContext(t *testing.T) {
	var startDuringShutdown, handlerDuringShutdown error
	anvil.Run(context.Background(), "test", noConfig{}, panickingLogger{}, func(ctx context.Context, shell testShell) error {
		shell.RegisterShutdownHandler(shutdown.Ingress, "observe", func(handlerCtx context.Context) error {
			startDuringShutdown = ctx.Err()
			handlerDuringShutdown = handlerCtx.Err()
			return nil
		})
		shell.Stop(nil)
		return nil
	})

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
		shell.RegisterShutdownHandler(shutdown.Core, "observe", func(handlerCtx context.Context) error {
			handlerTrace = handlerCtx.Value(traceKey{})
			handlerDuringShutdown = handlerCtx.Err()
			return nil
		})
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
		shell.RegisterShutdownHandler(shutdown.Core, "plain", shutdown.IgnoreContext(plain.Shutdown))
		return err
	}, anvil.WithStopSignals(syscall.SIGHUP), anvil.WithDrainDelay(0))
	if !plain.called {
		t.Fatal("handler not called")
	}
}

// A context-free Shutdown runs through IgnoreContext, and a closure is a Handler as it is
func TestShapes(t *testing.T) {
	plain := &contextless{}
	ran := false
	run(func(shell testShell) {
		shell.RegisterShutdownHandler(shutdown.Core, "plain", shutdown.IgnoreContext(plain.Shutdown))
		shell.RegisterShutdownHandler(shutdown.Core, "flush", func(context.Context) error {
			ran = true
			return nil
		})
	})
	if !plain.called || !ran {
		t.Fatalf("contextless %v, func %v", plain.called, ran)
	}
}

// Wiring mistakes panic at the point of the mistake
func TestRegisterPanics(t *testing.T) {
	expectPanic := func(name string, do func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s did not panic", name)
			}
		}()
		do()
	}
	anvil.Run(context.Background(), "test", noConfig{}, panickingLogger{}, func(ctx context.Context, shell testShell) error {
		expectPanic("empty name", func() {
			shell.RegisterShutdownHandler(shutdown.Core, "", shutdown.IgnoreContext((&contextless{}).Shutdown))
		})
		expectPanic("nil handler", func() {
			shell.RegisterShutdownHandler(shutdown.Core, "nil", nil)
		})
		expectPanic("unknown phase", func() {
			shell.RegisterShutdownHandler(shutdown.Phase("early"), "contextless", shutdown.IgnoreContext((&contextless{}).Shutdown))
		})
		shell.Stop(nil)
		return nil
	})
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

// A handler registered after wire returns is logged and left out, the process carries on, and the
// handlers registered in time still run
func TestRegisterAfterWiringIsRefused(t *testing.T) {
	logger := newRecordingLogger()
	var shell recordedShell
	ranEarly, ranLate := false, false
	finished := make(chan int)
	go func() {
		finished <- anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, started recordedShell) error {
			shell = started
			shell.RegisterShutdownHandler(shutdown.Core, "early", func(context.Context) error {
				ranEarly = true
				return nil
			})
			return nil
		})
	}()
	<-logger.started
	shell.RegisterShutdownHandler(shutdown.Core, "late", func(context.Context) error {
		ranLate = true
		return nil
	})
	shell.Stop(nil)

	if code := <-finished; code != 0 || !ranEarly || ranLate {
		t.Fatalf("exit %d, early ran %v, late ran %v, want 0, true, false", code, ranEarly, ranLate)
	}
	const refused = `shutdown handler "late" not registered: registrations are only permitted during startup`
	if lines := logger.errorLines(); len(lines) != 1 || !strings.HasPrefix(lines[0], refused) {
		t.Fatalf("errors %q, want the refusal of late", lines)
	}
}

// Registration from a goroutine wire launched is refused once wire returns, without that goroutine
// synchronising with Run
func TestRegisterFromAnotherGoroutineIsRefusedOnceWiringReturns(t *testing.T) {
	logger := newRecordingLogger()
	refused := make(chan bool, 1)
	code := anvil.Run(context.Background(), "test", noConfig{}, logger, func(ctx context.Context, shell recordedShell) error {
		go func() {
			defer shell.Stop(nil)
			giveUp := time.Now().Add(time.Second)
			for time.Now().Before(giveUp) {
				shell.RegisterShutdownHandler(shutdown.Core, "late", func(context.Context) error {
					return nil
				})
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
		t.Fatal("registration never refused after wire returned")
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

	// wire's own error is a failure, even one wrapping context.Canceled
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
// usual shutdown, once a provider that ignores the ctx has loaded the configuration anyway
func TestDoneCtxStopsAtOnce(t *testing.T) {
	done, cancel := context.WithCancel(context.Background())
	cancel()
	flushed := false
	code := anvil.Run(done, "test", quietConfig{}, quietLogger{}, func(ctx context.Context, shell quietShell) error {
		shell.RegisterShutdownHandler(shutdown.Egress, "outbox", func(context.Context) error {
			flushed = true
			return nil
		})
		return nil
	})
	if code != 0 || !flushed {
		t.Fatalf("exit %d, outbox flushed %v, want 0 and the shutdown run", code, flushed)
	}
}

// A clean stop a signal other than SIGTERM asked for exits 128 plus the signal, as Ctrl+C's 130 does;
// SIGTERM's clean stop exits 0; and a failure exits 1 whichever signal asked
func TestCleanStopExitCodeFollowsTheSignal(t *testing.T) {
	exitCodeAfter := func(received syscall.Signal, handler shutdown.Handler) int {
		return anvil.Run(context.Background(), "test", quietConfig{}, quietLogger{}, func(ctx context.Context, shell quietShell) error {
			shell.RegisterShutdownHandler(shutdown.Core, "handler", handler)
			return raise(received)
		}, anvil.WithStopSignals(syscall.SIGHUP), anvil.WithDrainDelay(0))
	}
	clean := func(context.Context) error {
		return nil
	}
	failing := func(context.Context) error {
		return errors.New("flush failed")
	}
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
		shell.RegisterShutdownHandler(shutdown.Core, "observe", func(context.Context) error {
			duringShutdown = shell.Stopping()
			return nil
		})
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
