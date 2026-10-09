// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/llingr/anvil/shutdown"
)

// Shell for running services requiring config,
// logging, and graceful shutdown orchestration.
type Shell[C, L any] interface {
	// Config for the service
	Config() C

	// Logger for the service, and the Shell
	Logger() L

	// AddShutdownGroup adds a group of handlers that stop
	// together, after the groups already added. Must only
	// be called during wiring.
	AddShutdownGroup(handlers ...shutdown.Handler) ShutdownGroup

	// Stop invokes shutdown without waiting for OS signals.
	// Use nil reason to indicate a clean stop, otherwise Run
	// will return exit code 1. This method is idempotent.
	Stop(reason error)

	// Stopping reports whether shutdown has begun, from any trigger, for a
	// readiness probe to fail during shutdown
	Stopping() bool
}

// Wiring wraps application initialization. This is the same
// as idiomatic wiring in main(), with the shell's provided
// Config C, Logger L, shutdown handlers and Go routines.
//
// SIGINT, SIGTERM, Stop() and ctx cancellation all invoke the
// Shell's coordinated shutdown. A stop before or during wiring
// cancels ctx at once, and an error from wire that is only that
// cancellation counts as a clean stop. A stop after wiring
// cancels ctx once the shutdown has run, just before Run returns.
type Wiring[C, L any] func(ctx context.Context, shell Shell[C, L]) error

// shell container wrapping
type shell[C, L any] struct {
	name           string                  // of the application, used in logging
	runCtx         context.Context         // Run's ctx without its cancellation
	loggerProvider LoggerProvider[L]       // provided to Run
	config         C                       // provided to Run
	options        options                 // for graceful shutdown
	wireCtx        context.Context         // wire's ctx: Run's values, a cancellation of its own
	wireCtxCancel  context.CancelFunc      // cancels wireCtx
	goCtx          context.Context         // every Go function's ctx is derived from it
	goCtxCancel    context.CancelCauseFunc // cancels goCtx
	stopped        chan struct{}           // closed once stoppedTime and stopReason are set, which never change after
	mu             sync.Mutex              // protects started, phases' handlers, and the stop until stopped closes
	started        bool                    // indicates wiring has returned; finalizes registration
	stoppedTime    time.Time               // first invoked time; stopping is idempotent
	stopReason     error                   // nil for a clean Stop(nil)
	groups         []*shutdownGroup        // in shutdown order
}

// Run wraps a service or applications' whole lifecycle,
// returning an integer to be using as an os.Exit code
func Run[C, L any](
	ctx context.Context, name string,
	configProvider ConfigProvider[C],
	loggerProvider LoggerProvider[L],
	wire Wiring[C, L],
	opts ...Option) int {

	// initialisation and core validation
	switch {
	case isNil(configProvider):
		const nilConfigProvider = "anvil: nil ConfigProvider[%s], cannot start"
		panic(fmt.Sprintf(nilConfigProvider, reflect.TypeFor[C]()))
	case isNil(loggerProvider):
		const nilLoggerProvider = "anvil: nil LoggerProvider[%s], cannot start"
		panic(fmt.Sprintf(nilLoggerProvider, reflect.TypeFor[L]()))
	case isNil(wire):
		const nilWiring = "anvil: nil Wiring[%s, %s] function, cannot start"
		panic(fmt.Sprintf(nilWiring, reflect.TypeFor[C](), reflect.TypeFor[L]()))
	}

	ctxNoCxl := context.WithoutCancel(ctx)
	defer flushLogger(ctxNoCxl, loggerProvider)
	loggerProvider.LifecycleInfo(ctxNoCxl, "starting "+name)
	processedOptions := processOptions(opts...)

	// config
	loggerProvider.LifecycleInfo(ctxNoCxl, "loading config")
	loadStart := time.Now()
	config, err := configProvider.Load(ctx) // blocking call, config providers must manage their own timeouts
	if err != nil {
		loggerProvider.LifecycleError(ctxNoCxl, "failed to load config", err)
		loggerProvider.LifecycleInfo(ctxNoCxl, "exiting "+name)
		return 1
	}
	const configLoadedMessage = "configuration loaded in %s"
	took := time.Since(loadStart).Truncate(time.Microsecond)
	if configLogger, ok := loggerProvider.(ConfigLogger); ok {
		configLogger.LogConfig(ctxNoCxl, fmt.Sprintf(configLoadedMessage, took), config)
	} else {
		loggerProvider.LifecycleInfo(ctxNoCxl, fmt.Sprintf(configLoadedMessage, took))
	}

	s := initShell(name, ctxNoCxl, loggerProvider, config, processedOptions)

	// shutdown orchestration
	shutdownSignals := []os.Signal{syscall.SIGINT, kubernetesTermSignal}
	for _, additional := range s.options.signals {
		if !slices.Contains(shutdownSignals, additional) {
			shutdownSignals = append(shutdownSignals, additional)
		}
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, shutdownSignals...)
	defer signal.Stop(signals)
	watching := make(chan struct{})
	defer close(watching)
	go s.watchSignals(signals, watching)

	releaseCtx := context.AfterFunc(ctx, func() {
		reason := errRunCtxCancelled
		if cause := context.Cause(ctx); cause != context.Canceled {
			reason = fmt.Errorf("%w: %w", errRunCtxCancelled, cause)
		}
		s.requestStop(reason)
	})
	defer releaseCtx()

	// call host application wiring
	return s.run(wire)
}

func initShell[C, L any](
	name string, runCtx context.Context, loggerProvider LoggerProvider[L], config C, options options,
) *shell[C, L] {
	s := &shell[C, L]{
		name:           name,
		runCtx:         runCtx,
		loggerProvider: loggerProvider,
		config:         config,
		options:        options,
		stopped:        make(chan struct{}),
	}
	s.wireCtx, s.wireCtxCancel = context.WithCancel(runCtx)
	s.goCtx, s.goCtxCancel = context.WithCancelCause(runCtx)
	return s
}

// run calls application wiring, then awaits shutdown
func (s *shell[C, L]) run(wire Wiring[C, L]) int {

	// call wire and report whether a stop signal came while it ran
	stoppedDuringWiring, wireErr := func() (bool, error) {
		err := catchPanic(func() error {
			return wire(s.wireCtx, s)
		})
		s.mu.Lock()
		defer s.mu.Unlock()
		s.started = true // no more shutdown handlers can be added
		return s.Stopping(), err
	}()

	switch {
	case stoppedDuringWiring && isStoppedCleanly(wireErr): // wire stopped as its ctx told it to
		wireErr = nil
	case wireErr != nil:
		wireErr = fmt.Errorf("wiring failed: %w", wireErr)
		s.requestStop(wireErr)
	default:
		s.loggerProvider.LifecycleInfo(s.runCtx, "started "+s.name)
	}

	// closed on a signal (SIGINT, SIGTERM or one added by WithStopSignals),
	// shell.Stop(), Run's ctx cancelled, a Go function failing, or wire() failing
	<-s.stopped

	if !stoppedDuringWiring { // a stop during wiring logged its line as it arrived
		s.loggerProvider.LifecycleInfo(s.runCtx, stoppingLine(s.stopReason))
	}

	if !slices.ContainsFunc(s.groups, func(group *shutdownGroup) bool {
		return len(group.handlers) > 0
	}) {
		s.loggerProvider.LifecycleInfo(s.runCtx, "no components are registered for graceful shutdown")
	}
	s.awaitDrainDelay()

	ctx, cancel := context.WithDeadline(s.runCtx, s.deadline())
	defer cancel()
	s.shutdown(ctx)

	s.goCtxCancel(stopCause{s.deadline()}) // for Go functions not stopped before deadline
	if s.wireCtx.Err() == nil {            // not already canceled
		s.loggerProvider.LifecycleInfo(s.runCtx, "cancelling wire's ctx")
		s.wireCtxCancel()
	}

	exitCode := cleanExitCode(s.stopReason)
	if err := s.failures(s.stopReason, wireErr); err != nil {
		s.loggerProvider.LifecycleError(s.runCtx, "stopped with an error", err)
		exitCode = 1
	}
	s.loggerProvider.LifecycleInfo(s.runCtx, "exiting "+s.name) // the last line
	return exitCode
}

// isStoppedCleanly reports whether wire's err is nil or wraps context.Canceled through single wraps
// only. errors.Is is not used because it looks inside joined errors, which would hide a real failure
// reported together with the cancellation.
func isStoppedCleanly(err error) bool {
	if err == nil {
		return true
	}
	for ; err != nil; err = errors.Unwrap(err) { // errors.Unwrap follows only Unwrap() error
		if err == context.Canceled {
			return true
		}
	}
	return false
}

// cleanExitCode is 128 plus the signal for a signal other than kubernetesTermSignal, as the calling
// process expects of Ctrl+C (130), and 0 otherwise
func cleanExitCode(reason error) int {
	received := stoppedBySignal(reason)
	if received == nil || received == kubernetesTermSignal {
		return 0
	}
	number, ok := received.(syscall.Signal)
	if !ok {
		return 1 // an os.Signal that is not a syscall.Signal has no number to report
	}
	return 128 + int(number) // the Unix convention for a process a signal ended
}

// Stop begins shutdown: a nil reason is a clean stop and any other makes Run return 1; the first
// reason wins
func (s *shell[C, L]) Stop(reason error) {
	if reason != nil {
		reason = fmt.Errorf("Stop called: %w", reason)
	}
	s.requestStop(reason)
}

// Config is the configuration Run loaded
func (s *shell[C, L]) Config() C {
	return s.config
}

// Logger is the logging library's logger, L
func (s *shell[C, L]) Logger() L {
	return s.loggerProvider.Logger()
}

// Stopping reports whether shutdown has begun, from any trigger
func (s *shell[C, L]) Stopping() bool {
	select {
	case <-s.stopped:
		return true
	default:
		return false
	}
}

// isNil reports a nil interface, or an interface holding a nil pointer, map, slice, func or chan
func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return reflected.IsNil()
	default:
		return false
	}
}
