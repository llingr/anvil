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
	"runtime/debug"
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

	// RegisterShutdownHandler includes a component in
	// graceful shutdown orchestration. Components must
	// only be registered during wiring.
	RegisterShutdownHandler(phase shutdown.Phase, name string, handler shutdown.Handler)

	// Go runs fn on its own goroutine and registered for
	// graceful shutdown. Must only be started during wiring.
	// Examples include HTTP servers and message consumers.
	Go(phase shutdown.Phase, name string, fn func(ctx context.Context) error)

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
// Config C, Logger L, shutdown registration and Go routines.
//
// SIGINT, SIGTERM, Stop() and ctx cancellation all invoke the
// Shell's coordinated shutdown.
type Wiring[C, L any] func(ctx context.Context, shell Shell[C, L]) error

// shell container wrapping
type shell[C, L any] struct {
	name             string                                // of the application, used in logging
	detached         context.Context                       // Run's ctx without its cancellation
	loggerProvider   LoggerProvider[L]                     // provided to Run
	config           C                                     // provided to Run
	options          options                               // for graceful shutdown
	phaseCtx         map[shutdown.Phase]context.Context    // provided to Go routines, canceled to signal shut-down
	endPhaseCtx      map[shutdown.Phase]context.CancelFunc // called to signal Go function to shut-down
	stopped          chan struct{}                         // closed when stoppedTime and stopReason are set
	mu               sync.Mutex                            // protects started, stoppedTime, stopReason, shutdownHandlers
	started          bool                                  // indicates wiring has returned; finalizes registration
	stoppedTime      time.Time                             // first invoked time; stopping is idempotent
	stopReason       error                                 // nil for a clean Stop(nil)
	shutdownHandlers map[shutdown.Phase][]registeredHandler
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
		return 1
	} else {
		took := time.Since(loadStart).Truncate(time.Microsecond)
		loggerProvider.LifecycleInfo(ctxNoCxl, fmt.Sprintf("configuration loaded in %s", took))
	}

	s := &shell[C, L]{
		name:             name,
		detached:         ctxNoCxl,
		loggerProvider:   loggerProvider,
		config:           config,
		options:          processedOptions,
		stopped:          make(chan struct{}),
		phaseCtx:         make(map[shutdown.Phase]context.Context),
		endPhaseCtx:      make(map[shutdown.Phase]context.CancelFunc),
		shutdownHandlers: make(map[shutdown.Phase][]registeredHandler),
	}
	for _, phase := range shutdown.Phases() {
		s.phaseCtx[phase], s.endPhaseCtx[phase] = context.WithCancel(ctxNoCxl)
	}

	// shutdown orchestration
	shutdownSignals := []os.Signal{syscall.SIGINT, syscall.SIGTERM}
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

// run calls application wiring, then awaits shutdown
func (s *shell[C, L]) run(wire Wiring[C, L]) int {
	ctx, cancel := context.WithCancel(s.detached)
	defer cancel()

	// blocking call, wiring must complete or exit with an error
	wireErr := s.callWire(ctx, wire)

	s.markStarted()
	if wireErr != nil {
		s.requestStop(fmt.Errorf("wiring failed: %w", wireErr))
	} else {
		s.loggerProvider.LifecycleInfo(s.detached, "started "+s.name)
	}

	<-s.stopped // a signal, Stop, Run's ctx, or wire failure
	err := s.shutdown(cancel)

	if wireErr != nil && !errors.Is(err, wireErr) {
		err = errors.Join(wireErr, err) // wire's error is a failure, even as a clean-looking stop reason
	}
	return s.exit(err)
}

// exit logs err, then "exiting <name>" as the last line, and returns 1 for an err, else the clean
// stop's exit code
func (s *shell[C, L]) exit(err error) int {
	exitCode := s.cleanExitCode()
	if err != nil {
		s.loggerProvider.LifecycleError(s.detached, "stopped with an error", err)
		exitCode = 1
	}
	s.loggerProvider.LifecycleInfo(s.detached, "exiting "+s.name)
	return exitCode
}

// cleanExitCode is 128 plus the signal when a signal other than SIGTERM asked for the stop, as the
// calling process expects of Ctrl+C (130), and 0 otherwise: SIGTERM is the platform's normal stop
func (s *shell[C, L]) cleanExitCode() int {
	received := stoppedBySignal(s.reason())
	if received == nil || received == syscall.SIGTERM {
		return 0
	}
	return signalExitCode(received)
}

// callWire calls wire, turning a panic into its error so the handlers registered before it still run
func (s *shell[C, L]) callWire(ctx context.Context, wire Wiring[C, L]) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("wiring panicked: %v\n%s", recovered, debug.Stack())
		}
	}()
	return wire(ctx, s)
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
