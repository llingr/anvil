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

// Shell is the handle wire receives: the service's config C and logger L, and the means to
// register its shutdown handlers and to stop it.
type Shell[C, L any] interface {
	// RegisterShutdownHandler adds handler to phase under name, from wire before it returns. A later
	// call, such as from a reconnect loop or a lazily built component, is refused: anvil logs an
	// error and the handler never runs. Register a component once in wire, and have its handler
	// cope with whatever state the component is in at shutdown.
	RegisterShutdownHandler(phase shutdown.Phase, name string, handler shutdown.Handler)

	// Go runs fn on its own goroutine, with a ctx cancelled as phase begins, and has phase wait for
	// it to return. An error or panic from fn stops the service, under name, and a nil return before
	// any stop is logged as an error; once told to stop, fn returns nil, and an error then fails the
	// shutdown. Like RegisterShutdownHandler it is refused once wire has returned.
	Go(phase shutdown.Phase, name string, fn func(ctx context.Context) error)

	// Stop begins shutdown: a nil reason is a clean stop and any other makes Run return 1; the first
	// reason wins
	Stop(reason error)

	// Config provided during Run
	Config() C

	// Logger convenience accessor to chosen logger
	Logger() L

	// Stopping reports whether shutdown has begun, from any trigger, for a
	// readiness probe to fail during shutdown
	Stopping() bool
}

// Wiring builds the application's components and registers their shutdown handlers, then returns
// rather than blocking. Its ctx carries the values of Run's ctx but has a cancellation of its own,
// which fires as the shutdown phases begin: after any drain delay, before Ingress.
type Wiring[C, L any] func(ctx context.Context, shell Shell[C, L]) error

type shell[C, L any] struct {
	name             string
	detached         context.Context // Run's ctx without its cancellation: logging, wire and the handlers build on it
	loggerProvider   LoggerProvider[L]
	config           C
	options          options
	phaseCtx         map[shutdown.Phase]context.Context    // what Go hands its goroutines
	endPhaseCtx      map[shutdown.Phase]context.CancelFunc // called as each phase begins
	stopped          chan struct{}                         // closed by the first stop, once stoppedAt and stopReason are set
	mu               sync.Mutex                            // protects started, stoppedAt, stopReason and shutdownHandlers
	started          bool                                  // wire has returned, closing registration
	stoppedAt        time.Time                             // when the stop that won arrived
	stopReason       error                                 // nil for a clean Stop(nil)
	shutdownHandlers map[shutdown.Phase][]registeredHandler
}

// Run is the service's whole lifecycle, and returns the exit code for main to pass to os.Exit.
//
// It logs "starting <name>", loads the configuration, then traps SIGINT and SIGTERM. It calls wire
// and waits for a trapped signal, the cancellation of ctx, or a Stop. Last, it runs the shutdown
// handlers in phases and logs "exiting <name>".
//
// It returns 0 after a clean stop, or 130 when Ctrl+C asked for it (128 plus the signal for any
// other signal but SIGTERM), as the calling process expects of an interrupt. It returns 1 after a
// failure it has logged: a configuration that cannot be loaded, wire, the stop reason or a handler
// failing. A second SIGINT or SIGTERM ends the process at once, by Go's default handling, so Run
// does not return.
//
// A nil provider or wire panics. Any other panic escaping Run is logged and the logger flushed,
// then raised again.
func Run[C, L any](
	ctx context.Context, name string,
	configProvider ConfigProvider[C],
	loggerProvider LoggerProvider[L],
	wire Wiring[C, L],
	opts ...Option) int {

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

	processedOptions := processOptions(opts...)

	loggerProvider.LifecycleInfo(ctxNoCxl, "starting "+name)

	// config
	loggerProvider.LifecycleInfo(ctxNoCxl, "loading config")
	loadStart := time.Now()

	// blocking call, config providers must manage their own timeouts
	config, err := configProvider.Load(ctx)
	if err != nil {
		loggerProvider.LifecycleError(ctxNoCxl, "loading config", err)
		loggerProvider.LifecycleInfo(ctxNoCxl, "exiting "+name)
		return 1
	}
	const configLoaded = "configuration loaded in %s"
	loggerProvider.LifecycleInfo(ctxNoCxl, fmt.Sprintf(configLoaded, time.Since(loadStart).Truncate(time.Microsecond)))

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

	// trapped only now: during the load the default handling ends the process, nothing having started
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
	return s.run(wire)
}

// run calls wire, awaits the stop and shuts down, returning the exit code
func (s *shell[C, L]) run(wire Wiring[C, L]) int {
	ctx, cancel := context.WithCancel(s.detached)
	defer cancel()

	wireErr := s.callWire(ctx, wire) // wire has no timeout, wiring must complete or exit with an error
	s.markStarted()
	if wireErr != nil {
		s.requestStop(fmt.Errorf("wiring failed: %w", wireErr))
	} else {
		s.loggerProvider.LifecycleInfo(s.detached, "started "+s.name)
	}
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
