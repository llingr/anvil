// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

// Package lifecycle shuts an application down in phases, triggered by a
// trapped signal, a Stop call, or a cancelled context, each phase within its
// own budget, with unspent time rolling forward from phase to phase
package lifecycle

import (
	"context"
	"os"
	"os/signal"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/llingr/anvil/lifecycle/shutdown"
)

// Wiring for a host application wrapped by Application.Run;
// ctx is canceled after the last shutdown phase completes
type Wiring func(ctx context.Context, app Application) error

// Application runs an application's wiring, then its shutdown handlers phase by phase
// once the application is told to stop
type Application interface {
	// Run calls wiring, then awaits OS signals (SIGINT, SIGTERM)
	Run(wiring Wiring) error

	// RegisterShutdownHandler adds a hook to be invoked during shutdown
	RegisterShutdownHandler(name string, handler shutdown.Handler, phase shutdown.Phase)

	// Stop without using OS signals
	Stop(reason error)
}

type application struct {
	running          atomic.Bool
	wired            atomic.Bool
	shutdownSignals  []os.Signal
	signalled        context.Context // done once a trapped signal has arrived
	stopping         context.Context
	stop             context.CancelCauseFunc
	releaseSignals   context.CancelFunc
	shutdownBudgets  shutdown.Budgets
	mu               sync.Mutex // protects shutdownHandlers
	shutdownHandlers map[shutdown.Phase][]registeredHandler
}

// New traps signals at once, so one arriving before Run is not lost
func New(opts ...Option) Application {
	options := processOptions(opts...)

	shutdownSignals := []os.Signal{syscall.SIGINT, syscall.SIGTERM}
	for _, additional := range options.signals {
		if !slices.Contains(shutdownSignals, additional) {
			shutdownSignals = append(shutdownSignals, additional)
		}
	}

	signalled, releaseSignals := signal.NotifyContext(context.Background(), shutdownSignals...)
	stopping, stop := context.WithCancelCause(signalled)

	return &application{
		shutdownSignals:  shutdownSignals,
		signalled:        signalled,
		stopping:         stopping,
		stop:             stop,
		releaseSignals:   releaseSignals,
		shutdownBudgets:  options.shutdownBudgets,
		shutdownHandlers: make(map[shutdown.Phase][]registeredHandler),
	}
}

// Run application with supplied wiring, returning once shutdown has completed
func (a *application) Run(wiring Wiring) error {
	if !a.running.CompareAndSwap(false, true) {
		panic("lifecycle: Run called twice")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // the wiring's goroutines wind down after the handlers

	err := wiring(ctx, a)
	a.wired.Store(true)
	if err != nil {
		a.Stop(err)
	}
	return a.shutdown()
}

// Stop application (idempotent)
func (a *application) Stop(reason error) {
	a.stop(reason)
}
