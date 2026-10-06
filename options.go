// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"fmt"
	"os"
	"slices"
	"syscall"
	"time"

	"github.com/llingr/anvil/shutdown"
)

// Option is a Run functional option
type Option func(*options)

// options set by Option functions for Run
type options struct {
	drainDelay       time.Duration
	shutdownDeadline time.Duration
	phaseBudgets     shutdown.Budgets // the phases WithShutdownPhaseBudget set
	shutdownBudgets  shutdown.Budgets // every phase, the rest sharing what the deadline leaves
	signals          []os.Signal
}

// maxShutdown caps the deadline, the drain and each phase budget: no grace period comes near it,
// so a minutes-for-seconds slip such as 28*time.Minute panics
const maxShutdown = 5 * time.Minute

// phaseWeights split the shutdown deadline, less the drain, among the phases no option set
var phaseWeights = map[shutdown.Phase]int{
	shutdown.Ingress: 2,
	shutdown.Core:    1,
	shutdown.Egress:  1,
}

// processOptions applies opts to the defaults, panicking when the drain and the phase budgets set
// leave a phase nothing within the shutdown deadline
func processOptions(opts ...Option) options {
	options := options{
		drainDelay:       5 * time.Second,  // Kubernetes takes 1-5s to take the pod out of its endpoints
		shutdownDeadline: 28 * time.Second, // inside Kubernetes' 30s termination grace period
		phaseBudgets:     shutdown.Budgets{},
	}
	for _, option := range opts {
		option(&options)
	}

	remaining := options.shutdownDeadline - options.drainDelay
	var unsetWeight int
	for _, phase := range shutdown.Phases() {
		budget, set := options.phaseBudgets[phase]
		if set {
			remaining -= budget
			continue
		}
		unsetWeight += phaseWeights[phase]
	}
	const overrun = "anvil: a %s drain and phase budgets %s leave nothing within the %s shutdown deadline"
	if remaining < 0 {
		panic(fmt.Errorf(overrun, options.drainDelay, options.phaseBudgets, options.shutdownDeadline))
	}
	options.shutdownBudgets = shutdown.Budgets{}
	for _, phase := range shutdown.Phases() {
		budget, set := options.phaseBudgets[phase]
		if !set {
			budget = remaining * time.Duration(phaseWeights[phase]) / time.Duration(unsetWeight)
		}
		if budget <= 0 { // a few nanoseconds left divide down to nothing
			panic(fmt.Errorf(overrun, options.drainDelay, options.phaseBudgets, options.shutdownDeadline))
		}
		options.shutdownBudgets[phase] = budget
	}
	return options
}

// WithShutdownDeadline sets the time the whole shutdown has, drain included: 28s by default, inside
// Kubernetes' 30s grace period. What the drain leaves is split 50/25/25 among Ingress, Core and
// Egress, less any phase WithShutdownPhaseBudget sets. It panics on a deadline of zero or less, or
// over five minutes.
func WithShutdownDeadline(deadline time.Duration) Option {
	if deadline <= 0 || deadline > maxShutdown {
		panic(fmt.Errorf("anvil: invalid shutdown deadline: %s", deadline))
	}
	return func(options *options) {
		options.shutdownDeadline = deadline
	}
}

// WithShutdownPhaseBudget sets one phase's time, in place of its part of the shutdown deadline; the
// phases left share what remains. It panics on an unknown phase, or a budget of zero or less, or
// over five minutes.
func WithShutdownPhaseBudget(phase shutdown.Phase, budget time.Duration) Option {
	if !slices.Contains(shutdown.Phases(), phase) {
		panic(fmt.Errorf("anvil: no shutdown phase %q", phase))
	}
	if budget <= 0 || budget > maxShutdown {
		panic(fmt.Errorf("anvil: invalid %q shutdown budget: %s", phase, budget))
	}
	return func(options *options) {
		options.phaseBudgets[phase] = budget
	}
}

// WithDrainDelay sets how long the application keeps serving after SIGTERM, before Ingress, while
// Kubernetes takes the pod out of its endpoints: 5s by default, and 0 turns it off. A shutdown
// started any other way skips the wait and gives its time to the phases. It panics on a negative
// delay or one over five minutes.
func WithDrainDelay(delay time.Duration) Option {
	if delay < 0 || delay > maxShutdown {
		panic(fmt.Errorf("anvil: invalid drain delay: %s", delay))
	}
	return func(options *options) {
		options.drainDelay = delay
	}
}

// WithStopSignals adds signals that stop the application as Ctrl+C's SIGINT does: a clean stop with
// no drain. SIGTERM, always trapped, keeps its drain if passed here too. A second SIGUSR1 or SIGUSR2
// does not end the process as a second SIGINT does: Go ignores them once they are no longer trapped.
// It panics on SIGKILL, which no process can trap.
func WithStopSignals(signals ...os.Signal) Option {
	for _, stopSignal := range signals {
		if stopSignal == syscall.SIGKILL {
			panic(fmt.Errorf("anvil: %s cannot be trapped", stopSignal))
		}
	}
	return func(options *options) {
		options.signals = append(options.signals, signals...)
	}
}
