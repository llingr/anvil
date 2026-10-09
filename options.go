// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"fmt"
	"os"
	"syscall"
	"time"
)

// Option is a Run functional option
type Option func(*options)

// options set by Option functions for Run
type options struct {
	drainDelay       time.Duration
	shutdownDeadline time.Duration
	signals          []os.Signal
}

// maxShutdown caps the deadline and the drain: no grace period comes near it, so a
// minutes-for-seconds slip such as 28*time.Minute panics
const maxShutdown = 5 * time.Minute

// processOptions applies opts to the defaults, panicking when the shutdown grace period leaves
// nothing after the drain
func processOptions(opts ...Option) options {
	options := options{
		drainDelay:       5 * time.Second,  // Kubernetes takes 1-5s to take the pod out of its endpoints
		shutdownDeadline: 28 * time.Second, // inside Kubernetes' 30s termination grace period
	}
	for _, option := range opts {
		option(&options)
	}
	if options.shutdownDeadline <= options.drainDelay {
		const overrun = "anvil: the %s shutdown grace period must be greater than the %s drain delay"
		panic(fmt.Errorf(overrun, options.shutdownDeadline, options.drainDelay))
	}
	return options
}

// WithShutdownGracePeriod sets the time the whole shutdown has, counted from the stop and including
// the drain: 28s by default, inside Kubernetes' 30s terminationGracePeriodSeconds. Its end is every
// handler's ctx deadline, and no handler is called once it has passed. It panics on a period of zero
// or less, or over five minutes, and Run panics if it is not greater than the drain delay.
func WithShutdownGracePeriod(period time.Duration) Option {
	if period <= 0 || period > maxShutdown {
		panic(fmt.Errorf("anvil: invalid shutdown grace period: %s", period))
	}
	return func(options *options) {
		options.shutdownDeadline = period
	}
}

// WithDrainDelay sets how long the application keeps serving after SIGTERM, before the first phase,
// while Kubernetes takes the pod out of its endpoints: 5s by default, and 0 turns it off. The drain
// is counted from the stop and spends the shutdown deadline. A shutdown started any other way skips
// the wait and gives its time to the phases. It panics on a negative delay or one over five minutes.
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
