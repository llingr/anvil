// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/llingr/anvil/lifecycle/shutdown"
)

// Option is a New functional option
type Option func(*Options)

// Options set by Option functions for New
type Options struct {
	shutdownBudgets shutdown.Budgets
	signals         []os.Signal
}

func processOptions(opts ...Option) Options {
	// defaultDeadline aligned to typical Kubernetes
	// termination grace period of 30s
	const defaultDeadline = 28 * time.Second

	options := Options{
		shutdownBudgets: shutdown.Budgets{
			shutdown.First:   defaultDeadline * 50 / 100, // 50% = 14s
			shutdown.Default: defaultDeadline * 25 / 100, // 25% = 7s
			shutdown.Last:    defaultDeadline * 25 / 100, // 25% = 7s
		},
		signals: []os.Signal{},
	}
	for _, option := range opts {
		option(&options)
	}
	return options
}

// WithShutdownPhaseBudget to override specific phases should defaults not
// align with application-specific requirements/context
func WithShutdownPhaseBudget(phase shutdown.Phase, budget time.Duration) Option {
	if !slices.Contains(shutdown.Phases(), phase) {
		panic(fmt.Errorf("lifecycle: no shutdown phase %q", phase))
	}
	const invalidPhaseBudget = "invalid %q shutdown budget: %s"
	if budget < 0 || budget > time.Hour {
		panic(fmt.Errorf(invalidPhaseBudget, phase, budget))
	}
	return func(options *Options) {
		options.shutdownBudgets[phase] = budget
	}
}

// WithSignals adds triggers to SIGINT and SIGTERM, which are always trapped
func WithSignals(signals ...os.Signal) Option {
	return func(options *Options) {
		options.signals = append(options.signals, signals...)
	}
}
