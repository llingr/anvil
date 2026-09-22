// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"testing"
	"time"

	"github.com/llingr/anvil/lifecycle/shutdown"
)

func TestDefaultShutdownBudgets(t *testing.T) {
	const want = "total: 28s, first: 14s, default: 7s, last: 7s"
	if got := processOptions().shutdownBudgets.String(); got != want {
		t.Errorf("budgets = %v, want %v", got, want)
	}
}

func TestWithShutdownPhaseBudgetOverridesOnePhase(t *testing.T) {
	budgets := processOptions(WithShutdownPhaseBudget(shutdown.Last, 10*time.Second)).shutdownBudgets
	const want = "total: 31s, first: 14s, default: 7s, last: 10s"
	if got := budgets.String(); got != want {
		t.Errorf("budgets = %v, want %v", got, want)
	}
}

func TestWithShutdownPhaseBudgetRejectsUnknownPhase(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("unknown phase did not panic")
		}
	}()
	WithShutdownPhaseBudget(shutdown.Phase("first"), time.Second)
}

func TestWithShutdownPhaseBudgetRejectsOutOfRange(t *testing.T) {
	for _, budget := range []time.Duration{-time.Nanosecond, time.Hour + time.Nanosecond} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("budget %s did not panic", budget)
				}
			}()
			WithShutdownPhaseBudget(shutdown.First, budget)
		}()
	}
	for _, budget := range []time.Duration{0, time.Hour} {
		WithShutdownPhaseBudget(shutdown.First, budget)
	}
}
