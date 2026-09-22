// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/llingr/anvil/lifecycle/shutdown"
)

// FuzzBudgetsTotalDuration feeds arbitrary budgets in: the total is the three phases summed,
// and String reports the same total
func FuzzBudgetsTotalDuration(f *testing.F) {
	f.Add(int64(14*time.Second), int64(7*time.Second), int64(7*time.Second))
	f.Add(int64(0), int64(0), int64(0))
	f.Add(int64(-1), int64(1), int64(0))
	f.Add(int64(time.Hour), int64(time.Hour), int64(time.Hour))

	f.Fuzz(func(t *testing.T, first, other, last int64) {
		budgets := shutdown.Budgets{
			shutdown.First:   time.Duration(first),
			shutdown.Default: time.Duration(other),
			shutdown.Last:    time.Duration(last),
		}
		want := time.Duration(first) + time.Duration(other) + time.Duration(last)
		if got := budgets.TotalDuration(); got != want {
			t.Fatalf("TotalDuration() = %s, want %s", got, want)
		}
		if !strings.Contains(budgets.String(), want.String()) {
			t.Fatalf("String() = %q, want the total %s in it", budgets.String(), want)
		}
	})
}

// FuzzBudgetsIgnoreUnknownPhases checks a phase nobody runs cannot change the total
func FuzzBudgetsIgnoreUnknownPhases(f *testing.F) {
	f.Add("FIRST", int64(time.Second))
	f.Add("first", int64(time.Second))
	f.Add("", int64(time.Second))
	f.Add("SECOND", int64(time.Second))

	f.Fuzz(func(t *testing.T, phaseName string, nanoseconds int64) {
		budgets := shutdown.Budgets{}
		budgets[shutdown.Phase(phaseName)] = time.Duration(nanoseconds)

		want := time.Duration(0)
		if slices.Contains(shutdown.Phases(), shutdown.Phase(phaseName)) {
			want = time.Duration(nanoseconds)
		}
		if got := budgets.TotalDuration(); got != want {
			t.Fatalf("phase %q gave a total of %s, want %s", phaseName, got, want)
		}
	})
}

// FuzzIgnoreContextPassesTheError checks the adapted function's error comes back untouched
func FuzzIgnoreContextPassesTheError(f *testing.F) {
	f.Add("close failed")
	f.Add("")

	f.Fuzz(func(t *testing.T, message string) {
		var failed error
		if message != "" {
			failed = errors.New(message)
		}
		handler := shutdown.IgnoreContext(func() error {
			return failed
		})
		if err := handler(context.Background()); !errors.Is(err, failed) {
			t.Fatalf("error %v, want %v", err, failed)
		}
	})
}
