// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/llingr/anvil/shutdown"
)

// FuzzBudgetsString feeds arbitrary budgets in: String reports each phase with its budget
func FuzzBudgetsString(f *testing.F) {
	f.Add(int64(14*time.Second), int64(7*time.Second), int64(7*time.Second))
	f.Add(int64(0), int64(0), int64(0))
	f.Add(int64(-1), int64(1), int64(0))
	f.Add(int64(time.Hour), int64(time.Hour), int64(time.Hour))

	f.Fuzz(func(t *testing.T, first, other, last int64) {
		budgets := shutdown.Budgets{
			shutdown.Ingress: time.Duration(first),
			shutdown.Core:    time.Duration(other),
			shutdown.Egress:  time.Duration(last),
		}
		for _, phase := range shutdown.Phases() {
			if part := fmt.Sprintf("%s %s", phase, budgets[phase]); !strings.Contains(budgets.String(), part) {
				t.Fatalf("String() = %q, want %q in it", budgets.String(), part)
			}
		}
	})
}

// FuzzBudgetsStringIgnoresUnknownPhases checks a phase nobody runs never reaches the log line
func FuzzBudgetsStringIgnoresUnknownPhases(f *testing.F) {
	f.Add("INGRESS", int64(time.Second))
	f.Add("ingress", int64(time.Second))
	f.Add("", int64(time.Second))
	f.Add("SECOND", int64(time.Second))

	f.Fuzz(func(t *testing.T, phaseName string, nanoseconds int64) {
		budgets := shutdown.Budgets{}
		budgets[shutdown.Phase(phaseName)] = time.Duration(nanoseconds)

		want := ""
		if slices.Contains(shutdown.Phases(), shutdown.Phase(phaseName)) {
			want = fmt.Sprintf("%s %s", phaseName, time.Duration(nanoseconds))
		}
		if got := budgets.String(); got != want {
			t.Fatalf("phase %q gave %q, want %q", phaseName, got, want)
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
