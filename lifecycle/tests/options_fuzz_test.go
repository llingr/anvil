// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/llingr/anvil/lifecycle"
	"github.com/llingr/anvil/lifecycle/shutdown"
)

// FuzzWithShutdownPhaseBudget feeds arbitrary phases and budgets in: a known phase with a
// budget between zero and an hour is accepted, anything else panics
func FuzzWithShutdownPhaseBudget(f *testing.F) {
	f.Add("FIRST", int64(time.Second))
	f.Add("DEFAULT", int64(0))
	f.Add("LAST", int64(time.Hour))
	f.Add("first", int64(time.Second))
	f.Add("", int64(-1))
	f.Add("FIRST", int64(time.Hour+1))

	f.Fuzz(func(t *testing.T, phaseName string, nanoseconds int64) {
		phase := shutdown.Phase(phaseName)
		budget := time.Duration(nanoseconds)
		valid := slices.Contains(shutdown.Phases(), phase) && budget >= 0 && budget <= time.Hour

		defer func() {
			switch recovered := recover(); {
			case recovered == nil && !valid:
				t.Fatalf("phase %q budget %s was accepted", phaseName, budget)
			case recovered != nil && valid:
				t.Fatalf("phase %q budget %s panicked: %v", phaseName, budget, recovered)
			}
		}()

		lifecycle.New(lifecycle.WithShutdownPhaseBudget(phase, budget))
	})
}

// FuzzRegisterShutdownHandler feeds arbitrary names and phases in: an accepted handler runs
// during shutdown, and anything invalid panics at registration
func FuzzRegisterShutdownHandler(f *testing.F) {
	f.Add("http server", "FIRST")
	f.Add("outbox", "LAST")
	f.Add("", "DEFAULT")
	f.Add("orders", "first")
	f.Add("orders", "")

	f.Fuzz(func(t *testing.T, name, phaseName string) {
		phase := shutdown.Phase(phaseName)
		valid := name != "" && slices.Contains(shutdown.Phases(), phase)

		ran := false
		err := lifecycle.New().Run(func(ctx context.Context, app lifecycle.Application) error {
			defer func() {
				switch recovered := recover(); {
				case recovered == nil && !valid:
					t.Errorf("name %q phase %q was accepted", name, phaseName)
				case recovered != nil && valid:
					t.Errorf("name %q phase %q panicked: %v", name, phaseName, recovered)
				}
				app.Stop(nil)
			}()
			app.RegisterShutdownHandler(name, func(context.Context) error {
				ran = true
				return nil
			}, phase)
			return nil
		})
		if err != nil {
			t.Fatalf("name %q phase %q: %v", name, phaseName, err)
		}
		if ran != valid {
			t.Fatalf("name %q phase %q ran %v, want %v", name, phaseName, ran, valid)
		}
	})
}
