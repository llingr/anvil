// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/llingr/anvil"
	"github.com/llingr/anvil/shutdown"
)

// FuzzWithShutdownPhaseBudget feeds arbitrary phases and budgets in: a known phase with a
// budget above zero and up to five minutes is accepted, anything else panics
func FuzzWithShutdownPhaseBudget(f *testing.F) {
	f.Add("INGRESS", int64(time.Second))
	f.Add("CORE", int64(0))
	f.Add("EGRESS", int64(5*time.Minute))
	f.Add("ingress", int64(time.Second))
	f.Add("", int64(-1))
	f.Add("INGRESS", int64(5*time.Minute+1))

	f.Fuzz(func(t *testing.T, phaseName string, nanoseconds int64) {
		phase := shutdown.Phase(phaseName)
		budget := time.Duration(nanoseconds)
		valid := slices.Contains(shutdown.Phases(), phase) && budget > 0 && budget <= 5*time.Minute

		defer func() {
			switch recovered := recover(); {
			case recovered == nil && !valid:
				t.Fatalf("phase %q budget %s was accepted", phaseName, budget)
			case recovered != nil && valid:
				t.Fatalf("phase %q budget %s panicked: %v", phaseName, budget, recovered)
			}
		}()

		anvil.WithShutdownPhaseBudget(phase, budget)
	})
}

// FuzzRegisterShutdownHandler feeds arbitrary names and phases in: an accepted handler runs
// during shutdown, and anything invalid panics at registration
func FuzzRegisterShutdownHandler(f *testing.F) {
	f.Add("http server", "INGRESS")
	f.Add("outbox", "EGRESS")
	f.Add("", "CORE")
	f.Add("orders", "ingress")
	f.Add("orders", "")

	f.Fuzz(func(t *testing.T, name, phaseName string) {
		phase := shutdown.Phase(phaseName)
		valid := name != "" && slices.Contains(shutdown.Phases(), phase)

		ran := false
		anvil.Run(context.Background(), "test", noConfig{}, stderrLogging{}, func(ctx context.Context, shell testShell) error {
			defer func() {
				switch recovered := recover(); {
				case recovered == nil && !valid:
					t.Errorf("name %q phase %q was accepted", name, phaseName)
				case recovered != nil && valid:
					t.Errorf("name %q phase %q panicked: %v", name, phaseName, recovered)
				}
				shell.Stop(nil)
			}()
			shell.RegisterShutdownHandler(phase, name, func(context.Context) error {
				ran = true
				return nil
			})
			return nil
		})

		if ran != valid {
			t.Fatalf("name %q phase %q ran %v, want %v", name, phaseName, ran, valid)
		}
	})
}
