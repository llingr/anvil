// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"syscall"
	"testing"
	"time"

	"github.com/llingr/anvil/shutdown"
)

// The 28s deadline less the 5s drain splits 50/25/25 among the phases
func TestDefaultShutdownBudgets(t *testing.T) {
	const want = "INGRESS 11.5s, CORE 5.75s, EGRESS 5.75s"
	if got := processOptions().shutdownBudgets.String(); got != want {
		t.Errorf("budgets = %v, want %v", got, want)
	}
}

// A phase set by option keeps its time, and the others split what the deadline leaves 2:1
func TestWithShutdownPhaseBudgetOverridesOnePhase(t *testing.T) {
	budgets := processOptions(WithShutdownPhaseBudget(shutdown.Egress, 8*time.Second)).shutdownBudgets
	const want = "INGRESS 10s, CORE 5s, EGRESS 8s"
	if got := budgets.String(); got != want {
		t.Errorf("budgets = %v, want %v", got, want)
	}
}

// The deadline scales every phase the options leave unset, and the drain always comes out first
func TestWithShutdownDeadlineScalesThePhases(t *testing.T) {
	options := processOptions(WithShutdownDeadline(60*time.Second), WithDrainDelay(10*time.Second))
	const want = "INGRESS 25s, CORE 12.5s, EGRESS 12.5s"
	if got := options.shutdownBudgets.String(); got != want {
		t.Errorf("budgets = %v, want %v", got, want)
	}
	if total := options.drainDelay + totalOf(options.shutdownBudgets); total != 60*time.Second {
		t.Errorf("drain and phases %s, want the 60s deadline", total)
	}
}

// A drain and budgets that leave a phase no time panic as Run starts, not as the pod is killed
func TestShutdownOverrunPanics(t *testing.T) {
	for name, opts := range map[string][]Option{
		"drain past the deadline":     {WithDrainDelay(30 * time.Second)},
		"budgets past the deadline":   {WithShutdownPhaseBudget(shutdown.Ingress, 30*time.Second)},
		"nothing left for two phases": {WithShutdownPhaseBudget(shutdown.Ingress, 23*time.Second)},
		"a nanosecond left for two":   {WithShutdownPhaseBudget(shutdown.Ingress, 23*time.Second-time.Nanosecond)},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s did not panic", name)
				}
			}()
			processOptions(opts...)
		}()
	}
}

// totalOf sums the budgets of the phases that run
func totalOf(budgets shutdown.Budgets) time.Duration {
	var total time.Duration
	for _, phase := range shutdown.Phases() {
		total += budgets[phase]
	}
	return total
}

func TestWithShutdownDeadlineRejectsOutOfRange(t *testing.T) {
	for _, deadline := range []time.Duration{0, -time.Nanosecond, maxShutdown + time.Nanosecond} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("deadline %s did not panic", deadline)
				}
			}()
			WithShutdownDeadline(deadline)
		}()
	}
	WithShutdownDeadline(maxShutdown)
}

func TestWithShutdownPhaseBudgetRejectsUnknownPhase(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("unknown phase did not panic")
		}
	}()
	WithShutdownPhaseBudget(shutdown.Phase("ingress"), time.Second)
}

func TestWithShutdownPhaseBudgetRejectsOutOfRange(t *testing.T) {
	for _, budget := range []time.Duration{0, -time.Nanosecond, maxShutdown + time.Nanosecond} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("budget %s did not panic", budget)
				}
			}()
			WithShutdownPhaseBudget(shutdown.Ingress, budget)
		}()
	}
	for _, budget := range []time.Duration{time.Nanosecond, maxShutdown} {
		WithShutdownPhaseBudget(shutdown.Ingress, budget)
	}
}

// The drain and the phases together fill the 28s inside Kubernetes' 30s grace period, and a drain
// turned off gives its time to the phases
func TestDefaultDrainDelayFitsTheGracePeriod(t *testing.T) {
	options := processOptions()
	if options.drainDelay != 5*time.Second {
		t.Errorf("drain delay %s, want 5s", options.drainDelay)
	}
	if total := options.drainDelay + totalOf(options.shutdownBudgets); total != 28*time.Second {
		t.Errorf("drain and phases %s, want 28s", total)
	}
	if total := totalOf(processOptions(WithDrainDelay(0)).shutdownBudgets); total != 28*time.Second {
		t.Errorf("phases without a drain %s, want all 28s", total)
	}
}

func TestWithDrainDelayRejectsOutOfRange(t *testing.T) {
	for _, delay := range []time.Duration{-time.Nanosecond, maxShutdown + time.Nanosecond} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("drain delay %s did not panic", delay)
				}
			}()
			WithDrainDelay(delay)
		}()
	}
	WithDrainDelay(maxShutdown)
}

// No process can trap SIGKILL, so asking to is a mistake
func TestWithStopSignalsRejectsKill(t *testing.T) {
	func() {
		defer func() {
			if recover() == nil {
				t.Error("SIGKILL did not panic")
			}
		}()
		WithStopSignals(syscall.SIGHUP, syscall.SIGKILL)
	}()
	WithStopSignals(syscall.SIGHUP, syscall.SIGQUIT)
}
