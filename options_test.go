// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The one deadline has to leave room after the drain, and the panic names both durations
func TestDeadlineMustExceedTheDrain(t *testing.T) {
	const drain = 30 * time.Second
	func() {
		defer func() {
			msg := fmt.Sprint(recover())
			if !strings.HasPrefix(msg, "anvil: ") || !strings.Contains(msg, drain.String()) ||
				!strings.Contains(msg, (28*time.Second).String()) {
				t.Errorf("panic %q, want an anvil: message naming both durations", msg)
			}
		}()
		processOptions(WithDrainDelay(drain), WithShutdownGracePeriod(28*time.Second))
	}()
	func() {
		defer func() {
			if recover() == nil {
				t.Error("a grace period equal to the drain did not panic")
			}
		}()
		processOptions(WithDrainDelay(10*time.Second), WithShutdownGracePeriod(10*time.Second))
	}()
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("a grace period 1ns past the drain panicked: %v", r)
			}
		}()
		processOptions(WithDrainDelay(10*time.Second), WithShutdownGracePeriod(10*time.Second+time.Nanosecond))
	}()
}

func TestWithShutdownGracePeriodRejectsOutOfRange(t *testing.T) {
	for _, period := range []time.Duration{0, -time.Nanosecond, maxShutdown + time.Nanosecond} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("grace period %s did not panic", period)
				}
			}()
			WithShutdownGracePeriod(period)
		}()
	}
	WithShutdownGracePeriod(maxShutdown)
}

// The defaults fit Kubernetes' 30s grace period: a 5s drain inside a 28s deadline
func TestDefaultDrainDelayFitsTheGracePeriod(t *testing.T) {
	options := processOptions()
	if options.drainDelay != 5*time.Second {
		t.Errorf("drain delay %s, want 5s", options.drainDelay)
	}
	if options.shutdownDeadline != 28*time.Second {
		t.Errorf("shutdown deadline %s, want 28s", options.shutdownDeadline)
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
