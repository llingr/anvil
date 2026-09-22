// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package shutdown

import (
	"testing"
	"time"
)

func TestBudgets_TotalDuration(t *testing.T) {
	budgets := Budgets{First: 14 * time.Second, Default: 7 * time.Second, Last: 7 * time.Second}
	if got, want := budgets.TotalDuration(), 28*time.Second; got != want {
		t.Errorf("TotalDuration() = %v, want %v", got, want)
	}
}

func TestBudgets_TotalDuration_ZeroValues(t *testing.T) {
	// confirm zero values from map don't panic
	budgets := Budgets{}
	if got := budgets.TotalDuration(); got != 0 {
		t.Errorf("TotalDuration() = %v, want %v", got, time.Duration(0))
	}
}

func TestBudgets_String(t *testing.T) {
	budgets := Budgets{First: 14 * time.Second, Default: 7 * time.Second, Last: 10 * time.Second}
	const want = "total: 31s, first: 14s, default: 7s, last: 10s"
	if got := budgets.String(); got != want {
		t.Errorf("String() = %v, want %v", got, want)
	}
}
