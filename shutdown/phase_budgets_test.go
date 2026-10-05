// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package shutdown

import (
	"testing"
	"time"
)

func TestBudgets_String(t *testing.T) {
	budgets := Budgets{Ingress: 14 * time.Second, Core: 7 * time.Second, Egress: 10 * time.Second}
	const want = "INGRESS 14s, CORE 7s, EGRESS 10s"
	if got := budgets.String(); got != want {
		t.Errorf("String() = %v, want %v", got, want)
	}
}
