// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package shutdown

import (
	"fmt"
	"time"
)

// Budgets for shutdown Phase durations. Unused budget
// from each phase carries over to the next
type Budgets map[Phase]time.Duration

// TotalDuration sums the phase budgets
func (b Budgets) TotalDuration() time.Duration {
	return b[First] + b[Default] + b[Last]
}

func (b Budgets) String() string {
	const budgets = "total: %s, first: %s, default: %s, last: %s"
	return fmt.Sprintf(budgets, b.TotalDuration(), b[First], b[Default], b[Last])
}
