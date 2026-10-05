// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package shutdown

import (
	"fmt"
	"strings"
	"time"
)

// Budgets is each phase's time within the shutdown; time a phase leaves unused carries over to the next
type Budgets map[Phase]time.Duration

// String reads "INGRESS 11.5s, CORE 5.75s, EGRESS 5.75s", in shutdown order
func (b Budgets) String() string {
	parts := make([]string, 0, len(phases))
	for _, phase := range phases {
		if budget, set := b[phase]; set {
			parts = append(parts, fmt.Sprintf("%s %s", phase, budget))
		}
	}
	return strings.Join(parts, ", ")
}
