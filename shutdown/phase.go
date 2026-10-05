// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package shutdown

import "slices"

// Phase is a stage of the shutdown; the three run in order
type Phase string

// Phases run in this order
const (
	Ingress Phase = "INGRESS" // consumers, listeners - concurrent
	Core    Phase = "CORE"    // application services - sequential, in reverse registration order
	Egress  Phase = "EGRESS"  // publishers, pools - concurrent
)

var phases = []Phase{Ingress, Core, Egress}

// Phases returns the phases in shutdown order
func Phases() []Phase {
	return slices.Clone(phases)
}
