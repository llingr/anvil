// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

// Package shutdown defines the handlers a component shuts down through and
// the phases they run in
package shutdown

import "slices"

// Phase orders components during shutdown
type Phase string

// Phases run in this order
const (
	First   Phase = "FIRST"   // ingress: consumers / listeners (concurrent)
	Default Phase = "DEFAULT" // application components; sequential (registration order)
	Last    Phase = "LAST"    // egress: publishers / pools (concurrent)
)

var phases = []Phase{First, Default, Last}

// Phases in shutdown order
func Phases() []Phase {
	return slices.Clone(phases)
}
