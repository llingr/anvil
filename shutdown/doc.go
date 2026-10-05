// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

// Package shutdown defines the handlers a component shuts down through and the phases anvil runs
// them in. Ingress and Egress run their handlers concurrently; Core runs its handlers one at a time,
// in reverse registration order.
package shutdown
