// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

// Package shutdown defines the handler a component shuts down through: anything with a
// Shutdown(ctx) error method, as http.Server has, plus adapters for functions and Named for a
// handler whose type does not name it.
package shutdown
