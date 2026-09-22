// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package shutdown

import "context"

// Handler for shutting down a component
type Handler func(ctx context.Context) error

// IgnoreContext adapts a Shutdown function that has no context
func IgnoreContext(handler func() error) Handler {
	return func(_ context.Context) error {
		return handler()
	}
}
