// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package shutdown

import "context"

// Handler shuts down one component within ctx's deadline
type Handler func(ctx context.Context) error

// IgnoreContext adapts a shutdown function that takes no context, such as a server's Close
func IgnoreContext(handler func() error) Handler {
	return func(_ context.Context) error {
		return handler()
	}
}

// Close adapts a close function that takes no context and returns no error, such as a pool's Close
func Close(closer func()) Handler {
	return func(_ context.Context) error {
		closer()
		return nil
	}
}
