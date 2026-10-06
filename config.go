// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import "context"

// ConfigProvider loads the application's typed configuration C from wherever the application keeps
// it: files, the environment, a secrets store. A provider that logs while loading takes its
// logger through its own constructor.
type ConfigProvider[C any] interface {
	// Load is a blocking call, providers may include their own deadline
	Load(ctx context.Context) (C, error)
}
