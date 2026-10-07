// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"context"
	"fmt"
)

// LoggerProvider supplies the application's logger L, from whichever logging library it chose, and
// logs anvil's own lifecycle lines through it.
type LoggerProvider[L any] interface {
	// Logger exposes the underlying concrete logger
	Logger() L

	// LifecycleInfo logs anvil's lifecycle msg
	LifecycleInfo(ctx context.Context, msg string)

	// LifecycleError logs anvil's lifecycle msg with err
	LifecycleError(ctx context.Context, msg string, err error)

	// Flush writes out buffered lines, called by anvil before Run returns
	Flush()
}

// ConfigLogger optionally implemented by a LoggerProvider to
// call LogConfig instead of issuing a plain 'config loaded'
// message. Providers must take care not to expose secrets.
type ConfigLogger interface {
	LogConfig(ctx context.Context, msg string, config any)
}

// flushLogger, deferred, flushes the logger, first logging a panic, which then carries on to crash
// the process with Go's own trace
func flushLogger[L any](ctx context.Context, loggerProvider LoggerProvider[L]) {
	defer loggerProvider.Flush()
	if recovered := recover(); recovered != nil {
		loggerProvider.LifecycleError(ctx, "panicked", fmt.Errorf("%v", recovered))
		panic(recovered)
	}
}
