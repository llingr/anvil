// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package shutdown_test

import (
	"context"
	"errors"
	"testing"

	"github.com/llingr/anvil/lifecycle/shutdown"
)

func TestIgnoreContextCallsTheFunction(t *testing.T) {
	failed := errors.New("close failed")
	err := shutdown.IgnoreContext(func() error {
		return failed
	})(context.Background())
	if !errors.Is(err, failed) {
		t.Fatalf("error %v, want the function's", err)
	}
}
