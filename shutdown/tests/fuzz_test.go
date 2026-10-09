// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/llingr/anvil/shutdown"
)

// FuzzIgnoreContextPassesTheError checks the adapted function's error comes back untouched
func FuzzIgnoreContextPassesTheError(f *testing.F) {
	f.Add("close failed")
	f.Add("")

	f.Fuzz(func(t *testing.T, message string) {
		var failed error
		if message != "" {
			failed = errors.New(message)
		}
		handler := shutdown.IgnoreContext(func() error {
			return failed
		})
		if err := handler(context.Background()); !errors.Is(err, failed) {
			t.Fatalf("error %v, want %v", err, failed)
		}
	})
}

// FuzzNamedGivesTheNameBack checks Named keeps any name as given, for anvil to validate
func FuzzNamedGivesTheNameBack(f *testing.F) {
	f.Add("outbox")
	f.Add("")
	f.Add("a->b")

	f.Fuzz(func(t *testing.T, name string) {
		named := shutdown.Named(name, shutdown.HandlerFunc(func(context.Context) error {
			return nil
		}))
		got, ok := named.(interface{ ShutdownName() string })
		if !ok || got.ShutdownName() != name {
			t.Fatalf("Named(%q) does not give its name back", name)
		}
	})
}
