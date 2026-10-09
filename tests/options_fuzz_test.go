// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/llingr/anvil"
	"github.com/llingr/anvil/shutdown"
)

// FuzzAnyNames names a group with SetName, then adds two handlers named by shutdown.Named, to that
// group or the second to a group of its own. No name is checked, so whatever the names, the same
// name twice included, nothing panics and both handlers run.
func FuzzAnyNames(f *testing.F) {
	f.Add("PAYMENTS", "http server", "outbox", false)
	f.Add("PAYMENTS", "outbox", "outbox", false)
	f.Add("PAYMENTS", "outbox", "outbox", true)
	f.Add("DB POOLS", "", "ledger", false)
	f.Add("X", "a->b", "a-b", true)
	f.Add("X", "a[0]", "a;b", false)
	f.Add("X", "a,b", "a\nb", true)
	f.Add("X", "a\rb", "a"+string(rune(0x2028))+"b", false)
	f.Add("X", "a"+string(rune(0xa0))+"b", "a\tb", true)
	f.Add("", "\xff", "café", false)

	f.Fuzz(func(t *testing.T, groupName, first, second string, apart bool) {
		var ran [2]atomic.Bool
		var recovered any
		handler := func(index int, name string) shutdown.Handler {
			return shutdown.Named(name, shutdown.HandlerFunc(func(context.Context) error {
				ran[index].Store(true)
				return nil
			}))
		}
		code := anvil.Run(context.Background(), "test", noConfig{}, stderrLogging{}, func(ctx context.Context, shell testShell) error {
			defer shell.Stop(nil)
			defer func() {
				recovered = recover()
			}()
			group := shell.AddShutdownGroup().SetName(groupName)
			group.Add(handler(0, first))
			if apart {
				group = shell.AddShutdownGroup()
			}
			group.Add(handler(1, second))
			return nil
		}, anvil.WithDrainDelay(0))

		if recovered != nil {
			t.Fatalf("group %q, handlers %q and %q panicked: %v", groupName, first, second, recovered)
		}
		if code != 0 {
			t.Errorf("exit code %d, want 0", code)
		}
		if !ran[0].Load() || !ran[1].Load() {
			t.Errorf("handlers %q and %q ran %v and %v, want both run", first, second, ran[0].Load(), ran[1].Load())
		}
	})
}
