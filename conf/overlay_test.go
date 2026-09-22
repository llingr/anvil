// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package conf

import (
	"reflect"
	"testing"
)

// fileTree is what the files defined, fresh per case because overlay writes into it
func fileTree() map[string]any {
	return map[string]any{
		"app": map[string]any{
			"port": 8080,
		},
	}
}

// Whatever reaches the overlay, it creates no key, so the files stay the complete list of settings
func TestOverlayCreatesNoKey(t *testing.T) {
	unknownKeys := []struct {
		name string
		src  map[string]any
	}{
		{"at the root", map[string]any{"rogue": "1"}},
		{"under a known branch", map[string]any{"app": map[string]any{"rogue": "1"}}},
		{"a branch of its own", map[string]any{"rogue": map[string]any{"port": "1"}}},
		{"differing only by case", map[string]any{"APP": map[string]any{"PORT": "1"}}},
	}
	for _, unknown := range unknownKeys {
		t.Run(unknown.name, func(t *testing.T) {
			dest := fileTree()
			overlay(unknown.src, dest)
			if before := fileTree(); !reflect.DeepEqual(dest, before) {
				t.Errorf("overlay changed the tree to %v, want %v", dest, before)
			}
		})
	}
}

// A value that is a branch where its key is not, or the reverse, leaves that key as the files set it
func TestOverlayLeavesAKeyItsValueDoesNotFit(t *testing.T) {
	mismatches := []struct {
		name string
		src  map[string]any
	}{
		{"scalar over a branch", map[string]any{"app": "1"}},
		{"branch over a scalar", map[string]any{"app": map[string]any{"port": map[string]any{"x": "1"}}}},
	}
	for _, mismatch := range mismatches {
		t.Run(mismatch.name, func(t *testing.T) {
			dest := fileTree()
			overlay(mismatch.src, dest)
			if port := dest["app"].(map[string]any)["port"]; port != 8080 {
				t.Errorf("port %v, want the file's 8080", port)
			}
		})
	}
}
