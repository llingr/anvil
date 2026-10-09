// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"errors"
	"syscall"
	"testing"
)

// otherSignal is an os.Signal that is not a syscall.Signal, so it has no number
type otherSignal struct{}

func (otherSignal) String() string {
	return "other"
}

func (otherSignal) Signal() {
}

// A clean stop exits 0, or 128 plus the signal for a signal other than SIGTERM; a signal with no
// number exits 1
func TestCleanExitCode(t *testing.T) {
	cases := []struct {
		name   string
		reason error
		want   int
	}{
		{"Stop(nil)", nil, 0},
		{"Run's ctx", errRunCtxCancelled, 0},
		{"SIGTERM", signalStop{syscall.SIGTERM}, 0},
		{"SIGINT", signalStop{syscall.SIGINT}, 130},
		{"SIGHUP", signalStop{syscall.SIGHUP}, 129},
		{"a signal with no number", signalStop{otherSignal{}}, 1},
		{"a reason that is not a signal", errors.New("lease lost"), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := cleanExitCode(tc.reason); got != tc.want {
				t.Errorf("exit code %d, want %d", got, tc.want)
			}
		})
	}
}
