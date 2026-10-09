// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"fmt"
	"strings"
	"time"
)

// deadlineError names every handler left running at the deadline or, when none is, the first
// handler never called. It is built only when the deadline passed with work unfinished.
func deadlineError(deadline time.Duration, groups []*shutdownGroup) error {
	var running []string
	var neverCalled string
	for _, group := range groups {
		for _, handler := range group.handlers {
			switch {
			case handler.isRunning():
				running = append(running, fmt.Sprintf("%s %s", group.label(), handler.name))
			case handler.isNeverCalled() && neverCalled == "":
				neverCalled = fmt.Sprintf("%s %s", group.label(), handler.name)
			}
		}
	}
	if len(running) > 0 {
		return fmt.Errorf("shutdown deadline %s passed: %s", deadline, strings.Join(running, ", "))
	}
	return fmt.Errorf("shutdown deadline %s passed before %s", deadline, neverCalled)
}
