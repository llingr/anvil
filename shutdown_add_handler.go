// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"errors"

	"github.com/llingr/anvil/shutdown"
)

// errAddedLate refuses a change to the shutdown made once wire has returned
var errAddedLate = errors.New("shutdown groups can only be changed during wiring")

// AddShutdownGroup adds a group, holding handlers, that stops before the ones already added. Once
// wire has returned it logs the refusal, and the group it returns refuses every change in the same way.
// Every change is decided under s.mu, so one made as wire returns is either kept or refused, never
// lost, and a refusal is logged once the lock is released, since requestStop waits on that lock.
func (s *shell[C, L]) AddShutdownGroup(handlers ...shutdown.Handler) ShutdownGroup {
	group := &shutdownGroup{
		mu:          &s.mu,
		started:     &s.started,
		logger:      s.loggerProvider,
		runCtx:      s.runCtx,
		goCtx:       s.goCtx,
		stopped:     s.stopped,
		requestStop: s.requestStop,
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		s.loggerProvider.LifecycleError(s.runCtx, "shutdown group not added", errAddedLate)
		return group
	}
	s.groups = append(s.groups, group)
	group.order = len(s.groups)
	s.mu.Unlock()
	return group.Add(handlers...)
}
