// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"errors"
	"fmt"
	"slices"

	"github.com/llingr/anvil/shutdown"
)

// registeredHandler is a handler and the name its errors carry
type registeredHandler struct {
	name            string
	shutdownHandler shutdown.Handler
}

// errRegisteredLate refuses a handler registered once wire has returned
var errRegisteredLate = errors.New("registrations are only permitted during startup, while wire runs")

// RegisterShutdownHandler adds handler to phase under name. Once wire has returned it logs the
// refusal and leaves the handler out, so a late call from another goroutine never crashes the process.
func (s *shell[C, L]) RegisterShutdownHandler(phase shutdown.Phase, name string, shutdownHandler shutdown.Handler) {
	if !s.register(phase, name, shutdownHandler) {
		s.refused(name)
	}
}

// refused logs a registration made once wire has returned
func (s *shell[C, L]) refused(name string) {
	s.loggerProvider.LifecycleError(s.detached, fmt.Sprintf("shutdown handler %q not registered", name), errRegisteredLate)
}

// register adds the handler unless wire has returned, deciding under the lock markStarted takes,
// so a registration racing wire's return is either kept or refused, never lost
func (s *shell[C, L]) register(phase shutdown.Phase, name string, shutdownHandler shutdown.Handler) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return false
	}
	validateArgs(phase, name, shutdownHandler)
	s.shutdownHandlers[phase] = append(s.shutdownHandlers[phase], registeredHandler{name, shutdownHandler})
	return true
}

// markStarted closes registration as wire returns
func (s *shell[C, L]) markStarted() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started = true
}

// validateArgs panics on a registration made by mistake: Run turns the panic into wire's error
// when wire itself made the call, and on any other goroutine it crashes the process
func validateArgs(phase shutdown.Phase, name string, handler shutdown.Handler) {
	switch {
	case name == "":
		panic("anvil: shutdown handler has no name")
	case handler == nil:
		panic(fmt.Sprintf("anvil: %s has a nil handler", name))
	case !slices.Contains(shutdown.Phases(), phase):
		panic(fmt.Sprintf("anvil: %s has no phase %q", name, phase))
	}
}
