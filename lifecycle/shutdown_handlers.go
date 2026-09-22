// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"fmt"
	"slices"

	"github.com/llingr/anvil/lifecycle/shutdown"
)

// registeredHandler invoked during shutdown
type registeredHandler struct {
	name            string
	shutdownHandler shutdown.Handler
}

// RegisterShutdownHandler for controlled shutdown, adding a handler to a shutdown phase
func (a *application) RegisterShutdownHandler(name string, shutdownHandler shutdown.Handler, phase shutdown.Phase) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.validateArgs(name, shutdownHandler, phase)
	a.shutdownHandlers[phase] = append(a.shutdownHandlers[phase], registeredHandler{name, shutdownHandler})
}

func (a *application) validateArgs(name string, handler shutdown.Handler, phase shutdown.Phase) {
	switch {
	case name == "":
		panic("lifecycle: shutdown handler has no name")
	case handler == nil:
		panic(fmt.Sprintf("lifecycle: %s has a nil handler", name))
	case !slices.Contains(shutdown.Phases(), phase):
		panic(fmt.Sprintf("lifecycle: %s has no phase %q", name, phase))
	case a.wired.Load():
		panic("lifecycle: RegisterShutdownHandler after wiring returned")
	}
}
