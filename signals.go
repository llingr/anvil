// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil

import (
	"errors"
	"os"
	"os/signal"
	"syscall"
)

const kubernetesTermSignal = syscall.SIGTERM

// signalStop is the stop reason a trapped signal gives; its unexported type tells a signal's stop
// from any reason passed to Stop
type signalStop struct {
	signal os.Signal
}

func (s signalStop) Error() string {
	return s.signal.String() + " signal received"
}

// stoppedBySignal is the signal that started the shutdown cause describes, or nil
func stoppedBySignal(cause error) os.Signal {
	var stop signalStop
	if !errors.As(cause, &stop) {
		return nil
	}
	return stop.signal
}

// watchSignals stops the application on the first trapped signal, then stops trapping, so a second
// signal ends the process by Go's default handling
func (s *shell[C, L]) watchSignals(signals chan os.Signal, done <-chan struct{}) {
	select {
	case received := <-signals:
		signal.Stop(signals)
		s.requestStop(signalStop{
			signal: received,
		})
	case <-done:
	}
}
