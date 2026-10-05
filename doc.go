// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

// Package anvil runs a Go service's lifecycle from main through to a graceful shutdown, with the
// logger and the static configuration supplied by providers.
//
// main makes one call and passes its result to os.Exit:
//
//	exitCode := anvil.Run(context.Background(), "orders", configProvider, loggerProvider, wire)
//	os.Exit(exitCode)
//
// Run loads the configuration, traps SIGINT and SIGTERM, and calls wire, which builds the
// service's components, registers each one's shutdown handler in a phase and returns; it must not
// block, and a handler registered after it returns is refused. Run then waits for a signal,
// Shell.Stop or the cancellation of its ctx. After SIGTERM it keeps serving through the drain
// delay (WithDrainDelay) while Kubernetes takes the pod out of its endpoints, and
// Shell.Stopping turns true for a readiness probe. It then runs the phases in order within the
// shutdown deadline (WithShutdownDeadline): shutdown.Ingress (consumers and servers, concurrently),
// shutdown.Core (one at a time, in reverse registration order) and shutdown.Egress (publishers
// and pools, concurrently).
//
// Run returns 0 after a clean stop, 130 after a clean stop Ctrl+C asked for, and 1 after a failure it
// has logged. A second SIGINT or SIGTERM ends the process at once.
//
// An alias keeps the two type parameters, the configuration type and the logger, out of every
// function that takes the shell:
//
//	type Shell = anvil.Shell[Config, *zap.Logger]
package anvil
