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
// Run loads the configuration and calls wire, which builds the service and adds what has to stop
// to shutdown groups, then returns:
//
//	func wire(_ context.Context, shell anvil.Shell[Config, *zap.Logger]) error {
//		server := &http.Server{Addr: ":8080"}
//		shell.AddShutdownGroup(server).Go(func(context.Context) error {
//			return server.ListenAndServe()
//		})
//		return nil
//	}
//
// Run then waits for SIGINT, SIGTERM, Shell.Stop or the cancellation of its ctx. Groups stop one
// after another, the last added first, and everything in a group stops together. The whole
// shutdown has one deadline, 28s by default (WithShutdownGracePeriod), and Run returns the exit
// code: 0 after a clean stop, 130 after Ctrl+C, and 1 after a failure it has logged.
package anvil
