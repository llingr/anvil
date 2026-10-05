// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil_test

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/llingr/anvil"
	"github.com/llingr/anvil/shutdown"
)

// OutboxConfig is the configuration, written in code where a service would read a file
type OutboxConfig struct {
	BatchSize int
}

// Load is the whole of a config provider
func (c OutboxConfig) Load(context.Context) (OutboxConfig, error) {
	return c, nil
}

// stdLogging is a logger provider over the standard library's log
type stdLogging struct {
	logger *log.Logger
}

func (s stdLogging) Logger() *log.Logger {
	return s.logger
}

func (s stdLogging) LifecycleInfo(_ context.Context, msg string) {
	s.logger.Print(msg)
}

func (s stdLogging) LifecycleError(_ context.Context, msg string, err error) {
	s.logger.Printf("%s: %v", msg, err)
}

// Flush has nothing to write out: log writes each line straight through
func (s stdLogging) Flush() {
}

// OutboxShell keeps the two type parameters out of every function that takes the shell
type OutboxShell = anvil.Shell[OutboxConfig, *log.Logger]

func ExampleRun() {
	loggerProvider := stdLogging{
		logger: log.New(io.Discard, "", 0), // os.Stderr in a service
	}
	configProvider := OutboxConfig{
		BatchSize: 100,
	}
	exitCode := anvil.Run(context.Background(), "outbox", configProvider, loggerProvider, wireOutbox)
	fmt.Println("exit code", exitCode) // main passes it to os.Exit
	// Output:
	// outbox flushed in batches of 100
	// exit code 0
}

// wireOutbox registers the outbox's flush, then stops at once where a service would wait for SIGTERM
func wireOutbox(_ context.Context, shell OutboxShell) error {
	batchSize := shell.Config().BatchSize
	shell.RegisterShutdownHandler(shutdown.Egress, "outbox", func(context.Context) error {
		fmt.Println("outbox flushed in batches of", batchSize)
		return nil
	})
	shell.Stop(nil)
	return nil
}
