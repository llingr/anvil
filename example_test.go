// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package anvil_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

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

// wireOutbox adds the outbox's flush, then stops at once where a service would wait for SIGTERM
func wireOutbox(_ context.Context, shell OutboxShell) error {
	batchSize := shell.Config().BatchSize
	shell.AddShutdownGroup(shutdown.Named("outbox", shutdown.HandlerFunc(func(context.Context) error {
		fmt.Println("outbox flushed in batches of", batchSize)
		return nil
	})))
	shell.Stop(nil)
	return nil
}

func ExampleStopContext() {
	loggerProvider := stdLogging{
		logger: log.New(io.Discard, "", 0), // os.Stderr in a service
	}
	exitCode := anvil.Run(context.Background(), "server", OutboxConfig{}, loggerProvider, wireServer)
	fmt.Println("exit code", exitCode)
	// Output:
	// stop context ended: false
	// stop context has the shutdown deadline: true
	// exit code 0
}

// wireServer starts the server's loop, then stops at once where a service would wait for SIGTERM
func wireServer(_ context.Context, shell OutboxShell) error {
	shell.AddShutdownGroup().SetName("SERVING").Go(serve)
	shell.Stop(nil)
	return nil
}

func ExampleWiring() {
	loggerProvider := stdLogging{
		logger: log.New(io.Discard, "", 0), // os.Stderr in a service
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // stands in for a SIGTERM during a slow start
	exitCode := anvil.Run(ctx, "slow", OutboxConfig{}, loggerProvider, wireSlowly)
	fmt.Println("exit code", exitCode)
	// Output:
	// wiring stopped: loading prices: context canceled
	// exit code 0
}

// wireSlowly stands in for a start that takes a minute, and returns as soon as a stop cancels its ctx
func wireSlowly(ctx context.Context, _ OutboxShell) error {
	select {
	case <-time.After(time.Minute):
		return nil
	case <-ctx.Done():
		err := fmt.Errorf("loading prices: %w", ctx.Err())
		fmt.Println("wiring stopped:", err)
		return err // a clean stop, since nobody wants the start finished
	}
}

// serve stands in for a server's loop: it serves until its ctx is cancelled, then stops on the stop ctx
func serve(ctx context.Context) error {
	<-ctx.Done() // told to stop
	stopCtx, cancel := anvil.StopContext(ctx)
	defer cancel()
	_, hasDeadline := stopCtx.Deadline()
	fmt.Println("stop context ended:", stopCtx.Err() != nil)
	fmt.Println("stop context has the shutdown deadline:", hasDeadline)
	return ctx.Err() // a clean stop
}
