// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package lifecycle_test

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/llingr/anvil/lifecycle"
	"github.com/llingr/anvil/lifecycle/shutdown"
)

func ExampleNew() {
	err := lifecycle.New(lifecycle.WithShutdownPhaseBudget(shutdown.First, 20*time.Second)).
		Run(func(ctx context.Context, app lifecycle.Application) error {
			server := &http.Server{
				Addr: ":8080",
			}
			go func() {
				_ = server.ListenAndServe()
			}()
			app.RegisterShutdownHandler("http server", server.Shutdown, shutdown.Default)
			app.RegisterShutdownHandler("flush outbox", func(ctx context.Context) error {
				return nil
			}, shutdown.Last)
			return nil
		})
	if err != nil {
		log.Print("shutdown: ", err)
		os.Exit(1)
	}
}
