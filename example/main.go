package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/llingr/anvil"
	"github.com/llingr/anvil-koanf/conf"
	"github.com/llingr/anvil-zap/zaplog"
	"go.uber.org/zap"
)

func main() {
	loggerProvider := zaplog.New(zaplog.DefaultConfig())
	configProvider := conf.NewProvider[Config](configFiles)

	exitCode := anvil.Run(context.Background(), "orders", configProvider, loggerProvider, wire)
	os.Exit(exitCode)
}

// wire builds the service's components and adds their shutdown handlers
func wire(_ context.Context, shell anvil.Shell[Config, *zap.Logger]) error {
	cfg := shell.Config()
	if cfg.Server.Port < 1024 {
		return fmt.Errorf("invalid port")
	}

	// normal startup like any application
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Server.Port),
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
	}

	// the server's Shutdown stops it, and Go runs it until then
	shell.AddShutdownGroup(server).Go(func(context.Context) error {
		return server.ListenAndServe()
	})

	return nil
}
