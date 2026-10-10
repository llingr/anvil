# anvil

A shell for running services requiring config, logging and lifecycle
management, with a harness for controlling graceful shutdown
ordering and deadlines.

It is Kubernetes-ready, but can be used in any service or non-trivial
script to simplify startup and shutdown.

Logging and configuration are promoted to first-class concerns, while the
Provider pattern allows any logger or config implementation to be used.

## Getting Started

```sh
go get github.com/llingr/anvil
```

```go
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
```

The whole service, with its `Config` and `config.yaml`, is in [example/](example/):
`cd example && go run .`

## Shutdown Groups

`shell.AddShutdownGroup` adds a group of handlers that stop together. Groups stop one after another,
the last added first, as deferred calls run. Add each group as its resource opens, and the service
stops in the reverse of the order it started: here the server stops before the database it uses.

```go
func wire(ctx context.Context, shell Shell) error {
    cfg := shell.Config()

    db, err := sql.Open("pgx", cfg.DatabaseURL)
    if err != nil {
        return err
    }
    shell.AddShutdownGroup(shutdown.Named("postgres", shutdown.IgnoreContext(db.Close)))
    err = db.PingContext(ctx)
    if err != nil {
        return err
    }

    server := &http.Server{
        Addr: cfg.Addr,
    }
    shell.AddShutdownGroup(server).Go(func(context.Context) error {
        return server.ListenAndServe()
    })
    return nil
}
```

When `wire` returns an error partway, as it does here when the database is down at start, the shutdown
still stops whatever was added. To stop a component earlier than where it opens, add its group later:
a broadcaster that ends long-lived streams, for example, is added after the server, since the
server's `Shutdown` waits for open connections to close.

- A handler is anything with a `Shutdown(ctx context.Context) error` method, as `http.Server` has.
  It is named in the logs by its type. `shutdown.IgnoreContext` and `shutdown.Close` adapt other stop
  functions, and are wrapped in `shutdown.Named`, since their type cannot name them.
- `Go` runs a function until its group stops, then cancels its context and waits for it to return.
  Whatever it returns then is a clean stop. An error or panic before then stops the service.
- Groups are numbered in the order added, so the shutdown log counts down: `shutdown group 2 with
  *http.Server, go`, then `shutdown group 1 with postgres`. `SetName` adds a name, as `group 2 (http)`.
- Groups, handlers and goroutines are added while `wire` runs, and refused after it returns.
- Every handler's context carries one deadline, 28s from the stop by default. At the deadline anvil
  calls no more handlers, logs which were still running, and `Run` returns 1.
- After SIGTERM the service keeps serving for 5s before the first group stops, while Kubernetes takes
  the pod out of its endpoints. `shell.Stopping()` is true from the stop, for a readiness probe.

## Options

- `anvil.WithShutdownGracePeriod(d)`: the whole shutdown, from the stop, drain included; 28s, more than
  the drain delay or `Run` panics
- `anvil.WithDrainDelay(d)`: by default anvil pauses for 5s after SIGTERM before shutting down. With a
  Kubernetes `preStop` hook that already pauses, set it to 0 and lower the grace period by the hook's
  length.
- `anvil.WithStopSignals(signals...)`: more signals that stop the service as Ctrl+C does
