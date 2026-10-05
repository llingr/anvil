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
import (
    "context"
    "embed"
    "errors"
    "net"
    "net/http"
    "os"
    "strconv"
    "time"

    "github.com/llingr/anvil"
    "github.com/llingr/anvil-koanf/conf"
    "github.com/llingr/anvil-zap/zaplog"
    "github.com/llingr/anvil/shutdown"
    "go.uber.org/zap"
)

//go:embed config.yaml
var configFiles embed.FS

type Config struct {
    Server struct {
        Port              int           `koanf:"port"`
        ReadHeaderTimeout time.Duration `koanf:"readHeaderTimeout"`
    } `koanf:"server"`
}

// Shell keeps the two type parameters out of every function that takes the shell
type Shell = anvil.Shell[Config, *zap.Logger]

func main() {
    loggerProvider := zaplog.New(zaplog.DefaultConfig())
    configProvider := conf.NewProvider[Config](configFiles)
    exitCode := anvil.Run(context.Background(), "orders", configProvider, loggerProvider, wire)
    os.Exit(exitCode)
}

// wire builds the service's components and registers their shutdown
func wire(ctx context.Context, shell Shell) error {
    config := shell.Config()
    mux := http.NewServeMux()
    mux.HandleFunc("/ready", func(writer http.ResponseWriter, _ *http.Request) {
        if shell.Stopping() {
            writer.WriteHeader(http.StatusServiceUnavailable)
        }
    })
    listener, err := net.Listen("tcp", net.JoinHostPort("", strconv.Itoa(config.Server.Port)))
    if err != nil {
        return err
    }
    server := &http.Server{
        Handler:           mux,
        ReadHeaderTimeout: config.Server.ReadHeaderTimeout,
    }
    shell.Go(shutdown.Ingress, "http server", func(context.Context) error {
        if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
            return err
        }
        return nil
    })
    shell.RegisterShutdownHandler(shutdown.Ingress, "http shutdown", server.Shutdown)
    return nil
}
```

## Features

- **One call.** `anvil.Run` loads the configuration, calls `wire` to build the service, waits for a
  stop, shuts down in phases and returns the exit code for `os.Exit`.
- **Any logger, any configuration source,** through `anvil.LoggerProvider[L]` and
  `anvil.ConfigProvider[C]`. See [anvil-zap](https://github.com/llingr/anvil-zap) and
  [anvil-koanf](https://github.com/llingr/anvil-koanf) for reference implementations.
- **Stops from a signal, `shell.Stop(err)`, or the context passed to `Run`.** A second SIGINT or SIGTERM
  exits immediately.
- **Kubernetes shutdown.** After SIGTERM the service keeps serving through a drain delay, with
  `shell.Stopping()` failing readiness, then shuts down in three phases within one deadline:
  - `shutdown.Ingress`: consumers and servers, all at once
  - `shutdown.Core`: application services, one at a time, in reverse registration order
  - `shutdown.Egress`: publishers and pools, all at once

  ```text
   0s      SIGTERM: readiness fails, still serving
           |  drain      5s
   5s      wire's context cancelled
           |  INGRESS    11.5s
  16.5s
           |  CORE       5.75s
  22.25s
           |  EGRESS     5.75s
  28s      deadline
  30s      Kubernetes sends SIGKILL
  ```

  A handler still running at its phase's deadline is reported and left behind.
- **Goroutines.** `shell.Go` runs a server's or a consumer's loop, stops the service if the loop fails
  or panics, logs an error if it returns before any stop, and has the loop's phase wait for it.
- **Lifecycle logs**, one line per step:

  ```text
  starting orders
  loading config
  configuration loaded in 2.1ms
  started orders
  stopping: terminated signal received
  draining for 5s before INGRESS
  INGRESS done in 312ms: http server 312ms, http shutdown 311ms
  exiting orders
  ```
- **Exit codes:** 0 after a clean stop, 130 after Ctrl+C (128 plus the signal for one added with
  `WithStopSignals`), 1 after a logged failure.

## Options

- `anvil.WithShutdownDeadline(d)`: the whole shutdown, drain included; 28s
- `anvil.WithShutdownPhaseBudget(phase, d)`: one phase's time; the others share what is left
- `anvil.WithDrainDelay(d)`: serving on after SIGTERM; 5s, set 0 outside Kubernetes
- `anvil.WithStopSignals(signals...)`: more signals that stop the service as Ctrl+C does
