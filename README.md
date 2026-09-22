# anvil

Building blocks for a Go application.

```sh
go get github.com/llingr/anvil
```

```go
import (
    "github.com/llingr/anvil/conf"
    "github.com/llingr/anvil/lifecycle"
    "github.com/llingr/anvil/lifecycle/shutdown"
)
```

## Config

Static config uses `github.com/knadh/koanf/v2` in `conf.Load`, adding an environment variable overlay for
environment-specific settings onto a known/stable YAML backbone. The `UnmarshalFromKoanf[T any]` callback
allows host applications to control the mapping and verification process.

```yaml
app:
  server:
    port: 8080
    environment: local
```

```go
//go:embed config.yaml
var configFS embed.FS

type Server struct {
    Port        int    `koanf:"port"`
    Environment string `koanf:"environment"`
}

func main() {
    // APP_SERVER_ENVIRONMENT=production overrides app.server.environment
    cfg, err := conf.Load(configFS, func(kfg *koanf.Koanf) (Server, error) {
        var server Server
        err := kfg.Unmarshal("app.server", &server)
        return server, err
    })
    if err != nil {
        panic(err)
    }
    fmt.Println(cfg.Port, cfg.Environment)
}
```

### Files

`Load` reads the `*.yaml` files at the root of a FS - customise using: `WithConfigFileGlob`.
Files load in matched order.

### Environment Variables

A variable overrides a key the files define, underscores between the words of the path, matched
regardless of case: `app.server.port` is `APP_SERVER_PORT`.

Which underscores divide a key from the next and which belong inside one is not in the name, so the file
keys settle it: `APP_SERVER_MAX_CONNS` reaches `app.server.max_conns`, and `MY_APP_SERVER_PORT` reaches
`my_app.server.port`. A variable naming no key is ignored, so the files stay the complete list of
settings. A name spelling out two keys at once fails the load rather than guess, as `APP_REQUEST_CEILING`
does where the files define both `app.request_ceiling` and `app.request.ceiling`. Values stay strings
until the callback reads or unmarshals them.

## Lifecycle

`Run` runs the wiring, waits for SIGINT, SIGTERM or `app.Stop`, then shuts registered handlers down in
phases, each within its own budget (28s in total by default, under the Kubernetes 30s grace period). It
returns once the last phase is done, with the reason for stopping and any handler errors joined together.

```go
func main() {
    err := lifecycle.New().Run(func(ctx context.Context, app lifecycle.Application) error {
        server := &http.Server{Addr: ":8080"}
        go func() { _ = server.ListenAndServe() }()
        app.RegisterShutdownHandler("http server", server.Shutdown, shutdown.First)
        return nil
    })
    if err != nil {
        log.Print("shutdown: ", err)
        os.Exit(1)
    }
}
```

| phase | runs | default budget |
|---|---|---|
| `shutdown.First` | concurrently: intake such as consumers and HTTP servers | 50%, 14s |
| `shutdown.Default` | in registration order: application services | 25%, 7s |
| `shutdown.Last` | concurrently: egress such as publishers and pools | 25%, 7s |

Each phase ends at the running total of budgets, so time an earlier phase leaves unused carries forward: if
`First` finishes in 1s, `Default` still ends at 21s and gets 20s.

A `shutdown.Handler` is a `func(ctx context.Context) error`, so a method value such as `server.Shutdown` or a
closure registers as it is, under the name its errors will carry. `shutdown.IgnoreContext(fn)` adapts a
`func() error`. Asking twice never waits for the handlers: the process exits with 128 plus the signal
number. A signal that started the shutdown is the first ask, so the next one quits; a shutdown `Stop`
started has yet to be asked for, so it absorbs one signal and quits on the one after, and a pod that
stops itself as the kubelet's SIGTERM arrives still gets its phases. `lifecycle.WithShutdownPhaseBudget`
sets one phase's budget and panics on an unknown phase, and `lifecycle.WithSignals` adds signals to
SIGINT and SIGTERM, which are always trapped.

## Licence

Apache-2.0. See LICENSE.
