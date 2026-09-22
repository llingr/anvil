// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package conf_test

import (
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/knadh/koanf/v2"
	"github.com/llingr/anvil/conf"
)

type serverConfig struct {
	Port    int           `koanf:"port"`
	Ceiling time.Duration `koanf:"requestCeiling"` // differs from its key, so the tag is load-bearing
	Hosts   []string      `koanf:"hosts"`
	Debug   bool          `koanf:"debug"`
}

type emailConfig struct {
	From string `koanf:"from"`
}

type appConfig struct {
	Server serverConfig
	Email  emailConfig
}

const serverYaml = `
app:
  server:
    port: 8080
    requestCeiling: 13s
    hosts: [a.example]
    debug: false
`

const emailYaml = `
app:
  email:
    from: noreply@example.com
`

const completeYaml = serverYaml + `
  email:
    from: noreply@example.com
`

func files(entries ...string) fstest.MapFS {
	mapFS := fstest.MapFS{}
	for index := 0; index < len(entries); index += 2 {
		mapFS[entries[index]] = &fstest.MapFile{
			Data: []byte(entries[index+1]),
		}
	}
	return mapFS
}

func unmarshalAppConfig(kfg *koanf.Koanf) (appConfig, error) {
	var config appConfig
	configPaths := map[string]any{
		"app.server": &config.Server,
		"app.email":  &config.Email,
	}
	for path, c := range configPaths {
		err := kfg.Unmarshal(path, c)
		if err != nil {
			return config, fmt.Errorf("error unmarshalling %s into %T - %w", path, c, err)
		}
	}
	return config, nil
}

func raw(kfg *koanf.Koanf) (map[string]any, error) {
	return kfg.Raw(), nil
}

func requireErrorContaining(t *testing.T, err error, fragments ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an error containing %q, got nil", fragments)
	}
	for _, fragment := range fragments {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("error %q does not contain %q", err, fragment)
		}
	}
}

func TestLoadsFileValues(t *testing.T) {
	config, err := conf.Load(files("config.yaml", completeYaml), unmarshalAppConfig)
	if err != nil {
		t.Fatal(err)
	}
	server := config.Server
	if server.Port != 8080 || server.Ceiling != 13*time.Second || server.Debug {
		t.Fatalf("file values not loaded: %+v", server)
	}
	if !slices.Equal(server.Hosts, []string{"a.example"}) {
		t.Fatalf("hosts %q", server.Hosts)
	}
	if config.Email.From != "noreply@example.com" {
		t.Fatalf("email %+v", config.Email)
	}
}

func TestEnvOverlaysFileKeysCaseInsensitively(t *testing.T) {
	t.Setenv("APP_SERVER_PORT", "9090")
	t.Setenv("APP_SERVER_REQUESTCEILING", "2m")
	t.Setenv("APP_SERVER_DEBUG", "true")
	config, err := conf.Load(files("config.yaml", completeYaml), unmarshalAppConfig)
	if err != nil {
		t.Fatal(err)
	}
	server := config.Server
	if server.Port != 9090 || server.Ceiling != 2*time.Minute || !server.Debug {
		t.Fatalf("overlay not applied: %+v", server)
	}
	if config.Email.From != "noreply@example.com" {
		t.Fatalf("file value lost: %+v", config.Email)
	}
}

// only keys the files define take a variable, so the files stay the complete list of settings
func TestEnvWithoutFileKeyIsIgnored(t *testing.T) {
	t.Setenv("APP_SERVER_PROT", "1")
	t.Setenv("APPLE_SERVER_PORT", "1")
	config, err := conf.Load(files("config.yaml", completeYaml), raw)
	if err != nil {
		t.Fatal(err)
	}
	if keys := slices.Sorted(maps.Keys(config)); !slices.Equal(keys, []string{"app"}) {
		t.Fatalf("variable under no file key applied: top-level keys %q", keys)
	}
	server := config["app"].(map[string]any)["server"].(map[string]any)
	keys := slices.Sorted(maps.Keys(server))
	if !slices.Equal(keys, []string{"debug", "hosts", "port", "requestCeiling"}) || server["port"] != 8080 {
		t.Fatalf("variable without a file key applied: %v", server)
	}
}

const underscoreYaml = `
my_app:
  request_ceiling: 13s
  server:
    max_conns: 10
    port: 8080
`

// a key's own underscores are part of the key, not path separators, at any depth
func TestEnvOverlaysKeysContainingUnderscores(t *testing.T) {
	t.Setenv("MY_APP_REQUEST_CEILING", "2m")
	t.Setenv("MY_APP_SERVER_MAX_CONNS", "99")
	t.Setenv("MY_APP_SERVER_PORT", "9090")
	config, err := conf.Load(files("config.yaml", underscoreYaml), raw)
	if err != nil {
		t.Fatal(err)
	}
	app := config["my_app"].(map[string]any)
	if app["request_ceiling"] != "2m" {
		t.Errorf("request_ceiling %v, want the variable's 2m", app["request_ceiling"])
	}
	server := app["server"].(map[string]any)
	if server["max_conns"] != "99" {
		t.Errorf("max_conns %v, want the variable's 99", server["max_conns"])
	}
	if server["port"] != "9090" {
		t.Errorf("port %v, want the variable's 9090", server["port"])
	}
}

// a name that spells out two keys names neither: picking one would set a key nobody meant
func TestEnvNameMatchingTwoKeysFails(t *testing.T) {
	const ambiguousYaml = `
app:
  request_ceiling: 13s
  request:
    ceiling: 13s
`
	t.Setenv("APP_REQUEST_CEILING", "2m")
	_, err := conf.Load(files("config.yaml", ambiguousYaml), raw)
	requireErrorContaining(t, err, "APP_REQUEST_CEILING", "app.request.ceiling", "app.request_ceiling")
}

// variables differing only by case collapse to one key, so the same one wins on every load
func TestEnvCaseVariantsResolveTheSameEveryLoad(t *testing.T) {
	t.Setenv("APP_SERVER_PORT", "1111")
	t.Setenv("app_server_port", "2222")
	seen := map[int]int{}
	for range 50 {
		config, err := conf.Load(files("config.yaml", completeYaml), unmarshalAppConfig)
		if err != nil {
			t.Fatal(err)
		}
		seen[config.Server.Port]++
	}
	if len(seen) != 1 {
		t.Fatalf("ports across loads %v, want one", seen)
	}
}

func TestUnmarshalFromKoanfErrorNamesTheSection(t *testing.T) {
	t.Setenv("APP_SERVER_PORT", "eighty")
	_, err := conf.Load(files("config.yaml", completeYaml), unmarshalAppConfig)
	requireErrorContaining(t, err, "app.server", "*conf_test.serverConfig", "'port'")
}

func TestUnmarshalFromKoanfErrorIsReturned(t *testing.T) {
	rejected := errors.New("signing key missing")
	_, err := conf.Load(files("config.yaml", completeYaml), func(*koanf.Koanf) (appConfig, error) {
		return appConfig{}, rejected
	})
	if !errors.Is(err, rejected) {
		t.Fatalf("error %v, want the callback's", err)
	}
}

// a callback that fails part way through its sections hands back nothing
func TestUnmarshalFromKoanfErrorReturnsNoConfig(t *testing.T) {
	config, err := conf.Load(files("config.yaml", completeYaml), func(kfg *koanf.Koanf) (appConfig, error) {
		var partial appConfig
		if err := kfg.Unmarshal("app.server", &partial.Server); err != nil {
			t.Fatal(err)
		}
		return partial, errors.New("email section failed")
	})
	if err == nil {
		t.Fatal("expected the callback's error")
	}
	if config.Server.Port != 0 {
		t.Fatalf("half-mapped config returned: %+v", config)
	}
}

func TestFilesMerge(t *testing.T) {
	config, err := conf.Load(files("server.yaml", serverYaml, "email.yaml", emailYaml), unmarshalAppConfig)
	if err != nil {
		t.Fatal(err)
	}
	if config.Server.Port != 8080 || config.Email.From != "noreply@example.com" {
		t.Fatalf("files not merged: %+v", config)
	}
}

func TestOnlyRootYamlFilesAreRead(t *testing.T) {
	mapFS := files(
		"config.yaml", completeYaml,
		"config.yml", "app:\n  server:\n    port: 1\n",
		"nested/extra.yaml", "app:\n  server:\n    port: 1\n",
	)
	config, err := conf.Load(mapFS, unmarshalAppConfig)
	if err != nil {
		t.Fatal(err)
	}
	if config.Server.Port != 8080 {
		t.Fatalf("a file other than config.yaml was read: port %d", config.Server.Port)
	}
}

func TestNoMatchingFilesFails(t *testing.T) {
	_, err := conf.Load(files("config.yml", completeYaml), unmarshalAppConfig)
	requireErrorContaining(t, err, "no config files", "*.yaml")
}

func TestFileGlobReplacesTheDefault(t *testing.T) {
	mapFS := files("base.yaml", completeYaml, "other.yaml", "app:\n  server:\n    port: 1\n")
	config, err := conf.Load(mapFS, unmarshalAppConfig, conf.WithConfigFileGlob("b*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if config.Server.Port != 8080 {
		t.Fatalf("unselected file was read: port %d", config.Server.Port)
	}
}

func TestFileGlobsAccumulate(t *testing.T) {
	mapFS := files("server.yaml", serverYaml, "email.yaml", emailYaml, "other.yaml", "app:\n  server:\n    port: 1\n")
	config, err := conf.Load(mapFS, unmarshalAppConfig,
		conf.WithConfigFileGlob("server.yaml"), conf.WithConfigFileGlob("email.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if config.Server.Port != 8080 || config.Email.From != "noreply@example.com" {
		t.Fatalf("both globs not applied: %+v", config)
	}
}

func TestMalformedFileGlobPanics(t *testing.T) {
	defer func() {
		recovered := recover()
		if recovered == nil || !strings.Contains(fmt.Sprint(recovered), `"a/[b"`) {
			t.Fatalf("recovered %v, want a panic naming the glob", recovered)
		}
	}()
	conf.WithConfigFileGlob("a/[b")
}

// failingGlobFS stands in for an fs.GlobFS whose Glob fails for a reason other than the pattern
type failingGlobFS struct {
	fstest.MapFS
}

func (failingGlobFS) Glob(string) ([]string, error) {
	return nil, errors.New("glob unavailable")
}

func TestGlobFailureFails(t *testing.T) {
	_, err := conf.Load(failingGlobFS{files("config.yaml", completeYaml)}, unmarshalAppConfig)
	requireErrorContaining(t, err, "*.yaml", "glob unavailable")
}

func TestMalformedFileFails(t *testing.T) {
	_, err := conf.Load(files("broken.yaml", "app: [unclosed\n"), unmarshalAppConfig)
	requireErrorContaining(t, err, "broken.yaml")
}

func TestUnreadableFileFails(t *testing.T) {
	mapFS := files("config.yaml", completeYaml)
	mapFS["config.yaml"].Mode = fs.ModeDir
	_, err := conf.Load(mapFS, unmarshalAppConfig)
	requireErrorContaining(t, err, "config.yaml")
}
