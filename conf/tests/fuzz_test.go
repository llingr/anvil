// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"fmt"
	"maps"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/knadh/koanf/v2"
	"github.com/llingr/anvil/conf"
)

type serverConfig struct {
	Port  int      `koanf:"port"`
	Hosts []string `koanf:"hosts"`
	Debug bool     `koanf:"debug"`
}

type emailConfig struct {
	From string `koanf:"from"`
}

type appConfig struct {
	Server serverConfig
	Email  emailConfig
}

const completeYaml = `
app:
  server:
    port: 8080
    hosts: [a.example]
    debug: false
  email:
    from: noreply@example.com
`

// underscoreYaml gives a variable's name more ways to divide than there are keys to divide it into
const underscoreYaml = `
my_app:
  server:
    max_conns: 10
    port: 8080
  email_from: noreply@example.com
`

func files(document string) fstest.MapFS {
	return fstest.MapFS{
		"config.yaml": &fstest.MapFile{Data: []byte(document)},
	}
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

// FuzzLoadRejectsRatherThanPanics feeds arbitrary bytes in as the config file: a load
// either returns a config or an error, never a half-mapped config
func FuzzLoadRejectsRatherThanPanics(f *testing.F) {
	f.Add(completeYaml)
	f.Add("app: [unclosed\n")
	f.Add("app:\n  server:\n    port: eighty\n")
	f.Add("app:\n  x: 1\n  x: 2\n")
	f.Add("- one\n- two\n")
	f.Add("app:\n  server.port: 1\n")
	f.Add("app:\n  server: \n    hosts: 00\n  email: 0")
	f.Add("")
	f.Add("\x00\x01")

	f.Fuzz(func(t *testing.T, document string) {
		config, err := conf.Load(files(document), unmarshalAppConfig)
		if err != nil && !reflect.DeepEqual(config, appConfig{}) {
			t.Fatalf("error %v returned config %+v, want the zero value", err, config)
		}
	})
}

// FuzzLoadKeepsKoanfUsable checks the raw tree survives any parsable document
func FuzzLoadKeepsKoanfUsable(f *testing.F) {
	f.Add(completeYaml)
	f.Add("app:\n  server:\n    hosts: [a, b]\n")
	f.Add("app: {}\n")

	f.Fuzz(func(t *testing.T, document string) {
		raw, err := conf.Load(files(document), func(kfg *koanf.Koanf) (map[string]any, error) {
			return kfg.Raw(), nil
		})
		if err == nil && raw == nil {
			t.Fatal("clean load returned a nil tree")
		}
	})
}

// FuzzEnvOverlayChangesOnlyItsOwnKey feeds arbitrary variable names in: only APP_SERVER_PORT
// moves the port, whatever shape the others take
func FuzzEnvOverlayChangesOnlyItsOwnKey(f *testing.F) {
	f.Add("APP_SERVER_PORT", "9090")
	f.Add("app_server_port", "9090")
	f.Add("APP_SERVER_PROT", "9090")
	f.Add("APP_SERVER", "9090")
	f.Add("APP_SERVER_PORT_EXTRA", "9090")
	f.Add("APPLE_SERVER_PORT", "9090")
	f.Add("APP", "9090")
	f.Add("", "9090")
	f.Add("APP_SERVER_PORT", "eighty")

	f.Fuzz(func(t *testing.T, name, value string) {
		if !validEnvName(name) {
			t.Skip("not a name os.Setenv accepts")
		}
		defer func() {
			_ = os.Unsetenv(name)
		}()
		if err := os.Setenv(name, value); err != nil {
			t.Skip("environment rejected the name")
		}

		config, err := conf.Load(files(completeYaml), unmarshalAppConfig)
		ownKey := strings.EqualFold(name, "APP_SERVER_PORT")
		switch {
		case err != nil && !ownKey:
			t.Fatalf("%s=%q failed the load: %v", name, value, err)
		case err != nil:
			return // only its own key can carry a value the port cannot hold
		case !ownKey && config.Server.Port != 8080:
			t.Fatalf("%s=%q changed the port to %d", name, value, config.Server.Port)
		case !ownKey && config.Email.From != "noreply@example.com":
			t.Fatalf("%s=%q changed the email to %q", name, value, config.Email.From)
		}
	})
}

// FuzzEnvOverlayReachesOnlyUnderscoreKeysItNames feeds arbitrary names at a tree whose keys have
// underscores of their own: only the name spelling a key out exactly moves it, and none invents one
func FuzzEnvOverlayReachesOnlyUnderscoreKeysItNames(f *testing.F) {
	f.Add("MY_APP_SERVER_MAX_CONNS", "99")
	f.Add("MY_APP_SERVER_MAXCONNS", "99")
	f.Add("MY_APP_SERVER_MAX", "99")
	f.Add("MYAPP_SERVER_MAX_CONNS", "99")
	f.Add("MY_APP_EMAIL_FROM", "a@example.com")
	f.Add("MY_APP_EMAIL", "a@example.com")
	f.Add("MY_APP", "99")
	f.Add("_MY_APP_SERVER_PORT", "99")

	const maxConns = "MY_APP_SERVER_MAX_CONNS"
	f.Fuzz(func(t *testing.T, name, value string) {
		if !validEnvName(name) {
			t.Skip("not a name os.Setenv accepts")
		}
		defer func() {
			_ = os.Unsetenv(name)
		}()
		if err := os.Setenv(name, value); err != nil {
			t.Skip("environment rejected the name")
		}

		tree, err := conf.Load(files(underscoreYaml), func(kfg *koanf.Koanf) (map[string]any, error) {
			return kfg.Raw(), nil
		})
		if err != nil {
			t.Fatalf("%s=%q failed the load: %v", name, value, err)
		}
		app, isBranch := tree["my_app"].(map[string]any)
		if !isBranch {
			t.Fatalf("%s=%q left my_app as %v", name, value, tree["my_app"])
		}
		server, isBranch := app["server"].(map[string]any)
		if !isBranch {
			t.Fatalf("%s=%q left my_app.server as %v", name, value, app["server"])
		}
		if keys := slices.Sorted(maps.Keys(app)); !slices.Equal(keys, []string{"email_from", "server"}) {
			t.Fatalf("%s=%q left my_app holding %q", name, value, keys)
		}
		if keys := slices.Sorted(maps.Keys(server)); !slices.Equal(keys, []string{"max_conns", "port"}) {
			t.Fatalf("%s=%q left my_app.server holding %q", name, value, keys)
		}
		if names := strings.EqualFold(name, maxConns); !names && server["max_conns"] != 10 {
			t.Fatalf("%s=%q changed max_conns to %v", name, value, server["max_conns"])
		} else if names && server["max_conns"] != value {
			t.Fatalf("%s=%q left max_conns at %v", name, value, server["max_conns"])
		}
	})
}

// validEnvName keeps the fuzzer to names an OS accepts, so a skip means the input, not a bug
func validEnvName(name string) bool {
	if name == "" || strings.ContainsAny(name, "=\x00") {
		return false
	}
	return true
}
