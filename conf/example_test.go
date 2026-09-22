// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package conf_test

import (
	"embed"
	"fmt"
	"os"

	"github.com/knadh/koanf/v2"
	"github.com/llingr/anvil/conf"
)

//go:embed example.yaml
var configFS embed.FS

// shows YAML example backbone adjusted with environment overrides
func ExampleLoad() {
	_ = os.Setenv("EXAMPLE_DATABASE_HOST", "a3c89fd.n3.acloud.db.io")
	_ = os.Setenv("EXAMPLE_DATABASE_ROLES_APPLICATION_USERNAME", "en12-prod-wm-app")
	_ = os.Setenv("EXAMPLE_DATABASE_ROLES_APPLICATION_PASSWORD", "uI8^sDnB")

	cfg, err := conf.Load(configFS, func(kfg *koanf.Koanf) (Config, error) {
		var config Config
		for path, c := range map[string]any{
			"example.server":   &config.Server,
			"example.database": &config.DB,
		} {
			if err := kfg.Unmarshal(path, c); err != nil {
				const unmarshalError = "error unmarshalling %s into %T - %w"
				return config, fmt.Errorf(unmarshalError, path, c, err)
			}
		}
		return config, nil
	})
	if err != nil {
		panic(err)
	}

	const out = "port: %d, db: %s, username: %s, password: %s"
	fmt.Printf(out, cfg.Server.Port, cfg.DB.Host, cfg.DB.Roles.App.Username, cfg.DB.Roles.App.Password)
	// Output: port: 8080, db: a3c89fd.n3.acloud.db.io, username: en12-prod-wm-app, password: uI8^sDnB
}

// Config for static (normally startup) settings
type Config struct {
	Server Server
	DB     Database
}

type Server struct {
	Port        int    `koanf:"port"`
	Environment string `koanf:"environment"`
}

type Database struct {
	Host  string `koanf:"host"`
	Port  int    `koanf:"port"`
	Name  string `koanf:"name"`
	Roles Roles  `koanf:"roles"`
}

type Roles struct {
	App Role `koanf:"application"`
}

type Role struct {
	Username string `koanf:"username"`
	Password string `koanf:"password"`
}
