package main

import (
	"embed"
	"time"
)

//go:embed config.yaml
var configFiles embed.FS

type Config struct {
	Server struct {
		Port              int           `koanf:"port"`
		ReadHeaderTimeout time.Duration `koanf:"readHeaderTimeout"`
	} `koanf:"server"`
}
