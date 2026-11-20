module github.com/llingr/anvil/example

go 1.26.0

// the anvil in this repository, rather than the latest release
replace github.com/llingr/anvil => ../

require (
	github.com/llingr/anvil v0.2.0
	github.com/llingr/anvil-koanf v0.2.0
	github.com/llingr/anvil-zap v0.2.1
	go.uber.org/zap v1.28.0
)

require (
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/knadh/koanf/maps v0.1.3 // indirect
	github.com/knadh/koanf/parsers/yaml v1.1.1 // indirect
	github.com/knadh/koanf/providers/env/v2 v2.0.2 // indirect
	github.com/knadh/koanf/providers/fs v1.0.1 // indirect
	github.com/knadh/koanf/v2 v2.4.1 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v3 v3.0.6 // indirect
)
