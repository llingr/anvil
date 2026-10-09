module github.com/llingr/anvil/example

go 1.25

// the anvil in this repository, rather than the latest release
replace github.com/llingr/anvil => ../

require (
	github.com/llingr/anvil v0.0.7
	github.com/llingr/anvil-koanf v0.0.1
	github.com/llingr/anvil-zap v0.1.0
	go.uber.org/zap v1.28.0
)

require (
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/knadh/koanf/maps v0.1.2 // indirect
	github.com/knadh/koanf/parsers/yaml v1.1.1 // indirect
	github.com/knadh/koanf/providers/env/v2 v2.0.1 // indirect
	github.com/knadh/koanf/providers/fs v1.0.1 // indirect
	github.com/knadh/koanf/v2 v2.3.6 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
)
