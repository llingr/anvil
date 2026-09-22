// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package conf

import (
	"fmt"
	"path"
)

// Option is a Load functional option
type Option func(*Options)

// Options set by Option functions for Load
type Options struct {
	configFileGlobs []string
}

func processOptions(opts ...Option) Options {
	options := Options{
		configFileGlobs: []string{},
	}
	for _, option := range opts {
		option(&options)
	}

	// include yaml files by default
	if len(options.configFileGlobs) == 0 {
		const defaultConfigFileGlob = "*.yaml"
		options.configFileGlobs = append(options.configFileGlobs, defaultConfigFileGlob)
	}
	return options
}

// WithConfigFileGlob adds an fs.Glob pattern selecting config files
func WithConfigFileGlob(pattern string) Option {
	if _, err := path.Match(pattern, ""); err != nil {
		panic(fmt.Errorf("conf: file glob %q - %w", pattern, err))
	}
	return func(options *Options) {
		options.configFileGlobs = append(options.configFileGlobs, pattern)
	}
}
