// SPDX-FileCopyrightText: Copyright (c) 2026 The anvil Authors
// SPDX-License-Identifier: Apache-2.0

package conf

import (
	"fmt"
	"io/fs"
	"slices"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env/v2"
	kfs "github.com/knadh/koanf/providers/fs"
	"github.com/knadh/koanf/v2"
)

const (
	delim         = "."
	wordSeparator = "_" // between the words of an environment variable's name
)

// UnmarshalFromKoanf callback for mapping to host app's config type
type UnmarshalFromKoanf[T any] func(kfg *koanf.Koanf) (T, error)

// Load static config files, overlaying environment variables onto
// the default 'backbone' (e.g. app.database.password from APP_DATABASE_PASSWORD)
//
// Parameters:
//   - fileSystem: recommend go:embed
//   - unmarshal: koanf to custom config type T
//   - opts: to override file glob(s), default *.yaml if none provided
func Load[T any](fileSystem fs.FS, unmarshal UnmarshalFromKoanf[T], opts ...Option) (T, error) {
	options := processOptions(opts...)
	var config T
	configFilenames, err := globFiles(fileSystem, options.configFileGlobs)
	if err != nil {
		return config, err
	}

	kfg := koanf.New(delim)
	for _, filename := range configFilenames {
		err = kfg.Load(kfs.Provider(fileSystem, filename), yaml.Parser())
		if err != nil {
			return config, fmt.Errorf("load config file %s - %w", filename, err)
		}
	}

	// overlay environment variables
	fileKeys := kfg.Raw()
	var ambiguous []string
	envLoader := env.Provider(delim, env.Opt{
		TransformFunc: func(key, value string) (string, any) {
			paths := resolveEnvName(fileKeys, strings.Split(strings.ToLower(key), wordSeparator))
			if len(paths) != 1 {
				if len(paths) > 1 {
					ambiguous = append(ambiguous, describeAmbiguity(key, paths))
				}
				return "", nil // a variable naming no key, or more than one, is not config
			}
			return strings.Join(paths[0], delim), value
		},
	})

	// the env provider reads os.Environ and overlayEnv cannot fail, so neither can this Load
	_ = kfg.Load(envLoader, nil, koanf.WithMergeFunc(overlayEnv))
	if len(ambiguous) > 0 {
		slices.Sort(ambiguous)
		return config, fmt.Errorf("config environment variable %s", strings.Join(ambiguous, "; "))
	}

	unmarshalled, err := unmarshal(kfg)
	if err != nil {
		return config, err // a half-mapped config is never returned
	}
	return unmarshalled, nil
}

// globFiles filtering for name pattern(s)
func globFiles(files fs.FS, globs []string) ([]string, error) {
	var configFilenames []string
	for _, glob := range globs {
		matches, err := fs.Glob(files, glob)
		if err != nil {
			return nil, fmt.Errorf("config file glob %q - %w", glob, err)
		}
		configFilenames = append(configFilenames, matches...)
	}
	if len(configFilenames) == 0 {
		return nil, fmt.Errorf("no config files match %s", strings.Join(globs, " "))
	}
	return configFilenames, nil
}

// resolveEnvName returns the file-key paths whose own words spell out the variable's name, so
// app.request_ceiling answers to APP_REQUEST_CEILING. A name spelling out two paths returns both.
func resolveEnvName(fileKeys map[string]any, words []string) [][]string {
	var paths [][]string
	for key, value := range fileKeys {
		keyWords := strings.Split(strings.ToLower(key), wordSeparator)
		if len(keyWords) > len(words) || !slices.Equal(keyWords, words[:len(keyWords)]) {
			continue
		}
		unspent := words[len(keyWords):]
		if len(unspent) == 0 {
			paths = append(paths, []string{key})
			continue
		}
		branch, isBranch := value.(map[string]any)
		if !isBranch {
			continue // words are left over and this key has no children to spend them on
		}
		for _, tail := range resolveEnvName(branch, unspent) {
			paths = append(paths, append([]string{key}, tail...))
		}
	}
	return paths
}

// describeAmbiguity names the paths in a stable order, the map's own being random
func describeAmbiguity(name string, paths [][]string) string {
	joined := make([]string, len(paths))
	for index, path := range paths {
		joined[index] = strings.Join(path, delim)
	}
	slices.Sort(joined)
	return fmt.Sprintf("%s names %s", name, strings.Join(joined, " and "))
}

// overlayEnv sets only keys the files already define, resolveEnvName having named them
func overlayEnv(src, dest map[string]any) error {
	overlay(src, dest)
	return nil
}

// overlay sets the keys dest already holds and creates none, so the files stay the complete list
// of settings. src arrives spelled as the files spell it, resolveEnvName having matched the names.
func overlay(src, dest map[string]any) {
	for key, srcValue := range src {
		destValue, known := dest[key]
		if !known {
			continue
		}
		srcChild, srcBranch := srcValue.(map[string]any)
		destChild, destBranch := destValue.(map[string]any)
		switch {
		case srcBranch != destBranch:
			continue // a variable shaped unlike the file's key leaves that key alone
		case srcBranch:
			overlay(srcChild, destChild)
		default:
			dest[key] = srcValue
		}
	}
}
