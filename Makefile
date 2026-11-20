FUZZTIME ?= 30s
FUZZPKGS ?= ./...

# The library and the example service, each built and linted alone; only the library has tests
MODULES := . example

# Built with a Go at least as new as go.mod's go directive, which it refuses otherwise
GOLANGCI_LINT_VERSION := v2.15.0

default: build test

all: build test lint fuzz mutation

# -o /dev/null, as go build writes a binary when ./... matches a single main package
build:
	@for module in $(MODULES); do \
		(cd $$module && go build -o /dev/null ./... && go vet ./...) || exit 1; done

test:
	go test -race -coverpkg=./... -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

# same image and version as the CI lint job, nothing installed on the host
lint:
	@for module in $(MODULES); do \
		docker run --rm -v "$(CURDIR)":/src -w /src/$$module \
			golangci/golangci-lint:$(GOLANGCI_LINT_VERSION) golangci-lint run ./... || exit 1; done

# each target runs on its own; FUZZTIME=5m FUZZPKGS=./shutdown/... make fuzz to narrow or lengthen the hunt
fuzz:
	@for package in $$(go list $(FUZZPKGS)); do \
		for target in $$(go test -list='Fuzz.*' $$package | grep '^Fuzz' || true); do \
			echo "=== $$target $$package"; \
			go test -run=NONE -fuzz=$$target -fuzztime=$(FUZZTIME) $$package || exit 1; \
		done; \
	done

# timeout-coefficient carries the phase-ordering tests' sleeps; coverpkg reaches the external test packages
mutation:
	gremlins unleash --integration --coverpkg=./... --timeout-coefficient 20 --workers 2 \
		--invert-assignments --invert-bitwise --invert-bwassign --invert-logical \
		--invert-loopctrl --remove-self-assignments --invert-negatives \
		--threshold-efficacy 95 --threshold-mcover 90
