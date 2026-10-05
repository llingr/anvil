FUZZTIME ?= 30s
FUZZPKGS ?= ./...

default: build test

all: build test lint fuzz mutation

build:
	go build ./...
	go vet ./...

test:
	go test -race -coverpkg=./... -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

lint:
	golangci-lint run ./...

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
