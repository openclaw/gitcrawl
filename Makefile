BINARY := gitcrawl
VERSION ?= dev

.DEFAULT_GOAL := help

.PHONY: help build generate-sqlc tidy-check fmt lint test test-coverage run clean smoke test-release check snapshot release verify-release release-artifacts release-snapshot

help:
	@printf '%s\n' \
		'Available targets:' \
		'  help              Print available targets (default).' \
		'  build             Build the CLI into bin/$(BINARY).' \
		'  test              Run the full Go test suite.' \
		'  fmt               Check Go formatting.' \
		'  lint              Run vet, vulnerability, and dead-code checks.' \
		'  check             Run every local gate enforced by CI.' \
		'  snapshot          Build credential-free release artifacts.' \
		'  release           Refuse local publishing and print the official CI command.' \
		'  verify-release    Verify existing release artifacts (VERSION=vX.Y.Z).' \
		'  generate-sqlc     Regenerate sqlc output.' \
		'  tidy-check        Verify go.mod and go.sum are tidy.' \
		'  test-coverage     Run tests and enforce the coverage floor.' \
		'  smoke             Build and run CLI smoke checks.' \
		'  test-release      Test the release scripts.' \
		'  run               Run the CLI (ARGS=...).' \
		'  clean             Remove local build output.' \
		'  release-artifacts Alias for release.' \
		'  release-snapshot  Alias for snapshot.'

build:
	mkdir -p bin
	go build -ldflags "-X github.com/openclaw/gitcrawl/internal/cli.version=$(VERSION)" -o bin/$(BINARY) ./cmd/gitcrawl

generate-sqlc:
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1 generate

tidy-check:
	go mod tidy
	git diff --exit-code -- go.mod go.sum

fmt:
	@set -e; \
	unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then echo "$$unformatted"; exit 1; fi

lint:
	go vet ./...
	GOWORK=off go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...
	@set -e; \
	output_file="$$(mktemp)"; \
	trap 'rm -f "$$output_file"' 0; \
	go run golang.org/x/tools/cmd/deadcode@v0.48.0 -test ./... > "$$output_file"; \
	if [ -s "$$output_file" ]; then cat "$$output_file"; exit 1; fi

test:
	GOWORK=off go test ./...

test-coverage:
	GOWORK=off go test ./... -covermode=atomic -coverprofile=coverage.out
	@total="$$(go tool cover -func=coverage.out | awk '/^total:/ { sub(/%/, "", $$3); print $$3 }')"; \
	if [ -z "$$total" ]; then echo "could not parse total coverage" >&2; exit 1; fi; \
	echo "total coverage: $${total}%"; \
	awk -v total="$$total" 'BEGIN { if (total + 0 < 85.0) { printf("coverage %.1f%% is below 85.0%%\n", total); exit 1 } }'

run:
	go run ./cmd/gitcrawl $(ARGS)

clean:
	rm -rf bin

smoke: build
	@set -e; version="$$(./bin/$(BINARY) --version)"; test -n "$$version"
	@set -e; output="$$(./bin/$(BINARY) metadata --json)"; printf '%s' "$$output" | grep -q '"schema_version"'
	@set -e; output="$$(./bin/$(BINARY) help tui)"; \
	printf '%s\n' "$$output"; \
	printf '%s' "$$output" | grep -q "gitcrawl tui"

test-release:
	./scripts/test-release.sh

check: tidy-check fmt lint test-coverage smoke test-release snapshot

snapshot:
	GOWORK=off goreleaser release --snapshot --clean --skip=publish

release:
	@./scripts/package-release.sh

verify-release:
	@test -n "$(VERSION)" && [ "$(VERSION)" != dev ] || (echo "usage: make verify-release VERSION=vX.Y.Z" >&2; exit 2)
	./scripts/verify-release.sh "$(VERSION)"

release-artifacts: release

release-snapshot: snapshot
