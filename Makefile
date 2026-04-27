BINARY := gitcrawl
VERSION ?= dev

.PHONY: build test run clean

build:
	go build -ldflags "-X github.com/openclaw/gitcrawl/internal/cli.version=$(VERSION)" -o bin/$(BINARY) ./cmd/gitcrawl

test:
	go test ./...

run:
	go run ./cmd/gitcrawl $(ARGS)

clean:
	rm -rf bin
