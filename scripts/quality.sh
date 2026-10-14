#!/usr/bin/env bash
set -euo pipefail
test -z "$(gofmt -l cmd internal)"
go vet ./...
govulncheck ./...
go build -o /tmp/tenant-worker ./cmd/worker
export WORKER_BINARY=/tmp/tenant-worker
export REQUIRE_INTEGRATION=1
go test ./... -count=1 -p 1 -race -coverprofile=/tmp/coverage.out
