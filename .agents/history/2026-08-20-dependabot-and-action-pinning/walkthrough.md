# Dependabot Alignment & GitHub Actions SHA Pinning

## Overview
This PR aligns `.github/dependabot.yml` with project requirements and deterministically pins all GitHub Actions across workflows to their latest immutable commit SHAs with version comments.

## Key Changes

### 1. Dependabot Configuration (`.github/dependabot.yml`)
- Configured conventional commit prefix `fix(deps)` for both regular and development updates.
- Set up ecosystem grouping using wildcard patterns (`*`):
  - `github-actions-dependencies` for all GitHub Actions.
  - `go-dependencies` for all Go module updates.
- Removed restrictive `minor-and-patch` update-type filters to enable unified grouped PRs.

### 2. GitHub Actions Pinning (`.github/workflows/ci.yml`)
Deterministically verified and pinned all actions to their latest release commit SHAs:
- `actions/checkout` -> `3d3c42e5aac5ba805825da76410c181273ba90b1` (`# v7.0.1`)
- `actions/setup-go` -> `b7ad1dad31e06c5925ef5d2fc7ad053ef454303e` (`# v7.0.0`)
- `golangci/golangci-lint-action` -> `ba0d7d2ec06a0ea1cb5fa41b2e4a3ab91d21278a` (`# v9.3.0`)
- `actions/upload-artifact` -> `043fb46d1a93c77aae656e7c1c64a875d1fc6a0a` (`# v7.0.1`)

*(Note: `.github/workflows/release.yml` was verified and already pinned to latest release SHAs.)*

## Verification Results
- `make fmt`: Formatting and import organization verified.
- `go test -v -race -coverprofile=coverage.out ./...`: Passed.
- `go test -p 1 -v -race -tags=integration ./...`: All integration tests with PostgreSQL containers passed.
