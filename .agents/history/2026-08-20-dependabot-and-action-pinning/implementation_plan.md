# Implementation Plan: Dependabot Configuration Alignment & GitHub Actions Pinning

## Goal
Align the Dependabot configuration with project requirements (grouping updates, `fix(deps)` commit prefix) and deterministically pin all GitHub Actions across workflows to their latest full commit SHAs with version comments.

## Proposed Changes

### Dependabot Configuration
- Update `.github/dependabot.yml`:
  - Set `commit-message.prefix` and `commit-message.prefix-development` to `fix(deps)` across ecosystems.
  - Group `github-actions` dependencies under `github-actions-dependencies` with `patterns: ["*"]`.
  - Group `gomod` dependencies under `go-dependencies` with `patterns: ["*"]`.

### GitHub Actions Pinning
- In `.github/workflows/ci.yml`:
  - `actions/checkout`: Pin to `3d3c42e5aac5ba805825da76410c181273ba90b1` (`# v7.0.1`)
  - `actions/setup-go`: Pin to `b7ad1dad31e06c5925ef5d2fc7ad053ef454303e` (`# v7.0.0`)
  - `golangci/golangci-lint-action`: Pin to `ba0d7d2ec06a0ea1cb5fa41b2e4a3ab91d21278a` (`# v9.3.0`)
  - `actions/upload-artifact`: Pin to `043fb46d1a93c77aae656e7c1c64a875d1fc6a0a` (`# v7.0.1`)
- In `.github/workflows/release.yml`:
  - Verify all actions are already pinned to latest release SHAs (`release-please-action` v5.0.0, `checkout` v7.0.1, `setup-go` v7.0.0).

## Verification Plan
- Run `make fmt` to ensure formatting standards.
- Run `go test ./...` and `go test -p 1 -v -race -tags=integration ./...` to verify test suite health.
- Validate YAML syntax and action SHA references.
