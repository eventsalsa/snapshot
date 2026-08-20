# Implementation Plan - Release Please Initialization

Align repository release automation with Release Please manifest mode, unified single-workflow architecture, and pinned commit SHAs.

## User Request
Align existing `release-please` setup with the `release-please-init` skill, ensure the very first released version is `0.0.1`, and open a PR when ready.

## Proposed Changes

### 1. Release Please Manifest & Configuration
- Create [.release-please-manifest.json](.release-please-manifest.json) mapping root package `.` to version `0.0.1`.
- Create [release-please-config.json](release-please-config.json) configuring Go package release type, `initial-version: 0.0.1`, `changelog-path: CHANGELOG.md`, `bump-minor-pre-major: true`, and schema validation.

### 2. GitHub Actions Workflow
- Update [.github/workflows/release.yml](.github/workflows/release.yml) to implement a unified single workflow:
  - Triggered on `push` to `main`.
  - Top-level `permissions: contents: write, pull-requests: write`.
  - Pin `googleapis/release-please-action` to verified commit SHA `@45996ed1f6d02564a971a2fa1b5860e934307cf7` (`# v5.0.0`) with manifest & config file arguments and `id: release`.
  - Conditionally run checkout (`actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1`) with `if: ${{ steps.release.outputs.release_created }}`.
  - Conditionally set up Go (`actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0`) with `go-version-file: 'go.mod'`.

### 3. Verification & PR
- Validate JSON files with `jq` and YAML workflow syntax with Python YAML parser.
- Run `make fmt` and `make lint` / `make test-unit`.
- Archive session artifacts into `.agents/history/2026-08-20-release-please-init/`.
- Commit changes adhering to conventional commit rules and multiline format.
- Push branch and open pull request via GitHub MCP server.
