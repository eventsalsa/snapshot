## Summary of Changes

This pull request aligns repository release automation with Release Please manifest mode and unified single-workflow architecture, ensuring the initial release version starts at `0.0.1`.

### Key Changes
1. **Manifest Configuration**:
   - Added `.release-please-manifest.json` setting root package `.` starting version to `0.0.1`.
   - Added `release-please-config.json` configuring schema, Go package release type, `initial-version: 0.0.1`, `changelog-path: CHANGELOG.md`, and `bump-minor-pre-major: true`.

2. **Unified Single Release Workflow**:
   - Updated `.github/workflows/release.yml` to run on push to `main` with `permissions: contents: write, pull-requests: write`.
   - Used step `id: release` for `googleapis/release-please-action` referencing the manifest and config files.
   - Pinned all GitHub Actions to exact 40-character commit SHAs with version comments:
     - `googleapis/release-please-action@45996ed1f6d02564a971a2fa1b5860e934307cf7 # v5.0.0`
     - `actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1`
     - `actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0`
   - Added conditional execution `if: ${{ steps.release.outputs.release_created }}` on subsequent checkout and Go setup steps.

## Verification Results
- Validated JSON schemas and syntax with `jq`.
- Validated YAML syntax for `.github/workflows/release.yml`.
- Verified code formatting with `make fmt`.
- Verified lint checks with `make lint` (0 issues).
- Verified unit test suite with `make test-unit` (all passed).
