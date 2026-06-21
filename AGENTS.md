# Developer Agent Instructions — eventsalsa/snapshot

This document contains guidelines, constraints, and git workflows for developer agents contributing to the `eventsalsa/snapshot` repository.

---

## 1. Git Workflow & Commit Conventions

To maintain a clean, traceable repository history, adhere strictly to the following conventions:

### Branch Management
- **No Direct Commits to Main**: Never commit directly to the `main` branch.
- **Branch Naming**: Always work on a separate branch. Branches must use the following conventional semantic prefixes:
  - `feat/<description>` for new features
  - `fix/<description>` for bug fixes
  - `docs/<description>` for documentation changes
  - `chore/<description>` for maintenance or project refactorings
- **Branch Checkout**: If currently on the `main` branch, checkout a new branch before modifying any codebase files:
  ```bash
  git checkout -b feat/my-new-feature
  ```

### Commit Messages
- **Conventional Commits**: Every commit message must start with a conventional commit type prefix (e.g., `feat:`, `fix:`, `docs:`, `test:`, `chore:`).
- **Extended Body Required**: Subject-only commits are not allowed. Every commit must contain a blank line followed by a descriptive body detailing what changed and why.
- **Multiline Format Restriction**: Do **not** use literal newline characters (`\n`) in a single commit message string. Instead, construct multiline commit messages by passing multiple `-m` flags on the command line:
  ```bash
  git commit -m "feat(postgres): support custom snapshots table" -m "This introduces functional configuration options to override the default snapshot table name." -m "Useful for deployments where eventsalsa infrastructure tables live in a custom DB schema."
  ```

---

## 2. Code Quality & Verification Suite

Before completing any task, execute the full local validation suite. All checks must pass with zero issues.

```bash
# Formats and organizes imports
make fmt

# Runs all tests, linters, and vulnerabilities audit
make check
```
