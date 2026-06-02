# Rust Workspace CI Requirements

## Status

Draft specification for the repository CI workflow that verifies the Rust
workspace.

## Scope

This document specifies the repository-level CI requirements for the current
Rust workspace.

This document defines quality-gate behavior only. It does not define release,
publishing, or distribution automation.

## Requirements

### REQ-CI-001

The repository shall define a GitHub Actions workflow that runs on pushes to
`main` and on pull requests targeting `main`.

### REQ-CI-002

The workflow shall trigger for Rust-workspace-relevant pull request changes,
including changes to:

- `Cargo.toml`
- `Cargo.lock`
- `crates/**`
- `.github/workflows/ci.yml`

### REQ-CI-003

The workflow shall enforce formatting with `cargo fmt --check --all`.

### REQ-CI-004

The workflow shall enforce linting with Clippy across the workspace and fail on
warnings treated as errors.

### REQ-CI-005

The workflow shall execute the Rust workspace test suite.

### REQ-CI-006

The workflow shall remain limited to repository quality verification and shall
not implement crate publishing, release creation, or release artifact
distribution in this pass.

### REQ-CI-007

The workflow shall use practical CI optimizations appropriate for routine Rust
development, including cancellation of superseded runs and Rust-aware caching.

### REQ-CI-008

The workflow shall align with the current repository structure and existing
workspace commands rather than introducing new quality tools.

## Out of Scope

This change does not define or own:

- crate publishing
- GitHub release automation
- binary artifact packaging
- coverage reporting
- fuzzing automation
- multi-platform expansion beyond the minimal hosted CI surface
