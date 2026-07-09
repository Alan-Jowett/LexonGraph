<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Workspace CI Requirements

## Status

Draft specification for the repository quality gates that verify the Rust
workspace and enforce repository-managed SPDX header policy.

## Scope

This document specifies the repository-level quality-gate requirements for the
current repository.

This document defines CI and local-hook quality-gate behavior only. It does not
define release, publishing, or distribution automation.

## Requirements

### REQ-CI-001

The repository shall define a GitHub Actions workflow that runs on pushes to
`main` and on pull requests targeting `main`.

### REQ-CI-002

The workflow shall trigger for repository-quality-relevant pull request changes,
including changes to:

- `Cargo.toml`
- `Cargo.lock`
- `crates/**`
- `docs/**`
- `hooks/**`
- `README.md`
- `.gitignore`
- `.gitattributes`
- `.github/skills/**`
- `.github/workflows/ci.yml`

### REQ-CI-003

The workflow shall enforce formatting with `cargo fmt --check --all`.

### REQ-CI-004

The workflow shall enforce linting with Clippy across the workspace and fail on
warnings treated as errors.

### REQ-CI-005

The workflow shall execute the Rust workspace test suite.

### REQ-CI-006

The repository quality gates shall remain limited to repository verification and
shall not implement crate publishing, release creation, release artifact
distribution, or minimum coverage threshold enforcement in this pass.

### REQ-CI-007

The workflow shall use practical CI optimizations appropriate for routine
repository development, including cancellation of superseded runs and
Rust-aware caching where applicable.

### REQ-CI-008

The repository quality gates shall align with the current repository structure,
Git, and existing workspace commands rather than requiring external hook
managers or third-party license scanners.

### REQ-CI-009

The repository shall provide versioned Git hook script artifacts in-repo for
contributor installation and local commit-time SPDX policy enforcement.

### REQ-CI-010

All tracked repository files of types `.md`, `.rs`, `.toml`, `.yml`, and
`.gitignore` shall contain an SPDX license header declaring MIT and the notice
`Copyright (c) 2026 LexonGraph contributors`, using comment syntax valid for
the file content they begin with.

### REQ-CI-011

Generated-but-tracked files outside the authored header policy surface,
including `Cargo.lock`, shall be excluded from required SPDX header
enforcement.

### REQ-CI-012

The local commit-time hook shall reject commits when staged governed files are
missing or have incomplete required SPDX headers, and shall evaluate the staged
index rather than the working tree.

### REQ-CI-013

The repository README shall serve as the contributor-facing entrypoint for
repository operation by documenting how contributors install and use the
repository-managed Git hooks, how they run the repository's local quality-gate
commands, and where they navigate to the current repository surfaces.

### REQ-CI-014

The GitHub Actions workflow shall verify that governed tracked files contain
the required SPDX headers and shall fail when any governed file is missing or
has an incomplete required header.

### REQ-CI-015

The GitHub Actions workflow shall include a dedicated coverage job that runs
the Rust workspace test suite with all Cargo features enabled under coverage
instrumentation and emits an LCOV coverage report.

### REQ-CI-016

The coverage job shall publish the generated LCOV report to Coveralls from
GitHub Actions.

### REQ-CI-017

The repository README shall display badges for the current repository quality
and status surfaces by linking to the main-branch CI workflow status, the
main-branch Coveralls coverage status, and the repository MIT license.

### REQ-CI-018

The repository README shall accurately summarize the current active repository
surface, including implemented workspace crates, maintained specification
packages, active protocol documents, and repository automation or
configuration surfaces that contributors are expected to navigate.

### REQ-CI-019

When the repository README references tracked artifacts that are not part of
the active governed or implemented surface, it shall label or group them so
readers can distinguish supporting, reference, or future-facing material from
active protocol, specification, implementation, and maintenance surfaces.

### REQ-CI-020

The repository README shall include newly added top-level navigational surfaces
that materially affect repository use or maintenance when those surfaces are
tracked and intended for contributor use.

### REQ-CI-021

The repository README shall remain a concise orientation document and shall
link to authoritative protocol and specification artifacts rather than
duplicating their normative behavior in full.

### REQ-CI-022

The repository CI workflow shall include a dedicated Azure live-verification
job within `.github/workflows/ci.yml` for the Azure-backed live-test crates:

- `lexongraph-block-store-azure`
- `lexongraph-block-store-azure-sdk`
- `lexongraph-block-store-azure-table-v2`

That job shall run on the same workflow events as the main CI workflow but
shall execute only when the change set touches Azure-live-test-relevant
surfaces, including:

- `crates/lexongraph-block-store-azure/**`
- `crates/lexongraph-block-store-azure-sdk/**`
- `crates/lexongraph-block-store-azure-table-v2/**`
- `Cargo.toml`
- `Cargo.lock`
- `docs/specs/rust-azure-blob-block-store/**`
- `docs/specs/rust-azure-blob-block-store-sdk/**`
- `docs/specs/rust-azure-table-block-store-v2/**`
- `docs/specs/rust-workspace-ci/**`
- `.github/workflows/ci.yml`

For `pull_request` events, the Azure live-verification job shall execute only
for pull requests whose head repository matches this repository, so forked pull
requests that cannot use the repository's Azure trust configuration do not
become hard failures.

### REQ-CI-023

The Azure live-verification job shall authenticate to Azure from GitHub Actions
using GitHub OIDC / federated credentials rather than long-lived storage-account
credentials or repository-stored SAS tokens.

### REQ-CI-024

The Azure live-verification job shall create and clean up isolated temporary
Azure storage resources for each run and shall not depend on shared
pre-provisioned blob or table test resources.

If the live job provisions more than a blob container, it shall keep the
provisioned resource scope limited to what is needed to run and clean up the
verification, including any temporary Azure Table needed for the selected live
tests.

### REQ-CI-025

The Azure live-verification job shall invoke the crate's dedicated live-test
mode explicitly and shall not fold live Azure verification into the default
workspace-wide `cargo test --workspace --locked` path.

## Out of Scope

This change does not define or own:

- crate publishing
- GitHub release automation
- binary artifact packaging
- minimum coverage threshold enforcement
- fuzzing automation
- multi-platform expansion beyond the minimal hosted CI surface
