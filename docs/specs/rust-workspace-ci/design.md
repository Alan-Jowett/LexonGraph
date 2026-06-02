<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Workspace CI Design

## Status

Draft design specification for the repository quality gates that verify the
Rust workspace and enforce repository-managed SPDX header policy.

## Design Goals

The workflow design is intended to be:

- minimal
- deterministic
- aligned with the current repository
- efficient for routine pull requests
- explicit about non-goals

## Workflow Boundary

The repository quality gates own:

- formatting verification
- lint verification
- Rust test execution
- SPDX header verification
- contributor-facing local Git hook enforcement
- hosted CI triggering and cancellation behavior

The repository quality gates do not own:

- release automation
- package publishing
- artifact distribution
- coverage or fuzzing jobs

## Workflow Shape

### DSG-CI-001 `Workflow file`

The repository defines the workflow at `.github/workflows/ci.yml`.

### DSG-CI-002 `Triggers`

The workflow triggers on:

- `push` to `main`
- `pull_request` targeting `main`

Pull request triggers are further limited to repository-quality-relevant paths:

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

### DSG-CI-003 `Concurrency`

The workflow uses GitHub Actions concurrency to cancel superseded runs for the
same workflow and pull request when a pull request number is available, and for
the same workflow and Git ref otherwise.

### DSG-CI-004 `Execution environment`

The workflow runs on `ubuntu-latest`. Rust verification jobs use the stable
Rust toolchain, and SPDX verification uses the repository-managed hook script
via Bash.

### DSG-CI-005 `Permissions`

The workflow uses least-privilege permissions sufficient for checkout and CI
execution.

### DSG-CI-006 `Formatting job`

The workflow contains a formatting job that installs `rustfmt` and runs:

`cargo fmt --check --all`

### DSG-CI-007 `Lint job`

The workflow contains a lint job that installs `clippy`, restores Cargo cache,
and runs:

`cargo clippy --workspace --all-targets --locked -- -D warnings`

### DSG-CI-008 `Test job`

The workflow contains a test job that restores Cargo cache and runs:

`cargo test --workspace --locked`

### DSG-CI-009 `Governed file set and header forms`

The SPDX policy governs tracked files matching:

- `*.md`
- `*.rs`
- `*.toml`
- `*.yml`
- `.gitignore`

and excludes generated tracked files outside that authored surface, including
`Cargo.lock`.

Required header forms are:

- Markdown without front matter:
  - `<!-- SPDX-License-Identifier: MIT`
  - `  Copyright (c) 2026 LexonGraph contributors -->`
- Markdown with leading YAML front matter:
  - `---`
  - `# SPDX-License-Identifier: MIT`
  - `# Copyright (c) 2026 LexonGraph contributors`
- Rust:
  - `// SPDX-License-Identifier: MIT`
  - `// Copyright (c) 2026 LexonGraph contributors`
- TOML, YAML, and `.gitignore`:
  - `# SPDX-License-Identifier: MIT`
  - `# Copyright (c) 2026 LexonGraph contributors`

### DSG-CI-010 `Shared SPDX checker`

The repository defines a shared Bash checker at `hooks/check-spdx-headers`.
Both CI and local-hook enforcement invoke this script so the governed file set,
header forms, and validation logic remain consistent.

### DSG-CI-011 `Pre-commit hook`

The repository defines `hooks/pre-commit`, which invokes the shared SPDX
checker in staged-file mode and validates the staged index rather than the
working tree.

### DSG-CI-012 `SPDX CI job`

The workflow contains an SPDX job that checks out the repository and invokes
the shared SPDX checker in all-tracked-files mode.

### DSG-CI-013 `Contributor installation`

Contributor documentation instructs users to enable the repository-managed
hooks via `git config core.hooksPath hooks`.

### DSG-CI-014 `Caching`

The workflow uses Rust-aware dependency and build caching suitable for GitHub
Actions.

### DSG-CI-015 `Non-goals`

The workflow does not include:

- crate publishing
- GitHub release creation
- release artifact upload
- coverage reporting
- fuzzing
- platform-matrix expansion

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-CI-001..005 | REQ-CI-001, REQ-CI-002, REQ-CI-006, REQ-CI-007, REQ-CI-008 |
| DSG-CI-006 | REQ-CI-003, REQ-CI-008 |
| DSG-CI-007 | REQ-CI-004, REQ-CI-007, REQ-CI-008 |
| DSG-CI-008 | REQ-CI-005, REQ-CI-008 |
| DSG-CI-009 | REQ-CI-010, REQ-CI-011 |
| DSG-CI-010 | REQ-CI-010, REQ-CI-012, REQ-CI-014 |
| DSG-CI-011 | REQ-CI-009, REQ-CI-012 |
| DSG-CI-012 | REQ-CI-014 |
| DSG-CI-013 | REQ-CI-009, REQ-CI-013 |
| DSG-CI-014 | REQ-CI-007 |
| DSG-CI-015 | REQ-CI-006 |
