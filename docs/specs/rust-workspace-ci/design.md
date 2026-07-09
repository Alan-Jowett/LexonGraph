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
- Azure live integration verification for the Azure-backed live-test crates
- Rust coverage execution and publication
- README surfacing of repository quality/status badges and repository navigation
- SPDX header verification
- contributor-facing local Git hook enforcement
- hosted CI triggering and cancellation behavior

The repository quality gates do not own:

- release automation
- package publishing
- artifact distribution
- minimum coverage threshold enforcement
- fuzzing jobs

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

### DSG-CI-022 `Azure live verification job`

The workflow contains a dedicated Azure live-verification job in
`.github/workflows/ci.yml` for:

- `lexongraph-block-store-azure`
- `lexongraph-block-store-azure-sdk`
- `lexongraph-block-store-azure-table-v2`

That job:

- runs on `ubuntu-latest`
- is separate from the default workspace test job
- skips forked `pull_request` runs before requesting Azure credentials or OIDC
  tokens
- performs change detection for Azure-live-test-relevant paths before executing
  live Azure setup and test steps
- exits without running the live Azure setup and test steps when those relevant
  paths were not changed for the current workflow event

The Azure-live-test-relevant paths are:

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

### DSG-CI-023 `Azure OIDC authentication`

The Azure live-verification job grants only the GitHub Actions permissions
needed for checkout and OIDC token exchange, then authenticates to Azure via a
federated GitHub identity before provisioning any test resources.

The workflow does not use repository-stored storage-account keys, account
connection strings, or repository-managed SAS tokens as the primary
authentication path for this job.

If a selected live-test surface requires a service-specific SAS that cannot be
minted directly through the Azure CLI with OIDC alone, the workflow may
retrieve a key for the isolated temporary storage account after successful OIDC
authentication and use it only to derive the temporary test SAS needed for that
run.

For the current Azure Table live-test surface, the workflow uses that temporary
account key only to create the isolated table and to mint a table-scoped SAS
with the minimum permissions required by the test flow, avoiding additional
table-specific data-plane RBAC assumptions for the federated identity.

### DSG-CI-024 `Ephemeral resource lifecycle`

After authenticating to Azure, the live-verification job provisions isolated
temporary Azure storage test resources for the current workflow run, derives the
SAS URLs needed by the selected live tests, passes those SAS URLs only to the
selected live test invocations, and then cleans up the temporary Azure
resources even when a live test fails.

For the current repository surface, this includes an isolated Azure Blob
container for the blob-backed crates and an isolated Azure Table for
`lexongraph-block-store-azure-table-v2`.

The provisioned scope remains limited to the minimum resource boundary needed to
obtain the isolated test-owned blob and table resources and to clean them up
deterministically.

### DSG-CI-025 `Explicit live test invocation`

The Azure live-verification job invokes only each selected crate's dedicated
live-test surface by explicit Cargo test selection rather than broad workspace
test execution.

The default `cargo test --workspace --locked` job remains the routine
repository-wide fast path and does not require live Azure credentials.

### DSG-CI-016 `Coverage job`

The workflow contains a dedicated coverage job that:

- runs on `ubuntu-latest`
- uses the stable Rust toolchain with the `llvm-tools` component
- restores Rust-aware cache state suitable for repeated coverage runs
- installs `cargo-llvm-cov`
- generates an `lcov.info` report for the Rust workspace test suite with all
  Cargo features enabled

### DSG-CI-017 `Coveralls publication`

The coverage job publishes the generated `lcov.info` report to Coveralls from
GitHub Actions using repository-provided workflow credentials.

### DSG-CI-018 `README badges`

The repository README displays a compact badge row near the top of the document
that links to:

- the main-branch status badge for `.github/workflows/ci.yml`
- the main-branch Coveralls coverage badge for the repository
- the repository MIT license

The README badge set is limited to status surfaces that currently exist in this
repository and does not advertise workflows that are not present.

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

### DSG-CI-013 `Contributor entrypoint`

The repository README serves as the contributor-facing entrypoint for
repository operation by documenting:

- how to enable the repository-managed hooks via `git config core.hooksPath hooks`
- the CI-aligned local Rust verification commands
- where the current repository surfaces live at a high level

### DSG-CI-019 `README repository inventory`

The repository README summarizes the current tracked repository surface by
category rather than as an exhaustive file catalog.

At minimum, that summary includes:

- active protocol documents that define the current governed behavior
- maintained specification packages under `docs/specs/`
- implemented workspace crates under `crates/`
- active repository automation and configuration surfaces that contributors are
  expected to navigate, including `.github/workflows/ci.yml`,
  `.github/dependabot.yml`, and `.github/skills/`
- maintained supporting documentation collections such as `docs/arch/`,
  `docs/audits/`, and `docs/rca/` when present

### DSG-CI-020 `README status labeling`

When the README references tracked artifacts outside the active governed or
implemented surface, it groups or labels them so readers can distinguish:

- active governed protocol/specification/implementation surfaces
- active repository maintenance surfaces
- supporting, reference, or future-facing material

This includes labeling `docs/protocol/ebcp.md` according to its current
governed status in the repository rather than treating it as permanently
future-facing.

### DSG-CI-021 `README authority boundary`

The README remains an orientation document. It links readers to authoritative
artifacts such as `docs/protocol/` and `docs/specs/` for normative behavior and
does not restate those specifications in full.

### DSG-CI-014 `Caching`

The workflow uses Rust-aware dependency and build caching suitable for GitHub
Actions.

### DSG-CI-015 `Non-goals`

The workflow does not include:

- crate publishing
- GitHub release creation
- release artifact upload
- minimum coverage threshold enforcement
- fuzzing
- platform-matrix expansion

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-CI-001..005 | REQ-CI-001, REQ-CI-002, REQ-CI-006, REQ-CI-007, REQ-CI-008 |
| DSG-CI-006 | REQ-CI-003, REQ-CI-008 |
| DSG-CI-007 | REQ-CI-004, REQ-CI-007, REQ-CI-008 |
| DSG-CI-008 | REQ-CI-005, REQ-CI-008 |
| DSG-CI-022 | REQ-CI-022 |
| DSG-CI-023 | REQ-CI-023 |
| DSG-CI-024 | REQ-CI-024 |
| DSG-CI-025 | REQ-CI-025 |
| DSG-CI-016 | REQ-CI-007, REQ-CI-015 |
| DSG-CI-017 | REQ-CI-016 |
| DSG-CI-018 | REQ-CI-017 |
| DSG-CI-009 | REQ-CI-010, REQ-CI-011 |
| DSG-CI-010 | REQ-CI-010, REQ-CI-012, REQ-CI-014 |
| DSG-CI-011 | REQ-CI-009, REQ-CI-012 |
| DSG-CI-012 | REQ-CI-014 |
| DSG-CI-013 | REQ-CI-009, REQ-CI-013 |
| DSG-CI-019 | REQ-CI-018, REQ-CI-020 |
| DSG-CI-020 | REQ-CI-019 |
| DSG-CI-021 | REQ-CI-021 |
| DSG-CI-014 | REQ-CI-007 |
| DSG-CI-015 | REQ-CI-006 |
