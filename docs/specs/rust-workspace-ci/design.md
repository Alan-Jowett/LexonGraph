# Rust Workspace CI Design

## Status

Draft design specification for the repository CI workflow that verifies the
Rust workspace.

## Design Goals

The workflow design is intended to be:

- minimal
- deterministic
- aligned with the current Rust workspace
- efficient for routine pull requests
- explicit about non-goals

## Workflow Boundary

The workflow owns:

- formatting verification
- lint verification
- Rust test execution
- hosted CI triggering and cancellation behavior

The workflow does not own:

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

Pull request triggers are further limited to Rust-workspace-relevant paths:

- `Cargo.toml`
- `Cargo.lock`
- `crates/**`
- `.github/workflows/ci.yml`

### DSG-CI-003 `Concurrency`

The workflow uses GitHub Actions concurrency to cancel superseded runs for the
same workflow and branch or pull request.

### DSG-CI-004 `Execution environment`

The workflow runs on `ubuntu-latest` using the stable Rust toolchain.

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

### DSG-CI-009 `Caching`

The workflow uses Rust-aware dependency and build caching suitable for GitHub
Actions.

### DSG-CI-010 `Non-goals`

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
| DSG-CI-009 | REQ-CI-007 |
| DSG-CI-010 | REQ-CI-006 |
