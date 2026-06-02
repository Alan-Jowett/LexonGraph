# Rust Workspace CI Validation

## Status

Draft validation specification for the repository CI workflow that verifies the
Rust workspace.

## Validation Scope

These validation entries define the expected verification surface for the
repository CI workflow.

## Validation Entries

### VAL-CI-001

Open a pull request that changes a Rust-workspace-relevant path.

**Pass condition:** the CI workflow is triggered.

**Traces to:** REQ-CI-001, REQ-CI-002

### VAL-CI-002

Open a pull request that changes only paths outside the configured Rust CI path
filter.

**Pass condition:** the CI workflow is not triggered solely by that change.

**Traces to:** REQ-CI-002

### VAL-CI-003

Introduce a formatting violation in Rust source.

**Pass condition:** the formatting job fails.

**Traces to:** REQ-CI-003

### VAL-CI-004

Introduce a Clippy warning in the Rust workspace.

**Pass condition:** the lint job fails because warnings are treated as errors.

**Traces to:** REQ-CI-004

### VAL-CI-005

Introduce or expose a failing Rust test.

**Pass condition:** the test job fails.

**Traces to:** REQ-CI-005

### VAL-CI-006

Push multiple updates rapidly to the same branch or pull request.

**Pass condition:** superseded runs for the same workflow are canceled and the
newest run remains authoritative.

**Traces to:** REQ-CI-007

### VAL-CI-007

Inspect the workflow definition.

**Pass condition:** it uses stable Rust, least-privilege permissions, Rust-aware
caching, and no release or publish automation.

**Traces to:** REQ-CI-006, REQ-CI-007, REQ-CI-008
