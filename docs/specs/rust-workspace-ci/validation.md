<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Workspace CI Validation

## Status

Draft validation specification for the repository quality gates that verify the
Rust workspace and enforce repository-managed SPDX header policy.

## Validation Scope

These validation entries define the expected verification surface for the
repository CI workflow.

## Validation Entries

### VAL-CI-001

Open a pull request that changes a repository-quality-relevant path.

**Pass condition:** the CI workflow is triggered.

**Traces to:** REQ-CI-001, REQ-CI-002

### VAL-CI-002

Open a pull request that changes only paths outside the configured repository
quality path filter.

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

Introduce or expose a governed tracked file with a missing or incomplete SPDX
header.

**Pass condition:** the SPDX CI job fails.

**Traces to:** REQ-CI-010, REQ-CI-014

### VAL-CI-008

Stage a governed file whose working-tree content has the SPDX header but whose
staged content does not.

**Pass condition:** the pre-commit hook fails, demonstrating that it reads the
staged index rather than the working tree.

**Traces to:** REQ-CI-012

### VAL-CI-009

Inspect contributor documentation and repository hook artifacts.

**Pass condition:** the repository documents `git config core.hooksPath hooks`
and provides the referenced hook artifacts in-repo.

**Traces to:** REQ-CI-009, REQ-CI-013

### VAL-CI-010

Inspect governed Markdown files with leading YAML front matter.

**Pass condition:** the SPDX notice is present without removing the opening
front-matter delimiter.

**Traces to:** REQ-CI-010

### VAL-CI-011

Inspect the workflow definition.

**Pass condition:** it uses stable Rust, least-privilege permissions, Rust-aware
caching, an SPDX verification job, no release or publish automation, and no
minimum coverage threshold enforcement.

**Traces to:** REQ-CI-006, REQ-CI-007, REQ-CI-008, REQ-CI-014

### VAL-CI-012

Inspect the workflow definition for the coverage job.

**Pass condition:** the workflow defines a dedicated coverage job that uses the
stable Rust toolchain with `llvm-tools`, installs `cargo-llvm-cov`, generates
an `lcov.info` report for the workspace test suite with all Cargo features
enabled, and uploads that report to Coveralls.

**Traces to:** REQ-CI-007, REQ-CI-015, REQ-CI-016

### VAL-CI-013

Observe a successful CI run for a branch or pull request where the coverage job
is authorized to upload coverage results.

**Pass condition:** the coverage job succeeds and Coveralls records the uploaded
coverage report for the associated commit or pull request.

**Traces to:** REQ-CI-015, REQ-CI-016

### VAL-CI-014

Inspect the top section of `README.md`.

**Pass condition:** the README displays badges linking to the repository's
main-branch CI workflow status, main-branch Coveralls coverage status, and MIT
license, and does not advertise badges for workflows that are not present in
this repository.

**Traces to:** REQ-CI-017

### VAL-CI-015

Inspect the governed-file selector used by the repository-managed SPDX
verification surface.

**Pass condition:** generated-but-tracked files outside the authored header
policy surface, including `Cargo.lock`, remain excluded from required SPDX
header enforcement while governed authored files remain in scope.

**Traces to:** REQ-CI-011
