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

**Pass condition:** the README documents `git config core.hooksPath hooks`,
documents the CI-aligned local Rust verification commands, points readers to
the current repository surfaces at a high level, and the referenced hook
artifacts exist in-repo.

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

### VAL-CI-016

Inspect `README.md` against the current tracked repository surface.

**Pass condition:** the README accurately summarizes the active workspace
crates, maintained specification packages including `repository-dependabot`,
active governed protocol documents, and repository maintenance/configuration
surfaces that contributors are expected to navigate, without stale omissions.

**Traces to:** REQ-CI-018, REQ-CI-020

### VAL-CI-017

Inspect the README's grouping and status language for referenced repository
artifacts.

**Pass condition:** active governed and implemented surfaces are clearly
distinguished from active maintenance surfaces and from supporting, reference,
or future-facing material, and `docs/protocol/ebcp.md` is presented with the
same active or non-active status that the governed protocol and specification
surface currently gives it.

**Traces to:** REQ-CI-019

### VAL-CI-018

Inspect the README's scope and outbound links.

**Pass condition:** the README remains summary-level, links readers to
authoritative protocol and specification artifacts for normative behavior, and
does not duplicate those specifications in full.

**Traces to:** REQ-CI-021
