<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# RCA: Embeddings Trait Conformance Drift

## Status

Analysis of why the spec-first model did not fully constrain the conformance
helper behavior in `crates/lexongraph-embeddings-trait`.

## Summary

Coverage analysis of `crates/lexongraph-embeddings-trait/src/lib.rs` showed
that the main uncovered regions are not random leftovers. They cluster in the
feature-gated conformance surface where the crate defines helper expectations
for downstream embedding-provider implementations.

The central failure was not the absence of a spec package. The failure was that
the spec package defined the existence of reusable conformance helpers and the
high-level fixture roles, but it did not fully define the observable behavior
of those helpers when a downstream harness is misconfigured or when the
requested `EmbeddingSpec` is outside the helper's supported encoding set.

That left meaningful public behavior underconstrained across:

- requirements
- design
- validation
- implementation

## Impact

The immediate impact is that publicly exposed conformance-helper behavior can
drift without the spec package or validation layer detecting it.

Specifically, the implementation currently contains behavior for:

- exact-byte equality checking against `expected_embedding`
- explicit rejection when a supposed failing provider succeeds
- explicit rejection when a supposed invalid-output provider produces a valid
  embedding
- explicit rejection when conformance validation sees an unsupported embedding
  encoding
- public `ConformanceError` display mapping between provider and expectation
  failures

but only part of that surface is defined in the governing spec package.

This means:

1. downstream harness authors are bound by helper behavior that the spec does
   not fully describe
2. validation proves the happy-path fixture roles but not all observable helper
   rejection paths
3. future changes to `EmbeddingSpec.encoding` handling can drift silently inside
   the conformance layer

## Evidence

### Coverage

Coverage was gathered with `cargo llvm-cov --workspace --all-features --locked --lcov --output-path lcov.info`
and then filtered to `crates/lexongraph-embeddings-trait/src/lib.rs`.

- raw LCOV for the audited file: `46 / 75 = 61.33%`

The uncovered lines are concentrated in the conformance helper surface rather
than in the default production-facing trait boundary.

### Uncovered regions

The main uncovered regions were:

- `src/lib.rs:49-56` - `ConformanceError` display mapping
- `src/lib.rs:87-91` - exact-byte mismatch rejection in
  `run_embedding_provider_suite`
- `src/lib.rs:96-98` - rejection when the supposed failing provider succeeds
- `src/lib.rs:107-109` - rejection when the supposed invalid-output provider
  passes validation
- `src/lib.rs:115-121,133-140` - unsupported or future encoding rejection in
  conformance validation

These are not formatting-only branches. They determine what the public helper
surface accepts, rejects, and reports to downstream test code.

## Root Cause

The root cause was an incomplete conformance specification:

> The spec said what helper fixtures exist, but it did not fully say what the
> helper must do when those fixtures violate the helper's own assumptions.

In other words, the spec described the conformance surface at the level of
roles and success conditions, but not the full observable behavior of the
helper when downstream test harnesses are wrong or when the helper cannot
validate a supplied encoding.

That omission matters because the crate's public conformance surface is meant
to be reused by downstream crates. Once exposed publicly, helper expectations
and helper failures become contract-relevant behavior.

Without explicit requirements and validation for those paths, the implementation
had to invent policy at each branch point.

## Contributing Factors

### 1. The spec defined fixture roles more clearly than helper diagnostics

`docs/specs/rust-embeddings-trait/design.md` defines the harness shape and the
three main fixture roles, but it does not explicitly define:

- whether the conforming provider must match an exact expected byte vector
- what rejection message or category should be surfaced when a fixture violates
  its declared role
- what the helper must do for unsupported or future embedding encodings

### 2. Validation proves only the intended fixture arrangement

`docs/specs/rust-embeddings-trait/validation.md` verifies that:

- an async provider can return compatible bytes
- the conformance suite accepts the conforming fixture
- the conformance suite uses a failing fixture
- the conformance suite uses an invalid-output fixture

Those are important, but they do not verify the helper's observable behavior
when a harness is internally inconsistent or when conformance validation cannot
compute the expected embedding length.

### 3. Protocol evolution is acknowledged but not flowed into validation

`docs/protocol/blocks.md` defines the current encoding set and explicitly says
future revisions may add more encodings.

The conformance helper already contains an explicit unsupported-encoding
rejection path, which means the implementation recognizes the problem. The spec
package does not yet validate that behavior.

## What Went Wrong with the Spec-First Model

The problem was not that the repository lacked specs.

The problem was that the specs were not total for the conformance surface. They
defined:

- the existence of the shared trait
- the existence of reusable conformance helpers
- the intended fixture roles
- the opt-in feature-gated exposure model

but they did not define:

- whether conformance requires exact-byte equality or only spec compatibility
- the observable rejection behavior when a harness fixture violates its role
- the expected behavior when the helper sees an unsupported or future encoding
- whether the public `ConformanceError` display surface is contractual

That is the precise point where the spec-first model broke down: the model
described the nominal contract, but not the full helper behavior that the crate
publicly exposes to downstream test code.

## Corrective Framing

The right repair is not "add tests until coverage goes up."

The right repair is:

1. define the public conformance-helper contract more precisely
2. decide which helper behaviors are normative versus incidental diagnostics
3. make validation prove the intended helper rejection semantics
4. only then use coverage to confirm that the specified conformance behavior is
   exercised

## Required Spec Repairs

### Conforming fixture semantics

The spec should define whether a conforming provider fixture must:

- return any embedding compatible with the requested `EmbeddingSpec`
- or return the exact bytes named by `expected_embedding`

### Helper rejection semantics

The spec should define the required outcome when:

- the supposed conforming provider returns the wrong bytes
- the supposed failing provider succeeds
- the supposed invalid-output provider returns bytes that still satisfy the
  requested `EmbeddingSpec`

### Encoding validation boundary

The spec should define the required outcome when:

- conformance validation sees a known supported encoding
- conformance validation sees an unsupported encoding
- the block protocol adds a future encoding that this helper does not yet
  understand

### Public error surface

The spec should define whether `ConformanceError` display wording and category
mapping are part of the public contract or merely diagnostic detail.

## Recommended Validation Repairs

Add executable validation for:

1. a harness whose conforming provider returns a length-compatible but wrong
   embedding
2. a harness whose failing provider succeeds
3. a harness whose invalid-output provider accidentally satisfies the requested
   `EmbeddingSpec`
4. a harness using an unsupported embedding encoding
5. the intended public behavior of `ConformanceError` if that surface is meant
   to be contractual

## Lessons Learned

1. A conformance helper is part of the contract if it is publicly exported.
2. Fixture roles alone do not fully specify helper behavior.
3. Public diagnostic behavior should be either specified or deliberately treated
   as non-contractual.
4. Future-protocol hooks need matching validation, not just defensive code.
5. Coverage is useful here because it highlights where the conformance contract
   is least explicit.

## Follow-Up

The next repair pass for `lexongraph-embeddings-trait` should:

1. update `docs/specs/rust-embeddings-trait/requirements.md`
2. update `docs/specs/rust-embeddings-trait/design.md`
3. update `docs/specs/rust-embeddings-trait/validation.md`
4. add targeted tests for the helper rejection semantics listed above
5. re-run coverage and remap any remaining uncovered conformance branches
