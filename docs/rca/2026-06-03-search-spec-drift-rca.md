<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# RCA: Search Crate Coverage-Driven Spec Drift

## Status

Coverage-driven audit of `crates/lexongraph-search/src/lib.rs` against the
`docs/specs/rust-search-crate/` package.

## Summary

This audit used uncovered regions in `crates/lexongraph-search/src/lib.rs` as a
discovery signal, then traced those regions through the governing search spec,
protocol documents, validation plan, and executable tests.

The audit found **one confirmed spec-drift issue**:

- the crate's default scorer exposes an explicit `NonFiniteScore` failure path
  at `crates/lexongraph-search/src/lib.rs:91-96`, but the governing spec
  package does not define that behavior and the validation surface does not
  trace or exercise it

Most of the remaining uncovered code falls into one of two non-finding
categories:

- diagnostic formatting and error-chaining branches that the spec does not make
  contractual
- conformance-helper expectation branches where the spec requires
  representative failure-path checks, and the repository already provides those

## Scope Summary

| Surface | Artifact |
|---|---|
| Audited module | `crates/lexongraph-search/src/lib.rs` |
| Governing spec package | `docs/specs/rust-search-crate/requirements.md`, `docs/specs/rust-search-crate/design.md`, `docs/specs/rust-search-crate/validation.md` |
| Protocol documents consulted | `docs/protocol/search.md`, `docs/protocol/blocks.md` |
| Validation artifacts consulted | `crates/lexongraph-search/tests/spec_validation.rs`, `crates/lexongraph-search/tests/conformance_feature.rs` |
| Coverage artifact | `lcov.info` generated via `cargo llvm-cov --workspace --all-features --locked --lcov --output-path lcov.info` |

## Coverage Summary

- raw LCOV for `crates/lexongraph-search/src/lib.rs`: `436 / 546 = 79.85%`
- total normalized candidates: `38`
- excluded candidates: `23`
- inconclusive candidates: `0`
- classified findings: `1`

## Candidate Ledger

### Significant candidates carried forward

| Candidate | Module location | Behavioral unit |
|---|---|---|
| CG-001 | `src/lib.rs:94` | `CosineScore::from_f64` rejects a non-finite computed cosine score |
| CG-021 | `src/lib.rs:520` | reverse branch/leaf comparator arm in canonical tie-breaking |
| CG-026..CG-029 | `src/lib.rs:814-843` | conformance helper expectation failures for compatibility harnesses |
| CG-030..CG-038 | `src/lib.rs:863-931` | conformance helper expectation failures for scorer harnesses |

### Exclusions

| Candidate | Module location | Rationale |
|---|---|---|
| CG-002..CG-019 | `src/lib.rs:138-306` | `Display` and `Error::source` branches for `DefaultPolicyError` and `SearchError`; uncovered wording and chaining branches are diagnostic-only and no governing spec text makes the exact formatting contractual |
| CG-020 | `src/lib.rs:367` | explicit invariant panic guarded by `frontier.iter().take(n).all(SearchCandidate::is_leaf)`; inactive path with local proof in the same function |
| CG-022 | `src/lib.rs:681-683` | duplicate unsupported-encoding arm inside `cosine_similarity_bytes`; unsupported encodings already fail earlier through `validate_embedding_bytes` -> `element_width` at `src/lib.rs:579-596` and `src/lib.rs:702-709` |
| CG-023 | `src/lib.rs:720-722` | `usize::try_from` overflow branch in `expected_byte_len`; inactive for the current 64-bit audit target after the preceding `checked_mul` guard |
| CG-024..CG-025 | `src/lib.rs:749-755` | `ConformanceError` display wording only |

## Findings

### F-001

| Field | Value |
|---|---|
| Finding ID | `F-001` |
| Candidate ID | `CG-001` |
| Drift category | `D9_UNDOCUMENTED_BEHAVIOR` |
| Severity | Medium |
| Confidence | Medium |
| Module location | `crates/lexongraph-search/src/lib.rs:91-96` |
| Spec locations | None - no governing requirement identified for computed-score overflow or non-finite cosine output |
| Closest related spec text | `docs/specs/rust-search-crate/requirements.md:211-226`, `docs/specs/rust-search-crate/design.md:150-177`, `docs/specs/rust-search-crate/validation.md:279-294` |
| Validation and test locations | `crates/lexongraph-search/tests/spec_validation.rs:867-1030` covers `VAL-SEARCH-024`, but no validation entry or test names `NonFiniteScore` or the computed-score failure path |

**Evidence**

`CosineScore::from_f64` returns `DefaultPolicyError::NonFiniteScore` when the
computed cosine value is not finite at `crates/lexongraph-search/src/lib.rs:91-96`.
That variant is part of the public default-policy error taxonomy at
`crates/lexongraph-search/src/lib.rs:101-128` and has public display behavior at
`crates/lexongraph-search/src/lib.rs:130-165`.

The closest governing spec text for the default scorer is narrower:

- `REQ-SEARCH-021` defines explicit failure for unsupported encodings,
  inconsistent byte lengths, zero-magnitude embeddings, non-finite encoded
  values, and unsafe dimensionality, but not for a computed non-finite cosine
  score (`docs/specs/rust-search-crate/requirements.md:211-226`)
- `DSG-SEARCH-019` mirrors the same failure set (`docs/specs/rust-search-crate/design.md:150-177`)
- `VAL-SEARCH-024` validates the same enumerated cases (`docs/specs/rust-search-crate/validation.md:279-294`)

No repository test references `NonFiniteScore`, and the LCOV candidate for line
94 remained uncovered.

**Why this is not a false positive**

This is not just untested formatting. The implementation exposes a distinct
public failure outcome on the crate's default scoring boundary. The governing
requirements, design, and validation entries enumerate the default scorer's
failure modes in detail, but they do not include this one. That combination
rules out the safer explanation that the behavior is already specified
elsewhere.

**Impact**

Callers using the crate-provided default scorer can encounter a public failure
mode that the spec package never defines. Another implementation could handle
the same arithmetic-overflow condition differently and still appear conformant
to the current requirements and validation surface.

**Recommended next action**

Decide whether computed non-finite cosine results are contractual behavior.

1. If yes, update `REQ-SEARCH-021`, `DSG-SEARCH-019`, and `VAL-SEARCH-024` to
   define and validate that explicit failure path, then add a targeted test.
2. If no, collapse or internalize the extra error variant so the public default
   scorer surface matches the narrower behavior already specified.

## Rejected Candidates

| Candidate | Reason rejected | Exact safe mechanism |
|---|---|---|
| CG-021 | not a drift finding | `VAL-SEARCH-019` is already realized by `crates/lexongraph-search/tests/spec_validation.rs:589-673`, which validates observable canonical ordering through the public `Searcher` API; the uncovered reverse comparator arm is an internal branch-shape artifact, not an untraced acceptance gap |
| CG-026..CG-029 | not a drift finding | `REQ-SEARCH-015` requires representative direct checks of helper-owned expectation failures (`docs/specs/rust-search-crate/requirements.md:164-166`), and `crates/lexongraph-search/tests/conformance_feature.rs:399-440` already exercises the compatibility suite success path plus one representative expectation failure |
| CG-030..CG-038 | not a drift finding | `VAL-SEARCH-016` requires the shared helpers to reject violating fixtures, detect nondeterminism, verify repeated-input stability, and expose representative helper-owned expectation failures (`docs/specs/rust-search-crate/validation.md:182-194`); `crates/lexongraph-search/tests/conformance_feature.rs:399-440` already covers the success path and one representative scorer expectation failure, so the remaining uncovered branches are additional diagnostics rather than uncovered required behavior |

## Finding Distribution

| Drift category | Count |
|---|---|
| D2_UNTESTED_REQUIREMENT | 0 |
| D9_UNDOCUMENTED_BEHAVIOR | 1 |
| D11_UNIMPLEMENTED_TEST_CASE | 0 |
| D12_UNTESTED_ACCEPTANCE_CRITERION | 0 |
| D13_ASSERTION_MISMATCH | 0 |

## Dominant Drift Pattern

**Undocumented defensive behavior.**

The spec package is strong on the named default-scorer input guardrails, but
the implementation adds one extra public failure mode beyond that enumerated
surface. Coverage exposed the gap because the validation plan is tightly aligned
to the documented failure set and therefore never reaches the undocumented one.

## Root Cause

The root cause was not a missing spec package. The root cause was that the
default-scorer contract was written as an explicit list of expected guardrails,
while the implementation independently added another public guardrail for a
computed non-finite cosine result.

That created a narrow but real four-way drift:

- requirements enumerate one set of failure modes
- design mirrors that same set
- validation proves only that set
- implementation exposes one more public error outcome

Because the extra behavior is defensive and edge-case heavy, it remained
uncovered and therefore invisible to the current traceability surface until the
coverage audit isolated it.

## Corrective Framing

The right repair is not just "add a test for line 94."

The right repair is to decide whether `NonFiniteScore` is:

1. part of the public default-scorer contract, in which case the spec package
   and validation surface need to adopt it, or
2. an implementation detail that should not remain as an extra public behavior
   beyond the documented contract

## Scope Limitation

This audit examined **uncovered regions only** in
`crates/lexongraph-search/src/lib.rs`. It does **not** clear covered code for
compliance with the LexonGraph protocol or the `rust-search-crate`
specification package.
