<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# RCA: Search Crate Coverage Follow-up Spec-Drift Audit

## Status

Follow-up coverage-driven audit of `crates/lexongraph-search/src/lib.rs` against
the `docs/specs/rust-search-crate/` package, using
`docs/rca/2026-06-03-search-spec-drift-rca.md` as the baseline.

## Summary

The previous RCA's sole confirmed drift is resolved in the current repository:

- `CosineScore::from_f64` still exposes `DefaultPolicyError::NonFiniteScore` at
  `crates/lexongraph-search/src/lib.rs:91-96`
- the governing spec now defines that behavior in
  `docs/specs/rust-search-crate/requirements.md:211-229` and
  `docs/specs/rust-search-crate/design.md:150-181`
- the validation plan now traces it in
  `docs/specs/rust-search-crate/validation.md:279-296`
- executable coverage now reaches the path through
  `crates/lexongraph-search/tests/spec_validation.rs:938-947`

Even with that gap closed, line coverage for `src/lib.rs` remains modest because
the remaining uncovered regions are concentrated in **non-contractual diagnostic
formatting** and **helper-owned conformance expectation branches that the spec
requires only representatively, not exhaustively**.

This follow-up audit found **no current confirmed spec-drift findings** in the
remaining uncovered regions.

## Scope Summary

| Surface | Artifact |
|---|---|
| Audited module | `crates/lexongraph-search/src/lib.rs` |
| Governing spec package | `docs/specs/rust-search-crate/requirements.md`, `docs/specs/rust-search-crate/design.md`, `docs/specs/rust-search-crate/validation.md` |
| Protocol documents consulted | `docs/protocol/search.md`, `docs/protocol/blocks.md` |
| Validation artifacts consulted | `crates/lexongraph-search/tests/spec_validation.rs`, `crates/lexongraph-search/tests/conformance_feature.rs` |
| Baseline RCA | `docs/rca/2026-06-03-search-spec-drift-rca.md` |
| Coverage artifact | `lcov.info` generated via `cargo llvm-cov --workspace --all-features --locked --lcov --output-path lcov.info` |

## Coverage Summary

- raw LCOV for `crates/lexongraph-search/src/lib.rs`: `443 / 571 = 77.58%`
- total normalized candidates: `9`
- excluded candidates: `6`
- inconclusive candidates: `0`
- classified findings: `0`

## Phase 2 Candidate Ledger

### Significant candidates carried forward

| Candidate | Module location | Coverage kind | Behavioral unit |
|---|---|---|---|
| CG-004 | `src/lib.rs:520` | no hits | reverse leaf/branch comparator arm in canonical tie-breaking |
| CG-008 | `src/lib.rs:814-843` | no hits | embedding-compatibility helper expectation-failure branches |
| CG-009 | `src/lib.rs:863-931` | no hits | candidate-scorer helper expectation-failure branches |

### Exclusions

| Candidate | Module location | Coverage kind | Rationale |
|---|---|---|---|
| CG-001 | `src/lib.rs:138-164` | no hits | `DefaultPolicyError` `Display` wording only; the spec makes failures explicit but does not make exact error strings contractual |
| CG-002 | `src/lib.rs:246-306` | no hits | `SearchError` `Display` and `Error::source` branches are diagnostic/error-chaining detail rather than distinct required product behavior |
| CG-003 | `src/lib.rs:367` | no hits | internal invariant panic guarded by `frontier.iter().take(n).all(SearchCandidate::is_leaf)` in the same function |
| CG-005 | `src/lib.rs:681-683` | no hits | duplicate unsupported-encoding arm inside `cosine_similarity_bytes`; unsupported encodings already fail earlier through `element_width` and input validation |
| CG-006 | `src/lib.rs:720-722` | no hits | `usize::try_from` overflow branch in `expected_byte_len`; defensive 64-bit width guard, not a separately specified search behavior |
| CG-007 | `src/lib.rs:749-755` | no hits | `ConformanceError` `Display` wording only |

## Phase 3 Trace Matrix

| Candidate | Coverage region | Spec trace | Validation trace | Test trace | Result |
|---|---|---|---|---|---|
| CG-004 | `crates/lexongraph-search/src/lib.rs:520` | `docs/protocol/search.md:82-98`; `docs/specs/rust-search-crate/validation.md:221-232` | `VAL-SEARCH-019` requires canonical ordering for branch/leaf and ID ties | `crates/lexongraph-search/tests/spec_validation.rs:589-673` | traced |
| CG-008 | `crates/lexongraph-search/src/lib.rs:814-843` | `docs/specs/rust-search-crate/requirements.md:146-181`; `docs/specs/rust-search-crate/design.md:274-307` | `VAL-SEARCH-016`, `VAL-SEARCH-018` define reusable helper behavior plus representative helper-owned expectation failures | `crates/lexongraph-search/tests/conformance_feature.rs:399-440` | traced |
| CG-009 | `crates/lexongraph-search/src/lib.rs:863-931` | `docs/specs/rust-search-crate/requirements.md:146-181`; `docs/specs/rust-search-crate/design.md:274-307` | `VAL-SEARCH-016`, `VAL-SEARCH-018` define reusable helper behavior plus representative helper-owned expectation failures | `crates/lexongraph-search/tests/conformance_feature.rs:399-440` | traced |

## Findings

No confirmed drift findings remain in the current uncovered set.

## Rejected Candidates

| Candidate | Reason rejected | Exact safe mechanism |
|---|---|---|
| CG-004 | not a drift finding | `VAL-SEARCH-019` is already realized by `crates/lexongraph-search/tests/spec_validation.rs:589-673`, which validates the required observable canonical ordering through the public `Searcher` API. The uncovered reverse comparator arm is an internal branch-shape artifact of `sort_by(compare_candidates)` rather than a missing acceptance criterion. |
| CG-008 | not a drift finding | `REQ-SEARCH-015` and `VAL-SEARCH-016` require reusable helper checks plus **representative** direct checks of helper-owned expectation failures (`docs/specs/rust-search-crate/requirements.md:164-166`, `docs/specs/rust-search-crate/validation.md:192-218`). `crates/lexongraph-search/tests/conformance_feature.rs:425-431` already exercises one representative helper-owned expectation failure for the embedding-compatibility suite. The remaining uncovered branches are extra diagnostic branches, not untraced required behavior. |
| CG-009 | not a drift finding | `REQ-SEARCH-015` and `VAL-SEARCH-016` require the scorer helper to accept conforming implementations, reject violating fixtures, detect nondeterminism, verify repeated-input stability, verify preferred-over-alternate ordering, and expose representative helper-owned expectation failures. `crates/lexongraph-search/tests/conformance_feature.rs:435-440` provides that representative failure-path check, while the rest of `run_candidate_scorer_suite` is already covered by the conformance success-path tests. The remaining uncovered branches are additional helper diagnostics rather than missing specification or validation coverage. |

## Finding Distribution

| Drift category | Count |
|---|---|
| D2_UNTESTED_REQUIREMENT | 0 |
| D9_UNDOCUMENTED_BEHAVIOR | 0 |
| D11_UNIMPLEMENTED_TEST_CASE | 0 |
| D12_UNTESTED_ACCEPTANCE_CRITERION | 0 |
| D13_ASSERTION_MISMATCH | 0 |

## Dominant Drift Pattern

**No current classified drift in the remaining uncovered set.**

The dominant residual pattern is instead:

1. diagnostic formatting and error-chaining code that is intentionally public but not specification-contractual
2. reusable conformance-helper branches whose diagnostics are only required to be sampled representatively by the validation surface

## Root Cause Analysis

The remaining low coverage is **not explained by the previously reported drift**.
That baseline issue is now specified, validated, and executed.

Coverage remains low because `src/lib.rs` contains a large amount of code whose
value is operational or test-harness oriented rather than contract-defining:

1. public `Display` and `Error::source` implementations for `DefaultPolicyError`,
   `SearchError`, and `ConformanceError`
2. helper-owned expectation branches in the opt-in conformance surface that
   deliberately expose many distinct failure messages, while the spec asks only
   for representative direct checks of those failures
3. a few internal defensive branches (`unreachable!`, duplicate unsupported
   encoding guard, architecture-width overflow guard) that are either proven
   inactive in this audit target or already subsumed by earlier validation

The key distinction is that the **specified failure behaviors are being hit in
tests**, but the **remaining uncovered lines are mostly not those behaviors
themselves**.

- `crates/lexongraph-search/tests/spec_validation.rs:748-1068` exercises the
  meaningful failure outcomes for the runtime and default policies, including
  `SearchError::InvalidTraversalWidth`, `SearchError::ScoringFailure`,
  `DefaultPolicyError::IncompatibleEmbeddingSpec`,
  `DefaultPolicyError::UnsupportedEncoding`,
  `DefaultPolicyError::InvalidByteLength`,
  `DefaultPolicyError::ZeroMagnitude`,
  `DefaultPolicyError::NonFiniteValue`,
  `DefaultPolicyError::DimensionOverflow`, and
  `DefaultPolicyError::NonFiniteScore`.
- the still-uncovered `SearchError` and `DefaultPolicyError` regions at
  `crates/lexongraph-search/src/lib.rs:130-165` and
  `crates/lexongraph-search/src/lib.rs:245-306` are the `Display` and
  `Error::source` implementations; the tests primarily use `unwrap_err()`,
  `matches!`, and `assert_eq!` on the enum values, which proves the error
  category but does not execute `error.to_string()` or `error.source()`
- the still-uncovered conformance-helper regions at
  `crates/lexongraph-search/src/lib.rs:814-843` and
  `crates/lexongraph-search/src/lib.rs:863-931` are alternate
  helper-owned expectation-failure branches; the repository intentionally hits
  representative failures in
  `crates/lexongraph-search/tests/conformance_feature.rs:425-440`, but not
  every possible helper diagnostic string

So the residual coverage gap is best explained as **specified behavior with
representative validation plus non-contractual diagnostics**, not as current
spec drift.

## Why this is not a false positive

This conclusion survived an adversarial pass:

- the prior documented drift at line 94 no longer appears as an uncovered
  candidate, and the repository now has a complete evidence path from
  implementation to requirements, design, validation, and test
- the remaining carried-forward candidates each map either to an already
  realized validation case (`CG-004`) or to specification text that explicitly
  permits representative, not exhaustive, helper-failure checks (`CG-008`,
  `CG-009`)
- no remaining uncovered candidate exposed a public runtime behavior that lacked
  governing requirements or a validation route

## Recommended Next Action

If the goal is **higher coverage**, the next work is a coverage policy choice,
not a spec-drift repair:

1. add targeted tests for diagnostic formatting and additional helper-owned expectation branches, accepting that this raises coverage without materially changing contractual assurance, or
2. leave coverage as-is and treat the remaining gap as an expected artifact of representative validation and public diagnostic surfaces

If the goal is **spec health**, no follow-up spec repair is indicated by this
audit.

## Scope Limitation

This audit examined **uncovered regions only** in
`crates/lexongraph-search/src/lib.rs`. It does **not** clear covered code for
compliance with the LexonGraph protocol or the `rust-search-crate`
specification package.
