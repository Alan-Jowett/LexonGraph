<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# RCA: OpenAI Embeddings Crate Coverage-Driven Spec Drift

## Status

Coverage-driven audit of `crates/lexongraph-embeddings-openai/src/lib.rs`
against the `docs/specs/rust-embeddings-openai-crate/` package.

## Summary

The audited file is at **73.68% line coverage** because the current test suite
proves the main happy path plus the documented local rejection paths, but it
does not exercise two other clusters of public behavior:

1. optional OpenAI request-identity configuration branches
2. response-shape rejection when the endpoint returns anything other than one
   embedding vector

Most of the missing lines are not product-behavior findings. **25 of the 30
uncovered lines** sit in `Display`, `Error::source`, and dependency-error
conversion code for `OpenAiEmbeddingProviderError`. Those branches affect
diagnostic rendering and chained error exposure, but the governing spec package
does not make that wording or source-mapping contractual.

The remaining **5 uncovered lines** expose **two actual traceability gaps**:

- `from_openai_compatible` accepts optional `org_id` and `project_id`
  configuration, but the validation surface never proves that part of the
  request-construction contract
- `embed` exposes an explicit `UnexpectedEmbeddingCount` failure when a
  supposedly single-input response contains anything other than one embedding,
  but the governing spec package does not define that behavior

## Scope Summary

| Surface | Artifact |
|---|---|
| Audited module | `crates/lexongraph-embeddings-openai/src/lib.rs` |
| Governing spec package | `docs/specs/rust-embeddings-openai-crate/requirements.md`, `docs/specs/rust-embeddings-openai-crate/design.md`, `docs/specs/rust-embeddings-openai-crate/validation.md` |
| Protocol documents consulted | None directly; this crate delegates shared semantics to subordinate crate-level specs rather than a `docs/protocol/*.md` document |
| Validation artifacts consulted | `crates/lexongraph-embeddings-openai/tests/spec_validation.rs` |
| Coverage artifact | `lcov.info` filtered to `crates/lexongraph-embeddings-openai/src/lib.rs` |

## Coverage Summary

- raw LCOV for `crates/lexongraph-embeddings-openai/src/lib.rs`: `84 / 114 = 73.68%`
- total normalized candidates: `3`
- excluded candidates: `1`
- inconclusive candidates: `0`
- classified findings: `2`

## Candidate Ledger

### Significant candidates carried forward

| Candidate | Coverage kind | Module location | Behavioral unit |
|---|---|---|---|
| CG-001 | no hits | `src/lib.rs:55-60` | optional OpenAI request-identity branches in `from_openai_compatible` |
| CG-003 | no hits | `src/lib.rs:163-166` | explicit rejection when the endpoint response contains anything other than one embedding |

### Exclusions

| Candidate | Coverage kind | Module location | Rationale |
|---|---|---|---|
| CG-002 | no hits | `src/lib.rs:85-131` | `OpenAiEmbeddingProviderError` `Display`, `Error::source`, and `From<OpenAIError>` coverage only affects diagnostic wording, source chaining, and dependency-error passthrough; no governing requirement makes those details contractual |

## Findings

### F-001

| Field | Value |
|---|---|
| Finding ID | `F-001` |
| Candidate ID | `CG-001` |
| Drift category | `D12_UNTESTED_ACCEPTANCE_CRITERION` |
| Severity | Low |
| Confidence | Medium |
| Module location | `crates/lexongraph-embeddings-openai/src/lib.rs:55-60` |
| Spec locations | `docs/specs/rust-embeddings-openai-crate/requirements.md:37-45`, `docs/specs/rust-embeddings-openai-crate/design.md:51-60`, `docs/specs/rust-embeddings-openai-crate/validation.md:39-47` |
| Validation and test locations | `crates/lexongraph-embeddings-openai/tests/spec_validation.rs:12-47`, `:49-85`, `:96-102`, `:147-153` |

**Evidence**

`OpenAiCompatibleConfig` exposes `org_id` and `project_id` at
`crates/lexongraph-embeddings-openai/src/lib.rs:17-23`, and
`from_openai_compatible` conditionally applies both values at
`crates/lexongraph-embeddings-openai/src/lib.rs:55-60`.

The governing design says the provider configuration surface covers
`an OpenAI-compatible base URL plus model or request identity`
(`docs/specs/rust-embeddings-openai-crate/design.md:53-57`), and the governing
validation entry says the provider shall construct provider-specific requests
using the supplied configuration
(`docs/specs/rust-embeddings-openai-crate/validation.md:39-47`).

The only executable validation for that surface is
`val_embed_oai_003_azure_configuration_targets_azure_style_endpoint` plus the
OpenAI-compatible happy-path test, but every `OpenAiCompatibleConfig` in the
test file sets `org_id: None` and `project_id: None`
(`crates/lexongraph-embeddings-openai/tests/spec_validation.rs:25-31`,
`:96-102`, `:147-153`). The LCOV candidate therefore remained uncovered.

**Why this is not a false positive**

The safest rebuttal would be that request identity is out of scope and only the
base URL/model combination matters. The repository does not support that
rebuttal: `DSG-EMBED-OAI-002` explicitly keeps request identity inside the
provider configuration surface, and the implementation makes both fields public.
Because the validation plan names request construction and the tests never drive
these branches, this is a real acceptance-criterion gap rather than random
uncovered glue code.

**Impact**

Consumers can supply a public configuration shape that the current validation
surface never proves. If the `org_id` or `project_id` wiring regresses, the
crate could still satisfy the present test suite while violating the intended
provider-configuration contract.

**Recommended next action**

Tighten `VAL-EMBED-OAI-003` so it explicitly states whether request-identity
fields are normative in this revision. If they are, add a targeted validation
fixture that exercises non-`None` OpenAI-compatible request identity and checks
the resulting request construction.

### F-002

| Field | Value |
|---|---|
| Finding ID | `F-002` |
| Candidate ID | `CG-003` |
| Drift category | `D9_UNDOCUMENTED_BEHAVIOR` |
| Severity | Medium |
| Confidence | High |
| Module location | `crates/lexongraph-embeddings-openai/src/lib.rs:163-166` |
| Spec locations | None - no governing requirement identified for explicit failure when the endpoint returns zero or multiple embeddings |
| Closest related spec text | `docs/specs/rust-embeddings-openai-crate/requirements.md:52-73`, `docs/specs/rust-embeddings-openai-crate/design.md:82-99`, `docs/specs/rust-embeddings-openai-crate/validation.md:28-37` |
| Validation and test locations | `crates/lexongraph-embeddings-openai/tests/spec_validation.rs:12-47`; no test returns zero or multiple embeddings |

**Evidence**

`embed` rejects any response whose `data.len()` is not exactly `1` and returns
`OpenAiEmbeddingProviderError::UnexpectedEmbeddingCount`
(`crates/lexongraph-embeddings-openai/src/lib.rs:81`, `:103-107`, `:163-166`).

The closest governing texts say:

- the provider uses one embedding input per request path
  (`REQ-EMBED-OAI-005`, `docs/specs/rust-embeddings-openai-crate/requirements.md:52-55`)
- the provider receives one embedding vector and translates it into bytes
  (`DSG-EMBED-OAI-006`, `docs/specs/rust-embeddings-openai-crate/design.md:87-99`)
- the happy-path validation expects one embedding vector from the controlled
  endpoint fixture (`VAL-EMBED-OAI-002`, `docs/specs/rust-embeddings-openai-crate/validation.md:28-37`)

None of those artifacts defines the explicit public failure behavior for a
zero-item or multi-item endpoint response, and the executable validation only
uses a one-vector success body.

**Why this is not a false positive**

The obvious rebuttal is that the single-input request path already implies a
single-output response, so no extra specification is needed. That rebuttal
fails on the repository evidence: the implementation exposes a distinct public
error variant and message for the response-cardinality mismatch, while the
requirements, design, and validation artifacts never say what should happen
when an endpoint violates the one-vector assumption. This is a documented happy
path plus an undocumented defensive failure path.

**Impact**

Another implementation could handle malformed multi-vector responses
differently and still appear conformant to the present spec package. The current
crate already treats this as contract-relevant enough to expose a named public
error variant, but the spec and validation layers do not trace it.

**Recommended next action**

Decide whether malformed response cardinality is contractual behavior.

1. If yes, add a requirement/design statement and a validation case for zero or
   multiple embeddings in the endpoint response.
2. If no, collapse the extra public error surface so the implementation does
   not expose undocumented response-cardinality policy.

## Rejected Candidates

| Candidate | Reason rejected | Exact safe mechanism |
|---|---|---|
| CG-002 | not a drift finding | `tests/spec_validation.rs:89-176` already validates the semantic failure categories required by `VAL-EMBED-OAI-004` and `VAL-EMBED-OAI-005`; the uncovered lines in `src/lib.rs:85-131` only affect diagnostic rendering, source chaining, or dependency-owned request-error passthrough, and the spec package never makes those details normative |

## Finding Distribution

| Drift category | Count |
|---|---|
| D2_UNTESTED_REQUIREMENT | 0 |
| D9_UNDOCUMENTED_BEHAVIOR | 1 |
| D11_UNIMPLEMENTED_TEST_CASE | 0 |
| D12_UNTESTED_ACCEPTANCE_CRITERION | 1 |
| D13_ASSERTION_MISMATCH | 0 |

## Dominant Drift Pattern

**Mixed drift, skewed toward under-specified negative and configuration paths.**

The happy path and the main documented input/spec compatibility failures are
well aligned. Coverage drops because the remaining public behavior sits in
either:

- validation gaps around optional configuration acceptance criteria, or
- defensive error behavior that the implementation exposes but the spec package
  does not define

## Root Cause

The root cause is not simply "missing tests." The deeper issue is that the
specification package is **more precise about nominal request execution than
about boundary behavior**.

It clearly defines:

- successful one-input request execution
- Azure versus OpenAI-compatible endpoint styles
- rejection of non-text, non-UTF-8, unsupported-encoding, and dimensionality
  mismatch inputs

It does **not** equally define:

- whether OpenAI-compatible request identity fields are part of the normative
  configuration contract in this revision
- what the provider must do when the endpoint violates the assumed one-vector
  response shape
- whether request-library error passthrough and error formatting are intended to
  be contractual or incidental

That left the implementation carrying extra public behavior at the edges while
the validation suite stayed centered on the nominal path. Coverage exposed the
gap, but the actual problem is incomplete traceability between requirements,
design, validation, and implementation for those edge behaviors.

## Corrective Framing

The right repair is not "add enough tests to push coverage above 73%."

The right repair is:

1. decide which edge behaviors are normative for this crate's public API
2. state those behaviors explicitly in requirements/design/validation
3. add targeted validation only for the behaviors that remain contractual
4. treat the rest as implementation detail and keep them out of the public
   contract surface

## Scope Limitation

This audit examined **uncovered regions only** in
`crates/lexongraph-embeddings-openai/src/lib.rs`. It does **not** clear covered
code for compliance with the LexonGraph protocol or the
`rust-embeddings-openai-crate` specification package.
