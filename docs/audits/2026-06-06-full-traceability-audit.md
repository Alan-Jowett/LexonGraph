<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# 2026-06-06 Full Traceability Audit

## Scope

This audit reviewed the active LexonGraph artifact stack across:

- `docs/arch/`
- `docs/protocol/`
- `docs/specs/`
- `crates/`
- crate verification artifacts under `crates/*/tests/`

The audit goal was to identify behavior present in one layer but absent from
the others, classify the resulting drift, and define corrective actions for the
active governed surface.

## Coverage Summary

| Metric | Result | Notes |
| --- | ---: | --- |
| Code/config -> requirements traceability | 100.0% | All active crates plus CI/dependabot automation map to governing requirement packages |
| Requirements -> design -> validation completeness | 96.1% before fixes | 247 / 257 requirements fully traced before corrective patches |
| Arch/protocol docs with downstream spec linkage | 75.0% | 6 / 8 docs actively linked into subordinate spec packages |

## Findings

| ID | Category | Severity | Summary | Classification |
| --- | --- | --- | --- | --- |
| F-001 | D3 | Medium | `docs/protocol/ebcp.md` is an orphan protocol document with no active subordinate spec or implementation surface | defer |
| F-002 | D3 | Medium | `docs/arch/semantic-compression.md` is an orphan architecture note with no active subordinate spec or implementation surface | defer |
| F-003 | D1 | Medium | `REQ-BLOCK-CRATE-008` and `REQ-BLOCK-CRATE-014` were missing from the block-crate design trace table | fix-spec |
| F-004 | D2 | Medium | `REQ-BLOCK-CRATE-001` was missing from block-crate validation traceability | fix-spec |
| F-005 | D2 | Medium | `REQ-EMBED-TRAIT-016` was missing from embeddings-trait validation traceability | fix-spec |
| F-006 | D2 | Medium | PCA validation traceability lagged existing requirements, design, code, and tests | fix-spec |
| F-007 | D2 | Medium | `REQ-CI-011` lacked validation coverage for the `Cargo.lock` SPDX exclusion boundary | fix-spec |
| F-008 | D9 | Medium | Empty-batch embedding behavior existed in code but was not fully specified or verified | fix-both |

## Classification Summary

### Deferred

- **F-001**: `docs/protocol/ebcp.md` remains future protocol work and is not
  part of the active governed implementation surface in this maintenance pass.
- **F-002**: `docs/arch/semantic-compression.md` remains future architecture
  work and is not part of the active governed implementation surface in this
  maintenance pass.

### Corrective Actions

| Finding | Change IDs | Corrective scope |
| --- | --- | --- |
| F-003 | CHG-001 | Repair block-crate design traceability |
| F-004 | CHG-002 | Repair block-crate validation traceability |
| F-005 | CHG-003 | Repair embeddings-trait validation traceability |
| F-006 | CHG-004 | Repair PCA validation traceability without adding speculative validation IDs |
| F-007 | CHG-005 | Add CI validation coverage for governed-file exclusion rules |
| F-008 | CHG-006, CHG-007 | Specify and verify empty-batch embedding behavior in the shared trait and OpenAI provider surfaces |

## Corrective Patch Summary

| Change ID | Files | Summary |
| --- | --- | --- |
| CHG-001 | `docs/specs/rust-block-crate/design.md` | Add missing requirement coverage for `REQ-BLOCK-CRATE-008` and `REQ-BLOCK-CRATE-014` |
| CHG-002 | `docs/specs/rust-block-crate/validation.md` | Add missing validation trace for `REQ-BLOCK-CRATE-001` |
| CHG-003 | `docs/specs/rust-embeddings-trait/validation.md` | Add missing validation trace for `REQ-EMBED-TRAIT-016` |
| CHG-004 | `docs/specs/rust-pca-crate/validation.md` | Expand existing PCA validation traces to cover missing requirements without inventing a new validation surface |
| CHG-005 | `docs/specs/rust-workspace-ci/validation.md` | Add validation coverage for the generated-file SPDX exclusion boundary |
| CHG-006 | `docs/specs/rust-embeddings-trait/*`, `crates/lexongraph-embeddings-trait/tests/spec_validation.rs` | Specify and test shared empty-batch embedding semantics |
| CHG-007 | `docs/specs/rust-embeddings-openai-crate/*`, `crates/lexongraph-embeddings-openai/tests/spec_validation.rs` | Specify and test provider empty-batch embedding semantics |

## Resulting State

After applying the approved corrective changes:

- active `fix-spec` findings are closed by updated traceability documents
- active `fix-both` findings are closed across specification and verification
- deferred future-facing documents remain explicitly out of scope for the
  active governed surface

The expected post-correction steady state is full active-package
requirements-to-design-to-validation coverage for the currently implemented
LexonGraph repository surface.
