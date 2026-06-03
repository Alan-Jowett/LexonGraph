<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# RCA: Filesystem Block Store Spec-First Drift

## Status

Analysis of why the spec-first model failed to fully constrain the negative-path
behavior of `crates/lexongraph-block-store-fs`.

## Summary

Coverage analysis of `crates/lexongraph-block-store-fs/src/lib.rs` showed that
the low coverage is concentrated in negative-path logic rather than in
feature-gated code or formatting-only branches.

The central failure was not that the repository lacked a spec package. The
failure was that the spec package mostly described the happy-path sequence of
operations, but did not define the required behavior when intermediate steps
failed.

That left important behavior underconstrained, especially for filesystem error
handling, and created drift risk between:

- requirements
- design
- validation
- implementation

## Impact

The immediate impact is incomplete validation of behavior that is externally
meaningful to callers of the filesystem block store.

Specifically, the implementation currently contains behavior for:

- constructor failure
- unreadable mapped files during `get`
- staging failures during `put`
- publish-failure recovery and post-failure inspection

but those behaviors are not fully defined in the requirements and validation
layers.

This means:

1. another implementation could make different choices while still appearing to
   satisfy the current spec
2. coverage gaps cluster in precisely the regions where the spec is weakest
3. the codebase can drift while still looking superficially spec-first

## Evidence

### Coverage

Coverage was gathered with `cargo llvm-cov` for
`crates/lexongraph-block-store-fs/src/lib.rs`.

- default features: `75 / 117 = 64.10%`
- all features: `75 / 117 = 64.10%`

`--all-features` did not change the result, which shows that the gap is not
caused by feature-gated surfaces.

### Uncovered regions

The main uncovered regions were:

- `src/lib.rs:21-48` — constructor negative paths
- `src/lib.rs:65-91` — publish failure recovery and re-inspection
- `src/lib.rs:102-132` — directory creation, temp-file creation, write, and
  flush failures during `put`
- `src/lib.rs:147-155` — present-but-unreadable file handling during `get`

These are not incidental branches. They determine whether the backend returns:

- success
- absence
- integrity failure
- malformed-content failure
- backend failure

## Root Cause

The root cause was an incomplete spec-first contract:

> The spec said "do X, then Y", but it did not say what must happen if X fails.

In other words, the spec described sequencing, but not total operational
behavior.

That omission matters because filesystem-backed code is dominated by negative
paths:

- path creation can fail
- canonicalization can fail
- metadata lookup can fail
- file creation can fail
- writes can fail
- flush can fail
- rename/persist can fail
- a retry/read-after-failure can fail differently from the original operation

Without explicit negative-path requirements, the implementation had to invent
policy at each branch point.

## Contributing Factors

### 1. Design was stronger than requirements

`docs/specs/rust-filesystem-block-store/design.md` already implied stronger
error mapping than the requirements document made explicit, especially in
`DSG-FS-STORE-010`.

This meant the design partially compensated for requirement gaps, but the
overall spec package was still not fully deterministic.

### 2. Validation focused on representative happy paths

The validation package covered:

- deterministic published path mapping
- absent-file `get`
- integrity mismatch
- malformed content
- conflicting existing bytes
- atomic visibility
- same-block concurrent convergence

Those are important, but they do not cover the negative paths where the
operation starts correctly and fails partway through.

### 3. Filesystem behavior is branch-heavy

Filesystem implementations naturally accumulate many explicit failure branches.
If the spec defines only the nominal flow, large parts of the implementation
become implementation-defined by default.

## What Went Wrong with the Spec-First Model

The problem was not that the repository lacked specs.

The problem was that the specs were not **total**. They defined:

- what operations exist
- what success looks like
- some obvious failure modes

but they did not define:

- what to return when setup fails
- what to return when publication fails after staging succeeds
- what to return when inspection after failure produces a second failure
- what to return when the mapped file exists but cannot be read

That is the precise point where the spec-first model broke down: the model
assumed that describing the happy path plus a few named failures was enough to
constrain the implementation.

For filesystem code, it was not.

## Corrective Framing

The right repair is not "add tests until coverage goes up."

The right repair is:

1. define negative-path behavior explicitly in the requirements
2. make the design describe the required outcome of each failure point
3. make validation prove those failure semantics
4. only then use coverage to confirm that the specified behavior is exercised

## Required Spec Repairs

### Construction boundary

The spec should define the required outcome when:

- the store root cannot be created
- the store root cannot be canonicalized
- the store root cannot be stat'ed
- the resolved path is not a directory

### `get` behavior

The spec should define the required outcome for:

- absent file
- present readable valid file
- present readable malformed file
- present readable mismatched file
- present unreadable file

### `put` behavior

The spec should define the required outcome when:

- parent directory creation fails
- staging file creation fails
- staged write fails
- staged flush fails
- atomic publication fails
- post-publication inspection finds matching bytes
- post-publication inspection finds differing bytes
- post-publication inspection finds no file
- post-publication inspection itself fails

## Recommended Validation Repairs

Add executable validation for:

1. invalid or non-directory constructor roots
2. present-but-unreadable files during `get`
3. staging create/write/flush failures during `put`
4. publish failure followed by missing target
5. publish failure followed by unreadable target

## Lessons Learned

1. Happy-path sequencing is not enough for a spec-first process.
2. Negative paths are part of the contract, not just implementation detail.
3. Coverage is useful when read as a drift detector, not just as a score.
4. If design is more precise than requirements, the package is still at risk of
   drift.
5. For IO-heavy code, the spec must define outcomes at each failure boundary.

## Follow-Up

The next repair pass for `lexongraph-block-store-fs` should:

1. update `docs/specs/rust-filesystem-block-store/requirements.md`
2. update `docs/specs/rust-filesystem-block-store/design.md`
3. update `docs/specs/rust-filesystem-block-store/validation.md`
4. add tests for the negative-path semantics listed above
5. re-run coverage and remap the remaining uncovered lines
