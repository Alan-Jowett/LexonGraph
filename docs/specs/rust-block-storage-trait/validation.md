<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Block Storage Trait Validation

## Status

Draft validation specification for a Rust trait crate that defines the
LexonGraph block-storage contract.

## Validation Scope

These validation entries define the minimum conformance surface for a crate that
implements the requirements and design in this spec package.

Protocol-validity and block-identity expectations referenced here remain
normatively defined by `docs/protocol/blocks.md` and the
`docs/specs/rust-block-crate/` specification package.

## Validation Entries

### VAL-STORE-001

Store a valid typed block through `put`, then retrieve it through `get` using
the returned block ID.

**Pass condition:** `get` succeeds and returns a validated typed block with the
same logical meaning and block ID.

**Traces to:** REQ-BLOCK-STORE-002, REQ-BLOCK-STORE-003, REQ-BLOCK-STORE-004,
REQ-BLOCK-STORE-005

### VAL-STORE-002

Store the same logical block multiple times through `put`.

**Pass condition:** each call returns the same block ID and the store does not
present divergent content under that identifier.

**Traces to:** REQ-BLOCK-STORE-003, REQ-BLOCK-STORE-006

### VAL-STORE-003

Request a block ID that is not present in the store.

**Pass condition:** `get` returns `Ok(None)` for the missing block ID, rather
than returning a block or reporting an integrity or backend failure.

**Traces to:** REQ-BLOCK-STORE-004, REQ-BLOCK-STORE-008

### VAL-STORE-004

Attempt to retrieve stored content whose verified identity does not match the
requested block ID.

**Pass condition:** `get` fails explicitly with an integrity error.

**Traces to:** REQ-BLOCK-STORE-005, REQ-BLOCK-STORE-008

### VAL-STORE-005

Attempt to retrieve malformed or protocol-invalid stored content for a
requested block ID.

**Pass condition:** `get` fails explicitly and does not report success or
absence.

**Traces to:** REQ-BLOCK-STORE-005, REQ-BLOCK-STORE-008

### VAL-STORE-006

Use the trait from an indexing consumer to persist typed blocks and consume the
returned block IDs for parent-link construction.

**Pass condition:** the consumer can persist blocks without depending on
backend-specific addressing or storage semantics.

**Traces to:** REQ-BLOCK-STORE-001, REQ-BLOCK-STORE-003, REQ-BLOCK-STORE-007,
REQ-BLOCK-STORE-011

### VAL-STORE-007

Use the trait from a search consumer to resolve a root block ID and child block
IDs into validated typed blocks.

**Pass condition:** the consumer can load required blocks without depending on
backend-specific addressing or retrieval semantics.

**Traces to:** REQ-BLOCK-STORE-001, REQ-BLOCK-STORE-004, REQ-BLOCK-STORE-007,
REQ-BLOCK-STORE-011

### VAL-STORE-008

Evaluate the trait contract against distinct backend classes such as
filesystem, sqlite, Azure Blob, S3, or similar content-addressed stores.

**Pass condition:** the same `put`/`get` contract remains applicable without
changing the consumer-facing API.

**Traces to:** REQ-BLOCK-STORE-001, REQ-BLOCK-STORE-007

### VAL-STORE-009

Run the crate's contract tests against the internal-only memory-backed
implementation.

**Pass condition:** the internal implementation is sufficient to verify the
trait semantics while remaining non-public as a production backend surface.

**Traces to:** REQ-BLOCK-STORE-010

### VAL-STORE-010

Inspect the crate's public surface.

**Pass condition:** the crate's default public surface exposes the storage
contract and related public types only, does not expose concrete production
backend implementations, and keeps any implementer-facing conformance helper
behind an opt-in non-default test-oriented surface.

**Traces to:** REQ-BLOCK-STORE-009, REQ-BLOCK-STORE-010,
REQ-BLOCK-STORE-013

### VAL-STORE-011

Use the crate's opt-in conformance-test helper surface from a downstream crate
that implements `BlockStore`.

**Pass condition:** the downstream crate can depend on the helper surface in
tests and run the shared conformance checks without changing the default
production-facing API of the trait crate.

**Traces to:** REQ-BLOCK-STORE-012, REQ-BLOCK-STORE-013

### VAL-STORE-012

Run the downstream conformance harness against a backend under test while
supplying test-only hooks for corruption scenarios.

**Pass condition:** the shared harness can verify round-trip, idempotence,
absence, integrity-mismatch, and malformed-content behavior without requiring
backend-specific methods on the production `BlockStore` trait.

**Traces to:** REQ-BLOCK-STORE-005, REQ-BLOCK-STORE-008,
REQ-BLOCK-STORE-012, REQ-BLOCK-STORE-013

