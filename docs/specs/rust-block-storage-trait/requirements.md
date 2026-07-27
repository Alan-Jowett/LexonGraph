<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Block Storage Trait Requirements

## Status

Draft specification for a Rust trait crate that defines the storage-layer
contract for LexonGraph blocks.

## Scope

This document specifies the crate-level requirements for a Rust crate that
defines the contract between LexonGraph consumers and block-storage
implementations.

This crate is layered on top of `docs/protocol/blocks.md`,
`docs/protocol/blocks-v2.md`, and the `docs/specs/rust-block-crate/`
specification package.

This document does not define block canonicalization, block validation, or
block-ID derivation rules. Those remain owned by the block protocol and block
crate.

## Terminology

In this spec package, `block identifier`, `block ID`, and `block hash` refer to
the same protocol-defined value: `sha256(canonical_cbor_bytes(block))`.

`Block storage backend` means any concrete persistence layer capable of storing
and retrieving blocks by block ID, including filesystem, sqlite, Azure Blob,
S3, or similar systems.

## Requirements

### REQ-BLOCK-STORE-001

The crate shall define the backend-agnostic contract between LexonGraph
consumers and block-storage backends, including immutable block persistence,
optional batch persistence, retrieval, and streaming enumeration of stored
block identifiers.

### REQ-BLOCK-STORE-002

The crate shall use typed block-crate values and protocol-defined block
identifiers at its public API boundary.

### REQ-BLOCK-STORE-003

The crate shall expose a `put` operation that stores a block immutably and
returns its block ID.

### REQ-BLOCK-STORE-004

The crate shall expose a `get` operation that retrieves a block by block ID and
returns explicit absence when that block is not present.

### REQ-BLOCK-STORE-005

The crate shall only treat `get` as successful when the retrieved content
conforms to the requested block ID under the protocol's content-addressed
rules.

### REQ-BLOCK-STORE-006

The crate shall make `put` idempotent for logically identical blocks.

### REQ-BLOCK-STORE-007

The crate shall preserve a backend-neutral contract that does not expose
filesystem paths, SQL schemas, blob prefixes, bucket layout, or similar
backend-specific addressing details to consumers.

### REQ-BLOCK-STORE-008

The crate shall surface explicit failures for backend access, malformed stored
content, and block-identity mismatch, and shall not silently treat those
conditions as successful retrieval.

### REQ-BLOCK-STORE-009

The crate shall not include production storage backend implementations.

### REQ-BLOCK-STORE-010

The crate may include an internal-only memory-backed implementation solely to
support tests for the crate's contract.

### REQ-BLOCK-STORE-011

The crate shall not own search traversal behavior, indexing strategy, block
canonicalization policy, wire-format policy, or protocol evolution rules.

### REQ-BLOCK-STORE-012

The crate shall provide a reusable conformance-test harness that downstream
`BlockStore` implementers can invoke from their own test suites to verify the
required `put`/`get`/enumeration contract semantics.

### REQ-BLOCK-STORE-013

The reusable conformance-test harness shall be exposed through an opt-in,
non-default test-oriented surface so downstream implementers can use it in
tests without broadening the crate's default production-facing API.

### REQ-BLOCK-STORE-014

The crate shall expose a backend-neutral streaming enumeration surface that
permits callers to observe stored block identifiers without first materializing
the full identifier set in memory.

### REQ-BLOCK-STORE-015

The enumeration surface shall yield block identifiers only.

It shall not require the storage trait to classify identifiers as leaf blocks,
branch blocks, roots, levels, clusters, or reachable subgraphs.

### REQ-BLOCK-STORE-016

The enumeration surface shall preserve a backend-neutral contract that does not
expose filesystem paths, SQL schemas, blob prefixes, bucket layout, or similar
backend-specific listing details to consumers.

### REQ-BLOCK-STORE-017

Enumeration shall surface explicit backend failures encountered before or during
streaming and shall not silently skip unreadable or otherwise unlistable stored
state as though enumeration were complete.

### REQ-BLOCK-STORE-018

This revision shall not require enumeration ordering, snapshot isolation,
reachability filtering, root detection, leaf or branch classification, or
traversal semantics.

### REQ-BLOCK-STORE-019

The production storage trait boundary shall be protocol-agnostic and shall
persist canonical block bytes keyed by a caller-supplied block identifier.

### REQ-BLOCK-STORE-020

The production storage trait shall provide raw-byte retrieval by block
identifier and shall not require concrete backends to parse or validate a block
protocol version.

### REQ-BLOCK-STORE-021

Shared typed and version-aware block helpers may be layered above the raw-byte
storage trait so callers can store, batch-store, and load version-1 or
version-2 blocks without duplicating codec-selection logic in each backend
implementation.

### REQ-BLOCK-STORE-022

Concrete block-store backends should remain unaware of whether stored bytes
encode a version-1 branch block, a version-2 reserved block, or a version-2
custom block.

### REQ-BLOCK-STORE-023

The production storage trait shall expose an additional batch-write operation
over caller-supplied block identifiers and canonical block bytes.

### REQ-BLOCK-STORE-024

The batch-write operation shall remain optional for concrete backends.

Backends that do not opt in shall fail explicitly when the operation is
invoked, and shall not silently report success or partially emulate the batch
contract through implicit best-effort fallback behavior.

### REQ-BLOCK-STORE-025

Backends that opt in to the batch-write operation shall provide atomic
all-or-nothing semantics.

If any batch entry cannot be accepted because of backend failure, conflicting
already-persisted bytes, malformed batch input relative to the shared contract,
or other explicit contract failure, none of the entries in that batch operation
shall become committed by that operation.

### REQ-BLOCK-STORE-026

For backends that opt in, the batch-write operation shall preserve the same
backend-neutrality, immutability, idempotence, and explicit-failure rules that
apply to single-entry writes.

### REQ-BLOCK-STORE-027

The trait crate shall define a backend-neutral telemetry event type suitable for
observational status reporting by block-store implementations and related helper
surfaces.

### REQ-BLOCK-STORE-028

The shared storage contract shall define an optional telemetry callback
capability that callers may provide to observe store events.

Absence of a telemetry callback shall leave store behavior unchanged.

### REQ-BLOCK-STORE-029

The telemetry callback capability shall be observational only.

In this revision, a callback shall not control, cancel, mutate, or otherwise
steer backend behavior.

### REQ-BLOCK-STORE-030

The shared telemetry event representation shall remain generic rather than
backend-native.

At minimum it shall support:

- an event name
- an optional human-readable message
- structured name/value attributes

### REQ-BLOCK-STORE-031

Concrete backends are not required to emit telemetry in this revision merely
because the shared capability exists.

Backends that do not opt in remain conformant without synthesizing placeholder
events.

### REQ-BLOCK-STORE-032

When a backend or shared helper emits telemetry through the shared capability,
that emission shall not require callers to depend on backend-native types,
storage layout details, or transport-specific status objects.

## Out of Scope

This crate does not define or own:

- block canonicalization or wire encoding
- block-ID derivation rules
- indexing tree construction strategy
- search traversal or ranking behavior
- backend deletion or query APIs
- production filesystem, sqlite, Azure Blob, S3, or other concrete backend
  implementations

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/blocks.md` for block identity,
wire format, canonicalization, and validity rules, together with
`docs/protocol/blocks-v2.md` for the version-2 envelope.

This document is also subordinate to the `docs/specs/rust-block-crate/`
specification package for typed block modeling and block verification behavior.

If this document appears to conflict with either of those authorities, they are
authoritative for their respective concerns.
