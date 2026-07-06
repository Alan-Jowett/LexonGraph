<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Azure Table Block Store Requirements

## Status

Draft specification for a Rust crate that implements the LexonGraph
block-storage contract on Azure Table Storage using a table SAS URL.

## Scope

This document specifies implementation-specific requirements for a Rust crate
that realizes `docs/specs/rust-block-storage-trait/` on Azure Table Storage.

This document is layered on top of:

- `docs/protocol/blocks.md`
- `docs/specs/rust-block-crate/`
- `docs/specs/rust-block-storage-trait/`

This document does not repeat or redefine the parent trait contract. It adds
only Azure Table-specific requirements needed to realize that contract in this
repository.

## Terminology

In this spec package, `table SAS URL` means a caller-supplied Azure Table
Storage URL that addresses a specific table root and carries one or more
shared-access-signature query parameters, including a non-empty signature
parameter.

`Recognized block-entity candidate` means an Azure Table entity whose key shape
matches the deterministic block layout for this crate:

- `PartitionKey`: the first four lowercase hexadecimal characters of the block
  ID
- `RowKey`: the full lowercase hexadecimal block ID

A recognized block-entity candidate becomes a recognized block entity only when
the `RowKey` is a full valid lowercase block ID and the `PartitionKey` matches
the first four lowercase hexadecimal characters of that block ID.

A candidate whose key shape matches the deterministic layout but whose
`PartitionKey` does not match the decoded block ID remains a malformed
recognized block-entity candidate rather than unrelated content.

## Requirements

### REQ-AZURE-TABLE-STORE-001

The Azure Table block-store crate shall remain subordinate to
`docs/protocol/blocks.md`, `docs/specs/rust-block-crate/`, and
`docs/specs/rust-block-storage-trait/` for block identity, validation, and the
backend-neutral `BlockStore` contract.

### REQ-AZURE-TABLE-STORE-002

The repository shall include a Rust crate named
`lexongraph-block-store-azure-table`, separate from
`crates/lexongraph-block-store`, `crates/lexongraph-block-store-azure`, and
`crates/lexongraph-block-store-azure-sdk`, that implements `BlockStore` using a
caller-supplied Azure Table Storage table SAS URL.

### REQ-AZURE-TABLE-STORE-003

Construction shall accept a table SAS URL outside the `BlockStore` trait
boundary.

Construction shall either return an initialized store bound to that table or
fail explicitly as a backend failure when the supplied URL cannot be parsed,
does not address a table root, omits SAS query parameters, omits a non-empty
SAS signature parameter, or cannot be prepared for Azure Table operations.

Construction shall not create the target table, and this revision does not
require construction to preflight table existence or permissions embedded in the
SAS URL.

### REQ-AZURE-TABLE-STORE-004

`put` shall derive the canonical block bytes and block ID through the block
crate and map that block ID to exactly one deterministic Azure Table entity key
within the configured table.

### REQ-AZURE-TABLE-STORE-005

This revision shall use the deterministic entity-key layout:

- `PartitionKey`: first four lowercase hexadecimal characters of the block ID
- `RowKey`: full lowercase hexadecimal block ID

The consumer-facing runtime contract shall not require callers to know that
entity-key layout.

### REQ-AZURE-TABLE-STORE-006

`get` shall return `Ok(None)` when the mapped block entity is absent.

When the mapped block entity is present, `get` shall reconstruct the stored
canonical bytes and validate those bytes against the requested block ID before
reporting success.

`get` shall be total over the mapped entity state:

- present readable valid content for the requested block ID shall return
  `Ok(Some(validated_block))`
- present readable malformed entity payload or protocol-invalid reconstructed
  content shall fail explicitly as malformed content
- present readable content whose verified identity differs from the requested
  block ID shall fail explicitly as an integrity-mismatch condition
- present unreadable or otherwise inaccessible content shall fail explicitly as
  a backend failure

### REQ-AZURE-TABLE-STORE-007

`put` shall attempt a create-without-overwrite publication of the canonical
block bytes to the deterministic entity key, and it shall not overwrite any
previously published entity for that block ID.

If publication observes that the deterministic entity already exists, whether
before or after a concurrent publication race, `put` shall return success.

This success outcome does not require `put` to re-read or re-validate the
existing entity bytes against the requested block ID. Any later `get` remains
responsible for validating that the reconstructed bytes hash to the requested
block ID and shall fail explicitly if they do not.

### REQ-AZURE-TABLE-STORE-008

If `put`, `get`, or identifier enumeration cannot complete because the SAS URL
or backend denies the required operation, the operation shall fail explicitly as
a backend failure.

This explicit failure behavior applies even when construction previously
succeeded.

### REQ-AZURE-TABLE-STORE-009

Concurrent publishers of the same logical block to the same Azure table may
race, but the implementation shall converge on one valid published entity for
that block ID.

### REQ-AZURE-TABLE-STORE-010

The Azure Table-backed implementation shall implement the parent trait's
streaming block-ID enumeration over recognized block entities rooted at the
configured table.

### REQ-AZURE-TABLE-STORE-011

Azure Table enumeration shall expose only block identifiers at the parent trait
boundary and shall not expose table URLs, partition keys, row keys, filters, or
other Azure-specific addressing details.

### REQ-AZURE-TABLE-STORE-012

Azure Table enumeration shall report only recognized block entities.

It shall not report unrelated entities or other table artifacts as stored block
IDs.

### REQ-AZURE-TABLE-STORE-013

Azure Table enumeration shall surface explicit backend failure when entity
listing, payload inspection, or decoding of a recognized block-entity candidate
into a valid block ID cannot be completed.

This explicit failure rule includes malformed candidate keys and shard-prefix
mismatches within the deterministic candidate layout.

### REQ-AZURE-TABLE-STORE-014

This revision shall store each logical block entirely within one Azure Table
entity.

The implementation shall not fragment one logical block across multiple Azure
Table entities and shall not silently fall back to a different backend for
oversized blocks.

### REQ-AZURE-TABLE-STORE-015

`put` shall fail explicitly before publication when the canonical block bytes
and required storage metadata, encoded using this revision's single-entity
multi-property representation, cannot fit within one Azure Table entity under
the documented Azure Table service limits applicable to this revision,
including the per-entity size limit and per-property binary payload limit for
that encoded representation.

`put` shall not reject a block solely because its canonical bytes exceed the
per-property binary payload limit when those bytes can still be represented
within one entity by this revision's deterministic multi-property encoding.

### REQ-AZURE-TABLE-STORE-016

The repository shall provide a dedicated opt-in live integration-verification
mode for `lexongraph-block-store-azure-table` that exercises the crate against a
real Azure Table Storage table.

The live verification mode shall remain outside the default local and workspace
test path so contributors and routine non-Azure verification do not require
live Azure credentials.

### REQ-AZURE-TABLE-STORE-017

The live Azure verification mode shall prove the real-backend wiring needed for
the shared `BlockStore` contract by exercising:

- construction from a valid table SAS URL
- successful publication of a valid block through `put`
- successful retrieval of that block through `get`
- `Ok(None)` for a block whose mapped entity is absent
- streaming block-ID enumeration for blocks published by the test

This requirement does not replace the existing mock-backed verification surface
for malformed backend responses, permission failures, or other backend-simulated
cases that are impractical to make deterministic against a live subscription in
routine CI.

### REQ-AZURE-TABLE-STORE-018

If Azure publish transport fails before `put` receives a backend response, the
implementation shall retry that same deterministic insert request using a
bounded retry policy.

If a later retry reaches a backend response, `put` shall continue applying the
same success, idempotence, already-published, and explicit-failure rules that
govern a single publish attempt.

If the bounded retry policy is exhausted without any publish attempt reaching a
backend response, `put` shall fail explicitly as a backend failure and shall
not report success for that block ID.

### REQ-AZURE-TABLE-STORE-019

If Azure entity-read or table-query transport fails before `get` or identifier
enumeration receives a backend response, the implementation shall retry that
same read or query request using a bounded retry policy.

If a later retry reaches a backend response, `get` and identifier enumeration
shall continue applying their normal absence, decode, filtering, and explicit-
failure rules for that response.

For paginated identifier enumeration, a transport failure after one or more
pages have already been received shall retry the specific failed page request
using the existing continuation state rather than restarting enumeration from
the beginning.

If the bounded retry policy is exhausted without any read or query attempt
reaching a backend response, the operation shall fail explicitly as a backend
failure and shall not report success or absence for the affected state.

### REQ-AZURE-TABLE-STORE-020

The repository shall provide a mock-backed verification surface for
`lexongraph-block-store-azure-table` that can simulate Azure publish, read, and
query outcomes; inject malformed or integrity-mismatched recognized block
entities; and observe constructor behavior without requiring a live Azure table.

This mock-backed verification surface shall remain internal or test-only and
shall not broaden the public production `BlockStore` API boundary.

## Out of Scope

This crate does not define or own:

- block canonicalization, block validity, or block-ID derivation rules
- changes to the parent `BlockStore` API
- consumer-visible Azure-specific table layout beyond the inherited block-ID
  contract
- deletion, mutation, compaction, lease management, or lifecycle policy
- automatic table creation or other IaC concerns
- multi-entity block fragmentation in this revision
- non-Azure storage backends

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/blocks.md`.

This document is also subordinate to the `docs/specs/rust-block-crate/` and
`docs/specs/rust-block-storage-trait/` specification packages for their
respective concerns.

If this document appears to conflict with those authorities, they are
authoritative for their owned concerns.
