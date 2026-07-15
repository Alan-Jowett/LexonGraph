<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Azure Table Block Store v2 Requirements

## Status

Draft specification for a Rust crate that implements the LexonGraph
block-storage contract on Azure Table Storage using a table SAS URL.

## Scope

This document specifies implementation-specific requirements for a new Rust
crate that realizes `docs/specs/rust-block-storage-trait/` on Azure Table
Storage.

This document is layered on top of:

- `docs/protocol/blocks.md`
- `docs/specs/rust-block-crate/`
- `docs/specs/rust-block-storage-trait/`

This document does not repeat or redefine the parent trait contract. It adds
only Azure Table-specific requirements needed to realize that contract in this
repository for the v2 Azure Table backend line.

## Terminology

In this spec package, `table SAS URL` means a caller-supplied Azure Table
Storage URL that addresses a specific table root and carries one or more
shared-access-signature query parameters, including a non-empty signature
parameter.

`Recognized block-root candidate` means an Azure Table entity whose key shape
matches the deterministic root-row layout for this crate:

- `PartitionKey`: the first four lowercase hexadecimal characters of the block
  ID
- `RowKey`: the full lowercase hexadecimal block ID

A recognized block-root candidate becomes a recognized block root only when the
`RowKey` is a full valid lowercase block ID and the `PartitionKey` matches the
first four lowercase hexadecimal characters of that block ID.

`v2 chunked row-set format` means the multi-row representation owned by this
crate revision:

- one deterministic root row identified directly by the block ID
- zero or more deterministic continuation rows identified by the block ID plus a
  row ordinal suffix
- metadata sufficient to reconstruct the canonical block bytes across the full
  row set
- deterministic payload properties named `chunk0`, `chunk1`, `chunk2`, ... in
  each physical row
- row ordering defined by ascending row ordinal
- intra-row chunk ordering defined by ascending numeric chunk suffix

## Requirements

### REQ-AZURE-TABLE-STORE-V2-001

The Azure Table block-store v2 crate shall remain subordinate to
`docs/protocol/blocks.md`, `docs/specs/rust-block-crate/`, and
`docs/specs/rust-block-storage-trait/` for block identity, validation, and the
backend-neutral `BlockStore` contract.

### REQ-AZURE-TABLE-STORE-V2-002

The repository shall include a Rust crate named
`lexongraph-block-store-azure-table-v2`, separate from
`crates/lexongraph-block-store`, `crates/lexongraph-block-store-azure`,
`crates/lexongraph-block-store-azure-sdk`, and
`crates/lexongraph-block-store-azure-table`, that implements `BlockStore`
using a caller-supplied Azure Table Storage table SAS URL.

The existing `lexongraph-block-store-azure-table` crate remains a distinct
predecessor implementation line, and this revision introduces the v2 backend as
a separate successor crate rather than replacing or renaming the predecessor.

### REQ-AZURE-TABLE-STORE-V2-003

Construction shall accept a table SAS URL outside the `BlockStore` trait
boundary.

Construction shall either return an initialized store bound to that table or
fail explicitly as a backend failure when the supplied URL cannot be parsed,
does not address a table root, omits SAS query parameters, omits a non-empty
SAS signature parameter, or cannot be prepared for Azure Table operations.

Construction shall not create the target table, and this revision does not
require construction to preflight table existence or permissions embedded in the
SAS URL.

### REQ-AZURE-TABLE-STORE-V2-004

`put` shall derive the canonical block bytes and block ID through the block
crate and map that block ID to exactly one deterministic Azure Table row set
within the configured table.

### REQ-AZURE-TABLE-STORE-V2-005

This revision shall use the deterministic row-key layout:

- `PartitionKey`: first four lowercase hexadecimal characters of the block ID
- `RowKey`: full lowercase hexadecimal block ID

For continuation rows in the same logical block row set:

- `PartitionKey`: the same first four lowercase hexadecimal characters of the
  block ID
- `RowKey`: the full lowercase hexadecimal block ID followed by a deterministic
  row ordinal suffix

The consumer-facing runtime contract shall not require callers to know that
row-key layout.

### REQ-AZURE-TABLE-STORE-V2-006

`get` shall return `Ok(None)` when the mapped block root is absent, including
when orphan continuation rows exist without that root row.

When `get` reads a mapped block root or any required continuation row whose
deterministic `PartitionKey` and `RowKey` are already known from the block ID
and row-set metadata, it shall retrieve that row by direct entity-addressed
lookup rather than by issuing a filtered table query for the same point-read
state.

When the mapped block root is present, `get` shall reconstruct the stored
canonical bytes from the v2 chunked row-set format and validate those bytes
against the requested block ID before reporting success.

`get` shall be total over the mapped recognized row-set state:

- present readable valid content for the requested block ID shall return
  `Ok(Some(validated_block))`
- present readable malformed row-set payload, including missing expected
  continuation rows, duplicate or out-of-range row ordinals, missing `chunkN`
  properties within a row, malformed chunk values, malformed cross-row
  metadata, or inconsistent reconstructed byte length, shall fail explicitly as
  malformed content
- present readable content whose verified identity differs from the requested
  block ID shall fail explicitly as an integrity-mismatch condition
- present unreadable or otherwise inaccessible content shall fail explicitly as
  a backend failure

### REQ-AZURE-TABLE-STORE-V2-007

`put` shall attempt a create-without-overwrite publication of the canonical
block bytes to the deterministic row set, and it shall not overwrite any
previously published row in that row set for that block ID.

If publication observes that the deterministic root row already exists, whether
before or after a concurrent publication race, `put` shall return success.

This success outcome does not require `put` to re-read or re-validate the
existing row-set bytes against the requested block ID. Any later `get` remains
responsible for validating that the reconstructed bytes hash to the requested
block ID and shall fail explicitly if they do not.

This revision shall treat the root row as the publication commit point for a
recognized block. Continuation rows written without a corresponding root row do
not constitute a published block and shall not be enumerated as one.

### REQ-AZURE-TABLE-STORE-V2-008

If `put`, `get`, or identifier enumeration cannot complete because the SAS URL
or backend denies the required operation, the operation shall fail explicitly as
a backend failure.

This explicit failure behavior applies even when construction previously
succeeded.

### REQ-AZURE-TABLE-STORE-V2-009

Concurrent publishers of the same logical block to the same Azure table may
race, but the implementation shall converge on one valid published row set for
that block ID.

### REQ-AZURE-TABLE-STORE-V2-010

The Azure Table-backed implementation shall implement the parent trait's
streaming block-ID enumeration over recognized block roots rooted at the
configured table.

### REQ-AZURE-TABLE-STORE-V2-011

Azure Table enumeration shall expose only block identifiers at the parent trait
boundary and shall not expose table URLs, partition keys, row keys, filters, or
other Azure-specific addressing details.

### REQ-AZURE-TABLE-STORE-V2-012

Azure Table enumeration shall report only recognized block roots.

It shall not report unrelated entities or other table artifacts as stored block
IDs.

It shall not report continuation rows as independent stored blocks.

Enumeration shall decide whether to yield a block ID using the recognized
root-row key shape and the required root-row metadata already present in the
table-query response for that root row.

The normal enumeration path shall not fetch continuation rows or perform other
per-block point reads solely to verify row-set completeness before yielding a
recognized block ID.

### REQ-AZURE-TABLE-STORE-V2-013

Azure Table enumeration shall surface explicit backend failure when entity
listing, required root-row metadata inspection, or decoding of a recognized
block-root candidate into a valid block ID cannot be completed.

This explicit failure rule includes malformed candidate keys, shard-prefix
mismatches, and malformed required root metadata encountered during
enumeration.

This revision does not require enumeration to fetch or validate continuation
rows before yielding a recognized block ID. Full row-set validation remains the
responsibility of `get` when a caller materializes block content.

### REQ-AZURE-TABLE-STORE-V2-014

This revision shall store each logical block as one deterministic Azure Table
row set using the v2 chunked row-set format.

The row set shall contain one root row plus zero or more continuation rows.

Small supported blocks may fit entirely within the root row. Larger supported
blocks may span multiple rows, including more rows than any earlier
implementation-specific fixed per-block row-count cap. The implementation shall
not silently fall back to a different backend for oversized blocks.

### REQ-AZURE-TABLE-STORE-V2-015

The v2 chunked row-set format shall use deterministic payload properties named
`chunk0`, `chunk1`, `chunk2`, ... within each row and a chunk ordering defined
by ascending numeric suffix.

The format shall also use deterministic row ordering across the root row and
continuation rows.

For any successful publication, each stored chunk property value shall conform
to the real Azure Table service limits applicable to accepted property values
for this representation.

### REQ-AZURE-TABLE-STORE-V2-016

`put` shall fail explicitly before publication when the canonical block bytes
and required storage metadata, encoded using this revision's v2 chunked row-set
format, cannot fit within the supported deterministic row-set layout under the
documented Azure Table service limits and metadata-encoding limits applicable to
this revision, including the per-row property-count limit, the per-row
entity/property-footprint limit, the accepted per-property value-size limit for
the stored representation, and any metadata field-width limit inherent in the
stored row-set format.

This revision shall not reject an otherwise representable block solely because
it spans more than a fixed implementation-defined number of Azure Table rows.

### REQ-AZURE-TABLE-STORE-V2-017

The repository shall provide a dedicated opt-in live integration-verification
mode for `lexongraph-block-store-azure-table-v2` that exercises the crate
against a real Azure Table Storage table.

The live verification mode shall remain outside the default local and workspace
test path so contributors and routine non-Azure verification do not require
live Azure credentials.

### REQ-AZURE-TABLE-STORE-V2-018

The live Azure verification mode shall prove the real-backend wiring needed for
the shared `BlockStore` contract by exercising:

- construction from a valid table SAS URL
- successful publication of a valid block through `put`
- successful retrieval of that block through `get`
- `Ok(None)` for a block whose mapped root row is absent
- streaming block-ID enumeration for blocks published by the test
- successful publication and retrieval of a block whose canonical bytes require
  more than one `chunkN` property within one row in the v2 chunked row-set
  format
- successful publication and retrieval of a block whose canonical bytes require
  more than one Azure Table row in the v2 chunked row-set format
- idempotent success when re-publishing a block that is already present

### REQ-AZURE-TABLE-STORE-V2-019

If Azure publish transport fails before `put` receives a backend response for a
deterministic row insert, the implementation shall retry that same deterministic
row insert using a bounded retry policy.

If a later retry reaches a backend response, `put` shall continue applying the
same success, idempotence, already-published, and explicit-failure rules that
govern row-set publication.

If the bounded retry policy is exhausted before the publication commit point is
reached, `put` shall fail explicitly as a backend failure and shall not report
success for that block ID.

### REQ-AZURE-TABLE-STORE-V2-020

If Azure entity-read or table-query transport fails before `get` or identifier
enumeration receives a backend response, the implementation shall retry that
same read or query request using a bounded retry policy.

For `get`, that retried request shall remain the same deterministic
entity-addressed read for the mapped root row or required continuation row and
shall not fall back to a filtered table query for the same known row address.

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

### REQ-AZURE-TABLE-STORE-V2-021

The v2 implementation's single-block publish and single-block read paths shall
not fail solely because a real Azure Table backend response omits common storage
headers that are not required to determine the operation outcome.

This requirement applies when the underlying HTTP exchange otherwise provides
sufficient status and payload information to distinguish success, absence,
already-exists, and explicit backend-failure outcomes.

### REQ-AZURE-TABLE-STORE-V2-022

The repository shall provide a mock-backed verification surface for
`lexongraph-block-store-azure-table-v2` that can simulate Azure publish, read,
and query outcomes; inject malformed or integrity-mismatched recognized block
row sets in the v2 chunked row-set format; and observe constructor behavior
without requiring a live Azure table.

This mock-backed verification surface shall remain internal or test-only and
shall not broaden the public production `BlockStore` API boundary.

## Out of Scope

This crate does not define or own:

- block canonicalization, block validity, or block-ID derivation rules
- changes to the parent `BlockStore` API
- consumer-visible Azure-specific table layout beyond the inherited block-ID
  contract and the v2 chunked row-set format owned by this crate
- deletion, mutation, compaction, lease management, or lifecycle policy
- automatic table creation or other IaC concerns
- compatibility with, migration from, or mutation of entities owned by
  `lexongraph-block-store-azure-table`
- non-Azure storage backends

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/blocks.md`.

This document is also subordinate to the `docs/specs/rust-block-crate/` and
`docs/specs/rust-block-storage-trait/` specification packages for their
respective concerns.

If this document appears to conflict with those authorities, they are
authoritative for their owned concerns.
