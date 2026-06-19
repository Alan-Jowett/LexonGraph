<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Azure Blob Block Store Requirements

## Status

Draft specification for a Rust crate that implements the LexonGraph
block-storage contract on Azure Blob Storage using a container SAS URL.

## Scope

This document specifies implementation-specific requirements for a Rust crate
that realizes `docs/specs/rust-block-storage-trait/` on Azure Blob Storage.

This document is layered on top of:

- `docs/protocol/blocks.md`
- `docs/specs/rust-block-crate/`
- `docs/specs/rust-block-storage-trait/`

This document does not repeat or redefine the parent trait contract. It adds
only Azure Blob-specific requirements needed to realize that contract in this
repository.

## Terminology

In this spec package, `container SAS URL` means a caller-supplied Azure Blob
Storage URL that addresses a container root and carries one or more
shared-access-signature query parameters, including a non-empty signature
parameter.

`Recognized block-blob candidate` means a blob whose name matches the
deterministic sharded block layout shape:

`<hh>/<hh>/<full-lowercase-block-id>.cbor`

where the first two directory-style path segments are the first two bytes of
the block ID in lowercase hexadecimal.

A recognized block-blob candidate becomes a recognized block blob only when the
filename segment is a full valid lowercase block ID whose first two shard
segments match that block ID.

A blob whose name has the deterministic `hh/hh/<id>.cbor` shape but whose shard
segments do not match the decoded block ID remains a malformed recognized
block-blob candidate rather than unrelated content.

## Requirements

### REQ-AZURE-STORE-001

The Azure Blob block-store crate shall remain subordinate to
`docs/protocol/blocks.md`, `docs/specs/rust-block-crate/`, and
`docs/specs/rust-block-storage-trait/` for block identity, validation, and the
backend-neutral `BlockStore` contract.

### REQ-AZURE-STORE-002

The repository shall include a Rust crate, separate from
`crates/lexongraph-block-store`, that implements `BlockStore` using a
caller-supplied Azure Blob Storage container SAS URL.

### REQ-AZURE-STORE-003

Construction shall accept a container SAS URL outside the `BlockStore` trait
boundary.

Construction shall either return an initialized store bound to that container
or fail explicitly as a backend failure when the supplied URL cannot be parsed,
does not address a container root, omits SAS query parameters, omits a non-empty
SAS signature parameter, or cannot be prepared for Azure Blob operations.

This revision does not require construction to preflight read, list, create, or
write permissions embedded in the SAS URL.

### REQ-AZURE-STORE-004

`put` shall derive the canonical block bytes and block ID through the block
crate and map that block ID to exactly one deterministic blob name within the
configured container.

### REQ-AZURE-STORE-005

This revision shall use the deterministic blob-name layout:

- first path segment: first two lowercase hexadecimal characters of the block
  ID
- second path segment: next two lowercase hexadecimal characters of the block
  ID
- final path segment: full lowercase hexadecimal block ID plus `.cbor`

The consumer-facing runtime contract shall not require callers to know that
blob-name layout.

### REQ-AZURE-STORE-006

`get` shall return `Ok(None)` when the mapped block blob is absent.

When the mapped block blob is present, `get` shall validate the retrieved bytes
against the requested block ID before reporting success.

`get` shall be total over the mapped blob state:

- present readable valid content for the requested block ID shall return
  `Ok(Some(validated_block))`
- present readable malformed or protocol-invalid content shall fail explicitly
  as malformed content
- present readable content whose verified identity differs from the requested
  block ID shall fail explicitly as an integrity-mismatch condition
- present unreadable or otherwise inaccessible content shall fail explicitly as
  a backend failure

### REQ-AZURE-STORE-007

`put` shall publish the canonical block bytes to the deterministic blob name
without overwriting previously published differing bytes for that block ID.

If publication observes that the deterministic blob already exists, whether
before or after a concurrent publication race is re-inspected, `put` shall:

- return success when the existing blob bytes match the canonical bytes for the
  block
- fail explicitly as a backend failure describing corruption or integrity
  conflict when the existing blob bytes differ

### REQ-AZURE-STORE-008

If `put` cannot create or write the deterministic blob because the SAS URL or
backend denies the required operation, it shall fail explicitly as a backend
failure.

This explicit failure behavior applies even when construction previously
succeeded.

### REQ-AZURE-STORE-009

Concurrent publishers of the same logical block to the same Azure container may
race, but the implementation shall converge on one valid published blob for
that block ID.

### REQ-AZURE-STORE-010

The Azure-backed implementation shall implement the parent trait's streaming
block-ID enumeration over recognized block blobs rooted at the configured
container.

### REQ-AZURE-STORE-011

Azure enumeration shall expose only block identifiers at the parent trait
boundary and shall not expose container URLs, blob names, prefixes, or other
Azure-specific addressing details.

### REQ-AZURE-STORE-012

Azure enumeration shall report only recognized block blobs.

It shall not report unrelated blobs, partial uploads, directories, or other
container artifacts as stored block IDs.

### REQ-AZURE-STORE-013

Azure enumeration shall surface explicit backend failure when container listing,
blob inspection, or decoding of a recognized block-blob candidate into a valid
block ID cannot be completed.

This explicit failure rule includes malformed candidate names and shard-prefix
mismatches within the deterministic candidate layout.

## Out of Scope

This crate does not define or own:

- block canonicalization, block validity, or block-ID derivation rules
- changes to the parent `BlockStore` API
- consumer-visible Azure-specific paths, prefixes, or container layout beyond
  the inherited block-ID contract
- deletion, mutation, compaction, lease management, or lifecycle policy
- prefix-rooted multi-tenant layouts within one container in this revision
- non-Azure storage backends

## Relationship to Other Specifications

This document is subordinate to `docs/protocol/blocks.md`.

This document is also subordinate to the `docs/specs/rust-block-crate/` and
`docs/specs/rust-block-storage-trait/` specification packages for their
respective concerns.

If this document appears to conflict with those authorities, they are
authoritative for their owned concerns.
