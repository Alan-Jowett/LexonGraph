<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Azure SDK Blob Block Store Requirements

## Status

Draft specification for a Rust crate that realizes the LexonGraph
`BlockStore` contract on Azure Blob Storage through the official Azure Rust
SDK.

## Scope

This package specifies the new parallel crate
`crates/lexongraph-block-store-azure-sdk`.

It is layered on top of:

- `docs/protocol/blocks.md`
- `docs/specs/rust-block-crate/`
- `docs/specs/rust-block-storage-trait/`

This package does not replace or modify
`crates/lexongraph-block-store-azure`. The legacy crate remains available as a
separate implementation while this package defines the SDK-backed variant.

## Terminology

In this package, `container SAS URL` means a caller-supplied Azure Blob Storage
URL that addresses a container root and carries one or more shared access
signature query parameters, including a non-empty signature parameter.

`Recognized block-blob candidate` means a blob whose name matches the
deterministic layout:

`<hh>/<hh>/<full-lowercase-block-id>.cbor`

where the first two directory-style segments are the first two bytes of the
block ID in lowercase hexadecimal.

## Requirements

### REQ-AZURE-SDK-STORE-001

The SDK-backed Azure crate shall remain subordinate to
`docs/protocol/blocks.md`, `docs/specs/rust-block-crate/`, and
`docs/specs/rust-block-storage-trait/` for block identity, validation, and the
backend-neutral `BlockStore` contract.

### REQ-AZURE-SDK-STORE-002

The repository shall include a Rust crate named
`lexongraph-block-store-azure-sdk`, separate from both
`lexongraph-block-store` and `lexongraph-block-store-azure`, that implements
`BlockStore` against a caller-supplied Azure Blob Storage container SAS URL.

### REQ-AZURE-SDK-STORE-003

The new crate shall realize Azure Blob operations through the official Azure
Rust SDK rather than through repository-owned request signing, HTTP transport,
or XML parsing logic for publish, read, and listing operations.

### REQ-AZURE-SDK-STORE-004

Construction shall accept a container SAS URL outside the `BlockStore` trait
boundary.

Construction shall either return an initialized store bound to that container
or fail explicitly as a backend failure when the supplied URL cannot be parsed,
does not address a container root, omits SAS query parameters, omits a non-empty
SAS signature parameter, or cannot be prepared for Azure Blob operations.

This revision does not require construction to preflight read, list, create, or
write permissions embedded in the SAS URL.

### REQ-AZURE-SDK-STORE-005

`put` shall derive canonical block bytes and the block ID through the block
crate and map that block ID to exactly one deterministic blob name within the
configured container.

This revision shall use the sharded blob-name layout:

- first path segment: first two lowercase hexadecimal characters of the block ID
- second path segment: next two lowercase hexadecimal characters of the block ID
- final path segment: full lowercase hexadecimal block ID plus `.cbor`

### REQ-AZURE-SDK-STORE-006

`get` shall return `Ok(None)` when the mapped block blob is absent.

When the mapped block blob is present, `get` shall validate the retrieved bytes
against the requested block ID before reporting success.

Present malformed content, integrity-mismatched content, or inaccessible
content shall fail explicitly rather than being reported as absence.

### REQ-AZURE-SDK-STORE-007

`put` shall attempt a create-without-overwrite publication of the canonical
block bytes to the deterministic blob name, and it shall not overwrite a
previously published blob for that block ID.

If publication observes that the deterministic blob already exists, including
Azure outcomes such as HTTP 409 Conflict, HTTP 412 Precondition Failed,
`BlobAlreadyExists`, or `ConditionNotMet`, `put` shall return success.

This success outcome does not require `put` to re-read or re-validate the
existing blob bytes. Any later `get` remains responsible for validating the
retrieved bytes against the requested block ID.

### REQ-AZURE-SDK-STORE-008

If `put`, `get`, or enumeration cannot complete because the SAS URL or backend
denies the required operation, the operation shall fail explicitly as a backend
failure.

### REQ-AZURE-SDK-STORE-009

Concurrent publishers of the same logical block to the same Azure container may
race, but the implementation shall converge on one valid published blob for
that block ID.

### REQ-AZURE-SDK-STORE-010

The implementation shall expose the parent trait's streaming block-ID
enumeration over recognized block blobs rooted at the configured container and
shall not expose Azure-specific addressing details at the trait boundary.

### REQ-AZURE-SDK-STORE-011

Enumeration shall report only recognized block blobs. A malformed recognized
block-blob candidate, including a shard-prefix mismatch, shall fail explicitly
rather than being silently dropped as unrelated content.

### REQ-AZURE-SDK-STORE-012

Publish, read, and container-list operations shall use a bounded exponential
retry policy configured through the official Azure SDK client.

If a later retry reaches a backend response, the operation shall continue
applying its normal success, absence, filtering, decode, and explicit-failure
rules for that response.

If the bounded retry policy is exhausted without reaching a successful backend
response, the operation shall fail explicitly as a backend failure.

### REQ-AZURE-SDK-STORE-013

The repository shall provide a dedicated opt-in live integration-verification
mode for `lexongraph-block-store-azure-sdk` that exercises the crate against a
real Azure Blob Storage container.

The live verification mode shall remain outside the default local and workspace
test path so routine verification does not require live Azure credentials.

### REQ-AZURE-SDK-STORE-014

The CI workflow shall provision temporary Azure Blob resources, pass a real
container SAS URL into the SDK-backed crate's ignored live test, and clean up
the temporary Azure resources afterward when Azure-relevant SDK surfaces change.
