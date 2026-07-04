<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Azure SDK Blob Block Store Design

## Status

Draft design specification for `lexongraph-block-store-azure-sdk`.

## Design Goals

The SDK-backed crate is intended to be:

- subordinate to the backend-neutral `BlockStore` contract
- maintainable by reusing the official Azure Rust SDK transport surface
- deterministic in its block-ID-to-blob-name mapping
- explicit about backend failures
- parallel to, not a replacement for, the legacy Azure block-store crate

## Crate Boundary

### DSG-AZURE-SDK-STORE-001 `Dependencies and boundary`

The crate depends on:

- `lexongraph-block` for canonical serialization, block-ID derivation, and
  validated decoding
- `lexongraph-block-store` for the `BlockStore` trait and shared error taxonomy
- `azure_core` and `azure_storage_blob` for SAS-authenticated Azure Blob
  operations
- `tokio` for adapting the async SDK to the synchronous `BlockStore` trait

The crate does not redefine block validation, parent trait semantics, deletion,
or compaction behavior.

### DSG-AZURE-SDK-STORE-002 `Store type`

`AzureBlobBlockStore` owns:

- a normalized container SAS `Url`
- a redacted container display string for diagnostics
- an internal Tokio runtime used to drive async Azure SDK operations behind the
  synchronous `BlockStore` trait

### DSG-AZURE-SDK-STORE-003 `Construction`

Construction:

1. parses the supplied container SAS URL
2. strips fragments
3. verifies that the URL addresses a container root
4. verifies that the query includes a non-empty SAS signature parameter
5. prepares an Azure `BlobContainerClient` with SDK retry options

Construction does not perform an eager permission probe.

## Blob Mapping

### DSG-AZURE-SDK-STORE-004 `Deterministic blob name`

Each block ID maps to exactly one deterministic blob name:

`<hh>/<hh>/<full-lowercase-block-id>.cbor`

This sharded layout remains an implementation detail of the Azure backend and
is not promoted into the parent trait boundary.

## Runtime Behavior

### DSG-AZURE-SDK-STORE-005 `SDK client configuration`

Each container client is constructed with the official Azure SDK and a bounded
exponential retry policy through `BlobContainerClientOptions`.

The retry configuration is an implementation detail of this crate, but it must
remain bounded and exponential for publish, read, and list operations.

### DSG-AZURE-SDK-STORE-006 `put`

`put`:

1. canonicalizes the input block through the block crate
2. derives the deterministic blob name
3. uploads the canonical bytes through the Azure SDK using create-without-overwrite semantics
4. returns success on a successful upload
5. treats Azure existing-blob outcomes (`409`, `412`, `BlobAlreadyExists`,
   `ConditionNotMet`) as successful convergence
6. maps all other denied or failed publish outcomes to explicit backend failures

This SDK-backed design does not perform a manual post-failure readback after
retry exhaustion.

### DSG-AZURE-SDK-STORE-007 `get`

`get`:

1. derives the deterministic blob name from the requested block ID
2. uses the Azure SDK to check for blob existence
3. returns `Ok(None)` when Azure reports absence
4. downloads blob bytes when present
5. validates the downloaded bytes through the block crate before returning
   success

Malformed bytes map to malformed-content failures, block-ID mismatches map to
integrity-mismatch failures, and inaccessible reads map to backend failures.

### DSG-AZURE-SDK-STORE-008 `iter_block_ids`

`iter_block_ids` uses the Azure SDK to list container blobs, filters for
recognized deterministic blob names, decodes them back into block IDs, and
yields only block IDs at the parent trait boundary.

Unrecognized container content is ignored. Malformed recognized candidates are
surfaced as explicit backend failures.

### DSG-AZURE-SDK-STORE-009 `Concurrency`

Concurrent publishers rely on deterministic blob names plus create-without-
overwrite upload so that multiple writers of the same block converge on one
published blob.

### DSG-AZURE-SDK-STORE-010 `Error mapping`

Azure SDK construction, authorization, retry exhaustion, and transport failures
map to explicit backend failures through the parent error taxonomy.

Block decoding failures and block-ID mismatches continue to map to the parent
crate's malformed-content and integrity-mismatch errors.

## Verification Strategy

### DSG-AZURE-SDK-STORE-011 `Mock-backed verification`

The crate reuses the parent block-store conformance helpers and adds SDK-focused
tests for:

- constructor acceptance and rejection cases
- deterministic blob-name mapping
- round-trip `put`/`get`
- conflict-success handling for `409` and `412`
- transient publish, read, and list retries
- explicit failure after retry exhaustion
- enumeration filtering and malformed-candidate failures

### DSG-AZURE-SDK-STORE-012 `Live Azure verification`

The crate provides an ignored live Azure integration test that consumes a real
container SAS URL from the environment and proves constructor success,
publication, retrieval, absence handling, and enumeration against a real
provisioned Azure container.

The repository CI workflow provisions the temporary Azure resources needed for
that live test and cleans them up afterward.
