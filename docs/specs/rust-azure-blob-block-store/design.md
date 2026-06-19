<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Azure Blob Block Store Design

## Status

Draft design specification for a Rust crate that implements the LexonGraph
block-storage contract on Azure Blob Storage using a container SAS URL.

## Design Goals

The crate design is intended to be:

- subordinate to the backend-neutral `BlockStore` contract
- deterministic in its mapping from block ID to blob name
- strict about integrity conflicts
- narrow at the public API boundary
- compatible with SAS-authenticated Azure Blob operations

## Crate Boundary

The crate owns:

- Azure Blob-specific realization of `BlockStore`
- container-SAS configuration and normalization
- deterministic blob-name derivation
- Azure-specific publication, retrieval, and enumeration mechanics
- Azure-specific error mapping

The crate does not own:

- block canonicalization or block-ID derivation
- block validation rules beyond invoking the block crate
- changes to the parent trait boundary
- deletion or compaction behavior
- prefix-rooted sub-container tenancy in this revision

## External Dependencies

### DSG-AZURE-STORE-001 `Parent crate dependencies`

The Azure Blob block-store crate depends on:

- the block crate for canonical serialization, block-ID derivation, and
  validated decoding
- the block-storage trait crate for the `BlockStore` trait and shared error
  taxonomy
- an Azure Blob client dependency capable of SAS-authenticated blob upload,
  download, existence inspection, and container listing

The crate does not redefine those behaviors.

## Core Types

### DSG-AZURE-STORE-002 `AzureBlobBlockStore`

A concrete store type owns the normalized container endpoint and SAS
authorization material and implements the parent `BlockStore` trait for that
container.

### DSG-AZURE-STORE-003 `Construction boundary`

The crate exposes a constructor that accepts a container SAS URL and returns an
initialized store instance or an explicit error.

Construction:

1. parses the supplied URL
2. verifies that it addresses a container root rather than an individual blob
3. retains the normalized container endpoint and SAS query material needed for
   future operations

Construction does not require an eager permission probe for read, list, create,
or write operations. Missing permissions remain runtime backend failures on the
operations that need them.

## Blob Mapping

### DSG-AZURE-STORE-004 `Deterministic block blob name`

Each block ID maps to exactly one deterministic blob name inside the configured
container.

This revision uses the sharded layout:

- first path segment: first two lowercase hexadecimal characters of the block
  ID
- second path segment: next two lowercase hexadecimal characters of the block
  ID
- final segment: full lowercase hexadecimal block ID plus `.cbor`

Example:

`ab/cd/abcdef...0123.cbor`

This blob-name layout is an implementation detail for this crate and is not
promoted into the backend-neutral parent contract.

## Runtime Behavior

### DSG-AZURE-STORE-005 `put`

`put`:

1. canonicalizes the input block through the block crate
2. derives the deterministic blob name from the returned block ID
3. attempts to publish the canonical bytes to that blob name using an Azure
   write primitive that refuses to overwrite an already existing blob
4. returns the block ID on successful publication
5. maps denied or failed create/write operations to explicit backend failures
6. if publication reports that the blob already exists or otherwise leaves
   publication outcome ambiguous, re-reads the deterministic blob and:
   - treats byte-identical content as idempotent success
   - treats differing bytes as an explicit backend failure describing integrity
     conflict
   - treats missing or unreadable post-failure state as an explicit backend
     failure

### DSG-AZURE-STORE-006 `get`

`get`:

1. derives the deterministic blob name from the requested block ID
2. returns `Ok(None)` when the blob is absent
3. downloads the stored bytes when the blob is present
4. validates those bytes against the requested block ID through the block crate
5. returns `Ok(Some(validated_block))` only for present, valid, matching
   content

Malformed bytes map to malformed-content failures, block-ID mismatch maps to an
integrity-mismatch failure, and inaccessible blob reads map to backend
failures. None of those states are downgraded to absence.

### DSG-AZURE-STORE-007 `iter_block_ids`

`iter_block_ids` lists the configured container and streams block identifiers
for recognized block blobs that match the deterministic blob-name layout.

The enumeration realization:

1. lists container contents without exposing Azure listing details at the trait
   boundary
2. recognizes block blobs by the deterministic sharded layout and `.cbor`
   suffix
3. decodes each recognized blob name back into its block ID
4. yields only decoded block IDs to callers
5. ignores unrelated blobs that do not conform to the recognized block layout

Listing or blob-name decoding failures map to explicit backend failures through
the parent error taxonomy.

### DSG-AZURE-STORE-008 `Concurrent publication`

Two or more store instances targeting the same container may attempt to publish
the same block concurrently.

The design relies on deterministic blob names plus create-without-overwrite
publication so that all successful publishers converge on one valid blob for the
block ID.

If concurrent publishers race on the same block ID, the losing publisher
re-checks the blob:

- if the blob bytes equal the canonical bytes, `put` reports success
- if the blob bytes differ, `put` reports an explicit backend failure
  describing integrity conflict

### DSG-AZURE-STORE-009 `Azure-specific visibility boundary`

The parent trait contract remains limited to typed block values, block IDs, and
identifier enumeration.

The Azure backend does not expose:

- container URLs
- SAS query details
- blob names or prefixes
- Azure SDK request or response types
- backend-native filtering semantics

### DSG-AZURE-STORE-010 `Error mapping`

Azure client, authorization, transport, and listing failures are surfaced as
explicit backend failures through the parent error taxonomy.

Existing-blob byte conflicts are surfaced as explicit backend failures that
describe corruption or integrity conflict at the deterministic blob name.

Block decoding failures and block-ID mismatches continue to map to the parent
crate's malformed-content and integrity-mismatch errors.

## Verification Strategy

### DSG-AZURE-STORE-011 `Conformance reuse`

The crate reuses the parent block-store conformance helpers to verify the
backend-neutral `put`, `get`, and block-ID enumeration contract.

### DSG-AZURE-STORE-012 `Azure-specific verification`

The crate adds backend-specific tests for:

- constructor success for a valid container SAS URL
- explicit constructor failure for malformed or blob-scoped SAS URLs
- deterministic blob-name derivation within the container
- explicit runtime failure for `put` when SAS permissions deny create or write
- explicit runtime failure for inaccessible blob reads or container listing
- idempotent success when publish races converge on matching bytes
- explicit integrity-conflict failure when pre-existing or concurrently
  published bytes differ
- enumeration of recognized block blobs only
- exclusion of unrelated blobs from enumeration
- explicit failure for listing or blob-name decoding errors during enumeration

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-AZURE-STORE-001 | REQ-AZURE-STORE-001, REQ-AZURE-STORE-002 |
| DSG-AZURE-STORE-002..003 | REQ-AZURE-STORE-002, REQ-AZURE-STORE-003 |
| DSG-AZURE-STORE-004 | REQ-AZURE-STORE-004, REQ-AZURE-STORE-005 |
| DSG-AZURE-STORE-005 | REQ-AZURE-STORE-004, REQ-AZURE-STORE-007, REQ-AZURE-STORE-008, REQ-AZURE-STORE-009 |
| DSG-AZURE-STORE-006 | REQ-AZURE-STORE-006 |
| DSG-AZURE-STORE-007 | REQ-AZURE-STORE-010, REQ-AZURE-STORE-011, REQ-AZURE-STORE-012, REQ-AZURE-STORE-013 |
| DSG-AZURE-STORE-008 | REQ-AZURE-STORE-007, REQ-AZURE-STORE-009 |
| DSG-AZURE-STORE-009 | REQ-AZURE-STORE-001, REQ-AZURE-STORE-011 |
| DSG-AZURE-STORE-010 | REQ-AZURE-STORE-001, REQ-AZURE-STORE-006, REQ-AZURE-STORE-007, REQ-AZURE-STORE-008, REQ-AZURE-STORE-013 |
| DSG-AZURE-STORE-011..012 | REQ-AZURE-STORE-002, REQ-AZURE-STORE-003, REQ-AZURE-STORE-006, REQ-AZURE-STORE-007, REQ-AZURE-STORE-008, REQ-AZURE-STORE-009, REQ-AZURE-STORE-010, REQ-AZURE-STORE-012, REQ-AZURE-STORE-013 |
