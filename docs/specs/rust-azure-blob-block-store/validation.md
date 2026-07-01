<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Azure Blob Block Store Validation

## Status

Draft validation specification for a Rust crate that implements the LexonGraph
block-storage contract on Azure Blob Storage using a container SAS URL.

## Validation Scope

These validation entries define the expected conformance surface for the Azure
Blob backend in addition to the parent block-store trait validation surface.

Block validity, canonical serialization, block-ID derivation, and the
backend-neutral `BlockStore` contract remain normatively defined by
`docs/protocol/blocks.md`, `docs/specs/rust-block-crate/`, and
`docs/specs/rust-block-storage-trait/`.

## Validation Entries

### VAL-AZURE-STORE-001

Construct the Azure-backed store with a caller-supplied container SAS URL.

**Pass condition:** construction succeeds for a valid container-root SAS URL,
and the consumer is not required to know any implementation-specific blob-name
layout.

**Traces to:** REQ-AZURE-STORE-002, REQ-AZURE-STORE-003

### VAL-AZURE-STORE-002

Attempt to construct the Azure-backed store with malformed URLs or URLs that
address an individual blob rather than a container root.

**Pass condition:** construction fails explicitly as a backend failure and does
not return an initialized store.

**Traces to:** REQ-AZURE-STORE-003

### VAL-AZURE-STORE-003

Store a valid typed block through `put`, then retrieve it through `get` using
the returned block ID.

**Pass condition:** `get` succeeds and returns a validated typed block with the
same logical meaning and block ID.

**Traces to:** REQ-AZURE-STORE-004, REQ-AZURE-STORE-006

### VAL-AZURE-STORE-004

Store the same logical block multiple times through `put`, including a case in
which another publisher has already created the same deterministic blob.

**Pass condition:** each successful call returns the same block ID and the store
does not present divergent content under that identifier.

**Traces to:** REQ-AZURE-STORE-007, REQ-AZURE-STORE-009

### VAL-AZURE-STORE-005

Request a block ID whose mapped blob is not present in the container.

**Pass condition:** `get` returns `Ok(None)`.

**Traces to:** REQ-AZURE-STORE-006

### VAL-AZURE-STORE-006

Populate the mapped blob for a requested block ID with bytes whose verified
identity differs from that block ID.

**Pass condition:** `get` fails explicitly with an integrity-mismatch error.

**Traces to:** REQ-AZURE-STORE-006

### VAL-AZURE-STORE-007

Populate the mapped blob for a requested block ID with malformed or
protocol-invalid bytes.

**Pass condition:** `get` fails explicitly with a malformed-content error and
does not report absence.

**Traces to:** REQ-AZURE-STORE-006

### VAL-AZURE-STORE-008

Attempt `put` through a store constructed from a SAS URL that lacks the create
or write permissions required to publish the deterministic blob.

**Pass condition:** `put` fails explicitly as a backend failure, and
construction itself was not required to reject the SAS URL beforehand.

**Traces to:** REQ-AZURE-STORE-003, REQ-AZURE-STORE-008

### VAL-AZURE-STORE-009

Pre-populate the deterministic blob for a block ID with bytes that differ from
the canonical bytes of the block supplied to `put`.

**Pass condition:** `put` fails explicitly and leaves the conflicting blob in
place while reporting a backend failure that describes corruption or integrity
conflict.

**Traces to:** REQ-AZURE-STORE-007

### VAL-AZURE-STORE-016

Cause a transient transport failure during `put` before Azure returns any
backend response, including both a case that later succeeds on retry and a case
that continues failing until the bounded retry policy is exhausted.

**Pass condition:** `put` retries the deterministic publish after the transient
transport failure, succeeds when a later retry reaches a successful backend
response, and otherwise fails explicitly as a backend failure without claiming
the block was stored.

**Traces to:** REQ-AZURE-STORE-014

### VAL-AZURE-STORE-010

Run the parent block-store conformance suite against the Azure-backed
implementation.

**Pass condition:** the backend satisfies the shared `put`/`get`/enumeration
contract without backend-specific changes to the parent trait API.

**Traces to:** REQ-AZURE-STORE-001, REQ-AZURE-STORE-002

### VAL-AZURE-STORE-011

Publish multiple valid blocks in one container, then consume the parent trait's
enumeration surface through the Azure-backed implementation.

**Pass condition:** enumeration yields the published block IDs without exposing
container URLs or blob names at the trait boundary.

**Traces to:** REQ-AZURE-STORE-010, REQ-AZURE-STORE-011

### VAL-AZURE-STORE-012

Populate the container with recognized block blobs and unrelated blobs, then
enumerate block IDs.

**Pass condition:** enumeration reports only recognized block IDs and does not
report unrelated blobs or other container artifacts.

**Traces to:** REQ-AZURE-STORE-012

### VAL-AZURE-STORE-013

Cause container listing or decoding of a malformed recognized block-blob
candidate such as `hh/hh/*.cbor` or a shard-prefix-mismatched
`hh/hh/<valid-block-id>.cbor` candidate to fail during enumeration.

**Pass condition:** enumeration fails explicitly as a backend failure rather
than silently omitting the affected stored state.

**Traces to:** REQ-AZURE-STORE-013

### VAL-AZURE-STORE-014

Store a valid typed block through `put`, then inspect the Azure blob name used
for the published bytes.

**Pass condition:** the implementation derives the deterministic sharded blob
name from the returned block ID and stores the canonical bytes at that blob
name.

**Traces to:** REQ-AZURE-STORE-004, REQ-AZURE-STORE-005

### VAL-AZURE-STORE-015

Populate the mapped blob for a requested block ID with valid bytes, then make
the blob unreadable or otherwise inaccessible before calling `get`.

**Pass condition:** `get` fails explicitly as a backend failure and does not
report absence.

**Traces to:** REQ-AZURE-STORE-006
