<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Filesystem Block Store Validation

## Status

Draft validation specification for a Rust crate that implements the LexonGraph
block-storage contract on a local filesystem.

## Validation Scope

These validation entries define the expected conformance surface for the local
filesystem backend in addition to the parent block-store trait validation
surface.

Block validity, canonical serialization, block-ID derivation, and the
backend-neutral `BlockStore` contract remain normatively defined by
`docs/protocol/blocks.md`, `docs/specs/rust-block-crate/`, and
`docs/specs/rust-block-storage-trait/`.

## Validation Entries

### VAL-FS-STORE-001

Construct the filesystem-backed store with a caller-supplied directory path.

**Pass condition:** construction succeeds for an accessible root, and the
consumer is not required to know any implementation-specific path layout below
that root.

**Traces to:** REQ-FS-STORE-002, REQ-FS-STORE-003

### VAL-FS-STORE-002

Store a valid typed block through `put`, then inspect the on-disk location used
for the published block file.

**Pass condition:** the implementation derives one deterministic path below the
store root for that block ID and stores the canonical bytes at that location.

**Traces to:** REQ-FS-STORE-004

### VAL-FS-STORE-003

Attempt to retrieve a block ID whose published file is absent.

**Pass condition:** `get` returns `Ok(None)`.

**Traces to:** REQ-FS-STORE-006

### VAL-FS-STORE-004

Populate the published file path for a requested block ID with bytes whose
verified identity differs from that block ID.

**Pass condition:** `get` fails explicitly with an integrity-mismatch error.

**Traces to:** REQ-FS-STORE-006

### VAL-FS-STORE-005

Populate the published file path for a requested block ID with malformed or
protocol-invalid bytes.

**Pass condition:** `get` fails explicitly with a malformed-content error and
does not report absence.

**Traces to:** REQ-FS-STORE-006

### VAL-FS-STORE-006

Pre-populate the published file path for a block ID with bytes that differ from
the canonical bytes of the block supplied to `put`.

**Pass condition:** `put` fails explicitly and leaves the conflicting published
bytes in place while reporting a backend failure that describes corruption or
integrity conflict.

**Traces to:** REQ-FS-STORE-007

### VAL-FS-STORE-007

Observe the store root while `put` publishes a block.

**Pass condition:** readers observe either no published file or a complete
published block file, and never a partially published target file.

**Traces to:** REQ-FS-STORE-005

### VAL-FS-STORE-008

Use two or more store instances bound to the same store root to publish the
same logical block concurrently.

**Pass condition:** all successful publishers report the same block ID, the
store converges on one valid published block file for that ID, and readers can
subsequently load that block successfully.

**Traces to:** REQ-FS-STORE-005, REQ-FS-STORE-008

### VAL-FS-STORE-009

Inspect the implementation after a successful publish.

**Pass condition:** no staging file remains at the target path, and the
published block file is located below the configured store root.

**Traces to:** REQ-FS-STORE-003, REQ-FS-STORE-005

### VAL-FS-STORE-010

Run the parent block-store conformance suite against the filesystem-backed
implementation.

**Pass condition:** the backend satisfies the shared `put`/`get` contract
without backend-specific changes to the parent trait API.

**Traces to:** REQ-FS-STORE-001, REQ-FS-STORE-009

### VAL-FS-STORE-011

Inspect the repository verification artifacts for the filesystem block-store
crate.

**Pass condition:** the repository includes automated tests that realize this
validation surface and reuse the parent crate's conformance helpers where they
cover the same contract.

**Traces to:** REQ-FS-STORE-009, REQ-FS-STORE-012

### VAL-FS-STORE-012

Attempt to construct the filesystem-backed store with roots that cannot satisfy
the constructor boundary, including a non-directory path and controlled
create/canonicalize/stat failure cases.

**Pass condition:** construction fails explicitly as a backend failure and does
not return an initialized store.

**Traces to:** REQ-FS-STORE-003, REQ-FS-STORE-012

### VAL-FS-STORE-013

Populate the published file path for a requested block ID with valid bytes, then
make the file unreadable before calling `get`.

**Pass condition:** `get` fails explicitly as a backend failure and does not
report absence.

**Traces to:** REQ-FS-STORE-006, REQ-FS-STORE-012

### VAL-FS-STORE-014

Force parent-directory creation, staging-file creation, staged write, and
staged flush failures during `put`.

**Pass condition:** each case fails explicitly as a backend failure, and no
published target file becomes visible for the block ID.

**Traces to:** REQ-FS-STORE-005, REQ-FS-STORE-010, REQ-FS-STORE-012

### VAL-FS-STORE-015

Force atomic publication to fail after staging succeeds, while arranging for the
target path to contain byte-identical canonical content.

**Pass condition:** `put` reports success for the block ID and leaves matching
published bytes in place.

**Traces to:** REQ-FS-STORE-005, REQ-FS-STORE-007, REQ-FS-STORE-011,
REQ-FS-STORE-012

### VAL-FS-STORE-016

Force atomic publication to fail after staging succeeds, while arranging for the
target path to contain bytes that differ from the canonical block bytes.

**Pass condition:** `put` fails explicitly with a backend failure that
describes integrity conflict and leaves the differing published bytes in place.

**Traces to:** REQ-FS-STORE-007, REQ-FS-STORE-011, REQ-FS-STORE-012

### VAL-FS-STORE-017

Force atomic publication to fail after staging succeeds, then observe that no
published target file is present at the deterministic path.

**Pass condition:** `put` fails explicitly as a backend failure rather than
reporting success or silent absence.

**Traces to:** REQ-FS-STORE-011, REQ-FS-STORE-012

### VAL-FS-STORE-018

Force atomic publication to fail after staging succeeds, then make the target
path unreadable or otherwise uninspectable before recovery inspection.

**Pass condition:** `put` fails explicitly as a backend failure rather than
reporting success or silent absence.

**Traces to:** REQ-FS-STORE-011, REQ-FS-STORE-012

### VAL-FS-STORE-019

Publish multiple valid blocks under one store root, then consume the parent
trait's enumeration surface through the filesystem-backed implementation.

**Pass condition:** enumeration yields the published block IDs rooted under that
store without exposing filesystem paths at the trait boundary.

**Traces to:** REQ-FS-STORE-013, REQ-FS-STORE-014

### VAL-FS-STORE-020

Observe the store root while staging or temporary files exist alongside
published block files, then enumerate block IDs.

**Pass condition:** enumeration reports only published block IDs and does not
report staging files, temporary files, directories, or other non-published
artifacts.

**Traces to:** REQ-FS-STORE-015

### VAL-FS-STORE-021

Force store-root traversal or published-path decoding to fail during
enumeration.

**Pass condition:** enumeration fails explicitly as a backend failure rather
than silently omitting the affected stored state.

**Traces to:** REQ-FS-STORE-016

### VAL-FS-STORE-022

Construct the opt-in cache-mode filesystem store with a positive MB budget and
with zero MB.

**Pass condition:** positive MB construction succeeds, zero MB construction
fails explicitly, and cache-mode behavior uses `1 MB = 1,048,576 bytes` for the
configured payload-byte budget.

**Traces to:** REQ-FS-STORE-017

### VAL-FS-STORE-023

Fill the cache-mode filesystem store close to its byte budget, refresh one
cached block with a successful `get`, then insert another block whose payload
bytes require eviction.

**Pass condition:** the least-recently-used non-refreshed cached published file
is evicted according to payload-byte pressure rather than cached-block count.

**Traces to:** REQ-FS-STORE-018, REQ-FS-STORE-019

### VAL-FS-STORE-024

Construct the cache-mode filesystem store against an existing cache root whose
published payload bytes already exceed the configured byte budget.

**Pass condition:** construction evicts least-recently-used existing cached
published files, using filesystem last-modified time as the initial recency
signal, until the retained payload set fits the budget.

**Traces to:** REQ-FS-STORE-019, REQ-FS-STORE-020

### VAL-FS-STORE-025

Attempt to store one block whose canonical payload bytes exceed the total
cache-mode byte budget.

**Pass condition:** the direct cache write fails explicitly, no published block
file appears at the deterministic target path, and the remaining cache contents
stay unchanged.

**Traces to:** REQ-FS-STORE-021, REQ-FS-STORE-022
