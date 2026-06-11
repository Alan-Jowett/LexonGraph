<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Zip Block Store Validation

## Status

Draft validation specification for a Rust crate that exposes read-only
LexonGraph block storage over a single zip archive.

## Validation Scope

These validation entries define the expected verification surface for the
zip-backed read-only implementation in addition to the parent protocol and block
requirements it depends on.

This package does not claim full conformance to
`docs/specs/rust-block-storage-trait/` because `put` is intentionally read-only
for this backend.

## Validation Entries

### VAL-ZIP-STORE-001

Construct the zip-backed store from an accessible zip file.

**Pass condition:** construction succeeds and the caller does not need to know
archive internals beyond the supplied archive path.

**Traces to:** REQ-ZIP-STORE-002, REQ-ZIP-STORE-003

### VAL-ZIP-STORE-002

Attempt to construct the zip-backed store from missing, non-file, invalid-zip,
and unsupported zip64 inputs.

**Pass condition:** construction fails explicitly as a backend failure.

**Traces to:** REQ-ZIP-STORE-003

### VAL-ZIP-STORE-003

Retrieve a valid block from a recognized unique archive entry.

**Pass condition:** `get` returns `Ok(Some(validated_block))`.

**Traces to:** REQ-ZIP-STORE-004, REQ-ZIP-STORE-005

### VAL-ZIP-STORE-004

Request a block ID with no recognized archive entry.

**Pass condition:** `get` returns `Ok(None)`.

**Traces to:** REQ-ZIP-STORE-005

### VAL-ZIP-STORE-005

Populate a recognized archive entry with malformed or protocol-invalid bytes.

**Pass condition:** `get` fails explicitly with a malformed-content error.

**Traces to:** REQ-ZIP-STORE-005

### VAL-ZIP-STORE-006

Populate a recognized archive entry with valid block bytes for a different block
ID.

**Pass condition:** `get` fails explicitly with an integrity-mismatch error.

**Traces to:** REQ-ZIP-STORE-005

### VAL-ZIP-STORE-007

Populate the archive with duplicate entries for the same recognized block path.

**Pass condition:** `get` and enumeration fail explicitly rather than choosing
one duplicate entry.

**Traces to:** REQ-ZIP-STORE-005, REQ-ZIP-STORE-006

### VAL-ZIP-STORE-008

Populate the archive with entries outside the recognized block-entry layout.

**Pass condition:** unrelated entries are ignored and do not participate in
`get` or enumeration.

**Traces to:** REQ-ZIP-STORE-004, REQ-ZIP-STORE-006

### VAL-ZIP-STORE-009

Populate the archive with multiple recognized unique block entries, then
enumerate block IDs.

**Pass condition:** enumeration yields the recognized block IDs only.

**Traces to:** REQ-ZIP-STORE-006

### VAL-ZIP-STORE-010

Inspect the zip-backed implementation's public and verification surface.

**Pass condition:** the repository includes automated verification artifacts for
the approved read-only zip backend behavior and does not claim full parent
conformance.

**Traces to:** REQ-ZIP-STORE-008, REQ-ZIP-STORE-009

### VAL-ZIP-STORE-011

Call `put` on the zip-backed store.

**Pass condition:** `put` fails explicitly as a backend failure describing
read-only storage and does not mutate the archive.

**Traces to:** REQ-ZIP-STORE-007
