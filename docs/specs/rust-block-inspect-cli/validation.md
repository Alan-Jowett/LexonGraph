<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Block Inspect CLI Validation

## Status

Draft validation specification for a Rust workspace binary crate that inspects
one LexonGraph block and renders a JSON debug view.

## Validation Scope

These validation entries define the expected conformance surface for the inspect
CLI crate.

Block-validity, block-identity, and backend-neutral retrieval expectations
remain normatively defined by `docs/protocol/blocks.md`,
`docs/specs/rust-block-crate/`, and `docs/specs/rust-block-storage-trait/`.

## Validation Entries

### VAL-INSPECT-001

Inspect the repository workspace and package artifacts for the inspect CLI.

**Pass condition:** the repository contains a Rust workspace binary crate named
`lexongraph-block-inspect` wired into the workspace as the implementation
artifact for this specification package.

**Traces to:** REQ-INSPECT-001

### VAL-INSPECT-002

Inspect the CLI help or argument surface for the inspect binary.

**Pass condition:** the CLI requires a backend selector and a target block hash,
accepts backend-specific parameters outside the `BlockStore` trait boundary, and
exposes the filesystem backend's store-root input in this revision.

**Traces to:** REQ-INSPECT-003, REQ-INSPECT-004, REQ-INSPECT-010

### VAL-INSPECT-003

Run the CLI against a filesystem-backed store containing a valid branch block.

**Pass condition:** the CLI succeeds and emits one JSON document containing the
verified block hash, `level = 1`, and the decoded block content.

**Traces to:** REQ-INSPECT-004, REQ-INSPECT-005, REQ-INSPECT-006,
REQ-INSPECT-007

### VAL-INSPECT-004

Run the CLI against a filesystem-backed store containing a valid leaf block.

**Pass condition:** the CLI succeeds and emits one JSON document containing the
verified block hash, `level = 0`, and the decoded block content.

**Traces to:** REQ-INSPECT-004, REQ-INSPECT-005, REQ-INSPECT-006,
REQ-INSPECT-007

### VAL-INSPECT-005

Inspect a block whose rendered surface includes byte-bearing fields and
arbitrary CBOR-backed metadata or extension values.

**Pass condition:** the success JSON uses the documented debug JSON encoding for
those values rather than omitting them, flattening non-string map keys, or
coercing bytes into ambiguous plain strings.

**Traces to:** REQ-INSPECT-007, REQ-INSPECT-008

### VAL-INSPECT-006

Request a block hash that is syntactically valid but absent from the configured
store.

**Pass condition:** the CLI fails explicitly for block absence and does not emit
success-shaped JSON.

**Traces to:** REQ-INSPECT-009

### VAL-INSPECT-007

Populate the configured store so that the requested block hash resolves to
malformed or protocol-invalid stored content.

**Pass condition:** the CLI fails explicitly for malformed content and does not
emit success-shaped JSON.

**Traces to:** REQ-INSPECT-009

### VAL-INSPECT-008

Populate the configured store so that the requested block hash resolves to
stored bytes whose verified identity differs from the requested hash.

**Pass condition:** the CLI fails explicitly for integrity mismatch and does not
emit success-shaped JSON.

**Traces to:** REQ-INSPECT-009

### VAL-INSPECT-009

Invoke the CLI with an invalid block-hash argument and with an unsupported
backend selector.

**Pass condition:** each case fails explicitly as invalid CLI input or
unsupported backend selection and does not emit success-shaped JSON.

**Traces to:** REQ-INSPECT-009

### VAL-INSPECT-010

Invoke the CLI with filesystem backend construction inputs that cannot produce a
usable store instance.

**Pass condition:** the CLI fails explicitly for store construction failure and
does not emit success-shaped JSON.

**Traces to:** REQ-INSPECT-004, REQ-INSPECT-009

### VAL-INSPECT-011

Inspect a branch block whose entries reference child block hashes.

**Pass condition:** the CLI renders those child references as data in the JSON
output and does not recursively load additional blocks beyond the requested
block in this revision, and the runtime surface does not expose traversal,
mutation, enumeration, listing, or deletion operations.

**Traces to:** REQ-INSPECT-011

### VAL-INSPECT-012

Inspect the repository verification artifacts for the inspect CLI crate.

**Pass condition:** the repository includes automated tests that realize the
validation surface in this specification package.

**Traces to:** REQ-INSPECT-012

### VAL-INSPECT-013

Inspect the crate boundary and implementation dependencies for the inspect CLI.

**Pass condition:** the crate delegates block retrieval through a `BlockStore`
implementation, relies on the subordinate block crate and block-store trait
crate for verified block interpretation and retrieval semantics, and does not
redefine those contracts or bypass them with backend-internal inspection logic.

**Traces to:** REQ-INSPECT-002, REQ-INSPECT-005, REQ-INSPECT-006

### VAL-INSPECT-014

Populate the configured filesystem store so that the deterministic block path
for the requested block hash cannot be read as block bytes.

**Pass condition:** the CLI fails explicitly for backend retrieval failure and
does not emit success-shaped JSON.

**Traces to:** REQ-INSPECT-009, REQ-INSPECT-012

### VAL-INSPECT-015

Run the CLI against a filesystem-backed store containing a valid higher-level
child-bearing block.

**Pass condition:** the CLI succeeds and emits the preserved numeric `level`
value for that block.

**Traces to:** REQ-INSPECT-004, REQ-INSPECT-005, REQ-INSPECT-006,
REQ-INSPECT-007
