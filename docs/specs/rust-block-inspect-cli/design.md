<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Block Inspect CLI Design

## Status

Draft design specification for a Rust workspace binary crate that inspects one
LexonGraph block and renders a JSON debug view.

## Design Goals

The crate design is intended to be:

- layered on the existing block and block-store contracts
- explicit about backend construction seams
- strict about verified block identity
- debug-oriented rather than protocol-defining
- extensible across backend kinds
- read-only and minimal at the runtime boundary

## Crate Boundary

The crate owns:

- CLI argument parsing
- backend selection and backend-specific store construction
- inspection-oriented success and failure reporting
- debug JSON rendering of typed block values

The crate does not own:

- block wire encoding or block-ID derivation
- block validation rules beyond invoking subordinate crates
- storage backend implementations
- search or recursive traversal behavior

## External Dependencies

### DSG-INSPECT-001 `Subordinate crate dependencies`

The inspect CLI depends on:

- the block crate for the typed `Block` model and verified decode semantics
- the block-storage trait crate for backend-agnostic block retrieval semantics
- the filesystem block-store crate for the first-pass concrete backend

The inspect CLI does not redefine those behaviors.

## Core Types

### DSG-INSPECT-002 `Backend selector`

The CLI surface includes an explicit backend selector that chooses which
concrete store-construction path will be used for the request.

This revision defines one selector variant for the filesystem backend.

### DSG-INSPECT-003 `Inspection document`

A successful inspection is rendered as one top-level JSON object containing at
least:

- `hash`: the verified lowercase hexadecimal block hash
- `kind`: the decoded block kind, `branch` or `leaf`
- `block`: the decoded block content rendered through the debug JSON mapping

### DSG-INSPECT-004 `Debug JSON value mapping`

The crate defines a documented recursive debug JSON mapping for values that do
not fit ordinary JSON without loss:

- block hashes and child block references render as lowercase hexadecimal
  strings
- byte strings render as `{ "$type": "bytes", "hex": "<lowercase-hex>" }`
- arbitrary CBOR maps render as
  `{ "$type": "map", "entries": [{ "key": <debug-json>, "value": <debug-json> }, ...] }`
- arbitrary CBOR arrays render as JSON arrays of debug-mapped values
- text, booleans, and null-like values render as ordinary JSON values
- CBOR integers that may exceed ordinary JSON-number safety render as
  `{ "$type": "integer", "value": "<base-10-string>" }`

This mapping is a debug surface for inspection and is not promoted as a stable
protocol interchange format.

### DSG-INSPECT-005 `Inspect error taxonomy`

The CLI surfaces explicit failure categories for:

- invalid CLI input
- unsupported backend selector
- store construction failure
- block absence
- backend retrieval failure
- malformed stored content
- integrity mismatch

## Runtime Boundary

### DSG-INSPECT-006 `CLI shape`

The runtime boundary is a single binary whose invocation shape includes:

- a backend selector
- backend-specific parameters for the selected backend
- a required target block hash

This revision's filesystem backend accepts a store-root path as a backend-owned
construction input rather than widening the `BlockStore` trait.

### DSG-INSPECT-007 `Backend construction seam`

The CLI constructs the selected backend-specific store instance first, then
erases that concrete implementation behind the `BlockStore` contract for the
inspection operation.

Future backend kinds extend the selector and construction layer without
requiring a change to the existing filesystem invocation shape.

## Inspection Flow

### DSG-INSPECT-008 `Single-block inspection pipeline`

The fixed inspection flow is:

1. parse CLI input
2. validate the supplied block hash
3. construct the selected `BlockStore` implementation
4. issue exactly one `get` request for the requested block hash
5. treat `Ok(Some(validated_block))` as the only successful inspection path
6. map `validated_block` into the inspection document
7. serialize the inspection document as JSON

The CLI does not recursively inspect referenced child blocks in this revision.

### DSG-INSPECT-009 `Failure mapping`

Failures are mapped as follows:

- invalid hash syntax and other argument-shape problems become invalid CLI input
- store-construction errors become explicit store construction failures
- `Ok(None)` from the store becomes explicit block absence
- `BlockStoreError::BackendFailure` becomes explicit backend retrieval failure
- `BlockStoreError::MalformedContent` becomes explicit malformed-content failure
- `BlockStoreError::IntegrityMismatch` becomes explicit integrity-mismatch
  failure
- `BlockStoreError::ContractViolation` becomes an explicit inspection-boundary
  failure rather than silent success

### DSG-INSPECT-010 `Process I/O contract`

Successful inspection writes exactly one JSON document to standard output and
terminates successfully.

Failure writes a human-readable error to standard error and terminates with a
non-zero exit status.

## JSON Rendering

### DSG-INSPECT-011 `Typed block rendering`

The `block` field renders the typed block content in a shape that preserves the
distinction between branch and leaf blocks while keeping common block metadata
visible for debugging.

At minimum:

- both block kinds expose `version`, `embedding_spec`, and `ext`
- branch blocks expose ordered `entries` containing `embedding` and `child`
- leaf blocks expose ordered `entries` containing `embedding`, `metadata`, and
  `content`
- `content` exposes `media_type` and `body`

All byte-bearing fields use the debug JSON mapping rather than raw JSON
strings.

### DSG-INSPECT-012 `Implementation realization`

This specification package shall be realized as a Rust workspace binary crate
named `lexongraph-block-inspect` in the repository.

### DSG-INSPECT-013 `Verification realization`

The repository shall include automated verification artifacts that realize the
validation entries in `docs/specs/rust-block-inspect-cli/validation.md`.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-INSPECT-001 | REQ-INSPECT-002, REQ-INSPECT-004, REQ-INSPECT-005, REQ-INSPECT-006 |
| DSG-INSPECT-002 | REQ-INSPECT-003, REQ-INSPECT-004, REQ-INSPECT-010 |
| DSG-INSPECT-003..004 | REQ-INSPECT-007, REQ-INSPECT-008 |
| DSG-INSPECT-005 | REQ-INSPECT-009 |
| DSG-INSPECT-006..007 | REQ-INSPECT-003, REQ-INSPECT-004, REQ-INSPECT-005, REQ-INSPECT-010 |
| DSG-INSPECT-008 | REQ-INSPECT-005, REQ-INSPECT-006, REQ-INSPECT-011 |
| DSG-INSPECT-009..010 | REQ-INSPECT-009 |
| DSG-INSPECT-011 | REQ-INSPECT-007, REQ-INSPECT-008 |
| DSG-INSPECT-012 | REQ-INSPECT-001 |
| DSG-INSPECT-013 | REQ-INSPECT-012 |
