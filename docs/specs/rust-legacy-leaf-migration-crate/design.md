<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Legacy Leaf Migration Crate Design

## Status

Draft design specification for a disposable Rust crate that migrates historical
LexonGraph leaf blocks into the current protocol encoding.

## Design Goals

The crate design is intended to be:

- isolated from steady-state runtime crates
- explicit about source and destination boundaries
- deterministic in its manifest output
- strict about legacy-input scope
- disposable after one migration campaign

## Crate Boundary

The crate owns:

- legacy leaf decoding for the historical `kind = "leaf"` encoding
- raw source-corpus traversal for the filesystem-backed legacy corpus
- orchestration of source read, translate, publish, and manifest emission
- deterministic manifest generation

The crate does not own:

- steady-state block decoding
- production storage contracts
- parent regeneration
- runtime compatibility for legacy blocks

## Core Design

### DSG-MIG-001 `Separate crate boundary`

A standalone binary-oriented crate owns the migration workflow. No runtime crate
depends on it.

### DSG-MIG-002 `Source filesystem traversal`

The tool walks the source filesystem root using the published sharded block-file
layout and reads raw bytes directly from source files rather than through the
current `BlockStore` `get` surface.

### DSG-MIG-003 `Legacy leaf decoder`

The tool decodes version-1 CBOR maps whose top-level key `1` is textual
`kind`. Only `kind = "leaf"` is accepted for migration.

### DSG-MIG-004 `Typed translation`

The decoded legacy leaf is translated into the current typed `LeafBlock` model
with `level = 0`, preserving embedding spec, embedding bytes, metadata,
content, and `ext`.

### DSG-MIG-005 `Destination publication`

The destination store is a current filesystem-backed `BlockStore`
implementation. Publication of migrated blocks reuses the current store crate
rather than reimplementing canonical serialization or on-disk publication.

### DSG-MIG-006 `Manifest determinism`

The tool emits manifest rows sorted lexicographically by legacy block
identifier so output is deterministic across filesystem traversal order.

### DSG-MIG-007 `Operational safety`

The tool refuses in-place migration when source and destination roots resolve to
the same canonical directory and fails explicitly on source-integrity,
destination-publication, and manifest-write errors.

### DSG-MIG-008 `Disposable lifecycle`

The crate is specified as removable after the migration campaign completes,
without changing steady-state repository behavior.

## Traceability

| Design ID | Satisfies |
|---|---|
| DSG-MIG-001, DSG-MIG-008 | REQ-BLOCK-MIG-001, REQ-BLOCK-MIG-008 |
| DSG-MIG-002 | REQ-BLOCK-MIG-002, REQ-BLOCK-MIG-006 |
| DSG-MIG-003 | REQ-BLOCK-MIG-003, REQ-BLOCK-MIG-004 |
| DSG-MIG-004 | REQ-BLOCK-MIG-005, REQ-BLOCK-MIG-009 |
| DSG-MIG-005 | REQ-BLOCK-MIG-005, REQ-BLOCK-MIG-006 |
| DSG-MIG-006 | REQ-BLOCK-MIG-007 |
| DSG-MIG-007 | REQ-BLOCK-MIG-002, REQ-BLOCK-MIG-010 |
