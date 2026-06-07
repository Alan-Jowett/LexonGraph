<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Legacy Leaf Migration Crate Requirements

## Status

Draft specification for a disposable Rust crate that migrates historical
LexonGraph leaf blocks generated before
`127c0e58ced80b734a04abfc506708bfd171b219`.

## Scope

This document specifies a standalone migration crate that rewrites a
filesystem-backed corpus of legacy `kind = "leaf"` blocks into the current
`level = 0` block encoding.

This crate is operational tooling only. It is not part of the steady-state
runtime block, search, indexing, or storage surfaces.

## Requirements

### REQ-BLOCK-MIG-001

The repository shall include a separate Rust crate whose sole purpose is
migrating historical leaf blocks encoded before
`127c0e58ced80b734a04abfc506708bfd171b219`.

### REQ-BLOCK-MIG-002

The migration crate shall be offline and one-way: it shall read a legacy source
corpus, write current canonical blocks to a separate destination store, and
shall not modify the source corpus.

### REQ-BLOCK-MIG-003

The migration crate shall accept only the historical leaf encoding variant:
version-1 integer wire keys with top-level key `1` carrying textual
`kind = "leaf"`.

### REQ-BLOCK-MIG-004

The migration crate shall reject historical non-leaf inputs, malformed inputs,
and unsupported legacy variants explicitly.

### REQ-BLOCK-MIG-005

The migration crate shall translate imported legacy leaves into the current
typed leaf model and publish them through the current canonical serializer so
destination bytes and block identifiers conform to the current published
protocol.

### REQ-BLOCK-MIG-006

The migration crate shall write migrated blocks to a destination filesystem
block store separate from the source corpus.

### REQ-BLOCK-MIG-007

The migration crate shall emit a deterministic manifest mapping each legacy
block identifier to its migrated block identifier.

### REQ-BLOCK-MIG-008

The migration crate shall not be a dependency of steady-state runtime crates and
shall be removable after migration without altering normal block decode, search,
indexing, or storage behavior.

### REQ-BLOCK-MIG-009

The migration scope is a leaf-only corpus; the crate shall not rebuild parents
or other higher-level graph structure.

### REQ-BLOCK-MIG-010

The migration crate shall fail explicitly on destination conflicts, unreadable
source files, malformed manifest output, or any condition that would make the
migration result incomplete or ambiguous.

## Out of Scope

This crate does not define or own:

- steady-state runtime decoding of legacy blocks
- compatibility widening of `lexongraph-block`
- compatibility widening of `BlockStore`
- parent regeneration or tree rewriting
- in-place mutation of the source corpus
