<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Rust Legacy Leaf Migration Crate Validation

## Status

Draft validation specification for a disposable Rust crate that migrates
historical LexonGraph leaf blocks into the current protocol encoding.

## Validation Entries

### VAL-MIG-001

Inspect the repository manifests and test layout for the migration crate.

**Pass condition:** the repository includes the dedicated migration crate and
its verification artifacts.

**Traces to:** REQ-BLOCK-MIG-001, REQ-BLOCK-MIG-008

### VAL-MIG-002

Inspect the migration CLI help surface.

**Pass condition:** the CLI exposes the filesystem migration mode and its source,
destination, and manifest inputs.

**Traces to:** REQ-BLOCK-MIG-001, REQ-BLOCK-MIG-006

### VAL-MIG-003

Migrate a sample filesystem corpus of legacy `kind = "leaf"` blocks.

**Pass condition:** the destination store loads the migrated blocks through the
current `BlockStore` path, migrated blocks are leaf blocks at `level = 0`, the
source corpus remains unchanged, and the manifest rows are deterministic.

**Traces to:** REQ-BLOCK-MIG-002, REQ-BLOCK-MIG-005, REQ-BLOCK-MIG-006,
REQ-BLOCK-MIG-007, REQ-BLOCK-MIG-009

### VAL-MIG-004

Attempt to migrate a historical non-leaf input.

**Pass condition:** the tool fails explicitly rather than attempting parent or
branch migration.

**Traces to:** REQ-BLOCK-MIG-003, REQ-BLOCK-MIG-004, REQ-BLOCK-MIG-009

### VAL-MIG-005

Force the manifest path to collide with an existing file.

**Pass condition:** the tool fails explicitly with a manifest-write failure
rather than silently overwriting the manifest.

**Traces to:** REQ-BLOCK-MIG-007, REQ-BLOCK-MIG-010
