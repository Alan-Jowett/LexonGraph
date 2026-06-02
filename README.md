<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# LexonGraph

LexonGraph is a semantic indexing and retrieval system built around immutable,
content-addressed blocks. The repository now includes the canonical protocol
documents, traceable specification packages, a Rust workspace that implements
the current core crates, and CI that enforces the workspace quality gates.

## Repository status

LexonGraph is still evolving, but this repository is no longer just an
architecture sketch. It currently contains:

- canonical protocol documents for blocks, search, indexing, and DCBC
- requirements/design/validation spec packages for the current Rust work
- implemented Rust crates for blocks, storage contracts, filesystem storage,
  indexing, and search
- GitHub Actions CI for formatting, linting, and tests

The README is a summary. The protocol documents in `docs/protocol/` are the
authoritative source for wire format and protocol behavior.

## Architecture at a glance

- **Immutable blocks** are encoded as canonical CBOR maps and addressed by
  `sha256(canonical_cbor_bytes(block))`.
- **Branch blocks** point to child blocks, forming a Merkle-linked structure.
- **Leaf blocks** carry embeddings, metadata, and inline content payloads.
- **Search** uses deterministic frontier expansion over ranked candidates.
- **Indexing** builds deterministic block sets from application-supplied items.
- **DCBC** defines deterministic capacity-constrained balanced clustering for
  clustering and packing workflows.

## Core documents

| Area | Document | Purpose |
| --- | --- | --- |
| Vision | `docs/vision.md` | High-level architecture summary and design direction |
| Block protocol | `docs/protocol/blocks.md` | Canonical block encoding, invariants, and block identity |
| Search protocol | `docs/protocol/search.md` | Deterministic traversal, ranking, and termination semantics |
| Indexing protocol | `docs/protocol/indexing.md` | Deterministic index-construction inputs, invariants, and outputs |
| DCBC protocol | `docs/protocol/dcbc.md` | Deterministic capacity-constrained balanced clustering rules |

## Specification packages

The repository uses spec packages under `docs/specs/`. Each package follows the
same structure:

- `requirements.md` for the required behavior and boundaries
- `design.md` for the derived design
- `validation.md` for the verification surface

Current packages cover:

- `rust-block-crate`
- `rust-block-storage-trait`
- `rust-filesystem-block-store`
- `rust-indexer-crate`
- `rust-search-crate`
- `rust-workspace-ci`

## Rust workspace

The top-level Cargo workspace currently contains:

| Crate | Role |
| --- | --- |
| `lexongraph-block` | Typed block model, validation, canonical CBOR serialization, and block-hash derivation |
| `lexongraph-block-store` | Backend-agnostic `BlockStore` trait plus conformance harnesses |
| `lexongraph-block-store-fs` | Local filesystem implementation of the block-store contract |
| `lexongraph-indexer` | Protocol-conforming indexing orchestration with trait-based policy hooks |
| `lexongraph-search` | Protocol-conforming search orchestration with trait-based policy hooks |

At the moment, the implemented storage backend in this repository is the local
filesystem block store. Broader deployment shapes remain part of the overall
architecture direction rather than the current workspace implementation.

## Contributor entrypoint

For Rust changes, use the same workspace commands enforced by CI:

```bash
cargo fmt --check --all
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

The CI workflow lives in `.github/workflows/ci.yml` and currently runs on:

- pushes to `main`
- pull requests targeting `main` (filtered via `paths:` to repository-quality-relevant files including Rust workspace files, docs, hooks, `.gitignore`, `.gitattributes`, and workflow configuration)

To enable the repository-managed local checks, configure Git to use the
versioned hook directory:

```bash
git config core.hooksPath hooks
```

The `pre-commit` hook enforces SPDX headers on staged governed files, and CI
re-checks the full tracked repository surface.

## Repository layout

```text
.
|- crates/
|  |- lexongraph-block
|  |- lexongraph-block-store
|  |- lexongraph-block-store-fs
|  |- lexongraph-indexer
|  `- lexongraph-search
|- docs/
|  |- protocol/
|  |- specs/
|  `- vision.md
|- .github/workflows/ci.yml
|- hooks/
|- Cargo.toml
`- README.md
```

## Current focus

The repository is centered on making the protocol surface and Rust
implementation converge cleanly:

- protocol-first definitions in `docs/protocol/`
- traceable crate-level requirements in `docs/specs/`
- verification-backed Rust implementations in `crates/`

## License

MIT
