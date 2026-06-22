<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# LexonGraph

[![CI](https://github.com/Alan-Jowett/LexonGraph/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Alan-Jowett/LexonGraph/actions/workflows/ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/Alan-Jowett/LexonGraph/badge.svg?branch=main)](https://coveralls.io/github/Alan-Jowett/LexonGraph?branch=main)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

LexonGraph is a semantic indexing and retrieval system built around immutable,
content-addressed blocks. The repository now includes the canonical protocol
documents, traceable specification packages, an implemented Rust workspace for
the core repository surface, and CI that enforces the workspace quality gates.

## Repository status

LexonGraph is still evolving, but the repository is no longer just an
architecture sketch. It currently contains:

- **Active governed and implemented surface**
  - canonical protocol documents for blocks, search, indexing, and DCBC
  - requirements/design/validation spec packages for the current Rust workspace
    and repository automation
  - implemented Rust crates for blocks, storage contracts, filesystem storage,
    deterministic clustering, indexing, search, and embedding-provider
    integration
- **Active repository maintenance surface**
  - GitHub Actions CI for formatting, linting, tests, and coverage reporting
  - Dependabot configuration for Cargo and GitHub Actions dependency updates
  - repository maintenance skills under `.github/skills/`
- **Supporting, reference, and future-facing material**
  - architecture notes, audits, and RCA documents under `docs/arch/`,
    `docs/audits/`, and `docs/rca/`
  - `docs/protocol/ebcp.md` as reference or future protocol work rather than
    part of the active governed implementation surface

The README is a summary. The protocol documents in `docs/protocol/` and the
traceable packages in `docs/specs/` are the authoritative sources for protocol
and specification behavior.

## Architecture at a glance

- **Immutable blocks** are encoded as canonical CBOR maps and addressed by
  `sha256(canonical_cbor_bytes(block))`.
- **Branch blocks** point to child blocks, forming a Merkle-linked structure.
- **Leaf blocks** carry embeddings, metadata, and inline content payloads.
- **Embedding providers** are split between a provider-agnostic trait crate and
  concrete provider implementations.
- **Search** uses deterministic frontier expansion over ranked candidates.
- **Indexing** builds deterministic block sets from application-supplied items
  and uses DCBC-backed packing by default.
- **DCBC** provides deterministic capacity-constrained balanced clustering for
  clustering and packing workflows.

## Document map

| Area | Status | Document or path | Purpose |
| --- | --- | --- | --- |
| Vision | Supporting | `docs/vision.md` | High-level architecture summary and design direction |
| Block protocol | Active governed | `docs/protocol/blocks.md` | Canonical block encoding, invariants, and block identity |
| Search protocol | Active governed | `docs/protocol/search.md` | Deterministic traversal, ranking, and termination semantics |
| Indexing protocol | Active governed | `docs/protocol/indexing.md` | Deterministic index-construction inputs, invariants, and outputs |
| DCBC protocol | Active governed | `docs/protocol/dcbc.md` | Deterministic capacity-constrained balanced clustering rules |
| EBCP | Reference / future-facing | `docs/protocol/ebcp.md` | Embedding block compression protocol work that is not part of the active governed implementation surface in this pass |
| Architecture notes | Supporting | `docs/arch/` | Design explorations and deeper technical background |
| Audits | Supporting | `docs/audits/` | Cross-artifact drift and traceability audit records |
| Root cause analyses | Supporting | `docs/rca/` | Focused follow-up analyses for specific drift or implementation issues |

## Specification packages

The repository uses spec packages under `docs/specs/`. Each package follows the
same structure:

- `requirements.md` for the required behavior and boundaries
- `design.md` for the derived design
- `validation.md` for the verification surface

Current packages cover:

- `repository-dependabot`
- `rust-block-crate`
- `rust-block-inspect-cli`
- `rust-block-storage-trait`
- `rust-dcbc-streaming-crate`
- `rust-directional-pca-crate`
- `rust-embeddings-openai-crate`
- `rust-embeddings-trait`
- `rust-filesystem-block-store`
- `rust-pca-crate`
- `rust-search-crate`
- `rust-streaming-clustering-crate`
- `rust-streaming-indexer-crate`
- `rust-workspace-ci`

## Rust workspace

The top-level Cargo workspace currently contains:

| Crate | Role |
| --- | --- |
| `lexongraph-block` | Typed block model, validation, canonical CBOR serialization, and block-hash derivation |
| `lexongraph-block-inspect` | CLI for inspecting canonical block encodings and decoded block structure |
| `lexongraph-block-store` | Backend-agnostic `BlockStore` trait plus conformance harnesses |
| `lexongraph-block-store-azure` | Azure Blob Storage implementation of the block-store contract over container SAS URLs |
| `lexongraph-block-store-fs` | Local filesystem implementation of the block-store contract |
| `lexongraph-dcbc-streaming` | Deterministic streaming DCBC clustering implementation |
| `lexongraph-directional-pca` | Deterministic directional PCA utilities for streaming clustering workflows |
| `lexongraph-embeddings-trait` | Shared async embedding-provider contract plus opt-in conformance helpers |
| `lexongraph-embeddings-openai` | OpenAI-compatible and Azure OpenAI embedding-provider implementation |
| `lexongraph-pca` | Deterministic, streaming-first PCA accumulation, affine transform algebra, and stable transform artifact encoding |
| `lexongraph-search` | Protocol-conforming search orchestration with trait-based policy hooks |
| `lexongraph-streaming-clustering` | Shared streaming clustering contract plus conformance helpers |
| `lexongraph-streaming-indexer` | Protocol-conforming streaming indexing orchestration with replay-based ingestion |

The workspace includes multiple `BlockStore` backends. The primary persistence
backends implemented in this repository are the local filesystem block store
and an Azure Blob Storage block store over container SAS URLs, alongside
memory, overlay, and zip variants for in-memory, composed, and read-only
archive scenarios.

## Published streaming-indexer profiles

The streaming indexer exposes versioned published profiles so callers can select
an explicit repository-owned indexing bundle without wiring the low-level
planning knobs manually.

| Profile | Planning bundle | What it does |
| --- | --- | --- |
| `0.1.0` | Spherical k-means + greedy-pack hierarchy + exact-centroid summaries | Forms terminal groups with repository-owned spherical k-means settings, then greedily packs those groups into a finalized partition hierarchy using Euclidean centroid distance before the existing bottom-up block materialization flow persists the tree. |
| `0.2.0` | Divisive directional-PCA + exact-centroid summaries | Uses the existing built-in directional-PCA planning path with `Divisive` hierarchy construction and pinned `cluster_count = 2` to derive the finalized partition hierarchy, then reuses the same bottom-up block materialization flow to persist the tree. |
| `0.3.0` | Divisive directional-PCA + exact-centroid summaries | Uses the same built-in directional-PCA planning path, hierarchy construction, and summary policy as `0.2.0`, but pins `cluster_count = 64`, selects adaptive retained-axis participation, and uses density-valley axis partitioning before reusing the same bottom-up block materialization flow to persist the tree. |

All published profile versions remain explicitly selectable. The low-level streaming
indexer APIs are still available for callers that want direct control over
planning realization, direction, and settings.

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

The repository also defines `.github/dependabot.yml` for weekly Cargo and
GitHub Actions dependency update proposals.

To enable the repository-managed local checks, configure Git to use the
versioned hook directory:

```bash
git config core.hooksPath hooks
```

The `pre-commit` hook enforces SPDX headers on staged governed files, and CI
re-checks the full tracked repository surface.

Repository maintenance skills live under `.github/skills/` and support
specification and maintenance workflows for this repository.

## Repository layout

```text
.
|- crates/
|  |- lexongraph-block
|  |- lexongraph-block-inspect
|  |- lexongraph-block-store
|  |- lexongraph-block-store-azure
|  |- lexongraph-block-store-fs
|  |- lexongraph-dcbc-streaming
|  |- lexongraph-directional-pca
|  |- lexongraph-embeddings-openai
|  |- lexongraph-embeddings-trait
|  |- lexongraph-pca
|  |- lexongraph-search
|  |- lexongraph-streaming-clustering
|  `- lexongraph-streaming-indexer
|- docs/
|  |- arch/
|  |- audits/
|  |- protocol/
|  |- rca/
|  |- specs/
|  `- vision.md
|- .github/
|  |- dependabot.yml
|  |- skills/
|  `- workflows/ci.yml
|- hooks/
|- Cargo.toml
`- README.md
```

## Current focus

The repository is centered on keeping the protocol surface, crate-level specs,
and implemented Rust workspace aligned:

- protocol-first definitions in `docs/protocol/`
- traceable crate-level requirements in `docs/specs/`
- verification-backed Rust implementations in `crates/`

## License

MIT
