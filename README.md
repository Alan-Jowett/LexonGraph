<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# LexonGraph

[![CI](https://github.com/Alan-Jowett/LexonGraph/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Alan-Jowett/LexonGraph/actions/workflows/ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/Alan-Jowett/LexonGraph/badge.svg?branch=main)](https://coveralls.io/github/Alan-Jowett/LexonGraph?branch=main)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

LexonGraph is a Rust workspace for block-addressed semantic indexing, search,
and evaluation. This repository combines the protocol documents, traceable spec
packages, implementation crates, evaluation tooling, and repository automation
used to evolve the project.

The README is a guide to the repository surface. The authoritative behavior for
governed areas lives in `docs/protocol/` and `docs/specs/`.

## Repository at a glance

The current repository contains:

- **Protocol documents** for block encoding, search, indexing, clustering, and
  protocol evolution under `docs/protocol/`
- **Traceable specification packages** under `docs/specs/`, each using
  `requirements.md`, `design.md`, and `validation.md`
- **A 24-crate Rust workspace** spanning block formats, block stores,
  embedding-provider contracts, clustering/planning algorithms, indexing, and
  evaluator tooling
- **Supporting docs** under `docs/arch/`, `docs/audits/`, `docs/rca/`,
  `docs/research/`, and `docs/sop/`
- **Repository automation** including CI, coverage, Dependabot, local hooks, and
  repository-specific skills under `.github/skills/`

## Architecture at a glance

- **Blocks** are immutable and content-addressed by
  `sha256(canonical_cbor_bytes(block))`.
- **Version 1 blocks** are the current canonical block protocol.
- **Version 2 blocks** are being designed as a draft envelope that can coexist
  with version 1.
- **Branch blocks** link child blocks into a Merkle-linked hierarchy.
- **Leaf blocks** carry embeddings, metadata, and content payloads.
- **Embedding providers** are split between a provider-agnostic contract crate
  and concrete provider implementations.
- **Search** performs deterministic traversal over ranked candidates.
- **Indexing** builds deterministic block hierarchies from replayed item streams.
- **Clustering and planning** include DCBC, directional PCA, spherical k-means,
  PCA chunking, adaptive policy selection, and evaluator-owned benchmarking.

## Document map

| Area | Status | Path | Purpose |
| --- | --- | --- | --- |
| Vision | Supporting | `docs/vision.md` | High-level architecture summary and design direction |
| Block protocol v1 | Canonical | `docs/protocol/blocks.md` | Current block layout, invariants, and hashing rules |
| Block protocol v2 | Draft | `docs/protocol/blocks-v2.md` | Proposed version-2 block envelope and reserved types |
| Search protocol | Canonical | `docs/protocol/search.md` | Deterministic traversal, ranking, and termination semantics |
| Indexing protocol | Canonical | `docs/protocol/indexing.md` | Deterministic index-construction lifecycle and outputs |
| DCBC protocol | Canonical | `docs/protocol/dcbc.md` | Deterministic capacity-constrained balanced clustering rules |
| EBCP | Reference / future-facing | `docs/protocol/ebcp.md` | Embedding block compression protocol work referenced by the block model |
| Specification packages | Active governed | `docs/specs/` | Traceable requirements, design, and validation packages |
| Architecture notes | Supporting | `docs/arch/` | Design explorations and deeper technical background |
| Research notes | Supporting | `docs/research/` | Benchmark and clustering research artifacts |
| SOPs | Supporting | `docs/sop/` | Reproducible operational procedures |
| Audits | Supporting | `docs/audits/` | Cross-artifact drift and traceability audit records |
| Root cause analyses | Supporting | `docs/rca/` | Focused follow-up analyses for specific issues |

## Specification packages

Spec packages in `docs/specs/` use a `requirements.md` / `design.md` /
`validation.md` structure. The current packages are:

- **Repository automation:** `repository-dependabot`, `rust-workspace-ci`
- **Block model and stores:** `rust-block-crate`, `rust-block-storage-trait`,
  `rust-filesystem-block-store`, `rust-memory-block-store`,
  `rust-overlay-block-store`, `rust-zip-block-store`,
  `rust-azure-blob-block-store`, `rust-azure-blob-block-store-sdk`,
  `rust-azure-table-block-store`, `rust-azure-table-block-store-v2`
- **Search, indexing, and embeddings:** `rust-search-crate`,
  `rust-streaming-indexer-crate`, `rust-adaptive-planning-policy-crate`,
  `rust-embeddings-trait`, `rust-embeddings-openai-crate`
- **Clustering, math, and evaluation:** `rust-streaming-clustering-crate`,
  `rust-streaming-clustering-evaluator-crate`, `rust-dcbc-streaming-crate`,
  `rust-directional-pca-crate`, `rust-spherical-kmeans-crate`,
  `rust-pca-crate`, `rust-pca-chunking-crate`,
  `rust-linear-algebra-acceleration-crate`
- **CLI:** `rust-block-inspect-cli`

## Rust workspace

The top-level Cargo workspace currently contains:

| Crate | Role |
| --- | --- |
| `lexongraph-adaptive-planning-policy` | Deterministic adaptive selection between directional-PCA and DCBC planning paths |
| `lexongraph-block` | Typed block model, validation, canonical CBOR serialization, and block-hash derivation |
| `lexongraph-block-inspect` | CLI for inspecting stored blocks and rooted block trees in a filesystem block store |
| `lexongraph-block-store` | Backend-agnostic `BlockStore` trait plus conformance helpers |
| `lexongraph-block-store-azure` | Azure Blob Storage `BlockStore` implementation over container SAS URLs |
| `lexongraph-block-store-azure-sdk` | Azure Blob Storage `BlockStore` implementation built on the Azure SDK |
| `lexongraph-block-store-azure-table` | Azure Table Storage `BlockStore` implementation |
| `lexongraph-block-store-azure-table-v2` | Version-2 Azure Table Storage `BlockStore` implementation |
| `lexongraph-block-store-fs` | Local filesystem `BlockStore` implementation |
| `lexongraph-block-store-memory` | Volatile in-memory `BlockStore` implementation |
| `lexongraph-block-store-overlay` | Layered overlay `BlockStore` with cache, writable, and read-only tiers |
| `lexongraph-block-store-redb` | Durable local Redb-backed `BlockStore` implementation |
| `lexongraph-block-store-zip` | Read-only `BlockStore` backed by a zip archive with filesystem-style sharding |
| `lexongraph-dcbc-streaming` | Streaming deterministic capacity-constrained balanced clustering implementation |
| `lexongraph-directional-pca` | Streaming directional-PCA clustering implementation |
| `lexongraph-embeddings-openai` | OpenAI-compatible and Azure OpenAI embedding-provider implementation |
| `lexongraph-embeddings-trait` | Shared async embedding-provider contract plus opt-in conformance helpers |
| `lexongraph-linear-algebra-acceleration` | Execution-backend selection and dense-distance acceleration utilities |
| `lexongraph-pca` | Deterministic, streaming-first PCA transforms |
| `lexongraph-pca-chunking` | Streaming PCA projection, deterministic sorting, and exact chunking |
| `lexongraph-search` | Protocol-conforming search orchestration with trait-based policy hooks |
| `lexongraph-spherical-kmeans` | Streaming spherical k-means clustering implementation |
| `lexongraph-streaming-clustering` | Shared streaming clustering contract plus conformance helpers |
| `lexongraph-streaming-clustering-evaluator` | Benchmark harness and CLI for sectioned streaming-clustering evaluations |
| `lexongraph-streaming-indexer` | Protocol-conforming streaming indexing orchestration with replay-based ingestion |

## Published indexing profiles

`lexongraph-streaming-indexer` includes repository-owned published planning
profiles so callers can opt into pinned indexing bundles instead of wiring the
low-level planning knobs manually. The supported versions are defined in
`crates/lexongraph-streaming-indexer/src/lib.rs` and currently span
`0.1.0` through `0.7.0`.

## Contributor entrypoint

For Rust changes, start with the same workspace checks enforced by CI:

```bash
cargo fmt --check --all
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

The main CI workflow in `.github/workflows/ci.yml` currently runs:

- SPDX header checks
- workspace formatting, clippy, and tests
- conditional live Azure block-store tests when Azure-relevant paths change
- workspace coverage via `cargo llvm-cov`, uploaded to Coveralls

To enable the repository-managed local hooks, configure Git to use the versioned
hook directory:

```bash
git config core.hooksPath hooks
```

The checked-in hooks currently include:

- `hooks/check-spdx-headers`
- `hooks/pre-commit`

Repository maintenance skills live under `.github/skills/`.

## Repository layout

```text
.
|- crates/          # Rust workspace crates
|- docs/
|  |- arch/
|  |- audits/
|  |- protocol/
|  |- rca/
|  |- research/
|  |- sop/
|  |- specs/
|  `- vision.md
|- .github/
|  |- dependabot.yml
|  |- skills/
|  `- workflows/
|- hooks/
|- Cargo.toml
|- Cargo.lock
`- README.md
```

## License

MIT
