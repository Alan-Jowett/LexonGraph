# LexonGraph
 
LexonGraph is a log‑structured semantic index for large archival corpora.  
It stores embeddings as immutable blocks in a navigable vector graph, enabling fast
centroid‑guided search over millions of documents using only simple object storage
(local filesystem or Azure Blob Storage) fronted by a CDN.
 
LexonGraph is designed for LLM‑native retrieval: an MCP server or agent can walk the
graph using a query embedding and fetch only the blocks needed to answer a question.
 
---
 
Features
 
- Semantic vector graph  
  Embeddings are organized into a Merkle tree of immutable blocks. Each branch
  block stores embedding-keyed entries that point to child blocks, while leaf
  blocks store embedding-keyed entries with metadata and content. Higher-level
  summaries such as centroids can be layered on top.
 
- Log‑structured storage  
  All updates are append‑only. New blocks are created during monthly rebuilds and
  old blocks are retired via tombstones.
 
- Immutable, CDN‑friendly blocks  
  Every block is content‑addressed and safe to cache indefinitely. No in‑place
  mutation, no locking, no race conditions.
 
- Local or cloud backends  
  Works with:
  - Local filesystem (for offline or embedded use)
  - Azure Blob Storage (for global, CDN‑accelerated access)
 
- Efficient monthly compaction  
  New mailbox archives or document batches are ingested and merged into the index
  during a scheduled rebuild. Write amplification is amortized across the batch.
 
- Time‑travel and auditability  
  Archived block lists and link tombstones allow reconstruction of the graph at any
  historical point and safe reachability‑based garbage collection.
 
- LLM‑native query model  
  A query embedding is routed from the root through the block hierarchy to
  relevant chunks, messages, threads, or topics.
 
---
 
How It Works
 
1. Immutable Blocks
Each block contains:
- A canonical CBOR payload
- A block-scoped embedding specification
- A sorted set of embedding-keyed entries
- Either branch entries with child references or leaf entries with payloads
- Optional extension metadata
 
Blocks are never modified after creation.
The format remains map-based so it can evolve without forcing positional tuple
compatibility rules.
 
2. Append‑Only Logs
LexonGraph maintains:
- Archived Block List — records retired blocks  
- Link Log — records link creation  
- Link Tombstones — record link removal  
 
These logs allow full reconstruction and safe garbage collection.
 
3. Monthly Rebuild
When new data arrives:
1. New chunks/messages are embedded and appended.
2. A compaction pass clusters embeddings and builds new index blocks.
3. New blocks are written; old ones are retired.
4. A new root manifest is published.
 
4. Querying
A client (e.g., an MCP server):
1. Computes a query embedding.
2. Fetches the root block.
3. Greedily descends by scoring the embeddings or summaries stored in each
   block.
4. Repeats until reaching leaf blocks.
5. Returns the top‑K relevant items.
 
All reads are simple HTTP GETs, ideal for CDN caching.
 
---
 
Storage Layout
 
```
/lexongraph/
    /blocks/
        /2026-05/
            block-abc123.cbor
            block-def456.cbor
    /manifests/
        root-2026-05.json
    /logs/
        archived-blocks-2026-05.jsonl
        links-2026-05.jsonl
        tombstones-2026-05.jsonl
```
 
Everything is immutable and content‑addressed.
The canonical block format is defined in `docs/protocol/blocks.md`.
 
---
 
Use Cases
 
- Long‑term archival search (e.g., 40 years of IETF mailing lists)
- Semantic retrieval for LLM agents
- Distributed or offline‑first knowledge stores
- Large document collections with infrequent updates
- Systems requiring reproducible, versioned semantic indexes
 
---
 
Roadmap
 
- Rust and Python client libraries
- Parallel compaction pipeline
- Optional HNSW‑style shortcut links
- Bloom‑filter‑based freshness overlay
- WASM‑based local query engine
- MCP server integration
 
---
 
License
 
MIT
 
---
 
Status
 
LexonGraph is in early development.  
APIs and block formats may evolve as the system stabilizes. The canonical block
protocol is specified in `docs/protocol/blocks.md`.
 
---
 
