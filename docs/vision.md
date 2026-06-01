# LexonGraph: Block‑Based Semantic Index (Architecture Summary)
 
1. Block Model
- Data is stored in immutable blocks.
- Each block is identified by SHA‑256(canonical_cbor_bytes).
- Blocks are small (16–32 KB) to fit within QUIC/HTTP‑3 initial congestion window.
- Blocks are encoded as canonical CBOR maps with compact integer field keys on
  wire.
- Branch blocks map embeddings to child block references.
- Leaf blocks map embeddings to metadata and content payloads.
- Each block carries a shared embedding specification so dimensions and encoding
  are not repeated per entry.
- Embeddings are stored as raw bytes interpreted under the block's embedding
  specification, so different precisions and compressed representations remain
  distinguishable.
- Entries are serialized in deterministic order so identical logical blocks hash
  identically.
- Optional summaries such as centroids may be carried as higher-level indexing
  metadata, but they are not required by the canonical block protocol.

At the protocol layer, this forms a Merkle tree.

The canonical wire and layout specification is in `docs/protocol/blocks.md`.
 
---
 
2. Transport
- All block fetches use HTTP/3 over QUIC.
- QUIC gives:
  - no head‑of‑line blocking
  - parallel independent streams
  - 0‑RTT resumption
  - large initial congestion window
- CDN terminates QUIC and caches blocks indefinitely (immutable).
 
Traversal latency is dominated by RTT × depth.
 
---
 
3. Traversal Algorithm (Frontier Expansion)
At each layer:
 
1. Maintain a frontier of n candidate blocks.
2. Fetch all n blocks in parallel (1 RTT).
3. Score all embeddings in each block (matrix–vector multiply).
4. Expand to all children of those n blocks.
5. Rank children by centroid distance.
6. Keep top n for the next layer.
7. Repeat until leaves.
 
This avoids boundary misses and is deterministic.
 
---
 
4. Quantization Strategy
To reduce block size and depth:
 
- Root + Layer 1: PQ4 or INT8  
- Middle layers: FP16  
- Leaf blocks: FP32 or BF16  
 
Upper layers only need coarse geometry; leaves need precision.
 
Quantization shrinks upper layers → more entries per block → shallower tree.
 
---
 
5. Index Construction
Because monthly deltas are small:
 
- Append new items to an ingest segment.
- When ingest reaches threshold, run balanced k‑means on only the new items.
- Produce new blocks.
- Link them into the existing tree.
- Periodically (e.g., yearly) do a full rebuild for global optimization.
 
This minimizes compute and write amplification.
 
---
 
6. Performance Model
Compute is irrelevant; network dominates.
 
- Scoring embeddings = matrix–vector multiply (fast even without SIMD).
- Block fetch cost ≈ 1 RTT (if block ≤ cwnd).
- Total latency ≈ depth × RTT.
- Depth minimized via quantization + block size tuning.
 
---
 
7. Design Principles
- Immutable blocks → perfect caching + dedupe.
- Content addressing → safety + determinism.
- Small blocks → avoid slow‑start.
- Shallow tree → minimize RTT layers.
- Frontier search → avoid boundary misses.
- Quantized upper layers → reduce depth.
 
---
