<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
📦 pca — A Deterministic, Streaming, Composable PCA Library for Rust
 
The pca crate provides a deterministic, audit‑friendly implementation of Principal Component Analysis designed for large‑scale, streaming, block‑local, and hierarchical vector indexing systems.
 
It is built around four core principles:
 
1. Streaming-first — PCA must be buildable from arbitrarily large datasets without holding all vectors in memory.  
2. Deterministic — Given the same inputs, the PCA transform must be bit‑for‑bit reproducible.  
3. Composable — PCA transforms must support composition, inversion, and delta‑PCA operations.  
4. Auditable — PCA transforms must serialize deterministically and validate cleanly.
 
This crate is not a toy dimensionality reducer.  
It is a structural transform engine.
 
---
 
🎯 Core Concepts
 
A PCA transform is represented as:
 
- a mean vector  
- an orthonormal basis matrix (eigenvectors)  
- optional singular values / explained variance  
- optional truncation dimension  
 
A PCA accumulator is a streaming structure that incrementally builds the covariance matrix and can be merged with other accumulators.
 
A PCA delta is a transform that maps vectors from one PCA space into another.
 
---
 
🧱 1. Streaming PCA Construction
 
PcaAccumulator
A structure that supports:
 
- new(dim: usize)  
- update(&mut self, v: &[f32])  
- merge(&mut self, other: &PcaAccumulator)  
- finalize() -> PcaTransform
 
This enables:
 
- building PCA from a stream  
- parallel PCA (map‑reduce style)  
- block‑local PCA  
- global PCA from partials  
 
The accumulator uses a deterministic covariance accumulation algorithm (e.g., Welford‑style).
 
---
 
🧱 2. PCA Transform
 
PcaTransform
Represents a frozen PCA transform.
 
Operations:
 
- apply(&self, v: &[f32]) -> Vec<f32>  
- apply_batch(&self, input: &[&[f32]]) -> Vec<Vec<f32>>  
- invert(&self, v: &[f32]) -> Vec<f32>  
- truncate(&self, k: usize) -> PcaTransform  
- explained_variance(&self) -> &[f32]  
 
This is the core forward/backward transform.
 
---
 
🧱 3. PCA Composition & Delta PCA
 
compose(a: &PcaTransform, b: &PcaTransform) -> PcaTransform
Produces a transform equivalent to applying a then b.
 
delta(a: &PcaTransform, b: &PcaTransform) -> PcaTransform
Produces a transform d such that:
 
`
b(v) = d(a(v))
`
 
This is essential for:
 
- block‑local PCA stacks  
- hierarchical PCA  
- re‑expressing embeddings across blocks  
- versioned PCA transforms  
 
rebase(v, from: &PcaTransform, to: &PcaTransform)
Equivalent to:
 
`
to( from⁻¹(v) )
`
 
---
 
🧱 4. Serialization & Deterministic Encoding
 
serialize(&self) -> Vec<u8>
 
deserialize(bytes: &[u8]) -> PcaTransform
 
Requirements:
 
- deterministic byte layout  
- stable float encoding  
- versioned schema  
- endian‑safe  
- hash‑friendly  
 
This is critical for:
 
- auditability  
- reproducibility  
- Merkle hashing  
- block‑local PCA stored in .ext fields  
 
---
 
🧱 5. Quantization Support
 
quantize(&self, bits: u8) -> QuantizedPcaTransform
 
dequantize(&QuantizedPcaTransform) -> PcaTransform
 
Useful for:
 
- storing PCA transforms compactly  
- block‑local PCA in LexonGraph  
- deterministic hashing  
- reducing memory footprint  
 
---
 
🧱 6. Validation & Diagnostics
 
validate(&self) -> Result<(), PcaError>
Checks:
 
- orthonormality of eigenvectors  
- determinant sign consistency  
- mean vector dimension  
- matrix shape  
- explained variance monotonicity  
 
diagnostics(&self) -> PcaDiagnostics
Returns:
 
- explained variance  
- cumulative variance  
- condition number  
- orthonormality error  
 
---
 
🧱 7. Convenience Utilities
 
fit(vectors: &[Vec<f32>]) -> PcaTransform
Non‑streaming convenience.
 
fit_truncated(vectors, k)
Directly compute top‑k PCA.
 
applyinplace(&mut [Vec<f32>])
Batch transform.
 
applydeltachain(&[PcaTransform], v)
Apply a sequence of transforms.
 
---
 
🧠 Summary: What the PCA crate must provide
 
| Category | Required Operations |
|---------|---------------------|
| Streaming | Accumulator, update, merge, finalize |
| Transforms | Apply, invert, truncate |
| Composition | Compose, delta, rebase |
| Serialization | Deterministic encode/decode |
| Quantization | Quantize/dequantize transforms |
| Validation | Orthonormality, variance, shape checks |
| Diagnostics | Explained variance, condition number |
| Convenience | Fit, fit_truncated, batch apply |
 
