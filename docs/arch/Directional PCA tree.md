<!-- SPDX-License-Identifier: MIT
  Copyright (c) 2026 LexonGraph contributors -->
# Recursive PCA Quantization with Centroid-Biased Axis Resolution

## 1. Overall Goal

Build a hierarchical bucketing tree where:

- Each layer partitions its input points into buckets.
- Buckets are formed using PCA-aligned axes.
- Bucket counts per dimension reflect a heuristic estimate of how much resolution that dimension should receive.
- The tree becomes coarser as you move upward.
- The final structure is intended to be efficient, approximately cosine-aware, and stable enough to recurse.

This is best understood as a recursively re-centered spatial quantization scheme, not as a direct consequence of PCA theory. The aim is to use layer-local PCA to define convenient local coordinates, then allocate more or fewer cuts along each axis using a centroid-biased heuristic.

You want post-compression buckets to be roughly equal in size, but for now the focus is on the bucketing scheme itself, not compression estimation.

---

## 2. Layered Architecture

The tree is built bottom-up, and each layer is treated independently.

### Layer 0 (leaves)

- Input: raw embeddings (e.g., 160k IETF messages).
- Compute PCA on the entire dataset.
- Use PCA + centroid direction to determine bucket structure.
- Partition points into buckets.

### Layer 1

- Input: centroids of Layer 0 buckets.
- Compute PCA on these centroids.
- Use PCA + centroid direction to determine bucket structure.
- Partition Layer 0 buckets into Layer 1 buckets.

### Layer 2

- Input: centroids of Layer 1 buckets.
- Repeat the same process.

Continue until a stopping condition is met.

Each layer has its own PCA, its own centroid direction, and its own bucket allocation.

### 2.1 Recursion termination

Recursion should stop when any of the following is true:

- the layer has fewer than a minimum number of points $$n_{\min}$$
- the explained variance of the first $$k$$ PCs falls below a threshold
- the effective rank falls below $$r_{\min}$$
- centroid shift magnitude falls below $$\varepsilon$$ across successive layers
- a maximum depth is reached

Without explicit termination, upper layers will tend to perform PCA on noise and amplify discretization artifacts.

---

## 3. PCA at Each Layer

For each layer:

1. Compute PCA on the points entering that layer.
2. Extract:
   - Eigenvectors (PC axes)
   - Eigenvalues (variance per axis)
3. Compute the centroid of the layer’s points.
4. Project the centroid onto each PC axis:

$$
\alpha_i = c \cdot \mathrm{PC}_i
$$

Call $$\alpha_i$$ the directional bias coefficient for axis $$i$$. It measures how strongly the layer centroid is aligned with that principal axis.

That is not the same thing as PCA importance:

- Eigenvalues $$\lambda_i$$ measure spread structure along axis $$i$$.
- Directional bias coefficients $$\alpha_i$$ measure global directional skew along axis $$i$$.

This design uses directional bias as a signal for allocating resolution. It should therefore be described as a heuristic for axis allocation, not as a claim that centroid projection alone determines semantic importance.

---

## 4. Determining Bucket Counts per Dimension

This is the core of the scheme.

### 4.1 Signals used for bucket allocation

- $$|\alpha_i|$$ is a centroid-alignment signal.
- $$\lambda_i$$ is a variance signal.
- Either can be used to influence how much resolution axis $$i$$ receives.

The simplest version uses only centroid alignment:

$$
s_i = |\alpha_i|
$$

However, a more robust default is to blend the two:

$$
s_i = |\alpha_i| \lambda_i^\gamma
$$

where $$\gamma \in [0, 1]$$ controls how much variance influences bucket allocation.

This prevents a low-variance axis with strong centroid projection from dominating the budget, while still allowing centroid direction to bias the partitioning.

### 4.2 Why linear scaling is wrong

Because bucket counts multiply across dimensions:

$$
\text{Total buckets} = \prod_i b_i
$$

Linear scaling would make the highest-scoring axis dominate exponentially.

### 4.3 Temperature-controlled axis allocation

Use:

$$
w_i = \log(1 + s_i)
$$

Then convert the damped scores into a temperature-controlled allocation:

$$
\tilde{p}_i = \frac{\exp(w_i / \tau)}{\sum_j \exp(w_j / \tau)}
$$

where $$\tau > 0$$ is a temperature parameter. Lower temperatures make allocation sharper; higher temperatures make it flatter.

Allocate per-axis bin counts from an axis-resolution budget $$B_{\text{axes}}$$:

$$
b_i = \max(1, \mathrm{round}(B_{\text{axes}} \cdot \tilde{p}_i))
$$

If an exact sum constraint matters, a correction step can adjust the rounded counts so that $$\sum_i b_i$$ matches the intended axis budget.

### 4.4 Axis budget versus cell count

Two different quantities must be kept separate:

- Axis-resolution budget:

$$
B_{\text{axes}}
$$

- Actual number of grid cells:

$$
B_{\text{cells}} = \prod_i b_i
$$

These are not the same thing. $$B_{\text{axes}}$$ controls marginal resolution across axes, while $$B_{\text{cells}}$$ is the combinatorial size of the Cartesian grid induced by those choices.

### Effect of temperature-controlled allocation

- The highest-scoring axis still gets the most bins.
- Secondary axes can still get meaningful bins.
- Higher PCs get few but nonzero bins.
- No dimension is completely ignored.
- The temperature parameter provides an explicit stability knob.

This preserves multi-dimensional structure while reducing regime-switch behavior from hard exponential allocation. Bucket counts are still discrete, but their sensitivity is now tunable instead of accidental.

---

## 5. Spaces and Coordinate Systems

Each layer operates on points in three distinct spaces:

- the original embedding space
- the PCA coordinate space defined for that layer
- the discretized grid induced by the chosen bins

Partitioning occurs in PCA coordinate space, not directly in the original embedding space.

If the PCA basis at a layer is $$U$$ and the layer centroid is $$c$$, then points are first transformed to:

$$
z = U^\top (x - c)
$$

Bucketing is then performed on $$z$$. Buckets are therefore axis-aligned cells in PCA space, not metric balls in the original embedding space.

This is why the scheme should be described as approximately cosine-aware only in a weak heuristic sense: the partition uses a rotated coordinate system that may correlate with cosine structure, but it does not preserve cosine neighborhoods exactly.

---

## 6. Partitioning Points into Buckets

Once bucket counts $$b_i$$ are determined:

1. Project each point onto the first $$k$$ PCs.
2. For each dimension $$i$$, choose a binning policy and divide that axis into $$b_i$$ bins.
3. A point’s bucket ID is the tuple of its bin indices:

$$
(d_1, d_2, \dots, d_k)
$$

4. The combination of all dimensions yields the final bucket.

This is a PCA-aligned grid, but with dimension-specific resolution based on the chosen axis-allocation signal.

### 6.1 Default binning policy

The default binning policy should be quantile binning.

- Quantile bins keep occupancy more stable.
- They behave better under heavy-tailed or clustered projections.
- They reduce the chance that most points collapse into a few central cells.

Equal-width bins should be treated as diagnostic-only or deprecated behavior. They assume projection ranges are informative and reasonably well behaved, which is usually false in deeper layers.

### 6.2 Deterministic duplicate refinement

Some datasets contain exact duplicate embeddings, including the degenerate case
where an entire layer is identical. Quantile binning can still under-realize the
requested bucket count when those duplicates collapse into fewer populated cells
than the hard target.

When that shortfall is caused by indistinguishable members rather than by an
ordinary allocation failure, the implementation may apply a narrow deterministic
fallback after the primary PCA-plus-quantile partition is formed:

1. detect that the shortfall is attributable to duplicate-collapse rather than a
   generic exact-K failure
2. preserve the primary geometric partition unchanged
3. refine only the collapsed duplicate members with a stable non-geometric
   tie-break derived from the layer's observed point order

This fallback is intentionally narrow. It is not a general license to force
exact-K for arbitrary infeasible partitions, and it should not replace the
documented PCA, allocation, and quantile-binning path.

---

## 7. Expected Benefits and Design Intuition

This scheme gives you:

### 7.1 Approximately cosine-aware partitioning

Bucket resolution is biased toward directions that align with the layer centroid, which may improve retrieval locality when centroid direction tracks semantic bias.

### 7.2 Multi-scale semantic structure

Each layer sees a different distribution and gets its own PCA.

### 7.3 Computational efficiency

- Layer 0 PCA is the only expensive one.
- Higher layers get exponentially smaller.

### 7.4 Balanced buckets (pre-compression)

Log-damped axis allocation reduces axis-collapse pressure, and quantile bins can reduce occupancy imbalance.

### 7.5 Preservation of important semantic axes

Primary and secondary PCs can both retain meaningful resolution.

### 7.6 Avoidance of single-axis collapse

Log scaling helps prevent the top-scoring axis from monopolizing the partitioning.

---

## 8. Structural Risks and Failure Modes

This design is coherent, but several failure modes are likely to dominate real behavior:

### 8.1 Bucket-count jitter

The combination of log scaling, normalization, exponentiation, and rounding can make bucket counts unstable across nearby layers or nearby datasets.

### 8.2 Sparse multiplicative grids

Because the final bucket set is a Cartesian product, the number of possible cells grows quickly. At higher layers this can produce many empty or singleton buckets.

### 8.3 Axis-aligned approximation error

Cosine neighborhoods are angular, while this scheme cuts space with axis-aligned bins in PCA coordinates. That mismatch is an approximation, not a geometric equivalence.

### 8.4 Recursive PCA drift

As layers get smaller, centroids become noisier and PCA bases become less stable. The hierarchy can therefore amplify discretization artifacts instead of smoothing them.

### 8.5 Taxonomy collapse

In deep layers, the hierarchy may converge to partitions that reflect discretization history more than embedding geometry. At that point the tree behaves more like a recursively induced taxonomy than a faithful geometric summary.

### 8.6 Duplicate-collapse degeneracy

Exact duplicate embeddings can survive projection as indistinguishable retained
coordinates. When many such points land in the same populated cell, the layer
can under-realize the requested bucket count even when $$N \ge K$$.

---

## 9. Minimal Stabilizers

Without changing the overall philosophy, the following additions make the scheme more defensible:

### 9.1 Blend centroid alignment with variance

Use $$s_i = |\alpha_i| \lambda_i^\gamma$$ instead of only $$|\alpha_i|$$.

### 9.2 Prefer quantile bins

Use quantile bins by default to reduce occupancy collapse and heavy-tail sensitivity.

### 9.3 Add layer-stability checks

Use at least one stability constraint between layers, such as:

- a minimum point-count threshold before recomputing PCA
- a minimum explained-variance threshold
- subspace similarity checks against the previous layer
- Procrustes-style basis alignment when basis continuity matters

### 9.4 Optional stochastic smoothing

Small stochastic tie-breaking or jitter can reduce brittle boundary artifacts and hard discontinuities in assignment. This is optional, but useful when deterministic boundaries create unstable occupancies.

### 9.5 Deterministic duplicate refinement

When exact-K failure is caused specifically by duplicate-collapse, prefer a
deterministic refinement of the collapsed duplicate members over random jitter.
The tie-break should be stable for the same observed layer order and should be
used only to recover missing buckets trapped inside duplicate cells.

---

## 10. What This Scheme Does Not Yet Do

And that’s fine — you said you’re not going there yet.

- It does not estimate compressed size.
- It does not adjust bucket capacity based on variance.
- It does not enforce equal compressed size.
- It does not guarantee stable occupancy under recursion.
- It does not guarantee that axis-aligned cuts preserve semantic neighborhoods.
- It does not prevent taxonomy collapse on its own.

Those are future refinements.

---

## 11. The Scheme in One Sentence

A recursively re-centered, layer-local PCA quantization scheme that allocates per-axis resolution from a temperature-controlled axis budget using directional bias, optionally tempered by variance, and partitions points into axis-aligned cells in PCA space using density-aware binning.
