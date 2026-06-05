// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

//! Single-layer directional-PCA partitioning for LexonGraph block IDs.

use std::collections::BTreeMap;
use std::fmt;

use half::f16;
pub use lexongraph_block::{BlockHash, EmbeddingSpec};
use lexongraph_block::{TypedEntries, into_entries};
use lexongraph_block_store::{BlockStore, BlockStoreError};
use lexongraph_pca::{PcaError, fit};

#[derive(Clone, Debug, PartialEq)]
pub struct DirectionalPcaLayerInput {
    pub block_ids: Vec<BlockHash>,
    pub params: DirectionalPcaLayerParams,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DirectionalPcaLayerParams {
    pub retained_dimension_count: usize,
    pub axis_resolution_budget: usize,
    pub variance_exponent: f32,
    pub temperature: f32,
    pub min_input_count: usize,
    pub min_effective_rank: usize,
    pub min_cumulative_variance: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DirectionalPcaLayerOutcome {
    Partitioned(DirectionalPcaLayerResult),
    Ineligible(DirectionalPcaEligibility),
}

#[derive(Clone, Debug, PartialEq)]
pub struct DirectionalPcaLayerResult {
    pub embedding_spec: EmbeddingSpec,
    pub groups: Vec<DirectionalPcaGroup>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DirectionalPcaGroup {
    pub centroid: Vec<f32>,
    pub member_block_ids: Vec<BlockHash>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DirectionalPcaEligibility {
    InsufficientInputCount { actual: usize, minimum: usize },
    InsufficientExplainedVariance { actual: f32, minimum: f32 },
    InsufficientEffectiveRank { actual: usize, minimum: usize },
}

#[derive(Clone, Debug, PartialEq)]
pub enum DirectionalPcaError {
    MissingBlock {
        block_id: BlockHash,
    },
    BlockStore(BlockStoreError),
    EmptyBlockEmbeddings {
        block_id: BlockHash,
    },
    IncompatibleEmbeddingSpec {
        expected: EmbeddingSpec,
        actual: EmbeddingSpec,
        block_id: BlockHash,
    },
    UnsupportedEncoding {
        encoding: String,
    },
    InvalidEmbeddingLength {
        encoding: String,
        dims: u64,
        expected: usize,
        actual: usize,
    },
    NonFiniteValue {
        block_id: BlockHash,
        entry_index: usize,
        dimension_index: usize,
    },
    InvalidRetainedDimension {
        requested: usize,
        available: usize,
    },
    InvalidAxisResolutionBudget {
        axis_resolution_budget: usize,
        minimum_required: usize,
    },
    InvalidTemperature {
        temperature: f32,
    },
    InvalidMinimumInputCount {
        min_input_count: usize,
    },
    InvalidMinimumEffectiveRank {
        min_effective_rank: usize,
        retained_dimension_count: usize,
    },
    InvalidMinimumCumulativeVariance {
        min_cumulative_variance: f32,
    },
    Pca(PcaError),
    InvalidNumericState(String),
}

impl fmt::Display for DirectionalPcaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingBlock { block_id } => write!(f, "block {block_id} is not present"),
            Self::BlockStore(error) => write!(f, "block store failure: {error}"),
            Self::EmptyBlockEmbeddings { block_id } => {
                write!(f, "block {block_id} contains no embeddings")
            }
            Self::IncompatibleEmbeddingSpec {
                expected,
                actual,
                block_id,
            } => write!(
                f,
                "block {block_id} uses embedding spec {} dims under {}, expected {} dims under {}",
                actual.dims, actual.encoding, expected.dims, expected.encoding
            ),
            Self::UnsupportedEncoding { encoding } => {
                write!(f, "unsupported embedding encoding {encoding}")
            }
            Self::InvalidEmbeddingLength {
                encoding,
                dims,
                expected,
                actual,
            } => write!(
                f,
                "embedding length {actual} does not match expected length {expected} for {dims} dims under {encoding}"
            ),
            Self::NonFiniteValue {
                block_id,
                entry_index,
                dimension_index,
            } => write!(
                f,
                "block {block_id} entry {entry_index} contains a non-finite value at dimension {dimension_index}"
            ),
            Self::InvalidRetainedDimension {
                requested,
                available,
            } => write!(
                f,
                "invalid retained dimension {requested}; available dimension is {available}"
            ),
            Self::InvalidAxisResolutionBudget {
                axis_resolution_budget,
                minimum_required,
            } => write!(
                f,
                "axis_resolution_budget must be at least {minimum_required}, got {axis_resolution_budget}"
            ),
            Self::InvalidTemperature { temperature } => {
                write!(
                    f,
                    "temperature must be finite and greater than zero, got {temperature}"
                )
            }
            Self::InvalidMinimumInputCount { min_input_count } => {
                write!(
                    f,
                    "min_input_count must be at least 1, got {min_input_count}"
                )
            }
            Self::InvalidMinimumEffectiveRank {
                min_effective_rank,
                retained_dimension_count,
            } => write!(
                f,
                "min_effective_rank must be between 1 and retained_dimension_count ({retained_dimension_count}), got {min_effective_rank}"
            ),
            Self::InvalidMinimumCumulativeVariance {
                min_cumulative_variance,
            } => write!(
                f,
                "min_cumulative_variance must be finite and in [0, 1], got {min_cumulative_variance}"
            ),
            Self::Pca(error) => write!(f, "PCA failure: {error}"),
            Self::InvalidNumericState(message) => write!(f, "invalid numeric state: {message}"),
        }
    }
}

impl std::error::Error for DirectionalPcaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BlockStore(error) => Some(error),
            Self::Pca(error) => Some(error),
            Self::MissingBlock { .. }
            | Self::EmptyBlockEmbeddings { .. }
            | Self::IncompatibleEmbeddingSpec { .. }
            | Self::UnsupportedEncoding { .. }
            | Self::InvalidEmbeddingLength { .. }
            | Self::NonFiniteValue { .. }
            | Self::InvalidRetainedDimension { .. }
            | Self::InvalidAxisResolutionBudget { .. }
            | Self::InvalidTemperature { .. }
            | Self::InvalidMinimumInputCount { .. }
            | Self::InvalidMinimumEffectiveRank { .. }
            | Self::InvalidMinimumCumulativeVariance { .. }
            | Self::InvalidNumericState(_) => None,
        }
    }
}

impl From<BlockStoreError> for DirectionalPcaError {
    fn from(value: BlockStoreError) -> Self {
        Self::BlockStore(value)
    }
}

impl From<PcaError> for DirectionalPcaError {
    fn from(value: PcaError) -> Self {
        Self::Pca(value)
    }
}

pub fn run_directional_pca_layer(
    input: &DirectionalPcaLayerInput,
    store: &dyn BlockStore,
) -> Result<DirectionalPcaLayerOutcome, DirectionalPcaError> {
    validate_base_params(&input.params)?;

    let minimum_input_count = input.params.min_input_count.max(2);
    if input.block_ids.len() < minimum_input_count {
        return Ok(DirectionalPcaLayerOutcome::Ineligible(
            DirectionalPcaEligibility::InsufficientInputCount {
                actual: input.block_ids.len(),
                minimum: minimum_input_count,
            },
        ));
    }

    let loaded = load_representative_embeddings(&input.block_ids, store)?;
    validate_loaded_params(&input.params, loaded.embedding_spec.dims)?;

    let vectors = loaded
        .representatives
        .iter()
        .map(|entry| entry.vector.clone())
        .collect::<Vec<_>>();
    let transform = fit(&vectors)?;
    let effective_rank = transform.diagnostics().rank_estimate;
    if effective_rank < input.params.min_effective_rank {
        return Ok(DirectionalPcaLayerOutcome::Ineligible(
            DirectionalPcaEligibility::InsufficientEffectiveRank {
                actual: effective_rank,
                minimum: input.params.min_effective_rank,
            },
        ));
    }

    let cumulative_variance = transform
        .cumulative_variance()
        .and_then(|values| {
            values
                .get(input.params.retained_dimension_count.saturating_sub(1))
                .copied()
        })
        .unwrap_or(0.0);
    if cumulative_variance < input.params.min_cumulative_variance {
        return Ok(DirectionalPcaLayerOutcome::Ineligible(
            DirectionalPcaEligibility::InsufficientExplainedVariance {
                actual: cumulative_variance,
                minimum: input.params.min_cumulative_variance,
            },
        ));
    }
    let truncated = transform.truncate(input.params.retained_dimension_count)?;

    let coordinates = loaded
        .representatives
        .iter()
        .map(|entry| truncated.apply(&entry.vector))
        .collect::<Result<Vec<_>, _>>()?;
    let axis_scores = compute_axis_scores(&loaded.representatives, &truncated, &input.params)?;
    let axis_bin_counts = allocate_axis_bins(
        &axis_scores,
        input.params.axis_resolution_budget,
        input.params.temperature,
    )?;
    let point_bins = assign_quantile_bins(&coordinates, &axis_bin_counts);
    let groups = materialize_groups(&loaded.representatives, &point_bins)?;

    Ok(DirectionalPcaLayerOutcome::Partitioned(
        DirectionalPcaLayerResult {
            embedding_spec: loaded.embedding_spec,
            groups,
        },
    ))
}

#[derive(Clone)]
struct LoadedInputs {
    embedding_spec: EmbeddingSpec,
    representatives: Vec<RepresentativeEmbedding>,
}

#[derive(Clone)]
struct RepresentativeEmbedding {
    block_id: BlockHash,
    vector: Vec<f32>,
}

fn validate_base_params(params: &DirectionalPcaLayerParams) -> Result<(), DirectionalPcaError> {
    if params.retained_dimension_count == 0 {
        return Err(DirectionalPcaError::InvalidRetainedDimension {
            requested: 0,
            available: 0,
        });
    }
    if params.axis_resolution_budget == 0 {
        return Err(DirectionalPcaError::InvalidAxisResolutionBudget {
            axis_resolution_budget: 0,
            minimum_required: 1,
        });
    }
    if !params.temperature.is_finite() || params.temperature <= 0.0 {
        return Err(DirectionalPcaError::InvalidTemperature {
            temperature: params.temperature,
        });
    }
    if params.min_input_count == 0 {
        return Err(DirectionalPcaError::InvalidMinimumInputCount {
            min_input_count: params.min_input_count,
        });
    }
    if !params.min_cumulative_variance.is_finite()
        || !(0.0..=1.0).contains(&params.min_cumulative_variance)
    {
        return Err(DirectionalPcaError::InvalidMinimumCumulativeVariance {
            min_cumulative_variance: params.min_cumulative_variance,
        });
    }
    Ok(())
}

fn validate_loaded_params(
    params: &DirectionalPcaLayerParams,
    input_dims: u64,
) -> Result<(), DirectionalPcaError> {
    let available = usize::try_from(input_dims).map_err(|_| {
        DirectionalPcaError::InvalidNumericState("embedding dims overflow usize".into())
    })?;
    if params.retained_dimension_count > available {
        return Err(DirectionalPcaError::InvalidRetainedDimension {
            requested: params.retained_dimension_count,
            available,
        });
    }
    if params.axis_resolution_budget < params.retained_dimension_count {
        return Err(DirectionalPcaError::InvalidAxisResolutionBudget {
            axis_resolution_budget: params.axis_resolution_budget,
            minimum_required: params.retained_dimension_count,
        });
    }
    if params.min_effective_rank == 0 || params.min_effective_rank > params.retained_dimension_count
    {
        return Err(DirectionalPcaError::InvalidMinimumEffectiveRank {
            min_effective_rank: params.min_effective_rank,
            retained_dimension_count: params.retained_dimension_count,
        });
    }
    Ok(())
}

fn load_representative_embeddings(
    block_ids: &[BlockHash],
    store: &dyn BlockStore,
) -> Result<LoadedInputs, DirectionalPcaError> {
    let mut embedding_spec: Option<EmbeddingSpec> = None;
    let mut representatives = Vec::with_capacity(block_ids.len());

    for block_id in block_ids {
        let validated = store
            .get(block_id)?
            .ok_or(DirectionalPcaError::MissingBlock {
                block_id: *block_id,
            })?;
        let entries = into_entries(validated);
        let (spec, vector) = representative_embedding(entries, *block_id)?;
        if let Some(expected) = &embedding_spec {
            if expected != &spec {
                return Err(DirectionalPcaError::IncompatibleEmbeddingSpec {
                    expected: expected.clone(),
                    actual: spec,
                    block_id: *block_id,
                });
            }
        } else {
            embedding_spec = Some(spec.clone());
        }
        representatives.push(RepresentativeEmbedding {
            block_id: *block_id,
            vector,
        });
    }

    Ok(LoadedInputs {
        embedding_spec: embedding_spec.ok_or_else(|| {
            DirectionalPcaError::InvalidNumericState("missing embedding spec".into())
        })?,
        representatives,
    })
}

fn representative_embedding(
    entries: TypedEntries,
    block_id: BlockHash,
) -> Result<(EmbeddingSpec, Vec<f32>), DirectionalPcaError> {
    match entries {
        TypedEntries::Branch(metadata, entries) => {
            if entries.is_empty() {
                return Err(DirectionalPcaError::EmptyBlockEmbeddings { block_id });
            }
            let vectors = entries
                .iter()
                .enumerate()
                .map(|(index, entry)| {
                    decode_embedding(&entry.embedding, &metadata.embedding_spec, block_id, index)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok((
                metadata.embedding_spec,
                compute_centroid(&vectors, block_id)?,
            ))
        }
        TypedEntries::Leaf(metadata, entries) => {
            let Some(entry) = entries.first() else {
                return Err(DirectionalPcaError::EmptyBlockEmbeddings { block_id });
            };
            let vector = decode_embedding(&entry.embedding, &metadata.embedding_spec, block_id, 0)?;
            Ok((metadata.embedding_spec, vector))
        }
    }
}

fn decode_embedding(
    embedding: &[u8],
    spec: &EmbeddingSpec,
    block_id: BlockHash,
    entry_index: usize,
) -> Result<Vec<f32>, DirectionalPcaError> {
    let expected =
        expected_embedding_len(spec).ok_or_else(|| DirectionalPcaError::UnsupportedEncoding {
            encoding: spec.encoding.clone(),
        })?;
    if embedding.len() != expected {
        return Err(DirectionalPcaError::InvalidEmbeddingLength {
            encoding: spec.encoding.clone(),
            dims: spec.dims,
            expected,
            actual: embedding.len(),
        });
    }

    match spec.encoding.as_str() {
        "i8" => Ok(embedding
            .iter()
            .map(|byte| i8::from_le_bytes([*byte]) as f32)
            .collect()),
        "f32le" => embedding
            .chunks_exact(4)
            .enumerate()
            .map(|(dimension_index, chunk)| {
                let bytes: [u8; 4] = chunk.try_into().map_err(|_| {
                    DirectionalPcaError::InvalidNumericState("invalid f32 chunk length".into())
                })?;
                let value = f32::from_le_bytes(bytes);
                if !value.is_finite() {
                    return Err(DirectionalPcaError::NonFiniteValue {
                        block_id,
                        entry_index,
                        dimension_index,
                    });
                }
                Ok(value)
            })
            .collect(),
        "f16le" => embedding
            .chunks_exact(2)
            .enumerate()
            .map(|(dimension_index, chunk)| {
                let bytes: [u8; 2] = chunk.try_into().map_err(|_| {
                    DirectionalPcaError::InvalidNumericState("invalid f16 chunk length".into())
                })?;
                let value = f16::from_le_bytes(bytes).to_f32();
                if !value.is_finite() {
                    return Err(DirectionalPcaError::NonFiniteValue {
                        block_id,
                        entry_index,
                        dimension_index,
                    });
                }
                Ok(value)
            })
            .collect(),
        _ => Err(DirectionalPcaError::UnsupportedEncoding {
            encoding: spec.encoding.clone(),
        }),
    }
}

fn expected_embedding_len(spec: &EmbeddingSpec) -> Option<usize> {
    let dims = usize::try_from(spec.dims).ok()?;
    match spec.encoding.as_str() {
        "f32le" => dims.checked_mul(4),
        "f16le" => dims.checked_mul(2),
        "i8" => Some(dims),
        _ => None,
    }
}

fn compute_centroid(
    vectors: &[Vec<f32>],
    block_id: BlockHash,
) -> Result<Vec<f32>, DirectionalPcaError> {
    let Some(first) = vectors.first() else {
        return Err(DirectionalPcaError::EmptyBlockEmbeddings { block_id });
    };
    let dims = first.len();
    let mut sums = vec![0.0_f64; dims];
    for vector in vectors {
        for (index, value) in vector.iter().copied().enumerate() {
            sums[index] += f64::from(value);
            if !sums[index].is_finite() {
                return Err(DirectionalPcaError::InvalidNumericState(format!(
                    "centroid sum became non-finite at dimension {index}"
                )));
            }
        }
    }

    let divisor = vectors.len() as f64;
    sums.into_iter()
        .enumerate()
        .map(|(index, value)| {
            let centroid = (value / divisor) as f32;
            if !centroid.is_finite() {
                return Err(DirectionalPcaError::InvalidNumericState(format!(
                    "centroid value became non-finite at dimension {index}"
                )));
            }
            Ok(centroid)
        })
        .collect()
}

fn compute_axis_scores(
    representatives: &[RepresentativeEmbedding],
    transform: &lexongraph_pca::PcaTransform,
    params: &DirectionalPcaLayerParams,
) -> Result<Vec<f64>, DirectionalPcaError> {
    let centroid = compute_layer_centroid(representatives)?;
    let explained_variance = transform.explained_variance().ok_or_else(|| {
        DirectionalPcaError::InvalidNumericState("missing explained variance".into())
    })?;
    let gamma = f64::from(params.variance_exponent);

    (0..transform.output_dim)
        .map(|column| {
            let alpha = dot_with_basis_column(&centroid, transform, column)?;
            let lambda = f64::from(explained_variance[column]).max(0.0);
            let variance_factor = if gamma == 0.0 {
                1.0
            } else {
                lambda.powf(gamma)
            };
            let score = alpha.abs() * variance_factor;
            if !score.is_finite() {
                return Err(DirectionalPcaError::InvalidNumericState(format!(
                    "axis score became non-finite for retained dimension {column}"
                )));
            }
            Ok(score)
        })
        .collect()
}

fn compute_layer_centroid(
    representatives: &[RepresentativeEmbedding],
) -> Result<Vec<f32>, DirectionalPcaError> {
    let dims = representatives
        .first()
        .map(|value| value.vector.len())
        .ok_or_else(|| {
            DirectionalPcaError::InvalidNumericState("missing representatives".into())
        })?;
    let mut sums = vec![0.0_f64; dims];
    for representative in representatives {
        for (index, value) in representative.vector.iter().copied().enumerate() {
            sums[index] += f64::from(value);
            if !sums[index].is_finite() {
                return Err(DirectionalPcaError::InvalidNumericState(format!(
                    "layer centroid sum became non-finite at dimension {index}"
                )));
            }
        }
    }
    let divisor = representatives.len() as f64;
    sums.into_iter()
        .enumerate()
        .map(|(index, value)| {
            let centroid = (value / divisor) as f32;
            if !centroid.is_finite() {
                return Err(DirectionalPcaError::InvalidNumericState(format!(
                    "layer centroid became non-finite at dimension {index}"
                )));
            }
            Ok(centroid)
        })
        .collect()
}

fn dot_with_basis_column(
    vector: &[f32],
    transform: &lexongraph_pca::PcaTransform,
    column: usize,
) -> Result<f64, DirectionalPcaError> {
    let mut dot = 0.0_f64;
    for (row, value) in vector.iter().copied().enumerate() {
        dot += f64::from(value) * f64::from(transform.basis[row + column * transform.input_dim]);
    }
    if !dot.is_finite() {
        return Err(DirectionalPcaError::InvalidNumericState(format!(
            "directional coefficient became non-finite for retained dimension {column}"
        )));
    }
    Ok(dot)
}

fn allocate_axis_bins(
    axis_scores: &[f64],
    axis_resolution_budget: usize,
    temperature: f32,
) -> Result<Vec<usize>, DirectionalPcaError> {
    if axis_scores.is_empty() {
        return Err(DirectionalPcaError::InvalidNumericState(
            "cannot allocate zero retained dimensions".into(),
        ));
    }
    if axis_resolution_budget < axis_scores.len() {
        return Err(DirectionalPcaError::InvalidAxisResolutionBudget {
            axis_resolution_budget,
            minimum_required: axis_scores.len(),
        });
    }

    let damped = axis_scores
        .iter()
        .map(|score| (1.0 + score.max(0.0)).ln())
        .collect::<Vec<_>>();
    let temperature = f64::from(temperature);
    let max_scaled = damped
        .iter()
        .map(|value| value / temperature)
        .fold(f64::NEG_INFINITY, f64::max);
    let exp_values = damped
        .iter()
        .map(|value| ((value / temperature) - max_scaled).exp())
        .collect::<Vec<_>>();
    let exp_sum = exp_values.iter().sum::<f64>();
    if !exp_sum.is_finite() || exp_sum <= 0.0 {
        return Err(DirectionalPcaError::InvalidNumericState(
            "axis-allocation normalization failed".into(),
        ));
    }

    let mut counts = vec![1_usize; axis_scores.len()];
    let remaining_budget = axis_resolution_budget - axis_scores.len();
    if remaining_budget == 0 {
        return Ok(counts);
    }

    let desired = exp_values
        .iter()
        .map(|value| value * remaining_budget as f64 / exp_sum)
        .collect::<Vec<_>>();
    let base = desired
        .iter()
        .map(|value| value.floor() as usize)
        .collect::<Vec<_>>();
    for (count, addend) in counts.iter_mut().zip(base.iter().copied()) {
        *count += addend;
    }

    let used = base.iter().sum::<usize>();
    let mut leftovers = remaining_budget - used;
    let mut remainders = desired
        .iter()
        .enumerate()
        .map(|(index, value)| (index, value - value.floor()))
        .collect::<Vec<_>>();
    remainders.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    for (index, _) in remainders {
        if leftovers == 0 {
            break;
        }
        counts[index] += 1;
        leftovers -= 1;
    }

    Ok(counts)
}

fn assign_quantile_bins(coordinates: &[Vec<f32>], axis_bin_counts: &[usize]) -> Vec<Vec<usize>> {
    let point_count = coordinates.len();
    let retained_dims = axis_bin_counts.len();
    let mut point_bins = vec![vec![0_usize; retained_dims]; point_count];

    for (axis, &bin_count) in axis_bin_counts.iter().enumerate() {
        if bin_count == 1 {
            continue;
        }

        let mut order = (0..point_count).collect::<Vec<_>>();
        order.sort_by(|left, right| {
            coordinates[*left][axis]
                .total_cmp(&coordinates[*right][axis])
                .then_with(|| left.cmp(right))
        });

        for (rank, point_index) in order.into_iter().enumerate() {
            point_bins[point_index][axis] = rank * bin_count / point_count;
        }
    }

    point_bins
}

fn materialize_groups(
    representatives: &[RepresentativeEmbedding],
    point_bins: &[Vec<usize>],
) -> Result<Vec<DirectionalPcaGroup>, DirectionalPcaError> {
    let mut buckets: BTreeMap<Vec<usize>, Vec<usize>> = BTreeMap::new();
    for (point_index, key) in point_bins.iter().cloned().enumerate() {
        buckets.entry(key).or_default().push(point_index);
    }

    buckets
        .into_values()
        .map(|point_indexes| {
            let centroid = compute_group_centroid(representatives, &point_indexes)?;
            let member_block_ids = point_indexes
                .iter()
                .map(|index| representatives[*index].block_id)
                .collect();
            Ok(DirectionalPcaGroup {
                centroid,
                member_block_ids,
            })
        })
        .collect()
}

fn compute_group_centroid(
    representatives: &[RepresentativeEmbedding],
    point_indexes: &[usize],
) -> Result<Vec<f32>, DirectionalPcaError> {
    let dims = representatives
        .first()
        .map(|value| value.vector.len())
        .ok_or_else(|| {
            DirectionalPcaError::InvalidNumericState("missing representatives".into())
        })?;
    let mut sums = vec![0.0_f64; dims];

    for &point_index in point_indexes {
        let representative = &representatives[point_index];
        for (dimension, value) in representative.vector.iter().copied().enumerate() {
            sums[dimension] += f64::from(value);
            if !sums[dimension].is_finite() {
                return Err(DirectionalPcaError::InvalidNumericState(format!(
                    "group centroid sum became non-finite at dimension {dimension}"
                )));
            }
        }
    }

    let divisor = point_indexes.len() as f64;
    sums.into_iter()
        .enumerate()
        .map(|(dimension, value)| {
            let centroid = (value / divisor) as f32;
            if !centroid.is_finite() {
                return Err(DirectionalPcaError::InvalidNumericState(format!(
                    "group centroid became non-finite at dimension {dimension}"
                )));
            }
            Ok(centroid)
        })
        .collect()
}
