// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

//! Streaming PCA projection + deterministic sort + exact chunking for LexonGraph.

use lexongraph_pca::{PcaError, PcaTransform, fit};
use lexongraph_streaming_clustering::{
    ClusterId, Embedding, MetricDirection, PassReport, StreamingClusterClassifier,
    StreamingClusterTrainer, StreamingClusteringConfig, StreamingClusteringError, TrainerState,
    validate_config, validate_embedding,
};
use sha2::{Digest, Sha256};

pub const PCA_CHUNKING_SOFTWARE_IDENTITY: &str =
    concat!("lexongraph-pca-chunking-v", env!("CARGO_PKG_VERSION"));

#[derive(Clone, Debug, PartialEq)]
pub struct PcaChunkingParams {
    pub retained_dimension_count: usize,
    pub variance_exponent: f32,
}

#[derive(Clone, Debug)]
pub struct PcaChunkingStreamingTrainer {
    config: StreamingClusteringConfig,
    params: PcaChunkingParams,
    state: TrainerState,
    current_pass: Vec<Embedding>,
    baseline_fingerprint: Option<PassFingerprint>,
    completed_passes: usize,
    model: Option<PcaChunkingModel>,
}

#[derive(Clone, Debug)]
pub struct PcaChunkingStreamingClassifier {
    config: StreamingClusteringConfig,
    model: PcaChunkingModel,
}

#[derive(Clone, Debug)]
struct PcaChunkingModel {
    transform: PcaTransform,
    projection_weights: Vec<f32>,
    chunk_boundaries: Vec<f32>,
    quality_metric: f64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PassFingerprint {
    observed_count: usize,
    digest: [u8; 32],
}

impl PcaChunkingStreamingTrainer {
    pub fn new(
        config: StreamingClusteringConfig,
        params: PcaChunkingParams,
    ) -> Result<Self, StreamingClusteringError> {
        validate_config(&config)?;
        validate_params(&config, &params)?;
        reject_balance_constraints(&config)?;
        Ok(Self {
            config,
            params,
            state: TrainerState::Idle,
            current_pass: Vec::new(),
            baseline_fingerprint: None,
            completed_passes: 0,
            model: None,
        })
    }

    fn invalid_transition(&mut self, operation: &str) -> StreamingClusteringError {
        let state = self.state;
        self.state = TrainerState::Error;
        StreamingClusteringError::InvalidTransition {
            state,
            operation: operation.into(),
        }
    }

    fn fail(&mut self, error: StreamingClusteringError) -> StreamingClusteringError {
        self.state = TrainerState::Error;
        self.current_pass.clear();
        error
    }

    fn finish_pass_impl(&mut self) -> Result<PassReport, StreamingClusteringError> {
        if self.state != TrainerState::Ingesting {
            return Err(self.invalid_transition("finish_pass"));
        }
        if self.current_pass.is_empty() {
            return Err(malformed_input(
                "completed pass must contain at least one embedding",
            ));
        }

        let observed_count = self.current_pass.len();
        if observed_count < self.config.cluster_count as usize {
            return Err(unsatisfiable_constraint(format!(
                "completed pass established N = {observed_count}, smaller than required cluster_count {}",
                self.config.cluster_count
            )));
        }

        let current_fingerprint = fingerprint_pass(self.current_pass.as_slice());
        if self.completed_passes == 0 {
            self.baseline_fingerprint = Some(current_fingerprint);
        } else {
            let baseline = self.baseline_fingerprint.as_ref().ok_or_else(|| {
                unsatisfiable_constraint("missing baseline dataset for later PCA chunking passes")
            })?;
            if baseline != &current_fingerprint {
                return Err(malformed_input(
                    "later passes must replay the same logical dataset in the same order",
                ));
            }
        }

        let model = fit_pass_model(&self.current_pass, &self.config, &self.params)?;
        let quality_metric = model.quality_metric;
        self.model = Some(model);
        self.completed_passes += 1;
        self.current_pass.clear();
        self.state = TrainerState::PassComplete;

        Ok(PassReport {
            observed_count,
            quality_metric,
            balance_metric: 0.0,
            quality_direction: MetricDirection::SmallerIsBetter,
            balance_direction: MetricDirection::SmallerIsBetter,
            cluster_ids: (0..self.config.cluster_count).collect(),
        })
    }
}

impl StreamingClusterTrainer for PcaChunkingStreamingTrainer {
    type Classifier = PcaChunkingStreamingClassifier;

    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn state(&self) -> TrainerState {
        self.state
    }

    fn ingest_batch(&mut self, embeddings: &[Embedding]) -> Result<(), StreamingClusteringError> {
        match self.state {
            TrainerState::Idle | TrainerState::PassComplete => {
                self.state = TrainerState::Ingesting;
            }
            TrainerState::Ingesting => {}
            TrainerState::TrainingComplete | TrainerState::Error => {
                return Err(self.invalid_transition("ingest_batch"));
            }
        }

        for embedding in embeddings {
            validate_embedding(embedding, self.config.dimensions)?;
            self.current_pass.push(embedding.clone());
        }
        Ok(())
    }

    fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError> {
        self.finish_pass_impl().map_err(|error| self.fail(error))
    }

    fn complete_training(&mut self) -> Result<(), StreamingClusteringError> {
        if self.state != TrainerState::PassComplete {
            return Err(self.invalid_transition("complete_training"));
        }
        self.state = TrainerState::TrainingComplete;
        Ok(())
    }

    fn into_classifier(self) -> Result<Self::Classifier, StreamingClusteringError> {
        if self.state != TrainerState::TrainingComplete {
            return Err(StreamingClusteringError::InvalidTransition {
                state: self.state,
                operation: "into_classifier".into(),
            });
        }
        let model = self
            .model
            .ok_or_else(|| StreamingClusteringError::InvalidTransition {
                state: self.state,
                operation: "into_classifier".into(),
            })?;
        Ok(PcaChunkingStreamingClassifier {
            config: self.config,
            model,
        })
    }
}

impl StreamingClusterClassifier for PcaChunkingStreamingClassifier {
    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn assign(&self, embedding: &[f32]) -> Result<ClusterId, StreamingClusteringError> {
        validate_embedding(embedding, self.config.dimensions)?;
        let coordinates = self
            .model
            .transform
            .apply(embedding)
            .map_err(map_pca_error)?;
        let projection_key = scalar_projection_key(
            coordinates.as_slice(),
            self.model.projection_weights.as_slice(),
        )?;
        let cluster_index = self
            .model
            .chunk_boundaries
            .partition_point(|boundary| projection_key > *boundary);
        Ok(cluster_index as ClusterId)
    }
}

fn fit_pass_model(
    embeddings: &[Embedding],
    config: &StreamingClusteringConfig,
    params: &PcaChunkingParams,
) -> Result<PcaChunkingModel, StreamingClusteringError> {
    let transform = fit(embeddings).map_err(map_pca_error)?;
    let truncated = transform
        .truncate(params.retained_dimension_count)
        .map_err(map_pca_error)?;
    let projection_weights = projection_weights(&truncated, params.variance_exponent);
    let coordinates = embeddings
        .iter()
        .map(|embedding| truncated.apply(embedding).map_err(map_pca_error))
        .collect::<Result<Vec<_>, _>>()?;
    let sorted_indices = sort_indices_by_projection_key(&coordinates, &projection_weights)?;
    let chunk_members =
        contiguous_chunk_members(sorted_indices.as_slice(), config.cluster_count as usize)?;
    let chunk_boundaries =
        derive_chunk_boundaries(&coordinates, &projection_weights, chunk_members.as_slice())?;
    let quality_metric = compute_quality_metric(embeddings, chunk_members.as_slice())?;
    Ok(PcaChunkingModel {
        transform: truncated,
        projection_weights,
        chunk_boundaries,
        quality_metric,
    })
}

fn validate_params(
    config: &StreamingClusteringConfig,
    params: &PcaChunkingParams,
) -> Result<(), StreamingClusteringError> {
    if params.retained_dimension_count == 0 || params.retained_dimension_count > config.dimensions {
        return Err(invalid_configuration(format!(
            "retained_dimension_count must be in [1, {}], got {}",
            config.dimensions, params.retained_dimension_count
        )));
    }
    if !params.variance_exponent.is_finite() || params.variance_exponent < 0.0 {
        return Err(invalid_configuration(format!(
            "variance_exponent must be finite and non-negative, got {}",
            params.variance_exponent
        )));
    }
    Ok(())
}

fn reject_balance_constraints(
    config: &StreamingClusteringConfig,
) -> Result<(), StreamingClusteringError> {
    if config.balance_constraints.is_some() {
        return Err(invalid_configuration(
            "balance constraints are not supported by the PCA chunking trainer",
        ));
    }
    Ok(())
}

fn projection_weights(transform: &PcaTransform, variance_exponent: f32) -> Vec<f32> {
    transform
        .explained_variance()
        .map(|variances| {
            variances
                .iter()
                .map(|variance| variance.max(0.0).powf(variance_exponent))
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec![1.0; transform.output_dim])
}

fn scalar_projection_key(
    coordinates: &[f32],
    weights: &[f32],
) -> Result<f32, StreamingClusteringError> {
    if coordinates.len() != weights.len() {
        return Err(unsatisfiable_constraint(format!(
            "projection-key shape mismatch: {} coordinates for {} weights",
            coordinates.len(),
            weights.len()
        )));
    }
    let key = coordinates
        .iter()
        .zip(weights.iter())
        .map(|(coordinate, weight)| coordinate * weight)
        .sum::<f32>();
    if !key.is_finite() {
        return Err(unsatisfiable_constraint(
            "projection key became non-finite during PCA chunking",
        ));
    }
    Ok(key)
}

fn sort_indices_by_projection_key(
    coordinates: &[Vec<f32>],
    weights: &[f32],
) -> Result<Vec<usize>, StreamingClusteringError> {
    let mut keyed = coordinates
        .iter()
        .enumerate()
        .map(|(index, coordinate)| {
            scalar_projection_key(coordinate.as_slice(), weights).map(|key| (index, key))
        })
        .collect::<Result<Vec<_>, _>>()?;
    keyed.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    Ok(keyed.into_iter().map(|(index, _)| index).collect())
}

fn contiguous_chunk_members(
    sorted_indices: &[usize],
    cluster_count: usize,
) -> Result<Vec<Vec<usize>>, StreamingClusteringError> {
    if cluster_count == 0 {
        return Err(invalid_configuration(
            "cluster_count must be positive for contiguous chunking",
        ));
    }
    if sorted_indices.len() < cluster_count {
        return Err(unsatisfiable_constraint(format!(
            "cannot form {cluster_count} non-empty chunks from {} items",
            sorted_indices.len()
        )));
    }

    let base = sorted_indices.len() / cluster_count;
    let remainder = sorted_indices.len() % cluster_count;
    let mut cursor = 0usize;
    let mut chunks = Vec::with_capacity(cluster_count);
    for cluster_index in 0..cluster_count {
        let chunk_size = base + usize::from(cluster_index < remainder);
        let next_cursor = cursor + chunk_size;
        chunks.push(sorted_indices[cursor..next_cursor].to_vec());
        cursor = next_cursor;
    }
    Ok(chunks)
}

fn derive_chunk_boundaries(
    coordinates: &[Vec<f32>],
    weights: &[f32],
    chunk_members: &[Vec<usize>],
) -> Result<Vec<f32>, StreamingClusteringError> {
    let mut boundaries = Vec::with_capacity(chunk_members.len().saturating_sub(1));
    for pair in chunk_members.windows(2) {
        let left_max = pair[0].last().copied().ok_or_else(|| {
            unsatisfiable_constraint("chunk boundary encountered an empty left chunk")
        })?;
        let right_min = pair[1].first().copied().ok_or_else(|| {
            unsatisfiable_constraint("chunk boundary encountered an empty right chunk")
        })?;
        let left_key = scalar_projection_key(coordinates[left_max].as_slice(), weights)?;
        let right_key = scalar_projection_key(coordinates[right_min].as_slice(), weights)?;
        let boundary = ((f64::from(left_key) + f64::from(right_key)) / 2.0) as f32;
        if !boundary.is_finite() {
            return Err(unsatisfiable_constraint(
                "chunk boundary became non-finite during PCA chunking",
            ));
        }
        boundaries.push(boundary);
    }
    Ok(boundaries)
}

fn compute_quality_metric(
    embeddings: &[Embedding],
    chunk_members: &[Vec<usize>],
) -> Result<f64, StreamingClusteringError> {
    let mut total = 0.0f64;
    for members in chunk_members {
        let centroid = centroid(embeddings, members)?;
        for &member_index in members {
            total += squared_distance(embeddings[member_index].as_slice(), centroid.as_slice())?;
        }
    }
    if !total.is_finite() {
        return Err(unsatisfiable_constraint(
            "quality metric became non-finite during PCA chunking",
        ));
    }
    Ok(total)
}

fn centroid(
    embeddings: &[Embedding],
    members: &[usize],
) -> Result<Vec<f32>, StreamingClusteringError> {
    let first_index = *members
        .first()
        .ok_or_else(|| unsatisfiable_constraint("cannot compute centroid for empty chunk"))?;
    let dimensions = embeddings[first_index].len();
    let mut centroid = vec![0.0f32; dimensions];
    for &member_index in members {
        let embedding = embeddings.get(member_index).ok_or_else(|| {
            malformed_input(format!("chunk member index {member_index} is out of range"))
        })?;
        for (dimension, value) in embedding.iter().copied().enumerate() {
            centroid[dimension] += value;
        }
    }
    let scale = members.len() as f32;
    for value in &mut centroid {
        *value /= scale;
    }
    Ok(centroid)
}

fn squared_distance(left: &[f32], right: &[f32]) -> Result<f64, StreamingClusteringError> {
    if left.len() != right.len() {
        return Err(malformed_input(format!(
            "expected equal dimensionality for distance computation, got {} and {}",
            left.len(),
            right.len()
        )));
    }
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(lhs, rhs)| {
            let delta = f64::from(*lhs) - f64::from(*rhs);
            delta * delta
        })
        .sum())
}

fn map_pca_error(error: PcaError) -> StreamingClusteringError {
    match error {
        PcaError::DimensionMismatch { .. }
        | PcaError::InvalidTruncationDimension { .. }
        | PcaError::ValidationFailure(_)
        | PcaError::QuantizationConfigurationError(_)
        | PcaError::SchemaVersionMismatch { .. }
        | PcaError::InvalidSerializedFormat(_) => {
            invalid_configuration(format!("PCA chunking configuration is invalid: {error}"))
        }
        PcaError::NonFiniteInput { .. } => {
            malformed_input(format!("non-finite PCA input: {error}"))
        }
        PcaError::EmptyInput
        | PcaError::InsufficientSamples { .. }
        | PcaError::DegenerateCovariance { .. }
        | PcaError::DecompositionFailure(_)
        | PcaError::InvalidNumericState(_) => {
            unsatisfiable_constraint(format!("PCA chunking fit failed: {error}"))
        }
    }
}

fn fingerprint_pass(embeddings: &[Embedding]) -> PassFingerprint {
    let mut digest = Sha256::new();
    for embedding in embeddings {
        digest.update(embedding.len().to_le_bytes());
        for value in embedding {
            digest.update(value.to_le_bytes());
        }
    }
    PassFingerprint {
        observed_count: embeddings.len(),
        digest: digest.finalize().into(),
    }
}

fn invalid_configuration(message: impl Into<String>) -> StreamingClusteringError {
    StreamingClusteringError::InvalidConfiguration {
        message: message.into(),
    }
}

fn unsatisfiable_constraint(message: impl Into<String>) -> StreamingClusteringError {
    StreamingClusteringError::UnsatisfiableConstraint {
        message: message.into(),
    }
}

fn malformed_input(message: impl Into<String>) -> StreamingClusteringError {
    StreamingClusteringError::MalformedInput {
        message: message.into(),
    }
}
