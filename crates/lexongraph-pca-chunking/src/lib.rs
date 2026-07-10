// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

//! Streaming PCA projection + deterministic sort + exact chunking for LexonGraph.

use lexongraph_pca::{PcaAccumulator, PcaError, PcaTransform};
use lexongraph_streaming_clustering::{
    ClusterId, Embedding, MetricDirection, PassReadiness, PassReport, StreamingClusterClassifier,
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

#[derive(Debug)]
pub struct PcaChunkingStreamingTrainer {
    config: StreamingClusteringConfig,
    params: PcaChunkingParams,
    state: TrainerState,
    phase: ReplayPhase,
    active_pass: Option<ActivePassState>,
    baseline_fingerprint: Option<PassFingerprint>,
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
    chunk_upper_bounds: Vec<SortKey>,
    quality_metric: f64,
    observed_count: usize,
}

#[derive(Debug)]
enum ReplayPhase {
    AnalyzePca,
    DiscoverBoundaries(BoundaryDiscoveryState),
    Ready(PcaChunkingModel),
}

#[derive(Debug)]
enum ActivePassState {
    AnalyzePca(ActivePcaPass),
    DiscoverBoundaries(ActiveBoundaryPass),
    Ready(ActiveReadyPass),
}

#[derive(Debug)]
struct ActivePcaPass {
    tracker: PassTracker,
    accumulator: PcaAccumulator,
}

#[derive(Debug)]
struct ActiveBoundaryPass {
    tracker: PassTracker,
    discovery: BoundaryDiscoveryState,
    next_key: Option<SortKey>,
    next_key_count: usize,
}

#[derive(Debug)]
struct ActiveReadyPass {
    tracker: PassTracker,
    model: PcaChunkingModel,
}

#[derive(Clone, Debug)]
struct BoundaryDiscoveryState {
    transform: PcaTransform,
    projection_weights: Vec<f32>,
    quality_metric: f64,
    observed_count: usize,
    boundary_targets: Vec<usize>,
    next_boundary_index: usize,
    cumulative_count: usize,
    lower_bound: Option<SortKey>,
    chunk_upper_bounds: Vec<SortKey>,
}

#[derive(Debug)]
struct PassTracker {
    observed_count: usize,
    hasher: Sha256,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PassFingerprint {
    observed_count: usize,
    digest: [u8; 32],
}

#[derive(Clone, Debug)]
struct SortKey {
    projection_key: f32,
    retained_coordinates: Vec<f32>,
    embedding: Embedding,
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
            phase: ReplayPhase::AnalyzePca,
            active_pass: None,
            baseline_fingerprint: None,
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
        self.active_pass = None;
        error
    }

    fn ensure_active_pass(&mut self) {
        if self.active_pass.is_some() {
            return;
        }
        self.active_pass = Some(match &self.phase {
            ReplayPhase::AnalyzePca => ActivePassState::AnalyzePca(ActivePcaPass {
                tracker: PassTracker::new(),
                accumulator: PcaAccumulator::new(self.config.dimensions),
            }),
            ReplayPhase::DiscoverBoundaries(discovery) => {
                ActivePassState::DiscoverBoundaries(ActiveBoundaryPass {
                    tracker: PassTracker::new(),
                    discovery: discovery.clone(),
                    next_key: None,
                    next_key_count: 0,
                })
            }
            ReplayPhase::Ready(model) => ActivePassState::Ready(ActiveReadyPass {
                tracker: PassTracker::new(),
                model: model.clone(),
            }),
        });
    }

    fn finish_pass_impl(&mut self) -> Result<PassReport, StreamingClusteringError> {
        if self.state != TrainerState::Ingesting {
            return Err(self.invalid_transition("finish_pass"));
        }
        let active_pass = self
            .active_pass
            .take()
            .ok_or_else(|| malformed_input("completed pass must contain at least one embedding"))?;
        let (next_phase, report, model) = match active_pass {
            ActivePassState::AnalyzePca(pass) => self.finish_pca_pass(pass)?,
            ActivePassState::DiscoverBoundaries(pass) => self.finish_boundary_pass(pass)?,
            ActivePassState::Ready(pass) => self.finish_ready_pass(pass)?,
        };
        self.phase = next_phase;
        self.model = model;
        self.state = TrainerState::PassComplete;
        Ok(report)
    }

    fn finish_pca_pass(
        &mut self,
        pass: ActivePcaPass,
    ) -> Result<(ReplayPhase, PassReport, Option<PcaChunkingModel>), StreamingClusteringError> {
        let fingerprint = pass.tracker.finish();
        let observed_count = fingerprint.observed_count;
        if observed_count == 0 {
            return Err(malformed_input(
                "completed pass must contain at least one embedding",
            ));
        }
        if observed_count < self.config.cluster_count as usize {
            return Err(unsatisfiable_constraint(format!(
                "completed pass established N = {observed_count}, smaller than required cluster_count {}",
                self.config.cluster_count
            )));
        }

        self.baseline_fingerprint = Some(fingerprint);
        let transform = pass.accumulator.finalize().map_err(map_pca_error)?;
        let truncated = transform
            .truncate(self.params.retained_dimension_count)
            .map_err(map_pca_error)?;
        let projection_weights = projection_weights(&truncated, self.params.variance_exponent);
        let quality_metric = quality_metric(&truncated);
        let boundary_targets =
            boundary_targets(observed_count, self.config.cluster_count as usize)?;

        if boundary_targets.is_empty() {
            let model = PcaChunkingModel {
                transform: truncated,
                projection_weights,
                chunk_upper_bounds: Vec::new(),
                quality_metric,
                observed_count,
            };
            let report = partition_ready_report(&model, self.config.cluster_count);
            return Ok((ReplayPhase::Ready(model.clone()), report, Some(model)));
        }

        let discovery = BoundaryDiscoveryState {
            transform: truncated,
            projection_weights,
            quality_metric,
            observed_count,
            boundary_targets,
            next_boundary_index: 0,
            cumulative_count: 0,
            lower_bound: None,
            chunk_upper_bounds: Vec::new(),
        };
        let report =
            analysis_only_report(observed_count, self.config.cluster_count, quality_metric);
        Ok((ReplayPhase::DiscoverBoundaries(discovery), report, None))
    }

    fn finish_boundary_pass(
        &mut self,
        mut pass: ActiveBoundaryPass,
    ) -> Result<(ReplayPhase, PassReport, Option<PcaChunkingModel>), StreamingClusteringError> {
        let fingerprint = pass.tracker.finish();
        self.validate_replayed_pass(&fingerprint)?;

        let boundary_target = pass.discovery.boundary_targets[pass.discovery.next_boundary_index];
        let needed_from_next_group = boundary_target - pass.discovery.cumulative_count;
        let next_key = pass.next_key.ok_or_else(|| {
            unsatisfiable_constraint(
                "boundary replay did not discover any classifier-visible sort key above the prior lower bound",
            )
        })?;
        if pass.next_key_count > needed_from_next_group {
            return Err(unsatisfiable_constraint(
                "exact chunking would split identical classifier sort keys across a boundary",
            ));
        }

        pass.discovery.cumulative_count += pass.next_key_count;
        pass.discovery.lower_bound = Some(next_key.clone());
        if pass.next_key_count == needed_from_next_group {
            pass.discovery.chunk_upper_bounds.push(next_key);
            pass.discovery.next_boundary_index += 1;
        }

        if pass.discovery.next_boundary_index == pass.discovery.boundary_targets.len() {
            let model = PcaChunkingModel {
                transform: pass.discovery.transform,
                projection_weights: pass.discovery.projection_weights,
                chunk_upper_bounds: pass.discovery.chunk_upper_bounds,
                quality_metric: pass.discovery.quality_metric,
                observed_count: pass.discovery.observed_count,
            };
            let report = partition_ready_report(&model, self.config.cluster_count);
            Ok((ReplayPhase::Ready(model.clone()), report, Some(model)))
        } else {
            let report = analysis_only_report(
                pass.discovery.observed_count,
                self.config.cluster_count,
                pass.discovery.quality_metric,
            );
            Ok((
                ReplayPhase::DiscoverBoundaries(pass.discovery),
                report,
                None,
            ))
        }
    }

    fn finish_ready_pass(
        &mut self,
        pass: ActiveReadyPass,
    ) -> Result<(ReplayPhase, PassReport, Option<PcaChunkingModel>), StreamingClusteringError> {
        let fingerprint = pass.tracker.finish();
        self.validate_replayed_pass(&fingerprint)?;
        let report = partition_ready_report(&pass.model, self.config.cluster_count);
        Ok((
            ReplayPhase::Ready(pass.model.clone()),
            report,
            Some(pass.model),
        ))
    }

    fn validate_replayed_pass(
        &self,
        fingerprint: &PassFingerprint,
    ) -> Result<(), StreamingClusteringError> {
        let baseline = self.baseline_fingerprint.as_ref().ok_or_else(|| {
            unsatisfiable_constraint("missing baseline dataset for later PCA chunking passes")
        })?;
        if baseline != fingerprint {
            return Err(malformed_input(
                "later passes must replay the same logical dataset in the same order",
            ));
        }
        Ok(())
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
                self.ensure_active_pass();
            }
            TrainerState::Ingesting => {}
            TrainerState::TrainingComplete | TrainerState::Error => {
                return Err(self.invalid_transition("ingest_batch"));
            }
        }

        let active_pass = self
            .active_pass
            .as_mut()
            .ok_or_else(|| malformed_input("missing active PCA chunking pass state"))?;
        for embedding in embeddings {
            validate_embedding(embedding, self.config.dimensions)?;
            match active_pass {
                ActivePassState::AnalyzePca(pass) => {
                    pass.tracker.update(embedding);
                    pass.accumulator.update(embedding).map_err(map_pca_error)?;
                }
                ActivePassState::DiscoverBoundaries(pass) => {
                    pass.tracker.update(embedding);
                    let sort_key = build_sort_key(
                        embedding.as_slice(),
                        &pass.discovery.transform,
                        pass.discovery.projection_weights.as_slice(),
                    )?;
                    if let Some(lower_bound) = pass.discovery.lower_bound.as_ref()
                        && compare_sort_keys(&sort_key, lower_bound).is_le()
                    {
                        continue;
                    }
                    match pass.next_key.as_ref() {
                        None => {
                            pass.next_key = Some(sort_key);
                            pass.next_key_count = 1;
                        }
                        Some(candidate) => match compare_sort_keys(&sort_key, candidate) {
                            std::cmp::Ordering::Less => {
                                pass.next_key = Some(sort_key);
                                pass.next_key_count = 1;
                            }
                            std::cmp::Ordering::Equal => {
                                pass.next_key_count += 1;
                            }
                            std::cmp::Ordering::Greater => {}
                        },
                    }
                }
                ActivePassState::Ready(pass) => {
                    pass.tracker.update(embedding);
                }
            }
        }
        Ok(())
    }

    fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError> {
        self.finish_pass_impl().map_err(|error| self.fail(error))
    }

    fn complete_training(&mut self) -> Result<(), StreamingClusteringError> {
        if self.state != TrainerState::PassComplete || self.model.is_none() {
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
        let sort_key = build_sort_key(
            embedding,
            &self.model.transform,
            self.model.projection_weights.as_slice(),
        )?;
        let cluster_index = self
            .model
            .chunk_upper_bounds
            .partition_point(|upper_bound| compare_sort_keys(&sort_key, upper_bound).is_gt());
        Ok(cluster_index as ClusterId)
    }
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

fn build_sort_key(
    embedding: &[f32],
    transform: &PcaTransform,
    projection_weights: &[f32],
) -> Result<SortKey, StreamingClusteringError> {
    let retained_coordinates = transform.apply(embedding).map_err(map_pca_error)?;
    let projection_key =
        scalar_projection_key(retained_coordinates.as_slice(), projection_weights)?;
    Ok(SortKey {
        projection_key,
        retained_coordinates,
        embedding: embedding.to_vec(),
    })
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

fn compare_sort_keys(left: &SortKey, right: &SortKey) -> std::cmp::Ordering {
    left.projection_key
        .total_cmp(&right.projection_key)
        .then_with(|| {
            compare_f32_slices(
                left.retained_coordinates.as_slice(),
                right.retained_coordinates.as_slice(),
            )
        })
        .then_with(|| compare_f32_slices(left.embedding.as_slice(), right.embedding.as_slice()))
}

fn compare_f32_slices(left: &[f32], right: &[f32]) -> std::cmp::Ordering {
    left.iter()
        .zip(right.iter())
        .map(|(lhs, rhs)| lhs.total_cmp(rhs))
        .find(|ordering| !ordering.is_eq())
        .unwrap_or_else(|| left.len().cmp(&right.len()))
}

fn boundary_targets(
    observed_count: usize,
    cluster_count: usize,
) -> Result<Vec<usize>, StreamingClusteringError> {
    if cluster_count == 0 {
        return Err(invalid_configuration(
            "cluster_count must be positive for contiguous chunking",
        ));
    }
    if observed_count < cluster_count {
        return Err(unsatisfiable_constraint(format!(
            "cannot form {cluster_count} non-empty chunks from {observed_count} items",
        )));
    }
    let base = observed_count / cluster_count;
    let remainder = observed_count % cluster_count;
    let mut cumulative = 0usize;
    let mut targets = Vec::with_capacity(cluster_count.saturating_sub(1));
    for chunk_index in 0..cluster_count.saturating_sub(1) {
        cumulative += base + usize::from(chunk_index < remainder);
        targets.push(cumulative);
    }
    Ok(targets)
}

fn quality_metric(transform: &PcaTransform) -> f64 {
    1.0 - f64::from(
        transform
            .cumulative_variance()
            .and_then(|values| values.last().copied())
            .unwrap_or(0.0),
    )
}

fn analysis_only_report(
    observed_count: usize,
    requested_cluster_count: u32,
    quality_metric: f64,
) -> PassReport {
    PassReport {
        observed_count,
        requested_cluster_count,
        readiness: PassReadiness::AnalysisOnly,
        realized_cluster_count: None,
        quality_metric,
        balance_metric: 0.0,
        quality_direction: MetricDirection::SmallerIsBetter,
        balance_direction: MetricDirection::SmallerIsBetter,
        cluster_ids: None,
    }
}

fn partition_ready_report(model: &PcaChunkingModel, requested_cluster_count: u32) -> PassReport {
    PassReport {
        observed_count: model.observed_count,
        requested_cluster_count,
        readiness: PassReadiness::PartitionReady,
        realized_cluster_count: Some(requested_cluster_count),
        quality_metric: model.quality_metric,
        balance_metric: 0.0,
        quality_direction: MetricDirection::SmallerIsBetter,
        balance_direction: MetricDirection::SmallerIsBetter,
        cluster_ids: Some((0..requested_cluster_count).collect()),
    }
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
            unsatisfiable_constraint(format!("PCA chunking analysis failed: {error}"))
        }
    }
}

impl PassTracker {
    fn new() -> Self {
        Self {
            observed_count: 0,
            hasher: Sha256::new(),
        }
    }

    fn update(&mut self, embedding: &[f32]) {
        self.observed_count += 1;
        self.hasher.update((embedding.len() as u64).to_le_bytes());
        for value in embedding {
            self.hasher.update(value.to_bits().to_le_bytes());
        }
    }

    fn finish(self) -> PassFingerprint {
        PassFingerprint {
            observed_count: self.observed_count,
            digest: self.hasher.finalize().into(),
        }
    }
}

#[allow(dead_code)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boundary_targets_assign_remainder_to_earliest_chunks() {
        assert_eq!(boundary_targets(5, 2).unwrap(), vec![3]);
        assert_eq!(boundary_targets(7, 3).unwrap(), vec![3, 5]);
    }

    #[test]
    fn compare_sort_keys_uses_embedding_after_retained_coordinates() {
        let left = SortKey {
            projection_key: 1.0,
            retained_coordinates: vec![0.0],
            embedding: vec![1.0, 0.0],
        };
        let right = SortKey {
            projection_key: 1.0,
            retained_coordinates: vec![0.0],
            embedding: vec![2.0, 0.0],
        };
        assert!(compare_sort_keys(&left, &right).is_lt());
    }
}
