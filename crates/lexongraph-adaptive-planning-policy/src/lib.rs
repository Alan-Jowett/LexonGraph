// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

//! Deterministic adaptive planning-policy selection for LexonGraph.

use std::collections::BTreeMap;
use std::fmt;

use lexongraph_directional_pca::{DirectionalPcaParams, DirectionalPcaStreamingTrainer};
use lexongraph_streaming_clustering::{
    BalanceConstraints, StreamingClusterClassifier, StreamingClusterTrainer,
    StreamingClusteringConfig, StreamingClusteringError,
};

pub const DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD: f32 = 0.25;
const REPLAY_BATCH_SIZE: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdaptivePlanningDirection {
    Divisive,
    Agglomerative,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdaptiveDirectionalPcaSettings {
    pub cluster_count: u32,
    pub random_seed: Option<u64>,
    pub params: DirectionalPcaParams,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdaptiveDcbcSettings {
    pub cluster_count: u32,
    pub balance_constraints: Option<BalanceConstraints>,
    pub random_seed: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdaptiveSwitchCriteria {
    pub mean_cluster_radius_threshold: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdaptivePlanningSettings {
    pub direction: AdaptivePlanningDirection,
    pub directional_pca: AdaptiveDirectionalPcaSettings,
    pub dcbc: AdaptiveDcbcSettings,
    pub switch_criteria: AdaptiveSwitchCriteria,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivePlanningAlgorithm {
    DirectionalPca,
    Dcbc,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdaptivePlanningDecisionReason {
    InitialDirectionalPcaSegment,
    EvaluatedDirectionalPca,
    PreviouslySwitchedToDcbc,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdaptivePlanningDiagnostics {
    pub represented_item_count: usize,
    pub mean_cluster_radius: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdaptiveSwitchDecisionRecord {
    pub active_algorithm: ActivePlanningAlgorithm,
    pub switch_boundary_occurred: bool,
    pub reason: AdaptivePlanningDecisionReason,
    pub collapse_diagnostics: Option<AdaptivePlanningDiagnostics>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdaptivePlanningError {
    InvalidConfiguration(String),
    DiagnosticComputation(String),
}

impl fmt::Display for AdaptivePlanningError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(message) => {
                write!(f, "adaptive planning configuration is invalid: {message}")
            }
            Self::DiagnosticComputation(message) => {
                write!(
                    f,
                    "adaptive planning diagnostics could not be computed: {message}"
                )
            }
        }
    }
}

impl std::error::Error for AdaptivePlanningError {}

#[derive(Clone, Debug)]
pub struct AdaptivePlanningSelector {
    settings: AdaptivePlanningSettings,
    switched_to_dcbc: bool,
    decision_records: Vec<AdaptiveSwitchDecisionRecord>,
}

impl AdaptivePlanningSelector {
    pub fn new(settings: AdaptivePlanningSettings) -> Result<Self, AdaptivePlanningError> {
        validate_settings(&settings)?;
        Ok(Self {
            settings,
            switched_to_dcbc: false,
            decision_records: Vec::new(),
        })
    }

    pub fn settings(&self) -> &AdaptivePlanningSettings {
        &self.settings
    }

    pub fn switched_to_dcbc(&self) -> bool {
        self.switched_to_dcbc
    }

    pub fn decision_records(&self) -> &[AdaptiveSwitchDecisionRecord] {
        &self.decision_records
    }

    pub fn select_algorithm(
        &mut self,
        represented_item_count: usize,
        embeddings: &[Vec<f32>],
    ) -> Result<ActivePlanningAlgorithm, AdaptivePlanningError> {
        if self.switched_to_dcbc {
            self.decision_records.push(AdaptiveSwitchDecisionRecord {
                active_algorithm: ActivePlanningAlgorithm::Dcbc,
                switch_boundary_occurred: false,
                reason: AdaptivePlanningDecisionReason::PreviouslySwitchedToDcbc,
                collapse_diagnostics: None,
            });
            return Ok(ActivePlanningAlgorithm::Dcbc);
        }

        if self.decision_records.is_empty() {
            self.decision_records.push(AdaptiveSwitchDecisionRecord {
                active_algorithm: ActivePlanningAlgorithm::DirectionalPca,
                switch_boundary_occurred: false,
                reason: AdaptivePlanningDecisionReason::InitialDirectionalPcaSegment,
                collapse_diagnostics: None,
            });
            return Ok(ActivePlanningAlgorithm::DirectionalPca);
        }

        let diagnostics = evaluate_collapse_diagnostics(
            represented_item_count,
            embeddings,
            &self.settings.directional_pca,
        )?;
        self.select_algorithm_from_diagnostics(diagnostics)
    }

    pub fn select_algorithm_with_embedding_replay<Load>(
        &mut self,
        represented_item_count: usize,
        embedding_count: usize,
        dimensions: usize,
        mut load_embedding: Load,
    ) -> Result<ActivePlanningAlgorithm, AdaptivePlanningError>
    where
        Load: FnMut(usize) -> Result<Vec<f32>, AdaptivePlanningError>,
    {
        if self.switched_to_dcbc {
            self.decision_records.push(AdaptiveSwitchDecisionRecord {
                active_algorithm: ActivePlanningAlgorithm::Dcbc,
                switch_boundary_occurred: false,
                reason: AdaptivePlanningDecisionReason::PreviouslySwitchedToDcbc,
                collapse_diagnostics: None,
            });
            return Ok(ActivePlanningAlgorithm::Dcbc);
        }

        if self.decision_records.is_empty() {
            self.decision_records.push(AdaptiveSwitchDecisionRecord {
                active_algorithm: ActivePlanningAlgorithm::DirectionalPca,
                switch_boundary_occurred: false,
                reason: AdaptivePlanningDecisionReason::InitialDirectionalPcaSegment,
                collapse_diagnostics: None,
            });
            return Ok(ActivePlanningAlgorithm::DirectionalPca);
        }

        let diagnostics = evaluate_collapse_diagnostics_with_embedding_replay(
            represented_item_count,
            embedding_count,
            dimensions,
            &self.settings.directional_pca,
            &mut load_embedding,
        )?;
        self.select_algorithm_from_diagnostics(diagnostics)
    }

    pub fn select_algorithm_with_embedding_replay_batches<LoadBatch>(
        &mut self,
        represented_item_count: usize,
        embedding_count: usize,
        dimensions: usize,
        mut load_embeddings: LoadBatch,
    ) -> Result<ActivePlanningAlgorithm, AdaptivePlanningError>
    where
        LoadBatch: FnMut(&[usize]) -> Result<Vec<Vec<f32>>, AdaptivePlanningError>,
    {
        if self.switched_to_dcbc {
            self.decision_records.push(AdaptiveSwitchDecisionRecord {
                active_algorithm: ActivePlanningAlgorithm::Dcbc,
                switch_boundary_occurred: false,
                reason: AdaptivePlanningDecisionReason::PreviouslySwitchedToDcbc,
                collapse_diagnostics: None,
            });
            return Ok(ActivePlanningAlgorithm::Dcbc);
        }

        if self.decision_records.is_empty() {
            self.decision_records.push(AdaptiveSwitchDecisionRecord {
                active_algorithm: ActivePlanningAlgorithm::DirectionalPca,
                switch_boundary_occurred: false,
                reason: AdaptivePlanningDecisionReason::InitialDirectionalPcaSegment,
                collapse_diagnostics: None,
            });
            return Ok(ActivePlanningAlgorithm::DirectionalPca);
        }

        let diagnostics = evaluate_collapse_diagnostics_with_embedding_replay_batches(
            represented_item_count,
            embedding_count,
            dimensions,
            &self.settings.directional_pca,
            &mut load_embeddings,
        )?;
        self.select_algorithm_from_diagnostics(diagnostics)
    }

    pub fn select_algorithm_from_diagnostics(
        &mut self,
        diagnostics: AdaptivePlanningDiagnostics,
    ) -> Result<ActivePlanningAlgorithm, AdaptivePlanningError> {
        let switch_boundary_occurred = diagnostics.mean_cluster_radius
            > self.settings.switch_criteria.mean_cluster_radius_threshold;
        let active_algorithm = if switch_boundary_occurred {
            self.switched_to_dcbc = true;
            ActivePlanningAlgorithm::Dcbc
        } else {
            ActivePlanningAlgorithm::DirectionalPca
        };
        self.decision_records.push(AdaptiveSwitchDecisionRecord {
            active_algorithm,
            switch_boundary_occurred,
            reason: AdaptivePlanningDecisionReason::EvaluatedDirectionalPca,
            collapse_diagnostics: Some(diagnostics),
        });
        Ok(active_algorithm)
    }
}

fn validate_settings(settings: &AdaptivePlanningSettings) -> Result<(), AdaptivePlanningError> {
    if settings.directional_pca.cluster_count == 0 {
        return Err(AdaptivePlanningError::InvalidConfiguration(
            "directional-PCA cluster_count must be greater than zero".into(),
        ));
    }
    if settings.dcbc.cluster_count == 0 {
        return Err(AdaptivePlanningError::InvalidConfiguration(
            "DCBC cluster_count must be greater than zero".into(),
        ));
    }
    if !settings
        .switch_criteria
        .mean_cluster_radius_threshold
        .is_finite()
        || settings.switch_criteria.mean_cluster_radius_threshold < 0.0
    {
        return Err(AdaptivePlanningError::InvalidConfiguration(
            "mean_cluster_radius_threshold must be finite and non-negative".into(),
        ));
    }
    validate_directional_pca_params(&settings.directional_pca)?;
    Ok(())
}

fn validate_directional_pca_params(
    settings: &AdaptiveDirectionalPcaSettings,
) -> Result<(), AdaptivePlanningError> {
    let params = &settings.params;
    match params.retained_axis_policy {
        lexongraph_directional_pca::DirectionalPcaRetainedAxisPolicy::FixedCount(
            retained_dimension_count,
        ) => {
            if retained_dimension_count == 0 {
                return Err(AdaptivePlanningError::InvalidConfiguration(
                    "retained_axis_policy = FixedCount(n) requires n to be greater than zero"
                        .into(),
                ));
            }
            if retained_dimension_count > settings.cluster_count as usize {
                return Err(AdaptivePlanningError::InvalidConfiguration(format!(
                    "retained_axis_policy = FixedCount({}) cannot exceed directional-PCA cluster_count {}",
                    retained_dimension_count, settings.cluster_count
                )));
            }
            if params.min_effective_rank > retained_dimension_count {
                return Err(AdaptivePlanningError::InvalidConfiguration(format!(
                    "min_effective_rank must be in [1, FixedCount(n)={}], got {}",
                    retained_dimension_count, params.min_effective_rank
                )));
            }
        }
        lexongraph_directional_pca::DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible => {
            if params.allocation_policy
                == lexongraph_directional_pca::DirectionalPcaAllocationPolicy::CentroidWeightedBins
                && params.min_effective_rank > settings.cluster_count as usize
            {
                return Err(AdaptivePlanningError::InvalidConfiguration(format!(
                    "min_effective_rank {} cannot exceed centroid-weighted adaptive axis budget {}",
                    params.min_effective_rank, settings.cluster_count
                )));
            }
        }
    }
    if params.allocation_policy
        == lexongraph_directional_pca::DirectionalPcaAllocationPolicy::EigenvalueLogBits
        && !settings.cluster_count.is_power_of_two()
    {
        return Err(AdaptivePlanningError::InvalidConfiguration(format!(
            "directional-PCA eigenvalue log-bit allocation requires a power-of-two cluster_count, got {}",
            settings.cluster_count
        )));
    }
    if !params.variance_exponent.is_finite() || params.variance_exponent < 0.0 {
        return Err(AdaptivePlanningError::InvalidConfiguration(format!(
            "variance_exponent must be finite and non-negative, got {}",
            params.variance_exponent
        )));
    }
    if !params.temperature.is_finite() || params.temperature <= 0.0 {
        return Err(AdaptivePlanningError::InvalidConfiguration(format!(
            "temperature must be finite and positive, got {}",
            params.temperature
        )));
    }
    if params.min_input_count < 2 {
        return Err(AdaptivePlanningError::InvalidConfiguration(format!(
            "min_input_count must be at least 2, got {}",
            params.min_input_count
        )));
    }
    if params.min_effective_rank == 0 {
        return Err(AdaptivePlanningError::InvalidConfiguration(format!(
            "min_effective_rank must be at least 1, got {}",
            params.min_effective_rank
        )));
    }
    if !params.min_cumulative_variance.is_finite()
        || !(0.0..=1.0).contains(&params.min_cumulative_variance)
    {
        return Err(AdaptivePlanningError::InvalidConfiguration(format!(
            "directional-PCA min_cumulative_variance must be finite and in [0.0, 1.0], got {}",
            params.min_cumulative_variance
        )));
    }
    Ok(())
}

fn evaluate_collapse_diagnostics(
    represented_item_count: usize,
    embeddings: &[Vec<f32>],
    settings: &AdaptiveDirectionalPcaSettings,
) -> Result<AdaptivePlanningDiagnostics, AdaptivePlanningError> {
    if represented_item_count == 0 {
        return Err(AdaptivePlanningError::DiagnosticComputation(
            "represented_item_count must be greater than zero".into(),
        ));
    }
    if embeddings.is_empty() {
        return Err(AdaptivePlanningError::DiagnosticComputation(
            "adaptive diagnostics require at least one embedding".into(),
        ));
    }
    if represented_item_count < embeddings.len() {
        return Err(AdaptivePlanningError::DiagnosticComputation(format!(
            "represented_item_count {represented_item_count} cannot be smaller than the embedding count {}",
            embeddings.len()
        )));
    }
    let dimensions = embeddings[0].len();
    if dimensions == 0 {
        return Err(AdaptivePlanningError::DiagnosticComputation(
            "adaptive diagnostics require non-empty embeddings".into(),
        ));
    }
    let cluster_count = diagnostic_cluster_count(
        settings.cluster_count,
        embeddings.len(),
        settings.params.allocation_policy,
    )?;

    let config = StreamingClusteringConfig {
        cluster_count,
        dimensions,
        balance_constraints: None,
        random_seed: settings.random_seed,
    };
    let mut trainer = DirectionalPcaStreamingTrainer::new(config, settings.params.clone())
        .map_err(map_streaming_clustering_error)?;
    trainer
        .ingest_batch(embeddings)
        .map_err(map_streaming_clustering_error)?;
    trainer
        .finish_pass()
        .map_err(map_streaming_clustering_error)?;
    trainer
        .complete_training()
        .map_err(map_streaming_clustering_error)?;
    let classifier = trainer
        .into_classifier()
        .map_err(map_streaming_clustering_error)?;
    let assignments = classifier
        .assign_batch(embeddings)
        .map_err(map_streaming_clustering_error)?;
    let mean_cluster_radius = compute_mean_cluster_radius(embeddings, &assignments)?;

    Ok(AdaptivePlanningDiagnostics {
        represented_item_count,
        mean_cluster_radius,
    })
}

fn evaluate_collapse_diagnostics_with_embedding_replay<Load>(
    represented_item_count: usize,
    embedding_count: usize,
    dimensions: usize,
    settings: &AdaptiveDirectionalPcaSettings,
    load_embedding: &mut Load,
) -> Result<AdaptivePlanningDiagnostics, AdaptivePlanningError>
where
    Load: FnMut(usize) -> Result<Vec<f32>, AdaptivePlanningError>,
{
    evaluate_collapse_diagnostics_with_embedding_replay_batches(
        represented_item_count,
        embedding_count,
        dimensions,
        settings,
        &mut |indices| indices.iter().map(|&index| load_embedding(index)).collect(),
    )
}

fn evaluate_collapse_diagnostics_with_embedding_replay_batches<LoadBatch>(
    represented_item_count: usize,
    embedding_count: usize,
    dimensions: usize,
    settings: &AdaptiveDirectionalPcaSettings,
    mut load_embeddings: &mut LoadBatch,
) -> Result<AdaptivePlanningDiagnostics, AdaptivePlanningError>
where
    LoadBatch: FnMut(&[usize]) -> Result<Vec<Vec<f32>>, AdaptivePlanningError>,
{
    if represented_item_count == 0 {
        return Err(AdaptivePlanningError::DiagnosticComputation(
            "represented_item_count must be greater than zero".into(),
        ));
    }
    if embedding_count == 0 {
        return Err(AdaptivePlanningError::DiagnosticComputation(
            "adaptive diagnostics require at least one embedding".into(),
        ));
    }
    if represented_item_count < embedding_count {
        return Err(AdaptivePlanningError::DiagnosticComputation(format!(
            "represented_item_count {represented_item_count} cannot be smaller than the embedding count {embedding_count}"
        )));
    }
    if dimensions == 0 {
        return Err(AdaptivePlanningError::DiagnosticComputation(
            "adaptive diagnostics require non-empty embeddings".into(),
        ));
    }

    let cluster_count = diagnostic_cluster_count(
        settings.cluster_count,
        embedding_count,
        settings.params.allocation_policy,
    )?;
    let config = StreamingClusteringConfig {
        cluster_count,
        dimensions,
        balance_constraints: None,
        random_seed: settings.random_seed,
    };
    let mut trainer = DirectionalPcaStreamingTrainer::new(config, settings.params.clone())
        .map_err(map_streaming_clustering_error)?;
    for_each_replay_batch(embedding_count, &mut load_embeddings, |_, batch| {
        for embedding in batch {
            validate_replay_embedding_dimensions(embedding.as_slice(), dimensions)?;
        }
        trainer
            .ingest_batch(batch)
            .map_err(map_streaming_clustering_error)?;
        Ok(())
    })?;
    trainer
        .finish_pass()
        .map_err(map_streaming_clustering_error)?;
    trainer
        .complete_training()
        .map_err(map_streaming_clustering_error)?;
    let classifier = trainer
        .into_classifier()
        .map_err(map_streaming_clustering_error)?;

    let mut assignments = Vec::with_capacity(embedding_count);
    let mut centroid_sums: BTreeMap<u32, (Vec<f64>, usize)> = BTreeMap::new();
    for_each_replay_batch(embedding_count, &mut load_embeddings, |_, batch| {
        for embedding in batch {
            validate_replay_embedding_dimensions(embedding.as_slice(), dimensions)?;
        }
        let batch_assignments = classifier
            .assign_batch(batch)
            .map_err(map_streaming_clustering_error)?;
        for (embedding, cluster_id) in batch.iter().zip(batch_assignments.iter().copied()) {
            assignments.push(cluster_id);
            let entry = centroid_sums
                .entry(cluster_id)
                .or_insert_with(|| (vec![0.0; embedding.len()], 0));
            if entry.0.len() != embedding.len() {
                return Err(AdaptivePlanningError::DiagnosticComputation(
                    "cluster members must all share one embedding dimensionality".into(),
                ));
            }
            for (sum, value) in entry.0.iter_mut().zip(embedding.iter().copied()) {
                *sum += f64::from(value);
                if !sum.is_finite() {
                    return Err(AdaptivePlanningError::DiagnosticComputation(
                        "mean cluster radius centroid accumulation became non-finite".into(),
                    ));
                }
            }
            entry.1 += 1;
        }
        Ok(())
    })?;
    if centroid_sums.is_empty() {
        return Err(AdaptivePlanningError::DiagnosticComputation(
            "mean cluster radius requires at least one realized cluster".into(),
        ));
    }

    let mut centroids = BTreeMap::new();
    for (&cluster_id, (sum, count)) in &centroid_sums {
        let count = *count as f64;
        if !count.is_finite() || count <= 0.0 {
            return Err(AdaptivePlanningError::DiagnosticComputation(
                "mean cluster radius centroid normalization became invalid".into(),
            ));
        }
        centroids.insert(
            cluster_id,
            sum.iter()
                .map(|value| {
                    let normalized = *value / count;
                    if !normalized.is_finite() {
                        return Err(AdaptivePlanningError::DiagnosticComputation(
                            "mean cluster radius centroid normalization became non-finite".into(),
                        ));
                    }
                    checked_f32_from_f64(
                        normalized,
                        "mean cluster radius centroid normalization overflowed f32",
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
    }

    let mut cluster_radius_sums = BTreeMap::<u32, (f64, usize)>::new();
    for_each_replay_batch(
        embedding_count,
        &mut load_embeddings,
        |batch_start, batch| {
            for (offset, embedding) in batch.iter().enumerate() {
                validate_replay_embedding_dimensions(embedding.as_slice(), dimensions)?;
                let cluster_id = assignments[batch_start + offset];
                let centroid = centroids.get(&cluster_id).ok_or_else(|| {
                    AdaptivePlanningError::DiagnosticComputation(format!(
                        "cluster {cluster_id} did not have a computed centroid"
                    ))
                })?;
                let entry = cluster_radius_sums.entry(cluster_id).or_insert((0.0, 0));
                entry.0 += euclidean_distance(embedding, centroid)?;
                if !entry.0.is_finite() {
                    return Err(AdaptivePlanningError::DiagnosticComputation(
                        "mean cluster radius accumulation became non-finite".into(),
                    ));
                }
                entry.1 += 1;
            }
            Ok(())
        },
    )?;

    let mut total_cluster_radius = 0.0f64;
    for &cluster_id in centroids.keys() {
        let (member_radius_sum, member_count) = cluster_radius_sums
            .get(&cluster_id)
            .copied()
            .ok_or_else(|| {
                AdaptivePlanningError::DiagnosticComputation(format!(
                    "cluster {cluster_id} did not retain any members"
                ))
            })?;
        if member_count == 0 {
            return Err(AdaptivePlanningError::DiagnosticComputation(format!(
                "cluster {cluster_id} did not retain any members"
            )));
        }
        let mean_cluster_radius = member_radius_sum / member_count as f64;
        if !mean_cluster_radius.is_finite() {
            return Err(AdaptivePlanningError::DiagnosticComputation(
                "per-cluster mean radius became non-finite".into(),
            ));
        }
        total_cluster_radius += mean_cluster_radius;
        if !total_cluster_radius.is_finite() {
            return Err(AdaptivePlanningError::DiagnosticComputation(
                "mean cluster radius total became non-finite".into(),
            ));
        }
    }
    let mean_cluster_radius = total_cluster_radius / centroids.len() as f64;
    if !mean_cluster_radius.is_finite() {
        return Err(AdaptivePlanningError::DiagnosticComputation(
            "mean cluster radius became non-finite".into(),
        ));
    }

    Ok(AdaptivePlanningDiagnostics {
        represented_item_count,
        mean_cluster_radius: checked_f32_from_f64(
            mean_cluster_radius,
            "mean cluster radius overflowed f32",
        )?,
    })
}

fn for_each_replay_batch<LoadBatch, VisitBatch>(
    embedding_count: usize,
    load_embeddings: &mut LoadBatch,
    mut visit_batch: VisitBatch,
) -> Result<(), AdaptivePlanningError>
where
    LoadBatch: FnMut(&[usize]) -> Result<Vec<Vec<f32>>, AdaptivePlanningError>,
    VisitBatch: FnMut(usize, &[Vec<f32>]) -> Result<(), AdaptivePlanningError>,
{
    let mut batch_indices = Vec::with_capacity(REPLAY_BATCH_SIZE);
    for batch_start in (0..embedding_count).step_by(REPLAY_BATCH_SIZE) {
        let batch_end = (batch_start + REPLAY_BATCH_SIZE).min(embedding_count);
        batch_indices.clear();
        batch_indices.extend(batch_start..batch_end);
        let batch = load_embeddings(batch_indices.as_slice())?;
        if batch.len() != batch_indices.len() {
            return Err(AdaptivePlanningError::DiagnosticComputation(format!(
                "replay loader returned {} embeddings for {} requested indices",
                batch.len(),
                batch_indices.len()
            )));
        }
        visit_batch(batch_start, batch.as_slice())?;
    }
    Ok(())
}

fn map_streaming_clustering_error(error: StreamingClusteringError) -> AdaptivePlanningError {
    match error {
        StreamingClusteringError::InvalidConfiguration { message } => {
            AdaptivePlanningError::InvalidConfiguration(message)
        }
        StreamingClusteringError::InvalidTransition { .. }
        | StreamingClusteringError::UnsatisfiableConstraint { .. }
        | StreamingClusteringError::MalformedInput { .. } => {
            AdaptivePlanningError::DiagnosticComputation(error.to_string())
        }
    }
}

fn diagnostic_cluster_count(
    configured_cluster_count: u32,
    embedding_count: usize,
    allocation_policy: lexongraph_directional_pca::DirectionalPcaAllocationPolicy,
) -> Result<u32, AdaptivePlanningError> {
    let capped = usize::try_from(configured_cluster_count)
        .map_err(|_| {
            AdaptivePlanningError::InvalidConfiguration(
                "directional-PCA cluster_count does not fit in usize".into(),
            )
        })?
        .min(embedding_count.max(1));
    let adjusted = if allocation_policy
        == lexongraph_directional_pca::DirectionalPcaAllocationPolicy::EigenvalueLogBits
        && capped > 1
    {
        highest_power_of_two_at_most(capped)
    } else {
        capped
    };
    u32::try_from(adjusted).map_err(|_| {
        AdaptivePlanningError::InvalidConfiguration(
            "diagnostic cluster_count exceeds u32::MAX".into(),
        )
    })
}

fn highest_power_of_two_at_most(value: usize) -> usize {
    if value <= 1 {
        value
    } else {
        1usize << (usize::BITS - 1 - value.leading_zeros())
    }
}

fn compute_mean_cluster_radius(
    embeddings: &[Vec<f32>],
    assignments: &[u32],
) -> Result<f32, AdaptivePlanningError> {
    if assignments.len() != embeddings.len() {
        return Err(AdaptivePlanningError::DiagnosticComputation(format!(
            "assignment count {} did not match embedding count {}",
            assignments.len(),
            embeddings.len()
        )));
    }

    let mut centroid_sums: BTreeMap<u32, (Vec<f64>, usize)> = BTreeMap::new();
    for (embedding, &cluster_id) in embeddings.iter().zip(assignments.iter()) {
        let entry = centroid_sums
            .entry(cluster_id)
            .or_insert_with(|| (vec![0.0; embedding.len()], 0));
        if entry.0.len() != embedding.len() {
            return Err(AdaptivePlanningError::DiagnosticComputation(
                "cluster members must all share one embedding dimensionality".into(),
            ));
        }
        for (sum, value) in entry.0.iter_mut().zip(embedding.iter().copied()) {
            *sum += f64::from(value);
            if !sum.is_finite() {
                return Err(AdaptivePlanningError::DiagnosticComputation(
                    "mean cluster radius centroid accumulation became non-finite".into(),
                ));
            }
        }
        entry.1 += 1;
    }

    if centroid_sums.is_empty() {
        return Err(AdaptivePlanningError::DiagnosticComputation(
            "mean cluster radius requires at least one realized cluster".into(),
        ));
    }

    let mut centroids = BTreeMap::new();
    for (&cluster_id, (sum, count)) in &centroid_sums {
        let count = *count as f64;
        if !count.is_finite() || count <= 0.0 {
            return Err(AdaptivePlanningError::DiagnosticComputation(
                "mean cluster radius centroid normalization became invalid".into(),
            ));
        }
        centroids.insert(
            cluster_id,
            sum.iter()
                .map(|value| {
                    let normalized = *value / count;
                    if !normalized.is_finite() {
                        return Err(AdaptivePlanningError::DiagnosticComputation(
                            "mean cluster radius centroid normalization became non-finite".into(),
                        ));
                    }
                    checked_f32_from_f64(
                        normalized,
                        "mean cluster radius centroid normalization overflowed f32",
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
    }

    let mut cluster_radius_sums = BTreeMap::<u32, (f64, usize)>::new();
    for (embedding, &cluster_id) in embeddings.iter().zip(assignments.iter()) {
        let centroid = centroids.get(&cluster_id).ok_or_else(|| {
            AdaptivePlanningError::DiagnosticComputation(format!(
                "cluster {cluster_id} did not have a computed centroid"
            ))
        })?;
        let entry = cluster_radius_sums.entry(cluster_id).or_insert((0.0, 0));
        entry.0 += euclidean_distance(embedding, centroid)?;
        if !entry.0.is_finite() {
            return Err(AdaptivePlanningError::DiagnosticComputation(
                "mean cluster radius accumulation became non-finite".into(),
            ));
        }
        entry.1 += 1;
    }

    let mut total_cluster_radius = 0.0f64;
    for &cluster_id in centroids.keys() {
        let (member_radius_sum, member_count) = cluster_radius_sums
            .get(&cluster_id)
            .copied()
            .ok_or_else(|| {
                AdaptivePlanningError::DiagnosticComputation(format!(
                    "cluster {cluster_id} did not retain any members"
                ))
            })?;
        if member_count == 0 {
            return Err(AdaptivePlanningError::DiagnosticComputation(format!(
                "cluster {cluster_id} did not retain any members"
            )));
        }
        let mean_cluster_radius = member_radius_sum / member_count as f64;
        if !mean_cluster_radius.is_finite() {
            return Err(AdaptivePlanningError::DiagnosticComputation(
                "per-cluster mean radius became non-finite".into(),
            ));
        }
        total_cluster_radius += mean_cluster_radius;
        if !total_cluster_radius.is_finite() {
            return Err(AdaptivePlanningError::DiagnosticComputation(
                "mean cluster radius total became non-finite".into(),
            ));
        }
    }

    let mean_cluster_radius = total_cluster_radius / centroids.len() as f64;
    if !mean_cluster_radius.is_finite() {
        return Err(AdaptivePlanningError::DiagnosticComputation(
            "mean cluster radius became non-finite".into(),
        ));
    }

    checked_f32_from_f64(mean_cluster_radius, "mean cluster radius overflowed f32")
}

fn checked_f32_from_f64(value: f64, message: &'static str) -> Result<f32, AdaptivePlanningError> {
    let value = value as f32;
    if !value.is_finite() {
        return Err(AdaptivePlanningError::DiagnosticComputation(message.into()));
    }
    Ok(value)
}

fn validate_replay_embedding_dimensions(
    embedding: &[f32],
    expected_dimensions: usize,
) -> Result<(), AdaptivePlanningError> {
    if embedding.len() != expected_dimensions {
        return Err(AdaptivePlanningError::DiagnosticComputation(format!(
            "replayed embedding dimension {} did not match expected {}",
            embedding.len(),
            expected_dimensions
        )));
    }
    Ok(())
}

fn euclidean_distance(left: &[f32], right: &[f32]) -> Result<f64, AdaptivePlanningError> {
    if left.len() != right.len() {
        return Err(AdaptivePlanningError::DiagnosticComputation(format!(
            "cannot compare embeddings with different dimensionalities: {} and {}",
            left.len(),
            right.len()
        )));
    }

    let mut squared_distance = 0.0f64;
    for (&lhs, &rhs) in left.iter().zip(right.iter()) {
        let delta = f64::from(lhs) - f64::from(rhs);
        if !delta.is_finite() {
            return Err(AdaptivePlanningError::DiagnosticComputation(
                "distance computation produced a non-finite delta".into(),
            ));
        }
        squared_distance += delta * delta;
        if !squared_distance.is_finite() {
            return Err(AdaptivePlanningError::DiagnosticComputation(
                "distance computation produced a non-finite squared distance".into(),
            ));
        }
    }
    let distance = squared_distance.sqrt();
    if !distance.is_finite() {
        return Err(AdaptivePlanningError::DiagnosticComputation(
            "distance computation produced a non-finite radius".into(),
        ));
    }
    Ok(distance)
}

#[cfg(test)]
mod tests {
    use super::{AdaptivePlanningError, checked_f32_from_f64, compute_mean_cluster_radius};

    #[test]
    fn mean_cluster_radius_rejects_f32_overflow() {
        let embeddings = vec![vec![f32::MAX, f32::MAX], vec![-f32::MAX, -f32::MAX]];
        let error = compute_mean_cluster_radius(&embeddings, &[0, 0]).unwrap_err();
        assert!(matches!(
            error,
            AdaptivePlanningError::DiagnosticComputation(message)
                if message == "mean cluster radius overflowed f32"
        ));
    }

    #[test]
    fn checked_f32_from_f64_rejects_overflow() {
        let error =
            checked_f32_from_f64(f64::MAX, "mean cluster radius overflowed f32").unwrap_err();
        assert!(matches!(
            error,
            AdaptivePlanningError::DiagnosticComputation(message)
                if message == "mean cluster radius overflowed f32"
        ));
    }
}
