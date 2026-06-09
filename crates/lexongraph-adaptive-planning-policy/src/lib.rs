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
    if params.retained_dimension_count == 0 {
        return Err(AdaptivePlanningError::InvalidConfiguration(
            "retained_dimension_count must be greater than zero".into(),
        ));
    }
    if params.retained_dimension_count > settings.cluster_count as usize {
        return Err(AdaptivePlanningError::InvalidConfiguration(format!(
            "retained_dimension_count {} cannot exceed directional-PCA cluster_count {}",
            params.retained_dimension_count, settings.cluster_count
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
    if params.min_effective_rank == 0 || params.min_effective_rank > params.retained_dimension_count
    {
        return Err(AdaptivePlanningError::InvalidConfiguration(format!(
            "min_effective_rank must be in [1, {}], got {}",
            params.retained_dimension_count, params.min_effective_rank
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
    let cluster_count = diagnostic_cluster_count(settings.cluster_count, embeddings.len())?;

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
) -> Result<u32, AdaptivePlanningError> {
    let capped = usize::try_from(configured_cluster_count)
        .map_err(|_| {
            AdaptivePlanningError::InvalidConfiguration(
                "directional-PCA cluster_count does not fit in usize".into(),
            )
        })?
        .min(embedding_count.max(1));
    u32::try_from(capped).map_err(|_| {
        AdaptivePlanningError::InvalidConfiguration(
            "diagnostic cluster_count exceeds u32::MAX".into(),
        )
    })
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
                    Ok(normalized as f32)
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

    Ok(mean_cluster_radius as f32)
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
