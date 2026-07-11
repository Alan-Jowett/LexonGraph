// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

//! Deterministic adaptive planning-policy selection for LexonGraph.

use std::collections::BTreeMap;
use std::fmt;

use lexongraph_directional_pca::{
    DirectionalPcaParams, DirectionalPcaStreamingClassifier, DirectionalPcaStreamingTrainer,
};
use lexongraph_streaming_clustering::{
    BalanceConstraints, PassReadiness, StreamingClusterTrainer, StreamingClusteringConfig,
    StreamingClusteringError,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdaptiveReplayStage {
    CollectingSummaries,
    MeasuringDecision,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdaptiveSelectionProgress {
    Selected(ActivePlanningAlgorithm),
    ReplayRequired(AdaptiveReplayStage),
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

#[derive(Debug)]
pub struct AdaptivePlanningSelector {
    settings: AdaptivePlanningSettings,
    switched_to_dcbc: bool,
    decision_records: Vec<AdaptiveSwitchDecisionRecord>,
    active_boundary: Option<AdaptiveDecisionBoundary>,
}

impl AdaptivePlanningSelector {
    pub fn new(settings: AdaptivePlanningSettings) -> Result<Self, AdaptivePlanningError> {
        validate_settings(&settings)?;
        Ok(Self {
            settings,
            switched_to_dcbc: false,
            decision_records: Vec::new(),
            active_boundary: None,
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

    pub fn begin_selection_boundary(
        &mut self,
        represented_item_count: usize,
        boundary_embedding_count: usize,
        dimensions: usize,
    ) -> Result<AdaptiveSelectionProgress, AdaptivePlanningError> {
        if self.active_boundary.is_some() {
            return Err(AdaptivePlanningError::DiagnosticComputation(
                "adaptive selection boundary is already active".into(),
            ));
        }
        if self.switched_to_dcbc {
            self.decision_records.push(AdaptiveSwitchDecisionRecord {
                active_algorithm: ActivePlanningAlgorithm::Dcbc,
                switch_boundary_occurred: false,
                reason: AdaptivePlanningDecisionReason::PreviouslySwitchedToDcbc,
                collapse_diagnostics: None,
            });
            return Ok(AdaptiveSelectionProgress::Selected(
                ActivePlanningAlgorithm::Dcbc,
            ));
        }

        if self.decision_records.is_empty() {
            self.decision_records.push(AdaptiveSwitchDecisionRecord {
                active_algorithm: ActivePlanningAlgorithm::DirectionalPca,
                switch_boundary_occurred: false,
                reason: AdaptivePlanningDecisionReason::InitialDirectionalPcaSegment,
                collapse_diagnostics: None,
            });
            return Ok(AdaptiveSelectionProgress::Selected(
                ActivePlanningAlgorithm::DirectionalPca,
            ));
        }

        self.active_boundary = Some(AdaptiveDecisionBoundary::new(
            represented_item_count,
            boundary_embedding_count,
            dimensions,
            &self.settings.directional_pca,
        )?);
        Ok(AdaptiveSelectionProgress::ReplayRequired(
            AdaptiveReplayStage::CollectingSummaries,
        ))
    }

    pub fn ingest_selection_batch(
        &mut self,
        embeddings: &[Vec<f32>],
    ) -> Result<(), AdaptivePlanningError> {
        let boundary = self.active_boundary.as_mut().ok_or_else(|| {
            AdaptivePlanningError::DiagnosticComputation(
                "adaptive selection boundary is not active".into(),
            )
        })?;
        boundary.ingest_batch(embeddings)
    }

    pub fn finish_selection_pass(
        &mut self,
    ) -> Result<AdaptiveSelectionProgress, AdaptivePlanningError> {
        let boundary = self.active_boundary.as_mut().ok_or_else(|| {
            AdaptivePlanningError::DiagnosticComputation(
                "adaptive selection boundary is not active".into(),
            )
        })?;
        match boundary.finish_pass()? {
            AdaptiveBoundaryState::CollectingSummaries => Ok(
                AdaptiveSelectionProgress::ReplayRequired(AdaptiveReplayStage::CollectingSummaries),
            ),
            AdaptiveBoundaryState::MeasuringDecision => Ok(
                AdaptiveSelectionProgress::ReplayRequired(AdaptiveReplayStage::MeasuringDecision),
            ),
            AdaptiveBoundaryState::Selected(diagnostics) => {
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
                self.active_boundary = None;
                Ok(AdaptiveSelectionProgress::Selected(active_algorithm))
            }
        }
    }
}

#[derive(Debug)]
struct AdaptiveDecisionBoundary {
    represented_item_count: usize,
    phase: AdaptiveReplayStage,
    partition_ready_passes: usize,
    trainer: Option<DirectionalPcaStreamingTrainer>,
    classifier: Option<DirectionalPcaStreamingClassifier>,
    measurement: Option<MeanClusterRadiusMeasurement>,
}

#[derive(Debug)]
enum AdaptiveBoundaryState {
    CollectingSummaries,
    MeasuringDecision,
    Selected(AdaptivePlanningDiagnostics),
}

#[derive(Debug)]
struct MeanClusterRadiusMeasurement {
    represented_item_count: usize,
    cluster_radius_sums: BTreeMap<u32, (f64, usize)>,
    observed_embedding_count: usize,
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

impl AdaptiveDecisionBoundary {
    fn new(
        represented_item_count: usize,
        boundary_embedding_count: usize,
        dimensions: usize,
        settings: &AdaptiveDirectionalPcaSettings,
    ) -> Result<Self, AdaptivePlanningError> {
        if represented_item_count == 0 {
            return Err(AdaptivePlanningError::DiagnosticComputation(
                "represented_item_count must be greater than zero".into(),
            ));
        }
        if boundary_embedding_count == 0 {
            return Err(AdaptivePlanningError::DiagnosticComputation(
                "adaptive diagnostics require at least one boundary embedding".into(),
            ));
        }
        if represented_item_count < boundary_embedding_count {
            return Err(AdaptivePlanningError::DiagnosticComputation(format!(
                "represented_item_count {represented_item_count} cannot be smaller than the boundary embedding count {boundary_embedding_count}"
            )));
        }
        if dimensions == 0 {
            return Err(AdaptivePlanningError::DiagnosticComputation(
                "adaptive diagnostics require non-empty embeddings".into(),
            ));
        }
        let cluster_count = diagnostic_cluster_count(
            settings.cluster_count,
            boundary_embedding_count,
            settings.params.allocation_policy,
        )?;
        let trainer = DirectionalPcaStreamingTrainer::new(
            StreamingClusteringConfig {
                cluster_count,
                dimensions,
                balance_constraints: None,
                random_seed: settings.random_seed,
            },
            settings.params.clone(),
        )
        .map_err(map_streaming_clustering_error)?;
        Ok(Self {
            represented_item_count,
            phase: AdaptiveReplayStage::CollectingSummaries,
            partition_ready_passes: 0,
            trainer: Some(trainer),
            classifier: None,
            measurement: None,
        })
    }

    fn ingest_batch(&mut self, embeddings: &[Vec<f32>]) -> Result<(), AdaptivePlanningError> {
        match self.phase {
            AdaptiveReplayStage::CollectingSummaries => self
                .trainer
                .as_mut()
                .ok_or_else(|| {
                    AdaptivePlanningError::DiagnosticComputation(
                        "directional-PCA training pass is not active".into(),
                    )
                })?
                .ingest_batch(embeddings)
                .map_err(map_streaming_clustering_error),
            AdaptiveReplayStage::MeasuringDecision => self
                .measurement
                .as_mut()
                .ok_or_else(|| {
                    AdaptivePlanningError::DiagnosticComputation(
                        "adaptive measurement pass is not active".into(),
                    )
                })?
                .observe_batch(
                    self.classifier.as_ref().ok_or_else(|| {
                        AdaptivePlanningError::DiagnosticComputation(
                            "directional-PCA classifier is not available for measurement".into(),
                        )
                    })?,
                    embeddings,
                ),
        }
    }

    fn finish_pass(&mut self) -> Result<AdaptiveBoundaryState, AdaptivePlanningError> {
        match self.phase {
            AdaptiveReplayStage::CollectingSummaries => {
                let trainer = self.trainer.as_mut().ok_or_else(|| {
                    AdaptivePlanningError::DiagnosticComputation(
                        "directional-PCA training pass is not active".into(),
                    )
                })?;
                let report = trainer
                    .finish_pass()
                    .map_err(map_streaming_clustering_error)?;
                match report.readiness {
                    PassReadiness::AnalysisOnly => Ok(AdaptiveBoundaryState::CollectingSummaries),
                    PassReadiness::PartitionReady => {
                        self.partition_ready_passes += 1;
                        if self.partition_ready_passes < 2 {
                            Ok(AdaptiveBoundaryState::CollectingSummaries)
                        } else {
                            let mut trainer = self.trainer.take().ok_or_else(|| {
                                AdaptivePlanningError::DiagnosticComputation(
                                    "directional-PCA trainer disappeared before classifier creation"
                                        .into(),
                                )
                            })?;
                            trainer
                                .complete_training()
                                .map_err(map_streaming_clustering_error)?;
                            let classifier = trainer
                                .into_classifier()
                                .map_err(map_streaming_clustering_error)?;
                            self.classifier = Some(classifier);
                            self.measurement = Some(MeanClusterRadiusMeasurement::new(
                                self.represented_item_count,
                            ));
                            self.phase = AdaptiveReplayStage::MeasuringDecision;
                            Ok(AdaptiveBoundaryState::MeasuringDecision)
                        }
                    }
                }
            }
            AdaptiveReplayStage::MeasuringDecision => Ok(AdaptiveBoundaryState::Selected(
                self.measurement
                    .take()
                    .ok_or_else(|| {
                        AdaptivePlanningError::DiagnosticComputation(
                            "adaptive measurement pass is not active".into(),
                        )
                    })?
                    .finish()?,
            )),
        }
    }
}

impl MeanClusterRadiusMeasurement {
    fn new(represented_item_count: usize) -> Self {
        Self {
            represented_item_count,
            cluster_radius_sums: BTreeMap::new(),
            observed_embedding_count: 0,
        }
    }

    fn observe_batch(
        &mut self,
        classifier: &DirectionalPcaStreamingClassifier,
        embeddings: &[Vec<f32>],
    ) -> Result<(), AdaptivePlanningError> {
        for embedding in embeddings {
            let (cluster_id, distance) = classifier
                .assigned_distance(embedding)
                .map_err(map_streaming_clustering_error)?;
            let entry = self
                .cluster_radius_sums
                .entry(cluster_id)
                .or_insert((0.0, 0));
            entry.0 += distance;
            if !entry.0.is_finite() {
                return Err(AdaptivePlanningError::DiagnosticComputation(
                    "mean cluster radius accumulation became non-finite".into(),
                ));
            }
            entry.1 += 1;
            self.observed_embedding_count += 1;
        }
        Ok(())
    }

    fn finish(self) -> Result<AdaptivePlanningDiagnostics, AdaptivePlanningError> {
        if self.observed_embedding_count == 0 || self.cluster_radius_sums.is_empty() {
            return Err(AdaptivePlanningError::DiagnosticComputation(
                "mean cluster radius requires at least one observed embedding".into(),
            ));
        }

        let mut total_cluster_radius = 0.0f64;
        for (&cluster_id, (member_radius_sum, member_count)) in &self.cluster_radius_sums {
            if *member_count == 0 {
                return Err(AdaptivePlanningError::DiagnosticComputation(format!(
                    "cluster {cluster_id} did not retain any members"
                )));
            }
            let mean_cluster_radius = *member_radius_sum / *member_count as f64;
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

        let mean_cluster_radius = total_cluster_radius / self.cluster_radius_sums.len() as f64;
        if !mean_cluster_radius.is_finite() {
            return Err(AdaptivePlanningError::DiagnosticComputation(
                "mean cluster radius became non-finite".into(),
            ));
        }

        Ok(AdaptivePlanningDiagnostics {
            represented_item_count: self.represented_item_count,
            mean_cluster_radius: mean_cluster_radius as f32,
        })
    }
}
