// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Deterministic adaptive planning-policy selection for LexonGraph.

use std::fmt;

use lexongraph_directional_pca::DirectionalPcaParams;
use lexongraph_pca::{PcaError, fit};
use lexongraph_streaming_clustering::BalanceConstraints;

pub const DEFAULT_PC1_EXPLAINED_VARIANCE_RATIO_THRESHOLD: f32 = 0.25;
pub const DEFAULT_DCBC_MAX_EMBEDDING_COUNT: usize = 2048;
pub const DEFAULT_EMBEDDING_COUNT_CUTOFF: usize = 1000;

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
pub struct AdaptiveDivisiveSwitchSettings {
    pub pc1_explained_variance_ratio_threshold: f32,
    pub dcbc_max_embedding_count: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdaptivePlanningSettings {
    pub direction: AdaptivePlanningDirection,
    pub directional_pca: AdaptiveDirectionalPcaSettings,
    pub dcbc: AdaptiveDcbcSettings,
    pub divisive_switch: AdaptiveDivisiveSwitchSettings,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivePlanningAlgorithm {
    DirectionalPca,
    Dcbc,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdaptivePlanningDecisionReason {
    InitialDirectionalPcaSegment,
    StayedOnDirectionalPcaAtOrAbovePc1Threshold,
    StayedOnDirectionalPcaAtOrAboveEmbeddingCountLimit,
    SelectedDcbcBelowPc1ThresholdAndBelowEmbeddingCountLimit,
    StayedOnDirectionalPcaAtOrAboveEmbeddingCountCutoff,
    SwitchedToDcbcBelowEmbeddingCountCutoff,
    PreviouslySwitchedToDcbc,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdaptivePlanningDiagnostics {
    pub embedding_count: usize,
    pub pc1_explained_variance_ratio: Option<f32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdaptiveSwitchDecisionRecord {
    pub boundary_position: usize,
    pub active_algorithm: ActivePlanningAlgorithm,
    pub switch_boundary_occurred: bool,
    pub pc1_explained_variance_ratio_threshold: Option<f32>,
    pub dcbc_max_embedding_count: Option<usize>,
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
        embeddings: &[Vec<f32>],
    ) -> Result<ActivePlanningAlgorithm, AdaptivePlanningError> {
        let boundary_position = self.decision_records.len();

        if self.settings.direction == AdaptivePlanningDirection::Agglomerative
            && self.switched_to_dcbc
        {
            self.decision_records.push(AdaptiveSwitchDecisionRecord {
                boundary_position,
                active_algorithm: ActivePlanningAlgorithm::Dcbc,
                switch_boundary_occurred: false,
                pc1_explained_variance_ratio_threshold: None,
                dcbc_max_embedding_count: None,
                reason: AdaptivePlanningDecisionReason::PreviouslySwitchedToDcbc,
                collapse_diagnostics: None,
            });
            return Ok(ActivePlanningAlgorithm::Dcbc);
        }

        if self.decision_records.is_empty() {
            self.decision_records.push(AdaptiveSwitchDecisionRecord {
                boundary_position,
                active_algorithm: ActivePlanningAlgorithm::DirectionalPca,
                switch_boundary_occurred: false,
                pc1_explained_variance_ratio_threshold: None,
                dcbc_max_embedding_count: None,
                reason: AdaptivePlanningDecisionReason::InitialDirectionalPcaSegment,
                collapse_diagnostics: None,
            });
            return Ok(ActivePlanningAlgorithm::DirectionalPca);
        }

        let previous_algorithm = self
            .decision_records
            .last()
            .map(|record| record.active_algorithm);
        let decision = match self.settings.direction {
            AdaptivePlanningDirection::Divisive => self.select_divisive(embeddings)?,
            AdaptivePlanningDirection::Agglomerative => self.select_agglomerative(embeddings)?,
        };
        let switch_boundary_occurred = previous_algorithm
            == Some(ActivePlanningAlgorithm::DirectionalPca)
            && decision.active_algorithm == ActivePlanningAlgorithm::Dcbc;
        if self.settings.direction == AdaptivePlanningDirection::Agglomerative
            && switch_boundary_occurred
        {
            self.switched_to_dcbc = true;
        }

        self.decision_records.push(AdaptiveSwitchDecisionRecord {
            boundary_position,
            active_algorithm: decision.active_algorithm,
            switch_boundary_occurred,
            pc1_explained_variance_ratio_threshold: decision.pc1_explained_variance_ratio_threshold,
            dcbc_max_embedding_count: decision.dcbc_max_embedding_count,
            reason: decision.reason,
            collapse_diagnostics: Some(decision.diagnostics),
        });
        Ok(decision.active_algorithm)
    }

    fn select_divisive(
        &self,
        embeddings: &[Vec<f32>],
    ) -> Result<AdaptiveDecision, AdaptivePlanningError> {
        let diagnostics = evaluate_divisive_diagnostics(embeddings)?;
        let pc1 = diagnostics.pc1_explained_variance_ratio.ok_or_else(|| {
            AdaptivePlanningError::DiagnosticComputation(
                "divisive adaptive diagnostics require a PC1 explained variance ratio".into(),
            )
        })?;
        let threshold = self
            .settings
            .divisive_switch
            .pc1_explained_variance_ratio_threshold;
        let dcbc_max_embedding_count = self.settings.divisive_switch.dcbc_max_embedding_count;

        let (active_algorithm, reason) = if pc1 >= threshold {
            (
                ActivePlanningAlgorithm::DirectionalPca,
                AdaptivePlanningDecisionReason::StayedOnDirectionalPcaAtOrAbovePc1Threshold,
            )
        } else if diagnostics.embedding_count < dcbc_max_embedding_count {
            (
                ActivePlanningAlgorithm::Dcbc,
                AdaptivePlanningDecisionReason::SelectedDcbcBelowPc1ThresholdAndBelowEmbeddingCountLimit,
            )
        } else {
            (
                ActivePlanningAlgorithm::DirectionalPca,
                AdaptivePlanningDecisionReason::StayedOnDirectionalPcaAtOrAboveEmbeddingCountLimit,
            )
        };

        Ok(AdaptiveDecision {
            active_algorithm,
            reason,
            diagnostics,
            pc1_explained_variance_ratio_threshold: Some(threshold),
            dcbc_max_embedding_count: Some(dcbc_max_embedding_count),
        })
    }

    fn select_agglomerative(
        &self,
        embeddings: &[Vec<f32>],
    ) -> Result<AdaptiveDecision, AdaptivePlanningError> {
        let diagnostics = evaluate_agglomerative_diagnostics(embeddings)?;
        let (active_algorithm, reason) =
            if diagnostics.embedding_count < DEFAULT_EMBEDDING_COUNT_CUTOFF {
                (
                    ActivePlanningAlgorithm::Dcbc,
                    AdaptivePlanningDecisionReason::SwitchedToDcbcBelowEmbeddingCountCutoff,
                )
            } else {
                (
                ActivePlanningAlgorithm::DirectionalPca,
                AdaptivePlanningDecisionReason::StayedOnDirectionalPcaAtOrAboveEmbeddingCountCutoff,
            )
            };
        Ok(AdaptiveDecision {
            active_algorithm,
            reason,
            diagnostics,
            pc1_explained_variance_ratio_threshold: None,
            dcbc_max_embedding_count: Some(DEFAULT_EMBEDDING_COUNT_CUTOFF),
        })
    }
}

struct AdaptiveDecision {
    active_algorithm: ActivePlanningAlgorithm,
    reason: AdaptivePlanningDecisionReason,
    diagnostics: AdaptivePlanningDiagnostics,
    pc1_explained_variance_ratio_threshold: Option<f32>,
    dcbc_max_embedding_count: Option<usize>,
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
    let threshold = settings
        .divisive_switch
        .pc1_explained_variance_ratio_threshold;
    if !threshold.is_finite() || !(0.0..=1.0).contains(&threshold) {
        return Err(AdaptivePlanningError::InvalidConfiguration(
            "pc1_explained_variance_ratio_threshold must be finite and in [0.0, 1.0]".into(),
        ));
    }
    if settings.divisive_switch.dcbc_max_embedding_count == 0 {
        return Err(AdaptivePlanningError::InvalidConfiguration(
            "dcbc_max_embedding_count must be greater than zero".into(),
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

fn evaluate_divisive_diagnostics(
    embeddings: &[Vec<f32>],
) -> Result<AdaptivePlanningDiagnostics, AdaptivePlanningError> {
    if embeddings.is_empty() {
        return Err(AdaptivePlanningError::DiagnosticComputation(
            "adaptive diagnostics require at least one embedding".into(),
        ));
    }
    let first_dim = embeddings.first().map(std::vec::Vec::len).ok_or_else(|| {
        AdaptivePlanningError::DiagnosticComputation(
            "adaptive diagnostics require at least one embedding".into(),
        )
    })?;
    if first_dim == 0 {
        return Err(AdaptivePlanningError::DiagnosticComputation(
            "adaptive diagnostics require non-empty embeddings".into(),
        ));
    }
    if embeddings
        .iter()
        .any(|embedding| embedding.len() != first_dim)
    {
        return Err(AdaptivePlanningError::DiagnosticComputation(
            "adaptive diagnostics require one consistent embedding dimensionality".into(),
        ));
    }

    let transform = fit(embeddings).map_err(map_pca_error)?;
    let pc1_explained_variance_ratio = transform
        .cumulative_variance()
        .and_then(|ratios| ratios.into_iter().next())
        .unwrap_or(0.0);
    if !pc1_explained_variance_ratio.is_finite() {
        return Err(AdaptivePlanningError::DiagnosticComputation(
            "PC1 explained variance ratio became non-finite".into(),
        ));
    }

    Ok(AdaptivePlanningDiagnostics {
        embedding_count: embeddings.len(),
        pc1_explained_variance_ratio: Some(pc1_explained_variance_ratio),
    })
}

fn evaluate_agglomerative_diagnostics(
    embeddings: &[Vec<f32>],
) -> Result<AdaptivePlanningDiagnostics, AdaptivePlanningError> {
    if embeddings.is_empty() {
        return Err(AdaptivePlanningError::DiagnosticComputation(
            "adaptive diagnostics require at least one embedding".into(),
        ));
    }
    Ok(AdaptivePlanningDiagnostics {
        embedding_count: embeddings.len(),
        pc1_explained_variance_ratio: None,
    })
}

fn map_pca_error(error: PcaError) -> AdaptivePlanningError {
    AdaptivePlanningError::DiagnosticComputation(error.to_string())
}
