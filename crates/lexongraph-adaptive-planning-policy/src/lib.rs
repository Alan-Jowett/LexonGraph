// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

//! Deterministic adaptive planning-policy selection for LexonGraph.

use std::fmt;

use lexongraph_directional_pca::DirectionalPcaParams;
use lexongraph_pca::fit;
use lexongraph_streaming_clustering::BalanceConstraints;

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
    pub min_effective_rank: usize,
    pub min_cumulative_variance: f32,
    pub tie_break: AdaptiveSwitchTieBreak,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdaptiveSwitchTieBreak {
    PreferDirectionalPca,
    PreferDcbc,
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
    EvaluatedDirectionalPca,
    PreviouslySwitchedToDcbc,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AdaptivePlanningDiagnostics {
    pub represented_item_count: usize,
    pub effective_rank: usize,
    pub retained_cumulative_variance: f32,
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

        let diagnostics = evaluate_collapse_diagnostics(
            represented_item_count,
            embeddings,
            &self.settings.directional_pca.params,
        )?;
        let switch_boundary_occurred = diagnostics.effective_rank
            < self.settings.switch_criteria.min_effective_rank
            || diagnostics.retained_cumulative_variance
                < self.settings.switch_criteria.min_cumulative_variance
            || (diagnostics.retained_cumulative_variance
                == self.settings.switch_criteria.min_cumulative_variance
                && matches!(
                    self.settings.switch_criteria.tie_break,
                    AdaptiveSwitchTieBreak::PreferDcbc
                ));
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
    if settings.switch_criteria.min_effective_rank == 0 {
        return Err(AdaptivePlanningError::InvalidConfiguration(
            "min_effective_rank must be at least 1".into(),
        ));
    }
    if !settings.switch_criteria.min_cumulative_variance.is_finite()
        || !(0.0..=1.0).contains(&settings.switch_criteria.min_cumulative_variance)
    {
        return Err(AdaptivePlanningError::InvalidConfiguration(
            "min_cumulative_variance must be finite and in [0.0, 1.0]".into(),
        ));
    }
    if settings.directional_pca.params.retained_dimension_count == 0 {
        return Err(AdaptivePlanningError::InvalidConfiguration(
            "retained_dimension_count must be greater than zero".into(),
        ));
    }
    Ok(())
}

fn evaluate_collapse_diagnostics(
    represented_item_count: usize,
    embeddings: &[Vec<f32>],
    params: &DirectionalPcaParams,
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

    let transform = fit(embeddings)
        .map_err(|error| AdaptivePlanningError::DiagnosticComputation(error.to_string()))?;
    let diagnostics = transform.diagnostics();
    let retained_cumulative_variance = diagnostics
        .cumulative_variance
        .as_ref()
        .and_then(|values| {
            values
                .get(params.retained_dimension_count.saturating_sub(1))
                .copied()
        })
        .unwrap_or(0.0);
    Ok(AdaptivePlanningDiagnostics {
        represented_item_count,
        effective_rank: diagnostics.rank_estimate,
        retained_cumulative_variance,
    })
}
