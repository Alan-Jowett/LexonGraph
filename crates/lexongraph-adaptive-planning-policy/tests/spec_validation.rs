// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Executable verification for docs/specs/rust-adaptive-planning-policy-crate/validation.md

use lexongraph_adaptive_planning_policy::{
    ActivePlanningAlgorithm, AdaptiveDcbcSettings, AdaptiveDirectionalPcaSettings,
    AdaptiveDivisiveSwitchSettings, AdaptivePlanningDecisionReason, AdaptivePlanningDirection,
    AdaptivePlanningError, AdaptivePlanningSelector, AdaptivePlanningSettings,
    DEFAULT_DCBC_MAX_EMBEDDING_COUNT, DEFAULT_EMBEDDING_COUNT_CUTOFF,
    DEFAULT_PC1_EXPLAINED_VARIANCE_RATIO_THRESHOLD,
};
use lexongraph_directional_pca::DirectionalPcaParams;

fn directional_pca_settings() -> AdaptiveDirectionalPcaSettings {
    AdaptiveDirectionalPcaSettings {
        cluster_count: 2,
        random_seed: Some(7),
        params: DirectionalPcaParams {
            retained_dimension_count: 1,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance: 0.0,
        },
    }
}

fn dcbc_settings() -> AdaptiveDcbcSettings {
    AdaptiveDcbcSettings {
        cluster_count: 2,
        balance_constraints: None,
        random_seed: Some(11),
    }
}

fn settings(
    direction: AdaptivePlanningDirection,
    pc1_explained_variance_ratio_threshold: f32,
    dcbc_max_embedding_count: usize,
) -> AdaptivePlanningSettings {
    AdaptivePlanningSettings {
        direction,
        directional_pca: directional_pca_settings(),
        dcbc: dcbc_settings(),
        divisive_switch: AdaptiveDivisiveSwitchSettings {
            pc1_explained_variance_ratio_threshold,
            dcbc_max_embedding_count,
        },
    }
}

fn strong_pc1_embeddings() -> Vec<Vec<f32>> {
    vec![
        vec![-3.0, 0.0],
        vec![-1.0, 0.0],
        vec![1.0, 0.0],
        vec![3.0, 0.0],
    ]
}

fn weak_pc1_embeddings() -> Vec<Vec<f32>> {
    vec![
        vec![-1.0, -1.0],
        vec![-1.0, 1.0],
        vec![1.0, -1.0],
        vec![1.0, 1.0],
    ]
}

fn embeddings(count: usize) -> Vec<Vec<f32>> {
    (0..count)
        .map(|index| vec![index as f32, (index % 17) as f32])
        .collect()
}

#[test]
fn val_adaptive_policy_011_rejects_invalid_directional_pca_configuration() {
    let mut invalid = settings(
        AdaptivePlanningDirection::Divisive,
        DEFAULT_PC1_EXPLAINED_VARIANCE_RATIO_THRESHOLD,
        DEFAULT_DCBC_MAX_EMBEDDING_COUNT,
    );
    invalid.directional_pca.params.temperature = 0.0;
    let err = AdaptivePlanningSelector::new(invalid).unwrap_err();
    assert!(matches!(
        err,
        AdaptivePlanningError::InvalidConfiguration(_)
    ));
}

#[test]
fn val_adaptive_policy_004_starts_with_directional_pca_before_divisive_diagnostics_exist() {
    let mut selector = AdaptivePlanningSelector::new(settings(
        AdaptivePlanningDirection::Divisive,
        DEFAULT_PC1_EXPLAINED_VARIANCE_RATIO_THRESHOLD,
        DEFAULT_DCBC_MAX_EMBEDDING_COUNT,
    ))
    .unwrap();
    let algorithm = selector.select_algorithm(&strong_pc1_embeddings()).unwrap();
    assert_eq!(algorithm, ActivePlanningAlgorithm::DirectionalPca);
    assert!(!selector.switched_to_dcbc());
    let decision = selector.decision_records().last().unwrap();
    assert_eq!(
        decision.reason,
        AdaptivePlanningDecisionReason::InitialDirectionalPcaSegment
    );
    assert!(!decision.switch_boundary_occurred);
    assert_eq!(decision.pc1_explained_variance_ratio_threshold, None);
    assert_eq!(decision.dcbc_max_embedding_count, None);
    assert!(decision.collapse_diagnostics.is_none());
}

#[test]
fn val_adaptive_policy_005_supports_both_direction_modes() {
    for direction in [
        AdaptivePlanningDirection::Divisive,
        AdaptivePlanningDirection::Agglomerative,
    ] {
        let selector = AdaptivePlanningSelector::new(settings(
            direction,
            DEFAULT_PC1_EXPLAINED_VARIANCE_RATIO_THRESHOLD,
            DEFAULT_DCBC_MAX_EMBEDDING_COUNT,
        ))
        .unwrap();
        assert_eq!(selector.settings().direction, direction);
    }
}

#[test]
fn val_adaptive_policy_006_records_structured_divisive_diagnostics() {
    let mut selector =
        AdaptivePlanningSelector::new(settings(AdaptivePlanningDirection::Divisive, 0.6, 8))
            .unwrap();
    selector.select_algorithm(&weak_pc1_embeddings()).unwrap();
    selector.select_algorithm(&weak_pc1_embeddings()).unwrap();
    let decision = selector.decision_records().last().unwrap();
    let diagnostics = decision.collapse_diagnostics.as_ref().unwrap();
    assert_eq!(diagnostics.embedding_count, 4);
    let pc1 = diagnostics.pc1_explained_variance_ratio.unwrap();
    assert!((pc1 - 0.5).abs() < 1e-6);
    assert_eq!(decision.pc1_explained_variance_ratio_threshold, Some(0.6));
    assert_eq!(decision.dcbc_max_embedding_count, Some(8));
    assert_eq!(
        decision.reason,
        AdaptivePlanningDecisionReason::SelectedDcbcBelowPc1ThresholdAndBelowEmbeddingCountLimit
    );
}

#[test]
fn val_adaptive_policy_007_stays_on_directional_pca_when_pc1_is_at_or_above_threshold() {
    let mut selector = AdaptivePlanningSelector::new(settings(
        AdaptivePlanningDirection::Divisive,
        DEFAULT_PC1_EXPLAINED_VARIANCE_RATIO_THRESHOLD,
        DEFAULT_DCBC_MAX_EMBEDDING_COUNT,
    ))
    .unwrap();
    assert_eq!(
        selector.select_algorithm(&strong_pc1_embeddings()).unwrap(),
        ActivePlanningAlgorithm::DirectionalPca
    );
    assert_eq!(
        selector.select_algorithm(&strong_pc1_embeddings()).unwrap(),
        ActivePlanningAlgorithm::DirectionalPca
    );
    assert!(!selector.switched_to_dcbc());
    assert_eq!(
        selector.decision_records().last().unwrap().reason,
        AdaptivePlanningDecisionReason::StayedOnDirectionalPcaAtOrAbovePc1Threshold
    );
}

#[test]
fn val_adaptive_policy_008_switches_to_dcbc_when_pc1_is_below_threshold_and_collection_is_small_enough()
 {
    let mut selector =
        AdaptivePlanningSelector::new(settings(AdaptivePlanningDirection::Divisive, 0.6, 8))
            .unwrap();
    assert_eq!(
        selector.select_algorithm(&weak_pc1_embeddings()).unwrap(),
        ActivePlanningAlgorithm::DirectionalPca
    );
    assert_eq!(
        selector.select_algorithm(&weak_pc1_embeddings()).unwrap(),
        ActivePlanningAlgorithm::Dcbc
    );
    assert!(!selector.switched_to_dcbc());
    let decision = selector.decision_records().last().unwrap();
    assert!(decision.switch_boundary_occurred);
    assert_eq!(
        decision.reason,
        AdaptivePlanningDecisionReason::SelectedDcbcBelowPc1ThresholdAndBelowEmbeddingCountLimit
    );
}

#[test]
fn val_adaptive_policy_009_agglomerative_does_not_switch_back_after_dcbc_boundary() {
    let switch_fixture = embeddings(DEFAULT_EMBEDDING_COUNT_CUTOFF - 1);
    let stay_fixture = embeddings(DEFAULT_EMBEDDING_COUNT_CUTOFF);
    let mut selector = AdaptivePlanningSelector::new(settings(
        AdaptivePlanningDirection::Agglomerative,
        DEFAULT_PC1_EXPLAINED_VARIANCE_RATIO_THRESHOLD,
        DEFAULT_DCBC_MAX_EMBEDDING_COUNT,
    ))
    .unwrap();
    selector.select_algorithm(&switch_fixture).unwrap();
    selector.select_algorithm(&switch_fixture).unwrap();
    let third = selector.select_algorithm(&stay_fixture).unwrap();
    assert_eq!(third, ActivePlanningAlgorithm::Dcbc);
    let last = selector.decision_records().last().unwrap();
    assert_eq!(
        last.reason,
        AdaptivePlanningDecisionReason::PreviouslySwitchedToDcbc
    );
    assert!(!last.switch_boundary_occurred);
    assert_eq!(last.pc1_explained_variance_ratio_threshold, None);
    assert_eq!(last.dcbc_max_embedding_count, None);
    assert!(last.collapse_diagnostics.is_none());
}

#[test]
fn val_adaptive_policy_012_divisive_decisions_are_deterministic_per_collection() {
    let fixture = weak_pc1_embeddings();
    let mut first =
        AdaptivePlanningSelector::new(settings(AdaptivePlanningDirection::Divisive, 0.6, 8))
            .unwrap();
    let mut second =
        AdaptivePlanningSelector::new(settings(AdaptivePlanningDirection::Divisive, 0.6, 8))
            .unwrap();
    first.select_algorithm(&fixture).unwrap();
    first.select_algorithm(&fixture).unwrap();
    second.select_algorithm(&fixture).unwrap();
    second.select_algorithm(&fixture).unwrap();
    assert_eq!(first.decision_records(), second.decision_records());
}

#[test]
fn regression_adaptive_policy_keeps_directional_pca_when_pc1_is_below_threshold_but_collection_is_too_large()
 {
    let fixture = weak_pc1_embeddings();
    let mut selector =
        AdaptivePlanningSelector::new(settings(AdaptivePlanningDirection::Divisive, 0.6, 4))
            .unwrap();
    assert_eq!(
        selector.select_algorithm(&fixture).unwrap(),
        ActivePlanningAlgorithm::DirectionalPca
    );
    assert_eq!(
        selector.select_algorithm(&fixture).unwrap(),
        ActivePlanningAlgorithm::DirectionalPca
    );
    let decision = selector.decision_records().last().unwrap();
    let diagnostics = decision.collapse_diagnostics.as_ref().unwrap();
    assert_eq!(diagnostics.embedding_count, 4);
    assert_eq!(
        decision.reason,
        AdaptivePlanningDecisionReason::StayedOnDirectionalPcaAtOrAboveEmbeddingCountLimit
    );
}

#[test]
fn regression_adaptive_policy_treats_threshold_equality_as_directional_pca() {
    let fixture = weak_pc1_embeddings();
    let mut selector =
        AdaptivePlanningSelector::new(settings(AdaptivePlanningDirection::Divisive, 0.5, 8))
            .unwrap();
    selector.select_algorithm(&fixture).unwrap();
    let algorithm = selector.select_algorithm(&fixture).unwrap();
    assert_eq!(algorithm, ActivePlanningAlgorithm::DirectionalPca);
    assert_eq!(
        selector.decision_records().last().unwrap().reason,
        AdaptivePlanningDecisionReason::StayedOnDirectionalPcaAtOrAbovePc1Threshold
    );
}
