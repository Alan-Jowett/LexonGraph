// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Executable verification for docs/specs/rust-adaptive-planning-policy-crate/validation.md

use lexongraph_adaptive_planning_policy::{
    ActivePlanningAlgorithm, AdaptiveDcbcSettings, AdaptiveDirectionalPcaSettings,
    AdaptivePlanningDecisionReason, AdaptivePlanningDirection, AdaptivePlanningError,
    AdaptivePlanningSelector, AdaptivePlanningSettings, DEFAULT_EMBEDDING_COUNT_CUTOFF,
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

fn settings(direction: AdaptivePlanningDirection) -> AdaptivePlanningSettings {
    AdaptivePlanningSettings {
        direction,
        directional_pca: directional_pca_settings(),
        dcbc: dcbc_settings(),
    }
}

fn embeddings(count: usize) -> Vec<Vec<f32>> {
    (0..count)
        .map(|index| vec![index as f32, (index % 17) as f32])
        .collect()
}

#[test]
fn val_adaptive_policy_011_rejects_invalid_directional_pca_configuration() {
    let mut invalid = settings(AdaptivePlanningDirection::Divisive);
    invalid.directional_pca.params.temperature = 0.0;
    let err = AdaptivePlanningSelector::new(invalid).unwrap_err();
    assert!(matches!(
        err,
        AdaptivePlanningError::InvalidConfiguration(_)
    ));
}

#[test]
fn val_adaptive_policy_004_starts_with_directional_pca_when_signal_is_strong() {
    let fixture = embeddings(DEFAULT_EMBEDDING_COUNT_CUTOFF);
    let mut selector =
        AdaptivePlanningSelector::new(settings(AdaptivePlanningDirection::Divisive)).unwrap();
    let algorithm = selector.select_algorithm(&fixture).unwrap();
    assert_eq!(algorithm, ActivePlanningAlgorithm::DirectionalPca);
    assert!(!selector.switched_to_dcbc());
    let decision = selector.decision_records().last().unwrap();
    assert_eq!(
        decision.reason,
        AdaptivePlanningDecisionReason::InitialDirectionalPcaSegment
    );
    assert!(!decision.switch_boundary_occurred);
    assert_eq!(decision.embedding_count_cutoff, None);
    assert!(decision.collapse_diagnostics.is_none());
}

#[test]
fn val_adaptive_policy_005_supports_both_direction_modes() {
    for direction in [
        AdaptivePlanningDirection::Divisive,
        AdaptivePlanningDirection::Agglomerative,
    ] {
        let selector = AdaptivePlanningSelector::new(settings(direction)).unwrap();
        assert_eq!(selector.settings().direction, direction);
    }
}

#[test]
fn val_adaptive_policy_006_records_structured_diagnostics() {
    let fixture = embeddings(DEFAULT_EMBEDDING_COUNT_CUTOFF);
    let mut selector =
        AdaptivePlanningSelector::new(settings(AdaptivePlanningDirection::Divisive)).unwrap();
    selector.select_algorithm(&fixture).unwrap();
    selector.select_algorithm(&fixture).unwrap();
    let decision = selector.decision_records().last().unwrap();
    let diagnostics = decision.collapse_diagnostics.as_ref().unwrap();
    assert_eq!(diagnostics.embedding_count, DEFAULT_EMBEDDING_COUNT_CUTOFF);
    assert_eq!(
        decision.embedding_count_cutoff,
        Some(DEFAULT_EMBEDDING_COUNT_CUTOFF)
    );
    assert_eq!(
        decision.reason,
        AdaptivePlanningDecisionReason::StayedOnDirectionalPcaAtOrAboveEmbeddingCountCutoff
    );
}

#[test]
fn val_adaptive_policy_007_stays_on_directional_pca_when_embedding_count_is_at_or_above_cutoff() {
    let fixture = embeddings(DEFAULT_EMBEDDING_COUNT_CUTOFF);
    let mut selector =
        AdaptivePlanningSelector::new(settings(AdaptivePlanningDirection::Divisive)).unwrap();
    assert_eq!(
        selector.select_algorithm(&fixture).unwrap(),
        ActivePlanningAlgorithm::DirectionalPca
    );
    assert_eq!(
        selector.select_algorithm(&fixture).unwrap(),
        ActivePlanningAlgorithm::DirectionalPca
    );
    assert!(!selector.switched_to_dcbc());
}

#[test]
fn val_adaptive_policy_008_switches_to_dcbc_when_embedding_count_drops_below_cutoff() {
    let fixture = embeddings(DEFAULT_EMBEDDING_COUNT_CUTOFF - 1);
    let mut selector =
        AdaptivePlanningSelector::new(settings(AdaptivePlanningDirection::Divisive)).unwrap();
    assert_eq!(
        selector.select_algorithm(&fixture).unwrap(),
        ActivePlanningAlgorithm::DirectionalPca
    );
    assert_eq!(
        selector.select_algorithm(&fixture).unwrap(),
        ActivePlanningAlgorithm::Dcbc
    );
    assert!(selector.switched_to_dcbc());
    let decision = selector.decision_records().last().unwrap();
    assert!(decision.switch_boundary_occurred);
    assert_eq!(
        decision.embedding_count_cutoff,
        Some(DEFAULT_EMBEDDING_COUNT_CUTOFF)
    );
    assert_eq!(
        decision.reason,
        AdaptivePlanningDecisionReason::SwitchedToDcbcBelowEmbeddingCountCutoff
    );
}

#[test]
fn val_adaptive_policy_009_does_not_switch_back_after_dcbc_boundary() {
    let switch_fixture = embeddings(DEFAULT_EMBEDDING_COUNT_CUTOFF - 1);
    let stay_fixture = embeddings(DEFAULT_EMBEDDING_COUNT_CUTOFF);
    let mut selector =
        AdaptivePlanningSelector::new(settings(AdaptivePlanningDirection::Agglomerative)).unwrap();
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
    assert_eq!(last.embedding_count_cutoff, None);
    assert!(last.collapse_diagnostics.is_none());
}

#[test]
fn val_adaptive_policy_012_repeats_the_same_switch_boundary() {
    let fixture = embeddings(DEFAULT_EMBEDDING_COUNT_CUTOFF - 1);
    let mut first =
        AdaptivePlanningSelector::new(settings(AdaptivePlanningDirection::Divisive)).unwrap();
    let mut second =
        AdaptivePlanningSelector::new(settings(AdaptivePlanningDirection::Divisive)).unwrap();
    first.select_algorithm(&fixture).unwrap();
    first.select_algorithm(&fixture).unwrap();
    second.select_algorithm(&fixture).unwrap();
    second.select_algorithm(&fixture).unwrap();
    assert_eq!(first.decision_records(), second.decision_records());
}

#[test]
fn regression_adaptive_policy_switches_only_below_cutoff() {
    let fixture = embeddings(DEFAULT_EMBEDDING_COUNT_CUTOFF);
    let mut selector =
        AdaptivePlanningSelector::new(settings(AdaptivePlanningDirection::Divisive)).unwrap();
    assert_eq!(
        selector.select_algorithm(&fixture).unwrap(),
        ActivePlanningAlgorithm::DirectionalPca
    );
    assert_eq!(
        selector.select_algorithm(&fixture).unwrap(),
        ActivePlanningAlgorithm::DirectionalPca
    );
    let diagnostics = selector
        .decision_records()
        .last()
        .and_then(|record| record.collapse_diagnostics.as_ref())
        .unwrap();
    assert_eq!(diagnostics.embedding_count, DEFAULT_EMBEDDING_COUNT_CUTOFF);
}
