// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Executable verification for docs/specs/rust-adaptive-planning-policy-crate/validation.md

use lexongraph_adaptive_planning_policy::{
    ActivePlanningAlgorithm, AdaptiveDcbcSettings, AdaptiveDirectionalPcaSettings,
    AdaptivePlanningDecisionReason, AdaptivePlanningDirection, AdaptivePlanningError,
    AdaptivePlanningSelector, AdaptivePlanningSettings, AdaptiveSwitchCriteria,
    AdaptiveSwitchTieBreak,
};
use lexongraph_directional_pca::DirectionalPcaParams;

fn directional_pca_settings(min_cumulative_variance: f32) -> AdaptiveDirectionalPcaSettings {
    AdaptiveDirectionalPcaSettings {
        cluster_count: 2,
        random_seed: Some(7),
        params: DirectionalPcaParams {
            retained_dimension_count: 1,
            variance_exponent: 1.0,
            temperature: 1.0,
            min_input_count: 2,
            min_effective_rank: 1,
            min_cumulative_variance,
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
    min_cumulative_variance: f32,
) -> AdaptivePlanningSettings {
    AdaptivePlanningSettings {
        direction,
        directional_pca: directional_pca_settings(min_cumulative_variance),
        dcbc: dcbc_settings(),
        switch_criteria: AdaptiveSwitchCriteria {
            min_effective_rank: 1,
            min_cumulative_variance,
            tie_break: AdaptiveSwitchTieBreak::PreferDirectionalPca,
        },
    }
}

fn line_embeddings() -> Vec<Vec<f32>> {
    vec![
        vec![-3.0, 0.0],
        vec![-1.0, 0.0],
        vec![1.0, 0.0],
        vec![3.0, 0.0],
    ]
}

fn square_embeddings() -> Vec<Vec<f32>> {
    vec![
        vec![-1.0, -1.0],
        vec![-1.0, 1.0],
        vec![1.0, -1.0],
        vec![1.0, 1.0],
    ]
}

#[test]
fn val_adaptive_policy_003_rejects_invalid_switch_configuration() {
    let err = AdaptivePlanningSelector::new(AdaptivePlanningSettings {
        direction: AdaptivePlanningDirection::Divisive,
        directional_pca: directional_pca_settings(0.5),
        dcbc: dcbc_settings(),
        switch_criteria: AdaptiveSwitchCriteria {
            min_effective_rank: 1,
            min_cumulative_variance: f32::NAN,
            tie_break: AdaptiveSwitchTieBreak::PreferDirectionalPca,
        },
    })
    .unwrap_err();
    assert!(matches!(
        err,
        AdaptivePlanningError::InvalidConfiguration(_)
    ));
}

#[test]
fn val_adaptive_policy_007_rejects_invalid_directional_pca_configuration() {
    let mut invalid = settings(AdaptivePlanningDirection::Divisive, 0.5);
    invalid.directional_pca.params.temperature = 0.0;
    let err = AdaptivePlanningSelector::new(invalid).unwrap_err();
    assert!(matches!(
        err,
        AdaptivePlanningError::InvalidConfiguration(_)
    ));
}

#[test]
fn val_adaptive_policy_004_starts_with_directional_pca_when_signal_is_strong() {
    let mut selector =
        AdaptivePlanningSelector::new(settings(AdaptivePlanningDirection::Divisive, 0.8)).unwrap();
    let algorithm = selector
        .select_algorithm(line_embeddings().len(), &line_embeddings())
        .unwrap();
    assert_eq!(algorithm, ActivePlanningAlgorithm::DirectionalPca);
    assert!(!selector.switched_to_dcbc());
    let decision = selector.decision_records().last().unwrap();
    assert_eq!(
        decision.reason,
        AdaptivePlanningDecisionReason::InitialDirectionalPcaSegment
    );
    assert!(!decision.switch_boundary_occurred);
    assert!(decision.collapse_diagnostics.is_none());
}

#[test]
fn val_adaptive_policy_005_supports_both_direction_modes() {
    for direction in [
        AdaptivePlanningDirection::Divisive,
        AdaptivePlanningDirection::Agglomerative,
    ] {
        let selector = AdaptivePlanningSelector::new(settings(direction, 0.8)).unwrap();
        assert_eq!(selector.settings().direction, direction);
    }
}

#[test]
fn val_adaptive_policy_006_records_structured_diagnostics() {
    let mut selector =
        AdaptivePlanningSelector::new(settings(AdaptivePlanningDirection::Divisive, 0.8)).unwrap();
    selector
        .select_algorithm(line_embeddings().len(), &line_embeddings())
        .unwrap();
    selector
        .select_algorithm(line_embeddings().len(), &line_embeddings())
        .unwrap();
    let diagnostics = selector
        .decision_records()
        .last()
        .and_then(|record| record.collapse_diagnostics.as_ref())
        .unwrap();
    assert_eq!(diagnostics.represented_item_count, 4);
    assert!(diagnostics.effective_rank >= 1);
    assert!(diagnostics.retained_cumulative_variance >= 0.8);
}

#[test]
fn val_adaptive_policy_008_switches_to_dcbc_when_pca_signal_collapses() {
    let mut selector =
        AdaptivePlanningSelector::new(settings(AdaptivePlanningDirection::Divisive, 0.8)).unwrap();
    let algorithm = selector
        .select_algorithm(square_embeddings().len(), &square_embeddings())
        .unwrap();
    assert_eq!(algorithm, ActivePlanningAlgorithm::DirectionalPca);
    let algorithm = selector
        .select_algorithm(square_embeddings().len(), &square_embeddings())
        .unwrap();
    assert_eq!(algorithm, ActivePlanningAlgorithm::Dcbc);
    assert!(selector.switched_to_dcbc());
    assert!(
        selector
            .decision_records()
            .last()
            .unwrap()
            .switch_boundary_occurred
    );
}

#[test]
fn val_adaptive_policy_009_does_not_switch_back_after_dcbc_boundary() {
    let mut selector =
        AdaptivePlanningSelector::new(settings(AdaptivePlanningDirection::Agglomerative, 0.8))
            .unwrap();
    selector
        .select_algorithm(square_embeddings().len(), &square_embeddings())
        .unwrap();
    selector
        .select_algorithm(square_embeddings().len(), &square_embeddings())
        .unwrap();
    let second = selector
        .select_algorithm(line_embeddings().len(), &line_embeddings())
        .unwrap();
    assert_eq!(second, ActivePlanningAlgorithm::Dcbc);
    let last = selector.decision_records().last().unwrap();
    assert_eq!(
        last.reason,
        AdaptivePlanningDecisionReason::PreviouslySwitchedToDcbc
    );
    assert!(!last.switch_boundary_occurred);
    assert!(last.collapse_diagnostics.is_none());
}

#[test]
fn val_adaptive_policy_012_repeats_the_same_switch_boundary() {
    let fixture = square_embeddings();
    let mut first =
        AdaptivePlanningSelector::new(settings(AdaptivePlanningDirection::Divisive, 0.8)).unwrap();
    let mut second =
        AdaptivePlanningSelector::new(settings(AdaptivePlanningDirection::Divisive, 0.8)).unwrap();
    first.select_algorithm(fixture.len(), &fixture).unwrap();
    first.select_algorithm(fixture.len(), &fixture).unwrap();
    second.select_algorithm(fixture.len(), &fixture).unwrap();
    second.select_algorithm(fixture.len(), &fixture).unwrap();
    assert_eq!(first.decision_records(), second.decision_records());
}

#[test]
fn val_adaptive_policy_003_tie_break_can_prefer_dcbc_at_exact_threshold() {
    let fixture = line_embeddings();
    let mut selector = AdaptivePlanningSelector::new(AdaptivePlanningSettings {
        direction: AdaptivePlanningDirection::Divisive,
        directional_pca: directional_pca_settings(1.0),
        dcbc: dcbc_settings(),
        switch_criteria: AdaptiveSwitchCriteria {
            min_effective_rank: 1,
            min_cumulative_variance: 1.0,
            tie_break: AdaptiveSwitchTieBreak::PreferDcbc,
        },
    })
    .unwrap();
    assert_eq!(
        selector.select_algorithm(fixture.len(), &fixture).unwrap(),
        ActivePlanningAlgorithm::DirectionalPca
    );
    assert_eq!(
        selector.select_algorithm(fixture.len(), &fixture).unwrap(),
        ActivePlanningAlgorithm::Dcbc
    );
}
