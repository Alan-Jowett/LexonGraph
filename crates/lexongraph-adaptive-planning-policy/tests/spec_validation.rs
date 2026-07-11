// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
//! Executable verification for docs/specs/rust-adaptive-planning-policy-crate/validation.md

use lexongraph_adaptive_planning_policy::{
    ActivePlanningAlgorithm, AdaptiveDcbcSettings, AdaptiveDirectionalPcaSettings,
    AdaptivePlanningDecisionReason, AdaptivePlanningDirection, AdaptivePlanningError,
    AdaptivePlanningSelector, AdaptivePlanningSettings, AdaptiveReplayStage,
    AdaptiveSelectionProgress, AdaptiveSwitchCriteria, DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
};
use lexongraph_directional_pca::{
    DirectionalPcaAllocationPolicy, DirectionalPcaBinningPolicy,
    DirectionalPcaClusterCardinalityMode, DirectionalPcaParams, DirectionalPcaRetainedAxisPolicy,
};

fn directional_pca_settings() -> AdaptiveDirectionalPcaSettings {
    AdaptiveDirectionalPcaSettings {
        cluster_count: 2,
        random_seed: Some(7),
        params: DirectionalPcaParams {
            retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(1),
            allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
            binning_policy: DirectionalPcaBinningPolicy::Quantile,
            cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::Exact,
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
    mean_cluster_radius_threshold: f32,
) -> AdaptivePlanningSettings {
    AdaptivePlanningSettings {
        direction,
        directional_pca: directional_pca_settings(),
        dcbc: dcbc_settings(),
        switch_criteria: AdaptiveSwitchCriteria {
            mean_cluster_radius_threshold,
        },
    }
}

fn compact_cluster_embeddings() -> Vec<Vec<f32>> {
    vec![
        vec![-0.1, 0.0],
        vec![0.1, 0.0],
        vec![1.9, 0.0],
        vec![2.1, 0.0],
    ]
}

fn diffuse_cluster_embeddings() -> Vec<Vec<f32>> {
    vec![
        vec![-0.4, 0.0],
        vec![0.4, 0.0],
        vec![1.6, 0.0],
        vec![2.4, 0.0],
    ]
}

fn select_algorithm(
    selector: &mut AdaptivePlanningSelector,
    represented_item_count: usize,
    embeddings: &[Vec<f32>],
) -> ActivePlanningAlgorithm {
    let mut progress = selector
        .begin_selection_boundary(
            represented_item_count,
            embeddings.len(),
            embeddings.first().map_or(0, std::vec::Vec::len),
        )
        .unwrap();
    loop {
        match progress {
            AdaptiveSelectionProgress::Selected(algorithm) => return algorithm,
            AdaptiveSelectionProgress::ReplayRequired(AdaptiveReplayStage::CollectingSummaries)
            | AdaptiveSelectionProgress::ReplayRequired(AdaptiveReplayStage::MeasuringDecision) => {
                selector.ingest_selection_batch(embeddings).unwrap();
                progress = selector.finish_selection_pass().unwrap();
            }
        }
    }
}

#[test]
fn val_adaptive_policy_011_rejects_invalid_switch_configuration() {
    let err = AdaptivePlanningSelector::new(AdaptivePlanningSettings {
        direction: AdaptivePlanningDirection::Divisive,
        directional_pca: directional_pca_settings(),
        dcbc: dcbc_settings(),
        switch_criteria: AdaptiveSwitchCriteria {
            mean_cluster_radius_threshold: f32::NAN,
        },
    })
    .unwrap_err();
    assert!(matches!(
        err,
        AdaptivePlanningError::InvalidConfiguration(_)
    ));
}

#[test]
fn val_adaptive_policy_011_rejects_invalid_directional_pca_configuration() {
    let mut invalid = settings(
        AdaptivePlanningDirection::Divisive,
        DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
    );
    invalid.directional_pca.params.temperature = 0.0;
    let err = AdaptivePlanningSelector::new(invalid).unwrap_err();
    assert!(matches!(
        err,
        AdaptivePlanningError::InvalidConfiguration(_)
    ));
}

#[test]
fn val_adaptive_policy_rejects_non_power_of_two_eigenvalue_bit_configuration() {
    let mut invalid = settings(
        AdaptivePlanningDirection::Divisive,
        DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
    );
    invalid.directional_pca.cluster_count = 3;
    invalid.directional_pca.params.retained_axis_policy =
        DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible;
    invalid.directional_pca.params.allocation_policy =
        DirectionalPcaAllocationPolicy::EigenvalueLogBits;
    invalid.directional_pca.params.binning_policy = DirectionalPcaBinningPolicy::DensityValley;
    let err = AdaptivePlanningSelector::new(invalid).unwrap_err();
    assert!(matches!(
        err,
        AdaptivePlanningError::InvalidConfiguration(_)
    ));
}

#[test]
fn val_adaptive_policy_rejects_centroid_weighted_adaptive_rank_above_cluster_count() {
    let mut invalid = settings(
        AdaptivePlanningDirection::Divisive,
        DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
    );
    invalid.directional_pca.params.retained_axis_policy =
        DirectionalPcaRetainedAxisPolicy::AdaptiveAllEligible;
    invalid.directional_pca.params.allocation_policy =
        DirectionalPcaAllocationPolicy::CentroidWeightedBins;
    invalid.directional_pca.params.min_effective_rank = 3;
    let err = AdaptivePlanningSelector::new(invalid).unwrap_err();
    assert!(matches!(
        err,
        AdaptivePlanningError::InvalidConfiguration(_)
    ));
}

#[test]
fn regression_adaptive_policy_caps_diagnostic_cluster_count_to_available_embeddings() {
    let mut selector = AdaptivePlanningSelector::new(AdaptivePlanningSettings {
        direction: AdaptivePlanningDirection::Divisive,
        directional_pca: AdaptiveDirectionalPcaSettings {
            cluster_count: 8,
            random_seed: Some(7),
            params: DirectionalPcaParams {
                retained_axis_policy: DirectionalPcaRetainedAxisPolicy::FixedCount(1),
                allocation_policy: DirectionalPcaAllocationPolicy::CentroidWeightedBins,
                binning_policy: DirectionalPcaBinningPolicy::Quantile,
                cluster_cardinality_mode: DirectionalPcaClusterCardinalityMode::Exact,
                variance_exponent: 1.0,
                temperature: 1.0,
                min_input_count: 2,
                min_effective_rank: 1,
                min_cumulative_variance: 0.0,
            },
        },
        dcbc: dcbc_settings(),
        switch_criteria: AdaptiveSwitchCriteria {
            mean_cluster_radius_threshold: DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
        },
    })
    .unwrap();
    let fixture = diffuse_cluster_embeddings();
    assert_eq!(
        select_algorithm(&mut selector, fixture.len(), &fixture),
        ActivePlanningAlgorithm::DirectionalPca
    );
    let algorithm = select_algorithm(&mut selector, fixture.len(), &fixture);
    assert!(matches!(
        algorithm,
        ActivePlanningAlgorithm::DirectionalPca | ActivePlanningAlgorithm::Dcbc
    ));
    assert!(
        selector
            .decision_records()
            .last()
            .and_then(|record| record.collapse_diagnostics.as_ref())
            .is_some()
    );
}

#[test]
fn val_adaptive_policy_004_starts_with_directional_pca_when_signal_is_strong() {
    let mut selector = AdaptivePlanningSelector::new(settings(
        AdaptivePlanningDirection::Divisive,
        DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
    ))
    .unwrap();
    let algorithm = selector
        .begin_selection_boundary(
            compact_cluster_embeddings().len(),
            compact_cluster_embeddings().len(),
            compact_cluster_embeddings()[0].len(),
        )
        .unwrap();
    let algorithm = match algorithm {
        AdaptiveSelectionProgress::Selected(algorithm) => algorithm,
        AdaptiveSelectionProgress::ReplayRequired(_) => {
            panic!("initial segment should select immediately")
        }
    };
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
        let selector = AdaptivePlanningSelector::new(settings(
            direction,
            DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
        ))
        .unwrap();
        assert_eq!(selector.settings().direction, direction);
    }
}

#[test]
fn val_adaptive_policy_006_records_structured_diagnostics() {
    let fixture = compact_cluster_embeddings();
    let mut selector = AdaptivePlanningSelector::new(settings(
        AdaptivePlanningDirection::Divisive,
        DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
    ))
    .unwrap();
    select_algorithm(&mut selector, fixture.len(), &fixture);
    select_algorithm(&mut selector, fixture.len(), &fixture);
    let diagnostics = selector
        .decision_records()
        .last()
        .and_then(|record| record.collapse_diagnostics.as_ref())
        .unwrap();
    assert_eq!(diagnostics.represented_item_count, 4);
    assert!((diagnostics.mean_cluster_radius - 0.1).abs() < 1e-5);
}

#[test]
fn val_adaptive_policy_007_stays_on_directional_pca_when_mean_cluster_radius_is_at_or_below_threshold()
 {
    let fixture = compact_cluster_embeddings();
    let mut selector = AdaptivePlanningSelector::new(settings(
        AdaptivePlanningDirection::Divisive,
        DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
    ))
    .unwrap();
    let algorithm = select_algorithm(&mut selector, fixture.len(), &fixture);
    assert_eq!(algorithm, ActivePlanningAlgorithm::DirectionalPca);
    let algorithm = select_algorithm(&mut selector, fixture.len(), &fixture);
    assert_eq!(algorithm, ActivePlanningAlgorithm::DirectionalPca);
    assert!(!selector.switched_to_dcbc());
}

#[test]
fn val_adaptive_policy_008_switches_to_dcbc_when_mean_cluster_radius_exceeds_threshold() {
    let fixture = diffuse_cluster_embeddings();
    let mut selector = AdaptivePlanningSelector::new(settings(
        AdaptivePlanningDirection::Divisive,
        DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
    ))
    .unwrap();
    let algorithm = select_algorithm(&mut selector, fixture.len(), &fixture);
    assert_eq!(algorithm, ActivePlanningAlgorithm::DirectionalPca);
    let algorithm = select_algorithm(&mut selector, fixture.len(), &fixture);
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
    let diffuse = diffuse_cluster_embeddings();
    let compact = compact_cluster_embeddings();
    let mut selector = AdaptivePlanningSelector::new(settings(
        AdaptivePlanningDirection::Agglomerative,
        DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
    ))
    .unwrap();
    select_algorithm(&mut selector, diffuse.len(), &diffuse);
    select_algorithm(&mut selector, diffuse.len(), &diffuse);
    let second = select_algorithm(&mut selector, compact.len(), &compact);
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
    let fixture = diffuse_cluster_embeddings();
    let mut first = AdaptivePlanningSelector::new(settings(
        AdaptivePlanningDirection::Divisive,
        DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
    ))
    .unwrap();
    let mut second = AdaptivePlanningSelector::new(settings(
        AdaptivePlanningDirection::Divisive,
        DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
    ))
    .unwrap();
    select_algorithm(&mut first, fixture.len(), &fixture);
    select_algorithm(&mut first, fixture.len(), &fixture);
    select_algorithm(&mut second, fixture.len(), &fixture);
    select_algorithm(&mut second, fixture.len(), &fixture);
    assert_eq!(first.decision_records(), second.decision_records());
}

#[test]
fn regression_adaptive_policy_rejects_partial_measurement_replays() {
    let fixture = diffuse_cluster_embeddings();
    let mut selector = AdaptivePlanningSelector::new(settings(
        AdaptivePlanningDirection::Divisive,
        DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
    ))
    .unwrap();

    assert!(matches!(
        select_algorithm(&mut selector, fixture.len(), &fixture),
        ActivePlanningAlgorithm::DirectionalPca
    ));

    let mut progress = selector
        .begin_selection_boundary(fixture.len(), fixture.len(), fixture[0].len())
        .unwrap();
    loop {
        match progress {
            AdaptiveSelectionProgress::Selected(_) => {
                panic!("partial replay should not produce a selection")
            }
            AdaptiveSelectionProgress::ReplayRequired(AdaptiveReplayStage::CollectingSummaries) => {
                selector.ingest_selection_batch(&fixture).unwrap();
                progress = selector.finish_selection_pass().unwrap();
            }
            AdaptiveSelectionProgress::ReplayRequired(AdaptiveReplayStage::MeasuringDecision) => {
                selector
                    .ingest_selection_batch(&fixture[..fixture.len() - 1])
                    .unwrap();
                let error = selector.finish_selection_pass().unwrap_err();
                let message = error.to_string();
                assert!(
                    message.contains("expected 4 observed embeddings but received 3"),
                    "unexpected error: {message}"
                );
                break;
            }
        }
    }
}

#[test]
fn regression_adaptive_policy_requires_threshold_exceedance_instead_of_switching_at_equality() {
    let fixture = compact_cluster_embeddings();
    let mut selector = AdaptivePlanningSelector::new(settings(
        AdaptivePlanningDirection::Divisive,
        DEFAULT_MEAN_CLUSTER_RADIUS_THRESHOLD,
    ))
    .unwrap();
    select_algorithm(&mut selector, fixture.len(), &fixture);
    select_algorithm(&mut selector, fixture.len(), &fixture);
    let measured_radius = selector
        .decision_records()
        .last()
        .and_then(|record| record.collapse_diagnostics.as_ref())
        .map(|diagnostics| diagnostics.mean_cluster_radius)
        .unwrap();
    let mut selector = AdaptivePlanningSelector::new(AdaptivePlanningSettings {
        direction: AdaptivePlanningDirection::Divisive,
        directional_pca: directional_pca_settings(),
        dcbc: dcbc_settings(),
        switch_criteria: AdaptiveSwitchCriteria {
            mean_cluster_radius_threshold: measured_radius,
        },
    })
    .unwrap();
    assert_eq!(
        select_algorithm(&mut selector, fixture.len(), &fixture),
        ActivePlanningAlgorithm::DirectionalPca
    );
    assert_eq!(
        select_algorithm(&mut selector, fixture.len(), &fixture),
        ActivePlanningAlgorithm::DirectionalPca
    );
}
