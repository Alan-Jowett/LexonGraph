// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use std::path::Path;

use lexongraph_pca::{
    AffineTransform, CURRENT_SCHEMA_VERSION, DeltaMode, PcaAccumulator, PcaError, PcaTransform,
    QuantizationBits, QuantizationConfig, QuantizedValues, ValidationTolerances, apply_delta_chain,
    apply_in_place, compose, delta_exact, delta_reconstructing, dequantize, fit, fit_truncated,
    rebase_exact, rebase_reconstructing,
};

const TOLERANCE: f32 = 1e-4;

#[test]
fn val_pca_001_streaming_and_fixed_tree_merges_are_deterministic() {
    let vectors = fixture_vectors();
    let mut sequential = PcaAccumulator::new(2);
    for vector in &vectors {
        sequential.update(vector).unwrap();
    }

    let mut left = PcaAccumulator::new(2);
    left.update(&vectors[0]).unwrap();
    left.update(&vectors[1]).unwrap();

    let mut right = PcaAccumulator::new(2);
    right.update(&vectors[2]).unwrap();
    right.update(&vectors[3]).unwrap();

    let mut merged_a = left.clone();
    merged_a.merge(&right).unwrap();
    let mut merged_b = left.clone();
    merged_b.merge(&right).unwrap();

    assert_close_slice_f64(
        &sequential.covariance_f64().unwrap(),
        &merged_a.covariance_f64().unwrap(),
        1e-10,
    );

    let bytes_a = merged_a.finalize().unwrap().serialize().unwrap();
    let bytes_b = merged_b.finalize().unwrap().serialize().unwrap();
    assert_eq!(bytes_a, bytes_b);
}

#[test]
fn val_pca_002_dimension_mismatches_fail_explicitly() {
    let mut accumulator = PcaAccumulator::new(2);
    assert!(matches!(
        accumulator.update(&[1.0]),
        Err(PcaError::DimensionMismatch { .. })
    ));

    let mut other = PcaAccumulator::new(3);
    other.update(&[1.0, 2.0, 3.0]).unwrap();
    assert!(matches!(
        accumulator.merge(&other),
        Err(PcaError::DimensionMismatch { .. })
    ));

    let transform = identity_pca([1.0, 2.0]);
    assert!(matches!(
        transform.apply(&[1.0]),
        Err(PcaError::DimensionMismatch { .. })
    ));
    assert!(matches!(
        transform.reconstruct(&[1.0]),
        Err(PcaError::DimensionMismatch { .. })
    ));
    assert!(matches!(
        transform.truncate(3),
        Err(PcaError::InvalidTruncationDimension { .. })
    ));
}

#[test]
fn val_pca_003_empty_and_single_sample_finalization_fail_explicitly() {
    assert!(matches!(
        PcaAccumulator::new(2).finalize(),
        Err(PcaError::EmptyInput)
    ));

    let mut single = PcaAccumulator::new(2);
    single.update(&[1.0, 2.0]).unwrap();
    assert!(matches!(
        single.finalize(),
        Err(PcaError::InsufficientSamples { sample_count: 1 })
    ));
}

#[test]
fn val_pca_004_repeated_fit_yields_identical_serialized_output() {
    let vectors = fixture_vectors();
    let first = fit(&vectors).unwrap().serialize().unwrap();
    let second = fit(&vectors).unwrap().serialize().unwrap();
    assert_eq!(first, second);
}

#[test]
fn val_pca_005_merge_matches_single_pass_covariance_and_bytes() {
    let vectors = fixture_vectors();
    let sequential = fit(&vectors).unwrap();

    let mut left = PcaAccumulator::new(2);
    let mut right = PcaAccumulator::new(2);
    for vector in &vectors[..2] {
        left.update(vector).unwrap();
    }
    for vector in &vectors[2..] {
        right.update(vector).unwrap();
    }
    left.merge(&right).unwrap();
    let merged = left.finalize().unwrap();

    assert_close_slice_f32(&sequential.mean, &merged.mean, TOLERANCE);
    assert_eq!(sequential.serialize().unwrap(), merged.serialize().unwrap());
}

#[test]
fn val_pca_006_full_rank_reconstruction_matches_input_within_tolerance() {
    let transform = fit(&fixture_vectors()).unwrap();
    for vector in fixture_vectors() {
        let projected = transform.apply(&vector).unwrap();
        let reconstructed = transform.reconstruct(&projected).unwrap();
        assert_close_slice_f32(&vector, &reconstructed, 2e-4);
    }

    let batch_inputs = fixture_vectors();
    let batch_refs = batch_inputs.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let projected = transform.apply_batch(&batch_refs).unwrap();
    let mut in_place = fixture_vectors();
    apply_in_place(&mut in_place, &transform).unwrap();
    assert_eq!(projected, in_place);
}

#[test]
fn val_pca_007_truncation_preserves_the_prefix_structure() {
    let transform = fit(&fixture_vectors()).unwrap();
    let truncated = transform.truncate(1).unwrap();

    assert_eq!(truncated.mean, transform.mean);
    assert_eq!(
        truncated.basis,
        transform.basis[..transform.input_dim].to_vec()
    );
    assert_eq!(
        truncated.explained_variance.unwrap(),
        transform.explained_variance.unwrap()[..1].to_vec()
    );
}

#[test]
fn val_pca_008_eigenpair_ordering_and_sign_canonicalization_are_stable() {
    let vectors = vec![
        vec![1.0f32, 0.0],
        vec![-1.0, 0.0],
        vec![0.0, 1.0],
        vec![0.0, -1.0],
    ];
    let first = fit(&vectors).unwrap();
    let second = fit(&vectors).unwrap();
    assert_eq!(first.serialize().unwrap(), second.serialize().unwrap());

    for column in 0..first.output_dim {
        let basis_column = &first.basis[column * first.input_dim..(column + 1) * first.input_dim];
        let pivot = basis_column
            .iter()
            .enumerate()
            .max_by(|(left_index, left_value), (right_index, right_value)| {
                let magnitude_order = left_value.abs().total_cmp(&right_value.abs());
                if magnitude_order == std::cmp::Ordering::Equal {
                    right_index.cmp(left_index)
                } else {
                    magnitude_order
                }
            })
            .unwrap()
            .0;
        assert!(basis_column[pivot] >= 0.0);
    }
}

#[test]
fn val_pca_009_rank_deficient_inputs_remain_structurally_valid() {
    let vectors = vec![vec![1.0f32, 0.0], vec![2.0, 0.0], vec![3.0, 0.0]];
    let transform = fit(&vectors).unwrap();
    transform.validate().unwrap();
    assert!(transform.explained_variance().unwrap()[1] <= 1e-5);
}

#[test]
fn val_pca_010_composition_preserves_affine_mean_offsets() {
    let first = identity_pca([1.0, 2.0]);
    let second = identity_pca([3.0, 4.0]);
    let composed = compose(&first, &second).unwrap();

    let direct = second.apply(&first.apply(&[10.0, 20.0]).unwrap()).unwrap();
    let composed_result = composed.apply(&[10.0, 20.0]).unwrap();
    assert_eq!(direct, composed_result);
}

#[test]
fn val_pca_011_exact_delta_matches_the_explicit_inverse_then_apply_path() {
    let from = identity_pca([1.0, 2.0]);
    let to = identity_pca([3.0, 4.0]);
    let delta = delta_exact(&from, &to).unwrap();

    assert_eq!(delta.mode, DeltaMode::Exact);
    let exact = to.apply(&from.reconstruct(&[5.0, 6.0]).unwrap()).unwrap();
    assert_eq!(delta.apply(&[5.0, 6.0]).unwrap(), exact);
    assert_eq!(rebase_exact(&[5.0, 6.0], &from, &to).unwrap(), exact);
}

#[test]
fn val_pca_012_reconstructing_delta_is_explicit_for_truncated_sources() {
    let from = identity_pca([1.0, 2.0]).truncate(1).unwrap();
    let to = identity_pca([0.0, 0.0]);
    let delta = delta_reconstructing(&from, &to).unwrap();

    assert_eq!(delta.mode, DeltaMode::Reconstructing);
    let expected = to.apply(&from.reconstruct(&[3.0]).unwrap()).unwrap();
    assert_eq!(delta.apply(&[3.0]).unwrap(), expected);
    assert_eq!(rebase_reconstructing(&[3.0], &from, &to).unwrap(), expected);
    assert_eq!(apply_delta_chain(&[delta], &[3.0]).unwrap(), expected);
}

#[test]
fn val_pca_013_serialization_roundtrip_preserves_pca_and_affine_transforms() {
    let transform = fit(&fixture_vectors()).unwrap();
    let roundtrip = PcaTransform::deserialize(&transform.serialize().unwrap()).unwrap();
    assert_eq!(transform, roundtrip);

    let affine = transform.to_affine();
    let affine_roundtrip = AffineTransform::deserialize(&affine.serialize().unwrap()).unwrap();
    assert_eq!(affine, affine_roundtrip);
}

#[test]
fn val_pca_014_quantization_is_deterministic_and_excludes_negative_128_for_i8() {
    let transform = fit(&fixture_vectors()).unwrap();
    let config = QuantizationConfig {
        bits: QuantizationBits::I8,
    };
    let first = transform.quantize(config).unwrap();
    let second = transform.quantize(config).unwrap();
    assert_eq!(first, second);

    for column in &first.basis_columns {
        match &column.values {
            QuantizedValues::I8(values) => assert!(values.iter().all(|value| *value >= -127)),
            QuantizedValues::I16(_) => panic!("expected i8 basis quantization"),
        }
    }

    let dequantized = dequantize(&first).unwrap();
    dequantized.validate().unwrap();
}

#[test]
fn val_pca_015_validation_rejects_invalid_transforms() {
    let mut nonfinite = identity_pca([0.0, 0.0]);
    nonfinite.mean[0] = f32::NAN;
    assert!(matches!(
        nonfinite.validate(),
        Err(PcaError::NonFiniteInput { .. })
    ));

    let shape_mismatch = PcaTransform {
        basis: vec![1.0, 0.0, 0.0],
        ..identity_pca([0.0, 0.0])
    };
    assert!(matches!(
        shape_mismatch.validate(),
        Err(PcaError::DimensionMismatch { .. })
    ));

    let invalid_variance = PcaTransform {
        explained_variance: Some(vec![1.0, 2.0]),
        ..identity_pca([0.0, 0.0])
    };
    assert!(matches!(
        invalid_variance.validate(),
        Err(PcaError::ValidationFailure(_))
    ));

    let non_orthonormal = PcaTransform {
        basis: vec![1.0, 0.0, 1.0, 0.0],
        ..identity_pca([0.0, 0.0])
    };
    assert!(matches!(
        non_orthonormal.validate_with_tolerances(&ValidationTolerances::default()),
        Err(PcaError::ValidationFailure(_))
    ));
}

#[test]
fn val_pca_016_diagnostics_expose_consistent_metadata() {
    let transform = fit_truncated(&fixture_vectors(), 1).unwrap();
    let diagnostics = transform.diagnostics();

    assert_eq!(diagnostics.input_dim, 2);
    assert_eq!(diagnostics.output_dim, 1);
    assert!(diagnostics.is_truncated);
    assert_eq!(diagnostics.rank_estimate, 1);
    assert!(!diagnostics.contains_nan);
    assert!(!diagnostics.contains_inf);
    assert!(diagnostics.cumulative_variance.unwrap()[0] >= 0.0);
}

#[test]
fn val_pca_017_repository_contains_the_crate_spec_package_and_tests() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
    assert!(repo_root.join("crates").join("lexongraph-pca").is_dir());
    assert!(
        repo_root
            .join("docs")
            .join("specs")
            .join("rust-pca-crate")
            .join("requirements.md")
            .is_file()
    );
    assert!(
        repo_root
            .join("docs")
            .join("specs")
            .join("rust-pca-crate")
            .join("design.md")
            .is_file()
    );
    assert!(
        repo_root
            .join("docs")
            .join("specs")
            .join("rust-pca-crate")
            .join("validation.md")
            .is_file()
    );

    let workspace_toml = std::fs::read_to_string(repo_root.join("Cargo.toml")).unwrap();
    assert!(workspace_toml.contains("\"crates/lexongraph-pca\""));

    let transform = identity_pca([0.0, 0.0]);
    assert_eq!(transform.schema_version, CURRENT_SCHEMA_VERSION);
}

fn identity_pca(mean: [f32; 2]) -> PcaTransform {
    PcaTransform {
        input_dim: 2,
        output_dim: 2,
        mean: mean.to_vec(),
        basis: vec![1.0, 0.0, 0.0, 1.0],
        explained_variance: Some(vec![2.0, 1.0]),
        schema_version: CURRENT_SCHEMA_VERSION,
    }
}

fn fixture_vectors() -> Vec<Vec<f32>> {
    vec![
        vec![2.0, 0.0],
        vec![0.0, 2.0],
        vec![-2.0, 0.0],
        vec![0.0, -2.0],
    ]
}

fn assert_close_slice_f32(left: &[f32], right: &[f32], tolerance: f32) {
    assert_eq!(left.len(), right.len());
    for (index, (left_value, right_value)) in left.iter().zip(right).enumerate() {
        assert!(
            (left_value - right_value).abs() <= tolerance,
            "index {index}: left={left_value} right={right_value}"
        );
    }
}

fn assert_close_slice_f64(left: &[f64], right: &[f64], tolerance: f64) {
    assert_eq!(left.len(), right.len());
    for (index, (left_value, right_value)) in left.iter().zip(right).enumerate() {
        assert!(
            (left_value - right_value).abs() <= tolerance,
            "index {index}: left={left_value} right={right_value}"
        );
    }
}
