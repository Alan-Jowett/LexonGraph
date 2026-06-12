// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

mod support;

use ciborium::value::Value as CborValue;
use std::fs;
use std::path::Path;
use std::process::Command as ProcessCommand;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use lexongraph_block::{
    Block, BranchEntry, Content, EmbeddingSpec, LeafEntry, VERSION_1, build_branch_block,
    build_leaf_block,
};
use lexongraph_block_store::BlockStore;
use lexongraph_block_store_fs::FilesystemBlockStore;
use lexongraph_block_store_zip::ZipBlockStore;
use lexongraph_streaming_clustering::{
    MetricDirection, PassReport, StreamingClusterClassifier, StreamingClusterTrainer,
    StreamingClusteringConfig, StreamingClusteringError, TrainerState, validate_embedding,
};
use lexongraph_streaming_clustering_evaluator::{
    AlignmentPolicy, BenchmarkProfile, BlockStoreCorpusReference, BlockStoreReferenceStore,
    CampaignReport, CandidateIdentity, CandidateRunStatus, CompressionBenchmark, CompressionMethod,
    DeferredMeasurementStatus, EmbeddingWorkloadSource, EvaluationEntitySource, EvaluatorError,
    FsOverlayZipBlockStore, GateStatus, Section4CorpusFamily,
    Section4HarvestEmbeddingAdmissibility, Section4HarvestPolicy, Section4HarvestSubsetSelection,
    Section4MetricContract, Section4ProfileSourceSpec, Section4ProfileSpec, Section4SuiteManifest,
    Section4SuiteSpec, SharedBalanceConstraints, StructuredFailure, TrainingPassSource,
    built_in_fixture_candidate_names, candidate_adapter, emit_campaign_artifacts,
    generate_section4_suite_assets, registered_candidate_names, resolve_registered_candidates,
    run_evaluation_campaign, run_section4_suite, write_section4_suite_artifacts,
};
use support::{
    archive_backed_profile, balanced_and_skewed_candidates, block_store_backed_profile,
    broken_archive_backed_profile, broken_block_store_profile, corrupt_archive_backed_profile,
    duplicate_evaluation_entities_block_store_profile, duplicate_source_id_profile,
    empty_synthetic_metadata_key_profile, invalid_profile, lib_source,
    missing_synthetic_metadata_key_profile, nondeterministic_candidate,
    shared_contract_failure_candidate, strict_alignment_profile, synthetic_padding_profile,
    wrong_entity_count_block_store_profile,
};
use tempfile::tempdir;

#[derive(Clone, Copy)]
enum InvalidRangeMode {
    Probe,
    LeafMembership,
}

struct InvalidRangeTrainer {
    config: StreamingClusteringConfig,
    state: TrainerState,
    mode: InvalidRangeMode,
}

impl InvalidRangeTrainer {
    fn new(config: &StreamingClusteringConfig, mode: InvalidRangeMode) -> Self {
        Self {
            config: config.clone(),
            state: TrainerState::Idle,
            mode,
        }
    }
}

impl StreamingClusterTrainer for InvalidRangeTrainer {
    type Classifier = InvalidRangeClassifier;

    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn state(&self) -> TrainerState {
        self.state
    }

    fn ingest_batch(&mut self, embeddings: &[Vec<f32>]) -> Result<(), StreamingClusteringError> {
        for embedding in embeddings {
            validate_embedding(embedding, self.config.dimensions)?;
        }
        self.state = TrainerState::Ingesting;
        Ok(())
    }

    fn finish_pass(&mut self) -> Result<PassReport, StreamingClusteringError> {
        if self.state != TrainerState::Ingesting {
            let invalid_state = self.state;
            self.state = TrainerState::Error;
            return Err(StreamingClusteringError::InvalidTransition {
                state: invalid_state,
                operation: "finish_pass".into(),
            });
        }
        self.state = TrainerState::PassComplete;
        Ok(PassReport {
            observed_count: 4,
            quality_metric: 0.0,
            balance_metric: 0.0,
            quality_direction: MetricDirection::SmallerIsBetter,
            balance_direction: MetricDirection::SmallerIsBetter,
            cluster_ids: vec![0, 1],
        })
    }

    fn complete_training(&mut self) -> Result<(), StreamingClusteringError> {
        if self.state != TrainerState::PassComplete {
            let invalid_state = self.state;
            self.state = TrainerState::Error;
            return Err(StreamingClusteringError::InvalidTransition {
                state: invalid_state,
                operation: "complete_training".into(),
            });
        }
        self.state = TrainerState::TrainingComplete;
        Ok(())
    }

    fn into_classifier(self) -> Result<Self::Classifier, StreamingClusteringError> {
        if self.state != TrainerState::TrainingComplete {
            return Err(StreamingClusteringError::InvalidTransition {
                state: self.state,
                operation: "into_classifier".into(),
            });
        }
        Ok(InvalidRangeClassifier {
            config: self.config,
            mode: self.mode,
        })
    }
}

struct InvalidRangeClassifier {
    config: StreamingClusteringConfig,
    mode: InvalidRangeMode,
}

impl StreamingClusterClassifier for InvalidRangeClassifier {
    fn config(&self) -> &StreamingClusteringConfig {
        &self.config
    }

    fn assign(&self, embedding: &[f32]) -> Result<u32, StreamingClusteringError> {
        validate_embedding(embedding, self.config.dimensions)?;
        let first = embedding[0];
        match self.mode {
            InvalidRangeMode::Probe
                if (0.1..0.2).contains(&first) || (10.0..10.1).contains(&first) =>
            {
                Ok(self.config.cluster_count)
            }
            InvalidRangeMode::LeafMembership if first < 0.05 => Ok(self.config.cluster_count),
            _ => Ok(if first < 5.0 { 0 } else { 1 }),
        }
    }
}

fn section4_reproducibility() -> lexongraph_streaming_clustering_evaluator::ReproducibilityMetadata
{
    lexongraph_streaming_clustering_evaluator::ReproducibilityMetadata {
        seed_policy: "fixed-seed-11".into(),
        software_identity: "section4-test-harness".into(),
        floating_point_profile: "ieee754-deterministic-no-fma".into(),
        hardware_profile: "fixture-cpu".into(),
    }
}

fn section4_suite_spec(profiles: Vec<Section4ProfileSpec>) -> Section4SuiteSpec {
    Section4SuiteSpec {
        suite_id: "section4-corpus-panel-suite".into(),
        leaf_size: 2,
        dimensions: 2,
        batch_size: 2,
        metric_contract: Section4MetricContract::Euclidean,
        neighbor_count: 10,
        balance_constraints: None,
        random_seed: Some(11),
        compression_benchmark: CompressionBenchmark {
            method: CompressionMethod::ScalarQuantization8Bit,
            global_baseline_label: "global-real-dataset-8bit".into(),
        },
        reproducibility: section4_reproducibility(),
        profiles,
    }
}

fn harvested_policy() -> Section4HarvestPolicy {
    Section4HarvestPolicy {
        embedding_admissibility:
            Section4HarvestEmbeddingAdmissibility::FiniteF32MatchingSuiteDimensions,
        subset_selection: Section4HarvestSubsetSelection::SortByEntityIdTakeFirst,
    }
}

fn strict_synthetic_profile(
    profile_id: &str,
    corpus_id: &str,
    real_entity_count: usize,
) -> Section4ProfileSpec {
    Section4ProfileSpec {
        profile_id: profile_id.into(),
        corpus_id: corpus_id.into(),
        scale_tier_id: format!("n-{real_entity_count}"),
        source: Section4ProfileSourceSpec::Synthetic {
            family: Section4CorpusFamily::WellClusteredSynthetic,
            real_entity_count,
            alignment_policy: AlignmentPolicy::StrictAlignment,
        },
    }
}

fn padding_synthetic_profile(
    profile_id: &str,
    corpus_id: &str,
    real_entity_count: usize,
) -> Section4ProfileSpec {
    Section4ProfileSpec {
        profile_id: profile_id.into(),
        corpus_id: corpus_id.into(),
        scale_tier_id: format!("n-{real_entity_count}"),
        source: Section4ProfileSourceSpec::Synthetic {
            family: Section4CorpusFamily::NearDuplicateHeavy,
            real_entity_count,
            alignment_policy: AlignmentPolicy::DeterministicSyntheticPadding,
        },
    }
}

fn non_zero_strict_alignment_profile() -> BenchmarkProfile {
    let mut profile = strict_alignment_profile();
    for pass in &mut profile.training_passes {
        if let TrainingPassSource::Inline { batches } = pass {
            for batch in batches {
                for embedding in batch {
                    if embedding.iter().all(|value| *value == 0.0) {
                        embedding[0] = 0.1;
                    }
                }
            }
        }
    }
    for workload in &mut profile.probe_workloads {
        if let EmbeddingWorkloadSource::Inline { embeddings } = &mut workload.source {
            for embedding in embeddings {
                if embedding.iter().all(|value| *value == 0.0) {
                    embedding[0] = 0.1;
                }
            }
        }
    }
    if let EvaluationEntitySource::Inline { entities } = &mut profile.evaluation_entities {
        for entity in entities {
            if entity.embedding.iter().all(|value| *value == 0.0) {
                entity.embedding[0] = 0.1;
            }
        }
    }
    profile
}

fn harvested_archive_reference()
-> lexongraph_streaming_clustering_evaluator::BlockStoreCorpusReference {
    let suite_dir = checked_in_section4_suite_dir();
    let suite_spec_path = suite_dir.join("section4-suite-spec.json");
    let suite_spec: Section4SuiteSpec =
        serde_json::from_str(&fs::read_to_string(suite_spec_path).unwrap()).unwrap();
    let mut reference = suite_spec
        .profiles
        .into_iter()
        .find_map(|profile| match profile.source {
            Section4ProfileSourceSpec::Harvested { source, .. } => Some(source),
            Section4ProfileSourceSpec::Synthetic { .. } => None,
        })
        .expect("checked-in section-4 suite should expose a harvested source");
    if let BlockStoreReferenceStore::ZipArchive { archive_path } = &mut reference.store
        && archive_path.is_relative()
    {
        *archive_path = suite_dir.join(&*archive_path);
    }
    reference
}

fn checked_in_section4_suite_dir() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("section4")
        .join("corpus-panel-suite")
}

fn checked_in_section4_suite_manifest() -> Section4SuiteManifest {
    let suite_dir = checked_in_section4_suite_dir();
    let manifest_path = suite_dir.join("section4-suite-manifest.json");
    let contents = fs::read_to_string(manifest_path).unwrap();
    let mut manifest: Section4SuiteManifest = serde_json::from_str(&contents).unwrap();
    for profile in &mut manifest.generated_profiles {
        if profile.profile_path.is_relative() {
            profile.profile_path = suite_dir.join(&profile.profile_path);
        }
        if profile.corpus_archive_path.is_relative() {
            profile.corpus_archive_path = suite_dir.join(&profile.corpus_archive_path);
        }
    }
    manifest
}

struct HarvestedFixtureRecord {
    entity_id_metadata: Option<CborValue>,
    synthetic_metadata: Option<CborValue>,
    embedding: Vec<f32>,
}

fn unique_section4_store_root(prefix: &str) -> std::path::PathBuf {
    static NEXT_UNIQUE_SUFFIX: AtomicU64 = AtomicU64::new(0);
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = NEXT_UNIQUE_SUFFIX.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("{prefix}-{unique}-{counter}"));
    fs::create_dir_all(&path).unwrap();
    path
}

fn encode_f32_embedding(values: &[f32]) -> Vec<u8> {
    values
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect()
}

fn harvested_fixture_reference(
    source_id: &str,
    records: impl IntoIterator<Item = HarvestedFixtureRecord>,
) -> BlockStoreCorpusReference {
    let store_root = unique_section4_store_root("section4-harvested-fixture");
    let store = FilesystemBlockStore::new(&store_root).unwrap();
    let spec = EmbeddingSpec {
        dims: 2,
        encoding: "f32le".into(),
    };
    let mut leaves = Vec::new();
    for record in records {
        let mut metadata = Vec::new();
        if let Some(entity_id) = record.entity_id_metadata {
            metadata.push((CborValue::Text("entity_id".into()), entity_id));
        }
        if let Some(synthetic) = record.synthetic_metadata {
            metadata.push((CborValue::Text("synthetic".into()), synthetic));
        }
        let encoded_embedding = encode_f32_embedding(&record.embedding);
        let leaf = build_leaf_block(
            VERSION_1,
            spec.clone(),
            vec![LeafEntry {
                embedding: encoded_embedding.clone(),
                metadata,
                content: Content {
                    media_type: "application/octet-stream".into(),
                    body: Vec::new(),
                },
            }],
            None,
        )
        .unwrap();
        let block_id = store.put(&Block::Leaf(leaf)).unwrap();
        leaves.push((block_id, encoded_embedding));
    }

    let root_block_id = if leaves.len() == 1 {
        leaves[0].0
    } else {
        store
            .put(&Block::Branch(
                build_branch_block(
                    VERSION_1,
                    1,
                    spec,
                    leaves
                        .iter()
                        .map(|(block_id, embedding)| BranchEntry {
                            embedding: embedding.clone(),
                            child: *block_id,
                        })
                        .collect(),
                    None,
                )
                .unwrap(),
            ))
            .unwrap()
    };

    BlockStoreCorpusReference {
        source_id: source_id.into(),
        root_block_id: root_block_id.to_string(),
        store: BlockStoreReferenceStore::Filesystem { store_root },
    }
}

fn harvested_profile(
    profile_id: &str,
    corpus_id: &str,
    source: BlockStoreCorpusReference,
    real_entity_count: usize,
    alignment_policy: AlignmentPolicy,
) -> Section4ProfileSpec {
    Section4ProfileSpec {
        profile_id: profile_id.into(),
        corpus_id: corpus_id.into(),
        scale_tier_id: format!("n-{real_entity_count}"),
        source: Section4ProfileSourceSpec::Harvested {
            family: Section4CorpusFamily::RealWorldHarvested,
            source,
            entity_id_metadata_key: "entity_id".into(),
            harvesting_policy: harvested_policy(),
            real_entity_count,
            alignment_policy,
        },
    }
}

#[test]
fn val_stream_eval_001_repository_includes_crate_and_spec_package() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(manifest_dir.join("src").join("lib.rs").exists());
    assert!(manifest_dir.join("src").join("main.rs").exists());
    assert!(
        manifest_dir
            .join("..")
            .join("..")
            .join("docs")
            .join("specs")
            .join("rust-streaming-clustering-evaluator-crate")
            .join("requirements.md")
            .exists()
    );
}

#[test]
fn val_stream_eval_002_public_surface_remains_subordinate_to_the_shared_contract() {
    let source = lib_source();
    assert!(source.contains("docs/specs/rust-streaming-clustering-crate/"));
    assert!(source.contains("candidate_adapter"));
    assert!(source.contains("StreamingClusterTrainer"));
    assert!(!source.contains("algorithm-specific evaluation hooks"));
}

#[test]
fn val_stream_eval_003_campaign_runs_two_registered_candidates_under_one_profile() {
    let candidates = resolve_registered_candidates(&[
        "balanced-threshold".to_string(),
        "pca-sort-exact-chunking".to_string(),
    ])
    .unwrap();
    let report = run_evaluation_campaign(&strict_alignment_profile(), &candidates).unwrap();

    assert_eq!(report.run_reports.len(), 2);
    assert_eq!(
        report.run_reports[0].provenance.profile_id,
        "strict-alignment-campaign"
    );
    assert!(
        report
            .run_reports
            .iter()
            .any(|run| run.candidate_identity.candidate_id == "pca-sort-exact-chunking")
    );
}

#[test]
fn val_stream_eval_004_candidate_registration_uses_adapter_or_factory_to_construct_trainers() {
    let source = lib_source();
    assert!(source.contains("pub fn candidate_adapter"));
    assert!(source.contains("Fn(&StreamingClusteringConfig)"));
    assert!(source.contains("T: StreamingClusterTrainer"));
    assert!(source.contains("registered_candidate("));
}

#[test]
fn val_stream_eval_005_benchmark_profile_declares_the_required_campaign_fields() {
    let profile = strict_alignment_profile();

    assert_eq!(profile.corpus_ids, vec!["fixture-corpus-a"]);
    assert!(matches!(
        &profile.training_passes[0],
        lexongraph_streaming_clustering_evaluator::TrainingPassSource::Inline { .. }
    ));
    assert_eq!(profile.leaf_model.leaf_size, 2);
    assert_eq!(profile.metric_declarations.len(), 2);
    assert!(!profile.gate_declarations.is_empty());
    assert!(!profile.deferred_research_goals.is_empty());
    assert_eq!(profile.reproducibility.seed_policy, "fixed-seed-7");
}

#[test]
fn val_stream_eval_006_repeated_execution_reports_determinism() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();

    assert!(report.run_reports[0].determinism.deterministic);
    assert!(
        report.run_reports[0]
            .determinism
            .mismatch_details
            .is_empty()
    );
}

#[test]
fn val_stream_eval_007_provenance_manifest_records_reproducibility_metadata() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let provenance = &report.run_reports[0].provenance;

    assert_eq!(
        provenance.candidate_identity.candidate_id,
        "balanced-threshold"
    );
    assert_eq!(provenance.seed_policy, "fixed-seed-7");
    assert_eq!(provenance.software_identity, "fixture-campaign-builder");
    assert_eq!(
        provenance.floating_point_profile,
        "ieee754-deterministic-no-fma"
    );
    assert_eq!(provenance.hardware_profile, "fixture-cpu");
    assert!(provenance.source_reference_ids.is_empty());
}

#[test]
fn val_stream_eval_008_run_report_includes_lifecycle_outputs_and_leaf_membership_materialization() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let run_report = &report.run_reports[0];

    assert_eq!(run_report.pass_reports.len(), 2);
    assert_eq!(run_report.probe_results.len(), 1);
    assert_eq!(run_report.leaf_membership.len(), 4);
}

#[test]
fn val_stream_eval_009_strict_alignment_profile_verifies_fixed_capacity_invariants() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let run_report = &report.run_reports[0];

    assert!(run_report
        .gate_results
        .iter()
        .any(|gate| gate.gate_id == "exact-leaf-occupancy" && gate.status == GateStatus::Passed));
    assert!(
        run_report
            .gate_results
            .iter()
            .any(|gate| gate.gate_id == "complete-coverage" && gate.status == GateStatus::Passed)
    );
    assert!(
        run_report
            .gate_results
            .iter()
            .any(|gate| gate.gate_id == "one-cluster-per-entity"
                && gate.status == GateStatus::Passed)
    );
}

#[test]
fn val_stream_eval_010_synthetic_padding_is_distinguished_and_excluded_from_external_metrics() {
    let report = run_evaluation_campaign(
        &synthetic_padding_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let run_report = &report.run_reports[0];

    assert!(
        run_report
            .leaf_membership
            .iter()
            .any(|member| member.entity_id == "pad-1" && member.synthetic)
    );
    assert!(
        run_report
            .cluster_occupancies
            .iter()
            .any(|occupancy| occupancy.synthetic_count == 1)
    );
    assert!(
        run_report
            .metric_results
            .iter()
            .find(|metric| metric.metric_id == "same-leaf-neighborhood-coherence")
            .unwrap()
            .value
            > 0.0
    );
    let concentration = run_report
        .synthetic_padding_concentration
        .as_ref()
        .expect("padding profiles should report synthetic padding concentration");
    assert_eq!(concentration.synthetic_entity_count, 1);
    assert_eq!(concentration.clusters_with_synthetic_entities, 1);
    assert_eq!(concentration.minimum_possible_cluster_count, 1);
    assert!(concentration.satisfies_minimum_concentration);
}

#[test]
fn val_stream_eval_011_same_leaf_locality_metric_uses_ground_truth_over_real_entities() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let metric = report.run_reports[0]
        .metric_results
        .iter()
        .find(|metric| metric.metric_id == "same-leaf-neighborhood-coherence")
        .unwrap();

    assert_eq!(metric.value, 1.0);
}

#[test]
fn val_stream_eval_012_local_compression_metric_compares_local_and_global_baselines() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let metric = report.run_reports[0]
        .metric_results
        .iter()
        .find(|metric| metric.metric_id == "local-compression-gain")
        .unwrap();

    assert!(metric.value > 0.05);
}

#[test]
fn val_stream_eval_013_report_distinguishes_prerequisites_gates_and_metrics_and_excludes_gate_failures_from_ranking()
 {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &balanced_and_skewed_candidates(),
    )
    .unwrap();
    let balanced = report
        .run_reports
        .iter()
        .find(|run| run.candidate_identity.candidate_id == "balanced-threshold")
        .unwrap();
    let skewed = report
        .run_reports
        .iter()
        .find(|run| run.candidate_identity.candidate_id == "skewed-gate-fail")
        .unwrap();

    assert!(balanced.prerequisite_checks[0].passed);
    assert!(!balanced.metric_results.is_empty());
    assert!(!balanced.gate_results.is_empty());
    assert_eq!(balanced.run_status, CandidateRunStatus::Succeeded);
    assert_eq!(skewed.run_status, CandidateRunStatus::GateFailed);
    assert!(skewed.ranking_score.is_none());
    assert_eq!(report.ranking.len(), 1);
}

#[test]
fn val_stream_eval_014_metric_gate_and_deferred_records_trace_to_research_goals() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let run_report = &report.run_reports[0];

    assert!(
        run_report
            .metric_results
            .iter()
            .all(|metric| !metric.research_goal_ids.is_empty())
    );
    assert!(
        run_report
            .gate_results
            .iter()
            .all(|gate| !gate.research_goal_ids.is_empty())
    );
    assert!(run_report.deferred_research_goals.iter().all(|goal| {
        !goal.research_goal_ids.is_empty() && goal.status == DeferredMeasurementStatus::Deferred
    }));
}

#[test]
fn val_stream_eval_015_emits_machine_readable_reports_and_a_human_scorecard() {
    let report = run_evaluation_campaign(
        &synthetic_padding_profile(),
        &balanced_and_skewed_candidates(),
    )
    .unwrap();
    let artifacts = emit_campaign_artifacts(&report).unwrap();

    assert_eq!(artifacts.per_candidate_reports.len(), 2);
    assert!(
        artifacts
            .campaign_report
            .contents
            .contains("\"profile_id\": \"synthetic-padding-campaign\"")
    );
    assert!(artifacts.scorecard.contents.contains("Campaign scorecard"));
    assert!(
        artifacts
            .scorecard
            .contents
            .contains("synthetic-padding-concentration")
    );
}

#[test]
fn val_stream_eval_016_invalid_profiles_and_shared_contract_failures_are_distinguished() {
    let invalid = run_evaluation_campaign(
        &invalid_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    );
    assert!(matches!(
        invalid,
        Err(EvaluatorError::InvalidConfiguration(_))
    ));

    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &[shared_contract_failure_candidate()],
    )
    .unwrap();
    assert_eq!(
        report.run_reports[0].run_status,
        CandidateRunStatus::CandidateSharedContractFailure
    );

    let source_failure = run_evaluation_campaign(
        &broken_block_store_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    assert_eq!(
        source_failure.run_reports[0].run_status,
        CandidateRunStatus::CorpusSourceFailure
    );

    let archive_source_failure = run_evaluation_campaign(
        &broken_archive_backed_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    assert_eq!(
        archive_source_failure.run_reports[0].run_status,
        CandidateRunStatus::CorpusSourceFailure
    );
    assert!(matches!(
        archive_source_failure.run_reports[0].terminal_failure,
        Some(StructuredFailure::ArchiveSourceOpenFailure { .. })
    ));

    let corrupt_archive_failure = run_evaluation_campaign(
        &corrupt_archive_backed_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    assert_eq!(
        corrupt_archive_failure.run_reports[0].run_status,
        CandidateRunStatus::CorpusSourceFailure
    );
    assert!(matches!(
        corrupt_archive_failure.run_reports[0].terminal_failure,
        Some(StructuredFailure::ArchiveSourceReadFailure { .. })
    ));
}

#[test]
fn val_stream_eval_017_gate_failures_are_reported_separately_from_deferred_goals() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &balanced_and_skewed_candidates(),
    )
    .unwrap();
    let skewed = report
        .run_reports
        .iter()
        .find(|run| run.candidate_identity.candidate_id == "skewed-gate-fail")
        .unwrap();

    assert!(
        skewed
            .gate_results
            .iter()
            .any(|gate| gate.status == GateStatus::Failed)
    );
    assert!(!skewed.deferred_research_goals.is_empty());
}

#[test]
fn val_stream_eval_018_deferred_hierarchy_and_search_goals_remain_explicitly_deferred() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();

    let reason = &report.run_reports[0].deferred_research_goals[0].reason;
    assert!(reason.contains("outside the leaf-stage evaluator boundary"));
    assert!(reason.contains("staged leaf-stage evidence toward docs/research/clustering.md"));
    assert!(reason.contains("future end-to-end evaluator"));
}

#[test]
fn val_stream_eval_019_repository_verification_artifacts_cover_the_evaluator_surface() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(
        manifest_dir
            .join("tests")
            .join("spec_validation.rs")
            .exists()
    );
    assert!(
        manifest_dir
            .join("tests")
            .join("support")
            .join("mod.rs")
            .exists()
    );
    assert!(built_in_fixture_candidate_names().contains(&"nondeterministic-probe"));
}

#[test]
fn val_stream_eval_020_block_store_sources_cover_training_replay_and_probes() {
    let report = run_evaluation_campaign(
        &block_store_backed_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();

    let run_report = &report.run_reports[0];
    assert_eq!(run_report.run_status, CandidateRunStatus::Succeeded);
    assert_eq!(run_report.pass_reports.len(), 2);
    assert_eq!(run_report.probe_results.len(), 1);
    assert_eq!(run_report.leaf_membership.len(), 4);
    assert_eq!(
        run_report.provenance.source_reference_ids,
        vec![
            "evaluation-corpus",
            "probe-corpus",
            "training-pass-1",
            "training-pass-2",
        ]
    );
}

#[test]
fn val_stream_eval_021_inline_and_block_store_profiles_are_semantically_equivalent() {
    let inline_report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let block_store_report = run_evaluation_campaign(
        &block_store_backed_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let archive_report = run_evaluation_campaign(
        &archive_backed_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();

    let inline_run = &inline_report.run_reports[0];
    let block_store_run = &block_store_report.run_reports[0];
    let archive_run = &archive_report.run_reports[0];
    let mut inline_membership = inline_run.leaf_membership.clone();
    inline_membership.sort_by(|left, right| left.entity_id.cmp(&right.entity_id));
    let mut block_store_membership = block_store_run.leaf_membership.clone();
    block_store_membership.sort_by(|left, right| left.entity_id.cmp(&right.entity_id));
    let mut archive_membership = archive_run.leaf_membership.clone();
    archive_membership.sort_by(|left, right| left.entity_id.cmp(&right.entity_id));

    assert_eq!(inline_run.pass_reports, block_store_run.pass_reports);
    assert_eq!(inline_run.pass_reports, archive_run.pass_reports);
    assert_eq!(inline_run.probe_results, block_store_run.probe_results);
    assert_eq!(inline_run.probe_results, archive_run.probe_results);
    assert_eq!(inline_membership, block_store_membership);
    assert_eq!(inline_membership, archive_membership);
    assert_eq!(inline_run.metric_results, block_store_run.metric_results);
    assert_eq!(inline_run.metric_results, archive_run.metric_results);
}

#[test]
fn val_stream_eval_022_archive_backed_profiles_use_zip_archive_source_declarations() {
    let profile = archive_backed_profile();
    let parsed: lexongraph_streaming_clustering_evaluator::BenchmarkProfile =
        serde_json::from_str(&serde_json::to_string(&profile).unwrap()).unwrap();

    let TrainingPassSource::BlockStore { corpus, .. } = &parsed.training_passes[0] else {
        panic!("archive-backed fixture should use a block-store training pass");
    };
    let BlockStoreReferenceStore::ZipArchive { archive_path } = &corpus.store else {
        panic!("archive-backed fixture should use zip archive corpus references");
    };
    assert!(archive_path.is_file());

    let EmbeddingWorkloadSource::BlockStore { corpus } = &parsed.probe_workloads[0].source else {
        panic!("archive-backed fixture should use a block-store probe workload");
    };
    assert!(matches!(
        &corpus.store,
        BlockStoreReferenceStore::ZipArchive { .. }
    ));
    let lexongraph_streaming_clustering_evaluator::EvaluationEntitySource::BlockStore { corpora } =
        &parsed.evaluation_entities
    else {
        panic!("archive-backed fixture should use block-store evaluation entities");
    };
    assert!(corpora.iter().all(|corpus| matches!(
        &corpus.corpus.store,
        BlockStoreReferenceStore::ZipArchive { .. }
    )));

    let report = run_evaluation_campaign(
        &parsed,
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    assert_eq!(
        report.run_reports[0].run_status,
        CandidateRunStatus::Succeeded
    );
}

#[test]
fn regression_legacy_filesystem_profile_json_still_deserializes() {
    fn strip_store_kind(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Object(map) => {
                map.remove("store_kind");
                for child in map.values_mut() {
                    strip_store_kind(child);
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    strip_store_kind(value);
                }
            }
            serde_json::Value::Null
            | serde_json::Value::Bool(_)
            | serde_json::Value::Number(_)
            | serde_json::Value::String(_) => {}
        }
    }

    let mut legacy_json = serde_json::to_value(block_store_backed_profile()).unwrap();
    strip_store_kind(&mut legacy_json);
    let parsed: lexongraph_streaming_clustering_evaluator::BenchmarkProfile =
        serde_json::from_value(legacy_json).unwrap();

    let report = run_evaluation_campaign(
        &parsed,
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    assert_eq!(
        report.run_reports[0].run_status,
        CandidateRunStatus::Succeeded
    );
}

#[test]
fn val_stream_eval_023_archive_backed_sources_cover_training_replay_and_probes() {
    let report = run_evaluation_campaign(
        &archive_backed_profile(),
        &[
            lexongraph_streaming_clustering_evaluator::built_in_fixture_candidate(
                "balanced-threshold",
            )
            .unwrap(),
        ],
    )
    .unwrap();

    let run_report = &report.run_reports[0];
    assert_eq!(run_report.run_status, CandidateRunStatus::Succeeded);
    assert_eq!(run_report.pass_reports.len(), 2);
    assert_eq!(run_report.probe_results.len(), 1);
    assert_eq!(run_report.leaf_membership.len(), 4);
}

#[test]
fn val_stream_eval_024_overlay_helper_writes_new_blocks_only_to_the_mutable_fs_layer() {
    let profile = archive_backed_profile();
    let TrainingPassSource::BlockStore { corpus, .. } = &profile.training_passes[0] else {
        panic!("archive-backed fixture should use a block-store training pass");
    };
    let BlockStoreReferenceStore::ZipArchive { archive_path } = &corpus.store else {
        panic!("archive-backed fixture should use zip archive corpus references");
    };
    let store = FsOverlayZipBlockStore::new(archive_path).unwrap();

    let block = Block::Leaf(
        build_leaf_block(
            VERSION_1,
            EmbeddingSpec {
                dims: 2,
                encoding: "f32le".into(),
            },
            vec![LeafEntry {
                embedding: [1.0f32.to_le_bytes(), 2.0f32.to_le_bytes()].concat(),
                metadata: vec![(
                    CborValue::Text("entity_id".into()),
                    CborValue::Text("extra".into()),
                )],
                content: Content {
                    media_type: "application/octet-stream".into(),
                    body: Vec::new(),
                },
            }],
            None,
        )
        .unwrap(),
    );

    let block_id = store.put(&block).unwrap();
    assert!(store.get(&block_id).unwrap().is_some());
    assert!(
        FilesystemBlockStore::new(store.writable_layer_path())
            .unwrap()
            .get(&block_id)
            .unwrap()
            .is_some()
    );
    assert!(
        ZipBlockStore::new(archive_path)
            .unwrap()
            .get(&block_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn val_stream_eval_determinism_gate_detects_observable_nondeterminism() {
    let report =
        run_evaluation_campaign(&strict_alignment_profile(), &[nondeterministic_candidate()])
            .unwrap();

    assert!(!report.run_reports[0].determinism.deterministic);
    assert_eq!(
        report.run_reports[0].run_status,
        CandidateRunStatus::GateFailed
    );
}

#[test]
fn regression_probe_assignments_outside_k_are_reported_as_shared_contract_failures() {
    let candidate = candidate_adapter(
        CandidateIdentity {
            candidate_id: "invalid-probe-cluster-id".into(),
            implementation_label: "Invalid probe cluster-id fixture".into(),
            software_identity: "invalid-probe-cluster-id-v1".into(),
        },
        |config| Ok(InvalidRangeTrainer::new(config, InvalidRangeMode::Probe)),
    );
    let report = run_evaluation_campaign(&strict_alignment_profile(), &[candidate]).unwrap();

    assert_eq!(
        report.run_reports[0].run_status,
        CandidateRunStatus::CandidateSharedContractFailure
    );
    assert!(
        report.run_reports[0].prerequisite_checks[0]
            .detail
            .contains("outside [0, 2)")
    );
}

#[test]
fn regression_leaf_membership_assignments_outside_k_are_reported_as_shared_contract_failures() {
    let candidate = candidate_adapter(
        CandidateIdentity {
            candidate_id: "invalid-leaf-cluster-id".into(),
            implementation_label: "Invalid leaf cluster-id fixture".into(),
            software_identity: "invalid-leaf-cluster-id-v1".into(),
        },
        |config| {
            Ok(InvalidRangeTrainer::new(
                config,
                InvalidRangeMode::LeafMembership,
            ))
        },
    );
    let report = run_evaluation_campaign(&strict_alignment_profile(), &[candidate]).unwrap();

    assert_eq!(
        report.run_reports[0].run_status,
        CandidateRunStatus::CandidateSharedContractFailure
    );
    assert!(
        report.run_reports[0].prerequisite_checks[0]
            .detail
            .contains("outside [0, 2)")
    );
}

#[test]
fn regression_candidate_artifact_names_are_sanitized_before_writing() {
    let mut report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &balanced_and_skewed_candidates(),
    )
    .unwrap();
    report.run_reports[0].candidate_identity.candidate_id = "..\\evil/name".into();

    let artifacts = emit_campaign_artifacts(&report).unwrap();

    assert_eq!(
        artifacts.per_candidate_reports[0].file_name,
        "evil_name-run-report.json"
    );
}

#[test]
fn regression_sanitized_candidate_artifact_names_remain_unique() {
    let mut report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &balanced_and_skewed_candidates(),
    )
    .unwrap();
    report.run_reports[0].candidate_identity.candidate_id = "a/b".into();
    report.run_reports[1].candidate_identity.candidate_id = "a\\b".into();

    let artifacts = emit_campaign_artifacts(&report).unwrap();

    assert_eq!(
        artifacts.per_candidate_reports[0].file_name,
        "a_b-run-report.json"
    );
    assert_eq!(
        artifacts.per_candidate_reports[1].file_name,
        "a_b-1-run-report.json"
    );
}

#[test]
fn regression_write_campaign_artifacts_includes_output_path_in_io_errors() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &balanced_and_skewed_candidates(),
    )
    .unwrap();
    let artifacts = emit_campaign_artifacts(&report).unwrap();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let temp_file = std::env::temp_dir().join(format!(
        "lexongraph-streaming-evaluator-io-error-{unique}.tmp"
    ));
    fs::write(&temp_file, "occupied").unwrap();

    let result =
        lexongraph_streaming_clustering_evaluator::write_campaign_artifacts(&temp_file, &artifacts);

    assert!(
        matches!(result, Err(EvaluatorError::Io(message)) if message.contains(&temp_file.display().to_string()))
    );

    fs::remove_file(temp_file).unwrap();
}

#[test]
fn regression_duplicate_corpus_ids_are_rejected() {
    let mut profile = strict_alignment_profile();
    profile.corpus_ids.push("fixture-corpus-a".into());

    let result = run_evaluation_campaign(&profile, &balanced_and_skewed_candidates());

    assert!(
        matches!(result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("duplicate value in corpus ids"))
    );
}

#[test]
fn regression_unknown_entity_corpus_ids_are_rejected() {
    let mut profile = strict_alignment_profile();
    profile
        .inline_evaluation_entities_mut()
        .expect("unknown-corpus regression fixture should use inline entities")[0]
        .corpus_id = "unknown-corpus".into();

    let result = run_evaluation_campaign(&profile, &balanced_and_skewed_candidates());

    assert!(
        matches!(result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("references unknown corpus"))
    );
}

#[test]
fn regression_duplicate_corpus_source_ids_are_rejected() {
    let result = run_evaluation_campaign(
        &duplicate_source_id_profile(),
        &balanced_and_skewed_candidates(),
    );

    assert!(
        matches!(result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("duplicate value in corpus source ids"))
    );
}

#[test]
fn regression_empty_synthetic_metadata_keys_are_rejected() {
    let result = run_evaluation_campaign(
        &empty_synthetic_metadata_key_profile(),
        &balanced_and_skewed_candidates(),
    );

    assert!(
        matches!(result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("must not declare an empty synthetic_metadata_key"))
    );
}

#[test]
fn regression_missing_synthetic_metadata_keys_are_rejected_for_block_store_padding_profiles() {
    let result = run_evaluation_campaign(
        &missing_synthetic_metadata_key_profile(),
        &balanced_and_skewed_candidates(),
    );

    assert!(
        matches!(result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("must declare synthetic_metadata_key when using deterministic synthetic padding"))
    );
}

#[test]
fn regression_failed_candidate_runs_keep_evaluation_entities_in_determinism_schema() {
    let report = run_evaluation_campaign(
        &strict_alignment_profile(),
        &[shared_contract_failure_candidate()],
    )
    .expect("shared-contract failures should still produce a campaign report")
    .run_reports
    .into_iter()
    .next()
    .expect("campaign should include one candidate report");

    assert_eq!(
        report.determinism.compared_fields,
        vec![
            "pass_reports",
            "probe_results",
            "leaf_membership",
            "evaluation_entities",
            "provenance",
        ]
    );
}

#[test]
fn regression_failed_corpus_source_runs_keep_evaluation_entities_in_determinism_schema() {
    let report = run_evaluation_campaign(
        &broken_block_store_profile(),
        &balanced_and_skewed_candidates()[..1],
    )
    .expect("corpus source failures should still produce a campaign report")
    .run_reports
    .into_iter()
    .next()
    .expect("campaign should include one candidate report");

    assert_eq!(
        report.determinism.compared_fields,
        vec![
            "pass_reports",
            "probe_results",
            "leaf_membership",
            "evaluation_entities",
            "provenance",
        ]
    );
}

#[test]
fn regression_duplicate_materialized_block_store_entities_are_load_failures() {
    let report = run_evaluation_campaign(
        &duplicate_evaluation_entities_block_store_profile(),
        &balanced_and_skewed_candidates()[..1],
    )
    .expect("corpus content failures should still produce a campaign report");

    assert!(matches!(
        report.run_reports[0].terminal_failure,
        Some(StructuredFailure::CorpusSourceLoadFailure { .. })
    ));
}

#[test]
fn regression_invalid_materialized_block_store_entity_counts_are_load_failures() {
    let report = run_evaluation_campaign(
        &wrong_entity_count_block_store_profile(),
        &balanced_and_skewed_candidates()[..1],
    )
    .expect("materialized entity validation failures should still produce a campaign report");

    assert!(matches!(
        report.run_reports[0].terminal_failure,
        Some(StructuredFailure::CorpusSourceLoadFailure { .. })
    ));
}

#[test]
fn regression_non_finite_or_negative_ranking_weights_are_rejected() {
    for invalid_weight in [f64::NAN, f64::INFINITY, -0.5] {
        let mut profile = strict_alignment_profile();
        profile.metric_declarations[0].ranking_weight = invalid_weight;

        let result = run_evaluation_campaign(&profile, &balanced_and_skewed_candidates());

        assert!(
            matches!(result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("ranking_weight must be finite and non-negative"))
        );
    }
}

#[test]
fn regression_non_finite_gate_minima_are_rejected() {
    for invalid_minimum in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let mut profile = strict_alignment_profile();
        let gate = profile
            .gate_declarations
            .iter_mut()
            .find(|gate| gate.gate_id == "same-leaf-coherence-threshold")
            .expect("strict fixture profile should include the same-leaf coherence gate");
        let lexongraph_streaming_clustering_evaluator::GateKind::MetricAtLeast { minimum, .. } =
            &mut gate.kind
        else {
            panic!("same-leaf coherence threshold gate should use MetricAtLeast");
        };
        *minimum = invalid_minimum;

        let result = run_evaluation_campaign(&profile, &balanced_and_skewed_candidates());

        assert!(
            matches!(result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("minimum must be finite"))
        );
    }
}

#[test]
fn regression_empty_candidate_ids_are_rejected() {
    let mut candidates = balanced_and_skewed_candidates();
    candidates[0].identity.candidate_id = "   ".into();

    let result = run_evaluation_campaign(&strict_alignment_profile(), &candidates);

    assert!(
        matches!(result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("candidate_id must not be empty"))
    );
}

#[test]
fn regression_duplicate_candidate_ids_are_rejected() {
    let mut candidates = balanced_and_skewed_candidates();
    candidates[1].identity.candidate_id = candidates[0].identity.candidate_id.clone();

    let result = run_evaluation_campaign(&strict_alignment_profile(), &candidates);

    assert!(
        matches!(result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("duplicate value in candidate ids"))
    );
}

#[test]
fn regression_invalid_transition_errors_report_the_original_state() {
    let config = strict_alignment_profile()
        .shared_candidate_config
        .to_streaming_config();

    let mut trainer = InvalidRangeTrainer::new(&config, InvalidRangeMode::Probe);
    assert!(matches!(
        trainer.finish_pass(),
        Err(StreamingClusteringError::InvalidTransition {
            state: TrainerState::Idle,
            ..
        })
    ));

    let mut trainer = InvalidRangeTrainer::new(&config, InvalidRangeMode::Probe);
    trainer.ingest_batch(&[vec![0.0, 0.0]]).unwrap();
    trainer.finish_pass().unwrap();
    trainer.complete_training().unwrap();
    assert!(matches!(
        trainer.complete_training(),
        Err(StreamingClusteringError::InvalidTransition {
            state: TrainerState::TrainingComplete,
            ..
        })
    ));
}

#[test]
fn regression_cli_profile_errors_include_profile_path_context() {
    let source = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("main.rs"),
    )
    .unwrap();

    assert!(source.contains("failed to read benchmark profile {}"));
    assert!(source.contains("failed to parse benchmark profile {}"));
    assert!(source.contains("profile_path.display()"));
}

#[test]
fn val_stream_eval_025_section4_suite_materializes_reproducible_leaf_stage_assets() {
    let output_dir = tempdir().unwrap();
    let spec = section4_suite_spec(vec![strict_synthetic_profile(
        "strict-leaf-tier",
        "well-clustered-small",
        12,
    )]);

    let manifest = generate_section4_suite_assets(&spec, output_dir.path()).unwrap();

    assert_eq!(manifest.suite_id, "section4-corpus-panel-suite");
    assert!(
        output_dir
            .path()
            .join("section4-suite-manifest.json")
            .exists()
    );
    assert_eq!(manifest.generated_profiles.len(), 1);
    assert_eq!(
        manifest.generated_profiles[0].metric_contract,
        Section4MetricContract::Euclidean
    );
    let profile: lexongraph_streaming_clustering_evaluator::BenchmarkProfile =
        serde_json::from_str(
            &fs::read_to_string(&manifest.generated_profiles[0].profile_path).unwrap(),
        )
        .unwrap();
    assert_eq!(profile.locality_ground_truth.len(), 12);
    assert!(
        profile
            .locality_ground_truth
            .iter()
            .all(|entry| entry.neighbor_ids.len() == 10)
    );
    assert!(profile.deferred_research_goals.iter().any(|goal| {
        goal.research_goal_ids
            .iter()
            .any(|goal_id| goal_id == "RG-HIERARCHY")
    }));
}

#[test]
fn regression_section4_suite_generator_allows_non_top10_neighbor_counts_for_custom_suites() {
    let output_dir = tempdir().unwrap();
    let mut spec = section4_suite_spec(vec![strict_synthetic_profile(
        "custom-neighbor-count",
        "custom-corpus",
        12,
    )]);
    spec.neighbor_count = 3;

    let manifest = generate_section4_suite_assets(&spec, output_dir.path()).unwrap();
    let profile: lexongraph_streaming_clustering_evaluator::BenchmarkProfile =
        serde_json::from_str(
            &fs::read_to_string(&manifest.generated_profiles[0].profile_path).unwrap(),
        )
        .unwrap();

    assert_eq!(manifest.generated_profiles[0].neighbor_count, 3);
    assert!(
        profile
            .locality_ground_truth
            .iter()
            .all(|entry| entry.neighbor_ids.len() == 3)
    );
}

#[test]
fn val_stream_eval_026_section4_suite_covers_required_corpus_families_and_scale_tiers() {
    let output_dir = tempdir().unwrap();
    let harvested_source = harvested_archive_reference();
    let spec = section4_suite_spec(vec![
        Section4ProfileSpec {
            profile_id: "harvested-tier".into(),
            corpus_id: "real-world-tier".into(),
            scale_tier_id: "n-4".into(),
            source: Section4ProfileSourceSpec::Harvested {
                family: Section4CorpusFamily::RealWorldHarvested,
                source: harvested_source,
                entity_id_metadata_key: "entity_id".into(),
                harvesting_policy: harvested_policy(),
                real_entity_count: 12,
                alignment_policy: AlignmentPolicy::StrictAlignment,
            },
        },
        Section4ProfileSpec {
            profile_id: "clustered-tier".into(),
            corpus_id: "clustered-tier".into(),
            scale_tier_id: "n-4".into(),
            source: Section4ProfileSourceSpec::Synthetic {
                family: Section4CorpusFamily::WellClusteredSynthetic,
                real_entity_count: 12,
                alignment_policy: AlignmentPolicy::StrictAlignment,
            },
        },
        Section4ProfileSpec {
            profile_id: "weak-tier".into(),
            corpus_id: "weak-tier".into(),
            scale_tier_id: "n-4".into(),
            source: Section4ProfileSourceSpec::Synthetic {
                family: Section4CorpusFamily::WeakClusterUniform,
                real_entity_count: 12,
                alignment_policy: AlignmentPolicy::StrictAlignment,
            },
        },
        Section4ProfileSpec {
            profile_id: "manifold-tier".into(),
            corpus_id: "manifold-tier".into(),
            scale_tier_id: "n-4".into(),
            source: Section4ProfileSourceSpec::Synthetic {
                family: Section4CorpusFamily::AnisotropicManifold,
                real_entity_count: 12,
                alignment_policy: AlignmentPolicy::StrictAlignment,
            },
        },
        Section4ProfileSpec {
            profile_id: "duplicates-tier".into(),
            corpus_id: "duplicates-tier".into(),
            scale_tier_id: "n-12".into(),
            source: Section4ProfileSourceSpec::Synthetic {
                family: Section4CorpusFamily::NearDuplicateHeavy,
                real_entity_count: 12,
                alignment_policy: AlignmentPolicy::StrictAlignment,
            },
        },
    ]);

    let manifest = generate_section4_suite_assets(&spec, output_dir.path()).unwrap();

    let families = manifest
        .generated_profiles
        .iter()
        .map(|profile| profile.family.clone())
        .collect::<Vec<_>>();
    assert_eq!(families.len(), 5);
    assert!(families.contains(&Section4CorpusFamily::RealWorldHarvested));
    assert!(families.contains(&Section4CorpusFamily::WellClusteredSynthetic));
    assert!(families.contains(&Section4CorpusFamily::WeakClusterUniform));
    assert!(families.contains(&Section4CorpusFamily::AnisotropicManifold));
    assert!(families.contains(&Section4CorpusFamily::NearDuplicateHeavy));
    assert!(
        manifest
            .generated_profiles
            .iter()
            .all(|profile| !profile.profile_id.trim().is_empty()
                && !profile.scale_tier_id.trim().is_empty())
    );
}

#[test]
fn val_stream_eval_027_ground_truth_is_deterministic_and_excludes_synthetic_padding() {
    let output_dir = tempdir().unwrap();
    let spec = section4_suite_spec(vec![padding_synthetic_profile(
        "padding-tier",
        "near-duplicates-small",
        11,
    )]);

    let manifest = generate_section4_suite_assets(&spec, output_dir.path()).unwrap();
    let profile: lexongraph_streaming_clustering_evaluator::BenchmarkProfile =
        serde_json::from_str(
            &fs::read_to_string(&manifest.generated_profiles[0].profile_path).unwrap(),
        )
        .unwrap();

    let EvaluationEntitySource::BlockStore { corpora } = &profile.evaluation_entities else {
        panic!("section-4 assets should materialize archive-backed evaluation entities");
    };
    assert_eq!(manifest.generated_profiles[0].real_entity_count, 11);
    assert_eq!(manifest.generated_profiles[0].evaluated_entity_count, 12);
    assert_eq!(profile.locality_ground_truth.len(), 11);
    assert!(profile.locality_ground_truth.iter().all(|entry| {
        !entry.entity_id.contains("-synthetic-")
            && entry
                .neighbor_ids
                .iter()
                .all(|neighbor_id| !neighbor_id.contains("-synthetic-"))
    }));
    assert!(matches!(
        &corpora[0].corpus.store,
        BlockStoreReferenceStore::ZipArchive { .. }
    ));
    assert!(
        profile
            .locality_ground_truth
            .iter()
            .all(|entry| entry.neighbor_ids.len() == 10)
    );
}

#[test]
fn val_stream_eval_028_harvesting_is_deterministic_and_preserves_source_identity() {
    let output_dir_a = tempdir().unwrap();
    let output_dir_b = tempdir().unwrap();
    let harvested_source = harvested_archive_reference();
    let spec = section4_suite_spec(vec![Section4ProfileSpec {
        profile_id: "harvested-tier".into(),
        corpus_id: "real-world-tier".into(),
        scale_tier_id: "n-4".into(),
        source: Section4ProfileSourceSpec::Harvested {
            family: Section4CorpusFamily::RealWorldHarvested,
            source: harvested_source.clone(),
            entity_id_metadata_key: "entity_id".into(),
            harvesting_policy: harvested_policy(),
            real_entity_count: 12,
            alignment_policy: AlignmentPolicy::StrictAlignment,
        },
    }]);

    let manifest_a = generate_section4_suite_assets(&spec, output_dir_a.path()).unwrap();
    let manifest_b = generate_section4_suite_assets(&spec, output_dir_b.path()).unwrap();

    assert_eq!(
        manifest_a.generated_profiles[0].root_block_id,
        manifest_b.generated_profiles[0].root_block_id
    );
    assert_eq!(
        manifest_a.generated_profiles[0]
            .harvested_source_id
            .as_deref(),
        Some(harvested_source.source_id.as_str())
    );
    assert_eq!(
        manifest_a.generated_profiles[0]
            .harvested_source_root_block_id
            .as_deref(),
        Some(harvested_source.root_block_id.as_str())
    );
    assert_eq!(
        manifest_a.generated_profiles[0]
            .harvested_entity_id_metadata_key
            .as_deref(),
        Some("entity_id")
    );
    assert_eq!(
        manifest_a.generated_profiles[0].harvested_policy.as_ref(),
        Some(&harvested_policy())
    );
}

#[test]
fn val_stream_eval_029_generated_large_corpus_assets_run_directly_from_zip_archives() {
    let output_dir = tempdir().unwrap();
    let spec = section4_suite_spec(vec![strict_synthetic_profile(
        "archive-tier",
        "well-clustered-small",
        12,
    )]);

    let manifest = generate_section4_suite_assets(&spec, output_dir.path()).unwrap();
    let profile: lexongraph_streaming_clustering_evaluator::BenchmarkProfile =
        serde_json::from_str(
            &fs::read_to_string(&manifest.generated_profiles[0].profile_path).unwrap(),
        )
        .unwrap();

    for pass in &profile.training_passes {
        let TrainingPassSource::BlockStore { corpus, .. } = pass else {
            panic!("section-4 training passes should be block-store backed");
        };
        assert!(matches!(
            corpus.store,
            BlockStoreReferenceStore::ZipArchive { .. }
        ));
    }
    let EmbeddingWorkloadSource::BlockStore { corpus } = &profile.probe_workloads[0].source else {
        panic!("section-4 probe workload should be block-store backed");
    };
    assert!(matches!(
        corpus.store,
        BlockStoreReferenceStore::ZipArchive { .. }
    ));
}

#[test]
fn val_stream_eval_030_section4_screening_runs_strict_and_padding_profiles() {
    let asset_dir = tempdir().unwrap();
    let report_dir = tempdir().unwrap();
    let spec = section4_suite_spec(vec![
        strict_synthetic_profile("strict-tier", "clustered-small", 12),
        padding_synthetic_profile("padding-tier", "duplicate-pad", 11),
        Section4ProfileSpec {
            profile_id: "harvested-tier".into(),
            corpus_id: "real-world-tier".into(),
            scale_tier_id: "n-12".into(),
            source: Section4ProfileSourceSpec::Harvested {
                family: Section4CorpusFamily::RealWorldHarvested,
                source: harvested_archive_reference(),
                entity_id_metadata_key: "entity_id".into(),
                harvesting_policy: harvested_policy(),
                real_entity_count: 12,
                alignment_policy: AlignmentPolicy::StrictAlignment,
            },
        },
    ]);

    let manifest = generate_section4_suite_assets(&spec, asset_dir.path()).unwrap();
    let report = run_section4_suite(
        &manifest,
        &balanced_and_skewed_candidates(),
        report_dir.path(),
    )
    .unwrap();

    assert_eq!(report.profile_reports.len(), 3);
    for profile in &report.profile_reports {
        assert_eq!(profile.candidate_reports.len(), 2);
    }
    let strict_campaign: CampaignReport = serde_json::from_str(
        &fs::read_to_string(
            report_dir
                .path()
                .join("strict-tier")
                .join("campaign-report.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let padding_campaign: CampaignReport = serde_json::from_str(
        &fs::read_to_string(
            report_dir
                .path()
                .join("padding-tier")
                .join("campaign-report.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let harvested_campaign: CampaignReport = serde_json::from_str(
        &fs::read_to_string(
            report_dir
                .path()
                .join("harvested-tier")
                .join("campaign-report.json"),
        )
        .unwrap(),
    )
    .unwrap();
    for campaign in [&strict_campaign, &padding_campaign, &harvested_campaign] {
        assert!(campaign.run_reports.iter().any(|run| {
            run.gate_results
                .iter()
                .any(|gate| gate.gate_id == "exact-leaf-occupancy")
        }));
        assert!(campaign.run_reports.iter().any(|run| {
            run.metric_results
                .iter()
                .any(|metric| metric.metric_id == "same-leaf-neighborhood-coherence")
        }));
        assert!(campaign.run_reports.iter().any(|run| {
            run.metric_results
                .iter()
                .any(|metric| metric.metric_id == "local-compression-gain")
        }));
        assert!(campaign.run_reports.iter().any(|run| {
            run.gate_results
                .iter()
                .any(|gate| gate.gate_id == "deterministic-observable-results")
        }));
    }
}

#[test]
fn val_stream_eval_031_section4_reports_scale_tiers_and_build_time_per_vector() {
    let asset_dir = tempdir().unwrap();
    let report_dir = tempdir().unwrap();
    let spec = section4_suite_spec(vec![
        strict_synthetic_profile("tier-small", "clustered-small", 12),
        strict_synthetic_profile("tier-medium", "clustered-medium", 14),
    ]);

    let manifest = generate_section4_suite_assets(&spec, asset_dir.path()).unwrap();
    let report = run_section4_suite(
        &manifest,
        &balanced_and_skewed_candidates(),
        report_dir.path(),
    )
    .unwrap();
    let artifacts = write_section4_suite_artifacts(&report, report_dir.path()).unwrap();

    assert!(artifacts.suite_report_path.exists());
    assert!(artifacts.scorecard_path.exists());
    assert_eq!(report.profile_reports.len(), 2);
    assert_ne!(
        report.profile_reports[0].scale_tier_id,
        report.profile_reports[1].scale_tier_id
    );
    assert!(
        report
            .profile_reports
            .iter()
            .all(
                |profile| profile.candidate_reports.iter().all(|candidate| candidate
                    .campaign_time_per_vector_nanos
                    .is_finite()
                    && candidate.campaign_time_per_vector_nanos > 0.0)
            )
    );
}

#[test]
fn val_stream_eval_032_checked_in_section4_suite_supports_repository_owned_candidates() {
    let manifest = checked_in_section4_suite_manifest();
    let report_dir = tempdir().unwrap();
    let candidate_ids = [
        "pca-sort-exact-chunking".to_string(),
        "directional-pca".to_string(),
        "dcbc-streaming".to_string(),
    ];
    let candidates = resolve_registered_candidates(&candidate_ids).unwrap();

    let report = run_section4_suite(&manifest, &candidates, report_dir.path()).unwrap();

    assert_eq!(report.profile_reports.len(), 8);
    assert!(report.profile_reports.iter().all(|profile| {
        candidate_ids.iter().all(|candidate_id| {
            profile
                .candidate_reports
                .iter()
                .any(|candidate| &candidate.candidate_id == candidate_id)
        })
    }));
}

#[test]
fn val_stream_eval_033_fixture_and_repository_candidates_share_one_campaign_model() {
    let candidates = resolve_registered_candidates(&[
        "balanced-threshold".to_string(),
        "pca-sort-exact-chunking".to_string(),
        "directional-pca".to_string(),
        "dcbc-streaming".to_string(),
    ])
    .unwrap();
    let report =
        run_evaluation_campaign(&non_zero_strict_alignment_profile(), &candidates).unwrap();

    let fixture = report
        .run_reports
        .iter()
        .find(|run| run.candidate_identity.candidate_id == "balanced-threshold")
        .unwrap();
    let expected_identities = [
        ("pca-sort-exact-chunking", "lexongraph-pca-chunking-v"),
        ("directional-pca", "lexongraph-directional-pca-v"),
        ("dcbc-streaming", "lexongraph-dcbc-streaming-v"),
    ];
    for (candidate_id, software_prefix) in expected_identities {
        let concrete = report
            .run_reports
            .iter()
            .find(|run| run.candidate_identity.candidate_id == candidate_id)
            .unwrap();
        assert_eq!(
            fixture.provenance.profile_id,
            concrete.provenance.profile_id
        );
        assert_eq!(fixture.pass_reports.len(), concrete.pass_reports.len());
        assert!(
            concrete
                .candidate_identity
                .software_identity
                .starts_with(software_prefix)
        );
        assert!(registered_candidate_names().contains(&candidate_id));
    }
}

#[test]
fn val_stream_eval_034_registered_candidate_listing_includes_repository_owned_candidates() {
    let names = registered_candidate_names();
    assert!(names.contains(&"pca-sort-exact-chunking"));
    assert!(names.contains(&"directional-pca"));
    assert!(names.contains(&"dcbc-streaming"));

    let binary = env!("CARGO_BIN_EXE_lexongraph-streaming-clustering-evaluator");
    let output = ProcessCommand::new(binary)
        .arg("list-candidates")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("pca-sort-exact-chunking"));
    assert!(stdout.contains("directional-pca"));
    assert!(stdout.contains("dcbc-streaming"));
}

#[test]
fn val_stream_eval_035_candidate_incompatibilities_are_reported_explicitly() {
    let mut directional_profile = strict_alignment_profile();
    directional_profile
        .shared_candidate_config
        .balance_constraints = Some(SharedBalanceConstraints {
        min_cluster_occupancy: Some(1),
        max_cluster_occupancy: None,
        max_cluster_size_ratio: None,
        soft_balance_penalty: None,
    });
    let directional_candidates =
        resolve_registered_candidates(&["directional-pca".to_string()]).unwrap();
    let directional_report =
        run_evaluation_campaign(&directional_profile, &directional_candidates).unwrap();
    let directional_run = &directional_report.run_reports[0];
    assert_eq!(
        directional_run.run_status,
        CandidateRunStatus::CandidateSharedContractFailure
    );
    assert!(matches!(
        directional_run.terminal_failure.as_ref(),
        Some(StructuredFailure::CandidateSharedContractFailure { candidate_id, message })
            if candidate_id == "directional-pca"
                && message.contains("balance constraints")
    ));

    let zero_norm_candidates =
        resolve_registered_candidates(&["dcbc-streaming".to_string()]).unwrap();
    let zero_norm_report =
        run_evaluation_campaign(&strict_alignment_profile(), &zero_norm_candidates).unwrap();
    let zero_norm_run = &zero_norm_report.run_reports[0];
    assert_eq!(
        zero_norm_run.run_status,
        CandidateRunStatus::CandidateSharedContractFailure
    );
    assert!(matches!(
        zero_norm_run.terminal_failure.as_ref(),
        Some(StructuredFailure::CandidateSharedContractFailure { candidate_id, message })
            if candidate_id == "dcbc-streaming"
                && message.contains("non-zero Euclidean norm")
    ));

    let mut unsupported_balance_profile = non_zero_strict_alignment_profile();
    unsupported_balance_profile
        .shared_candidate_config
        .balance_constraints = Some(SharedBalanceConstraints {
        min_cluster_occupancy: None,
        max_cluster_occupancy: None,
        max_cluster_size_ratio: Some(1.5),
        soft_balance_penalty: None,
    });
    let unsupported_balance_report =
        run_evaluation_campaign(&unsupported_balance_profile, &zero_norm_candidates).unwrap();
    let unsupported_balance_run = &unsupported_balance_report.run_reports[0];
    assert_eq!(
        unsupported_balance_run.run_status,
        CandidateRunStatus::CandidateSharedContractFailure
    );
    assert!(matches!(
        unsupported_balance_run.terminal_failure.as_ref(),
        Some(StructuredFailure::CandidateSharedContractFailure { candidate_id, message })
            if candidate_id == "dcbc-streaming"
                && message.contains("max_cluster_size_ratio")
    ));

    let mut soft_penalty_profile = non_zero_strict_alignment_profile();
    soft_penalty_profile
        .shared_candidate_config
        .balance_constraints = Some(SharedBalanceConstraints {
        min_cluster_occupancy: None,
        max_cluster_occupancy: None,
        max_cluster_size_ratio: None,
        soft_balance_penalty: Some(0.25),
    });
    let soft_penalty_report =
        run_evaluation_campaign(&soft_penalty_profile, &zero_norm_candidates).unwrap();
    let soft_penalty_run = &soft_penalty_report.run_reports[0];
    assert_eq!(
        soft_penalty_run.run_status,
        CandidateRunStatus::CandidateSharedContractFailure
    );
    assert!(matches!(
        soft_penalty_run.terminal_failure.as_ref(),
        Some(StructuredFailure::CandidateSharedContractFailure { candidate_id, message })
            if candidate_id == "dcbc-streaming"
                && message.contains("soft_balance_penalty")
    ));
}

#[test]
fn regression_section4_cli_commands_execute_end_to_end() {
    let suite_dir = tempdir().unwrap();
    let report_dir = tempdir().unwrap();
    let suite_path = suite_dir.path().join("suite.json");
    let spec = section4_suite_spec(vec![strict_synthetic_profile("cli-tier", "cli-corpus", 12)]);
    fs::write(&suite_path, serde_json::to_string_pretty(&spec).unwrap()).unwrap();

    let binary = env!("CARGO_BIN_EXE_lexongraph-streaming-clustering-evaluator");
    let generate = ProcessCommand::new(binary)
        .args([
            "generate-section4-assets",
            "--suite",
            suite_path.to_str().unwrap(),
            "--output-dir",
            suite_dir.path().to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        generate.status.success(),
        "{}",
        String::from_utf8_lossy(&generate.stderr)
    );

    let manifest_path = suite_dir.path().join("section4-suite-manifest.json");
    let run = ProcessCommand::new(binary)
        .args([
            "run-section4-suite",
            "--manifest",
            manifest_path.to_str().unwrap(),
            "--candidate",
            "balanced-threshold",
            "--output-dir",
            report_dir.path().to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(
        report_dir
            .path()
            .join("section4-suite-report.json")
            .exists()
    );
    assert!(
        report_dir
            .path()
            .join("section4-suite-scorecard.txt")
            .exists()
    );
}

#[test]
fn regression_checked_in_section4_suite_assets_exist_and_match_the_spec() {
    let suite_dir = checked_in_section4_suite_dir();
    let suite_spec_path = suite_dir.join("section4-suite-spec.json");
    let suite_spec: Section4SuiteSpec =
        serde_json::from_str(&fs::read_to_string(suite_spec_path).unwrap()).unwrap();
    let manifest_contents =
        fs::read_to_string(suite_dir.join("section4-suite-manifest.json")).unwrap();
    let manifest = checked_in_section4_suite_manifest();

    assert_eq!(suite_spec.suite_id, manifest.suite_id);
    assert_eq!(suite_spec.neighbor_count, 10);
    assert_eq!(suite_spec.profiles.len(), 8);
    assert_eq!(manifest.generated_profiles.len(), suite_spec.profiles.len());
    assert!(!manifest_contents.contains("\\\\"));
    assert!(
        suite_spec
            .profiles
            .iter()
            .any(|profile| matches!(&profile.source, Section4ProfileSourceSpec::Harvested { .. }))
    );
    for generated in &manifest.generated_profiles {
        assert!(generated.profile_path.exists());
        assert!(generated.corpus_archive_path.exists());
        assert!(generated.profile_path.starts_with(&suite_dir));
        assert!(generated.corpus_archive_path.starts_with(&suite_dir));
        let profile_contents = fs::read_to_string(&generated.profile_path).unwrap();
        assert!(!profile_contents.contains("\\\\"));
    }
    assert!(manifest.generated_profiles.iter().any(|profile| {
        profile.family == Section4CorpusFamily::RealWorldHarvested
            && profile.harvested_policy.as_ref() == Some(&harvested_policy())
    }));
}

#[test]
fn regression_checked_in_section4_suite_assets_run_successfully() {
    let report_dir = tempdir().unwrap();
    let manifest = checked_in_section4_suite_manifest();
    let report = run_section4_suite(
        &manifest,
        &balanced_and_skewed_candidates()[..1],
        report_dir.path(),
    )
    .unwrap();

    assert_eq!(report.suite_id, "section4-corpus-panel-v2");
    assert_eq!(report.profile_reports.len(), 8);
    assert!(
        report
            .profile_reports
            .iter()
            .all(|profile| !profile.candidate_reports.is_empty())
    );
    assert!(
        report_dir
            .path()
            .join("real-world-harvested-strict-small")
            .exists()
    );
}

#[test]
fn regression_checked_in_section4_suite_cli_runs_from_non_repo_working_directory() {
    let report_dir = tempdir().unwrap();
    let run_dir = tempdir().unwrap();
    let binary = env!("CARGO_BIN_EXE_lexongraph-streaming-clustering-evaluator");
    let manifest_path = checked_in_section4_suite_dir().join("section4-suite-manifest.json");
    let run = ProcessCommand::new(binary)
        .current_dir(run_dir.path())
        .args([
            "run-section4-suite",
            "--manifest",
            manifest_path.to_str().unwrap(),
            "--candidate",
            "balanced-threshold",
            "--output-dir",
            report_dir.path().to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(
        report_dir
            .path()
            .join("section4-suite-report.json")
            .exists()
    );
}

#[test]
fn regression_section4_suite_rejects_unsafe_profile_ids() {
    let output_dir = tempdir().unwrap();
    let spec = section4_suite_spec(vec![strict_synthetic_profile("../escape", "unsafe", 12)]);

    let result = generate_section4_suite_assets(&spec, output_dir.path());

    assert!(
        matches!(result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("profile_id"))
    );
}

#[test]
fn regression_section4_suite_rejects_duplicate_profile_ids_and_empty_ids() {
    let output_dir = tempdir().unwrap();
    let mut duplicate = strict_synthetic_profile("duplicate-id", "corpus-a", 12);
    duplicate.scale_tier_id = "n-12".into();
    let spec = section4_suite_spec(vec![
        strict_synthetic_profile("duplicate-id", "corpus-b", 12),
        duplicate,
    ]);

    let duplicate_result = generate_section4_suite_assets(&spec, output_dir.path());

    assert!(
        matches!(duplicate_result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("duplicate profile_id"))
    );

    let output_dir = tempdir().unwrap();
    let mut empty_fields = strict_synthetic_profile("valid-id", "corpus-a", 12);
    empty_fields.corpus_id = "   ".into();
    let empty_corpus_result =
        generate_section4_suite_assets(&section4_suite_spec(vec![empty_fields]), output_dir.path());
    assert!(
        matches!(empty_corpus_result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("non-empty corpus_id"))
    );

    let output_dir = tempdir().unwrap();
    let mut empty_tier = strict_synthetic_profile("valid-id", "corpus-a", 12);
    empty_tier.scale_tier_id = "".into();
    let empty_tier_result =
        generate_section4_suite_assets(&section4_suite_spec(vec![empty_tier]), output_dir.path());
    assert!(
        matches!(empty_tier_result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("non-empty scale_tier_id"))
    );

    let output_dir = tempdir().unwrap();
    let invalid_chars_result = generate_section4_suite_assets(
        &section4_suite_spec(vec![strict_synthetic_profile("bad:name", "corpus-a", 12)]),
        output_dir.path(),
    );
    assert!(
        matches!(invalid_chars_result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("portable pattern"))
    );
}

#[test]
fn regression_section4_suite_rejects_unsafe_profile_ids_in_manifests() {
    let asset_dir = tempdir().unwrap();
    let report_dir = tempdir().unwrap();
    let spec = section4_suite_spec(vec![strict_synthetic_profile("safe-id", "safe-corpus", 12)]);
    let mut manifest = generate_section4_suite_assets(&spec, asset_dir.path()).unwrap();
    manifest.generated_profiles[0].profile_id = "..\\escape".into();

    let result = run_section4_suite(
        &manifest,
        &balanced_and_skewed_candidates()[..1],
        report_dir.path(),
    );

    assert!(
        matches!(result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("profile_id"))
    );
}

#[test]
fn regression_section4_suite_rejects_zero_evaluated_entity_count_in_manifests() {
    let asset_dir = tempdir().unwrap();
    let report_dir = tempdir().unwrap();
    let spec = section4_suite_spec(vec![strict_synthetic_profile("safe-id", "safe-corpus", 12)]);
    let mut manifest = generate_section4_suite_assets(&spec, asset_dir.path()).unwrap();
    manifest.generated_profiles[0].evaluated_entity_count = 0;

    let result = run_section4_suite(
        &manifest,
        &balanced_and_skewed_candidates()[..1],
        report_dir.path(),
    );

    assert!(
        matches!(result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("evaluated_entity_count"))
    );
}

#[test]
fn regression_section4_suite_rejects_bruteforce_ground_truth_on_large_corpora() {
    let output_dir = tempdir().unwrap();
    let spec = section4_suite_spec(vec![strict_synthetic_profile(
        "large-ground-truth",
        "large-corpus",
        8_194,
    )]);

    let result = generate_section4_suite_assets(&spec, output_dir.path());

    assert!(
        matches!(result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("brute-force exact neighbors"))
    );
}

#[test]
fn regression_section4_suite_rejects_empty_suite_and_zero_controls() {
    let output_dir = tempdir().unwrap();
    let mut empty_suite_id =
        section4_suite_spec(vec![strict_synthetic_profile("valid-id", "corpus-a", 12)]);
    empty_suite_id.suite_id = "   ".into();
    let empty_suite_result = generate_section4_suite_assets(&empty_suite_id, output_dir.path());
    assert!(
        matches!(empty_suite_result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("suite_id"))
    );

    let output_dir = tempdir().unwrap();
    let mut zero_leaf_size =
        section4_suite_spec(vec![strict_synthetic_profile("valid-id", "corpus-a", 12)]);
    zero_leaf_size.leaf_size = 0;
    let zero_leaf_result = generate_section4_suite_assets(&zero_leaf_size, output_dir.path());
    assert!(
        matches!(zero_leaf_result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("leaf_size"))
    );

    let output_dir = tempdir().unwrap();
    let mut zero_dimensions =
        section4_suite_spec(vec![strict_synthetic_profile("valid-id", "corpus-a", 12)]);
    zero_dimensions.dimensions = 0;
    let zero_dimensions_result =
        generate_section4_suite_assets(&zero_dimensions, output_dir.path());
    assert!(
        matches!(zero_dimensions_result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("dimensions"))
    );

    let output_dir = tempdir().unwrap();
    let mut zero_batch_size =
        section4_suite_spec(vec![strict_synthetic_profile("valid-id", "corpus-a", 12)]);
    zero_batch_size.batch_size = 0;
    let zero_batch_result = generate_section4_suite_assets(&zero_batch_size, output_dir.path());
    assert!(
        matches!(zero_batch_result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("batch_size"))
    );

    let output_dir = tempdir().unwrap();
    let mut zero_neighbor_count =
        section4_suite_spec(vec![strict_synthetic_profile("valid-id", "corpus-a", 12)]);
    zero_neighbor_count.neighbor_count = 0;
    let zero_neighbor_result =
        generate_section4_suite_assets(&zero_neighbor_count, output_dir.path());
    assert!(
        matches!(zero_neighbor_result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("neighbor_count"))
    );

    let output_dir = tempdir().unwrap();
    let empty_profiles_result =
        generate_section4_suite_assets(&section4_suite_spec(vec![]), output_dir.path());
    assert!(
        matches!(empty_profiles_result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("at least one profile"))
    );
}

#[test]
fn regression_section4_suite_rejects_invalid_alignment_policy_preconditions() {
    let output_dir = tempdir().unwrap();
    let strict_result = generate_section4_suite_assets(
        &section4_suite_spec(vec![strict_synthetic_profile(
            "strict-bad",
            "strict-bad",
            11,
        )]),
        output_dir.path(),
    );
    assert!(
        matches!(strict_result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("not divisible by leaf_size"))
    );

    let output_dir = tempdir().unwrap();
    let padding_result = generate_section4_suite_assets(
        &section4_suite_spec(vec![padding_synthetic_profile(
            "padding-bad",
            "padding-bad",
            12,
        )]),
        output_dir.path(),
    );
    assert!(
        matches!(padding_result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("already divisible by leaf_size"))
    );
}

#[test]
fn regression_section4_suite_rejects_too_small_ground_truth_corpora() {
    let output_dir = tempdir().unwrap();
    let result = generate_section4_suite_assets(
        &section4_suite_spec(vec![strict_synthetic_profile("too-small", "small", 10)]),
        output_dir.path(),
    );

    assert!(
        matches!(result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("more than 10 real entities"))
    );
}

#[test]
fn regression_section4_suite_rejects_zero_norm_cosine_ground_truth_inputs() {
    let output_dir = tempdir().unwrap();
    let source = harvested_fixture_reference(
        "zero-norm-cosine",
        (0..12).map(|index| HarvestedFixtureRecord {
            entity_id_metadata: Some(CborValue::Text(format!("entity-{index:02}"))),
            synthetic_metadata: Some(CborValue::Bool(false)),
            embedding: if index == 0 {
                vec![0.0, 0.0]
            } else {
                vec![index as f32, 1.0]
            },
        }),
    );
    let mut spec = section4_suite_spec(vec![harvested_profile(
        "cosine-zero-norm",
        "cosine-zero-norm",
        source,
        12,
        AlignmentPolicy::StrictAlignment,
    )]);
    spec.metric_contract = Section4MetricContract::Cosine;

    let result = generate_section4_suite_assets(&spec, output_dir.path());

    assert!(
        matches!(result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("zero-norm embeddings"))
    );
}

#[test]
fn regression_section4_suite_rejects_malformed_harvested_metadata() {
    let output_dir = tempdir().unwrap();
    let missing_entity_id_result = generate_section4_suite_assets(
        &section4_suite_spec(vec![harvested_profile(
            "missing-entity-id",
            "harvested-missing-entity-id",
            harvested_fixture_reference(
                "missing-entity-id-source",
                (0..11).map(|index| HarvestedFixtureRecord {
                    entity_id_metadata: (index != 0)
                        .then(|| CborValue::Text(format!("entity-{index:02}"))),
                    synthetic_metadata: Some(CborValue::Bool(false)),
                    embedding: vec![index as f32 + 1.0, 1.0],
                }),
            ),
            11,
            AlignmentPolicy::StrictAlignment,
        )]),
        output_dir.path(),
    );
    assert!(
        matches!(missing_entity_id_result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("was missing"))
    );

    let output_dir = tempdir().unwrap();
    let non_text_entity_id_result = generate_section4_suite_assets(
        &section4_suite_spec(vec![harvested_profile(
            "non-text-entity-id",
            "harvested-non-text-entity-id",
            harvested_fixture_reference(
                "non-text-entity-id-source",
                (0..11).map(|index| HarvestedFixtureRecord {
                    entity_id_metadata: Some(if index == 0 {
                        CborValue::Bool(true)
                    } else {
                        CborValue::Text(format!("entity-{index:02}"))
                    }),
                    synthetic_metadata: Some(CborValue::Bool(false)),
                    embedding: vec![index as f32 + 1.0, 1.0],
                }),
            ),
            11,
            AlignmentPolicy::StrictAlignment,
        )]),
        output_dir.path(),
    );
    assert!(
        matches!(non_text_entity_id_result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("must be text"))
    );

    let output_dir = tempdir().unwrap();
    let invalid_synthetic_result = generate_section4_suite_assets(
        &section4_suite_spec(vec![harvested_profile(
            "invalid-synthetic",
            "harvested-invalid-synthetic",
            harvested_fixture_reference(
                "invalid-synthetic-source",
                (0..11).map(|index| HarvestedFixtureRecord {
                    entity_id_metadata: Some(CborValue::Text(format!("entity-{index:02}"))),
                    synthetic_metadata: Some(if index == 0 {
                        CborValue::Text("not-bool".into())
                    } else {
                        CborValue::Bool(false)
                    }),
                    embedding: vec![index as f32 + 1.0, 1.0],
                }),
            ),
            11,
            AlignmentPolicy::StrictAlignment,
        )]),
        output_dir.path(),
    );
    assert!(
        matches!(invalid_synthetic_result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("must be boolean"))
    );
}

#[test]
fn regression_section4_suite_rejects_invalid_harvested_embeddings_and_underfilled_sources() {
    let output_dir = tempdir().unwrap();
    let invalid_embedding_result = generate_section4_suite_assets(
        &section4_suite_spec(vec![harvested_profile(
            "invalid-embedding",
            "harvested-invalid-embedding",
            harvested_fixture_reference(
                "invalid-embedding-source",
                (0..12).map(|index| HarvestedFixtureRecord {
                    entity_id_metadata: Some(CborValue::Text(format!("entity-{index:02}"))),
                    synthetic_metadata: Some(CborValue::Bool(false)),
                    embedding: if index == 0 {
                        vec![f32::NAN, 1.0]
                    } else {
                        vec![index as f32 + 1.0, 1.0]
                    },
                }),
            ),
            12,
            AlignmentPolicy::StrictAlignment,
        )]),
        output_dir.path(),
    );
    assert!(
        matches!(invalid_embedding_result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("failed embedding validation"))
    );

    let output_dir = tempdir().unwrap();
    let underfilled_result = generate_section4_suite_assets(
        &section4_suite_spec(vec![harvested_profile(
            "underfilled-harvest",
            "harvested-underfilled",
            harvested_fixture_reference(
                "underfilled-source",
                (0..11).map(|index| HarvestedFixtureRecord {
                    entity_id_metadata: Some(CborValue::Text(format!("entity-{index:02}"))),
                    synthetic_metadata: Some(CborValue::Bool(true)),
                    embedding: vec![index as f32 + 1.0, 1.0],
                }),
            ),
            11,
            AlignmentPolicy::StrictAlignment,
        )]),
        output_dir.path(),
    );
    assert!(
        matches!(underfilled_result, Err(EvaluatorError::InvalidConfiguration(message)) if message.contains("contains only 0 real entities"))
    );
}
