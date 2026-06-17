// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

#![allow(dead_code)]

use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use ciborium::value::Value as CborValue;
use lexongraph_block::{
    Block, BranchEntry, Content, EmbeddingSpec, LeafEntry, VERSION_1, build_branch_block,
    build_leaf_block,
};
use lexongraph_block_store::BlockStore;
use lexongraph_block_store_fs::FilesystemBlockStore;
use lexongraph_streaming_clustering_evaluator::{
    AlignmentPolicy, BenchmarkProfile, BlockStoreCorpusReference, BlockStoreEvaluationCorpus,
    BlockStoreReferenceStore, CompressionBenchmark, CompressionMethod,
    DEFAULT_DEFERRED_HIERARCHY_ROUTING_REASON, DeferredResearchGoal, EmbeddingWorkloadSource,
    EvaluationEntity, EvaluationEntitySource, ExecutionBudget, GateDeclaration, GateKind,
    LaterPhaseIdentity, LaterPhaseIdentityKind, MetricDeclaration, MetricKind, ProbeWorkload,
    RegisteredCandidate, ReproducibilityMetadata, ResearchCoverage, Section5DepthBoundPolicy,
    Section5EpsilonPolicy, Section5HierarchyContract, Section6SummaryContract,
    SharedCandidateConfig, TrainingPassSource, built_in_fixture_candidate,
    registered_packing_strategy_names,
};
use zip::CompressionMethod as ZipCompressionMethod;
use zip::ZipWriter;
use zip::write::SimpleFileOptions;

pub fn strict_alignment_profile() -> BenchmarkProfile {
    BenchmarkProfile {
        profile_id: "strict-alignment-campaign".into(),
        corpus_ids: vec!["fixture-corpus-a".into()],
        shared_candidate_config: SharedCandidateConfig {
            cluster_count: 2,
            dimensions: 2,
            balance_constraints: None,
            random_seed: Some(7),
        },
        training_passes: vec![
            TrainingPassSource::Inline {
                batches: vec![
                    vec![vec![0.0, 0.0], vec![0.3, 0.0]],
                    vec![vec![9.9, 0.0], vec![10.2, 0.0]],
                ],
            },
            TrainingPassSource::Inline {
                batches: vec![
                    vec![vec![0.0, 0.0], vec![0.3, 0.0]],
                    vec![vec![9.9, 0.0], vec![10.2, 0.0]],
                ],
            },
        ],
        probe_workloads: vec![ProbeWorkload {
            workload_id: "heldout-probes".into(),
            source: EmbeddingWorkloadSource::Inline {
                embeddings: vec![vec![0.15, 0.0], vec![10.05, 0.0]],
            },
        }],
        evaluation_entities: EvaluationEntitySource::Inline {
            entities: vec![
                EvaluationEntity {
                    entity_id: "a".into(),
                    corpus_id: "fixture-corpus-a".into(),
                    embedding: vec![0.0, 0.0],
                    synthetic: false,
                },
                EvaluationEntity {
                    entity_id: "b".into(),
                    corpus_id: "fixture-corpus-a".into(),
                    embedding: vec![0.3, 0.0],
                    synthetic: false,
                },
                EvaluationEntity {
                    entity_id: "c".into(),
                    corpus_id: "fixture-corpus-a".into(),
                    embedding: vec![9.9, 0.0],
                    synthetic: false,
                },
                EvaluationEntity {
                    entity_id: "d".into(),
                    corpus_id: "fixture-corpus-a".into(),
                    embedding: vec![10.2, 0.0],
                    synthetic: false,
                },
            ],
        },
        leaf_model: lexongraph_streaming_clustering_evaluator::LeafModel {
            leaf_size: 2,
            declared_final_cluster_count: 2,
            alignment_policy: AlignmentPolicy::StrictAlignment,
        },
        locality_ground_truth: vec![
            lexongraph_streaming_clustering_evaluator::GroundTruthNeighborhood {
                entity_id: "a".into(),
                neighbor_ids: vec!["b".into()],
            },
            lexongraph_streaming_clustering_evaluator::GroundTruthNeighborhood {
                entity_id: "b".into(),
                neighbor_ids: vec!["a".into()],
            },
            lexongraph_streaming_clustering_evaluator::GroundTruthNeighborhood {
                entity_id: "c".into(),
                neighbor_ids: vec!["d".into()],
            },
            lexongraph_streaming_clustering_evaluator::GroundTruthNeighborhood {
                entity_id: "d".into(),
                neighbor_ids: vec!["c".into()],
            },
        ],
        compression_benchmark: CompressionBenchmark {
            method: CompressionMethod::ScalarQuantization8Bit,
            global_baseline_label: "global-real-dataset-8bit".into(),
        },
        metric_declarations: vec![
            MetricDeclaration {
                metric_id: "same-leaf-neighborhood-coherence".into(),
                label: "Same-leaf neighborhood coherence".into(),
                kind: MetricKind::SameLeafNeighborhoodCoherence,
                coverage: ResearchCoverage::Direct,
                research_goal_ids: vec!["RG-LOCALITY".into()],
                ranking_weight: 1.0,
            },
            MetricDeclaration {
                metric_id: "local-compression-gain".into(),
                label: "Local compression gain".into(),
                kind: MetricKind::LocalCompressionGain,
                coverage: ResearchCoverage::Direct,
                research_goal_ids: vec!["RG-COMPRESSION".into()],
                ranking_weight: 0.25,
            },
        ],
        gate_declarations: vec![
            GateDeclaration {
                gate_id: "exact-leaf-occupancy".into(),
                label: "Exact leaf occupancy".into(),
                kind: GateKind::ExactLeafOccupancy,
                coverage: ResearchCoverage::Direct,
                research_goal_ids: vec!["RG-FIXED-LEAF-SIZE".into()],
            },
            GateDeclaration {
                gate_id: "leaf-size-lower-bound".into(),
                label: "Leaf size lower bound".into(),
                kind: GateKind::LeafSizeAtLeast { minimum: 1 },
                coverage: ResearchCoverage::Direct,
                research_goal_ids: vec!["RG-FIXED-LEAF-SIZE".into()],
            },
            GateDeclaration {
                gate_id: "leaf-size-upper-bound".into(),
                label: "Leaf size upper bound".into(),
                kind: GateKind::LeafSizeAtMost { maximum: 2 },
                coverage: ResearchCoverage::Direct,
                research_goal_ids: vec!["RG-FIXED-LEAF-SIZE".into()],
            },
            GateDeclaration {
                gate_id: "complete-coverage".into(),
                label: "Complete coverage".into(),
                kind: GateKind::CompleteCoverage,
                coverage: ResearchCoverage::Direct,
                research_goal_ids: vec!["RG-COVERAGE".into()],
            },
            GateDeclaration {
                gate_id: "one-cluster-per-entity".into(),
                label: "One cluster per entity".into(),
                kind: GateKind::OneClusterPerEntity,
                coverage: ResearchCoverage::Direct,
                research_goal_ids: vec!["RG-COVERAGE".into()],
            },
            GateDeclaration {
                gate_id: "no-empty-declared-clusters".into(),
                label: "No empty declared clusters".into(),
                kind: GateKind::NoEmptyDeclaredClusters,
                coverage: ResearchCoverage::Direct,
                research_goal_ids: vec!["RG-FIXED-LEAF-SIZE".into()],
            },
            GateDeclaration {
                gate_id: "deterministic-observable-results".into(),
                label: "Deterministic observable results".into(),
                kind: GateKind::DeterministicObservableResults,
                coverage: ResearchCoverage::Direct,
                research_goal_ids: vec!["RG-DETERMINISM".into()],
            },
            GateDeclaration {
                gate_id: "same-leaf-coherence-threshold".into(),
                label: "Same-leaf coherence threshold".into(),
                kind: GateKind::MetricAtLeast {
                    metric_id: "same-leaf-neighborhood-coherence".into(),
                    minimum: 0.75,
                },
                coverage: ResearchCoverage::Direct,
                research_goal_ids: vec!["RG-LOCALITY".into()],
            },
            GateDeclaration {
                gate_id: "compression-gain-threshold".into(),
                label: "Compression gain threshold".into(),
                kind: GateKind::MetricAtLeast {
                    metric_id: "local-compression-gain".into(),
                    minimum: 0.05,
                },
                coverage: ResearchCoverage::Direct,
                research_goal_ids: vec!["RG-COMPRESSION".into()],
            },
        ],
        packing_strategy_ids: registered_packing_strategy_names(),
        deferred_research_goals: vec![DeferredResearchGoal {
            deferred_id: "deferred-hierarchy-routing".into(),
            label: "Hierarchy routing proof".into(),
            reason: DEFAULT_DEFERRED_HIERARCHY_ROUTING_REASON.into(),
            research_goal_ids: vec!["RG-HIERARCHY".into(), "RG-ROUTING".into()],
            coverage: ResearchCoverage::Deferred,
            later_evaluation_line: "future hierarchy-routing evaluator".into(),
        }],
        later_phase_identities: vec![LaterPhaseIdentity {
            identity_id: "fixture-heldout-query-set".into(),
            label: "Fixture held-out query set".into(),
            kind: LaterPhaseIdentityKind::HeldOutQuerySet,
            corpus_id: Some("fixture-corpus-a".into()),
            scale_tier_id: None,
            asset_path: Some(PathBuf::from("fixtures/heldout-queries.zip")),
            later_evaluation_line: "future hierarchy-routing evaluator".into(),
        }],
        reproducibility: ReproducibilityMetadata {
            seed_policy: "fixed-seed-7".into(),
            software_identity: "fixture-campaign-builder".into(),
            floating_point_profile: "ieee754-deterministic-no-fma".into(),
            hardware_profile: "fixture-cpu".into(),
            candidate_threading_model: "host-scaled deterministic candidate execution".into(),
            reduction_order_strategy: "deterministic stable input-order reduction".into(),
        },
    }
}

pub fn synthetic_padding_profile() -> BenchmarkProfile {
    let mut profile = strict_alignment_profile();
    profile.profile_id = "synthetic-padding-campaign".into();
    *profile
        .inline_evaluation_entities_mut()
        .expect("synthetic padding fixture should use inline entities") = vec![
        EvaluationEntity {
            entity_id: "a".into(),
            corpus_id: "fixture-corpus-a".into(),
            embedding: vec![0.0, 0.0],
            synthetic: false,
        },
        EvaluationEntity {
            entity_id: "b".into(),
            corpus_id: "fixture-corpus-a".into(),
            embedding: vec![0.3, 0.0],
            synthetic: false,
        },
        EvaluationEntity {
            entity_id: "c".into(),
            corpus_id: "fixture-corpus-a".into(),
            embedding: vec![9.9, 0.0],
            synthetic: false,
        },
        EvaluationEntity {
            entity_id: "pad-1".into(),
            corpus_id: "fixture-corpus-a".into(),
            embedding: vec![10.2, 0.0],
            synthetic: true,
        },
    ];
    profile.leaf_model.alignment_policy = AlignmentPolicy::DeterministicSyntheticPadding;
    profile.locality_ground_truth = vec![
        lexongraph_streaming_clustering_evaluator::GroundTruthNeighborhood {
            entity_id: "a".into(),
            neighbor_ids: vec!["b".into()],
        },
        lexongraph_streaming_clustering_evaluator::GroundTruthNeighborhood {
            entity_id: "b".into(),
            neighbor_ids: vec!["a".into()],
        },
    ];
    profile
}

pub fn block_store_backed_profile() -> BenchmarkProfile {
    let profile = strict_alignment_profile();
    let store_root = unique_store_root("streaming-clustering-evaluator");
    let store = FilesystemBlockStore::new(&store_root).unwrap();

    let shared_corpus = write_corpus(
        &store,
        &store_root,
        "shared-corpus",
        &[
            StoredEntity {
                entity_id: Some("a"),
                embedding: vec![0.0, 0.0],
                synthetic: false,
            },
            StoredEntity {
                entity_id: Some("b"),
                embedding: vec![0.3, 0.0],
                synthetic: false,
            },
            StoredEntity {
                entity_id: Some("c"),
                embedding: vec![9.9, 0.0],
                synthetic: false,
            },
            StoredEntity {
                entity_id: Some("d"),
                embedding: vec![10.2, 0.0],
                synthetic: false,
            },
        ],
    );
    let probe_source = write_corpus(
        &store,
        &store_root,
        "probe-corpus",
        &[
            StoredEntity {
                entity_id: Some("probe-a"),
                embedding: vec![0.15, 0.0],
                synthetic: false,
            },
            StoredEntity {
                entity_id: Some("probe-b"),
                embedding: vec![10.05, 0.0],
                synthetic: false,
            },
        ],
    );

    BenchmarkProfile {
        training_passes: vec![
            TrainingPassSource::BlockStore {
                corpus: BlockStoreCorpusReference {
                    source_id: "training-pass-1".into(),
                    ..shared_corpus.clone()
                },
                batch_size: 2,
            },
            TrainingPassSource::BlockStore {
                corpus: BlockStoreCorpusReference {
                    source_id: "training-pass-2".into(),
                    ..shared_corpus.clone()
                },
                batch_size: 2,
            },
        ],
        probe_workloads: vec![ProbeWorkload {
            workload_id: "heldout-probes".into(),
            source: EmbeddingWorkloadSource::BlockStore {
                corpus: probe_source.clone(),
            },
        }],
        evaluation_entities: EvaluationEntitySource::BlockStore {
            corpora: vec![BlockStoreEvaluationCorpus {
                corpus_id: "fixture-corpus-a".into(),
                corpus: BlockStoreCorpusReference {
                    source_id: "evaluation-corpus".into(),
                    ..shared_corpus
                },
                entity_id_metadata_key: "entity_id".into(),
                synthetic_metadata_key: Some("synthetic".into()),
            }],
        },
        ..profile
    }
}

pub fn archive_backed_profile() -> BenchmarkProfile {
    let mut profile = block_store_backed_profile();
    let mut archive_by_store_root = std::collections::BTreeMap::<PathBuf, PathBuf>::new();

    for pass in &mut profile.training_passes {
        if let TrainingPassSource::BlockStore { corpus, .. } = pass {
            *corpus = archive_backed_reference(corpus, &mut archive_by_store_root);
        }
    }
    for workload in &mut profile.probe_workloads {
        if let EmbeddingWorkloadSource::BlockStore { corpus } = &mut workload.source {
            *corpus = archive_backed_reference(corpus, &mut archive_by_store_root);
        }
    }
    let EvaluationEntitySource::BlockStore { corpora } = &mut profile.evaluation_entities else {
        panic!("archive-backed fixture should start from block-store evaluation entities");
    };
    for corpus in corpora {
        corpus.corpus = archive_backed_reference(&corpus.corpus, &mut archive_by_store_root);
    }

    profile
}

pub fn broken_block_store_profile() -> BenchmarkProfile {
    let mut profile = block_store_backed_profile();
    profile.training_passes = vec![TrainingPassSource::BlockStore {
        corpus: BlockStoreCorpusReference {
            source_id: "missing-training-source".into(),
            root_block_id: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                .into(),
            store: BlockStoreReferenceStore::Filesystem {
                store_root: unique_store_root("streaming-clustering-evaluator-missing"),
            },
        },
        batch_size: 2,
    }];
    profile
}

pub fn broken_archive_backed_profile() -> BenchmarkProfile {
    let mut profile = archive_backed_profile();
    profile.training_passes = vec![TrainingPassSource::BlockStore {
        corpus: BlockStoreCorpusReference {
            source_id: "missing-archive-source".into(),
            root_block_id: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                .into(),
            store: BlockStoreReferenceStore::ZipArchive {
                archive_path: unique_store_root("streaming-clustering-evaluator-missing-archive")
                    .join("missing-corpus.zip"),
            },
        },
        batch_size: 2,
    }];
    profile
}

pub fn corrupt_archive_backed_profile() -> BenchmarkProfile {
    let mut profile = archive_backed_profile();
    let corrupt_archive_path =
        unique_store_root("streaming-clustering-evaluator-corrupt-archive").join("corrupt.zip");
    std::fs::write(&corrupt_archive_path, b"not a zip archive").unwrap();
    profile.training_passes = vec![TrainingPassSource::BlockStore {
        corpus: BlockStoreCorpusReference {
            source_id: "corrupt-archive-source".into(),
            root_block_id: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                .into(),
            store: BlockStoreReferenceStore::ZipArchive {
                archive_path: corrupt_archive_path,
            },
        },
        batch_size: 2,
    }];
    profile
}

pub fn duplicate_source_id_profile() -> BenchmarkProfile {
    let mut profile = block_store_backed_profile();
    let duplicate = match &profile.training_passes[0] {
        TrainingPassSource::BlockStore { corpus, .. } => corpus.clone(),
        TrainingPassSource::Inline { .. } => panic!("expected block-store training pass"),
    };
    profile.probe_workloads = vec![ProbeWorkload {
        workload_id: "heldout-probes".into(),
        source: EmbeddingWorkloadSource::BlockStore { corpus: duplicate },
    }];
    profile
}

pub fn empty_synthetic_metadata_key_profile() -> BenchmarkProfile {
    let mut profile = block_store_backed_profile();
    let EvaluationEntitySource::BlockStore { corpora } = &mut profile.evaluation_entities else {
        panic!("expected block-store evaluation corpus");
    };
    corpora[0].synthetic_metadata_key = Some("   ".into());
    profile
}

pub fn missing_synthetic_metadata_key_profile() -> BenchmarkProfile {
    let mut profile = block_store_backed_profile();
    profile.leaf_model.alignment_policy = AlignmentPolicy::DeterministicSyntheticPadding;
    let EvaluationEntitySource::BlockStore { corpora } = &mut profile.evaluation_entities else {
        panic!("expected block-store evaluation corpus");
    };
    corpora[0].synthetic_metadata_key = None;
    profile
}

pub fn duplicate_evaluation_entities_block_store_profile() -> BenchmarkProfile {
    let mut profile = block_store_backed_profile();
    let EvaluationEntitySource::BlockStore { corpora } = &mut profile.evaluation_entities else {
        panic!("expected block-store evaluation corpus");
    };
    let mut duplicate = corpora[0].clone();
    duplicate.corpus.source_id = "evaluation-corpus-duplicate".into();
    corpora.push(duplicate);
    profile
}

pub fn wrong_entity_count_block_store_profile() -> BenchmarkProfile {
    let mut profile = block_store_backed_profile();
    profile.leaf_model.leaf_size = 3;
    profile
}

pub fn invalid_profile() -> BenchmarkProfile {
    let mut profile = strict_alignment_profile();
    profile
        .inline_evaluation_entities_mut()
        .expect("invalid profile fixture should use inline entities")
        .pop();
    profile
}

pub fn balanced_and_skewed_candidates() -> Vec<RegisteredCandidate> {
    vec![
        built_in_fixture_candidate("balanced-threshold").unwrap(),
        built_in_fixture_candidate("skewed-gate-fail").unwrap(),
    ]
}

pub fn shared_contract_failure_candidate() -> RegisteredCandidate {
    built_in_fixture_candidate("shared-contract-failure").unwrap()
}

pub fn nondeterministic_candidate() -> RegisteredCandidate {
    built_in_fixture_candidate("nondeterministic-probe").unwrap()
}

pub fn section5_hierarchy_contract() -> Section5HierarchyContract {
    Section5HierarchyContract {
        contract_id: "section5-fixture-contract".into(),
        fanout_min: 2,
        fanout_max: 2,
        depth_bound_policy: Section5DepthBoundPolicy::CeilLogByMinFanout,
        metric_semantics_profile: "euclidean".into(),
        grouping_functional: "euclidean-centroid-distance".into(),
        dispersion_functional: "mean-squared-radius".into(),
        metric_compatibility_rule: "closed-profile-v1".into(),
        beta_threshold: 1.25,
        epsilon_policy: Section5EpsilonPolicy {
            parent_to_root_dispersion_ratio_max: 0.01,
        },
        section4_source_label: "fixture-leaf-stage-profile".into(),
        later_evaluation_line: "future parent-summary and routing evaluator".into(),
        execution_budget: Some(ExecutionBudget {
            wall_clock_limit_millis: 60_000,
        }),
    }
}

pub fn section5_cosine_hierarchy_contract() -> Section5HierarchyContract {
    let mut contract = section5_hierarchy_contract();
    contract.contract_id = "section5-cosine-fixture-contract".into();
    contract.metric_semantics_profile = "cosine".into();
    contract.grouping_functional = "cosine-centroid-distance".into();
    contract.dispersion_functional = "mean-cosine-deviation".into();
    contract
}

pub fn strict_alignment_nonzero_profile() -> BenchmarkProfile {
    let mut profile = strict_alignment_profile();
    profile.profile_id = "strict-alignment-nonzero-campaign".into();
    *profile
        .inline_evaluation_entities_mut()
        .expect("nonzero section-5 fixture should use inline entities") = vec![
        EvaluationEntity {
            entity_id: "a".into(),
            corpus_id: "fixture-corpus-a".into(),
            embedding: vec![1.0, 0.0],
            synthetic: false,
        },
        EvaluationEntity {
            entity_id: "b".into(),
            corpus_id: "fixture-corpus-a".into(),
            embedding: vec![0.9, 0.1],
            synthetic: false,
        },
        EvaluationEntity {
            entity_id: "c".into(),
            corpus_id: "fixture-corpus-a".into(),
            embedding: vec![0.0, 1.0],
            synthetic: false,
        },
        EvaluationEntity {
            entity_id: "d".into(),
            corpus_id: "fixture-corpus-a".into(),
            embedding: vec![0.1, 0.9],
            synthetic: false,
        },
    ];
    profile.locality_ground_truth = vec![
        lexongraph_streaming_clustering_evaluator::GroundTruthNeighborhood {
            entity_id: "a".into(),
            neighbor_ids: vec!["b".into()],
        },
        lexongraph_streaming_clustering_evaluator::GroundTruthNeighborhood {
            entity_id: "b".into(),
            neighbor_ids: vec!["a".into()],
        },
        lexongraph_streaming_clustering_evaluator::GroundTruthNeighborhood {
            entity_id: "c".into(),
            neighbor_ids: vec!["d".into()],
        },
        lexongraph_streaming_clustering_evaluator::GroundTruthNeighborhood {
            entity_id: "d".into(),
            neighbor_ids: vec!["c".into()],
        },
    ];
    profile.probe_workloads[0].source = EmbeddingWorkloadSource::Inline {
        embeddings: vec![vec![0.95, 0.05], vec![0.05, 0.95]],
    };
    profile
}

pub fn section6_summary_contract() -> Section6SummaryContract {
    Section6SummaryContract {
        contract_id: "section6-fixture-contract".into(),
        section5_source_label: "fixture-section5-carry-forward".into(),
        exact_reference_semantics: "descendant-exact-summary-v1".into(),
        delta_floor: 1.0e-6,
        perturbation_scale: 1.0e-3,
        storage_measurement_semantics: "f32-slot-count-v1".into(),
        metric_compatibility_rule: "closed-profile-v1".into(),
        relative_error_bound_max: Some(0.01),
        later_evaluation_line: "future routing evaluator".into(),
        execution_budget: Some(ExecutionBudget {
            wall_clock_limit_millis: 60_000,
        }),
    }
}

pub fn crate_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
}

pub fn lib_source() -> String {
    std::fs::read_to_string(crate_root().join("src").join("lib.rs")).unwrap()
}

#[derive(Clone)]
struct StoredEntity<'a> {
    entity_id: Option<&'a str>,
    embedding: Vec<f32>,
    synthetic: bool,
}

fn unique_store_root(prefix: &str) -> PathBuf {
    static NEXT_UNIQUE_SUFFIX: AtomicU64 = AtomicU64::new(0);
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = NEXT_UNIQUE_SUFFIX.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("{prefix}-{unique}-{counter}"));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn write_corpus(
    store: &FilesystemBlockStore,
    store_root: &Path,
    source_id: &str,
    entities: &[StoredEntity<'_>],
) -> BlockStoreCorpusReference {
    let spec = embedding_spec();
    let mut leaves = Vec::with_capacity(entities.len());
    for entity in entities {
        let leaf = build_leaf_block(
            VERSION_1,
            spec.clone(),
            vec![LeafEntry {
                embedding: encode_embedding(&entity.embedding),
                metadata: vec![
                    (
                        CborValue::Text("entity_id".into()),
                        CborValue::Text(entity.entity_id.unwrap_or("entity").into()),
                    ),
                    (
                        CborValue::Text("synthetic".into()),
                        CborValue::Bool(entity.synthetic),
                    ),
                ],
                content: Content {
                    media_type: "application/octet-stream".into(),
                    body: Vec::new(),
                },
            }],
            None,
        )
        .unwrap();
        let block = Block::Leaf(leaf);
        let block_id = store.put(&block).unwrap();
        leaves.push((block_id, encode_embedding(&entity.embedding)));
    }

    let root_block_id = if leaves.len() == 1 {
        leaves[0].0
    } else {
        let root = build_branch_block(
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
        .unwrap();
        store.put(&Block::Branch(root)).unwrap()
    };

    BlockStoreCorpusReference {
        source_id: source_id.into(),
        root_block_id: root_block_id.to_string(),
        store: BlockStoreReferenceStore::Filesystem {
            store_root: store_root.to_path_buf(),
        },
    }
}

fn archive_backed_reference(
    reference: &BlockStoreCorpusReference,
    archive_by_store_root: &mut std::collections::BTreeMap<PathBuf, PathBuf>,
) -> BlockStoreCorpusReference {
    let BlockStoreReferenceStore::Filesystem { store_root } = &reference.store else {
        panic!("archive-backed fixture expected filesystem-backed source");
    };
    let archive_path = archive_by_store_root
        .entry(store_root.clone())
        .or_insert_with(|| write_zip_archive_from_directory(store_root))
        .clone();
    BlockStoreCorpusReference {
        source_id: reference.source_id.clone(),
        root_block_id: reference.root_block_id.clone(),
        store: BlockStoreReferenceStore::ZipArchive { archive_path },
    }
}

fn write_zip_archive_from_directory(store_root: &Path) -> PathBuf {
    let archive_path =
        unique_store_root("streaming-clustering-evaluator-archive").join("corpus.zip");
    let file = File::create(&archive_path).unwrap();
    let mut zip = ZipWriter::new(file);
    write_directory_to_zip(store_root, store_root, &mut zip);
    zip.finish().unwrap();
    archive_path
}

fn write_directory_to_zip(root: &Path, directory: &Path, zip: &mut ZipWriter<File>) {
    let options = SimpleFileOptions::default().compression_method(ZipCompressionMethod::Stored);
    let mut entries = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    entries.sort();

    for path in entries {
        if path.is_dir() {
            write_directory_to_zip(root, &path, zip);
            continue;
        }

        let relative = path
            .strip_prefix(root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        zip.start_file(relative, options).unwrap();
        let mut file = File::open(&path).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        zip.write_all(&bytes).unwrap();
    }
}

fn embedding_spec() -> EmbeddingSpec {
    EmbeddingSpec {
        dims: 2,
        encoding: "f32le".into(),
    }
}

fn encode_embedding(values: &[f32]) -> Vec<u8> {
    values
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect()
}
