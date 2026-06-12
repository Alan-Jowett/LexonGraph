// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use lexongraph_streaming_clustering_evaluator::{
    BenchmarkProfile, EvaluatorError, Section4SuiteManifest, Section4SuiteSpec,
    built_in_fixture_candidate_names, emit_campaign_artifacts, generate_section4_suite_assets,
    resolve_profile_block_store_paths, resolve_registered_candidates,
    resolve_section4_suite_manifest_paths, resolve_section4_suite_spec_paths,
    run_evaluation_campaign, run_section4_suite, write_campaign_artifacts,
    write_section4_suite_artifacts,
};

#[derive(Parser, Debug)]
#[command(version, about = "Run the LexonGraph streaming clustering evaluator")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// List the built-in fixture candidates that this executable can run.
    ListFixtureCandidates,
    /// Run one benchmark profile against one or more registered candidates.
    Run {
        #[arg(long, value_name = "PATH")]
        profile: PathBuf,
        #[arg(long = "candidate", value_name = "NAME", required = true)]
        candidates: Vec<String>,
        #[arg(long, value_name = "PATH")]
        output_dir: PathBuf,
    },
    /// Generate section-4 benchmark assets and profile JSON files.
    GenerateSection4Assets {
        #[arg(long, value_name = "PATH")]
        suite: PathBuf,
        #[arg(long, value_name = "PATH")]
        output_dir: PathBuf,
    },
    /// Run a generated section-4 benchmark suite against one or more candidates.
    RunSection4Suite {
        #[arg(long, value_name = "PATH")]
        manifest: PathBuf,
        #[arg(long = "candidate", value_name = "NAME", required = true)]
        candidates: Vec<String>,
        #[arg(long, value_name = "PATH")]
        output_dir: PathBuf,
    },
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), EvaluatorError> {
    match Cli::parse().command {
        Command::ListFixtureCandidates => {
            for candidate in built_in_fixture_candidate_names() {
                println!("{candidate}");
            }
            Ok(())
        }
        Command::Run {
            profile,
            candidates,
            output_dir,
        } => {
            let profile_path = profile;
            let profile = std::fs::read_to_string(&profile_path).map_err(|error| {
                EvaluatorError::Io(format!(
                    "failed to read benchmark profile {}: {error}",
                    profile_path.display()
                ))
            })?;
            let profile: BenchmarkProfile = serde_json::from_str(&profile).map_err(|error| {
                EvaluatorError::Json(format!(
                    "failed to parse benchmark profile {}: {error}",
                    profile_path.display()
                ))
            })?;
            let mut profile = profile;
            if let Some(profile_dir) = profile_path.parent() {
                resolve_profile_block_store_paths(&mut profile, profile_dir);
            }

            let registered_candidates = resolve_registered_candidates(&candidates)?;
            let report = run_evaluation_campaign(&profile, &registered_candidates)?;
            let artifacts = emit_campaign_artifacts(&report)?;
            let paths = write_campaign_artifacts(&output_dir, &artifacts)?;
            for path in paths {
                println!("{}", path.display());
            }
            Ok(())
        }
        Command::GenerateSection4Assets { suite, output_dir } => {
            let suite_path = suite;
            let suite = std::fs::read_to_string(&suite_path).map_err(|error| {
                EvaluatorError::Io(format!(
                    "failed to read section-4 suite spec {}: {error}",
                    suite_path.display()
                ))
            })?;
            let suite: Section4SuiteSpec = serde_json::from_str(&suite).map_err(|error| {
                EvaluatorError::Json(format!(
                    "failed to parse section-4 suite spec {}: {error}",
                    suite_path.display()
                ))
            })?;
            let mut suite = suite;
            if let Some(suite_dir) = suite_path.parent() {
                resolve_section4_suite_spec_paths(&mut suite, suite_dir);
            }
            let manifest = generate_section4_suite_assets(&suite, &output_dir)?;
            let manifest_path = output_dir.join("section4-suite-manifest.json");
            println!("{}", manifest_path.display());
            for generated in manifest.generated_profiles {
                println!("{}", generated.profile_path.display());
                println!("{}", generated.corpus_archive_path.display());
            }
            Ok(())
        }
        Command::RunSection4Suite {
            manifest,
            candidates,
            output_dir,
        } => {
            let manifest_path = manifest;
            let manifest = std::fs::read_to_string(&manifest_path).map_err(|error| {
                EvaluatorError::Io(format!(
                    "failed to read section-4 suite manifest {}: {error}",
                    manifest_path.display()
                ))
            })?;
            let manifest: Section4SuiteManifest =
                serde_json::from_str(&manifest).map_err(|error| {
                    EvaluatorError::Json(format!(
                        "failed to parse section-4 suite manifest {}: {error}",
                        manifest_path.display()
                    ))
                })?;
            let mut manifest = manifest;
            if let Some(manifest_dir) = manifest_path.parent() {
                resolve_section4_suite_manifest_paths(&mut manifest, manifest_dir);
            }
            let candidates = resolve_registered_candidates(&candidates)?;
            let report = run_section4_suite(&manifest, &candidates, &output_dir)?;
            let artifacts = write_section4_suite_artifacts(&report, &output_dir)?;
            println!("{}", artifacts.suite_report_path.display());
            println!("{}", artifacts.scorecard_path.display());
            for path in artifacts.profile_output_dirs {
                println!("{}", path.display());
            }
            Ok(())
        }
    }
}
