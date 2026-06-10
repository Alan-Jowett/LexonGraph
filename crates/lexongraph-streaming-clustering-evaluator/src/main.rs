// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use lexongraph_streaming_clustering_evaluator::{
    BenchmarkProfile, EvaluatorError, built_in_fixture_candidate, built_in_fixture_candidate_names,
    emit_campaign_artifacts, run_evaluation_campaign, write_campaign_artifacts,
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
    /// Run one benchmark profile against one or more built-in fixture candidates.
    Run {
        #[arg(long, value_name = "PATH")]
        profile: PathBuf,
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
            let profile = std::fs::read_to_string(&profile)
                .map_err(|error| EvaluatorError::Io(error.to_string()))?;
            let profile: BenchmarkProfile = serde_json::from_str(&profile)
                .map_err(|error| EvaluatorError::Json(error.to_string()))?;

            let mut registered_candidates = Vec::with_capacity(candidates.len());
            for candidate_name in candidates {
                let Some(candidate) = built_in_fixture_candidate(&candidate_name) else {
                    return Err(EvaluatorError::InvalidConfiguration(format!(
                        "unknown built-in fixture candidate {candidate_name}; available: {}",
                        built_in_fixture_candidate_names().join(", ")
                    )));
                };
                registered_candidates.push(candidate);
            }

            let report = run_evaluation_campaign(&profile, &registered_candidates)?;
            let artifacts = emit_campaign_artifacts(&report)?;
            let paths = write_campaign_artifacts(&output_dir, &artifacts)?;
            for path in paths {
                println!("{}", path.display());
            }
            Ok(())
        }
    }
}
