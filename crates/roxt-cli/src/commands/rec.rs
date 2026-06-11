//! `roxt rec` — ingest a recording into a roxt database.

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{bail, Context};
use clap::{Args, ValueEnum};
use roxt_core::TelemetrySource;
use roxt_ingest::{McapSource, Ros2BagSource};
use roxt_store::{SqliteStore, BATCH_SIZE};

#[derive(Args)]
pub struct RecArgs {
    /// Telemetry source format.
    #[arg(long, value_enum)]
    source: SourceKind,
    /// Input file (mcap), bag directory or .db3 file (ros2bag).
    #[arg(long)]
    input: PathBuf,
    /// Stable robot identifier, e.g. "amr-unit-042".
    #[arg(long)]
    robot_id: String,
    /// Session identifier — one per recording or live run.
    #[arg(long)]
    session_id: String,
    /// Output `SQLite` database (created if absent).
    #[arg(long)]
    out: PathBuf,
}

#[derive(Clone, Copy, ValueEnum)]
enum SourceKind {
    Mcap,
    Ros2bag,
    Zenoh,
}

pub fn run(args: &RecArgs) -> anyhow::Result<ExitCode> {
    validate(args)?;
    let mut source = open_source(args)?;
    let mut store = SqliteStore::open(&args.out)
        .with_context(|| format!("cannot open output database {}", args.out.display()))?;

    let mut batch: Vec<roxt_core::TelemetryEvent> = Vec::with_capacity(BATCH_SIZE);
    let mut total: usize = 0;
    loop {
        let event = source
            .next_event()
            .with_context(|| format!("ingest failed after {total} events were committed"))?;
        match event {
            Some(event) => {
                batch.push(event);
                if batch.len() == BATCH_SIZE {
                    total += store.insert_events(&batch)?;
                    batch.clear();
                }
            }
            None => break,
        }
    }
    total += store.insert_events(&batch)?;
    println!(
        "recorded {total} events from {} into {}",
        args.input.display(),
        args.out.display()
    );
    Ok(ExitCode::SUCCESS)
}

/// All argument checks happen here, before any file is opened.
fn validate(args: &RecArgs) -> anyhow::Result<()> {
    if args.robot_id.trim().is_empty() {
        bail!("--robot-id must not be empty");
    }
    if args.session_id.trim().is_empty() {
        bail!("--session-id must not be empty");
    }
    if !matches!(args.source, SourceKind::Zenoh) && !args.input.exists() {
        bail!("input {} does not exist", args.input.display());
    }
    Ok(())
}

fn open_source(args: &RecArgs) -> anyhow::Result<Box<dyn TelemetrySource>> {
    match args.source {
        SourceKind::Mcap => Ok(Box::new(McapSource::new(
            &args.input,
            &args.robot_id,
            &args.session_id,
        )?)),
        SourceKind::Ros2bag => Ok(Box::new(Ros2BagSource::new(
            &args.input,
            &args.robot_id,
            &args.session_id,
        )?)),
        // FUTURE(v2): see docs/zenoh-plan.md — live capture is feature-gated
        // and stubbed until then.
        SourceKind::Zenoh => bail!("zenoh live capture is planned for v2 and not yet available"),
    }
}
