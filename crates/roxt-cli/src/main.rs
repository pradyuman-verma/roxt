//! roxt — structured telemetry recorder and query tool for ROS 2 fleets.
//!
//! The binary is deliberately thin: argument parsing, wiring, and output
//! formatting live here; every behaviour worth testing lives in the
//! library crates. `anyhow` is permitted in this crate only — at the
//! process boundary, errors become messages and exit codes, not types.

mod commands;

use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "roxt",
    version,
    about = "Structured telemetry recorder and query tool for ROS 2 robot fleets"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Record telemetry from a source file into a roxt database.
    Rec(commands::rec::RecArgs),
    /// Query stored events with optional filters.
    Query(commands::query::QueryArgs),
    /// Replay stored events to stdout at recorded (or scaled) pace.
    Replay(commands::replay::ReplayArgs),
    /// Compare event timelines of two databases; exits 1 on divergence.
    Diff(commands::diff::DiffArgs),
}

fn main() -> ExitCode {
    // Operator-facing diagnostics go to stderr so stdout stays parseable
    // (json/csv output is consumed by scripts).
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Rec(args) => commands::rec::run(&args),
        Command::Query(args) => commands::query::run(&args),
        Command::Replay(args) => commands::replay::run(&args),
        Command::Diff(args) => commands::diff::run(&args),
    };
    match result {
        Ok(code) => code,
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}
