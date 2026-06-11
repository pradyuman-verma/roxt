//! `roxt query` — filtered, deterministic read of stored events.

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{bail, Context};
use clap::{Args, ValueEnum};
use roxt_query::{QueryFilter, StoredEvent};

mod format;

#[derive(Args)]
pub struct QueryArgs {
    /// roxt `SQLite` database to query.
    #[arg(long)]
    db: PathBuf,
    /// Restrict to one robot.
    #[arg(long)]
    robot_id: Option<String>,
    /// Restrict to one ROS 2 topic.
    #[arg(long)]
    topic: Option<String>,
    /// Inclusive lower bound: nanoseconds since epoch or RFC3339.
    #[arg(long)]
    from: Option<String>,
    /// Exclusive upper bound: nanoseconds since epoch or RFC3339.
    #[arg(long)]
    to: Option<String>,
    /// Output format.
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    format: OutputFormat,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum OutputFormat {
    Table,
    Json,
    Csv,
}

pub fn run(args: &QueryArgs) -> anyhow::Result<ExitCode> {
    let filter = build_filter(args)?;
    if !args.db.exists() {
        bail!("database {} does not exist", args.db.display());
    }
    let events = roxt_query::query_events(&args.db, &filter)
        .with_context(|| format!("query against {} failed", args.db.display()))?;
    print_events(&events, args.format);
    Ok(ExitCode::SUCCESS)
}

/// Parses and validates every argument before the database is touched.
fn build_filter(args: &QueryArgs) -> anyhow::Result<QueryFilter> {
    let from_ns = args
        .from
        .as_deref()
        .map(parse_timestamp)
        .transpose()
        .context("invalid --from")?;
    let to_ns = args
        .to
        .as_deref()
        .map(parse_timestamp)
        .transpose()
        .context("invalid --to")?;
    Ok(QueryFilter {
        robot_id: args.robot_id.clone(),
        topic: args.topic.clone(),
        from_ns,
        to_ns,
    })
}

/// Accepts raw nanoseconds since epoch ("1700000000000000000") or RFC3339
/// ("2023-11-14T22:13:20Z"). Two formats only — guessing beyond that risks
/// silently querying the wrong incident window.
fn parse_timestamp(text: &str) -> anyhow::Result<i64> {
    let looks_numeric = text
        .strip_prefix('-')
        .unwrap_or(text)
        .chars()
        .all(|c| c.is_ascii_digit());
    if looks_numeric && !text.is_empty() {
        return text
            .parse::<i64>()
            .with_context(|| format!("'{text}' is not a valid i64 nanosecond timestamp"));
    }
    let parsed = time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339)
        .with_context(|| format!("'{text}' is neither nanoseconds-since-epoch nor RFC3339"))?;
    i64::try_from(parsed.unix_timestamp_nanos())
        .with_context(|| format!("'{text}' is outside the representable nanosecond range"))
}

fn print_events(events: &[StoredEvent], format: OutputFormat) {
    match format {
        OutputFormat::Table => print!("{}", format::table(events)),
        OutputFormat::Json => print!("{}", format::json(events)),
        OutputFormat::Csv => print!("{}", format::csv(events)),
    }
}
