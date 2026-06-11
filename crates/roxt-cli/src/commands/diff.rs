//! `roxt diff` — compare event timelines between two databases.
//!
//! Used to answer "did these two recordings of the same session capture
//! the same thing?" — e.g. a robot-local database versus the fleet
//! aggregate. Follows `diff(1)` convention: exit 0 when identical, 1 when
//! divergent, so it can gate scripts.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{bail, Context};
use clap::Args;
use roxt_core::TelemetryEvent;
use roxt_query::QueryFilter;

#[derive(Args)]
pub struct DiffArgs {
    /// First database.
    #[arg(long)]
    db_a: PathBuf,
    /// Second database.
    #[arg(long)]
    db_b: PathBuf,
    /// Restrict comparison to one robot.
    #[arg(long)]
    robot_id: Option<String>,
}

pub fn run(args: &DiffArgs) -> anyhow::Result<ExitCode> {
    for db in [&args.db_a, &args.db_b] {
        if !db.exists() {
            bail!("database {} does not exist", db.display());
        }
    }
    let filter = QueryFilter {
        robot_id: args.robot_id.clone(),
        ..QueryFilter::default()
    };
    let timeline_a = load_timeline(&args.db_a, &filter)?;
    let timeline_b = load_timeline(&args.db_b, &filter)?;

    match first_divergence(&timeline_a, &timeline_b) {
        None => {
            println!(
                "no divergence: {} events compared in both databases",
                timeline_a.len()
            );
            Ok(ExitCode::SUCCESS)
        }
        Some(report) => {
            println!("{report}");
            println!(
                "({} events in {}, {} events in {})",
                timeline_a.len(),
                args.db_a.display(),
                timeline_b.len(),
                args.db_b.display()
            );
            Ok(ExitCode::from(1))
        }
    }
}

fn load_timeline(db: &Path, filter: &QueryFilter) -> anyhow::Result<Vec<TelemetryEvent>> {
    let events = roxt_query::query_events(db, filter)
        .with_context(|| format!("cannot read events from {}", db.display()))?;
    Ok(events.into_iter().map(|stored| stored.event).collect())
}

/// Compares what the robot actually emitted: stamp, topic, type, payload.
/// `ingested_at_ns` is deliberately excluded — two databases recorded at
/// different times from the same session are still "the same timeline".
fn first_divergence(a: &[TelemetryEvent], b: &[TelemetryEvent]) -> Option<String> {
    for (index, (event_a, event_b)) in a.iter().zip(b.iter()).enumerate() {
        if !same_observation(event_a, event_b) {
            return Some(format!(
                "divergence at event {index}: \
                 a=({}, {}, {} bytes) vs b=({}, {}, {} bytes)",
                event_a.stamp_ns,
                event_a.topic,
                event_a.payload_cdr.len(),
                event_b.stamp_ns,
                event_b.topic,
                event_b.payload_cdr.len()
            ));
        }
    }
    if a.len() != b.len() {
        return Some(format!(
            "timelines match for {} events, then lengths diverge",
            a.len().min(b.len())
        ));
    }
    None
}

fn same_observation(a: &TelemetryEvent, b: &TelemetryEvent) -> bool {
    a.stamp_ns == b.stamp_ns
        && a.robot_id == b.robot_id
        && a.session_id == b.session_id
        && a.topic == b.topic
        && a.msg_type == b.msg_type
        && a.payload_cdr == b.payload_cdr
        && a.annotation == b.annotation
}

#[cfg(test)]
mod tests {
    use roxt_core::TelemetryEvent;

    fn event(stamp_ns: i64, topic: &str) -> TelemetryEvent {
        TelemetryEvent {
            stamp_ns,
            ingested_at_ns: 0,
            robot_id: "amr-1".to_owned(),
            session_id: "s1".to_owned(),
            topic: topic.to_owned(),
            msg_type: "t".to_owned(),
            payload_cdr: vec![1],
            annotation: None,
        }
    }

    #[test]
    fn identical_timelines_have_no_divergence() {
        let a = vec![event(1, "/odom"), event(2, "/cmd_vel")];
        assert_eq!(super::first_divergence(&a, &a.clone()), None);
    }

    #[test]
    fn different_ingest_times_are_not_divergence() {
        let a = vec![event(1, "/odom")];
        let mut b = a.clone();
        b[0].ingested_at_ns = 999;
        assert_eq!(super::first_divergence(&a, &b), None);
    }

    #[test]
    fn payload_change_is_reported_with_its_index() {
        let a = vec![event(1, "/odom"), event(2, "/cmd_vel")];
        let mut b = a.clone();
        b[1].payload_cdr = vec![9];
        let report = super::first_divergence(&a, &b).expect("divergence");
        assert!(report.contains("divergence at event 1"));
    }

    #[test]
    fn length_mismatch_is_divergence() {
        let a = vec![event(1, "/odom")];
        let b = vec![event(1, "/odom"), event(2, "/cmd_vel")];
        assert!(super::first_divergence(&a, &b).is_some());
    }
}
