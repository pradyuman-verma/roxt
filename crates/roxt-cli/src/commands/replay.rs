//! `roxt replay` — re-emit stored events at recorded (or scaled) pace.
//!
//! Replay prints one line per event to stdout, sleeping the recorded
//! inter-event gap divided by `--speed`. It is a human review tool: an
//! operator watches an incident unfold at 1x or skims it at 20x.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::{bail, Context};
use clap::Args;
use roxt_query::QueryFilter;

#[derive(Args)]
pub struct ReplayArgs {
    /// roxt `SQLite` database to replay.
    #[arg(long)]
    db: PathBuf,
    /// Playback speed multiplier (1.0 = real time).
    #[arg(long, default_value_t = 1.0)]
    speed: f64,
}

pub fn run(args: &ReplayArgs) -> anyhow::Result<ExitCode> {
    if !(args.speed.is_finite() && args.speed > 0.0) {
        bail!("--speed must be a finite number greater than zero");
    }
    if !args.db.exists() {
        bail!("database {} does not exist", args.db.display());
    }
    let events = roxt_query::query_events(&args.db, &QueryFilter::default())
        .with_context(|| format!("cannot read events from {}", args.db.display()))?;

    let mut previous_stamp: Option<i64> = None;
    for stored in &events {
        let event = &stored.event;
        if let Some(previous) = previous_stamp {
            std::thread::sleep(scaled_gap(previous, event.stamp_ns, args.speed));
        }
        previous_stamp = Some(event.stamp_ns);
        let annotation = event
            .annotation
            .as_ref()
            .map(|a| format!("  [{} {}] {}", a.severity, a.kind, a.description))
            .unwrap_or_default();
        println!(
            "{} {} {} ({} bytes){annotation}",
            event.stamp_ns,
            event.topic,
            event.msg_type,
            event.payload_cdr.len()
        );
    }
    println!("replayed {} events", events.len());
    Ok(ExitCode::SUCCESS)
}

/// Gap between consecutive stamps scaled by speed. Out-of-order stamps
/// (possible across topics from different node clocks) replay back-to-back
/// rather than erroring: replay is review tooling, not validation.
fn scaled_gap(previous_ns: i64, current_ns: i64, speed: f64) -> Duration {
    let gap_ns = current_ns.saturating_sub(previous_ns).max(0);
    // f64 precision loss at nanosecond scale is irrelevant for human-paced
    // playback, and the cast saturates within the non-negative range
    // guaranteed by the max(0) above.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation
    )]
    Duration::from_nanos((gap_ns as f64 / speed) as u64)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[test]
    fn gap_is_scaled_by_speed() {
        assert_eq!(
            super::scaled_gap(0, 1_000_000_000, 2.0),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn out_of_order_stamps_replay_back_to_back() {
        assert_eq!(super::scaled_gap(100, 50, 1.0), Duration::ZERO);
    }
}
