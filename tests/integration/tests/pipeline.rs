//! End-to-end tests: fixture MCAP files through ingest, store, query, and
//! the actual `roxt` binary.

use std::path::{Path, PathBuf};

use assert_cmd::Command;
use roxt_core::TelemetrySource;
use roxt_ingest::McapSource;

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../fixtures")
        .join(name)
}

fn roxt() -> Command {
    Command::cargo_bin("roxt").expect("roxt binary")
}

fn record(db: &Path, input: &Path) {
    roxt()
        .args(["rec", "--source", "mcap"])
        .arg("--input")
        .arg(input)
        .args(["--robot-id", "amr-unit-042", "--session-id", "session-001"])
        .arg("--out")
        .arg(db)
        .assert()
        .success();
}

#[test]
fn mcap_fixture_parses_with_known_count_and_monotonic_stamps() {
    let mut source =
        McapSource::new(&fixture("sample.mcap"), "amr-unit-042", "session-001").expect("open");
    let mut events = Vec::new();
    while let Some(event) = source.next_event().expect("next_event") {
        events.push(event);
    }
    assert_eq!(events.len(), 24, "fixture has a known message count");
    assert!(
        events.windows(2).all(|w| w[0].stamp_ns <= w[1].stamp_ns),
        "timestamps must be monotonically non-decreasing"
    );
}

#[test]
fn empty_mcap_produces_zero_events_not_an_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("empty.db");
    record(&db, &fixture("empty.mcap"));
    let events =
        roxt_query::query_events(&db, &roxt_query::QueryFilter::default()).expect("query empty db");
    assert!(events.is_empty());
}

#[test]
fn full_pipeline_query_json_matches_fixture() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("events.db");
    record(&db, &fixture("sample.mcap"));

    let output = roxt()
        .arg("query")
        .arg("--db")
        .arg(&db)
        .args(["--format", "json"])
        .output()
        .expect("run query");
    assert!(output.status.success());
    let mut actual: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("stdout is json");
    // ingested_at_ns is the daemon wall clock and differs per run; zero it
    // before comparing. Everything else must match the fixture exactly.
    for item in actual.as_array_mut().expect("array") {
        item["ingested_at_ns"] = serde_json::json!(0);
    }
    let expected: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(fixture("expected_query.json")).expect("read fixture"),
    )
    .expect("fixture is json");
    assert_eq!(actual, expected);
}

#[test]
fn query_time_window_filters_fixture_events() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("events.db");
    record(&db, &fixture("sample.mcap"));
    // Fixture cadence is 50 ms from 1_700_000_000s; [base, base+200ms)
    // holds exactly 4 events.
    let filter = roxt_query::QueryFilter {
        from_ns: Some(1_700_000_000_000_000_000),
        to_ns: Some(1_700_000_000_200_000_000),
        ..roxt_query::QueryFilter::default()
    };
    let events = roxt_query::query_events(&db, &filter).expect("query");
    assert_eq!(events.len(), 4);
}

#[test]
fn diff_of_two_identical_recordings_reports_zero_divergence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_a = dir.path().join("a.db");
    let db_b = dir.path().join("b.db");
    record(&db_a, &fixture("sample.mcap"));
    record(&db_b, &fixture("sample.mcap"));
    roxt()
        .arg("diff")
        .arg("--db-a")
        .arg(&db_a)
        .arg("--db-b")
        .arg(&db_b)
        .assert()
        .success()
        .stdout(predicates::str::contains("no divergence"));
}

#[test]
fn diff_of_divergent_databases_exits_nonzero() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_a = dir.path().join("a.db");
    let db_b = dir.path().join("b.db");
    record(&db_a, &fixture("sample.mcap"));
    record(&db_b, &fixture("empty.mcap"));
    roxt()
        .arg("diff")
        .arg("--db-a")
        .arg(&db_a)
        .arg("--db-b")
        .arg(&db_b)
        .assert()
        .code(1);
}

#[test]
fn rec_rejects_empty_robot_id_with_clear_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("events.db");
    roxt()
        .args(["rec", "--source", "mcap"])
        .arg("--input")
        .arg(fixture("sample.mcap"))
        .args(["--robot-id", "", "--session-id", "session-001"])
        .arg("--out")
        .arg(&db)
        .assert()
        .failure()
        .stderr(predicates::str::contains("--robot-id"));
}
