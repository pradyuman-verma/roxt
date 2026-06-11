//! Edge-case coverage the conformance files and happy-path fixtures miss:
//! compression variants, robot/daemon clock skew, u64 log_time overflow,
//! and pre-epoch timestamps (only representable via rosbag2 — MCAP's
//! log_time is unsigned, which is exactly why the schema uses i64).

use std::path::{Path, PathBuf};

use roxt_core::{IngestError, TelemetryEvent, TelemetrySource};
use roxt_ingest::{McapSource, Ros2BagSource};

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../fixtures")
        .join(name)
}

fn read_all(path: &Path) -> Vec<TelemetryEvent> {
    let mut source = McapSource::new(path, "amr-unit-042", "session-001").expect("open");
    let mut events = Vec::new();
    while let Some(event) = source.next_event().expect("next_event") {
        events.push(event);
    }
    events
}

fn observed(events: &[TelemetryEvent]) -> Vec<(i64, String, Vec<u8>)> {
    events
        .iter()
        .map(|e| (e.stamp_ns, e.topic.clone(), e.payload_cdr.clone()))
        .collect()
}

#[test]
fn compression_variants_decode_to_identical_events() {
    let zstd = read_all(&fixture("sample_zstd.mcap"));
    let lz4 = read_all(&fixture("sample_lz4.mcap"));
    let none = read_all(&fixture("sample_uncompressed.mcap"));
    assert_eq!(zstd.len(), 12);
    // Compression is a container concern; the telemetry must not change.
    assert_eq!(observed(&zstd), observed(&lz4));
    assert_eq!(observed(&zstd), observed(&none));
}

#[test]
fn clock_skew_is_recorded_not_reconciled() {
    let events = read_all(&fixture("clock_skew.mcap"));
    assert_eq!(events.len(), 4);
    // Robot clock says 2001; both clocks must be stored unaltered.
    assert_eq!(events[0].stamp_ns, 984_130_000_000_000_000);
    let year_2020_ns = 1_577_836_800_000_000_000;
    assert!(
        events.iter().all(|e| e.ingested_at_ns > year_2020_ns),
        "ingest clock must be the daemon's wall clock, not the robot's"
    );
}

#[test]
fn log_time_overflowing_i64_is_a_corrupt_payload_error() {
    let mut source =
        McapSource::new(&fixture("overflow.mcap"), "amr-unit-042", "session-001").expect("open");
    let error = source.next_event().expect_err("u64::MAX must not ingest");
    match error {
        IngestError::CorruptPayload { topic, reason, .. } => {
            assert_eq!(topic, "/imu");
            assert!(reason.contains("overflows"), "reason was: {reason}");
        }
        other => panic!("expected CorruptPayload, got: {other}"),
    }
}

#[test]
fn pre_epoch_rosbag2_timestamps_survive_the_full_pipeline() {
    let dir = tempfile::tempdir().expect("tempdir");
    let bag = dir.path().join("preepoch_bag");
    std::fs::create_dir(&bag).expect("mkdir");
    write_pre_epoch_bag(&bag);

    let mut source = Ros2BagSource::new(&bag, "amr-unit-042", "session-001").expect("open bag");
    let mut events = Vec::new();
    while let Some(event) = source.next_event().expect("next_event") {
        events.push(event);
    }
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].stamp_ns, -1_000_000_000);

    // Through store and query as well: i64 stamps must round-trip.
    let db = dir.path().join("events.db");
    let mut store = roxt_store::SqliteStore::open(&db).expect("open store");
    store.insert_events(&events).expect("insert");
    let stored =
        roxt_query::query_events(&db, &roxt_query::QueryFilter::default()).expect("query");
    let stamps: Vec<i64> = stored.iter().map(|s| s.event.stamp_ns).collect();
    assert_eq!(stamps, vec![-1_000_000_000, -500_000_000, 0]);
}

/// Minimal rosbag2 (sqlite3) bag with timestamps straddling the epoch.
fn write_pre_epoch_bag(dir: &Path) {
    std::fs::write(
        dir.join("metadata.yaml"),
        "rosbag2_bagfile_information:\n  version: 5\n  storage_identifier: sqlite3\n",
    )
    .expect("write metadata");
    let conn = rusqlite::Connection::open(dir.join("bag_0.db3")).expect("create db3");
    conn.execute_batch(
        "CREATE TABLE topics (
            id INTEGER PRIMARY KEY, name TEXT NOT NULL, type TEXT NOT NULL,
            serialization_format TEXT NOT NULL, offered_qos_profiles TEXT NOT NULL
        );
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY, topic_id INTEGER NOT NULL,
            timestamp INTEGER NOT NULL, data BLOB NOT NULL
        );
        INSERT INTO topics VALUES (1, '/fixture', 'std_msgs/msg/Empty', 'cdr', '');
        INSERT INTO messages (topic_id, timestamp, data) VALUES
            (1, -1000000000, x'00'),
            (1, -500000000, x'01'),
            (1, 0, x'02');",
    )
    .expect("seed bag");
}
