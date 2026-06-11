use std::path::Path;

use roxt_core::{IngestError, TelemetrySource};
use rusqlite::Connection;

use crate::Ros2BagSource;

/// Builds a minimal rosbag2 (sqlite3 storage) directory: metadata.yaml plus
/// a .db3 with the standard `topics`/`messages` tables.
fn write_bag(dir: &Path, message_count: i64, metadata_version: u32, storage: &str) {
    let metadata = format!(
        "rosbag2_bagfile_information:\n  version: {metadata_version}\n  storage_identifier: {storage}\n"
    );
    std::fs::write(dir.join("metadata.yaml"), metadata).expect("write metadata");
    let conn = Connection::open(dir.join("bag_0.db3")).expect("create db3");
    conn.execute_batch(
        "CREATE TABLE topics (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT NOT NULL,
            serialization_format TEXT NOT NULL,
            offered_qos_profiles TEXT NOT NULL
        );
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            topic_id INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            data BLOB NOT NULL
        );
        INSERT INTO topics VALUES (1, '/scan', 'sensor_msgs/msg/LaserScan', 'cdr', '');",
    )
    .expect("create tables");
    for i in 0..message_count {
        conn.execute(
            "INSERT INTO messages (topic_id, timestamp, data) VALUES (1, ?1, ?2)",
            rusqlite::params![i * 1_000, vec![0u8, 1, 0, 0]],
        )
        .expect("insert message");
    }
}

#[test]
fn reads_all_messages_in_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_bag(dir.path(), 600, 5, "sqlite3");
    let mut source = Ros2BagSource::new(dir.path(), "amr-unit-042", "session-001").expect("open");
    let mut events = Vec::new();
    while let Some(event) = source.next_event().expect("next_event") {
        events.push(event);
    }
    // 600 spans multiple FETCH_BATCH pages, exercising pagination.
    assert_eq!(events.len(), 600);
    assert!(events.windows(2).all(|w| w[0].stamp_ns <= w[1].stamp_ns));
    assert_eq!(events[0].topic, "/scan");
    assert_eq!(events[0].msg_type, "sensor_msgs/msg/LaserScan");
}

#[test]
fn empty_bag_yields_zero_events() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_bag(dir.path(), 0, 5, "sqlite3");
    let mut source = Ros2BagSource::new(dir.path(), "amr-unit-042", "session-001").expect("open");
    assert!(source.next_event().expect("poll").is_none());
    assert!(source.next_event().expect("re-poll").is_none());
}

#[test]
fn unsupported_metadata_version_fails_loudly() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_bag(dir.path(), 1, 99, "sqlite3");
    let result = Ros2BagSource::new(dir.path(), "amr-unit-042", "session-001");
    assert!(matches!(
        result,
        Err(IngestError::SchemaVersionMismatch {
            expected: _,
            found: 99
        })
    ));
}

#[test]
fn non_sqlite_storage_is_unsupported() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_bag(dir.path(), 1, 5, "mcap");
    let result = Ros2BagSource::new(dir.path(), "amr-unit-042", "session-001");
    assert!(matches!(result, Err(IngestError::UnsupportedFormat { .. })));
}

#[test]
fn missing_metadata_yaml_is_an_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_bag(dir.path(), 1, 5, "sqlite3");
    std::fs::remove_file(dir.path().join("metadata.yaml")).expect("remove");
    let result = Ros2BagSource::new(dir.path(), "amr-unit-042", "session-001");
    assert!(matches!(result, Err(IngestError::SourceIo { .. })));
}

#[test]
fn direct_db3_path_uses_sibling_metadata() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_bag(dir.path(), 3, 5, "sqlite3");
    let db3 = dir.path().join("bag_0.db3");
    let mut source = Ros2BagSource::new(&db3, "amr-unit-042", "session-001").expect("open");
    let mut count = 0;
    while source.next_event().expect("next_event").is_some() {
        count += 1;
    }
    assert_eq!(count, 3);
}
