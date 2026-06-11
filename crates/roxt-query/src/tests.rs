use std::path::Path;

use roxt_core::{QueryError, TelemetryEvent};
use roxt_store::SqliteStore;

use crate::{query_events, QueryFilter};

fn event(stamp_ns: i64, robot_id: &str, topic: &str) -> TelemetryEvent {
    TelemetryEvent {
        stamp_ns,
        ingested_at_ns: stamp_ns,
        robot_id: robot_id.to_owned(),
        session_id: "session-001".to_owned(),
        topic: topic.to_owned(),
        msg_type: "geometry_msgs/msg/Twist".to_owned(),
        payload_cdr: vec![1],
        annotation: None,
    }
}

fn seeded_db(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("events.db");
    let mut store = SqliteStore::open(&path).expect("open");
    store
        .insert_events(&[
            event(100, "amr-1", "/cmd_vel"),
            event(200, "amr-1", "/odom"),
            event(300, "amr-2", "/cmd_vel"),
            event(400, "amr-2", "/odom"),
        ])
        .expect("seed");
    path
}

#[test]
fn unfiltered_query_returns_all_in_stamp_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = seeded_db(dir.path());
    let events = query_events(&db, &QueryFilter::default()).expect("query");
    let stamps: Vec<i64> = events.iter().map(|e| e.event.stamp_ns).collect();
    assert_eq!(stamps, vec![100, 200, 300, 400]);
}

#[test]
fn filters_compose_with_and_semantics() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = seeded_db(dir.path());
    let filter = QueryFilter {
        robot_id: Some("amr-1".to_owned()),
        topic: Some("/odom".to_owned()),
        ..QueryFilter::default()
    };
    let events = query_events(&db, &filter).expect("query");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event.stamp_ns, 200);
}

#[test]
fn time_window_is_inclusive_from_exclusive_to() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = seeded_db(dir.path());
    let filter = QueryFilter {
        from_ns: Some(200),
        to_ns: Some(400),
        ..QueryFilter::default()
    };
    let events = query_events(&db, &filter).expect("query");
    let stamps: Vec<i64> = events.iter().map(|e| e.event.stamp_ns).collect();
    assert_eq!(stamps, vec![200, 300]);
}

#[test]
fn inverted_time_range_is_rejected_before_io() {
    let filter = QueryFilter {
        from_ns: Some(400),
        to_ns: Some(200),
        ..QueryFilter::default()
    };
    // A nonexistent path proves validation happens before the open.
    let result = query_events(Path::new("/nonexistent/no.db"), &filter);
    assert!(matches!(result, Err(QueryError::InvalidQuery { .. })));
}

#[test]
fn query_is_deterministic_across_runs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = seeded_db(dir.path());
    let first = query_events(&db, &QueryFilter::default()).expect("first");
    let second = query_events(&db, &QueryFilter::default()).expect("second");
    assert_eq!(first, second);
}

#[test]
fn corrupt_severity_reports_the_offending_row() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = seeded_db(dir.path());
    let conn = rusqlite::Connection::open(&db).expect("open");
    conn.execute(
        "UPDATE events
         SET annotation_kind = 'X', annotation_description = '',
             annotation_severity = 'BOGUS', annotation_metadata = '{}'
         WHERE stamp_ns = 300",
        [],
    )
    .expect("corrupt");
    drop(conn);
    let result = query_events(&db, &QueryFilter::default());
    assert!(matches!(result, Err(QueryError::CorruptRow { .. })));
}
