use std::collections::BTreeMap;

use proptest::prelude::*;
use roxt_core::{EventAnnotation, Severity, StoreError, TelemetryEvent};
use rusqlite::Connection;

use crate::{row, SqliteStore};

fn read_all(path: &std::path::Path) -> Vec<TelemetryEvent> {
    let conn = Connection::open(path).expect("open for read");
    let sql = format!(
        "SELECT {} FROM events ORDER BY stamp_ns, id",
        row::SELECT_COLUMNS
    );
    let mut stmt = conn.prepare(&sql).expect("prepare");
    let rows = stmt
        .query_map([], |r| Ok(row::decode_row(r).map(|(_, event)| event)))
        .expect("query");
    rows.map(|r| r.expect("row").expect("decode")).collect()
}

fn event(stamp_ns: i64, annotation: Option<EventAnnotation>) -> TelemetryEvent {
    TelemetryEvent {
        stamp_ns,
        ingested_at_ns: stamp_ns + 1,
        robot_id: "amr-unit-042".to_owned(),
        session_id: "session-001".to_owned(),
        topic: "/odom".to_owned(),
        msg_type: "nav_msgs/msg/Odometry".to_owned(),
        payload_cdr: vec![1, 2, 3],
        annotation,
    }
}

#[test]
fn open_enables_wal_mode() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.db");
    let _store = SqliteStore::open(&path).expect("open");
    let conn = Connection::open(&path).expect("reopen");
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .expect("pragma");
    assert_eq!(mode.to_lowercase(), "wal");
}

#[test]
fn reopen_is_idempotent_and_records_migration_version() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.db");
    drop(SqliteStore::open(&path).expect("first open"));
    drop(SqliteStore::open(&path).expect("second open"));
    let conn = Connection::open(&path).expect("reopen");
    let version: u32 = conn
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |r| {
            r.get(0)
        })
        .expect("version");
    assert_eq!(version, 1);
}

#[test]
fn future_schema_version_refuses_to_open() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.db");
    drop(SqliteStore::open(&path).expect("create"));
    let conn = Connection::open(&path).expect("reopen");
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at_ns) VALUES (999, 0)",
        [],
    )
    .expect("insert");
    drop(conn);
    let result = SqliteStore::open(&path);
    assert!(matches!(
        result,
        Err(StoreError::VersionFromFuture { found: 999, .. })
    ));
}

#[test]
fn events_round_trip_with_and_without_annotation() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.db");
    let mut store = SqliteStore::open(&path).expect("open");
    let annotated = event(
        2,
        Some(EventAnnotation {
            kind: "ESTOP_TRIGGERED".to_owned(),
            description: "operator pressed e-stop".to_owned(),
            severity: Severity::Fatal,
            metadata: BTreeMap::from([("zone".to_owned(), "dock-3".to_owned())]),
        }),
    );
    let plain = event(1, None);
    let written = store
        .insert_events(&[plain.clone(), annotated.clone()])
        .expect("insert");
    assert_eq!(written, 2);
    assert_eq!(read_all(&path), vec![plain, annotated]);
}

#[test]
fn batches_larger_than_batch_size_are_fully_written() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.db");
    let mut store = SqliteStore::open(&path).expect("open");
    let events: Vec<_> = (0..i64::try_from(crate::BATCH_SIZE).expect("size") + 7)
        .map(|i| event(i, None))
        .collect();
    let written = store.insert_events(&events).expect("insert");
    assert_eq!(written, events.len());
    assert_eq!(read_all(&path).len(), events.len());
}

#[test]
fn store_error_messages_are_human_readable() {
    let error = StoreError::VersionFromFuture {
        found: 9,
        supported: 1,
    };
    assert_eq!(
        error.to_string(),
        "database schema version 9 is newer than this build supports (1)"
    );
}

fn severity_strategy() -> impl Strategy<Value = Severity> {
    prop_oneof![
        Just(Severity::Debug),
        Just(Severity::Info),
        Just(Severity::Warn),
        Just(Severity::Error),
        Just(Severity::Fatal),
    ]
}

fn annotation_strategy() -> impl Strategy<Value = EventAnnotation> {
    (
        "[A-Z_]{1,16}",
        ".{0,64}",
        severity_strategy(),
        proptest::collection::btree_map("[a-z0-9_]{1,64}", ".{0,32}", 0..=32),
    )
        .prop_map(|(kind, description, severity, metadata)| EventAnnotation {
            kind,
            description,
            severity,
            metadata,
        })
}

fn event_strategy() -> impl Strategy<Value = TelemetryEvent> {
    (
        any::<i64>(),
        any::<i64>(),
        "[a-z0-9-]{1,24}",
        "[a-z0-9-]{1,24}",
        "/[a-z_/]{1,32}",
        "[a-z_]{1,16}/msg/[A-Za-z]{1,16}",
        proptest::collection::vec(any::<u8>(), 0..256),
        proptest::option::of(annotation_strategy()),
    )
        .prop_map(
            |(
                stamp_ns,
                ingested_at_ns,
                robot_id,
                session_id,
                topic,
                msg_type,
                payload_cdr,
                annotation,
            )| TelemetryEvent {
                stamp_ns,
                ingested_at_ns,
                robot_id,
                session_id,
                topic,
                msg_type,
                payload_cdr,
                annotation,
            },
        )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn random_valid_events_survive_sqlite_round_trip(events in proptest::collection::vec(event_strategy(), 1..8)) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("events.db");
        let mut store = SqliteStore::open(&path).expect("open");
        for event in &events {
            prop_assert_eq!(event.validate(), Ok(()));
        }
        store.insert_events(&events).expect("insert");

        let conn = Connection::open(&path).expect("reopen");
        let sql = format!("SELECT {} FROM events ORDER BY id", row::SELECT_COLUMNS);
        let mut stmt = conn.prepare(&sql).expect("prepare");
        let stored: Vec<TelemetryEvent> = stmt
            .query_map([], |r| Ok(row::decode_row(r).map(|(_, e)| e)))
            .expect("query")
            .map(|r| r.expect("row").expect("decode"))
            .collect();
        prop_assert_eq!(stored, events);
    }
}
