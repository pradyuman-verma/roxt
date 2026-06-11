//! Versioned, append-only schema migrations.
//!
//! Migrations are embedded SQL strings (not files) so a roxt binary is
//! self-contained: the schema it expects is the schema it carries. The
//! `schema_migrations` table records what has been applied; migrations are
//! never edited or removed once shipped, only appended — a forensic
//! database must be upgradable in place without reinterpreting old rows.

use roxt_core::StoreError;
use rusqlite::Connection;

/// One schema step. `sql` may contain multiple statements; the runner wraps
/// each migration in its own transaction.
struct Migration {
    version: u32,
    sql: &'static str,
}

/// Append new entries here; never modify shipped ones. Every column
/// addition is a new version.
const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    sql: "
        CREATE TABLE events (
            id                     INTEGER PRIMARY KEY,
            stamp_ns               INTEGER NOT NULL,
            ingested_at_ns         INTEGER NOT NULL,
            robot_id               TEXT    NOT NULL,
            session_id             TEXT    NOT NULL,
            topic                  TEXT    NOT NULL,
            msg_type               TEXT    NOT NULL,
            payload_cdr            BLOB    NOT NULL,
            annotation_kind        TEXT,
            annotation_description TEXT,
            annotation_severity    TEXT,
            annotation_metadata    TEXT
        );
        CREATE INDEX idx_events_robot_session_stamp
            ON events (robot_id, session_id, stamp_ns);
        CREATE INDEX idx_events_topic_stamp
            ON events (topic, stamp_ns);
    ",
}];

/// Applies every migration newer than the database's recorded version.
///
/// # Errors
///
/// Returns [`StoreError::SchemaMigration`] if a step fails (the step's
/// transaction rolls back, leaving the database at the previous version)
/// and [`StoreError::VersionFromFuture`] if the database was written by a
/// newer roxt than this build.
pub fn apply_pending(conn: &Connection) -> Result<(), StoreError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version       INTEGER PRIMARY KEY,
            applied_at_ns INTEGER NOT NULL
        );",
    )
    .map_err(|source| StoreError::SchemaMigration { version: 0, source })?;

    let current = current_version(conn)?;
    let supported = MIGRATIONS.last().map_or(0, |m| m.version);
    if current > supported {
        return Err(StoreError::VersionFromFuture {
            found: current,
            supported,
        });
    }
    for migration in MIGRATIONS.iter().filter(|m| m.version > current) {
        apply_one(conn, migration)?;
    }
    Ok(())
}

fn current_version(conn: &Connection) -> Result<u32, StoreError> {
    conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
        [],
        |row| row.get(0),
    )
    .map_err(|source| StoreError::SchemaMigration { version: 0, source })
}

fn apply_one(conn: &Connection, migration: &Migration) -> Result<(), StoreError> {
    let map_err = |source: rusqlite::Error| StoreError::SchemaMigration {
        version: migration.version,
        source,
    };
    let tx = conn.unchecked_transaction().map_err(map_err)?;
    tx.execute_batch(migration.sql).map_err(map_err)?;
    tx.execute(
        "INSERT INTO schema_migrations (version, applied_at_ns) VALUES (?1, ?2)",
        rusqlite::params![migration.version, wall_clock_ns()],
    )
    .map_err(map_err)?;
    tx.commit().map_err(map_err)?;
    tracing::info!(version = migration.version, "applied schema migration");
    Ok(())
}

/// Wall-clock nanoseconds for the migration audit column. Saturates rather
/// than fails: a wrong audit timestamp must never block a migration.
fn wall_clock_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
}
