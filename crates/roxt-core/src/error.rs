//! The structured error hierarchy shared by every roxt crate.
//!
//! Library crates return these typed errors so that callers can match on
//! failure modes programmatically — `Box<dyn Error>` and `anyhow` are
//! banned from library APIs because an insurability pipeline needs to know
//! *which* failure occurred, not just that one did. Every variant carries
//! enough context (path, offset, topic, version) to identify the offending
//! input without re-running the ingest.

use std::path::PathBuf;

use thiserror::Error;

/// Top-level error for roxt operations, wrapping the per-layer errors.
///
/// The CLI maps this to an exit code and a human-readable message; library
/// callers match on the inner variant.
#[derive(Debug, Error)]
pub enum RoxtError {
    /// A telemetry source failed while producing events.
    #[error(transparent)]
    Ingest(#[from] IngestError),
    /// The `SQLite` store failed while persisting events.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The query engine failed while reading stored events.
    #[error(transparent)]
    Query(#[from] QueryError),
    /// An IO failure outside any specific layer (e.g. fixture access).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Failures raised by telemetry sources during ingestion.
#[derive(Debug, Error)]
pub enum IngestError {
    /// The requested source format is not one roxt can read.
    #[error("unsupported telemetry format: {format}")]
    UnsupportedFormat {
        /// The format name as supplied by the user or detected from input.
        format: String,
    },
    /// A message payload could not be decoded at a known location.
    ///
    /// Carries topic and byte offset so the offending record can be found
    /// in the original file with a hex editor — corrupt payloads in an
    /// incident bag are themselves evidence and must stay locatable.
    #[error("corrupt payload on topic {topic} at byte offset {offset}: {reason}")]
    CorruptPayload {
        /// Topic of the message that failed to decode.
        topic: String,
        /// Byte offset of the record within the source file.
        offset: u64,
        /// Decoder-specific explanation of the failure.
        reason: String,
    },
    /// The source file declares a schema version roxt does not support.
    #[error("schema version mismatch: expected {expected}, found {found}")]
    SchemaVersionMismatch {
        /// Highest version this build of roxt understands.
        expected: u32,
        /// Version declared by the input file.
        found: u32,
    },
    /// `next_event` was called again after the source reported exhaustion.
    #[error("telemetry source already exhausted")]
    SourceExhausted,
    /// An event was read successfully but violates schema invariants.
    ///
    /// Raised at the ingest boundary (not in the store) so the error can
    /// still be tied to its originating file and record.
    #[error("invalid event: {0}")]
    InvalidEvent(#[from] ValidationError),
    /// The source file or endpoint could not be read at the IO level.
    #[error("failed to read source {path}: {source}")]
    SourceIo {
        /// Path of the input that failed.
        path: PathBuf,
        /// Underlying IO error.
        source: std::io::Error,
    },
    /// A rosbag2 sidecar (metadata.yaml or the .db3 itself) is malformed.
    #[error("malformed rosbag2 input {path}: {reason}")]
    MalformedBag {
        /// Path of the bag directory or database file.
        path: PathBuf,
        /// What was wrong with it.
        reason: String,
    },
}

/// Failures raised by the `SQLite` storage layer.
#[derive(Debug, Error)]
pub enum StoreError {
    /// The database file could not be opened or configured.
    #[error("failed to open database {path}: {source}")]
    DatabaseOpen {
        /// Path of the `SQLite` database.
        path: PathBuf,
        /// Underlying `SQLite` error.
        source: rusqlite::Error,
    },
    /// A schema migration failed; the database is at the named version.
    #[error("schema migration to version {version} failed: {source}")]
    SchemaMigration {
        /// The migration version that failed to apply.
        version: u32,
        /// Underlying `SQLite` error.
        source: rusqlite::Error,
    },
    /// The database was written by a newer roxt than this build.
    ///
    /// Refusing to open is deliberate: silently reading a schema with
    /// unknown columns could misrepresent forensic data.
    #[error("database schema version {found} is newer than this build supports ({supported})")]
    VersionFromFuture {
        /// Version recorded in the database's migration table.
        found: u32,
        /// Highest version this build can apply.
        supported: u32,
    },
    /// A batched event write failed and was rolled back.
    ///
    /// Carries the stamp of the first event in the failed batch so an
    /// operator can re-ingest exactly the dropped range — events are never
    /// silently discarded.
    #[error("write failure for batch starting at stamp {event_stamp_ns} ns: {source}")]
    WriteFailure {
        /// `stamp_ns` of the first event in the failed batch.
        event_stamp_ns: i64,
        /// Underlying `SQLite` error.
        source: rusqlite::Error,
    },
}

/// Failures raised by the query engine.
#[derive(Debug, Error)]
pub enum QueryError {
    /// The query itself is invalid (e.g. `from` after `to`).
    #[error("invalid query: {reason}")]
    InvalidQuery {
        /// Why the query was rejected before touching the database.
        reason: String,
    },
    /// The database rejected the query.
    #[error("query execution failed: {source}")]
    Execution {
        /// Underlying `SQLite` error.
        source: rusqlite::Error,
    },
    /// A stored row violates the schema (e.g. unknown severity text).
    ///
    /// This indicates the database was written by a foreign tool or
    /// corrupted; the row id is included so it can be inspected directly.
    #[error("corrupt row {row_id}: {source}")]
    CorruptRow {
        /// `SQLite` rowid of the offending row.
        row_id: i64,
        /// The schema invariant the row violates.
        source: ValidationError,
    },
}

/// Schema invariant violations on a single event or annotation.
///
/// Shared between ingest-time validation and read-time row decoding so the
/// same invariant produces the same error text everywhere.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ValidationError {
    /// A required field was empty.
    #[error("field {field} must not be empty")]
    EmptyField {
        /// Name of the offending field.
        field: &'static str,
    },
    /// Annotation metadata exceeds the documented pair bound.
    #[error("annotation metadata has {count} pairs, maximum is {max}", max = crate::event::MAX_METADATA_PAIRS)]
    TooManyMetadataPairs {
        /// Number of pairs supplied.
        count: usize,
    },
    /// An annotation metadata key exceeds the documented length bound.
    #[error("metadata key {key:?} is {len} bytes, maximum is {max}", max = crate::event::MAX_METADATA_KEY_LEN)]
    MetadataKeyTooLong {
        /// The offending key.
        key: String,
        /// Its length in bytes.
        len: usize,
    },
    /// A severity string did not match any known level.
    #[error("invalid severity {value:?}, expected DEBUG|INFO|WARN|ERROR|FATAL")]
    InvalidSeverity {
        /// The unrecognised input.
        value: String,
    },
    /// Stored annotation metadata is not the canonical JSON object form.
    ///
    /// Only reachable when reading a database written by a foreign tool —
    /// roxt itself always writes metadata as a JSON string→string object.
    #[error("malformed annotation metadata: {reason}")]
    MalformedMetadata {
        /// Parser explanation of the failure.
        reason: String,
    },
}
