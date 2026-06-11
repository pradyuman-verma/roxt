//! The single source of truth for how a [`TelemetryEvent`] maps to an
//! `events` row.
//!
//! Both the writer (this crate) and the reader (`roxt-query`) go through
//! these functions, so a schema change cannot drift between write and read
//! paths. Annotation metadata is stored as canonical JSON: `BTreeMap`
//! iteration order is sorted, so the same map always produces byte-equal
//! text — a requirement for `roxt diff` to be meaningful.

use std::collections::BTreeMap;
use std::str::FromStr;

use roxt_core::{EventAnnotation, Severity, TelemetryEvent, ValidationError};
use rusqlite::types::Value;
use rusqlite::Row;

/// Insert statement matching [`decode_row`]'s column order.
pub const INSERT_SQL: &str = "
    INSERT INTO events (
        stamp_ns, ingested_at_ns, robot_id, session_id, topic, msg_type,
        payload_cdr, annotation_kind, annotation_description,
        annotation_severity, annotation_metadata
    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
";

/// Column list for readers, in the order [`decode_row`] expects.
pub const SELECT_COLUMNS: &str = "
    id, stamp_ns, ingested_at_ns, robot_id, session_id, topic, msg_type,
    payload_cdr, annotation_kind, annotation_description,
    annotation_severity, annotation_metadata
";

/// Why a stored row could not be decoded back into an event.
#[derive(Debug)]
pub enum RowDecodeError {
    /// Column access or type conversion failed at the `SQLite` level.
    Sql(rusqlite::Error),
    /// The row decoded but violates a schema invariant.
    Invalid(ValidationError),
}

impl From<rusqlite::Error> for RowDecodeError {
    fn from(source: rusqlite::Error) -> Self {
        Self::Sql(source)
    }
}

/// Binds an event to the [`INSERT_SQL`] placeholders.
#[must_use]
pub fn insert_params(event: &TelemetryEvent) -> [Value; 11] {
    let (kind, description, severity, metadata) = match &event.annotation {
        Some(a) => (
            Value::Text(a.kind.clone()),
            Value::Text(a.description.clone()),
            Value::Text(a.severity.as_str().to_owned()),
            Value::Text(metadata_to_json(&a.metadata)),
        ),
        None => (Value::Null, Value::Null, Value::Null, Value::Null),
    };
    [
        Value::Integer(event.stamp_ns),
        Value::Integer(event.ingested_at_ns),
        Value::Text(event.robot_id.clone()),
        Value::Text(event.session_id.clone()),
        Value::Text(event.topic.clone()),
        Value::Text(event.msg_type.clone()),
        Value::Blob(event.payload_cdr.clone()),
        kind,
        description,
        severity,
        metadata,
    ]
}

/// Decodes a row selected with [`SELECT_COLUMNS`], returning the `SQLite`
/// rowid alongside the event so callers can name the row in errors.
///
/// # Errors
///
/// Returns [`RowDecodeError::Sql`] on column-level failures and
/// [`RowDecodeError::Invalid`] when the stored data violates the schema
/// (possible only for databases written by foreign tools).
pub fn decode_row(row: &Row<'_>) -> Result<(i64, TelemetryEvent), RowDecodeError> {
    let row_id: i64 = row.get(0)?;
    let event = TelemetryEvent {
        stamp_ns: row.get(1)?,
        ingested_at_ns: row.get(2)?,
        robot_id: row.get(3)?,
        session_id: row.get(4)?,
        topic: row.get(5)?,
        msg_type: row.get(6)?,
        payload_cdr: row.get(7)?,
        annotation: decode_annotation(row)?,
    };
    Ok((row_id, event))
}

fn decode_annotation(row: &Row<'_>) -> Result<Option<EventAnnotation>, RowDecodeError> {
    let kind: Option<String> = row.get(8)?;
    let Some(kind) = kind else {
        return Ok(None);
    };
    let description: String = row.get(9)?;
    let severity_text: String = row.get(10)?;
    let metadata_json: String = row.get(11)?;
    let severity = Severity::from_str(&severity_text).map_err(RowDecodeError::Invalid)?;
    let metadata = metadata_from_json(&metadata_json).map_err(RowDecodeError::Invalid)?;
    Ok(Some(EventAnnotation {
        kind,
        description,
        severity,
        metadata,
    }))
}

fn metadata_to_json(metadata: &BTreeMap<String, String>) -> String {
    // Serialising a map of valid UTF-8 strings to JSON cannot fail; the
    // fallback exists only to keep the unwrap ban honest.
    serde_json::to_string(metadata).unwrap_or_else(|_| "{}".to_owned())
}

fn metadata_from_json(json: &str) -> Result<BTreeMap<String, String>, ValidationError> {
    serde_json::from_str(json).map_err(|e| ValidationError::MalformedMetadata {
        reason: e.to_string(),
    })
}
