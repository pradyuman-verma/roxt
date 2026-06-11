//! Query engine over stored telemetry events.
//!
//! Queries open the database read-only: incident analysis must never be
//! able to mutate the evidence it is examining. Output order is fully
//! deterministic — `(stamp_ns, rowid)` — so the same query over the same
//! database always produces byte-identical results, which `roxt diff`
//! and forensic reproducibility both depend on.

use std::path::Path;

use roxt_core::{QueryError, TelemetryEvent};
use roxt_store::row::{self, RowDecodeError};
use rusqlite::{Connection, OpenFlags};

/// Filters for an event query. `None` fields are unconstrained.
///
/// Timestamps are nanoseconds since Unix epoch; `from_ns` is inclusive and
/// `to_ns` exclusive, so adjacent windows partition a session without
/// overlap or gaps.
#[derive(Debug, Default, Clone)]
pub struct QueryFilter {
    /// Restrict to one robot.
    pub robot_id: Option<String>,
    /// Restrict to one ROS 2 topic.
    pub topic: Option<String>,
    /// Inclusive lower bound on `stamp_ns`.
    pub from_ns: Option<i64>,
    /// Exclusive upper bound on `stamp_ns`.
    pub to_ns: Option<i64>,
}

/// An event together with the `SQLite` rowid it was read from, so callers
/// can cite the exact row in reports and errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEvent {
    /// `SQLite` rowid in the `events` table.
    pub row_id: i64,
    /// The decoded event.
    pub event: TelemetryEvent,
}

/// Runs `filter` against the database at `db_path`.
///
/// # Errors
///
/// Returns [`QueryError::InvalidQuery`] for an inverted time range (checked
/// before any IO), [`QueryError::Execution`] for database failures, and
/// [`QueryError::CorruptRow`] when a stored row violates the schema.
pub fn query_events(db_path: &Path, filter: &QueryFilter) -> Result<Vec<StoredEvent>, QueryError> {
    validate_filter(filter)?;
    let conn = Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|source| QueryError::Execution { source })?;
    let (sql, params) = build_sql(filter);
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|source| QueryError::Execution { source })?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params), |r| {
            // Defer decode errors so row-level failures keep their rowid.
            Ok((r.get::<_, i64>(0), row::decode_row(r)))
        })
        .map_err(|source| QueryError::Execution { source })?;

    let mut events = Vec::new();
    for item in rows {
        let (row_id, decoded) = item.map_err(|source| QueryError::Execution { source })?;
        events.push(into_stored(row_id, decoded)?);
    }
    Ok(events)
}

fn into_stored(
    row_id: Result<i64, rusqlite::Error>,
    decoded: Result<(i64, TelemetryEvent), RowDecodeError>,
) -> Result<StoredEvent, QueryError> {
    let row_id = row_id.map_err(|source| QueryError::Execution { source })?;
    match decoded {
        Ok((_, event)) => Ok(StoredEvent { row_id, event }),
        Err(RowDecodeError::Sql(source)) => Err(QueryError::Execution { source }),
        Err(RowDecodeError::Invalid(source)) => Err(QueryError::CorruptRow { row_id, source }),
    }
}

fn validate_filter(filter: &QueryFilter) -> Result<(), QueryError> {
    if let (Some(from), Some(to)) = (filter.from_ns, filter.to_ns) {
        if from > to {
            return Err(QueryError::InvalidQuery {
                reason: format!("--from ({from}) is after --to ({to})"),
            });
        }
    }
    Ok(())
}

fn build_sql(filter: &QueryFilter) -> (String, Vec<rusqlite::types::Value>) {
    use rusqlite::types::Value;
    let mut clauses = Vec::new();
    let mut params: Vec<Value> = Vec::new();
    if let Some(robot_id) = &filter.robot_id {
        params.push(Value::Text(robot_id.clone()));
        clauses.push(format!("robot_id = ?{}", params.len()));
    }
    if let Some(topic) = &filter.topic {
        params.push(Value::Text(topic.clone()));
        clauses.push(format!("topic = ?{}", params.len()));
    }
    if let Some(from_ns) = filter.from_ns {
        params.push(Value::Integer(from_ns));
        clauses.push(format!("stamp_ns >= ?{}", params.len()));
    }
    if let Some(to_ns) = filter.to_ns {
        params.push(Value::Integer(to_ns));
        clauses.push(format!("stamp_ns < ?{}", params.len()));
    }
    let where_clause = if clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", clauses.join(" AND "))
    };
    let sql = format!(
        "SELECT {} FROM events{where_clause} ORDER BY stamp_ns, id",
        row::SELECT_COLUMNS
    );
    (sql, params)
}

#[cfg(test)]
mod tests;
