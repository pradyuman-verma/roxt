//! Helpers shared by all ingestion sources.

use roxt_core::{IngestError, ValidationError};

/// Nanoseconds since Unix epoch on the roxt daemon clock, for the
/// `ingested_at_ns` field. Saturates at the i64 bounds: a clock anomaly on
/// the recording host must never abort an ingest, and the saturated value
/// is itself evidence that the clock was wrong.
pub(crate) fn wall_clock_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
}

/// Rejects empty partitioning identifiers before any IO happens, so the
/// operator's mistake is reported instantly rather than after a long parse.
pub(crate) fn require_non_empty(value: &str, field: &'static str) -> Result<(), IngestError> {
    if value.is_empty() {
        return Err(IngestError::InvalidEvent(ValidationError::EmptyField {
            field,
        }));
    }
    Ok(())
}
