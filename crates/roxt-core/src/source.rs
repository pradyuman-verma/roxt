//! The transport-agnostic ingestion contract.
//!
//! Everything downstream of ingestion (storage, query, diff) sees only
//! [`TelemetryEvent`] values, so adding a new transport later (live Zenoh
//! sessions in v2) must not touch the store or query crates — it only needs
//! a new implementor of this trait.

use crate::error::IngestError;
use crate::event::TelemetryEvent;

/// Implemented by every telemetry source.
///
/// Implementors must be `Send` (the daemon runs sources on a tokio runtime).
/// Sources are responsible for their own resource cleanup on drop.
/// The stream terminates naturally when the source is exhausted (file-based)
/// or is cancelled via the provided `CancellationToken` (live sources).
pub trait TelemetrySource: Send {
    /// Returns the next event, or `None` when exhausted.
    /// Must not block indefinitely without honouring cancellation.
    ///
    /// # Errors
    ///
    /// Returns an [`IngestError`] when a record cannot be read or decoded.
    /// Errors are not necessarily fatal to the stream — callers decide
    /// whether to skip or abort — but implementors must keep the byte
    /// offset and topic in the error so the record stays locatable.
    fn next_event(&mut self) -> Result<Option<TelemetryEvent>, IngestError>;
}
