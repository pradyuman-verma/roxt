//! Live Zenoh session source — intentional stub.
//!
//! v1 of roxt is file-based; live capture is a v2 deliverable. The type
//! exists now only so the CLI surface and feature gate are stable when the
//! real implementation lands.
//!
//! FUTURE(v2): see docs/zenoh-plan.md

use roxt_core::{IngestError, TelemetryEvent, TelemetrySource};

/// Placeholder for the v2 live Zenoh session source.
///
/// Compiled only under the `zenoh` feature; constructing one is possible
/// but polling it aborts, which is the documented and intended behaviour
/// until v2.
pub struct ZenohSource;

impl TelemetrySource for ZenohSource {
    fn next_event(&mut self) -> Result<Option<TelemetryEvent>, IngestError> {
        // FUTURE(v2): see docs/zenoh-plan.md — the only sanctioned todo!()
        // in the workspace.
        todo!("live Zenoh ingestion lands in v2")
    }
}
