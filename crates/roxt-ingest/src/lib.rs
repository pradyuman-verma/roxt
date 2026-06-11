//! Telemetry ingestion sources.
//!
//! Each module wraps one transport behind [`roxt_core::TelemetrySource`],
//! so storage and query never know where events came from. File-based
//! sources stream — none of them buffers a whole recording in memory,
//! because incident bags from a warehouse fleet routinely exceed RAM.

mod common;
mod mcap_source;
mod ros2bag;
#[cfg(feature = "zenoh")]
mod zenoh;

pub use mcap_source::McapSource;
pub use ros2bag::Ros2BagSource;
#[cfg(feature = "zenoh")]
pub use zenoh::ZenohSource;
