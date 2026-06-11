//! The canonical telemetry event schema.
//!
//! This schema is the upstream foundation for fleet insurability scoring:
//! after a robot incident, these records are the forensic evidence. Field
//! changes here ripple into the `SQLite` schema (`roxt-store`) and the
//! documented contract in `docs/schema.md` — keep all three in lockstep.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use crate::error::ValidationError;

/// Upper bound on annotation metadata pairs. Annotations are decision-point
/// evidence, not a dumping ground for full state snapshots — the bound keeps
/// rows small enough for fleet-scale queries.
pub const MAX_METADATA_PAIRS: usize = 32;

/// Upper bound on annotation metadata key length, in bytes. Keys are meant
/// to be short machine-readable labels; long keys usually indicate data
/// smuggled into the key position.
pub const MAX_METADATA_KEY_LEN: usize = 64;

/// A single normalised telemetry event captured from a robot node.
///
/// All timestamps are nanoseconds since Unix epoch (i64 to handle
/// pre-epoch test fixtures gracefully). Nanosecond precision matches
/// ROS 2 `builtin_interfaces/Time`.
///
/// `robot_id` and `session_id` are the primary partitioning keys for
/// fleet-level queries. Never default these silently — require explicit
/// provision at ingest time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryEvent {
    /// Monotonic nanosecond timestamp from the originating ROS 2 node clock.
    pub stamp_ns: i64,
    /// Wall-clock nanosecond timestamp at ingest time (roxt daemon clock).
    pub ingested_at_ns: i64,
    /// Stable robot identifier (e.g. "amr-unit-042"). Must be non-empty.
    pub robot_id: String,
    /// Session identifier — one per rosbag recording or live run.
    pub session_id: String,
    /// ROS 2 topic name, e.g. "/`cmd_vel`", "/odom", "/diagnostics".
    pub topic: String,
    /// ROS 2 message type string, e.g. "`geometry_msgs/msg/Twist`".
    pub msg_type: String,
    /// Serialised message payload. Stored as CDR bytes; decode on read.
    pub payload_cdr: Vec<u8>,
    /// Optional structured annotation emitted by the robot's own code.
    pub annotation: Option<EventAnnotation>,
}

impl TelemetryEvent {
    /// Checks the invariants that make an event usable as forensic evidence.
    ///
    /// Sources must call this before handing an event downstream so that a
    /// bad record is rejected at the ingest boundary, where file/offset
    /// context still exists, rather than deep inside the store.
    ///
    /// # Errors
    ///
    /// Returns the first violated invariant: empty partitioning keys, empty
    /// topic, or an annotation that breaks the metadata bounds.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.robot_id.is_empty() {
            return Err(ValidationError::EmptyField { field: "robot_id" });
        }
        if self.session_id.is_empty() {
            return Err(ValidationError::EmptyField {
                field: "session_id",
            });
        }
        if self.topic.is_empty() {
            return Err(ValidationError::EmptyField { field: "topic" });
        }
        if let Some(annotation) = &self.annotation {
            annotation.validate()?;
        }
        Ok(())
    }
}

/// A structured decision-point annotation, emitted explicitly by robot
/// application code via the roxt annotation SDK.
///
/// This is the layer that separates roxt from a dumb bag recorder.
/// These annotations become the primary signal for incident reconstruction
/// and insurability scoring.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventAnnotation {
    /// Short machine-readable label, e.g. "`OBSTACLE_DETECTED`", "`ESTOP_TRIGGERED`".
    pub kind: String,
    /// Human-readable description for post-mortem review.
    pub description: String,
    /// Severity level. Use sparingly — must mean something.
    pub severity: Severity,
    /// Arbitrary key-value metadata (bounded: max 32 pairs, keys ≤ 64 chars).
    pub metadata: BTreeMap<String, String>,
}

impl EventAnnotation {
    /// Enforces the documented metadata bounds and a non-empty kind.
    ///
    /// # Errors
    ///
    /// Returns an error when `kind` is empty, the map holds more than
    /// [`MAX_METADATA_PAIRS`] entries, or any key exceeds
    /// [`MAX_METADATA_KEY_LEN`] bytes.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.kind.is_empty() {
            return Err(ValidationError::EmptyField {
                field: "annotation.kind",
            });
        }
        if self.metadata.len() > MAX_METADATA_PAIRS {
            return Err(ValidationError::TooManyMetadataPairs {
                count: self.metadata.len(),
            });
        }
        if let Some(key) = self
            .metadata
            .keys()
            .find(|k| k.len() > MAX_METADATA_KEY_LEN)
        {
            return Err(ValidationError::MetadataKeyTooLong {
                key: key.clone(),
                len: key.len(),
            });
        }
        Ok(())
    }
}

/// Severity of an annotation, mirroring conventional log levels.
///
/// Severity drives incident triage: `Error` and `Fatal` events are what an
/// insurer's forensic query pulls first, so emitting code must not inflate
/// levels. Stored in `SQLite` as the upper-case text form for greppability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    /// Developer-facing detail; never used for incident scoring.
    Debug,
    /// Normal operational milestones (mission started, goal reached).
    Info,
    /// Degraded but recoverable conditions (re-planning, sensor dropout).
    Warn,
    /// A failed operation the robot could not complete as commanded.
    Error,
    /// Safety-relevant termination (e-stop, watchdog kill).
    Fatal,
}

impl Severity {
    /// The canonical storage form, shared by `SQLite` rows and CLI output so
    /// that the same string round-trips everywhere.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
            Self::Fatal => "FATAL",
        }
    }
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Severity {
    type Err = ValidationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "DEBUG" => Ok(Self::Debug),
            "INFO" => Ok(Self::Info),
            "WARN" => Ok(Self::Warn),
            "ERROR" => Ok(Self::Error),
            "FATAL" => Ok(Self::Fatal),
            other => Err(ValidationError::InvalidSeverity {
                value: other.to_owned(),
            }),
        }
    }
}

#[cfg(test)]
mod tests;
