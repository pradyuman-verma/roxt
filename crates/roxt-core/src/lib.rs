//! Core types for roxt: the canonical telemetry event schema, the error
//! hierarchy shared by every crate, and the [`TelemetrySource`] trait that
//! every ingestion backend implements.
//!
//! This crate sits at the bottom of the dependency graph. It must never
//! depend on ingestion, storage, query, or CLI crates — the event schema
//! defined here is the forensic record of what a robot did, and everything
//! else in the workspace is built around it.

pub mod error;
pub mod event;
pub mod source;

pub use error::{IngestError, QueryError, RoxtError, StoreError, ValidationError};
pub use event::{EventAnnotation, Severity, TelemetryEvent};
pub use source::TelemetrySource;
