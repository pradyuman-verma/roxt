# Changelog

All notable changes to roxt are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow SemVer.

## [Unreleased]

### Added

- Workspace scaffold with the five-crate layout: `roxt-core`,
  `roxt-ingest`, `roxt-store`, `roxt-query`, `roxt-cli`.
- Canonical `TelemetryEvent` / `EventAnnotation` / `Severity` schema with
  ingest-time validation (non-empty partitioning keys; annotation metadata
  bounded to 32 pairs with 64-byte keys). Documented in `docs/schema.md`.
- Structured `thiserror` hierarchy (`RoxtError`, `IngestError`,
  `StoreError`, `QueryError`, `ValidationError`).
- `TelemetrySource` trait and two implementations: `McapSource` (streaming
  via a reader thread and bounded channel; never buffers the file in heap)
  and `Ros2BagSource` (metadata.yaml checked before the database is
  touched; rowid-paginated streaming). Feature-gated `ZenohSource` stub.
- SQLite store with WAL mode, versioned append-only embedded migrations
  (schema version 1), and transactional 500-event batch writes that report
  the exact failed range.
- Query engine with deterministic `(stamp_ns, rowid)` ordering, read-only
  database access, and robot/topic/time-window filters.
- CLI: `roxt rec`, `roxt query` (table/json/csv; ns or RFC3339
  timestamps), `roxt replay` (speed-scaled), `roxt diff` (exit 1 on
  divergence, ingest-time differences ignored by design).
- Test suite: unit tests per crate, proptest round-trip and metadata-bound
  properties, and end-to-end integration tests in `tests/integration`
  against MCAP fixtures in `tests/fixtures`.
