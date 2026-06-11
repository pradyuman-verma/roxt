//! `SQLite` persistence for telemetry events.
//!
//! The store is the system of record for incident forensics, so the rules
//! here are strict: WAL mode always on (daemon-safe concurrent reads),
//! versioned append-only migrations, and transactional batch writes that
//! either land completely or report exactly which range was lost.

mod migrations;
pub mod row;

use std::path::{Path, PathBuf};

use roxt_core::{StoreError, TelemetryEvent};
use rusqlite::Connection;

/// Events per write transaction. Large enough to amortise fsync cost on a
/// busy ingest, small enough that a failed batch loses a bounded,
/// re-ingestable range.
pub const BATCH_SIZE: usize = 500;

/// A writable handle to a roxt `SQLite` database.
///
/// Opening runs all pending migrations; an old database is upgraded in
/// place, and a database from a *newer* roxt refuses to open rather than
/// risk silently misreading columns this build does not know about.
pub struct SqliteStore {
    conn: Connection,
    path: PathBuf,
}

impl SqliteStore {
    /// Opens (creating if absent) the database at `path`, enables WAL, and
    /// applies pending migrations.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::DatabaseOpen`] if the file cannot be opened or
    /// configured, and [`StoreError::SchemaMigration`] if a migration fails.
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        let conn = Connection::open(path).map_err(|source| StoreError::DatabaseOpen {
            path: path.to_path_buf(),
            source,
        })?;
        // WAL is non-negotiable: the daemon writes while operators query.
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|source| StoreError::DatabaseOpen {
                path: path.to_path_buf(),
                source,
            })?;
        migrations::apply_pending(&conn)?;
        Ok(Self {
            conn,
            path: path.to_path_buf(),
        })
    }

    /// The path this store was opened at, for diagnostics.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Persists events in transactions of [`BATCH_SIZE`], returning the
    /// number written.
    ///
    /// Events must already be validated at the ingest boundary; the store
    /// deliberately does not re-validate so that a single layer owns each
    /// invariant.
    ///
    /// # Errors
    ///
    /// On failure the open transaction rolls back and
    /// [`StoreError::WriteFailure`] reports the first stamp of the failed
    /// batch; earlier batches stay committed. The failed range is also
    /// logged so operators can re-ingest exactly what was lost.
    pub fn insert_events(&mut self, events: &[TelemetryEvent]) -> Result<usize, StoreError> {
        let mut written = 0;
        for batch in events.chunks(BATCH_SIZE) {
            self.insert_batch(batch)?;
            written += batch.len();
        }
        Ok(written)
    }

    fn insert_batch(&mut self, batch: &[TelemetryEvent]) -> Result<(), StoreError> {
        let first_stamp = batch.first().map_or(0, |e| e.stamp_ns);
        let last_stamp = batch.last().map_or(0, |e| e.stamp_ns);
        let result = self.try_insert_batch(batch, first_stamp);
        if let Err(error) = &result {
            tracing::error!(
                batch_len = batch.len(),
                first_stamp_ns = first_stamp,
                last_stamp_ns = last_stamp,
                %error,
                "event batch write failed; range rolled back"
            );
        }
        result
    }

    fn try_insert_batch(
        &mut self,
        batch: &[TelemetryEvent],
        first_stamp: i64,
    ) -> Result<(), StoreError> {
        let map_err = |source: rusqlite::Error| StoreError::WriteFailure {
            event_stamp_ns: first_stamp,
            source,
        };
        let tx = self.conn.transaction().map_err(map_err)?;
        {
            let mut stmt = tx.prepare_cached(row::INSERT_SQL).map_err(map_err)?;
            for event in batch {
                stmt.execute(rusqlite::params_from_iter(row::insert_params(event)))
                    .map_err(map_err)?;
            }
        }
        tx.commit().map_err(map_err)
    }
}

#[cfg(test)]
mod tests;
