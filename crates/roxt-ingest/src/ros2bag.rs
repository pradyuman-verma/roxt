//! rosbag2 (`SQLite3` storage) source.
//!
//! The metadata.yaml sidecar is read *before* the database is touched: it
//! declares the metadata version and storage backend, and ingesting a bag
//! whose layout we merely guessed at would taint the forensic record.
//! Messages are then streamed out of the `.db3` in rowid-paginated batches,
//! so a multi-gigabyte bag never sits in memory.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use roxt_core::{IngestError, TelemetryEvent, TelemetrySource};
use rusqlite::{Connection, OpenFlags};
use serde::Deserialize;

use crate::common::{require_non_empty, wall_clock_ns};

/// Highest rosbag2 metadata version this build understands (Jazzy era).
/// Newer bags fail loudly instead of being half-read.
const MAX_SUPPORTED_METADATA_VERSION: u32 = 9;

/// Rows fetched per pagination step. Small enough to bound memory, large
/// enough that `SQLite` query overhead is amortised.
const FETCH_BATCH: usize = 256;

#[derive(Deserialize)]
struct BagMetadataFile {
    rosbag2_bagfile_information: BagInfo,
}

#[derive(Deserialize)]
struct BagInfo {
    version: u32,
    storage_identifier: String,
}

/// Reads telemetry events from a rosbag2 recording (sqlite3 storage).
pub struct Ros2BagSource {
    conn: Connection,
    db_path: PathBuf,
    robot_id: String,
    session_id: String,
    last_rowid: i64,
    buffer: VecDeque<TelemetryEvent>,
    exhausted: bool,
}

impl Ros2BagSource {
    /// Opens a bag directory (containing `metadata.yaml`) or a `.db3` file
    /// whose `metadata.yaml` sits beside it.
    ///
    /// # Errors
    ///
    /// Fails before reading any message when identifiers are empty, the
    /// metadata is missing or malformed, the metadata version is newer
    /// than [`MAX_SUPPORTED_METADATA_VERSION`], or the storage backend is
    /// not sqlite3.
    pub fn new(input: &Path, robot_id: &str, session_id: &str) -> Result<Self, IngestError> {
        require_non_empty(robot_id, "robot_id")?;
        require_non_empty(session_id, "session_id")?;
        let (metadata_path, db_path) = locate_bag_files(input)?;
        check_metadata(&metadata_path)?;
        let conn = Connection::open_with_flags(&db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|e| IngestError::MalformedBag {
                path: db_path.clone(),
                reason: e.to_string(),
            })?;
        Ok(Self {
            conn,
            db_path,
            robot_id: robot_id.to_owned(),
            session_id: session_id.to_owned(),
            last_rowid: 0,
            buffer: VecDeque::new(),
            exhausted: false,
        })
    }

    fn refill(&mut self) -> Result<(), IngestError> {
        let db_path = &self.db_path;
        let map_err = |e: rusqlite::Error| IngestError::MalformedBag {
            path: db_path.clone(),
            reason: format!("failed reading messages table: {e}"),
        };
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT m.id, m.timestamp, m.data, t.name, t.type
                 FROM messages m JOIN topics t ON t.id = m.topic_id
                 WHERE m.id > ?1 ORDER BY m.id LIMIT ?2",
            )
            .map_err(map_err)?;
        let ingested_at_ns = wall_clock_ns();
        let rows = stmt
            .query_map(
                rusqlite::params![
                    self.last_rowid,
                    i64::try_from(FETCH_BATCH).unwrap_or(i64::MAX)
                ],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .map_err(map_err)?;
        for row in rows {
            let (rowid, stamp_ns, payload_cdr, topic, msg_type) = row.map_err(map_err)?;
            self.last_rowid = rowid;
            let event = TelemetryEvent {
                stamp_ns,
                ingested_at_ns,
                robot_id: self.robot_id.clone(),
                session_id: self.session_id.clone(),
                topic,
                msg_type,
                payload_cdr,
                annotation: None,
            };
            event.validate()?;
            self.buffer.push_back(event);
        }
        if self.buffer.is_empty() {
            self.exhausted = true;
        }
        Ok(())
    }
}

impl TelemetrySource for Ros2BagSource {
    fn next_event(&mut self) -> Result<Option<TelemetryEvent>, IngestError> {
        if self.buffer.is_empty() && !self.exhausted {
            self.refill()?;
        }
        Ok(self.buffer.pop_front())
    }
}

/// Resolves the metadata.yaml and .db3 paths from either a bag directory
/// or a direct .db3 path.
fn locate_bag_files(input: &Path) -> Result<(PathBuf, PathBuf), IngestError> {
    if input.is_dir() {
        let metadata = input.join("metadata.yaml");
        let db = first_db3_in(input)?;
        return Ok((metadata, db));
    }
    let metadata = input
        .parent()
        .map(|p| p.join("metadata.yaml"))
        .ok_or_else(|| IngestError::MalformedBag {
            path: input.to_path_buf(),
            reason: "input file has no parent directory".to_owned(),
        })?;
    Ok((metadata, input.to_path_buf()))
}

fn first_db3_in(dir: &Path) -> Result<PathBuf, IngestError> {
    let entries = std::fs::read_dir(dir).map_err(|source| IngestError::SourceIo {
        path: dir.to_path_buf(),
        source,
    })?;
    let mut db3: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "db3"))
        .collect();
    db3.sort();
    db3.into_iter()
        .next()
        .ok_or_else(|| IngestError::MalformedBag {
            path: dir.to_path_buf(),
            reason: "no .db3 file in bag directory".to_owned(),
        })
}

fn check_metadata(path: &Path) -> Result<(), IngestError> {
    let text = std::fs::read_to_string(path).map_err(|source| IngestError::SourceIo {
        path: path.to_path_buf(),
        source,
    })?;
    let metadata: BagMetadataFile =
        serde_yaml::from_str(&text).map_err(|e| IngestError::MalformedBag {
            path: path.to_path_buf(),
            reason: format!("unparseable metadata.yaml: {e}"),
        })?;
    let info = metadata.rosbag2_bagfile_information;
    if info.version > MAX_SUPPORTED_METADATA_VERSION {
        return Err(IngestError::SchemaVersionMismatch {
            expected: MAX_SUPPORTED_METADATA_VERSION,
            found: info.version,
        });
    }
    if info.storage_identifier != "sqlite3" {
        return Err(IngestError::UnsupportedFormat {
            format: format!("rosbag2 storage '{}'", info.storage_identifier),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests;
