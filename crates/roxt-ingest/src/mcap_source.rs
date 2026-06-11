//! Streaming MCAP file source.
//!
//! `mcap::MessageStream` borrows the mapped file for its whole lifetime,
//! which a safe self-owning struct cannot express. Instead of `unsafe` or a
//! self-reference crate, the reader runs on a dedicated thread that owns
//! the mmap and feeds a bounded channel. The channel bound doubles as
//! backpressure, so a slow `SQLite` writer throttles the reader instead of
//! the reader ballooning memory — the file is never buffered wholesale.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread::JoinHandle;

use roxt_core::{IngestError, TelemetryEvent, TelemetrySource};

use crate::common::{require_non_empty, wall_clock_ns};

/// Events buffered between the reader thread and the consumer. Sized for a
/// few write batches of headroom without holding a meaningful slice of a
/// large bag in memory.
const CHANNEL_BOUND: usize = 2048;

/// Reads telemetry events from an MCAP file (Foxglove container format).
pub struct McapSource {
    receiver: Option<mpsc::Receiver<Result<TelemetryEvent, IngestError>>>,
    reader: Option<JoinHandle<()>>,
}

impl McapSource {
    /// Opens `path` and starts the background reader.
    ///
    /// `robot_id` and `session_id` are stamped onto every event; they are
    /// required here precisely because an MCAP file carries neither — the
    /// operator must say which robot and run this recording belongs to.
    ///
    /// # Errors
    ///
    /// Fails fast (before reading any message) on empty identifiers or an
    /// unreadable/unmappable file.
    pub fn new(path: &Path, robot_id: &str, session_id: &str) -> Result<Self, IngestError> {
        require_non_empty(robot_id, "robot_id")?;
        require_non_empty(session_id, "session_id")?;
        let file = File::open(path).map_err(|source| IngestError::SourceIo {
            path: path.to_path_buf(),
            source,
        })?;
        // SAFETY of the mapping is the memmap2 crate's contract; roxt opens
        // the file read-only and the map lives only on the reader thread.
        let mmap =
            unsafe { memmap2::Mmap::map(&file) }.map_err(|source| IngestError::SourceIo {
                path: path.to_path_buf(),
                source,
            })?;
        let (sender, receiver) = mpsc::sync_channel(CHANNEL_BOUND);
        let ctx = ReaderContext {
            mmap,
            path: path.to_path_buf(),
            robot_id: robot_id.to_owned(),
            session_id: session_id.to_owned(),
        };
        let reader = std::thread::spawn(move || run_reader(&ctx, &sender));
        Ok(Self {
            receiver: Some(receiver),
            reader: Some(reader),
        })
    }
}

impl TelemetrySource for McapSource {
    fn next_event(&mut self) -> Result<Option<TelemetryEvent>, IngestError> {
        let Some(receiver) = &self.receiver else {
            return Ok(None);
        };
        match receiver.recv() {
            Ok(item) => item.map(Some),
            // Disconnected sender means the reader finished (or died after
            // reporting its error); either way the stream is over.
            Err(mpsc::RecvError) => {
                self.shut_down();
                Ok(None)
            }
        }
    }
}

impl McapSource {
    fn shut_down(&mut self) {
        // Dropping the receiver first unblocks a reader stuck on a full
        // channel, letting the join below complete promptly.
        self.receiver = None;
        if let Some(handle) = self.reader.take() {
            // A panicked reader already surfaced its failure as a channel
            // error; nothing more to report here.
            drop(handle.join());
        }
    }
}

impl Drop for McapSource {
    fn drop(&mut self) {
        self.shut_down();
    }
}

struct ReaderContext {
    mmap: memmap2::Mmap,
    path: PathBuf,
    robot_id: String,
    session_id: String,
}

fn run_reader(ctx: &ReaderContext, sender: &mpsc::SyncSender<Result<TelemetryEvent, IngestError>>) {
    let stream = match mcap::MessageStream::new(&ctx.mmap) {
        Ok(stream) => stream,
        Err(error) => {
            drop(sender.send(Err(open_error(&ctx.path, &error))));
            return;
        }
    };
    for (index, message) in stream.enumerate() {
        let item = message
            .map_err(|error| message_error(index, &error))
            .and_then(|message| convert(ctx, index, &message));
        let stop_on_error = item.is_err();
        if sender.send(item).is_err() {
            return; // Consumer dropped; stop reading.
        }
        if stop_on_error {
            return; // A corrupt record poisons the rest of the stream.
        }
    }
}

fn convert(
    ctx: &ReaderContext,
    index: usize,
    message: &mcap::Message<'_>,
) -> Result<TelemetryEvent, IngestError> {
    let stamp_ns = i64::try_from(message.log_time).map_err(|_| IngestError::CorruptPayload {
        topic: message.channel.topic.clone(),
        offset: index as u64,
        reason: format!("log_time {} overflows i64 nanoseconds", message.log_time),
    })?;
    let event = TelemetryEvent {
        stamp_ns,
        ingested_at_ns: wall_clock_ns(),
        robot_id: ctx.robot_id.clone(),
        session_id: ctx.session_id.clone(),
        topic: message.channel.topic.clone(),
        // A channel without a schema (rare but legal MCAP) yields an empty
        // msg_type rather than a guess; downstream consumers must treat
        // empty as "type unrecorded".
        msg_type: message
            .channel
            .schema
            .as_ref()
            .map(|s| s.name.clone())
            .unwrap_or_default(),
        payload_cdr: message.data.clone().into_owned(),
        annotation: None,
    };
    event.validate()?;
    Ok(event)
}

fn open_error(path: &Path, error: &mcap::McapError) -> IngestError {
    IngestError::MalformedBag {
        path: path.to_path_buf(),
        reason: error.to_string(),
    }
}

/// MCAP chunks are compressed, so a byte offset into the file would not
/// identify a record; the message ordinal is the locatable unit instead.
fn message_error(index: usize, error: &mcap::McapError) -> IngestError {
    IngestError::CorruptPayload {
        topic: String::from("<unknown>"),
        offset: index as u64,
        reason: error.to_string(),
    }
}

#[cfg(test)]
mod tests;
