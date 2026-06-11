//! `McapSource` against the upstream MCAP conformance files (vendored in
//! `tests/fixtures/conformance/`, see its README for provenance).
//!
//! The `mcap` crate is itself tested against these upstream; the point
//! here is pinning *roxt's* observable behaviour — counts, schemaless
//! handling, non-message records skipped — so an `mcap` crate upgrade
//! that changes semantics fails loudly in our suite, not in production.

use std::path::PathBuf;

use roxt_core::{TelemetryEvent, TelemetrySource};
use roxt_ingest::McapSource;

fn conformance(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../fixtures/conformance")
        .join(name)
}

fn read_all(name: &str) -> Vec<TelemetryEvent> {
    let mut source =
        McapSource::new(&conformance(name), "conformance-bot", "conformance").expect("open");
    let mut events = Vec::new();
    while let Some(event) = source.next_event().expect("next_event") {
        events.push(event);
    }
    events
}

#[test]
fn no_data_files_yield_zero_events() {
    assert_eq!(read_all("NoData.mcap").len(), 0);
    assert_eq!(read_all("NoData-pad-st-sum.mcap").len(), 0);
}

#[test]
fn one_message_is_read_with_schema_name_as_msg_type() {
    for file in [
        "OneMessage.mcap",
        // Same logical content with every optional section enabled
        // (chunks, indexes, padding, repeated records, summary).
        "OneMessage-ch-chx-mx-pad-rch-rsh-st-sum.mcap",
    ] {
        let events = read_all(file);
        assert_eq!(events.len(), 1, "{file}");
        assert_eq!(events[0].topic, "example", "{file}");
        assert_eq!(events[0].msg_type, "Example", "{file}");
        assert!(!events[0].payload_cdr.is_empty(), "{file}");
    }
}

#[test]
fn ten_messages_chunked_and_unchunked_agree() {
    let plain = read_all("TenMessages.mcap");
    let full = read_all("TenMessages-ch-chx-mx-pad-rch-rsh-st-sum.mcap");
    assert_eq!(plain.len(), 10);
    // Chunking/indexing/padding are container concerns; the decoded
    // telemetry must be identical apart from ingest wall-clock times.
    let strip = |events: &[TelemetryEvent]| {
        events
            .iter()
            .map(|e| {
                (
                    e.stamp_ns,
                    e.topic.clone(),
                    e.msg_type.clone(),
                    e.payload_cdr.clone(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(strip(&plain), strip(&full));
}

#[test]
fn schemaless_channel_yields_empty_msg_type() {
    let events = read_all("OneSchemalessMessage.mcap");
    assert_eq!(events.len(), 1);
    // Empty means "type unrecorded" per docs/schema.md — never a guess.
    assert_eq!(events[0].msg_type, "");
}

#[test]
fn attachment_and_metadata_records_are_skipped_not_errors() {
    assert_eq!(read_all("OneAttachment.mcap").len(), 0);
    assert_eq!(read_all("OneMetadata.mcap").len(), 0);
}
