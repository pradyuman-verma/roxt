use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

use roxt_core::{IngestError, TelemetrySource};

use crate::McapSource;

/// Writes a tiny MCAP file with `count` messages alternating over two
/// topics, `log_time` = message index in nanoseconds.
fn write_mcap(path: &Path, count: u64) {
    let file = BufWriter::new(File::create(path).expect("create"));
    let mut writer = mcap::Writer::new(file).expect("writer");
    let schema_id = writer
        .add_schema("geometry_msgs/msg/Twist", "ros2msg", b"")
        .expect("schema");
    let cmd_vel = writer
        .add_channel(schema_id, "/cmd_vel", "cdr", &BTreeMap::new())
        .expect("channel");
    let odom = writer
        .add_channel(schema_id, "/odom", "cdr", &BTreeMap::new())
        .expect("channel");
    for i in 0..count {
        let channel_id = if i % 2 == 0 { cmd_vel } else { odom };
        writer
            .write_to_known_channel(
                &mcap::records::MessageHeader {
                    channel_id,
                    sequence: u32::try_from(i).expect("sequence"),
                    log_time: i,
                    publish_time: i,
                },
                &i.to_le_bytes(),
            )
            .expect("write message");
    }
    writer.finish().expect("finish");
}

fn drain(source: &mut McapSource) -> Vec<roxt_core::TelemetryEvent> {
    let mut events = Vec::new();
    while let Some(event) = source.next_event().expect("next_event") {
        events.push(event);
    }
    events
}

#[test]
fn reads_all_messages_with_expected_fields() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("sample.mcap");
    write_mcap(&path, 10);
    let mut source = McapSource::new(&path, "amr-unit-042", "session-001").expect("open");
    let events = drain(&mut source);
    assert_eq!(events.len(), 10);
    let first = &events[0];
    assert_eq!(first.stamp_ns, 0);
    assert_eq!(first.robot_id, "amr-unit-042");
    assert_eq!(first.session_id, "session-001");
    assert_eq!(first.topic, "/cmd_vel");
    assert_eq!(first.msg_type, "geometry_msgs/msg/Twist");
    assert_eq!(first.payload_cdr, 0u64.to_le_bytes().to_vec());
    assert!(first.ingested_at_ns > 0);
}

#[test]
fn timestamps_are_monotonically_non_decreasing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("sample.mcap");
    write_mcap(&path, 50);
    let mut source = McapSource::new(&path, "amr-unit-042", "session-001").expect("open");
    let events = drain(&mut source);
    assert!(events.windows(2).all(|w| w[0].stamp_ns <= w[1].stamp_ns));
}

#[test]
fn empty_file_yields_zero_events_and_stays_exhausted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("empty.mcap");
    write_mcap(&path, 0);
    let mut source = McapSource::new(&path, "amr-unit-042", "session-001").expect("open");
    assert!(source.next_event().expect("first poll").is_none());
    // Exhaustion is idempotent: polling again must not error or hang.
    assert!(source.next_event().expect("second poll").is_none());
}

#[test]
fn missing_file_fails_before_streaming() {
    let result = McapSource::new(Path::new("/nonexistent/never.mcap"), "r", "s");
    assert!(matches!(result, Err(IngestError::SourceIo { .. })));
}

#[test]
fn empty_robot_id_is_rejected_before_io() {
    let result = McapSource::new(Path::new("/nonexistent/never.mcap"), "", "s");
    assert!(matches!(result, Err(IngestError::InvalidEvent(_))));
}

#[test]
fn truncated_file_surfaces_a_locatable_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("truncated.mcap");
    write_mcap(&path, 10);
    let bytes = std::fs::read(&path).expect("read");
    std::fs::write(&path, &bytes[..bytes.len() / 2]).expect("truncate");
    let mut source = McapSource::new(&path, "amr-unit-042", "session-001").expect("open");
    let mut saw_error = false;
    loop {
        match source.next_event() {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(error) => {
                saw_error = true;
                assert!(matches!(
                    error,
                    IngestError::CorruptPayload { .. } | IngestError::MalformedBag { .. }
                ));
                break;
            }
        }
    }
    assert!(saw_error, "truncated file must surface an error");
}

#[test]
fn dropping_a_partially_consumed_source_does_not_hang() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("sample.mcap");
    write_mcap(&path, 5000);
    let mut source = McapSource::new(&path, "amr-unit-042", "session-001").expect("open");
    let _first = source.next_event().expect("next_event");
    drop(source); // Must join the reader thread promptly.
}
