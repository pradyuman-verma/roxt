//! Deterministic fixture generator, kept in-tree so the checked-in MCAP
//! fixtures in `tests/fixtures/` are reproducible.
//!
//! The fixtures are small synthetic recordings (Foxglove's public sample
//! datasets are tens of megabytes — too large to vendor), written with the
//! same `mcap` crate the reader uses. Regenerate with:
//!
//! ```text
//! cargo test -p roxt-integration --test fixture_gen -- --ignored
//! ```
//!
//! Determinism matters: the expected-output fixtures (e.g.
//! `expected_query.json`) are byte-compared against pipeline output, so
//! nothing here may depend on wall-clock time or randomness.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

use mcap::records::MessageHeader;
use mcap::{Compression, WriteOptions, Writer};

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures")
}

/// Base stamp: 2023-11-14T22:13:20Z, comfortably in the i64 range.
const BASE_STAMP_NS: u64 = 1_700_000_000_000_000_000;

/// Messages in sample.mcap; integration tests assert this exact count.
const SAMPLE_MESSAGE_COUNT: u64 = 24;

/// Messages in each compression-variant fixture.
const COMPRESSION_MESSAGE_COUNT: u64 = 12;

/// Stamp used by clock_skew.mcap: 2001-03-09T09:46:40Z, decades away from
/// any plausible ingest wall clock.
const SKEWED_STAMP_NS: u64 = 984_130_000_000_000_000;

#[test]
#[ignore = "writes checked-in fixtures; run explicitly to regenerate"]
fn generate_fixtures() {
    write_sample(&fixtures_dir().join("sample.mcap"));
    write_empty(&fixtures_dir().join("empty.mcap"));
    write_compression_variant(
        &fixtures_dir().join("sample_zstd.mcap"),
        Some(Compression::Zstd),
    );
    write_compression_variant(
        &fixtures_dir().join("sample_lz4.mcap"),
        Some(Compression::Lz4),
    );
    write_compression_variant(&fixtures_dir().join("sample_uncompressed.mcap"), None);
    write_clock_skew(&fixtures_dir().join("clock_skew.mcap"));
    write_overflow(&fixtures_dir().join("overflow.mcap"));
}

fn write_sample(path: &Path) {
    let file = BufWriter::new(File::create(path).expect("create sample.mcap"));
    let mut writer = Writer::new(file).expect("writer");
    let topics = [
        ("/cmd_vel", "geometry_msgs/msg/Twist"),
        ("/odom", "nav_msgs/msg/Odometry"),
        ("/diagnostics", "diagnostic_msgs/msg/DiagnosticArray"),
    ];
    let channels: Vec<u16> = topics
        .iter()
        .map(|(topic, msg_type)| {
            let schema_id = writer.add_schema(msg_type, "ros2msg", b"").expect("schema");
            writer
                .add_channel(schema_id, topic, "cdr", &BTreeMap::new())
                .expect("channel")
        })
        .collect();
    for i in 0..SAMPLE_MESSAGE_COUNT {
        let topic_index = (i % 3) as usize;
        let log_time = BASE_STAMP_NS + i * 50_000_000; // 50 ms cadence
        write_message(&mut writer, channels[topic_index], i, log_time, topic_index);
    }
    writer.finish().expect("finish");
}

fn write_empty(path: &Path) {
    let file = BufWriter::new(File::create(path).expect("create empty.mcap"));
    let mut writer = Writer::new(file).expect("writer");
    writer.finish().expect("finish");
}

/// Same logical content for every compression mode, so tests can assert
/// the decoded events are identical regardless of container compression.
fn write_compression_variant(path: &Path, compression: Option<Compression>) {
    let file = BufWriter::new(File::create(path).expect("create compression fixture"));
    let mut writer = WriteOptions::new()
        .compression(compression)
        .create(file)
        .expect("writer");
    let schema_id = writer
        .add_schema("sensor_msgs/msg/LaserScan", "ros2msg", b"")
        .expect("schema");
    let channel = writer
        .add_channel(schema_id, "/scan", "cdr", &BTreeMap::new())
        .expect("channel");
    for i in 0..COMPRESSION_MESSAGE_COUNT {
        let log_time = BASE_STAMP_NS + i * 100_000_000;
        write_message(&mut writer, channel, i, log_time, 0);
    }
    writer.finish().expect("finish");
}

/// Robot clock decades behind any plausible daemon clock. roxt v1 must
/// record both clocks unaltered — reconciliation is explicitly out of
/// scope, and "the clocks disagree" is itself forensic signal.
fn write_clock_skew(path: &Path) {
    let file = BufWriter::new(File::create(path).expect("create clock_skew.mcap"));
    let mut writer = Writer::new(file).expect("writer");
    let schema_id = writer
        .add_schema("nav_msgs/msg/Odometry", "ros2msg", b"")
        .expect("schema");
    let channel = writer
        .add_channel(schema_id, "/odom", "cdr", &BTreeMap::new())
        .expect("channel");
    for i in 0..4 {
        write_message(
            &mut writer,
            channel,
            i,
            SKEWED_STAMP_NS + i * 1_000_000_000,
            0,
        );
    }
    writer.finish().expect("finish");
}

/// A log_time that cannot fit the schema's i64 stamp. MCAP allows it
/// (log_time is u64); roxt must surface CorruptPayload, not wrap silently.
fn write_overflow(path: &Path) {
    let file = BufWriter::new(File::create(path).expect("create overflow.mcap"));
    let mut writer = Writer::new(file).expect("writer");
    let schema_id = writer
        .add_schema("sensor_msgs/msg/Imu", "ros2msg", b"")
        .expect("schema");
    let channel = writer
        .add_channel(schema_id, "/imu", "cdr", &BTreeMap::new())
        .expect("channel");
    write_message(&mut writer, channel, 0, u64::MAX, 0);
    writer.finish().expect("finish");
}

fn write_message(
    writer: &mut Writer<BufWriter<File>>,
    channel_id: u16,
    index: u64,
    log_time: u64,
    topic_index: usize,
) {
    writer
        .write_to_known_channel(
            &MessageHeader {
                channel_id,
                sequence: u32::try_from(index).expect("sequence"),
                log_time,
                publish_time: log_time,
            },
            &[
                u8::try_from(index).expect("byte"),
                0xAA,
                u8::try_from(topic_index).expect("topic index"),
            ],
        )
        .expect("write message");
}
