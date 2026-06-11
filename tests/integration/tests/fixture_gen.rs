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

use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures")
}

/// Base stamp: 2023-11-14T22:13:20Z, comfortably in the i64 range.
const BASE_STAMP_NS: u64 = 1_700_000_000_000_000_000;

/// Messages in sample.mcap; integration tests assert this exact count.
const SAMPLE_MESSAGE_COUNT: u64 = 24;

#[test]
#[ignore = "writes checked-in fixtures; run explicitly to regenerate"]
fn generate_fixtures() {
    write_sample(&fixtures_dir().join("sample.mcap"));
    write_empty(&fixtures_dir().join("empty.mcap"));
}

fn write_sample(path: &Path) {
    let file = BufWriter::new(File::create(path).expect("create sample.mcap"));
    let mut writer = mcap::Writer::new(file).expect("writer");
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
        writer
            .write_to_known_channel(
                &mcap::records::MessageHeader {
                    channel_id: channels[topic_index],
                    sequence: u32::try_from(i).expect("sequence"),
                    log_time,
                    publish_time: log_time,
                },
                &[
                    u8::try_from(i).expect("byte"),
                    0xAA,
                    u8::try_from(topic_index).expect("topic index"),
                ],
            )
            .expect("write message");
    }
    writer.finish().expect("finish");
}

fn write_empty(path: &Path) {
    let file = BufWriter::new(File::create(path).expect("create empty.mcap"));
    let mut writer = mcap::Writer::new(file).expect("writer");
    writer.finish().expect("finish");
}
