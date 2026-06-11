//! Opt-in smoke test against real ROS 2 MCAP data (JKK Research Center
//! `DATASET_02`, <https://jkk-research.github.io/dataset/>).
//!
//! The dataset is licensed for research/educational use without a
//! redistribution grant, so it is never vendored into this repository.
//! Point `ROXT_JKK_MCAP` at a downloaded `.mcap` and run explicitly:
//!
//! ```text
//! ROXT_JKK_MCAP=/path/to/file.mcap \
//!     cargo test -p roxt-integration --test jkk_smoke -- --ignored
//! ```
//!
//! In CI this runs only when the `JKK_MCAP_URL` repository variable is
//! configured (see .github/workflows/ci.yml); it is not part of the gate.

use std::path::PathBuf;

use roxt_core::TelemetrySource;
use roxt_ingest::McapSource;

fn jkk_mcap() -> PathBuf {
    let path = std::env::var_os("ROXT_JKK_MCAP")
        .expect("ROXT_JKK_MCAP must point at a JKK dataset .mcap file");
    let path = PathBuf::from(path);
    assert!(path.exists(), "ROXT_JKK_MCAP does not exist: {path:?}");
    path
}

#[test]
#[ignore = "needs real JKK dataset; set ROXT_JKK_MCAP and run explicitly"]
fn real_ros2_mcap_ingests_end_to_end() {
    let input = jkk_mcap();

    // Stream-count first: every message must either convert cleanly or
    // surface a typed error (no panics on real-world data).
    let mut source = McapSource::new(&input, "test-001", "jkk-ds02").expect("open mcap");
    let mut streamed: usize = 0;
    while let Some(event) = source.next_event().expect("next_event on real data") {
        assert!(!event.topic.is_empty());
        streamed += 1;
    }
    assert!(streamed > 0, "dataset file should contain messages");

    // Then the full pipeline: ingest into SQLite and read everything back.
    // No monotonicity assertion — real multi-node recordings interleave
    // clocks, and roxt records what happened rather than what was tidy.
    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("jkk.db");
    let mut source = McapSource::new(&input, "test-001", "jkk-ds02").expect("reopen mcap");
    let mut store = roxt_store::SqliteStore::open(&db).expect("open store");
    let mut batch = Vec::with_capacity(roxt_store::BATCH_SIZE);
    let mut written = 0;
    while let Some(event) = source.next_event().expect("next_event") {
        batch.push(event);
        if batch.len() == roxt_store::BATCH_SIZE {
            written += store.insert_events(&batch).expect("insert");
            batch.clear();
        }
    }
    written += store.insert_events(&batch).expect("insert tail");
    assert_eq!(written, streamed);

    let stored = roxt_query::query_events(&db, &roxt_query::QueryFilter::default())
        .expect("query everything back");
    assert_eq!(stored.len(), streamed, "no events lost or invented");
}
