# roxt

> Structured telemetry recorder and query tool for ROS 2 robot fleets.

![Rust](https://img.shields.io/badge/rust-1.85%2B-orange?logo=rust)
![License](https://img.shields.io/badge/license-MIT-blue)
![Status](https://img.shields.io/badge/status-v0.1%20pre--release-yellow)

**roxt** ingests robot telemetry from ROS 2 recordings (MCAP, rosbag2),
normalises it into a typed, queryable event schema backed by SQLite, and
gives developers and operators a CLI to record, query, replay, and diff
fleet telemetry.

Unlike a plain bag recorder, roxt is built around **structured
decision-point annotations**: events your robot code emits explicitly
(`OBSTACLE_DETECTED`, `ESTOP_TRIGGERED`, …) that become the primary signal
for incident reconstruction. The event schema is designed to serve as
forensic evidence after a robot incident — every field is explicit, every
timestamp is nanosecond-precision, and nothing is defaulted silently.

---

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [CLI Reference](#cli-reference)
- [Architecture](#architecture)
- [Event Schema](#event-schema)
- [Development](#development)
- [Roadmap](#roadmap)
- [License](#license)

## Features

- **Multiple ingestion sources** — MCAP files (Foxglove container format)
  and rosbag2 (sqlite3 storage), behind one transport-agnostic
  `TelemetrySource` trait. Live Zenoh capture is planned for v2.
- **Streaming by design** — recordings are never buffered wholesale in
  memory; multi-gigabyte incident bags ingest in bounded RAM.
- **Forensic-grade storage** — WAL-mode SQLite, versioned append-only
  migrations, transactional 500-event batch writes that report the exact
  failed range. Events are never silently dropped.
- **Deterministic queries** — identical query, identical database,
  byte-identical output. Filter by robot, topic, and nanosecond or RFC3339
  time windows; render as table, JSON, or CSV.
- **Timeline diffing** — compare two databases of the same session and get
  the first point of divergence, `diff(1)`-style exit codes included.
- **Strict validation at the ingest boundary** — empty partitioning keys
  and out-of-bounds annotation metadata are rejected where file context
  still exists, not deep inside the store.

## Installation

### From source

Requires Rust **1.85+** ([rustup.rs](https://rustup.rs)). SQLite is bundled;
there are no system dependencies.

```sh
git clone https://github.com/pradyuman-verma/roxt
cd roxt
cargo install --path crates/roxt-cli
```

Or build without installing:

```sh
cargo build --release
# binary at target/release/roxt
```

## Quick Start

Record an MCAP file into a roxt database:

```sh
roxt rec --source mcap \
         --input warehouse-run.mcap \
         --robot-id amr-unit-042 \
         --session-id shift-2026-06-11-am \
         --out fleet.db
# recorded 31742 events from warehouse-run.mcap into fleet.db
```

Query an incident window:

```sh
roxt query --db fleet.db \
           --robot-id amr-unit-042 \
           --topic /cmd_vel \
           --from 2026-06-11T09:14:00Z \
           --to   2026-06-11T09:15:00Z \
           --format json
```

Replay a session at 20x to skim it:

```sh
roxt replay --db fleet.db --speed 20
```

Verify two recordings captured the same timeline:

```sh
roxt diff --db-a robot-local.db --db-b fleet-aggregate.db --robot-id amr-unit-042
# no divergence: 31742 events compared in both databases
```

## CLI Reference

### `roxt rec`

Ingest a recording into a roxt database.

| Flag                              | Description                                                                |
| --------------------------------- | -------------------------------------------------------------------------- |
| `--source <mcap\|ros2bag\|zenoh>` | Source format. `zenoh` is reserved for v2.                                 |
| `--input <path>`                  | MCAP file, or rosbag2 directory / `.db3` file.                             |
| `--robot-id <id>`                 | Stable robot identifier (e.g. `amr-unit-042`). Required — never defaulted. |
| `--session-id <id>`               | One per recording or live run. Required — never defaulted.                 |
| `--out <path>`                    | Output SQLite database (created if absent).                                |

### `roxt query`

Filtered, deterministic read of stored events.

| Flag                          | Description                                                 |
| ----------------------------- | ----------------------------------------------------------- |
| `--db <path>`                 | Database to query.                                          |
| `--robot-id <id>`             | Optional robot filter.                                      |
| `--topic <topic>`             | Optional topic filter.                                      |
| `--from <ts>`                 | Inclusive lower bound — nanoseconds since epoch or RFC3339. |
| `--to <ts>`                   | Exclusive upper bound — same formats.                       |
| `--format <table\|json\|csv>` | Output format (default `table`).                            |

### `roxt replay`

Re-emit stored events to stdout at recorded (or scaled) pace.

| Flag          | Description                                |
| ------------- | ------------------------------------------ |
| `--db <path>` | Database to replay.                        |
| `--speed <f>` | Playback speed multiplier (default `1.0`). |

### `roxt diff`

Compare event timelines of two databases. Exits `0` when identical, `1` on
divergence. Ingest-time differences (`ingested_at_ns`) are deliberately
ignored: two recordings of the same session are the same timeline.

| Flag                              | Description                                 |
| --------------------------------- | ------------------------------------------- |
| `--db-a <path>` / `--db-b <path>` | Databases to compare.                       |
| `--robot-id <id>`                 | Optional: restrict comparison to one robot. |

## Architecture

Cargo workspace; crates depend inward only, and the CLI is the only binary.

```
                ┌─────────────────────────────────────────────┐
                │                  roxt-cli                   │
                │        rec · query · replay · diff          │
                └──────┬──────────────┬──────────────┬────────┘
                       │              │              │
            ┌──────────▼───┐   ┌──────▼─────┐  ┌─────▼──────┐
            │ roxt-ingest  │   │ roxt-store │  │ roxt-query │
            │ McapSource   │──▶│ SQLite WAL │◀─│ filters,   │
            │ Ros2BagSource│   │ migrations │  │ ordering   │
            │ (ZenohSource)│   │ 500/batch  │  │ read-only  │
            └──────────┬───┘   └──────┬─────┘  └─────┬──────┘
                       │              │              │
                ┌──────▼──────────────▼──────────────▼────────┐
                │                 roxt-core                   │
                │  TelemetryEvent · errors · TelemetrySource  │
                └─────────────────────────────────────────────┘
```

Data flow: **source → parse → validate → batch → transactional write**.
Only the current batch is held in memory.

| Crate                               | Role                                                              |
| ----------------------------------- | ----------------------------------------------------------------- |
| [`roxt-core`](crates/roxt-core)     | Canonical event schema, error hierarchy, `TelemetrySource` trait. |
| [`roxt-ingest`](crates/roxt-ingest) | MCAP and rosbag2 sources; feature-gated Zenoh stub.               |
| [`roxt-store`](crates/roxt-store)   | SQLite writer: WAL, versioned migrations, batched transactions.   |
| [`roxt-query`](crates/roxt-query)   | Read-only query engine with deterministic ordering.               |
| [`roxt-cli`](crates/roxt-cli)       | The `roxt` binary: argument parsing, wiring, output formatting.   |

## Event Schema

The full contract lives in [`docs/schema.md`](docs/schema.md). In short,
every stored event is:

```rust
pub struct TelemetryEvent {
    pub stamp_ns: i64,            // ROS 2 node clock, ns since epoch
    pub ingested_at_ns: i64,      // roxt daemon clock at ingest
    pub robot_id: String,         // required, never defaulted
    pub session_id: String,       // required, never defaulted
    pub topic: String,            // e.g. "/cmd_vel"
    pub msg_type: String,         // e.g. "geometry_msgs/msg/Twist"
    pub payload_cdr: Vec<u8>,     // raw CDR bytes — the evidence
    pub annotation: Option<EventAnnotation>,
}
```

Annotations carry a machine-readable `kind`, human-readable `description`,
a `Severity` (`DEBUG`–`FATAL`), and bounded key-value metadata (max 32
pairs, keys ≤ 64 bytes). Indices cover `(robot_id, session_id, stamp_ns)`
and `(topic, stamp_ns)`.

## Development

```sh
# the full verification gate — all four must pass before merging
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
cargo test -p roxt-integration
```

- Integration tests live in [`tests/integration`](tests/integration) and run
  the real `roxt` binary against the MCAP fixtures in
  [`tests/fixtures`](tests/fixtures), including the vendored upstream
  [MCAP conformance files](tests/fixtures/conformance/README.md).
- Synthetic fixtures are deterministic and regenerable:
  `cargo test -p roxt-integration --test fixture_gen -- --ignored`.
- An opt-in smoke test runs against real ROS 2 data
  ([JKK DATASET_02](https://jkk-research.github.io/dataset/) — research/
  educational license, never vendored):
  `ROXT_JKK_MCAP=/path/to/file.mcap cargo test -p roxt-integration --test jkk_smoke -- --ignored`.
  CI runs it automatically when the `JKK_MCAP_URL` repository variable is set.
- Library crates use structured `thiserror` errors and `tracing` — no
  `anyhow`, no `println!`, no `.unwrap()` outside tests.
- Schema changes require updating `docs/schema.md`, the Rust types, and a
  new migration version in the same commit. Migrations are append-only.
- User-visible changes get a [`CHANGELOG.md`](CHANGELOG.md) entry.

## Roadmap

- [x] MCAP and rosbag2 ingestion, SQLite storage, query/replay/diff CLI
- [ ] Recording daemon mode (long-running, multi-session)
- [ ] Python annotation SDK — emit decision-point events from robot nodes
- [ ] Live Zenoh session capture ([design notes](docs/zenoh-plan.md))
- [ ] Parquet export for fleet-scale analytics

Out of scope by design: GUIs and dashboards (use [Foxglove](https://foxglove.dev)),
real-time streaming analytics, ML anomaly detection, multi-robot clock
reconciliation.

## License

MIT. See [LICENSE](LICENSE).
