//! Output renderers for `roxt query`.
//!
//! All three renderers are pure string builders so they can be unit-tested
//! without a terminal, and so output is byte-deterministic for a given
//! event list — a stated guarantee of `roxt query`.

use roxt_query::StoredEvent;

pub fn table(events: &[StoredEvent]) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{:<20} {:<16} {:<16} {:<24} {:<32} {:>8} {}\n",
        "stamp_ns", "robot_id", "session_id", "topic", "msg_type", "payload", "annotation"
    ));
    for stored in events {
        let event = &stored.event;
        let annotation = event
            .annotation
            .as_ref()
            .map(|a| format!("{}:{}", a.severity, a.kind))
            .unwrap_or_default();
        out.push_str(&format!(
            "{:<20} {:<16} {:<16} {:<24} {:<32} {:>8} {}\n",
            event.stamp_ns,
            event.robot_id,
            event.session_id,
            event.topic,
            event.msg_type,
            format!("{}B", event.payload_cdr.len()),
            annotation
        ));
    }
    out.push_str(&format!("({} events)\n", events.len()));
    out
}

pub fn json(events: &[StoredEvent]) -> String {
    let items: Vec<serde_json::Value> = events
        .iter()
        .map(|stored| {
            let event = &stored.event;
            let annotation = event.annotation.as_ref().map(|a| {
                serde_json::json!({
                    "kind": a.kind,
                    "description": a.description,
                    "severity": a.severity.as_str(),
                    "metadata": a.metadata,
                })
            });
            serde_json::json!({
                "stamp_ns": event.stamp_ns,
                "ingested_at_ns": event.ingested_at_ns,
                "robot_id": event.robot_id,
                "session_id": event.session_id,
                "topic": event.topic,
                "msg_type": event.msg_type,
                "payload_cdr_hex": hex(&event.payload_cdr),
                "annotation": annotation,
            })
        })
        .collect();
    let mut text = serde_json::to_string_pretty(&items).unwrap_or_else(|_| "[]".to_owned());
    text.push('\n');
    text
}

pub fn csv(events: &[StoredEvent]) -> String {
    let mut out = String::from(
        "stamp_ns,ingested_at_ns,robot_id,session_id,topic,msg_type,\
         payload_cdr_hex,annotation_kind,annotation_severity\n",
    );
    for stored in events {
        let event = &stored.event;
        let (kind, severity) = event.annotation.as_ref().map_or((String::new(), ""), |a| {
            (a.kind.clone(), a.severity.as_str())
        });
        out.push_str(&format!(
            "{},{},{},{},{},{},{},{},{}\n",
            event.stamp_ns,
            event.ingested_at_ns,
            csv_field(&event.robot_id),
            csv_field(&event.session_id),
            csv_field(&event.topic),
            csv_field(&event.msg_type),
            hex(&event.payload_cdr),
            csv_field(&kind),
            severity
        ));
    }
    out
}

/// RFC 4180 quoting, applied only when needed so common output stays clean.
fn csv_field(value: &str) -> String {
    if value.contains([',', '"', '\n']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

/// Payload bytes as lowercase hex: lossless, diff-friendly, and avoids a
/// base64 dependency for what is debug-oriented output.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut out, b| {
        // Writing to a String cannot fail; ignore the fmt::Result.
        let _ = write!(out, "{b:02x}");
        out
    })
}

#[cfg(test)]
mod tests {
    use roxt_core::TelemetryEvent;
    use roxt_query::StoredEvent;

    fn stored(stamp_ns: i64) -> StoredEvent {
        StoredEvent {
            row_id: 1,
            event: TelemetryEvent {
                stamp_ns,
                ingested_at_ns: stamp_ns,
                robot_id: "amr-1".to_owned(),
                session_id: "s1".to_owned(),
                topic: "/odom".to_owned(),
                msg_type: "nav_msgs/msg/Odometry".to_owned(),
                payload_cdr: vec![0xab, 0xcd],
                annotation: None,
            },
        }
    }

    #[test]
    fn csv_escapes_embedded_quotes_and_commas() {
        assert_eq!(super::csv_field("a,b"), "\"a,b\"");
        assert_eq!(super::csv_field("a\"b"), "\"a\"\"b\"");
        assert_eq!(super::csv_field("plain"), "plain");
    }

    #[test]
    fn json_renders_payload_as_hex() {
        let text = super::json(&[stored(5)]);
        assert!(text.contains("\"payload_cdr_hex\": \"abcd\""));
    }

    #[test]
    fn renderers_are_deterministic() {
        let events = vec![stored(1), stored(2)];
        assert_eq!(super::json(&events), super::json(&events));
        assert_eq!(super::csv(&events), super::csv(&events));
        assert_eq!(super::table(&events), super::table(&events));
    }
}
