use std::collections::BTreeMap;
use std::str::FromStr;

use crate::error::ValidationError;
use crate::event::{
    EventAnnotation, Severity, TelemetryEvent, MAX_METADATA_KEY_LEN, MAX_METADATA_PAIRS,
};

fn sample_event() -> TelemetryEvent {
    TelemetryEvent {
        stamp_ns: 1_700_000_000_000_000_000,
        ingested_at_ns: 1_700_000_001_000_000_000,
        robot_id: "amr-unit-042".to_owned(),
        session_id: "session-001".to_owned(),
        topic: "/cmd_vel".to_owned(),
        msg_type: "geometry_msgs/msg/Twist".to_owned(),
        payload_cdr: vec![0, 1, 0, 0],
        annotation: None,
    }
}

fn sample_annotation(pairs: usize) -> EventAnnotation {
    let metadata: BTreeMap<String, String> = (0..pairs)
        .map(|i| (format!("key-{i:03}"), format!("value-{i}")))
        .collect();
    EventAnnotation {
        kind: "OBSTACLE_DETECTED".to_owned(),
        description: "lidar cluster within stop zone".to_owned(),
        severity: Severity::Warn,
        metadata,
    }
}

#[test]
fn valid_event_passes_validation() {
    assert_eq!(sample_event().validate(), Ok(()));
}

#[test]
fn empty_robot_id_is_rejected() {
    let mut event = sample_event();
    event.robot_id.clear();
    assert_eq!(
        event.validate(),
        Err(ValidationError::EmptyField { field: "robot_id" })
    );
}

#[test]
fn empty_session_id_is_rejected() {
    let mut event = sample_event();
    event.session_id.clear();
    assert_eq!(
        event.validate(),
        Err(ValidationError::EmptyField {
            field: "session_id"
        })
    );
}

#[test]
fn metadata_at_bound_is_accepted() {
    let annotation = sample_annotation(MAX_METADATA_PAIRS);
    assert_eq!(annotation.validate(), Ok(()));
}

#[test]
fn metadata_over_bound_is_rejected() {
    let annotation = sample_annotation(MAX_METADATA_PAIRS + 1);
    assert_eq!(
        annotation.validate(),
        Err(ValidationError::TooManyMetadataPairs {
            count: MAX_METADATA_PAIRS + 1
        })
    );
}

#[test]
fn oversized_metadata_key_is_rejected() {
    let mut annotation = sample_annotation(1);
    let long_key = "k".repeat(MAX_METADATA_KEY_LEN + 1);
    annotation.metadata.insert(long_key.clone(), "v".to_owned());
    assert_eq!(
        annotation.validate(),
        Err(ValidationError::MetadataKeyTooLong {
            key: long_key,
            len: MAX_METADATA_KEY_LEN + 1
        })
    );
}

#[test]
fn annotation_violations_surface_through_event_validate() {
    let mut event = sample_event();
    event.annotation = Some(sample_annotation(MAX_METADATA_PAIRS + 1));
    assert!(matches!(
        event.validate(),
        Err(ValidationError::TooManyMetadataPairs { .. })
    ));
}

#[test]
fn severity_round_trips_through_text() {
    for severity in [
        Severity::Debug,
        Severity::Info,
        Severity::Warn,
        Severity::Error,
        Severity::Fatal,
    ] {
        assert_eq!(Severity::from_str(severity.as_str()), Ok(severity));
    }
}

#[test]
fn unknown_severity_text_is_rejected() {
    assert_eq!(
        Severity::from_str("CRITICAL"),
        Err(ValidationError::InvalidSeverity {
            value: "CRITICAL".to_owned()
        })
    );
}

#[test]
fn error_messages_are_human_readable() {
    let cases: Vec<(ValidationError, &str)> = vec![
        (
            ValidationError::EmptyField { field: "robot_id" },
            "field robot_id must not be empty",
        ),
        (
            ValidationError::TooManyMetadataPairs { count: 33 },
            "annotation metadata has 33 pairs, maximum is 32",
        ),
        (
            ValidationError::InvalidSeverity {
                value: "CRITICAL".to_owned(),
            },
            "invalid severity \"CRITICAL\", expected DEBUG|INFO|WARN|ERROR|FATAL",
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(error.to_string(), expected);
    }
}
