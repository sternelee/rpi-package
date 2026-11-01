//! The 5-field agent-mesh envelope and its JSONL codec.
//!
//! ```text
//! { "from": "<address>", "to": "<address>" | ["<a>","<b>"] | "broadcast",
//!   "id": "<uuid>", "re": "<uuid>|null", "body": <any JSON> }
//! ```
//!
//! One envelope per line over the local Unix-domain socket. Validation is
//! strict on parse — a malformed line is dropped rather than routed, so a
//! confused peer cannot poison the mesh.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Reserved local address of the session broker itself.
pub const BROKER_NAME: &str = "broker";
/// Broadcast address: deliver to every peer except the sender.
pub const BROADCAST: &str = "broadcast";

/// Closed set of transport-error reasons a trusted relay may report.
pub const TRANSPORT_ERROR_REASONS: [&str; 3] = ["offline", "not_authorized", "bad_envelope"];

/// Routing address of a peer: `<cwd>@<name>`, or the bare name when a legacy
/// peer registered without a cwd.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    pub from: String,
    /// Single address, an array of addresses, or the literal `"broadcast"`.
    pub to: Value,
    pub id: String,
    pub re: Option<String>,
    pub body: Value,
}

#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    #[error("not JSON: {0}")]
    NotJson(String),
    #[error("{0}")]
    Invalid(String),
}

impl Envelope {
    /// Generates a fresh UUID v7 (time-ordered) for a new outbound envelope.
    pub fn new_id() -> String {
        uuid::Uuid::now_v7().to_string()
    }

    /// Builds an envelope with an auto-generated id.
    pub fn new(
        from: impl Into<String>,
        to: impl Into<Value>,
        body: Value,
        re: Option<String>,
    ) -> Self {
        Self {
            from: from.into(),
            to: to.into(),
            id: Self::new_id(),
            re,
            body,
        }
    }

    /// Targets this envelope addresses, normalized to a list.
    ///
    /// `None` means the envelope is a broadcast.
    pub fn targets(&self) -> Option<Vec<String>> {
        match &self.to {
            Value::String(single) if single == BROADCAST => None,
            Value::String(single) => Some(vec![single.clone()]),
            Value::Array(items) => Some(
                items
                    .iter()
                    .filter_map(|item| item.as_str().map(str::to_owned))
                    .collect(),
            ),
            _ => Some(Vec::new()),
        }
    }

    /// Whether the envelope is a broadcast (no single authoritative recipient,
    /// therefore never ACKed).
    pub fn is_broadcast(&self) -> bool {
        matches!(&self.to, Value::String(s) if s == BROADCAST)
    }
}

/// Parses one JSONL line into a validated [`Envelope`].
pub fn parse(line: &str) -> Result<Envelope, EnvelopeError> {
    let raw: Value =
        serde_json::from_str(line).map_err(|error| EnvelopeError::NotJson(error.to_string()))?;
    let Some(object) = raw.as_object() else {
        return Err(EnvelopeError::Invalid("not an object".into()));
    };
    let from = object
        .get("from")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| EnvelopeError::Invalid("from must be a non-empty string".into()))?;
    let to = object
        .get("to")
        .cloned()
        .ok_or_else(|| EnvelopeError::Invalid("to is required".into()))?;
    match &to {
        Value::String(single) if single.is_empty() => {
            return Err(EnvelopeError::Invalid("to must be non-empty".into()));
        }
        Value::String(_) => {}
        Value::Array(items) => {
            if items.is_empty() {
                return Err(EnvelopeError::Invalid("to[] must not be empty".into()));
            }
            if items
                .iter()
                .any(|item| item.as_str().map(|s| s.is_empty()).unwrap_or(true))
            {
                return Err(EnvelopeError::Invalid(
                    "to[] entries must be non-empty strings".into(),
                ));
            }
        }
        _ => {
            return Err(EnvelopeError::Invalid(
                "to must be a string or array".into(),
            ))
        }
    }
    let id = object
        .get("id")
        .and_then(Value::as_str)
        .filter(|value| is_uuid(value))
        .ok_or_else(|| EnvelopeError::Invalid("id must be a UUID".into()))?;
    let re = match object.get("re") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) if is_uuid(value) => Some(value.clone()),
        Some(_) => return Err(EnvelopeError::Invalid("re must be null or a UUID".into())),
    };
    let body = object
        .get("body")
        .cloned()
        .ok_or_else(|| EnvelopeError::Invalid("body is required".into()))?;
    Ok(Envelope {
        from: from.to_owned(),
        to,
        id: id.to_owned(),
        re,
        body,
    })
}

/// Serializes an envelope as one JSONL record (trailing newline included).
pub fn serialize(envelope: &Envelope) -> String {
    let mut line = serde_json::to_string(envelope).unwrap_or_else(|_| "{}".into());
    line.push('\n');
    line
}

/// Strict UUID check accepting the canonical 8-4-4-4-12 hex form.
pub fn is_uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok()
}

/// The `{type:"transport_error",reason}` body a trusted relay may send.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportError {
    pub reason: String,
}

/// Recognizes a transport-error body, accepting only the closed reason set.
pub fn as_transport_error(body: &Value) -> Option<TransportError> {
    let object = body.as_object()?;
    if object.get("type").and_then(Value::as_str) != Some("transport_error") {
        return None;
    }
    let reason = object.get("reason")?.as_str()?;
    if !TRANSPORT_ERROR_REASONS.contains(&reason) {
        return None;
    }
    Some(TransportError {
        reason: reason.to_owned(),
    })
}

/// An ACK body: `{type:"ack",status,target}`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ack {
    pub status: AckStatus,
    pub target: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckStatus {
    Received,
    /// Compatibility with an out-of-date broker leader that dropped the
    /// message. The current broker never emits this.
    Busy,
    Denied,
}

impl AckStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            AckStatus::Received => "received",
            AckStatus::Busy => "busy",
            AckStatus::Denied => "denied",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "received" => Some(AckStatus::Received),
            "busy" => Some(AckStatus::Busy),
            "denied" => Some(AckStatus::Denied),
            _ => None,
        }
    }
}

/// Recognizes an exact ACK body. Forged or partial shapes are rejected so they
/// can never settle a pending send.
pub fn as_ack(body: &Value) -> Option<Ack> {
    let object = body.as_object()?;
    if object.get("type").and_then(Value::as_str) != Some("ack") {
        return None;
    }
    let status = AckStatus::parse(object.get("status")?.as_str()?)?;
    let target = object.get("target")?.as_str()?;
    Some(Ack {
        status,
        target: target.to_owned(),
    })
}

/// Builds the ACK body a broker sends back for a delivered unicast envelope.
pub fn ack_body(status: AckStatus, target: &str) -> Value {
    serde_json::json!({ "type": "ack", "status": status.as_str(), "target": target })
}

/// Composes a peer address: `<cwd>@<name>`, or the bare name when `cwd` is
/// empty (a legacy peer that registered without a cwd).
pub fn compose_address(cwd: &str, name: &str) -> String {
    if cwd.is_empty() {
        name.to_owned()
    } else {
        format!("{cwd}@{name}")
    }
}

/// Sanitizes one mesh name segment, preserving a trailing `#N` collision suffix.
pub fn sanitize_mesh_name(raw: &str) -> String {
    let (base, suffix) = match raw.rfind('#') {
        Some(index)
            if raw[index + 1..].chars().all(|c| c.is_ascii_digit()) && index + 1 < raw.len() =>
        {
            (&raw[..index], &raw[index..])
        }
        _ => (raw, ""),
    };
    let cleaned = sanitize_segment(base).unwrap_or_else(|| "agent".into());
    format!("{cleaned}{suffix}")
}

/// Trims and normalizes one name segment. Returns `None` when nothing usable
/// remains or the token collides with a reserved address.
pub fn sanitize_segment(value: &str) -> Option<String> {
    let mut token = String::new();
    let mut last_dash = false;
    for ch in value.trim().chars() {
        if ch.is_whitespace() || matches!(ch, '/' | ':' | '@' | '#') {
            if !last_dash && !token.is_empty() {
                token.push('-');
                last_dash = true;
            }
            continue;
        }
        token.push(ch);
        last_dash = false;
    }
    while token.ends_with('-') {
        token.pop();
    }
    if token.is_empty() {
        return None;
    }
    let lowered = token.to_ascii_lowercase();
    if lowered == BROADCAST || lowered == BROKER_NAME {
        return None;
    }
    Some(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_an_envelope() {
        let envelope = Envelope::new(
            "/tmp/project@agent-a",
            Value::String("/tmp/project@agent-b".into()),
            serde_json::json!({ "type": "ping" }),
            None,
        );
        let line = serialize(&envelope);
        assert!(line.ends_with('\n'));
        assert_eq!(parse(line.trim_end()).unwrap(), envelope);
    }

    #[test]
    fn rejects_malformed_envelopes() {
        assert!(parse("not json").is_err());
        assert!(parse("[]").is_err());
        assert!(parse(r#"{"from":"a","to":"b","body":1}"#).is_err());
        assert!(parse(
            r#"{"from":"","to":"b","id":"00000000-0000-0000-0000-000000000000","re":null,"body":1}"#
        )
        .is_err());
        assert!(parse(
            r#"{"from":"a","to":"","id":"00000000-0000-0000-0000-000000000000","re":null,"body":1}"#
        )
        .is_err());
        assert!(parse(
            r#"{"from":"a","to":[],"id":"00000000-0000-0000-0000-000000000000","re":null,"body":1}"#
        )
        .is_err());
        assert!(parse(r#"{"from":"a","to":"b","id":"nope","re":null,"body":1}"#).is_err());
        assert!(parse(r#"{"from":"a","to":"b","id":"00000000-0000-0000-0000-000000000000","re":"nope","body":1}"#).is_err());
        assert!(parse(
            r#"{"from":"a","to":"b","id":"00000000-0000-0000-0000-000000000000","re":null}"#
        )
        .is_err());
    }

    #[test]
    fn classifies_targets_and_broadcast() {
        let unicast = Envelope::new("a", Value::String("b".into()), Value::Null, None);
        assert_eq!(unicast.targets(), Some(vec!["b".to_owned()]));
        assert!(!unicast.is_broadcast());

        let multi = Envelope::new("a", serde_json::json!(["b", "c"]), Value::Null, None);
        assert_eq!(multi.targets(), Some(vec!["b".to_owned(), "c".to_owned()]));

        let broadcast = Envelope::new("a", Value::String(BROADCAST.into()), Value::Null, None);
        assert_eq!(broadcast.targets(), None);
        assert!(broadcast.is_broadcast());
    }

    #[test]
    fn accepts_only_exact_ack_and_transport_error_shapes() {
        assert_eq!(
            as_ack(&serde_json::json!({"type":"ack","status":"received","target":"t"})),
            Some(Ack {
                status: AckStatus::Received,
                target: "t".into()
            })
        );
        assert!(as_ack(&serde_json::json!({"type":"ack","status":"weird","target":"t"})).is_none());
        assert!(as_ack(&serde_json::json!({"type":"ack","status":"received"})).is_none());
        assert!(as_ack(&serde_json::json!({"type":"ack", "target":"t"})).is_none());

        assert_eq!(
            as_transport_error(&serde_json::json!({"type":"transport_error","reason":"offline"})),
            Some(TransportError {
                reason: "offline".into()
            })
        );
        assert!(
            as_transport_error(&serde_json::json!({"type":"transport_error","reason":"nope"}))
                .is_none()
        );
        assert!(as_transport_error(&serde_json::json!({"reason":"offline"})).is_none());
    }

    #[test]
    fn sanitizes_names_and_preserves_collision_suffix() {
        assert_eq!(sanitize_segment("  my agent  "), Some("my-agent".into()));
        assert_eq!(sanitize_segment("a/b:c@d#e"), Some("a-b-c-d-e".into()));
        assert_eq!(sanitize_segment("broadcast"), None);
        assert_eq!(sanitize_segment("Broker"), None);
        assert_eq!(sanitize_segment("///"), None);
        assert_eq!(sanitize_mesh_name("api#3"), "api#3");
        assert_eq!(sanitize_mesh_name("my api#12"), "my-api#12");
        assert_eq!(sanitize_mesh_name("broker"), "agent");
    }

    #[test]
    fn composes_addresses() {
        assert_eq!(compose_address("/tmp/p", "a"), "/tmp/p@a");
        assert_eq!(compose_address("", "a"), "a");
    }

    #[test]
    fn ids_are_time_ordered_uuids() {
        let first = Envelope::new_id();
        let second = Envelope::new_id();
        assert!(is_uuid(&first));
        assert!(is_uuid(&second));
        assert!(first < second, "{first} should sort before {second}");
    }
}
