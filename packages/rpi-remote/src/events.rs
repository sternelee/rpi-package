//! Bridges the local mesh to the live agent session.
//!
//! Two directions:
//!
//! **Mesh → session.** An inbound envelope is injected into the session as a
//! user message, so the model sees it on its next turn (the receiving agent's
//! LLM is what "handles" a message, exactly as in the reference design).
//! Injection goes through the host's `SendMessage` runtime action with
//! `triggerTurn: true`, and the body is wrapped in a labelled envelope so the
//! model can tell mesh traffic from the human's typing and can find the `id`
//! and `from` it needs to reply with `re`.
//!
//! **Session → mesh.** Tool activity is broadcast to peers so a watching agent
//! can see what a teammate is doing, and `peer_joined` / `peer_left` notices are
//! surfaced into the session as context-only entries (no extra model turn).
//!
//! The pump is a plain polling thread rather than an event handler because the
//! host dispatches plugin event handlers synchronously and gives them no "wake
//! me when the mesh has input" hook; a short poll interval keeps delivery
//! latency well below a model round trip.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rpi_plugin_sdk::{EventTag, RuntimeActionId, StablePluginEvent, StbString, StbStringRef};
use serde_json::{json, Value};

/// How often the pump drains the mesh.
const PUMP_INTERVAL: Duration = Duration::from_millis(200);
/// Custom message type used for context-only notices.
const NOTICE_TYPE: &str = "rpi-remote";

static PUMP_RUNNING: AtomicBool = AtomicBool::new(false);

/// Starts the inbound pump once per process.
pub fn start_pump() {
    if PUMP_RUNNING.swap(true, Ordering::SeqCst) {
        return;
    }
    std::thread::Builder::new()
        .name("rpi-remote-pump".into())
        .spawn(|| loop {
            std::thread::sleep(PUMP_INTERVAL);
            let Some(session) = crate::state::current_session() else {
                continue;
            };
            for envelope in session.node.drain_messages() {
                deliver(&envelope);
            }
        })
        .ok();
}

/// Injects one inbound envelope into the session.
///
/// Control notices (`peer_joined` / `peer_left`) are added as context-only
/// entries: they are useful for the model's mental model of the mesh but must
/// not each cost a model turn.
fn deliver(envelope: &crate::envelope::Envelope) {
    let kind = envelope
        .body
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("");

    if kind == "peer_joined" || kind == "peer_left" {
        let verb = if kind == "peer_joined" {
            "joined"
        } else {
            "left"
        };
        send_message(
            json!({
                "message": {
                    "customType": NOTICE_TYPE,
                    "content": format!("Mesh peer {}: {}", verb, envelope.from),
                    "display": true,
                },
                "options": { "triggerTurn": false }
            }),
            "context",
        );
        return;
    }

    // A real message: the model must see it, and it may need to reply.
    let body = match &envelope.body {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    };
    let reply_hint = match &envelope.re {
        Some(id) => format!(
            "\n\nThis is a reply to the message with id {id}. If you respond to it, pass that id as `re`."
        ),
        None => format!(
            "\n\nTo reply, call `agent_send` with to=\"{}\" and re=\"{}\".",
            envelope.from, envelope.id
        ),
    };
    let text = format!(
        "[rpi agent mesh message]\nfrom: {}\nid: {}{reply_hint}\n\n{body}",
        envelope.from, envelope.id
    );
    send_message(
        json!({
            "message": { "customType": NOTICE_TYPE, "content": text, "display": true },
            "options": { "triggerTurn": true }
        }),
        "message",
    );
}

/// Invokes the host's `SendMessage` runtime action.
fn send_message(args: Value, what: &str) {
    let Some(host) = crate::host() else {
        return;
    };
    let encoded = args.to_string();
    let mut out = StbString::empty();
    let status = (host.runtime_action)(
        u32::from(RuntimeActionId::SendMessage),
        StbStringRef::from_str(&encoded),
        &mut out,
        host.user_data,
    );
    if status != 0 {
        // A host that predates `SendMessage`, or a session that is shutting
        // down: the message is lost rather than crashing the mesh. Report it on
        // the status line so it is visible instead of silent.
        let error = out.to_string_lossy();
        set_status(&format!("rpi-remote: dropped mesh {what}: {error}"));
    }
    (host.free_string)(out);
}

/// Publishes a short status string to the host's UI.
pub fn set_status(text: &str) {
    let Some(host) = crate::host() else {
        return;
    };
    let args = json!({ "key": "rpi-remote", "value": text }).to_string();
    let mut out = StbString::empty();
    let _ = (host.runtime_action)(
        u32::from(RuntimeActionId::SetStatus),
        StbStringRef::from_str(&args),
        &mut out,
        host.user_data,
    );
    (host.free_string)(out);
}

// ---------------------------------------------------------------------------
// Event handlers
// ---------------------------------------------------------------------------

/// Registers the session-observing handlers.
pub fn register_handlers(api: &rpi_plugin_sdk::PluginApi) -> i32 {
    let Some(register_event) = api.register_event_handler else {
        return 1;
    };
    let Some(register_before_agent) = api.register_before_agent_start else {
        return 1;
    };

    // Only shutdown needs observing: inbound delivery is driven by the pump,
    // and peers are never told about local tool activity (a `tool_activity`
    // broadcast would arrive as ordinary mesh content and cost the peer a model
    // turn for no information the peer can act on).
    let status = register_event(EventTag::SessionShutdown, on_event, std::ptr::null_mut());
    if status != 0 {
        return status;
    }

    // Teach the model how to use the mesh on every run, without persisting the
    // instruction into the session.
    let status = register_before_agent(
        on_before_agent_start,
        rpi_plugin_sdk::callbacks::free_string,
        std::ptr::null_mut(),
    );
    if status != 0 {
        return status;
    }

    // Publish the bundled skill so the host discovers it as a normal skill
    // (visible in `<available_skills>`) rather than only as prompt text.
    if let Some(register_resources) = api.register_resources_discover {
        let _ = register_resources(
            on_resources_discover,
            rpi_plugin_sdk::callbacks::free_string,
            std::ptr::null_mut(),
        );
    }
    0
}

/// Reports the directories this extension contributes to skill discovery.
extern "C" fn on_resources_discover(
    _cwd: StbStringRef,
    _reason: StbStringRef,
    out: *mut StbString,
    _user_data: *mut std::ffi::c_void,
) -> i32 {
    rpi_plugin_sdk::guard_or(1, || {
        let mut output = unsafe { rpi_plugin_sdk::callbacks::Output::from_raw(out) };
        let Some(dir) = crate::state::bundled_skills_dir() else {
            output.write("{}".to_owned());
            return 0;
        };
        output.write(json!({ "skillPaths": [dir] }).to_string());
        0
    })
}

/// Keeps the mesh guidance in the system prompt while a session is live.
///
/// The prompt is a **replacement**, not an append, so the current value is read
/// back and re-emitted with the guidance added. A session that is not on the
/// mesh is left untouched.
extern "C" fn on_before_agent_start(
    event_json: StbStringRef,
    out: *mut StbString,
    _user_data: *mut std::ffi::c_void,
) -> i32 {
    rpi_plugin_sdk::guard_or(1, || {
        let mut output = unsafe { rpi_plugin_sdk::callbacks::Output::from_raw(out) };
        if crate::state::current_session().is_none() {
            // Not on the mesh: return an empty object, meaning "unchanged".
            output.write("{}".to_owned());
            return 0;
        }
        let raw = unsafe { event_json.as_str() };
        let event: Value = serde_json::from_str(raw).unwrap_or(Value::Null);
        let current = event
            .get("systemPrompt")
            .and_then(Value::as_str)
            .unwrap_or("");
        let next = format!("{current}{}", MESH_PROMPT);
        output.write(json!({ "systemPrompt": next }).to_string());
        0
    })
}

/// Mesh usage guidance appended to the system prompt while a session is live.
const MESH_PROMPT: &str = r#"

## rpi agent mesh

This process participates in the local agent mesh. Other rpi agents on this
machine can message you, and you can message them:

- `list_peers()` lists the routing addresses currently reachable. Call it
  before sending when you are unsure who is available.
- `agent_send({to, body, re?})` sends a message. A unicast `to` waits for the
  broker's delivery ACK, so a returned `received` means the message is safely
  queued for the peer; `timeout` means the peer is offline. Broadcast and
  multicast are fire-and-forget.
- An incoming mesh message arrives as a user message labelled
  `[rpi agent mesh message]` carrying the sender's address (`from`) and the
  message `id`. To reply, call `agent_send` with `re` set to that `id`.
- Do not invent addresses. Addresses are opaque; echo them verbatim.
"#;

/// Tears the mesh down when the session shuts down.
extern "C" fn on_event(event: StablePluginEvent, _user_data: *mut std::ffi::c_void) -> i32 {
    rpi_plugin_sdk::guard_or(1, || {
        let view = unsafe { rpi_plugin_sdk::events::EventView::from_raw(&event) };
        if view.tag() == EventTag::SessionShutdown {
            if let Some(session) = crate::state::take_session() {
                session.node.peer().leave();
            }
            set_status("");
        }
        0
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mesh_prompt_teaches_the_reply_contract() {
        // The guidance is the model's only orientation, so the load-bearing
        // facts (the tools, the `re` reply contract, and the no-invented-address
        // rule) must be present.
        assert!(MESH_PROMPT.contains("list_peers"));
        assert!(MESH_PROMPT.contains("agent_send"));
        assert!(MESH_PROMPT.contains("`re`"));
        assert!(MESH_PROMPT.contains("[rpi agent mesh message]"));
    }
}
