//! The three mesh tools plus the `/remote-pi` command implementation.
//!
//! Each tool has its **own** `execute` callback. The host passes a plugin tool
//! no tool name, so dispatching on argument shape would be guesswork — one
//! entrypoint per tool is the only unambiguous mapping.
//!
//! All three follow the SDK's four-function lifecycle (`execute` → `poll` →
//! `cancel` → `destroy`), with per-call state behind an opaque handle from
//! [`rpi_plugin_sdk::handles::ToolHandles`]. `poll` is non-blocking: the work
//! runs on a worker thread and `poll` only checks the result slot, so a long
//! `agent_request` never occupies the host's driver thread.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::Arc;
use std::time::Duration;

use rpi_plugin_sdk::handles::ToolHandles;
use rpi_plugin_sdk::{
    FreeStringFn, PluginApi, StableToolSchema, StbString, StbStringRef, StepHandle, StepResult,
    ToolPartialCb,
};
use serde_json::{json, Value};

use crate::envelope::BROADCAST;
use crate::mesh_node::describe_ack;
use crate::peer::{SendOutcome, ACK_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT_MS};

/// How long a `list_peers` query may take. It is a broker metadata round trip,
/// so a few milliseconds is normal and two seconds is already generous.
const PEER_QUERY_TIMEOUT: Duration = Duration::from_millis(2_000);

/// Outcome a worker sends back to `poll`.
type WorkerResult = Result<(String, Value), String>;

/// Per-call drive state: the channel the worker reports its result on.
///
/// Cancellation is not tracked here: `ToolHandles` owns the flag per handle and
/// exposes it as `ToolState::cancelled`, which is the same `Arc` the worker
/// holds, so one flag serves `poll`, `cancel` and the worker.
struct Drive {
    result: Receiver<WorkerResult>,
}

static HANDLES: ToolHandles<Drive> = ToolHandles::new();

/// Registers the mesh tools, returning the SDK status code.
pub fn register_tools(api: &PluginApi) -> i32 {
    let Some(register) = api.register_tool else {
        return 1;
    };
    let definitions: [(&str, &str, &str, rpi_plugin_sdk::ToolExecuteFn); 3] = [
        (
            "list_peers",
            "Returns the peer inventory of the local agent session as routing addresses of the \
             form `<cwd>@<name>`. An address is an opaque routing key: pass it to `agent_send` \
             VERBATIM and never compose one by hand. Cheap — a broker metadata query, not a turn \
             of another agent — so call it whenever you are unsure who is available.",
            r#"{"type":"object","properties":{},"additionalProperties":false}"#,
            list_peers_execute,
        ),
        (
            "agent_send",
            "Send a message to another rpi agent in this machine's local session. Unicast waits \
             for the broker's delivery ACK and returns one of `received` (delivered; the peer \
             processes it in an upcoming turn), `denied` (refused), or `timeout` (no ACK within \
             5s — the peer is offline or the transport failed). Broadcast and multicast are \
             fire-and-forget. Set `re` to the `id` of an incoming message when you are REPLYING \
             to it; the peer correlates your answer through that field.",
            r#"{"type":"object","properties":{
                 "to":{"type":"string","description":"Recipient address from list_peers, the literal `broadcast`, or a JSON array of addresses. Unicast returns an ACK status; broadcast/multicast is fire-and-forget."},
                 "body":{"description":"Free-form JSON payload: a string or an object."},
                 "re":{"type":"string","description":"Set to the `id` of an incoming message when replying to it."}
               },"required":["to","body"],"additionalProperties":false}"#,
            agent_send_execute,
        ),
        (
            "agent_request",
            "DEPRECATED — prefer `agent_send` and observing your inbox for a reply correlated by \
             `re`. Still works: sends a message and blocks until the peer replies or the timeout \
             fires (default 30s).",
            r#"{"type":"object","properties":{
                 "to":{"type":"string","description":"Recipient address. Must be a single peer, not broadcast."},
                 "body":{"description":"Free-form JSON payload to send."},
                 "timeout_ms":{"type":"integer","minimum":1,"description":"Optional override of the default 30s reply timeout."}
               },"required":["to","body"],"additionalProperties":false}"#,
            agent_request_execute,
        ),
    ];

    for (name, description, parameters, execute_fn) in definitions {
        let schema = Box::new(StableToolSchema {
            name: StbString::from_string(name.to_owned()),
            description: StbString::from_string(description.to_owned()),
            parameters: StbString::from_string(parameters.to_owned()),
        });
        let status = register(
            &*schema,
            execute_fn,
            poll,
            cancel,
            destroy,
            rpi_plugin_sdk::callbacks::free_string,
        );
        drop(schema);
        if status != 0 {
            return status;
        }
    }
    0
}

// ---------------------------------------------------------------------------
// Tool entrypoints
// ---------------------------------------------------------------------------

extern "C" fn list_peers_execute(
    _tool_call_id: StbStringRef,
    params: StbString,
    free: Option<FreeStringFn>,
) -> StepHandle {
    spawn_drive(params, free, list_peers)
}

extern "C" fn agent_send_execute(
    _tool_call_id: StbStringRef,
    params: StbString,
    free: Option<FreeStringFn>,
) -> StepHandle {
    spawn_drive(params, free, agent_send)
}

extern "C" fn agent_request_execute(
    _tool_call_id: StbStringRef,
    params: StbString,
    free: Option<FreeStringFn>,
) -> StepHandle {
    spawn_drive(params, free, agent_request)
}

/// Parses the call, runs `work` on a worker thread, and returns its handle.
fn spawn_drive(
    params: StbString,
    free: Option<FreeStringFn>,
    work: fn(&Value, &AtomicBool) -> WorkerResult,
) -> StepHandle {
    rpi_plugin_sdk::guard_or(std::ptr::null_mut(), || {
        // `params` is host-owned: read it, then free it through the host's
        // allocator exactly once.
        let text = params.to_string_lossy();
        params.free_with(free);
        let parsed: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        let (sender, receiver) = mpsc::channel();
        // The flag is created here so it can be shared with the worker; the
        // registry stores the same `Arc` for `poll`/`cancel` to observe.
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker_cancel = Arc::clone(&cancelled);
        std::thread::Builder::new()
            .name("rpi-remote-tool".into())
            .spawn(move || {
                let outcome = work(&parsed, &worker_cancel);
                let _ = sender.send(outcome);
            })
            .ok();
        HANDLES.insert_with_cancel(Drive { result: receiver }, cancelled)
    })
}

/// `poll` never blocks: `Pending` until the worker has finished.
extern "C" fn poll(
    handle: StepHandle,
    _partial: Option<ToolPartialCb>,
    _user_data: *mut std::ffi::c_void,
) -> StepResult {
    rpi_plugin_sdk::guard_or(
        StepResult::err(StbString::from_string("rpi-remote poll panicked".into())),
        || {
            if handle.is_null() {
                return StepResult::err(StbString::from_string("null tool handle".into()));
            }
            let Some(entry) = HANDLES.get(handle) else {
                return StepResult::err(StbString::from_string(
                    "invalid or destroyed tool handle".into(),
                ));
            };
            if entry.cancelled.load(Ordering::SeqCst) {
                return StepResult::err(StbString::from_string("cancelled".into()));
            }
            let drive = entry.lock();
            match drive.result.try_recv() {
                Ok(Ok((text, details))) => StepResult::done(StbString::from_string(
                    json!({
                        "content": [{"type": "text", "text": text}],
                        "details": details,
                    })
                    .to_string(),
                )),
                Ok(Err(message)) => StepResult::err(StbString::from_string(message)),
                Err(TryRecvError::Empty) => StepResult::pending(StbString::from_string(
                    r#"{"progress":"working"}"#.to_owned(),
                )),
                Err(TryRecvError::Disconnected) => {
                    StepResult::err(StbString::from_string("worker thread disappeared".into()))
                }
            }
        },
    )
}

/// `cancel` only sets the flag; it is idempotent and never frees.
extern "C" fn cancel(handle: StepHandle) {
    HANDLES.cancel(handle);
}

/// `destroy` releases the drive exactly once.
extern "C" fn destroy(handle: StepHandle) {
    drop(HANDLES.remove(handle));
}

// ---------------------------------------------------------------------------
// Tool work
// ---------------------------------------------------------------------------

/// `list_peers`: queries the broker and hides this agent's own address.
fn list_peers(_params: &Value, _cancelled: &AtomicBool) -> WorkerResult {
    let Some(session) = crate::state::current_session() else {
        return Err(not_in_session());
    };
    let peers = session
        .node
        .list_peers(PEER_QUERY_TIMEOUT)
        .map_err(|error| format!("list_peers failed: {error}"))?;
    let self_address = session.node.address();
    let peers: Vec<String> = peers
        .into_iter()
        .filter(|address| address != &self_address)
        .collect();
    let text = if peers.is_empty() {
        "(no peers)".to_owned()
    } else {
        peers.join("\n")
    };
    Ok((text, json!({ "peers": peers })))
}

/// `agent_send`: unicast awaits the delivery ACK; broadcast is fire-and-forget.
fn agent_send(params: &Value, cancelled: &AtomicBool) -> WorkerResult {
    let session = require_session()?;
    let to = params.get("to").cloned().ok_or("missing `to`")?;
    let body = params.get("body").cloned().ok_or("missing `body`")?;
    let re = params.get("re").and_then(Value::as_str).map(str::to_owned);

    if let Some(target) = to.as_str() {
        refuse_self(&session.node, target)?;
    }

    // Broadcast / multicast has no single authoritative recipient to ACK.
    let is_unicast = matches!(&to, Value::String(single) if single != BROADCAST);
    if !is_unicast {
        session
            .node
            .send(to, body, re)
            .map_err(|error| format!("broadcast failed: {error}"))?;
        return Ok((
            "Broadcast sent.".to_owned(),
            json!({ "status": "sent", "ok": true }),
        ));
    }

    let target = to.as_str().unwrap_or_default().to_owned();
    // One send, one envelope id: the wait for the ACK must not re-send, or the
    // peer would receive duplicate copies under fresh ids and any reply's `re`
    // would point at a copy the sender never tracked.
    let outcome = session.node.send_with_ack(
        &target,
        body,
        re.clone(),
        Duration::from_millis(ACK_TIMEOUT_MS),
        Some(cancelled),
    );
    // A cancelled call is reported honestly rather than as a delivery failure.
    if cancelled.load(Ordering::SeqCst) {
        return Err("cancelled".into());
    }
    let (ok, text) = describe_ack(&outcome, &target, re.as_deref());
    let status = match &outcome {
        SendOutcome::Received { .. } => "received",
        SendOutcome::Sent => "sent",
        SendOutcome::Denied { .. } => "denied",
        SendOutcome::Timeout => "timeout",
    };
    Ok((
        text,
        json!({ "status": status, "ok": ok, "target": target }),
    ))
}

/// `agent_request`: the deprecated synchronous request/reply.
fn agent_request(params: &Value, _cancelled: &AtomicBool) -> WorkerResult {
    let session = require_session()?;
    let target = params
        .get("to")
        .and_then(Value::as_str)
        .ok_or("`agent_request` requires a single `to` address")?
        .to_owned();
    refuse_self(&session.node, &target)?;
    if target == BROADCAST {
        return Err("`agent_request` cannot target `broadcast`".into());
    }
    let body = params.get("body").cloned().ok_or("missing `body`")?;
    let timeout = params
        .get("timeout_ms")
        .and_then(Value::as_u64)
        .map(|ms| Duration::from_millis(ms.max(1)))
        .unwrap_or(Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS));

    let reply = session
        .node
        .request(&target, body, timeout)
        .map_err(|error| format!("request failed: {error}"))?;
    let text = match &reply.body {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    };
    Ok((text, reply.body))
}

fn require_session() -> Result<Arc<crate::state::Session>, String> {
    crate::state::current_session().ok_or_else(not_in_session)
}

fn refuse_self(node: &crate::mesh_node::MeshNode, target: &str) -> Result<(), String> {
    if target == node.address() || target == node.peer().name() {
        return Err(format!(
            "refused: cannot send to yourself (\"{target}\"); do the work directly"
        ));
    }
    Ok(())
}

fn not_in_session() -> String {
    "Not in a session. Run /remote-pi start first.".to_owned()
}

// ---------------------------------------------------------------------------
// /remote-pi command
// ---------------------------------------------------------------------------

/// Runs one `/remote-pi` subcommand, returning the TUI command output JSON.
pub fn run_command(args: &str) -> String {
    let mut parts = args.split_whitespace();
    let subcommand = parts.next().unwrap_or("status");
    let rest: Vec<&str> = parts.collect();
    match subcommand {
        "start" | "join" => command_start(),
        "stop" | "leave" => command_stop(),
        "status" => command_status(),
        "peers" => command_peers(),
        "rename" => command_rename(&rest),
        "help" => message("Usage: /remote-pi <start|stop|status|peers|rename <name>>"),
        other => message(&format!(
            "Unknown subcommand \"{other}\". Try /remote-pi help."
        )),
    }
}

fn command_start() -> String {
    if crate::state::current_session().is_some() {
        return message("Already on the local mesh.");
    }
    let cwd = crate::state::canonical_cwd(&crate::state::current_cwd());
    let name = crate::state::display_name(&cwd);
    match crate::mesh_node::MeshNode::join(&cwd, &name) {
        Ok(node) => {
            let address = node.address();
            let session = node.session_name().to_owned();
            crate::state::set_session(Arc::new(crate::state::Session { node }));
            message(&format!(
                "Joined the local mesh as {address}.\nSession: {session}"
            ))
        }
        Err(error) => message(&format!("Could not join the local mesh: {error}")),
    }
}

fn command_stop() -> String {
    match crate::state::take_session() {
        Some(session) => {
            let address = session.node.address();
            session.node.peer().leave();
            message(&format!("Left the local mesh ({address})."))
        }
        None => message("Not on the local mesh."),
    }
}

fn command_status() -> String {
    match crate::state::current_session() {
        Some(session) => {
            let count = session
                .node
                .list_peers(PEER_QUERY_TIMEOUT)
                .map(|peers| peers.len())
                .unwrap_or(0);
            message(&format!(
                "Session: {}\nAddress: {}\nPeers (including you): {}\nLock: {}",
                session.node.session_name(),
                session.node.address(),
                count,
                session.node.lock_endpoint().display()
            ))
        }
        None => message("Not on the local mesh. Run /remote-pi start."),
    }
}

fn command_peers() -> String {
    let Some(session) = crate::state::current_session() else {
        return message("Not on the local mesh. Run /remote-pi start.");
    };
    match session.node.list_peers(PEER_QUERY_TIMEOUT) {
        Ok(peers) => {
            let rendered: Vec<String> = peers
                .iter()
                .map(|address| {
                    if address == &session.node.address() {
                        format!("{address} (you)")
                    } else {
                        address.clone()
                    }
                })
                .collect();
            if rendered.is_empty() {
                message("No peers in this session.")
            } else {
                message(&rendered.join("\n"))
            }
        }
        Err(error) => message(&format!("Could not query peers: {error}")),
    }
}

fn command_rename(rest: &[&str]) -> String {
    let Some(requested) = rest.first() else {
        return message("Usage: /remote-pi rename <name>");
    };
    let Some(clean) = crate::envelope::sanitize_segment(requested) else {
        return message("That name is not usable (empty, or a reserved address).");
    };
    let Some(session) = crate::state::current_session() else {
        return message("Not on the local mesh. Run /remote-pi start.");
    };
    // Renaming is a re-join under a new identity: release the old lock before
    // claiming the new name, or two live locks would be held by one process.
    let cwd = session.node.locked_cwd();
    let old_address = session.node.address();
    crate::state::take_session();
    session.node.peer().leave();
    drop(session);
    match crate::mesh_node::MeshNode::join(&cwd, &clean) {
        Ok(node) => {
            let address = node.address();
            crate::state::set_session(Arc::new(crate::state::Session { node }));
            message(&format!("Renamed {old_address} to {address}."))
        }
        Err(error) => message(&format!(
            "Rename failed after releasing the old name: {error}. Run /remote-pi start to rejoin."
        )),
    }
}

fn message(text: &str) -> String {
    json!({ "kind": "message", "text": text }).to_string()
}
