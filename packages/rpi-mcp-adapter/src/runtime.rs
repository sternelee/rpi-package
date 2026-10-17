//! Persistent connections scoped to the host session and configuration identity.
use crate::connection::{
    initialize_request, initialized_notification, parse_initialize, response_result,
    tools_call_request,
};
use crate::control::{Control, Deadline};
use crate::http::HttpTransport;
use crate::{McpConfig, McpServerConfig, Request, Response, ServerInfo, StdioTransport, Tool};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, TryLockError};
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveredServer {
    pub name: String,
    pub info: ServerInfo,
    pub tools: Vec<Tool>,
}

#[derive(Debug)]
enum Transport {
    Stdio(StdioTransport),
    Http(HttpTransport),
}
impl Transport {
    fn request(&mut self, request: &Request, control: &Control) -> Result<Response, String> {
        match self {
            Self::Stdio(t) => t.request_control(request, control),
            Self::Http(t) => t.request(request, control),
        }
    }
    fn notify(&mut self, control: &Control) -> Result<(), String> {
        match self {
            Self::Stdio(t) => t.notify_control(&initialized_notification(), control),
            Self::Http(t) => t.notify(&initialized_notification(), control),
        }
    }
    fn timeout(&mut self, timeout: u64) {
        match self {
            Self::Stdio(t) => t.set_timeout(timeout),
            Self::Http(t) => t.set_timeout(timeout),
        }
    }
}
#[derive(Debug)]
struct Session {
    transport: Transport,
    info: ServerInfo,
    next_id: u64,
    failed: bool,
}
impl Session {
    fn start(
        config: &McpServerConfig,
        account: &str,
        timeout: u64,
        control: &Control,
        notifications: &SyncSender<Value>,
    ) -> Result<Self, String> {
        control.check()?;
        let mut transport = match config {
            McpServerConfig::Stdio(s) => Transport::Stdio(StdioTransport::start(
                &s.command,
                &s.args,
                &s.env,
                s.cwd.as_deref(),
                Some(notifications.clone()),
            )?),
            McpServerConfig::Http(s) => {
                Transport::Http(HttpTransport::with_account(s, Some(account))?)
            }
        };
        transport.timeout(timeout);
        let info = parse_initialize(transport.request(
            &initialize_request(1, "rpi-mcp-adapter", env!("CARGO_PKG_VERSION")),
            control,
        )?)?;
        if let Transport::Http(http) = &mut transport {
            http.version = Some(info.protocol_version.clone());
        }
        transport.notify(control)?;
        Ok(Self {
            transport,
            info,
            next_id: 2,
            failed: false,
        })
    }
    fn request(
        &mut self,
        method: &str,
        params: Value,
        control: &Control,
    ) -> Result<Response, String> {
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or("MCP request ID exhausted")?;
        let response = self
            .transport
            .request(&Request::new(id, method, Some(params)), control);
        if response.is_err() {
            self.failed = true;
        }
        response
    }
    fn tools(&mut self, control: &Control) -> Result<Vec<Tool>, String> {
        let mut tools = Vec::new();
        let mut cursor = None;
        let mut seen = HashSet::new();
        for _ in 0..100 {
            let params = cursor
                .as_ref()
                .map(|v| json!({"cursor":v}))
                .unwrap_or_else(|| json!({}));
            let result = response_result(self.request("tools/list", params, control)?)?;
            tools.extend(crate::tool_list(&result)?);
            if tools.len() > 16_384 {
                return Err("MCP tool list exceeded 16384 tools".into());
            }
            match result.get("nextCursor") {
                None => return Ok(tools),
                Some(Value::String(next)) => {
                    if !seen.insert(next.clone()) {
                        return Err("MCP tools/list returned a repeated pagination cursor".into());
                    }
                    cursor = Some(next.clone());
                }
                _ => return Err("MCP tools/list nextCursor must be a string".into()),
            }
        }
        Err("MCP tools/list exceeded 100 pages".into())
    }
}
// Keep the last configuration even after a failed transport is dropped, so
// a config edit cannot replay the old server's subscriptions onto a new server.
#[derive(Default)]
struct ConnectionState {
    config: Option<McpServerConfig>,
    session: Option<Session>,
}

struct Entry {
    session: Mutex<ConnectionState>,
    stop: Control,
    /// Notifications from this connection's reader, consumed by its dispatcher.
    /// Owned by the entry so a re-opened session reuses the same dispatcher.
    notifications: SyncSender<Value>,
    /// The dispatcher thread, joined on close so a released connection leaves no
    /// thread behind. `stop` is cancelled first, which unblocks its wait.
    dispatcher: Mutex<Option<JoinHandle<()>>>,
}
type Sessions = BTreeMap<String, Arc<Entry>>;
static SESSIONS: OnceLock<Mutex<Sessions>> = OnceLock::new();
fn sessions() -> &'static Mutex<Sessions> {
    SESSIONS.get_or_init(Default::default)
}

// ---------------------------------------------------------------------------
// Resource subscriptions and pushed updates
// ---------------------------------------------------------------------------

// Each subscription lifetime gets its own identity, so a read begun before
// unsubscribe/close can never be accepted by a later subscription to the same URI.
// Pointer identity distinguishes subscription lifetimes; the counter identifies
// the newest read, including one delivered by an overlapping flush.
type Subscription = Arc<AtomicU64>;
type Subscriptions = HashMap<String, HashMap<String, Subscription>>;
static SUBSCRIPTIONS: OnceLock<Mutex<Subscriptions>> = OnceLock::new();
fn subscriptions() -> &'static Mutex<Subscriptions> {
    SUBSCRIPTIONS.get_or_init(Default::default)
}

/// A resource read in response to `notifications/resources/updated`, waiting to
/// be shown to the model. Drained at the turn boundary (`TurnEnd`) so injection
/// never splits an assistant `tool_calls` from its `toolResult`s.
#[derive(Debug, Clone)]
pub struct ResourceUpdate {
    /// Session scope this update belongs to (see [`scope_of_key`]). The host
    /// serves one session at a time, so an update produced under a different
    /// scope must never be injected into the current one.
    pub scope: String,
    pub key: String,
    pub uri: String,
    pub text: String,
}
struct QueuedResourceUpdate {
    update: ResourceUpdate,
    subscription: Subscription,
    revision: u64,
}

impl QueuedResourceUpdate {
    fn new(update: ResourceUpdate, subscription: Subscription) -> Self {
        let revision = subscription.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        Self {
            update,
            subscription,
            revision,
        }
    }
}

static PENDING: OnceLock<Mutex<Vec<QueuedResourceUpdate>>> = OnceLock::new();
fn pending() -> &'static Mutex<Vec<QueuedResourceUpdate>> {
    PENDING.get_or_init(Default::default)
}
const MAX_PENDING: usize = 64;
const MAX_RESOURCE_TEXT: usize = 20_000;

/// The session scope of the most recent tool call.
///
/// Fallback for older embedders without the SDK session environment variable.
/// Current hosts publish the active session on every hot switch, even when no
/// MCP tool has been called in the new session.
static CURRENT_SCOPE: OnceLock<Mutex<Option<String>>> = OnceLock::new();
fn current_scope_slot() -> &'static Mutex<Option<String>> {
    CURRENT_SCOPE.get_or_init(|| Mutex::new(None))
}
fn remember_scope(scope: &str) {
    *current_scope_slot()
        .lock()
        .unwrap_or_else(|p| p.into_inner()) = Some(scope.to_string());
}
pub(crate) fn current_scope() -> Option<String> {
    if let Some(id) = std::env::var_os(rpi_plugin_sdk::SESSION_ID_ENV) {
        // A present but invalid id must fail closed, never reuse the old scope
        // or fall back to the unscoped queue drain.
        return Some(
            id.to_str()
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .map(|id| format!("session:{id}"))
                .unwrap_or_default(),
        );
    }
    current_scope_slot()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
}

/// The session scope embedded in a connection key (`[scope, identity, name]`).
fn scope_of_key(key: &str) -> String {
    serde_json::from_str::<Value>(key)
        .ok()
        .and_then(|value| value.get(0).and_then(Value::as_str).map(str::to_string))
        .unwrap_or_default()
}

/// Drain the queued updates, keeping only those for `scope`.
///
/// Updates belonging to any other scope are dropped: they were produced for a
/// session the host is no longer serving, and injecting them would leak one
/// session's resource contents into another's context. A host that predates
/// `__rpi.sessionId` reports no scope, in which case everything is delivered.
pub(crate) fn take_pending_updates(scope: Option<&str>) -> Vec<ResourceUpdate> {
    take_pending_batch(scope)
        .into_iter()
        .map(|queued| queued.update)
        .collect()
}

fn take_pending_batch(scope: Option<&str>) -> Vec<QueuedResourceUpdate> {
    let drained = std::mem::take(&mut *pending().lock().unwrap_or_else(|p| p.into_inner()));
    match scope {
        Some(scope) => drained
            .into_iter()
            .filter(|queued| queued.update.scope == scope)
            .collect(),
        None => drained,
    }
}

/// Deliver resource notifications at a safe turn boundary.
pub(crate) fn flush_resource_updates(
    append: impl FnOnce(Value) -> Result<(), String>,
) -> Result<(), String> {
    let scope = current_scope();
    let updates = take_pending_batch(scope.as_deref());
    if updates.is_empty() {
        return Ok(());
    }
    let mut text = String::from("🔔 MCP resource update(s):");
    for queued in &updates {
        let update = &queued.update;
        text.push_str(&format!(
            "\n\n--- {} (server {})\n{}",
            update.uri, update.key, update.text
        ));
    }
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let result = append(json!({
        "kind": "custom",
        "role": "custom",
        "content": [{"type":"text","text": text}],
        "data": {"customType": "mcp-resources", "display": true},
        "timestamp": timestamp,
    }));
    if result.is_err() && current_scope() == scope {
        restore_pending_batch(updates);
    }
    result
}

fn restore_pending_batch(updates: Vec<QueuedResourceUpdate>) {
    // Retirement and enqueue use this same lock order. A failed append must
    // never resurrect an unsubscribed URI, a closed connection or an older
    // subscription lifetime. A newer read supersedes an older failed delivery,
    // even if another flush has already drained and delivered that newer read.
    let map = subscriptions().lock().unwrap_or_else(|p| p.into_inner());
    let mut pending = pending().lock().unwrap_or_else(|p| p.into_inner());
    let mut retry = Vec::new();
    for queued in updates {
        if pending.len() + retry.len() >= MAX_PENDING {
            break;
        }
        if map
            .get(&queued.update.key)
            .and_then(|uris| uris.get(&queued.update.uri))
            .is_some_and(|current| {
                Arc::ptr_eq(current, &queued.subscription)
                    && current.load(Ordering::Relaxed) == queued.revision
            })
        {
            retry.push(queued);
        }
    }
    retry.append(&mut *pending);
    *pending = retry;
}

/// Drop queued updates for one connection (close, unsubscribe, failed subscribe).
fn clear_pending_for_key(key: &str) {
    pending()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .retain(|queued| queued.update.key != key);
}

/// Drop queued updates for one connection + URI.
fn clear_pending_for_uri(key: &str, uri: &str) {
    pending()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .retain(|queued| !(queued.update.key == key && queued.update.uri == uri));
}

fn add_subscription(key: &str, uri: &str) -> Subscription {
    let token = Arc::new(AtomicU64::new(0));
    let mut map = subscriptions().lock().unwrap_or_else(|p| p.into_inner());
    map.entry(key.to_string())
        .or_default()
        .insert(uri.to_string(), token.clone());
    // Rotation starts a new delivery lifetime. Purge queued content under the
    // same lock used by enqueue and retirement, as well as invalidating reads.
    clear_pending_for_uri(key, uri);
    token
}

fn subscription(key: &str, uri: &str) -> Option<Subscription> {
    subscriptions()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(key)
        .and_then(|uris| uris.get(uri))
        .cloned()
}

/// Always lock subscriptions before pending, and hold both while invalidating
/// and purging. Enqueue uses the same order, so nothing can slip in after purge.
/// An expected token prevents a failed older operation retiring a newer lifetime.
fn remove_subscription(key: &str, uri: &str, expected: Option<&Subscription>) {
    let mut map = subscriptions().lock().unwrap_or_else(|p| p.into_inner());
    if let Some(expected) = expected {
        if !map
            .get(key)
            .and_then(|uris| uris.get(uri))
            .is_some_and(|current| Arc::ptr_eq(current, expected))
        {
            return;
        }
    }
    if let Some(uris) = map.get_mut(key) {
        uris.remove(uri);
        if uris.is_empty() {
            map.remove(key);
        }
    }
    clear_pending_for_uri(key, uri);
}

fn clear_subscriptions(key: &str) {
    let mut map = subscriptions().lock().unwrap_or_else(|p| p.into_inner());
    map.remove(key);
    clear_pending_for_key(key);
}

/// Re-register only resource subscriptions when a transport is recreated.
/// User tool calls are never replayed. Rotate identities and purge the old
/// connection's queued reads atomically before accepting early notifications.
fn restore_subscriptions(
    key: &str,
    session: &mut Session,
    control: &Control,
    skip_uri: Option<&str>,
) -> Result<(), String> {
    let uris = {
        let mut map = subscriptions().lock().unwrap_or_else(|p| p.into_inner());
        let mut uris = Vec::new();
        if let Some(subscribed) = map.get_mut(key) {
            for (uri, token) in subscribed {
                // An explicit subscribe/unsubscribe handles this URI once the
                // connection is ready; do not issue a duplicate RPC.
                if skip_uri == Some(uri.as_str()) {
                    continue;
                }
                *token = Arc::new(AtomicU64::new(0));
                uris.push(uri.clone());
            }
        }
        clear_pending_for_key(key);
        uris.sort();
        uris
    };
    for uri in uris {
        response_result(session.request("resources/subscribe", json!({"uri":uri}), control)?)?;
    }
    Ok(())
}

/// One dispatcher per connection. It ends when every sender is gone (entry
/// removed and session dropped) or when the connection is stopped, whichever
/// happens first — the stop check is what lets `close`/`shutdown` join it
/// promptly instead of waiting on the reader.
fn spawn_dispatcher(key: String, receiver: Receiver<Value>, stop: Control) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("mcp-notify".into())
        .spawn(move || loop {
            if stop.check().is_err() {
                break;
            }
            match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(value) => dispatch_notification(&key, &value),
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        })
        .expect("spawn MCP notification dispatcher")
}

fn dispatch_notification(key: &str, value: &Value) {
    dispatch_notification_with(key, value, read_resource_by_key);
}

fn dispatch_notification_with(
    key: &str,
    value: &Value,
    read: impl FnOnce(&str, &str) -> Result<String, String>,
) {
    let method = value
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if method != "notifications/resources/updated" {
        return;
    }
    let Some(uri) = value
        .get("params")
        .and_then(|params| params.get("uri"))
        .and_then(Value::as_str)
        .map(str::to_string)
    else {
        return;
    };
    // A subscription is marked *before* its `subscribe` request goes out, so a
    // server that pushes the first update while the handshake is still in flight
    // is accepted here rather than dropped. A failed subscribe rolls the mark
    // back (and purges anything queued in the meantime).
    let Some(token) = subscription(key, &uri) else {
        return;
    };
    if let Ok(text) = read(key, &uri) {
        let map = subscriptions().lock().unwrap_or_else(|p| p.into_inner());
        if !map
            .get(key)
            .and_then(|uris| uris.get(&uri))
            .is_some_and(|current| Arc::ptr_eq(current, &token))
        {
            return;
        }
        // Keep the subscription lock through enqueue, synchronized with retirement.
        let mut pending = pending().lock().unwrap_or_else(|p| p.into_inner());
        if pending.len() < MAX_PENDING {
            pending.push(QueuedResourceUpdate::new(
                ResourceUpdate {
                    scope: scope_of_key(key),
                    key: key.to_string(),
                    uri,
                    text,
                },
                token,
            ));
        }
    }
}

/// Read a resource on an existing connection (used by the dispatcher). Never
/// creates a session: if the connection is gone, the update is dropped. Runs on
/// the dispatcher thread, not the reader, so it can await the reply.
///
/// The control is linked to the connection's `stop`, so `close`/`shutdown`
/// cancels an in-flight read instead of waiting for its timeout — and the lock
/// wait below shares that same deadline.
fn read_resource_by_key(key: &str, uri: &str) -> Result<String, String> {
    let entry = sessions()
        .lock()
        .map_err(|_| "MCP session registry poisoned")?
        .get(key)
        .cloned()
        .ok_or("MCP connection is gone")?;
    let control = Control::default().linked(&entry.stop);
    let deadline = Deadline::new(120, "resources/read", &control);
    let mut state = lock_until(&entry.session, &deadline)?;
    let session = state.session.as_mut().ok_or("MCP connection is closed")?;
    let result = session
        .request("resources/read", json!({"uri": uri}), &control)
        .and_then(response_result);
    if session.failed {
        state.session.take();
    }
    result.map(|result| resource_text(&result))
}

/// Flatten a `resources/read` result's `contents` into text (blobs are noted by
/// size, not inlined).
fn resource_text(result: &Value) -> String {
    let Some(contents) = result.get("contents").and_then(Value::as_array) else {
        return bound_resource_text(result.to_string());
    };
    let mut out = String::new();
    for item in contents {
        let uri = item.get("uri").and_then(Value::as_str).unwrap_or("");
        let body = if let Some(text) = item.get("text").and_then(Value::as_str) {
            text.to_string()
        } else if let Some(blob) = item.get("blob").and_then(Value::as_str) {
            format!("[blob, {} base64 chars]", blob.len())
        } else {
            continue;
        };
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&format!("{uri}\n{body}"));
    }
    bound_resource_text(out)
}

fn bound_resource_text(mut out: String) -> String {
    if out.len() > MAX_RESOURCE_TEXT {
        let mut end = MAX_RESOURCE_TEXT;
        while end > 0 && !out.is_char_boundary(end) {
            end -= 1;
        }
        out.truncate(end);
        out.push_str("\n[truncated]");
    }
    out
}

/// `resources/list` | `read` | `subscribe` | `unsubscribe`.
pub(crate) fn resources(params: &Value, control: &Control) -> Result<Value, String> {
    control.check()?;
    let action = params
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or("list");
    if !matches!(action, "list" | "read" | "subscribe" | "unsubscribe") {
        return Err("mcp_resource action must be list, read, subscribe or unsubscribe".into());
    }
    let (key, name, config, timeout) = target(params)?;

    if action == "list" {
        return with_session(
            key,
            &name,
            config,
            timeout,
            control,
            None,
            |session, control, _| {
                let mut resources = Vec::new();
                let mut cursor = None;
                let mut seen = HashSet::new();
                for _ in 0..100 {
                    let params = cursor
                        .as_ref()
                        .map(|c| json!({"cursor": c}))
                        .unwrap_or_else(|| json!({}));
                    let result =
                        response_result(session.request("resources/list", params, control)?)?;
                    if let Some(items) = result.get("resources").and_then(Value::as_array) {
                        resources.extend(items.iter().cloned());
                    }
                    if resources.len() > 16_384 {
                        return Err("MCP resource list exceeded 16384 resources".into());
                    }
                    match result.get("nextCursor") {
                        None => return Ok(json!({"server": name, "resources": resources})),
                        Some(Value::String(next)) => {
                            if !seen.insert(next.clone()) {
                                return Err(
                                    "MCP resources/list returned a repeated pagination cursor"
                                        .into(),
                                );
                            }
                            cursor = Some(next.clone());
                        }
                        _ => return Err("MCP resources/list nextCursor must be a string".into()),
                    }
                }
                Err("MCP resources/list exceeded 100 pages".into())
            },
        );
    }

    let uri = crate::kit::string_param(params, "uri")?;
    if action == "unsubscribe" {
        let previous = subscription(&key, &uri);
        let mut retired = false;
        let _ = with_session(
            key.clone(),
            &name,
            config,
            timeout,
            control,
            Some(&uri),
            |session, control, _| {
                // Serialize subscription changes with their RPCs on this connection.
                // Invalidate before sending, so even a failed remote unsubscribe
                // stops local delivery and a read cannot outlive the purge.
                remove_subscription(&key, &uri, None);
                retired = true;
                let result = session
                    .request("resources/unsubscribe", json!({"uri": uri}), control)
                    .and_then(response_result);
                // A rejected unsubscribe can leave the remote registration alive
                // after local delivery is retired. Reconnect rather than reusing
                // that mismatched state; other URIs will be restored normally.
                if result.is_err() {
                    session.failed = true;
                }
                result
            },
        );
        // If connecting/locking failed, retire only the lifetime we observed;
        // never remove a fresh subscription created by a concurrent call.
        if !retired {
            if let Some(previous) = previous {
                remove_subscription(&key, &uri, Some(&previous));
            }
        }
        return Ok(json!({"server": name, "uri": uri, "unsubscribed": true}));
    }
    if action == "subscribe" {
        if matches!(&config, McpServerConfig::Http(_)) {
            return Err(
                "mcp_resource subscribe is not supported for HTTP servers yet (no push channel)"
                    .into(),
            );
        }
        with_session(
            key.clone(),
            &name,
            config,
            timeout,
            control,
            Some(&uri),
            |session, control, new_connection| {
                // On an existing transport, a mark means the server is already
                // subscribed. A fresh transport skips restoration for this URI
                // and needs exactly one RPC below, with its new local lifetime
                // installed before any early notification can arrive.
                let already_subscribed = !new_connection && subscription(&key, &uri).is_some();
                // Register before the RPC so early notifications are accepted, while
                // holding the connection lock so subscribe/unsubscribe stay ordered.
                let token = add_subscription(&key, &uri);
                if already_subscribed {
                    return Ok(json!({}));
                }
                let subscribed = session
                    .request("resources/subscribe", json!({"uri": uri}), control)
                    .and_then(response_result);
                if subscribed.is_err() {
                    remove_subscription(&key, &uri, Some(&token));
                }
                subscribed
            },
        )?;
        return Ok(json!({"server": name, "uri": uri, "subscribed": true}));
    }
    // read — flatten and bound the contents: a large blob must not reach the
    // model through the tool result.
    with_session(
        key,
        &name,
        config,
        timeout,
        control,
        None,
        |session, control, _| {
            let result = response_result(session.request(
                "resources/read",
                json!({"uri": uri}),
                control,
            )?)?;
            Ok(json!({"server": name, "uri": uri, "text": resource_text(&result)}))
        },
    )
}

fn lock_until<'a, T>(
    mutex: &'a Mutex<T>,
    deadline: &Deadline,
) -> Result<MutexGuard<'a, T>, String> {
    loop {
        deadline.check()?;
        match mutex.try_lock() {
            Ok(guard) => return Ok(guard),
            Err(TryLockError::Poisoned(_)) => return Err("MCP connection lock poisoned".into()),
            Err(TryLockError::WouldBlock) => std::thread::sleep(deadline.slice()?),
        }
    }
}
fn with_session<T>(
    key: String,
    name: &str,
    config: McpServerConfig,
    timeout: u64,
    control: &Control,
    skip_restore_uri: Option<&str>,
    run: impl FnOnce(&mut Session, &Control, bool) -> Result<T, String>,
) -> Result<T, String> {
    let entry = {
        let mut sessions = sessions()
            .lock()
            .map_err(|_| "MCP session registry poisoned")?;
        if !sessions.contains_key(&key) && sessions.len() >= 64 {
            return Err("MCP connection limit (64) reached; close unused connections with mcp_list action=close".into());
        }
        match sessions.get(&key) {
            Some(entry) => entry.clone(),
            None => {
                let (notifications, receiver) = mpsc::sync_channel(64);
                let stop = Control::default();
                let dispatcher = spawn_dispatcher(key.clone(), receiver, stop.clone());
                let entry = Arc::new(Entry {
                    session: Mutex::new(ConnectionState::default()),
                    stop,
                    notifications,
                    dispatcher: Mutex::new(Some(dispatcher)),
                });
                sessions.insert(key.clone(), Arc::clone(&entry));
                entry
            }
        }
    };
    let control = control.linked(&entry.stop);
    let deadline = Deadline::new(timeout, "connection lock", &control);
    let mut state = lock_until(&entry.session, &deadline)?;
    if state.config.as_ref() != Some(&config) {
        clear_subscriptions(&key);
        state.session.take();
        state.config = Some(config.clone());
    }
    if state.session.as_ref().is_some_and(|session| session.failed) {
        state.session.take();
    }
    let new_connection = state.session.is_none();
    if new_connection {
        let mut session = Session::start(&config, name, timeout, &control, &entry.notifications)?;
        if let Err(error) = restore_subscriptions(&key, &mut session, &control, skip_restore_uri) {
            if session.failed || control.check().is_err() {
                return Err(format!("MCP resource subscription restoration interrupted: {error}; the next call will retry"));
            }
            clear_subscriptions(&key);
            return Err(format!("MCP resource subscriptions could not be restored: {error}; subscriptions cleared, subscribe again"));
        }
        state.session = Some(session);
    }
    let session = state.session.as_mut().ok_or("MCP session unavailable")?;
    session.transport.timeout(timeout);
    let result = run(session, &control, new_connection);
    if session.failed {
        state.session.take();
    } // Do not retry tool calls: they may have side effects.
    result
}

/// Stop a released entry and join its dispatcher. Cancelling `stop` first makes
/// the dispatcher's 100 ms wait return promptly, and it also aborts any
/// in-flight background `resources/read` (whose control is linked to it).
fn retire_entry(entry: &Arc<Entry>) {
    entry.stop.cancel();
    // Serialize joins too: another close/shutdown must not remove this entry
    // and allow a replacement while the first caller is still joining it.
    if let Ok(mut slot) = entry.dispatcher.lock() {
        if let Some(handle) = slot.take() {
            let _ = handle.join();
        }
    }
}

pub fn shutdown_connections() {
    let entries = if let Ok(map) = sessions().lock() {
        // Keep stopped entries registered until their active operations have
        // released the connection lock. New calls cannot reopen the same key
        // while its old subscription lifetimes are still being retired.
        for entry in map.values() {
            entry.stop.cancel();
        }
        map.clone()
    } else {
        return;
    };
    for (key, entry) in entries {
        let _ = release_entry(&key, &entry);
    }
}
pub(crate) fn close_key(key: &str) -> Result<(), String> {
    let entry = {
        let map = sessions()
            .lock()
            .map_err(|_| "MCP session registry poisoned")?;
        match map.get(key) {
            Some(entry) => {
                entry.stop.cancel();
                Some(entry.clone())
            }
            None => {
                clear_subscriptions(key);
                None
            }
        }
    };
    if let Some(entry) = entry {
        release_entry(key, &entry)?;
    }
    Ok(())
}

/// Drain active operations before purging local lifetimes. Keep removal and
/// purge atomic with respect to opening a replacement, and let only the owner
/// of the registered entry purge it (concurrent close/shutdown may join twice).
fn release_entry(key: &str, entry: &Arc<Entry>) -> Result<(), String> {
    {
        let mut state = entry
            .session
            .lock()
            .map_err(|_| "MCP connection lock poisoned")?;
        state.session.take();
    }
    // Keep the stopped entry registered while joining: an old notification
    // must not resolve this key to a newly opened connection and read from it.
    retire_entry(entry);
    let mut map = sessions()
        .lock()
        .map_err(|_| "MCP session registry poisoned")?;
    if map
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, entry))
    {
        clear_subscriptions(key);
        map.remove(key);
    }
    Ok(())
}

pub(crate) fn host_scope(params: &Value) -> String {
    let host = params.get(rpi_plugin_sdk::HOST_CONTEXT_KEY);
    if let Some(id) = host
        .and_then(|v| v.get("sessionId"))
        .and_then(Value::as_str)
    {
        return format!("session:{id}");
    }
    if let Some(cwd) = host.and_then(|v| v.get("cwd")).and_then(Value::as_str) {
        return format!(
            "project:{}",
            std::fs::canonicalize(cwd)
                .unwrap_or_else(|_| PathBuf::from(cwd))
                .to_string_lossy()
        );
    }
    "library".into()
}

fn host_cwd(params: &Value) -> PathBuf {
    params
        .get(rpi_plugin_sdk::HOST_CONTEXT_KEY)
        .and_then(|v| v.get("cwd"))
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
}
pub(crate) fn config_file(params: &Value) -> Result<PathBuf, String> {
    let path = params
        .get("configPath")
        .and_then(Value::as_str)
        .ok_or("missing configPath")?;
    std::fs::canonicalize(host_cwd(params).join(path))
        .map_err(|e| format!("read MCP config {path:?}: {e}"))
}
pub(crate) fn load_config(params: &Value) -> Result<(PathBuf, McpConfig), String> {
    let path = config_file(params)?;
    let text = std::fs::read_to_string(&path).map_err(|e| format!("read MCP config: {e}"))?;
    Ok((path, McpConfig::parse(&text)?))
}
/// The registry key for one `(scope, config identity, server name)` triple.
/// Extracted so the command surface can map a configured server back to its
/// live connection without duplicating the identity derivation in [`target`].
pub(crate) fn connection_key(scope: &str, identity: &str, name: &str) -> String {
    serde_json::to_string(&(scope, identity, name)).unwrap_or_default()
}

pub(crate) fn target(params: &Value) -> Result<(String, String, McpServerConfig, u64), String> {
    let scope = host_scope(params);
    // Latch the session this call belongs to: `TurnEnd` carries no payload, so
    // this is how the injection path learns which session it is serving.
    remember_scope(&scope);
    let (name, identity, mut config) = if params.get("configPath").is_some() {
        if params.get("url").is_some() {
            return Err("specify configPath/server or url, not both".into());
        }
        let name = crate::kit::string_param(params, "server")?;
        let (path, config) = load_config(params)?;
        let server = config
            .servers
            .get(&name)
            .ok_or_else(|| format!("MCP server {name:?} is not configured"))?
            .clone();
        (name, path.to_string_lossy().into_owned(), server)
    } else {
        if params.get("server").is_some() {
            return Err("server requires configPath".into());
        }
        let url =
            crate::http::validate_mcp_url(&crate::kit::string_param(params, "url")?)?.to_string();
        let config = McpServerConfig::Http(crate::HttpServer {
            url: url.clone(),
            headers: Default::default(),
            enabled: true,
            timeout: 60,
            exposure: Default::default(),
        });
        ("http".into(), url, config)
    };
    let key = connection_key(&scope, &identity, &name);
    let (enabled, default_timeout) = match &config {
        McpServerConfig::Stdio(s) => (s.enabled, s.timeout),
        McpServerConfig::Http(s) => (s.enabled, s.timeout),
    };
    if !enabled {
        close_key(&key)?;
        return Err(format!("MCP server {name:?} is disabled"));
    }
    if let McpServerConfig::Stdio(server) = &mut config {
        // A configured cwd is relative to the config file; otherwise use host project cwd.
        if let Some(cwd) = &server.cwd {
            if Path::new(cwd).is_relative() {
                let file = config_file(params)?;
                server.cwd = Some(
                    file.parent()
                        .unwrap_or(Path::new("."))
                        .join(cwd)
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        } else {
            server.cwd = Some(host_cwd(params).to_string_lossy().into_owned());
        }
    }
    let timeout = match params.get("timeoutSeconds") {
        None => default_timeout.clamp(1, 300),
        Some(value) => value
            .as_u64()
            .filter(|v| (1..=300).contains(v))
            .ok_or("timeoutSeconds must be an integer from 1 to 300")?,
    };
    Ok((key, name, config, timeout))
}

pub(crate) fn call(params: &Value, control: &Control) -> Result<Value, String> {
    let tool = crate::kit::string_param(params, "tool")?;
    if tool.trim().is_empty() || tool.len() > 200 {
        return Err("MCP tool must contain 1-200 characters".into());
    }
    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if !arguments.is_object() {
        return Err("MCP arguments must be an object".into());
    }
    let (key, name, config, timeout) = target(params)?;
    with_session(
        key,
        &name,
        config,
        timeout,
        control,
        None,
        |session, control, _| {
            let tools = session.tools(control)?;
            if !tools.iter().any(|t| t.name == tool) {
                return Err(format!("MCP server does not expose tool {tool:?}"));
            }
            let request = tools_call_request(0, &tool, arguments);
            response_result(session.request(
                "tools/call",
                request.params.unwrap_or_else(|| json!({})),
                control,
            )?)
        },
    )
}
pub(crate) fn list(params: &Value, control: &Control) -> Result<Value, String> {
    control.check()?;
    let action = params
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or("list");
    if !matches!(action, "list" | "close") {
        return Err("mcp_list action must be list or close".into());
    }
    if params.get("configPath").is_some() && params.get("server").is_none() {
        if action != "list" {
            return Err("close requires a server or url".into());
        }
        let (_, config) = load_config(params)?;
        let servers: Vec<Value> = config
            .servers
            .iter()
            .map(|(name, config)| {
                let (transport, enabled) = match config {
                    McpServerConfig::Stdio(s) => ("stdio", s.enabled),
                    McpServerConfig::Http(s) => ("http", s.enabled),
                };
                json!({"server":name, "transport":transport, "enabled":enabled})
            })
            .collect();
        return Ok(json!({"servers":servers}));
    }
    let (key, name, config, timeout) = target(params)?;
    if action == "close" {
        close_key(&key)?;
        return Ok(json!({"server":name,"closed":true}));
    }
    with_session(
        key,
        &name,
        config,
        timeout,
        control,
        None,
        |session, control, _| {
            let tools = session.tools(control)?;
            Ok(
                json!({"server":name, "serverInfo":{"name":session.info.name,"version":session.info.version},"protocolVersion":session.info.protocol_version,"tools":tools}),
            )
        },
    )
}

pub fn call_from_config(
    config_path: &str,
    server_name: &str,
    tool_name: &str,
    arguments: Value,
) -> Result<String, String> {
    call_from_config_with_timeout(config_path, server_name, tool_name, arguments, None)
}
pub fn call_from_config_with_timeout(
    config_path: &str,
    server_name: &str,
    tool_name: &str,
    arguments: Value,
    timeout: Option<u64>,
) -> Result<String, String> {
    let mut params = json!({"configPath":config_path,"server":server_name,"tool":tool_name,"arguments":arguments});
    if let Some(timeout) = timeout {
        params["timeoutSeconds"] = timeout.into();
    }
    let result = call(&params, &Control::default())?;
    crate::connection::check_tool_result(&result)?;
    Ok(crate::call_result_text(&result))
}
pub fn discover_from_config(
    config_path: &str,
    server_name: &str,
) -> Result<DiscoveredServer, String> {
    let value = list(
        &json!({"configPath":config_path,"server":server_name}),
        &Control::default(),
    )?;
    Ok(DiscoveredServer {
        name: server_name.into(),
        info: ServerInfo {
            name: value["serverInfo"]["name"]
                .as_str()
                .unwrap_or("unknown")
                .into(),
            version: value["serverInfo"]["version"]
                .as_str()
                .unwrap_or("unknown")
                .into(),
            protocol_version: value["protocolVersion"].as_str().unwrap_or("").into(),
        },
        tools: serde_json::from_value(value["tools"].clone()).map_err(|e| e.to_string())?,
    })
}
pub fn close_from_config(config_path: &str, server_name: &str) -> Result<(), String> {
    list(
        &json!({"configPath":config_path,"server":server_name,"action":"close"}),
        &Control::default(),
    )
    .map(|_| ())
}

/// `mcp_resource`'s actions against a configured server (`list`/`read`/
/// `subscribe`/`unsubscribe`), for embedders and integration tests.
pub fn resources_from_config(
    config_path: &str,
    server_name: &str,
    action: &str,
    uri: Option<&str>,
) -> Result<Value, String> {
    let mut params = json!({"configPath":config_path,"server":server_name,"action":action});
    if let Some(uri) = uri {
        params["uri"] = json!(uri);
    }
    resources(&params, &Control::default())
}

/// Drain the resource updates queued for `scope` — the same queue the `TurnEnd`
/// injection path consumes. `None` drains everything (a host with no session id).
pub fn take_resource_updates(scope: Option<&str>) -> Vec<ResourceUpdate> {
    take_pending_updates(scope)
}

/// Report whether a configured server currently has a live connection. Used by
/// the `/mcp` status surface; never opens a connection and never blocks on one.
pub(crate) fn is_connected(scope: &str, identity: &str, name: &str) -> bool {
    sessions()
        .lock()
        .map(|map| map.contains_key(&connection_key(scope, identity, name)))
        .unwrap_or(false)
}

/// Snapshot every configured server's transport, enabled flag and live
/// connection state. Reads the config file directly so disabled servers are
/// still reported; the model-facing `mcp_list` tool only lists configured
/// names, so this stays a command-surface concern.
pub(crate) fn status(params: &Value) -> Result<Value, String> {
    let scope = host_scope(params);
    remember_scope(&scope);
    let (path, config) = load_config(params)?;
    let identity = path.to_string_lossy().into_owned();
    let servers: Vec<Value> = config
        .servers
        .iter()
        .map(|(name, server)| {
            let (transport, enabled) = match server {
                McpServerConfig::Stdio(stdio) => ("stdio", stdio.enabled),
                McpServerConfig::Http(http) => ("http", http.enabled),
            };
            json!({
                "server": name,
                "transport": transport,
                "enabled": enabled,
                "connected": enabled && is_connected(&scope, &identity, name),
            })
        })
        .collect();
    Ok(json!({
        "config": path.to_string_lossy(),
        "servers": servers,
    }))
}

/// Release a server's connection so the next call reconnects from scratch.
/// Unlike the `mcp_list action=close` tool this never starts a connection and
/// tolerates a server that is not currently connected.
pub(crate) fn reconnect(params: &Value, control: &Control) -> Result<Value, String> {
    control.check()?;
    let (key, name, _config, _timeout) = target(params)?;
    close_key(&key)?;
    Ok(json!({"server": name, "reconnected": true}))
}

/// List a server's prompts (`prompts/list`), paginated like tools/resources.
pub(crate) fn prompts(params: &Value, control: &Control) -> Result<Value, String> {
    control.check()?;
    let (key, name, config, timeout) = target(params)?;
    with_session(
        key,
        &name,
        config,
        timeout,
        control,
        None,
        |session, control, _| {
            let mut prompts = Vec::new();
            let mut cursor = None;
            let mut seen = HashSet::new();
            for _ in 0..100 {
                let params = cursor
                    .as_ref()
                    .map(|c| json!({"cursor": c}))
                    .unwrap_or_else(|| json!({}));
                let result = response_result(session.request("prompts/list", params, control)?)?;
                if let Some(items) = result.get("prompts").and_then(Value::as_array) {
                    prompts.extend(items.iter().cloned());
                }
                if prompts.len() > 16_384 {
                    return Err("MCP prompt list exceeded 16384 prompts".into());
                }
                match result.get("nextCursor") {
                    None => return Ok(json!({"server": name, "prompts": prompts})),
                    Some(Value::String(next)) => {
                        if !seen.insert(next.clone()) {
                            return Err(
                                "MCP prompts/list returned a repeated pagination cursor".into()
                            );
                        }
                        cursor = Some(next.clone());
                    }
                    _ => return Err("MCP prompts/list nextCursor must be a string".into()),
                }
            }
            Err("MCP prompts/list exceeded 100 pages".into())
        },
    )
}

/// Fetch one prompt (`prompts/get`) as a list of messages. The command surface
/// renders the messages and puts them in the editor, mirroring how MCP prompt
/// templates behave as slash commands in other hosts.
pub(crate) fn prompt_get(params: &Value, control: &Control) -> Result<Value, String> {
    control.check()?;
    let name = crate::kit::string_param(params, "prompt")?;
    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if !arguments.is_object() {
        return Err("MCP prompt arguments must be an object".into());
    }
    let (key, server, config, timeout) = target(params)?;
    with_session(
        key,
        &server,
        config,
        timeout,
        control,
        None,
        |session, control, _| {
            let result = response_result(session.request(
                "prompts/get",
                json!({"name": name, "arguments": arguments}),
                control,
            )?)?;
            Ok(json!({"server": server, "prompt": name, "result": result}))
        },
    )
}

/// Serializes tests that read or write the process-global session scope
/// ([`CURRENT_SCOPE`]) or the `RPI_SESSION_ID` environment variable.
///
/// Connection tests drive [`target`], which latches the scope from the host
/// context; the queue tests assert on that latch. The parallel test harness
/// runs both, so they must share one lock or the latch races (a connection
/// test's `library` fallback can overwrite a queue test's `session:a`).
#[cfg(test)]
pub(crate) fn scope_test_guard() -> MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|p| p.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_text_flattens_text_and_notes_blobs() {
        let text = resource_text(&json!({"contents": [
            {"uri": "res://a", "mimeType": "text/plain", "text": "hello"},
            {"uri": "res://b", "blob": "QUJD"}]}));
        assert!(text.contains("res://a"));
        assert!(text.contains("hello"));
        assert!(text.contains("[blob, 4 base64 chars]"), "got: {text}");
    }

    #[test]
    fn resource_text_bounds_regular_and_unexpected_responses() {
        let huge = "中".repeat(MAX_RESOURCE_TEXT);
        for response in [
            json!({"contents":[{"uri":"mem://large","text":huge}]}),
            json!({"unexpected":huge}),
        ] {
            let text = resource_text(&response);
            assert!(
                text.len() <= MAX_RESOURCE_TEXT + "\n[truncated]".len(),
                "unbounded resource response: {} bytes",
                text.len()
            );
            assert!(text.ends_with("\n[truncated]"));
        }
    }

    /// The queue and session scope are process-global, so these tests must not
    /// interleave with each other or with connection tests that latch the scope.
    fn queue_test_guard() -> MutexGuard<'static, ()> {
        scope_test_guard()
    }

    fn queue(scope: &str, key: &str, uri: &str, text: &str) {
        let token = subscription(key, uri).unwrap_or_default();
        pending()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(QueuedResourceUpdate::new(
                ResourceUpdate {
                    scope: scope.into(),
                    key: key.into(),
                    uri: uri.into(),
                    text: text.into(),
                },
                token,
            ));
    }

    fn clear_queue() {
        pending().lock().unwrap_or_else(|p| p.into_inner()).clear();
    }

    #[test]
    fn a_new_subscription_lifetime_drops_the_previous_queued_content() {
        let _serial = queue_test_guard();
        clear_queue();
        const KEY: &str = r#"["session:renew","config","server"]"#;
        const URI: &str = "mem://renew";
        add_subscription(KEY, URI);
        queue("session:renew", KEY, URI, "obsolete content");
        add_subscription(KEY, URI);
        let obsolete = take_pending_updates(None);
        queue("session:renew", KEY, URI, "fresh content");
        let fresh = take_pending_updates(None);
        clear_subscriptions(KEY);
        assert!(
            obsolete.is_empty(),
            "old subscription content survived renewal: {obsolete:?}"
        );
        assert_eq!(fresh.len(), 1);
        assert_eq!(fresh[0].text, "fresh content");
    }

    #[test]
    fn failed_host_append_preserves_resource_updates_for_the_next_boundary() {
        let _serial = queue_test_guard();
        let _env = SessionEnvGuard::new();
        clear_queue();
        std::env::set_var(rpi_plugin_sdk::SESSION_ID_ENV, "delivery-retry");
        const KEY: &str = r#"["session:delivery-retry","config","server"]"#;
        const URI: &str = "mem://delivery";
        add_subscription(KEY, URI);
        let notification = json!({"method":"notifications/resources/updated","params":{"uri":URI}});
        dispatch_notification_with(KEY, &notification, |_, _| Ok("current content".into()));
        assert!(flush_resource_updates(|_| Err("host unavailable".into())).is_err());
        assert_eq!(
            pending().lock().unwrap().len(),
            1,
            "failed append lost the update"
        );
        let mut delivered = None;
        flush_resource_updates(|message| {
            delivered = Some(message);
            Ok(())
        })
        .unwrap();
        clear_subscriptions(KEY);
        assert!(delivered.unwrap()["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("current content"));
        flush_resource_updates(|_| panic!("successfully delivered update was replayed")).unwrap();
    }

    #[test]
    fn failed_delivery_cannot_revive_retired_or_superseded_resource_content() {
        let _serial = queue_test_guard();
        let _env = SessionEnvGuard::new();
        std::env::set_var(rpi_plugin_sdk::SESSION_ID_ENV, "delivery-lifetime");
        const KEY: &str = r#"["session:delivery-lifetime","config","server"]"#;
        const URI: &str = "mem://lifetime";
        for action in [
            "unsubscribe",
            "close",
            "replace",
            "newer-read",
            "newer-delivered",
        ] {
            clear_queue();
            add_subscription(KEY, URI);
            queue("session:delivery-lifetime", KEY, URI, "obsolete content");
            let mut delivered = Vec::new();
            let result = flush_resource_updates(|_| {
                match action {
                    "unsubscribe" => remove_subscription(KEY, URI, None),
                    "close" => clear_subscriptions(KEY),
                    _ => {
                        if action == "replace" {
                            add_subscription(KEY, URI);
                        }
                        queue("session:delivery-lifetime", KEY, URI, "fresh content");
                        if action == "newer-delivered" {
                            // Another boundary already delivered the newer read
                            // before this older append reports failure.
                            flush_resource_updates(|message| {
                                delivered.push(message);
                                Ok(())
                            })
                            .unwrap();
                        }
                    }
                }
                Err("host unavailable".into())
            });
            assert!(result.is_err());
            flush_resource_updates(|message| {
                delivered.push(message);
                Ok(())
            })
            .unwrap();
            clear_subscriptions(KEY);
            if matches!(action, "unsubscribe" | "close") {
                assert!(delivered.is_empty(), "{action}: {delivered:?}");
            } else {
                assert_eq!(delivered.len(), 1, "{action}: {delivered:?}");
                let text = delivered[0]["content"][0]["text"].as_str().unwrap();
                assert!(text.contains("fresh content"), "{action}: {text}");
                assert!(!text.contains("obsolete content"), "{action}: {text}");
            }
        }
    }

    #[test]
    fn failed_delivery_drops_updates_after_a_session_switch() {
        let _serial = queue_test_guard();
        let _env = SessionEnvGuard::new();
        clear_queue();
        const KEY_A: &str = r#"["session:delivery-a","config","server"]"#;
        const KEY_B: &str = r#"["session:delivery-b","config","server"]"#;
        const URI: &str = "mem://session";
        std::env::set_var(rpi_plugin_sdk::SESSION_ID_ENV, "delivery-a");
        add_subscription(KEY_A, URI);
        queue("session:delivery-a", KEY_A, URI, "A content");
        assert!(flush_resource_updates(|_| {
            std::env::set_var(rpi_plugin_sdk::SESSION_ID_ENV, "delivery-b");
            add_subscription(KEY_B, URI);
            queue("session:delivery-b", KEY_B, URI, "B content");
            Err("host switched session".into())
        })
        .is_err());
        let updates = take_pending_updates(None);
        clear_subscriptions(KEY_A);
        clear_subscriptions(KEY_B);
        assert_eq!(updates.len(), 1, "old session was requeued: {updates:?}");
        assert_eq!(updates[0].text, "B content");
    }

    #[test]
    fn failed_delivery_keeps_the_queue_bounded_and_prefers_new_updates() {
        let _serial = queue_test_guard();
        let _env = SessionEnvGuard::new();
        clear_queue();
        std::env::set_var(rpi_plugin_sdk::SESSION_ID_ENV, "delivery-bounds");
        const KEY: &str = r#"["session:delivery-bounds","config","server"]"#;
        add_subscription(KEY, "mem://old");
        queue("session:delivery-bounds", KEY, "mem://old", "old content");
        assert!(flush_resource_updates(|_| {
            for index in 0..MAX_PENDING {
                let uri = format!("mem://fresh-{index}");
                add_subscription(KEY, &uri);
                queue("session:delivery-bounds", KEY, &uri, "fresh content");
            }
            Err("host unavailable".into())
        })
        .is_err());
        let updates = take_pending_updates(None);
        clear_subscriptions(KEY);
        assert_eq!(updates.len(), MAX_PENDING);
        assert!(updates.iter().all(|update| update.text == "fresh content"));
    }

    fn assert_teardown_retires_late_subscription(shutdown: bool) {
        let _serial = queue_test_guard();
        clear_queue();
        const KEY: &str = r#"["session:closing","config","server"]"#;
        const URI: &str = "mem://closing";
        let (notifications, _receiver) = mpsc::sync_channel(1);
        let entry = Arc::new(Entry {
            session: Mutex::new(ConnectionState::default()),
            stop: Control::default(),
            notifications,
            dispatcher: Mutex::new(None),
        });
        sessions().lock().unwrap().insert(KEY.into(), entry.clone());
        // Subscribe mutates its local lifetime under this connection lock,
        // before its RPC observes cancellation. Pause it immediately before
        // that mutation, then start teardown on another thread.
        let state = entry.session.lock().unwrap();
        let closing = std::thread::spawn(move || {
            if shutdown {
                shutdown_connections();
            } else {
                close_key(KEY).unwrap();
            }
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while entry.stop.check().is_ok() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        let cancelled = entry.stop.check().is_err();
        add_subscription(KEY, URI);
        queue("session:closing", KEY, URI, "late content");
        drop(state);
        closing.join().unwrap();
        let retained = subscription(KEY, URI).is_some();
        let updates = take_pending_updates(None);
        clear_subscriptions(KEY);
        assert!(cancelled, "teardown did not cancel the active operation");
        assert!(
            !retained,
            "teardown left an in-flight subscription registered"
        );
        assert!(
            updates.is_empty(),
            "teardown left queued content: {updates:?}"
        );
        assert!(!sessions().lock().unwrap().contains_key(KEY));
    }

    #[test]
    fn close_retires_subscriptions_after_in_flight_operations_exit() {
        assert_teardown_retires_late_subscription(false);
    }

    #[test]
    fn closing_connection_stays_registered_until_its_dispatcher_exits() {
        let _serial = queue_test_guard();
        const KEY: &str = r#"["session:dispatcher-close","config","server"]"#;
        let stop = Control::default();
        let worker_stop = stop.clone();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let dispatcher = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            while worker_stop.check().is_ok() && std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(5));
            }
            ready_tx.send(()).unwrap();
            // Model a notification already being processed when close begins.
            let _ = resume_rx.recv_timeout(Duration::from_secs(3));
        });
        let (notifications, _receiver) = mpsc::sync_channel(1);
        let entry = Arc::new(Entry {
            session: Mutex::new(ConnectionState::default()),
            stop,
            notifications,
            dispatcher: Mutex::new(Some(dispatcher)),
        });
        sessions().lock().unwrap().insert(KEY.into(), entry.clone());
        let closing = std::thread::spawn(|| close_key(KEY));
        let ready = ready_rx.recv_timeout(Duration::from_secs(3));
        // Removing the key early lets a new call replace it while the old
        // dispatcher can still resolve it in read_resource_by_key.
        let retained = sessions()
            .lock()
            .unwrap()
            .get(KEY)
            .is_some_and(|current| Arc::ptr_eq(current, &entry));
        let _ = resume_tx.send(());
        closing.join().unwrap().unwrap();
        assert!(ready.is_ok(), "dispatcher fixture did not observe close");
        assert!(
            retained,
            "a replacement could open before the old dispatcher exited"
        );
        assert!(!sessions().lock().unwrap().contains_key(KEY));
    }

    #[test]
    fn shutdown_retires_subscriptions_after_in_flight_operations_exit() {
        // Shutdown intentionally affects every registry entry. Isolate this
        // test from parallel HTTP lifecycle tests that also use the registry.
        const CHILD: &str = "RPI_MCP_SHUTDOWN_TEST_CHILD";
        if std::env::var_os(CHILD).is_some() {
            assert_teardown_retires_late_subscription(true);
            return;
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime::tests::shutdown_retires_subscriptions_after_in_flight_operations_exit",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn repeated_retirement_cannot_clear_a_replacement_connections_subscriptions() {
        let _serial = queue_test_guard();
        clear_queue();
        const KEY: &str = r#"["session:replacement","config","server"]"#;
        const URI: &str = "mem://replacement";
        let make_entry = || {
            let (notifications, _receiver) = mpsc::sync_channel(1);
            Arc::new(Entry {
                session: Mutex::new(ConnectionState::default()),
                stop: Control::default(),
                notifications,
                dispatcher: Mutex::new(None),
            })
        };
        let old = make_entry();
        sessions().lock().unwrap().insert(KEY.into(), old.clone());
        close_key(KEY).unwrap();
        let current = make_entry();
        sessions()
            .lock()
            .unwrap()
            .insert(KEY.into(), current.clone());
        let token = add_subscription(KEY, URI);
        queue("session:replacement", KEY, URI, "new content");
        // A concurrent shutdown may have captured the old entry before close
        // finished, then reach its cleanup after the key has been reopened.
        release_entry(KEY, &old).unwrap();
        let retained = subscription(KEY, URI).is_some_and(|current| Arc::ptr_eq(&current, &token));
        let still_registered = sessions()
            .lock()
            .unwrap()
            .get(KEY)
            .is_some_and(|entry| Arc::ptr_eq(entry, &current));
        let updates = take_pending_updates(None);
        close_key(KEY).unwrap();
        assert!(retained, "old cleanup cleared the replacement subscription");
        assert!(
            still_registered,
            "old cleanup removed the replacement connection"
        );
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].text, "new content");
    }

    #[test]
    fn in_flight_read_cannot_enqueue_after_subscription_retirement() {
        let _serial = queue_test_guard();
        const KEY: &str = r#"["session:race","config","server"]"#;
        const URI: &str = "mem://race";
        let notification = json!({"method":"notifications/resources/updated","params":{"uri":URI}});
        for action in ["unsubscribe", "close-and-reopen", "replace"] {
            clear_queue();
            add_subscription(KEY, URI);
            let (ready_tx, ready_rx) = mpsc::channel();
            let (resume_tx, resume_rx) = mpsc::channel();
            let value = notification.clone();
            let worker = std::thread::spawn(move || {
                dispatch_notification_with(KEY, &value, |_, _| {
                    // Pause a successful read just before the dispatcher can
                    // enqueue it, reproducing the original retirement race.
                    ready_tx.send(()).unwrap();
                    resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok("obsolete content".into())
                });
            });
            ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            match action {
                "unsubscribe" => remove_subscription(KEY, URI, None),
                "close-and-reopen" => {
                    clear_subscriptions(KEY);
                    add_subscription(KEY, URI);
                }
                "replace" => {
                    add_subscription(KEY, URI);
                }
                _ => unreachable!(),
            }
            queue("session:other", "other-key", URI, "unrelated");
            resume_tx.send(()).unwrap();
            worker.join().unwrap();
            let updates = take_pending_updates(None);
            assert_eq!(
                updates.len(),
                1,
                "{action}: stale read was queued: {updates:?}"
            );
            assert_eq!(updates[0].text, "unrelated");

            if action != "unsubscribe" {
                dispatch_notification_with(KEY, &notification, |_, _| Ok("fresh content".into()));
                let updates = take_pending_updates(None);
                assert_eq!(
                    updates.len(),
                    1,
                    "{action}: new subscription stopped working"
                );
                assert_eq!(updates[0].text, "fresh content");
            }
            clear_subscriptions(KEY);
        }
    }

    #[test]
    fn stale_failure_cannot_remove_a_new_subscription_or_its_updates() {
        let _serial = queue_test_guard();
        clear_queue();
        let old = add_subscription("key", "mem://a");
        let current = add_subscription("key", "mem://a");
        queue("session:a", "key", "mem://a", "fresh");
        remove_subscription("key", "mem://a", Some(&old));
        assert!(Arc::ptr_eq(
            &subscription("key", "mem://a").unwrap(),
            &current
        ));
        assert_eq!(take_pending_updates(None).len(), 1);
        remove_subscription("key", "mem://a", Some(&current));
        assert!(subscription("key", "mem://a").is_none());
    }

    struct SessionEnvGuard(Option<std::ffi::OsString>, Option<String>);

    impl SessionEnvGuard {
        fn new() -> Self {
            Self(
                std::env::var_os(rpi_plugin_sdk::SESSION_ID_ENV),
                current_scope_slot().lock().unwrap().clone(),
            )
        }
    }

    impl Drop for SessionEnvGuard {
        fn drop(&mut self) {
            match &self.0 {
                Some(value) => std::env::set_var(rpi_plugin_sdk::SESSION_ID_ENV, value),
                None => std::env::remove_var(rpi_plugin_sdk::SESSION_ID_ENV),
            }
            *current_scope_slot().lock().unwrap() = self.1.clone();
            clear_queue();
        }
    }

    #[test]
    fn hot_switch_without_an_mcp_call_does_not_deliver_previous_session_updates() {
        let _serial = queue_test_guard();
        let _env = SessionEnvGuard::new();
        clear_queue();
        std::env::set_var(rpi_plugin_sdk::SESSION_ID_ENV, "a");
        remember_scope("session:a");
        queue("session:a", "key-a", "mem://secret", "A secret");

        // The host switches to B; B makes no MCP call to refresh the latch.
        std::env::set_var(rpi_plugin_sdk::SESSION_ID_ENV, "b");
        let scope = current_scope();
        assert_eq!(scope.as_deref(), Some("session:b"));
        assert!(take_pending_updates(scope.as_deref()).is_empty());
        assert!(take_pending_updates(Some("session:a")).is_empty());

        queue("session:b", "key-b", "mem://b", "B");
        let served = take_pending_updates(current_scope().as_deref());
        assert_eq!(served.len(), 1);
        assert_eq!(served[0].text, "B");
    }

    #[test]
    fn invalid_current_session_never_reuses_cached_scope() {
        let _serial = queue_test_guard();
        let _env = SessionEnvGuard::new();
        clear_queue();
        remember_scope("session:a");
        queue("session:a", "key-a", "mem://a", "A");
        std::env::set_var(rpi_plugin_sdk::SESSION_ID_ENV, "  ");
        assert!(take_pending_updates(current_scope().as_deref()).is_empty());

        std::env::remove_var(rpi_plugin_sdk::SESSION_ID_ENV);
        assert_eq!(current_scope().as_deref(), Some("session:a"));
    }

    /// Regression: the queue was global and `TurnEnd` drained all of it, so
    /// session A's resource contents could land in session B's context.
    #[test]
    fn queued_updates_are_scoped_to_the_session_that_produced_them() {
        let _serial = queue_test_guard();
        clear_queue();
        queue("session:a", "key-a", "mem://a", "A");
        queue("session:b", "key-b", "mem://b", "B");

        // The host is serving B: only B's update may reach the model.
        let served = take_pending_updates(Some("session:b"));
        assert_eq!(served.len(), 1, "got {served:?}");
        assert_eq!(served[0].text, "B");
        // A's update was dropped, not deferred into the next drain.
        assert!(take_pending_updates(Some("session:a")).is_empty());
        clear_queue();
    }

    /// A host with no session id (older, or an ephemeral session) still gets
    /// everything rather than nothing.
    #[test]
    fn unscoped_host_still_receives_updates() {
        let _serial = queue_test_guard();
        clear_queue();
        queue("library", "key-l", "mem://l", "L");
        let served = take_pending_updates(None);
        assert_eq!(served.len(), 1);
        clear_queue();
    }

    /// Closing a connection (or unsubscribing one URI) drops exactly its queued
    /// updates, and leaves other connections' alone.
    #[test]
    fn close_and_unsubscribe_drop_only_their_own_updates() {
        let _serial = queue_test_guard();
        clear_queue();
        queue("session:a", "key-a", "mem://a", "A");
        queue("session:b", "key-b", "mem://b", "B");

        // Unsubscribing one URI drops exactly that update.
        clear_pending_for_uri("key-a", "mem://a");
        let remaining = take_pending_updates(None);
        assert_eq!(remaining.len(), 1, "got {remaining:?}");
        assert_eq!(remaining[0].text, "B");

        // Closing one connection drops all of its updates, not the other's.
        queue("session:a", "key-a", "mem://other", "A2");
        queue("session:b", "key-b", "mem://b", "B2");
        clear_pending_for_key("key-a");
        let remaining = take_pending_updates(None);
        assert_eq!(remaining.len(), 1, "got {remaining:?}");
        assert_eq!(remaining[0].text, "B2");
        clear_queue();
    }

    /// The scope stored with an update is the one embedded in its connection key
    /// (`[scope, identity, name]`), which is what the filter above compares.
    #[test]
    fn scope_is_read_back_out_of_the_connection_key() {
        let key = serde_json::to_string(&("session:abc", "/tmp/mcp.json", "fake")).unwrap();
        assert_eq!(scope_of_key(&key), "session:abc");
        assert_eq!(scope_of_key("not json"), "");
    }
}
