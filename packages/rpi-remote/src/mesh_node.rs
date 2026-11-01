//! `MeshNode` — the composition point joining the local `SessionPeer` to the
//! per-`(cwd, name)` lock and the session directories.
//!
//! Callers (the tools, the slash command, the event bridge) talk to a
//! `MeshNode` rather than to `SessionPeer` directly, so lifecycle rules live in
//! one place: take the lock, join, expose the peer, and release everything on
//! drop.

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use crate::cwd_lock::{self, LockGuard, LockResult};
use crate::envelope::Envelope;
use crate::peer::{PeerError, SendOutcome, SessionPeer};

/// A joined participant in the local mesh.
pub struct MeshNode {
    peer: Arc<SessionPeer>,
    /// Held for the session's lifetime; dropping the node releases the lock.
    _lock: LockGuard,
    session_name: String,
    locked_cwd: String,
}

/// Why a join could not complete.
#[derive(Debug, thiserror::Error)]
pub enum JoinError {
    #[error("another agent already holds the lock for this folder and name ({path})")]
    LockHeld { path: String },
    #[error("mesh io error: {0}")]
    Io(String),
}

impl MeshNode {
    /// Joins the local mesh.
    ///
    /// The `(cwd, name)` lock is taken **before** the join so two agents cannot
    /// both claim one identity; a refusal is reported instead of silently
    /// joining under a `#N` name the user never chose.
    pub fn join(cwd: &str, name: &str) -> Result<Self, JoinError> {
        crate::state::ensure_dirs().map_err(|error| JoinError::Io(error.to_string()))?;
        crate::state::deploy_bundled_skill();
        let lock = match cwd_lock::acquire(cwd, name) {
            LockResult::Acquired(guard) => guard,
            LockResult::Refused { path } => return Err(JoinError::LockHeld { path }),
        };
        let session_name = crate::state::LOCAL_SESSION_NAME.to_owned();
        let sock = crate::state::session_sock(&session_name);
        let audit = Some(crate::state::session_audit(&session_name));
        Self::with_lock(lock, session_name, sock, audit, cwd, name)
    }

    /// Joins a mesh rooted at an explicit endpoint and lock directory.
    ///
    /// Exists so tests can run real brokers without touching the process-wide
    /// state root (and without racing each other on one socket path).
    pub fn join_with_endpoint(
        root: &std::path::Path,
        sock: &str,
        audit: Option<std::path::PathBuf>,
        cwd: &str,
        name: &str,
    ) -> Result<Self, JoinError> {
        let cwd = crate::state::canonical_cwd(cwd);
        let lock_path = cwd_lock::lock_path_in(root, &cwd, name);
        let lock = match cwd_lock::acquire_at(&lock_path) {
            LockResult::Acquired(guard) => guard,
            LockResult::Refused { path } => return Err(JoinError::LockHeld { path }),
        };
        Self::with_lock(lock, "local".to_owned(), sock.to_owned(), audit, &cwd, name)
    }

    fn with_lock(
        lock: LockGuard,
        session_name: String,
        sock: String,
        audit: Option<std::path::PathBuf>,
        cwd: &str,
        name: &str,
    ) -> Result<Self, JoinError> {
        let mut peer = SessionPeer::new(sock, name, cwd.to_owned(), audit);
        peer.start()
            .map_err(|error| JoinError::Io(error.to_string()))?;
        Ok(Self {
            peer: Arc::new(peer),
            _lock: lock,
            session_name,
            locked_cwd: cwd.to_owned(),
        })
    }

    /// The underlying peer handle.
    pub fn peer(&self) -> &Arc<SessionPeer> {
        &self.peer
    }

    /// The fixed local session name.
    pub fn session_name(&self) -> &str {
        &self.session_name
    }

    /// The canonical cwd this session holds the lock for.
    pub fn locked_cwd(&self) -> String {
        self.locked_cwd.clone()
    }

    /// The assigned routing address.
    pub fn address(&self) -> String {
        self.peer.address().to_owned()
    }

    /// Fire-and-forget send.
    pub fn send(
        &self,
        to: impl Into<Value>,
        body: Value,
        re: Option<String>,
    ) -> Result<(), PeerError> {
        self.peer.send(to, body, re)
    }

    /// Unicast send awaiting the broker's delivery ACK.
    ///
    /// `cancelled` is polled while waiting; it never causes a re-send.
    pub fn send_with_ack(
        &self,
        to: &str,
        body: Value,
        re: Option<String>,
        timeout: Duration,
        cancelled: Option<&std::sync::atomic::AtomicBool>,
    ) -> SendOutcome {
        self.peer.send_with_ack(to, body, re, timeout, cancelled)
    }

    /// Synchronous request/reply.
    pub fn request(&self, to: &str, body: Value, timeout: Duration) -> Result<Envelope, PeerError> {
        self.peer.request(to, body, timeout)
    }

    /// Current peer addresses, queried from the broker.
    pub fn list_peers(&self, timeout: Duration) -> Result<Vec<String>, PeerError> {
        let reply = self.request(
            crate::envelope::BROKER_NAME,
            serde_json::json!({ "type": "list_peers" }),
            timeout,
        )?;
        Ok(reply
            .body
            .get("peers")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Drains inbound envelopes that are not control traffic.
    pub fn drain_messages(&self) -> Vec<Envelope> {
        self.peer.drain_messages()
    }

    /// Whether this session is hosting the broker rather than connecting to one.
    pub fn is_leader(&self) -> bool {
        self.peer.is_leader()
    }

    /// The endpoint the `(cwd, name)` lock is held on.
    pub fn lock_endpoint(&self) -> &std::path::Path {
        std::path::Path::new(self._lock.path())
    }
}

/// Renders a delivery outcome for the model, reporting the honest result
/// instead of a success-shaped message when delivery failed.
pub fn describe_ack(outcome: &SendOutcome, to: &str, re: Option<&str>) -> (bool, String) {
    let suffix = match re {
        Some(id) => format!(" (re={id})"),
        None => String::new(),
    };
    match outcome {
        SendOutcome::Received { target } => (
            true,
            format!(
                "Delivered to \"{target}\"{suffix} — the peer will process it in an upcoming turn."
            ),
        ),
        SendOutcome::Sent => (true, format!("Broadcast sent{suffix}.")),
        SendOutcome::Denied { reason, .. } => {
            let reason = reason.as_deref().unwrap_or("denied");
            (
                false,
                format!("\"{to}\" refused the message{suffix} ({reason}). Do not retry."),
            )
        }
        SendOutcome::Timeout => (
            false,
            format!(
                "No delivery ACK from \"{to}\" within {}ms{suffix} — the peer is offline or the transport failed.",
                crate::peer::ACK_TIMEOUT_MS
            ),
        ),
    }
}
