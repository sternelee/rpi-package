//! Test support: an isolated mesh harness.
//!
//! Integration tests need real peers but must not touch the developer's state
//! root, must not collide with each other, and must stay inside the
//! `SUN_LEN` limit on Unix-domain socket paths. This module provides exactly
//! that: a short-lived temp root per harness, joined peers, and small helpers
//! for the common assertions.
//!
//! It is compiled into the crate (not a `#[cfg(test)]` module) so the
//! integration tests in `tests/` can use it; it contains no production logic.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::envelope::Envelope;
use crate::mesh_node::{JoinError, MeshNode};
use crate::peer::{SendOutcome, SessionPeer};

/// Serializes harness directory names so two tests never share a socket path.
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// An isolated mesh rooted in a private temp directory.
pub struct MeshHarness {
    root: PathBuf,
    sock: String,
    audit: Option<PathBuf>,
}

impl MeshHarness {
    /// Creates a harness with its own broker endpoint and audit log.
    pub fn new(tag: &str) -> Self {
        // A Unix-domain socket path is capped at `SUN_LEN` (~104 bytes), and
        // the temp dir already consumes a chunk of it: keep every component
        // short rather than descriptive.
        let tag: String = tag
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .take(6)
            .collect();
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "rpirh-{}-{unique}-{tag}",
            std::process::id() % 100_000
        ));
        std::fs::create_dir_all(&dir).expect("create harness dir");
        let sock = dir.join("b.sock").to_string_lossy().into_owned();
        Self {
            audit: Some(dir.join("audit.jsonl")),
            root: dir,
            sock,
        }
    }

    /// The audit log the broker writes routed envelopes to.
    pub fn audit_path(&self) -> std::path::PathBuf {
        self.audit
            .clone()
            .expect("the harness always configures an audit log")
    }

    /// Joins a peer to this harness's session.
    pub fn join(&self, cwd: &str, name: &str) -> Peer {
        self.try_join(cwd, name).expect("join succeeds")
    }

    /// Attempts a join, surfacing the refusal for lock-contention tests.
    pub fn try_join(&self, cwd: &str, name: &str) -> Result<Peer, JoinError> {
        MeshNode::join_with_endpoint(&self.root, &self.sock, self.audit.clone(), cwd, name)
            .map(|node| Peer(std::sync::Arc::new(node)))
    }
}

impl Drop for MeshHarness {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.root).ok();
    }
}

/// Runs a `/remote-pi` subcommand exactly as the host's command dispatcher
/// would, returning the parsed command-output JSON.
///
/// Exposed so a test can cover the command path (which the non-interactive CLI
/// never dispatches) without standing up a TUI.
pub fn run_command(args: &str) -> serde_json::Value {
    let raw = crate::tools::run_command(args);
    serde_json::from_str(&raw).unwrap_or(serde_json::Value::Null)
}

/// Joins through the **production** entrypoint, which derives the endpoint and
/// lock paths from the state root (`RPI_REMOTE_HOME` / `RPI_CODING_AGENT_DIR`).
///
/// Separate from [`MeshHarness`] so tests can exercise the real path derivation
/// without the harness's explicit endpoints.
pub fn join_default(cwd: &str, name: &str) -> Result<Peer, JoinError> {
    MeshNode::join(cwd, name).map(|node| Peer(std::sync::Arc::new(node)))
}

/// A joined peer plus the assertions integration tests need.
///
/// Cloning shares the same underlying session (the peer handle is an `Arc`), so
/// a responder thread and the test body can both hold one.
#[derive(Clone)]
pub struct Peer(std::sync::Arc<MeshNode>);

/// A readable mirror of [`SendOutcome`] for assertion messages.
#[derive(Debug)]
pub enum PeerOutcome {
    AckReceived { target: String },
    AckDenied { reason: Option<String> },
    AckTimeout,
    Sent,
}

impl Peer {
    /// The peer's canonical routing address.
    pub fn address(&self) -> String {
        self.0.address()
    }

    /// The peer's name.
    pub fn name(&self) -> String {
        self.0.peer().name().to_owned()
    }

    /// Sends with a bounded ACK wait.
    pub fn send_with_ack(
        &self,
        to: &str,
        body: serde_json::Value,
        re: Option<String>,
    ) -> PeerOutcome {
        match self
            .0
            .send_with_ack(to, body, re, Duration::from_millis(500), None)
        {
            SendOutcome::Received { target } => PeerOutcome::AckReceived { target },
            SendOutcome::Denied { reason, .. } => PeerOutcome::AckDenied { reason },
            SendOutcome::Timeout => PeerOutcome::AckTimeout,
            SendOutcome::Sent => PeerOutcome::Sent,
        }
    }

    /// Fire-and-forget broadcast.
    pub fn broadcast(&self, body: serde_json::Value) {
        self.0
            .send(crate::envelope::BROADCAST, body, None)
            .expect("broadcast");
    }

    /// Current peer addresses, newest broker state.
    pub fn list_peers(&self) -> Vec<String> {
        self.0
            .list_peers(Duration::from_millis(2_000))
            .expect("list_peers")
    }

    /// Waits for one non-control inbound envelope.
    pub fn drain_one(&self, timeout: Duration) -> Option<serde_json::Value> {
        let deadline = Instant::now() + timeout;
        loop {
            for envelope in self.0.drain_messages() {
                if is_control(&envelope) {
                    continue;
                }
                return serde_json::to_value(&envelope).ok();
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Sends a request and waits for the correlated reply.
    pub fn request(
        &self,
        to: &str,
        body: serde_json::Value,
        timeout: Duration,
    ) -> Result<serde_json::Value, String> {
        let reply = self
            .0
            .request(to, body, timeout)
            .map_err(|error| error.to_string())?;
        serde_json::to_value(&reply).map_err(|error| error.to_string())
    }

    /// The endpoint the session's `(cwd, name)` lock is held on.
    pub fn lock_path(&self) -> &std::path::Path {
        self.0.lock_endpoint()
    }

    /// Whether this peer won the election and hosts the broker.
    pub fn is_leader(&self) -> bool {
        self.0.is_leader()
    }

    /// The raw peer handle, for lifecycle operations a test drives directly.
    pub fn session_peer(&self) -> &std::sync::Arc<SessionPeer> {
        self.0.peer()
    }

    /// Reads the harness audit log, one parsed JSON record per line.
    pub fn audit_records(audit: &std::path::Path) -> Vec<serde_json::Value> {
        let Ok(raw) = std::fs::read_to_string(audit) else {
            return Vec::new();
        };
        raw.lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect()
    }
}

/// Whether an envelope is harness/control chatter rather than a message.
fn is_control(envelope: &Envelope) -> bool {
    envelope.from == crate::envelope::BROKER_NAME
}
