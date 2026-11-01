//! `SessionPeer` — the symmetric view of a participant in the local mesh.
//!
//! Callers never need to know whether this process hosts the broker or merely
//! connects to it: `send`, `send_with_ack`, `request` and `leave` behave
//! identically either way.
//!
//! Inbound lines land in **one shared queue** consumed by two kinds of reader:
//! a `request`/`send_with_ack` waiting for its own correlation id, and
//! `drain_messages` handing everything else to the session (for delivery to the
//! model). A waiting requester scans the queue and takes only the item it is
//! waiting for, leaving the rest in order — so a control reply and a subscriber
//! notification can never steal from each other, and nothing is ever lost by a
//! `recv` that turned out to be the wrong line.

use std::collections::VecDeque;
use std::io::BufReader;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::election::{self, Election};
use crate::envelope::{self, parse, serialize, AckStatus, Envelope};
use crate::ipc::{self, Stream};

/// Default timeout for a synchronous request/reply.
pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
/// Default timeout for a delivery ACK.
pub const ACK_TIMEOUT_MS: u64 = 5_000;

/// Shared inbound queue with a wakeup signal.
///
/// Shared by `Arc` so the reader thread can push without borrowing the peer,
/// and replaced wholesale on every (re)join so lines from a dead connection
/// cannot leak into the new session.
#[derive(Default)]
struct Inbox {
    items: VecDeque<Envelope>,
    closed: bool,
}

/// The mutex + condvar pair guarding an [`Inbox`].
struct InboxShared {
    state: Mutex<Inbox>,
    signal: Condvar,
}

impl Default for InboxShared {
    fn default() -> Self {
        Self {
            state: Mutex::new(Inbox::default()),
            signal: Condvar::new(),
        }
    }
}

impl InboxShared {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inbox> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn notify(&self) {
        self.signal.notify_all();
    }
}

/// What a waiter decides about each queued item while scanning.
enum Scan {
    /// Take this item as the awaited one.
    Take,
    /// Neither ours nor useful to anyone: drop it.
    Drop,
    /// Leave it in the queue for a different waiter or a subscriber.
    Skip,
}

/// Errors surfaced by [`SessionPeer`].
#[derive(Debug, thiserror::Error)]
pub enum PeerError {
    #[error("session peer not connected")]
    NotConnected,
    #[error("request to {target} timed out after {timeout_ms}ms")]
    RequestTimeout { target: String, timeout_ms: u64 },
    #[error("transport_error: {reason}")]
    Transport { reason: String },
    #[error("{0}")]
    Io(String),
}

/// The outcome of a unicast send that awaited the broker's delivery ACK.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SendOutcome {
    Received {
        target: String,
    },
    Denied {
        target: Option<String>,
        reason: Option<String>,
    },
    Timeout,
    /// Broadcast/multicast: no ACK semantics.
    Sent,
}

/// A participant in the local mesh.
pub struct SessionPeer {
    name: String,
    address: String,
    cwd: String,
    sock_path: String,
    audit: Option<std::path::PathBuf>,
    /// The live connection, or `None` while disconnected (before the first join,
    /// after leave, or while reconnecting). Shared with the reader thread's
    /// rejoin driver so a background rejoin can publish the new connection.
    connection: Arc<Mutex<Option<Arc<Mutex<Stream>>>>>,
    /// The broker's stop flag while this peer hosts it.
    ///
    /// Shared with the rejoin driver, so both the first join and a rejoin that
    /// won the election publish through one slot and `is_leader` reads one
    /// truth. Stopped when the peer ends, so an exited leader does not leave a
    /// broker running behind it.
    leadership: Arc<Mutex<Option<crate::broker::Shutdown>>>,
    inbox: Arc<InboxShared>,
}

impl SessionPeer {
    /// Creates a peer bound to `sock_path` without joining yet.
    pub fn new(
        sock_path: impl Into<String>,
        name: impl Into<String>,
        cwd: impl Into<String>,
        audit: Option<std::path::PathBuf>,
    ) -> Self {
        let name = name.into();
        Self {
            address: name.clone(),
            name,
            cwd: cwd.into(),
            sock_path: sock_path.into(),
            audit,
            connection: Arc::new(Mutex::new(None)),
            leadership: Arc::new(Mutex::new(None)),
            inbox: Arc::new(InboxShared::default()),
        }
    }

    /// Joins the session (or leads it), resolving the assigned address.
    pub fn start(&mut self) -> Result<String, PeerError> {
        self.join_or_lead()
    }

    /// The clean leaf name assigned by the broker.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The canonical routing address assigned by the broker.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Whether this peer won the election and is hosting the broker.
    pub fn is_leader(&self) -> bool {
        self.leadership
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some()
    }

    /// Fire-and-forget send. `re` correlates this message as a reply to an
    /// earlier envelope's id.
    pub fn send(
        &self,
        to: impl Into<Value>,
        body: Value,
        re: Option<String>,
    ) -> Result<(), PeerError> {
        let envelope = Envelope::new(self.address.clone(), to, body, re);
        self.write_line(&serialize(&envelope))
    }

    /// Unicast send that awaits the broker's delivery ACK.
    ///
    /// `cancelled` is polled while waiting, never to re-send: the envelope goes
    /// out exactly once so the peer cannot receive duplicates under fresh ids.
    pub fn send_with_ack(
        &self,
        to: &str,
        body: Value,
        re: Option<String>,
        timeout: Duration,
        cancelled: Option<&AtomicBool>,
    ) -> SendOutcome {
        if to == envelope::BROADCAST {
            return match self.send(to, body, re) {
                Ok(()) => SendOutcome::Sent,
                Err(_) => SendOutcome::Timeout,
            };
        }
        let envelope = Envelope::new(self.address.clone(), to.to_owned(), body, re);
        let id = envelope.id.clone();
        if self.write_line(&serialize(&envelope)).is_err() {
            return SendOutcome::Timeout;
        }
        let deadline = Instant::now() + timeout;
        loop {
            if cancelled.is_some_and(|flag| flag.load(Ordering::SeqCst)) {
                return SendOutcome::Timeout;
            }
            let Some(incoming) = self.take_matching(deadline, |candidate| {
                if candidate.from == envelope::BROKER_NAME
                    && candidate.re.as_deref() == Some(id.as_str())
                {
                    Scan::Take
                } else {
                    Scan::Skip
                }
            }) else {
                return SendOutcome::Timeout;
            };
            if let Some(ack) = envelope::as_ack(&incoming.body) {
                return match ack.status {
                    // `busy` is only produced by an out-of-date broker leader
                    // that dropped the message; report it honestly as a refusal.
                    AckStatus::Received => SendOutcome::Received { target: ack.target },
                    AckStatus::Denied => SendOutcome::Denied {
                        target: Some(ack.target),
                        reason: None,
                    },
                    AckStatus::Busy => SendOutcome::Denied {
                        target: Some(ack.target),
                        reason: Some("busy (out-of-date broker leader)".into()),
                    },
                };
            }
            if let Some(error) = envelope::as_transport_error(&incoming.body) {
                return match error.reason.as_str() {
                    "offline" => SendOutcome::Timeout,
                    reason => SendOutcome::Denied {
                        target: None,
                        reason: Some(reason.to_owned()),
                    },
                };
            }
            // An unrelated control line: `Scan::Skip` already left it in the
            // queue for the session, and the next iteration re-scans it.
        }
    }

    /// Sends and waits for the first inbound envelope whose `re` matches.
    pub fn request(&self, to: &str, body: Value, timeout: Duration) -> Result<Envelope, PeerError> {
        let envelope = Envelope::new(self.address.clone(), to.to_owned(), body, None);
        let id = envelope.id.clone();
        self.write_line(&serialize(&envelope))?;
        let deadline = Instant::now() + timeout;
        // A broker ACK carries the SAME `re` as the request it acknowledges, so
        // `re == id` alone would match the ACK before the reply. The ACK is
        // control traffic for the delivery half of this send, not an answer:
        // drop it and keep waiting for a real reply.
        let found = self.take_matching(deadline, |candidate| {
            if candidate.re.as_deref() != Some(id.as_str()) {
                return Scan::Skip;
            }
            if candidate.from == envelope::BROKER_NAME {
                if envelope::as_transport_error(&candidate.body).is_some() {
                    return Scan::Take;
                }
                if envelope::as_ack(&candidate.body).is_some() {
                    return Scan::Drop;
                }
            }
            Scan::Take
        });
        let Some(incoming) = found else {
            return Err(PeerError::RequestTimeout {
                target: to.to_owned(),
                timeout_ms: timeout.as_millis() as u64,
            });
        };
        if let Some(error) = envelope::as_transport_error(&incoming.body) {
            return Err(PeerError::Transport {
                reason: error.reason,
            });
        }
        Ok(incoming)
    }

    /// Drains the inbound envelopes that no waiting requester consumed.
    ///
    /// Control replies (ACKs, request replies) are excluded: they are either
    /// taken by their waiter or, if the waiter already gave up, are not
    /// session content and must not reach the model.
    pub fn drain_messages(&self) -> Vec<Envelope> {
        let mut inbox = self.inbox.lock();
        let mut messages = Vec::new();
        while let Some(envelope) = inbox.items.pop_front() {
            // A late ACK for a send whose waiter already timed out is control
            // traffic, not session content: drop it instead of feeding it to
            // the model.
            if envelope.from == envelope::BROKER_NAME
                && (envelope::as_ack(&envelope.body).is_some()
                    || envelope::as_transport_error(&envelope.body).is_some())
            {
                continue;
            }
            messages.push(envelope);
        }
        messages
    }

    /// Releases the connection so the broker can announce `peer_left`.
    ///
    /// Releasing the `Arc` also drops the last reference to the read half, so
    /// the socket actually closes and the broker can announce `peer_left`
    /// instead of keeping a ghost in its roster.
    pub fn leave(&self) {
        *self.connection.lock().unwrap_or_else(|p| p.into_inner()) = None;
        self.close_inbox();
    }

    // ── internals ───────────────────────────────────────────────────────────

    /// Removes the first queued item the scanner decides to `Take`, waiting
    /// until `deadline`. `Drop` items are consumed and discarded; `Skip` items
    /// stay in the queue, in order.
    fn take_matching(
        &self,
        deadline: Instant,
        scan: impl Fn(&Envelope) -> Scan,
    ) -> Option<Envelope> {
        let mut inbox = self.inbox.lock();
        loop {
            let mut taken = None;
            let mut index = 0;
            while index < inbox.items.len() {
                let decision = scan(&inbox.items[index]);
                match decision {
                    Scan::Take => {
                        taken = inbox.items.remove(index);
                        break;
                    }
                    Scan::Drop => {
                        inbox.items.remove(index);
                    }
                    Scan::Skip => index += 1,
                }
            }
            if taken.is_some() {
                return taken;
            }
            if inbox.closed {
                return None;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return None;
            }
            // Wake on a push or on the deadline; either way the loop re-scans,
            // because the scan above ran against the queue as it was *before*
            // the wait, and an item that arrived during it must be found.
            inbox = self
                .inbox
                .signal
                .wait_timeout(inbox, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
    }

    fn close_inbox(&self) {
        {
            let mut inbox = self.inbox.lock();
            inbox.closed = true;
        }
        self.inbox.notify();
    }

    fn write_line(&self, line: &str) -> Result<(), PeerError> {
        let writer = {
            let guard = self.connection.lock().unwrap_or_else(|p| p.into_inner());
            guard.clone()
        }
        .ok_or(PeerError::NotConnected)?;
        let mut guard = writer.lock().unwrap();
        ipc::write_line(&mut *guard, line).map_err(|error| PeerError::Io(error.to_string()))
    }

    /// Runs the join/election race and wires the connection.
    fn join_or_lead(&mut self) -> Result<String, PeerError> {
        let outcome = election::join_or_lead(&self.sock_path)
            .map_err(|error| PeerError::Io(error.to_string()))?;
        let stream = match outcome {
            Election::Leader(listener) => {
                // Leading: host the broker, then connect to ourselves so the rest
                // of the peer logic sees one uniform connection. The stop flag is
                // shared with the broker, so the broker cannot outlive the
                // session that publishes it here.
                let (broker, stop) = crate::broker::Broker::start(self.audit.clone());
                let broker = Arc::new(broker);
                *self.leadership.lock().unwrap() = Some(stop);
                std::thread::Builder::new()
                    .name("rpi-remote-broker".into())
                    .spawn(move || broker.serve(&listener))
                    .ok();
                ipc::connect(&self.sock_path).map_err(|error| PeerError::Io(error.to_string()))?
            }
            Election::Follower(stream) => stream,
        };

        // One socket, split into a read half owned by the reader thread and a
        // write half shared with senders. Both halves end together, so the
        // broker sees EOF when this peer goes away.
        let read_half = stream
            .try_clone()
            .map_err(|error| PeerError::Io(error.to_string()))?;
        let writer = Arc::new(Mutex::new(stream));
        *self.connection.lock().unwrap() = Some(Arc::clone(&writer));
        // A (re)join starts a fresh queue: stale buffered lines from a dead
        // connection must not leak into the new session.
        *self.inbox.lock() = Inbox::default();

        let registration = serde_json::json!({
            "type": "register",
            "name": self.name,
            "cwd": self.cwd,
        });
        self.write_line(&(registration.to_string() + "\n"))?;

        let mut reader = BufReader::new(read_half);
        let mut buffer = String::new();
        // The first line MUST be the register_ack.
        let ack_line = ipc::read_line(&mut reader, &mut buffer)
            .map_err(|error| PeerError::Io(error.to_string()))?
            .ok_or_else(|| PeerError::Io("broker closed before register_ack".into()))?;
        let ack: Value = serde_json::from_str(&ack_line)
            .map_err(|error| PeerError::Io(format!("invalid register_ack: {error}")))?;
        self.name = ack
            .get("name_assigned")
            .and_then(Value::as_str)
            .unwrap_or(&self.name)
            .to_owned();
        self.address = ack
            .get("address_assigned")
            .and_then(Value::as_str)
            .unwrap_or(&self.name)
            .to_owned();

        // Reader thread: never resolves correlation itself — it only queues, so
        // a waiter and a subscriber observe the same ordered stream. It holds
        // the shared inbox and rejoin plan, not `&self`, so it cannot outlive the
        // peer.
        let reader_inbox = Arc::clone(&self.inbox);
        let driver = Arc::new(RejoinDriver {
            sock_path: self.sock_path.clone(),
            name: self.name.clone(),
            cwd: self.cwd.clone(),
            audit: self.audit.clone(),
            inbox: Arc::clone(&self.inbox),
            connection: Arc::clone(&self.connection),
            leadership: Arc::clone(&self.leadership),
        });
        let driver_thread = Arc::clone(&driver);
        std::thread::Builder::new()
            .name("rpi-remote-peer-reader".into())
            .spawn(move || {
                let mut reader = reader;
                let mut buffer = String::new();
                while let Ok(Some(line)) = ipc::read_line(&mut reader, &mut buffer) {
                    if line.trim().is_empty() {
                        continue;
                    }
                    if let Ok(envelope) = parse(line.trim()) {
                        let mut state = reader_inbox.lock();
                        state.items.push_back(envelope);
                        drop(state);
                        reader_inbox.notify();
                    }
                }
                // EOF or error: this connection is dead. Releasing the `Arc`
                // also closes the socket, so the broker can announce
                // `peer_left` instead of keeping a ghost in its roster (which
                // would make every send to a neighbour wait on an ACK that can
                // never arrive).
                drop(writer);
                // Hand the duty to the driver, which re-joins in the background
                // and reports `open` or `gone` through the shared inbox.
                driver_thread.on_disconnected();
            })
            .ok();

        Ok(self.address.clone())
    }
}

/// Re-joins the mesh after a connection drops.
///
/// Held by `Arc` so the reader thread can trigger a reconnect without borrowing
/// `SessionPeer`. All the state it needs is already shared: the endpoint, the
/// identity to register, and the slots it writes back into.
struct RejoinDriver {
    sock_path: String,
    name: String,
    cwd: String,
    audit: Option<std::path::PathBuf>,
    inbox: Arc<InboxShared>,
    connection: Arc<Mutex<Option<Arc<Mutex<Stream>>>>>,
    /// The session's leadership slot, so a rejoin that wins the election is
    /// observable through the same flag `is_leader` reads.
    leadership: Arc<Mutex<Option<crate::broker::Shutdown>>>,
}

/// How long a rejoin attempt waits before retrying.
const REJOIN_RETRY: Duration = Duration::from_millis(200);
/// How many endpoints to probe before giving up and reporting `gone`.
const REJOIN_ATTEMPTS: usize = 40;

impl RejoinDriver {
    /// Called by the reader thread when the connection is gone.
    ///
    /// Runs the rejoin **on its own thread** (the reader's) so a caller blocked
    /// on `send_with_ack` is not the thing that stalls: waiters are released
    /// immediately, and the rejoin happens behind them.
    fn on_disconnected(&self) {
        // Signal waiters that the current connection is gone, so a blocked
        // `request`/`send_with_ack` returns now instead of burning its whole
        // deadline against a broker that is no longer there.
        {
            let mut inbox = self.inbox.lock();
            inbox.closed = true;
        }
        self.inbox.notify();

        // Re-run the same election the first join ran. Trying to *follow* first
        // keeps the common case (another live peer re-led) cheap, but the bind
        // attempt matters just as much: with this process alone left, nobody
        // else can release the endpoint, so the survivor must lead itself.
        for _ in 0..REJOIN_ATTEMPTS {
            std::thread::sleep(REJOIN_RETRY);
            let Ok(outcome) = election::join_or_lead(&self.sock_path) else {
                continue;
            };
            let read_half = match outcome {
                Election::Follower(stream) => match self.readopt(stream) {
                    Ok(read_half) => read_half,
                    Err(_) => continue,
                },
                Election::Leader(listener) => match self.lead(listener) {
                    Ok(read_half) => read_half,
                    Err(_) => continue,
                },
            };
            if self.spawn_reader(read_half).is_ok() {
                return;
            }
        }
        // The endpoint never came back (the last leader exited for good). The
        // inbox stays closed, so senders report a timeout rather than retrying
        // a mesh that no longer exists.
    }

    /// Hosts a fresh broker on a won endpoint and connects to it.
    ///
    /// The rejoin path mirrors the first join's leader branch, so a survivor that
    /// wins the endpoint behaves like a fresh process.
    ///
    /// The stop handle is moved into the reader thread's scope, so the broker
    /// outlives this call but not the reader: when the connection drops again,
    /// the thread ends and the handle is dropped, stopping the broker.
    fn lead(&self, listener: crate::ipc::Listener) -> Result<Stream, PeerError> {
        let (broker, stop) = crate::broker::Broker::start(self.audit.clone());
        // Publish before connecting so a failure to register cannot leave the
        // session claiming a broker it is not actually serving.
        *self.leadership.lock().unwrap() = Some(stop);
        let broker = Arc::new(broker);
        std::thread::Builder::new()
            .name("rpi-remote-broker".into())
            .spawn(move || broker.serve(&listener))
            .map_err(|error| PeerError::Io(error.to_string()))?;
        // Connecting to our own broker is not enough: the peer must also
        // *register*, exactly as the first join and every follower do, or it
        // appears on no roster and every request to `broker` times out.
        let stream =
            ipc::connect(&self.sock_path).map_err(|error| PeerError::Io(error.to_string()))?;
        self.readopt(stream)
    }

    /// Registers on a fresh connection and returns its read half.
    ///
    /// The caller must start a reader thread on the returned half; a connection
    /// nobody drains looks identical to a hung one.
    fn readopt(&self, stream: Stream) -> Result<Stream, PeerError> {
        let read_half = stream
            .try_clone()
            .map_err(|error| PeerError::Io(error.to_string()))?;
        let writer = Arc::new(Mutex::new(stream));
        *self.connection.lock().unwrap() = Some(Arc::clone(&writer));

        let registration = serde_json::json!({
            "type": "register",
            "name": self.name,
            "cwd": self.cwd,
        });
        ipc::write_line(
            &mut *writer.lock().unwrap(),
            &(registration.to_string() + "\n"),
        )
        .map_err(|error| PeerError::Io(error.to_string()))?;

        let mut reader = BufReader::new(read_half);
        let mut buffer = String::new();
        let ack_line = ipc::read_line(&mut reader, &mut buffer)
            .map_err(|error| PeerError::Io(error.to_string()))?
            .ok_or_else(|| PeerError::Io("broker closed before register_ack".into()))?;
        let ack: Value = serde_json::from_str(&ack_line)
            .map_err(|error| PeerError::Io(format!("invalid register_ack: {error}")))?;
        let name = ack
            .get("name_assigned")
            .and_then(Value::as_str)
            .unwrap_or(&self.name)
            .to_owned();
        let address = ack
            .get("address_assigned")
            .and_then(Value::as_str)
            .unwrap_or(&self.name)
            .to_owned();
        // Preserve the assigned identity for the next rejoin even though the
        // session's own copy stays as it was.
        let _ = (name, address, self.audit.clone());

        // The new connection delivers from a clean queue.
        *self.inbox.lock() = Inbox::default();
        Ok(reader.into_inner())
    }

    /// Starts the reader thread that drains one connection.
    ///
    /// Shared with the initial join as well as a rejoin, so both paths behave
    /// identically: read lines into the inbox, then on EOF hand the duty back to
    /// the driver.
    fn spawn_reader(&self, read_half: Stream) -> Result<(), PeerError> {
        let driver = Arc::new(self.clone());
        let inbox = Arc::clone(&self.inbox);
        std::thread::Builder::new()
            .name("rpi-remote-peer-reader".into())
            .spawn(move || {
                let mut reader = BufReader::new(read_half);
                let mut buffer = String::new();
                while let Ok(Some(line)) = ipc::read_line(&mut reader, &mut buffer) {
                    if line.trim().is_empty() {
                        continue;
                    }
                    if let Ok(envelope) = parse(line.trim()) {
                        let mut state = inbox.lock();
                        state.items.push_back(envelope);
                        drop(state);
                        inbox.notify();
                    }
                }
                driver.on_disconnected();
            })
            .map_err(|error| PeerError::Io(error.to_string()))?;
        Ok(())
    }
}

/// The driver is `Arc`-shared so a reader thread can own it for the life of the
/// connection; cloning it shares the same slots.
impl Clone for RejoinDriver {
    fn clone(&self) -> Self {
        Self {
            sock_path: self.sock_path.clone(),
            name: self.name.clone(),
            cwd: self.cwd.clone(),
            audit: self.audit.clone(),
            inbox: Arc::clone(&self.inbox),
            connection: Arc::clone(&self.connection),
            leadership: Arc::clone(&self.leadership),
        }
    }
}

impl Drop for SessionPeer {
    fn drop(&mut self) {
        // Dropping the connection closes the socket, so the broker can announce
        // `peer_left` rather than keep a ghost in its roster.
        self.leave();
        // Stopping the broker explicitly is what releases the endpoint: a
        // blocking `accept()` does not return merely because the listener's
        // owners were dropped, so without this the accept loop (and the
        // endpoint) outlive the leader.
        if let Some(stop) = self
            .leadership
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
        {
            stop.stop();
        }
    }
}
