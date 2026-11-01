//! The session broker: one process hosts it (the session *leader*), every other
//! process connects as a *follower*.
//!
//! Responsibilities:
//!   - assign each connection a canonical address `<cwd>@<name>`, appending
//!     `#N` on a same-address collision;
//!   - route envelopes to their `to` targets (single, list, or broadcast);
//!   - ACK delivered unicast envelopes so a sender can tell delivery from
//!     silence;
//!   - answer broker-addressed queries (`list_peers`);
//!   - broadcast `peer_joined` / `peer_left` so peers can refresh their view;
//!   - append every routed envelope to an audit log.

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufReader, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;

use crate::envelope::{
    self, ack_body, compose_address, parse, serialize, AckStatus, Envelope, BROKER_NAME,
};
use crate::ipc::{self, Listener, Stream};

/// One connected peer.
struct Peer {
    /// Clean leaf name (may carry a `#N` collision suffix).
    name: String,
    cwd: String,
    /// Canonical routing address — the map key and the value forced onto
    /// `envelope.from`.
    address: String,
    stream: Arc<Mutex<Stream>>,
}

impl Peer {
    fn info(&self) -> Value {
        serde_json::json!({ "cwd": self.cwd, "name": self.name, "address": self.address })
    }
}

/// Shared broker state.
struct Inner {
    peers: Mutex<HashMap<String, Peer>>,
    audit: Option<PathBuf>,
    /// Set when the broker must stop. An `AtomicBool` (rather than a channel
    /// receiver) keeps `Arc<Inner>` shareable across threads.
    stop: Arc<std::sync::atomic::AtomicBool>,
}

/// A running broker.
///
/// The broker lives exactly as long as the leader that hosts it. The leader
/// keeps the [`Listener`] for as long as its session runs, so when the leader's
/// peer is dropped the listener is closed, `serve` returns, and the endpoint is
/// released for the next process to win the election. Without that, the endpoint
/// stays bound behind the exited leader, nobody else can lead, and every
/// follower keeps a connection to a broker that serves no one.
pub struct Broker {
    inner: Arc<Inner>,
}

impl Broker {
    /// Creates a broker plus the handle that stops it.
    ///
    /// The leader keeps the [`Shutdown`] for as long as it hosts the mesh, so
    /// the broker lives exactly as long as its owner. Dropping the handle stops
    /// the accept loop.
    pub fn start(audit: Option<PathBuf>) -> (Self, Shutdown) {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let broker = Self {
            inner: Arc::new(Inner {
                peers: Mutex::new(HashMap::new()),
                audit,
                stop: Arc::clone(&stop),
            }),
        };
        (broker, Shutdown { stop })
    }

    /// Whether the owner asked the broker to stop.
    ///
    /// Non-blocking: a queued signal, a closed channel, or a drained one all
    /// mean "stop".
    fn stopped(&self) -> bool {
        self.inner.stop.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Accepts connections until asked to stop or `accept` fails.
    ///
    /// `listener` is put into non-blocking mode and polled with a short sleep,
    /// because a blocking `accept()` does **not** return when the listener's
    /// owners are dropped — that is the leak this loop exists to avoid.
    pub fn serve(self: &Arc<Self>, listener: &Listener) {
        if listener.set_nonblocking(true).is_err() {
            return;
        }
        while !self.stopped() {
            match listener.accept() {
                Ok(stream) => {
                    // The connection is used on a blocking thread, so restore
                    // its default blocking mode before handing it over.
                    let _ = stream.set_nonblocking(false);
                    let broker = Arc::clone(self);
                    if std::thread::Builder::new()
                        .name("rpi-remote-broker-conn".into())
                        .spawn(move || broker.handle_connection(stream))
                        .is_err()
                    {
                        break;
                    }
                }
                Err(ref error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    // Nothing pending: check the stop signal and back off rather
                    // than spinning.
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(_) => break,
            }
        }
    }
}

/// A view of the broker's stop flag.
///
/// Clones share **one** flag, so the flag — not ownership of a handle — decides
/// when the broker stops. That matters because the broker thread and the session
/// that hosts it each need a handle, and neither should stop a broker the other
/// still wants alive: the session stops it explicitly, and the broker thread's
/// copy is inert.
#[derive(Clone)]
pub struct Shutdown {
    stop: Arc<std::sync::atomic::AtomicBool>,
}

impl Shutdown {
    /// Stops the broker the handle belongs to.
    ///
    /// Idempotent; affects every clone of the handle.
    pub fn stop(&self) {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Broker {
    fn handle_connection(self: Arc<Self>, stream: Stream) {
        let write_stream = match stream.try_clone() {
            Ok(clone) => Arc::new(Mutex::new(clone)),
            Err(_) => return,
        };
        let mut reader = BufReader::new(stream);
        let mut buffer = String::new();

        // Handshake: the first line MUST be a `register` request. A
        // `list_peers` probe from an unregistered connection is answered
        // without assigning an address or announcing a peer.
        let registration = 'handshake: loop {
            match ipc::read_line(&mut reader, &mut buffer) {
                Ok(Some(line)) => {
                    if line.trim().is_empty() {
                        continue;
                    }
                    let parsed = match serde_json::from_str::<Value>(line.trim()) {
                        Ok(value) => value,
                        Err(_) => return,
                    };
                    if parsed.get("type").and_then(Value::as_str) == Some("list_peers") {
                        let reply = serde_json::json!({
                            "type": "list_peers_reply",
                            "peers": self.peer_addresses(),
                            "peers_detailed": self.peer_infos(),
                        });
                        let mut guard = write_stream.lock().unwrap();
                        let _ = ipc::write_line(&mut *guard, &(reply.to_string() + "\n"));
                        continue 'handshake;
                    }
                    break 'handshake parsed;
                }
                _ => return,
            }
        };

        let cwd = registration
            .get("cwd")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let requested = envelope::sanitize_mesh_name(
            registration
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("agent"),
        );

        let address = self.register(&requested, &cwd, &write_stream);
        let name = {
            let peers = self.inner.peers.lock().unwrap();
            peers
                .get(&address)
                .map(|peer| peer.name.clone())
                .unwrap_or_else(|| requested.clone())
        };

        let ack = serde_json::json!({
            "type": "register_ack",
            "name_assigned": name,
            "address_assigned": address,
        });
        {
            let mut guard = write_stream.lock().unwrap();
            if ipc::write_line(&mut *guard, &(ack.to_string() + "\n")).is_err() {
                self.unregister(&address);
                return;
            }
        }
        self.broadcast_system(
            serde_json::json!({ "type": "peer_joined", "name": address, "address": address }),
            &address,
        );

        // Routing loop: ends when the peer disconnects or the stream errors.
        while let Ok(Some(line)) = ipc::read_line(&mut reader, &mut buffer) {
            if line.trim().is_empty() {
                continue;
            }
            // A malformed line is dropped rather than routed, so one confused
            // peer cannot poison the mesh.
            let Ok(envelope) = parse(line.trim()) else {
                continue;
            };
            self.route(&address, envelope);
        }
        self.unregister(&address);
    }

    /// Claims an address for a new peer, returning the canonical address.
    ///
    /// A registration for an address already held replaces the incumbent: the
    /// same `(cwd, name)` arriving again is the logical agent reincarnating
    /// (a restart or a switch-session), and keeping the name beats handing the
    /// newcomer a mute `#N` identity it never chose.
    fn register(&self, requested: &str, cwd: &str, stream: &Arc<Mutex<Stream>>) -> String {
        let address = compose_address(cwd, requested);
        let mut peers = self.inner.peers.lock().unwrap();
        peers.insert(
            address.clone(),
            Peer {
                name: requested.to_owned(),
                cwd: cwd.to_owned(),
                address: address.clone(),
                stream: Arc::clone(stream),
            },
        );
        address
    }

    fn unregister(&self, address: &str) {
        let removed = self.inner.peers.lock().unwrap().remove(address).is_some();
        if removed {
            self.broadcast_system(
                serde_json::json!({ "type": "peer_left", "name": address, "address": address }),
                address,
            );
        }
    }

    fn peer_addresses(&self) -> Vec<String> {
        let peers = self.inner.peers.lock().unwrap();
        let mut addresses: Vec<String> = peers.keys().cloned().collect();
        addresses.sort();
        addresses
    }

    fn peer_infos(&self) -> Vec<Value> {
        let peers = self.inner.peers.lock().unwrap();
        let mut infos: Vec<Value> = peers.values().map(Peer::info).collect();
        infos.sort_by_key(|info| {
            info.get("address")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned()
        });
        infos
    }

    /// The write half of `address`, when that peer is registered.
    fn writer_for(&self, address: &str) -> Option<Arc<Mutex<Stream>>> {
        let peers = self.inner.peers.lock().unwrap();
        peers.get(address).map(|peer| Arc::clone(&peer.stream))
    }

    /// Sends `body` from the broker to every peer except `except`.
    fn broadcast_system(&self, body: Value, except: &str) {
        let targets: Vec<Arc<Mutex<Stream>>> = {
            let peers = self.inner.peers.lock().unwrap();
            peers
                .values()
                .filter(|peer| peer.address != except)
                .map(|peer| Arc::clone(&peer.stream))
                .collect()
        };
        let frame = serialize(&Envelope::new(BROKER_NAME, Value::Null, body, None));
        for target in targets {
            let mut guard = target.lock().unwrap();
            let _ = ipc::write_line(&mut *guard, &frame);
        }
    }

    /// Routes one inbound envelope, ACKing a delivered unicast.
    fn route(&self, sender_address: &str, mut envelope: Envelope) {
        // The broker owns `from`; a peer cannot claim another identity.
        envelope.from = sender_address.to_owned();

        if envelope.to.as_str() == Some(BROKER_NAME) {
            self.handle_broker_message(sender_address, &envelope);
            return;
        }

        let targets = match envelope.targets() {
            None => self.peer_addresses(),
            Some(targets) => targets,
        };
        let broadcast = envelope.is_broadcast();
        let is_unicast = !broadcast && targets.len() == 1;

        let mut delivered = Vec::new();
        for target in &targets {
            if target == sender_address || target == BROKER_NAME {
                continue;
            }
            let Some(stream) = self.writer_for(target) else {
                continue;
            };
            let frame = serialize(&envelope);
            let sent = {
                let mut guard = stream.lock().unwrap();
                ipc::write_line(&mut *guard, &frame).is_ok()
            };
            if sent {
                delivered.push(target.clone());
            }
        }

        // Unicast delivery is ACKed; multi-target sends are fire-and-forget
        // because there is no single authoritative recipient.
        //
        // The audit record is written BEFORE the ACK: an ACK means "delivered,
        // and durably recorded", so a sender that saw `received` can rely on the
        // audit trail existing rather than racing the broker's file write.
        if is_unicast {
            if delivered.is_empty() {
                // Peer offline: stay silent so the sender observes a timeout,
                // matching the reference protocol (no negative ACK).
                self.audit(&envelope, &delivered, Some("none"));
                return;
            }
            self.audit(&envelope, &delivered, Some("received"));
            let ack = Envelope::new(
                BROKER_NAME,
                Value::String(sender_address.to_owned()),
                ack_body(AckStatus::Received, &delivered[0]),
                Some(envelope.id.clone()),
            );
            if let Some(stream) = self.writer_for(sender_address) {
                let mut guard = stream.lock().unwrap();
                let _ = ipc::write_line(&mut *guard, &serialize(&ack));
            }
        } else {
            self.audit(&envelope, &delivered, Some("none"));
        }
    }

    /// Answers envelopes addressed to `broker` itself.
    fn handle_broker_message(&self, sender_address: &str, envelope: &Envelope) {
        let kind = envelope
            .body
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("");
        let reply_body = match kind {
            "list_peers" => serde_json::json!({
                "type": "list_peers_reply",
                "peers": self.peer_addresses(),
                "peers_detailed": self.peer_infos(),
            }),
            "peers_request" => serde_json::json!({
                "type": "peers_update",
                "peers": self.peer_addresses(),
                "peers_detailed": self.peer_infos(),
            }),
            _ => return,
        };
        let reply = Envelope::new(
            BROKER_NAME,
            Value::String(sender_address.to_owned()),
            reply_body,
            Some(envelope.id.clone()),
        );
        if let Some(stream) = self.writer_for(sender_address) {
            let mut guard = stream.lock().unwrap();
            let _ = ipc::write_line(&mut *guard, &serialize(&reply));
        }
    }

    fn audit(&self, envelope: &Envelope, delivered: &[String], ack: Option<&str>) {
        let Some(path) = &self.inner.audit else {
            return;
        };
        let record = serde_json::json!({
            "ts": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0),
            "id": envelope.id,
            "from": envelope.from,
            "to": envelope.to,
            "re": envelope.re,
            "delivered": delivered,
            "ack": ack,
        });
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
            let _ = writeln!(file, "{record}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::{buffered, connect, write_line};
    use std::time::{Duration, Instant};

    struct TestMesh {
        dir: PathBuf,
        path: String,
    }

    impl TestMesh {
        fn new(tag: &str) -> Self {
            // UDS paths are capped at SUN_LEN (~104 bytes on macOS); a long
            // temp path plus a long tag overflows it. Use a short, bounded
            // directory name and shorten the tag to stay inside the limit.
            let tag: String = tag.chars().take(6).collect();
            let short = format!("rpir-{}-{tag}", std::process::id() % 100_000);
            let dir = std::env::temp_dir().join(short);
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("b.sock").to_string_lossy().to_string();
            let listener = Listener::bind(&path).unwrap();
            let (broker, stop) = Broker::start(Some(dir.join("audit.jsonl")));
            let broker = Arc::new(broker);
            std::thread::spawn(move || broker.serve(&listener));
            // Keeping the handle alive for the test's duration; dropping it
            // would stop the broker.
            std::mem::forget(stop);
            Self { dir, path }
        }

        fn connect(&self, name: &str, cwd: &str) -> TestPeer {
            TestPeer::join(&self.path, name, cwd)
        }
    }

    impl Drop for TestMesh {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.dir).ok();
        }
    }

    struct TestPeer {
        stream: Stream,
        reader: BufReader<Stream>,
        buffer: String,
        address: String,
    }

    impl TestPeer {
        fn join(path: &str, name: &str, cwd: &str) -> Self {
            let stream = connect(path).unwrap();
            let reader = buffered(stream.try_clone().unwrap());
            let mut peer = Self {
                stream,
                reader,
                buffer: String::new(),
                address: String::new(),
            };
            let register = serde_json::json!({"type":"register","name":name,"cwd":cwd});
            write_line(&mut peer.stream, &(register.to_string() + "\n")).unwrap();
            let ack = peer
                .timed_raw_line(Duration::from_secs(3))
                .expect("register_ack");
            let ack: Value = serde_json::from_str(&ack).unwrap();
            peer.address = ack["address_assigned"].as_str().unwrap().to_owned();
            peer
        }

        /// Reads the next line verbatim, including broker control traffic.
        fn next_raw_line(&mut self) -> Option<String> {
            ipc::read_line(&mut self.reader, &mut self.buffer)
                .ok()
                .flatten()
        }

        /// Reads the next line, skipping the broker's `peer_joined` /
        /// `peer_left` broadcasts. Bounded so a broken expectation fails fast
        /// instead of hanging the test run.
        fn next_line(&mut self) -> Option<String> {
            self.next_line_within(Duration::from_secs(3))
        }

        /// `next_line` with an explicit deadline.
        fn next_line_within(&mut self, timeout: Duration) -> Option<String> {
            let deadline = Instant::now() + timeout;
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return None;
                }
                let line = self.timed_raw_line(remaining)?;
                let parsed: Value = serde_json::from_str(&line).ok()?;
                if parsed["body"]["type"].as_str() == Some("peer_joined")
                    || parsed["body"]["type"].as_str() == Some("peer_left")
                {
                    continue;
                }
                return Some(line);
            }
        }

        /// Reads one raw line with a bounded wait.
        fn timed_raw_line(&mut self, timeout: Duration) -> Option<String> {
            self.stream.set_read_timeout(Some(timeout)).ok();
            let line = self.next_raw_line();
            self.stream.set_read_timeout(None).ok();
            line
        }

        fn send(&mut self, envelope: &Envelope) {
            write_line(&mut self.stream, &serialize(envelope)).unwrap();
        }
    }

    #[test]
    fn assigns_canonical_addresses_and_routes_between_peers() {
        let mesh = TestMesh::new("route");
        let mut alpha = mesh.connect("alpha", "/tmp/project");
        let mut beta = mesh.connect("beta", "/tmp/project");
        assert_eq!(alpha.address, "/tmp/project@alpha");
        assert_eq!(beta.address, "/tmp/project@beta");

        let envelope = Envelope::new(
            "spoofed",
            Value::String(beta.address.clone()),
            serde_json::json!({"type":"ping"}),
            None,
        );
        let id = envelope.id.clone();
        alpha.send(&envelope);

        let received: Value = serde_json::from_str(&beta.next_line().unwrap()).unwrap();
        // The broker overwrites `from` with the authenticated sender address.
        assert_eq!(received["from"], alpha.address);
        assert_eq!(received["body"]["type"], "ping");
        assert_eq!(received["id"], id);
    }

    #[test]
    fn acks_a_delivered_unicast_and_times_out_on_a_missing_peer() {
        let mesh = TestMesh::new("ack");
        let mut alpha = mesh.connect("alpha", "/tmp/p");
        let beta = mesh.connect("beta", "/tmp/p");

        alpha.send(&Envelope::new(
            "unused",
            Value::String(beta.address.clone()),
            serde_json::json!({"type":"ping"}),
            None,
        ));
        let ack: Value = serde_json::from_str(&alpha.next_line().unwrap()).unwrap();
        assert_eq!(ack["from"], "broker");
        assert_eq!(ack["body"]["type"], "ack");
        assert_eq!(ack["body"]["status"], "received");
        assert_eq!(ack["body"]["target"], beta.address);

        // An offline target produces no ACK at all.
        alpha.send(&Envelope::new(
            "unused",
            Value::String("/tmp/p@ghost".into()),
            serde_json::json!({"type":"ping"}),
            None,
        ));
        assert!(alpha.next_line_within(Duration::from_millis(200)).is_none());
    }

    #[test]
    fn broadcast_reaches_every_other_peer_without_acking() {
        let mesh = TestMesh::new("broadc");
        let mut alpha = mesh.connect("alpha", "/tmp/p");
        let mut beta = mesh.connect("beta", "/tmp/p");
        let mut gamma = mesh.connect("gamma", "/tmp/p");

        alpha.send(&Envelope::new(
            "unused",
            Value::String("broadcast".into()),
            serde_json::json!({"type":"hello"}),
            None,
        ));
        let to_beta: Value = serde_json::from_str(&beta.next_line().unwrap()).unwrap();
        let to_gamma: Value = serde_json::from_str(&gamma.next_line().unwrap()).unwrap();
        assert_eq!(to_beta["body"]["type"], "hello");
        assert_eq!(to_gamma["body"]["type"], "hello");

        // The sender gets no ACK for a broadcast.
        assert!(alpha.next_line_within(Duration::from_millis(200)).is_none());
    }

    #[test]
    fn answers_list_peers_and_excludes_the_broker() {
        let mesh = TestMesh::new("peers");
        let mut alpha = mesh.connect("alpha", "/tmp/p");
        let beta = mesh.connect("beta", "/tmp/p");

        let request = Envelope::new(
            "unused",
            Value::String("broker".into()),
            serde_json::json!({"type":"list_peers"}),
            None,
        );
        let request_id = request.id.clone();
        alpha.send(&request);

        let reply: Value = serde_json::from_str(&alpha.next_line().unwrap()).unwrap();
        assert_eq!(reply["from"], "broker");
        assert_eq!(reply["re"], request_id);
        let peers: Vec<&str> = reply["body"]["peers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item.as_str().unwrap())
            .collect();
        assert_eq!(peers, vec!["/tmp/p@alpha", beta.address.as_str()]);
    }

    #[test]
    fn a_re_registration_takes_the_name_over_instead_of_suffixing() {
        let mesh = TestMesh::new("reincar");
        let first = mesh.connect("alpha", "/tmp/p");
        assert_eq!(first.address, "/tmp/p@alpha");
        drop(first);
        let second = mesh.connect("alpha", "/tmp/p");
        assert_eq!(second.address, "/tmp/p@alpha");
    }
}
