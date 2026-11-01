//! End-to-end mesh tests over real Unix-domain sockets.
//!
//! These exercise the whole path a tool call takes: join (election + lock),
//! register, unicast with a delivery ACK, broadcast, `list_peers`, and the
//! `request`/`reply` correlation that `agent_request` relies on. They use
//! explicit paths rather than the process-wide state root so they are
//! independent of the developer's real `~/.rpi` directory.

#![cfg(unix)]

use std::time::Duration;

use rpi_remote::testing::{MeshHarness, PeerOutcome};

/// Builds an isolated mesh in a short temp directory.
fn harness(tag: &str) -> MeshHarness {
    MeshHarness::new(tag)
}

#[test]
fn two_peers_discover_each_other_and_exchange_a_unicast() {
    let mesh = harness("unicast");
    let alpha = mesh.join("/tmp/mesh-project", "alpha");
    let beta = mesh.join("/tmp/mesh-project", "beta");

    assert_eq!(alpha.address(), "/tmp/mesh-project@alpha");
    assert_eq!(beta.address(), "/tmp/mesh-project@beta");

    // `list_peers` must report both, and must not include the broker itself.
    let mut peers = alpha.list_peers();
    peers.sort();
    let mut expected = vec![alpha.address(), beta.address()];
    expected.sort();
    assert_eq!(peers, expected);

    // A unicast is acknowledged only after the broker delivered it.
    let outcome = alpha.send_with_ack(&beta.address(), serde_json::json!({"type": "ping"}), None);
    assert!(
        matches!(outcome, PeerOutcome::AckReceived { .. }),
        "expected a delivery ACK, got {outcome:?}"
    );

    let received = beta
        .drain_one(Duration::from_secs(2))
        .expect("beta got the ping");
    assert_eq!(received["body"]["type"], "ping");
    // The broker owns `from`, so it is the canonical sender address.
    assert_eq!(received["from"], alpha.address());
}

#[test]
fn the_broker_audits_every_routed_envelope() {
    let mesh = harness("audit");
    let audit = mesh.audit_path();
    let alpha = mesh.join("/tmp/mesh-audit", "alpha");
    let beta = mesh.join("/tmp/mesh-audit", "beta");

    alpha.send_with_ack(&beta.address(), serde_json::json!({"type": "logged"}), None);
    assert!(beta.drain_one(Duration::from_secs(2)).is_some());

    let records = rpi_remote::testing::Peer::audit_records(&audit);
    let entry = records
        .iter()
        .find(|record| record["from"] == alpha.address() && record["to"] == beta.address())
        .unwrap_or_else(|| panic!("routed envelope missing from the audit log: {records:?}"));
    assert_eq!(entry["delivered"][0], beta.address());
    assert_eq!(entry["ack"], "received");
}

#[test]
fn a_reply_carries_the_original_id_in_re() {
    let mesh = harness("reply");
    let alpha = mesh.join("/tmp/mesh-reply", "alpha");
    let beta = mesh.join("/tmp/mesh-reply", "beta");

    alpha.send_with_ack(
        &beta.address(),
        serde_json::json!({"type": "question"}),
        None,
    );
    let question = beta.drain_one(Duration::from_secs(2)).expect("question");
    let question_id = question["id"].as_str().unwrap().to_owned();

    // The reply is a normal send with `re` set to the original id.
    beta.send_with_ack(
        &alpha.address(),
        serde_json::json!({"type": "answer"}),
        Some(question_id.clone()),
    );
    let reply = alpha.drain_one(Duration::from_secs(2)).expect("answer");
    assert_eq!(reply["re"], question_id);
    assert_eq!(reply["body"]["type"], "answer");
}

#[test]
fn broadcast_reaches_every_peer_and_is_not_acknowledged() {
    let mesh = harness("bcast");
    let alpha = mesh.join("/tmp/mesh-bcast", "alpha");
    let beta = mesh.join("/tmp/mesh-bcast", "beta");
    let gamma = mesh.join("/tmp/mesh-bcast", "gamma");

    alpha.broadcast(serde_json::json!({"type": "announce"}));
    for peer in [&beta, &gamma] {
        let received = peer.drain_one(Duration::from_secs(2)).expect("broadcast");
        assert_eq!(received["body"]["type"], "announce");
    }
    // The sender does not receive its own broadcast.
    assert!(alpha.drain_one(Duration::from_millis(200)).is_none());
}

#[test]
fn a_unicast_to_an_absent_peer_times_out_instead_of_faking_success() {
    let mesh = harness("offline");
    let alpha = mesh.join("/tmp/mesh-offline", "alpha");
    let outcome = alpha.send_with_ack(
        "/tmp/mesh-offline@ghost",
        serde_json::json!({"type": "ping"}),
        None,
    );
    assert!(
        matches!(outcome, PeerOutcome::AckTimeout),
        "an absent peer must surface as a timeout, got {outcome:?}"
    );
}

#[test]
fn request_and_reply_resolve_through_the_correlation_map() {
    let mesh = harness("request");
    let alpha = mesh.join("/tmp/mesh-request", "alpha");
    let beta = mesh.join("/tmp/mesh-request", "beta");

    // The responder answers with `re` set, which is the contract
    // `agent_request` depends on.
    let responder = std::thread::spawn({
        let beta = beta.clone();
        move || {
            let question = beta.drain_one(Duration::from_secs(3)).expect("question");
            let question_id = question["id"].as_str().unwrap().to_owned();
            beta.send_with_ack(
                question["from"].as_str().unwrap(),
                serde_json::json!({"text": "pong"}),
                Some(question_id),
            );
        }
    });
    let reply = alpha
        .request(
            &beta.address(),
            serde_json::json!({"text": "ping"}),
            Duration::from_secs(3),
        )
        .expect("reply");
    assert_eq!(reply["body"]["text"], "pong");
    responder.join().unwrap();
}

#[test]
fn a_second_session_cannot_take_the_same_name_in_the_same_folder() {
    let mesh = harness("lock");
    let _alpha = mesh.join("/tmp/mesh-lock", "alpha");
    // The same (cwd, name) is a single identity: the second attempt is refused
    // rather than silently joining as `alpha#2`.
    assert!(mesh.try_join("/tmp/mesh-lock", "alpha").is_err());
    // A different name in the same folder is a distinct identity.
    assert!(mesh.try_join("/tmp/mesh-lock", "beta").is_ok());
}

#[test]
fn peers_in_different_folders_share_the_session_but_not_the_lock() {
    let mesh = harness("folders");
    let alpha = mesh.join("/tmp/mesh-a", "agent");
    let beta = mesh.join("/tmp/mesh-b", "agent");
    assert_eq!(alpha.address(), "/tmp/mesh-a@agent");
    assert_eq!(beta.address(), "/tmp/mesh-b@agent");
    // They are separate locks but one shared session.
    let peers = alpha.list_peers();
    assert!(peers.contains(&beta.address()));
}

/// Covers the production entrypoints end to end: `MeshNode::join` (which derives
/// its endpoint and lock from the state root) and every `/remote-pi` subcommand.
///
/// Ignored by default because it needs
/// `RPI_REMOTE_HOME` pointed at a scratch directory, so it never touches the
/// developer's real `~/.rpi` state.
#[test]
#[ignore = "needs RPI_REMOTE_HOME set to a scratch directory"]
fn the_remote_pi_command_walks_start_status_peers_stop() {
    assert!(
        std::env::var("RPI_REMOTE_HOME").is_ok(),
        "set RPI_REMOTE_HOME to a scratch directory before running this test"
    );
    let run = rpi_remote::testing::run_command;
    let text = |value: &serde_json::Value| {
        value
            .get("text")
            .and_then(|text| text.as_str())
            .unwrap_or("")
            .to_owned()
    };

    // Before joining, every subcommand must say so plainly instead of guessing.
    assert!(text(&run("status")).contains("Not on the local mesh"));
    assert!(text(&run("peers")).contains("Not on the local mesh"));

    // Joining reports the assigned address.
    let started = text(&run("start"));
    assert!(started.contains("Joined the local mesh as"), "{started}");

    // A second `start` is a no-op, not a second session.
    assert!(text(&run("start")).contains("Already on the local mesh"));

    let status = text(&run("status"));
    assert!(status.contains("Session: local"), "{status}");
    assert!(status.contains("Lock:"), "{status}");
    // The address is `<cwd>@<name>`; assert the shape rather than a peer count,
    // which depends on the surrounding sandbox.
    assert!(
        status.contains("Address: ") && status.contains('@'),
        "status must announce the assigned address: {status}"
    );

    // `/remote-pi peers` shows the roster with this agent marked, rather than
    // hiding it: the command is a diagnostic view, unlike the `list_peers` tool
    // which filters self out because an agent cannot address itself.
    let peers = text(&run("peers"));
    assert!(peers.contains("(you)"), "{peers}");

    // `rename` re-joins under a new identity, so the address changes.
    assert!(text(&run("rename backend")).contains("Renamed"));
    assert!(text(&run("status")).contains("Address: ") && text(&run("status")).contains("backend"));

    assert!(text(&run("stop")).contains("Left the local mesh"));
    assert!(text(&run("status")).contains("Not on the local mesh"));
    // Unknown subcommands are reported rather than silently ignored.
    assert!(text(&run("nonsense")).contains("Unknown subcommand"));
}
