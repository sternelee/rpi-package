//! Test fixture for the cross-process failover tests.
//!
//! A tiny program that joins the mesh and answers one command per stdin line,
//! so a test can drive a peer in a separate process — including killing it.
//!
//! Commands (one JSON-free, tab-separated line each):
//!   `SEND\t{"to":"<address>","body":{...}}` — send and wait for the delivery
//!       ACK, then print `OK` or `ERROR\t<message>`.
//!   `PEERS` — print `PEERS\t<json array of addresses>`.
//!   `LEAVE` — drop the session, print `OK`.
//!
//! It prints `JOINED\t<address>` on startup and `LEADING` instead when this
//! process won the election.

use std::io::{BufRead, Write};

use rpi_remote::testing::join_default;

fn main() {
    let name = std::env::args().nth(1).unwrap_or_else(|| "peer".into());
    let cwd = std::env::args().nth(2).unwrap_or_else(|| ".".into());
    let peer = match join_default(&cwd, &name) {
        Ok(peer) => peer,
        Err(error) => {
            println!("ERROR\t{error}");
            return;
        }
    };
    // The test uses "LEADING" to know which child to kill.
    if peer.is_leader() {
        println!("{LEADING}");
    } else {
        println!("JOINED\t{}", peer.address());
    }
    let _ = std::io::stdout().flush();

    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        let line = line.trim().to_owned();
        if line.is_empty() {
            continue;
        }
        let (command, rest) = match line.split_once('\t') {
            Some((command, rest)) => (command.to_owned(), rest.to_owned()),
            None => (line.clone(), String::new()),
        };
        match command.as_str() {
            "SEND" => {
                let payload: serde_json::Value =
                    serde_json::from_str(&rest).unwrap_or(serde_json::Value::Null);
                let to = payload.get("to").and_then(|v| v.as_str()).unwrap_or("");
                let body = payload
                    .get("body")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                let outcome = peer.send_with_ack(to, body, None);
                println!("{}", describe(&outcome));
            }
            "PEERS" => {
                let peers = peer.list_peers();
                println!("PEERS\t{}", serde_json::to_string(&peers).unwrap());
            }
            "LEAVE" => {
                peer.session_peer().leave();
                println!("OK");
            }
            other => println!("ERROR\tunknown command {other}"),
        }
        let _ = std::io::stdout().flush();
    }
}

const LEADING: &str = "LEADING";

fn describe(outcome: &rpi_remote::testing::PeerOutcome) -> String {
    match outcome {
        rpi_remote::testing::PeerOutcome::AckReceived { .. } => "OK".to_owned(),
        rpi_remote::testing::PeerOutcome::Sent => "OK".to_owned(),
        rpi_remote::testing::PeerOutcome::AckDenied { reason } => {
            format!(
                "ERROR\tdenied: {}",
                reason.as_deref().unwrap_or("no reason")
            )
        }
        rpi_remote::testing::PeerOutcome::AckTimeout => "ERROR\tack timeout".to_owned(),
    }
}
