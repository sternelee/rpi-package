//! Cross-process failover tests.
//!
//! These are the only tests that exercise the path that actually matters in
//! production: several `rpi` processes on one machine, where **the leader is a
//! separate process that can die independently**. A single-process test cannot
//! reach this state, because the broker and its peer share a lifetime there —
//! which is exactly how the missing-failover bug survived every other test.
//!
//! Each test spawns real child processes running this crate's `fixture` binary
//! (see `src/bin/fixture.rs`), drives them over their stdin/stdout, and kills
//! one on purpose.
//!
//! Run with `cargo test -p rpi-remote --test failover`. The fixture path needs
//! no environment: the test injects `RPI_REMOTE_HOME` into each child so nothing
//! touches the real `~/.rpi` state.

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

/// Marker the child prints once it has joined the mesh.
const JOINED: &str = "JOINED";
/// Marker the child prints once it is hosting the broker.
const LEADING: &str = "LEADING";

/// A child process running the fixture binary.
///
/// Commands are one line on stdin; the child answers with a marker line read
/// back over a channel, so every wait is bounded.
struct Fixture {
    child: Child,
    rx: mpsc::Receiver<String>,
}

impl Fixture {
    fn spawn(dir: &PathBuf, name: &str, cwd: &str) -> Self {
        // Cargo sets this for an integration test only when the crate declares a
        // `[[bin]]`, which the fixture does, so it is the reliable handle.
        let bin = PathBuf::from(std::env::var("CARGO_BIN_EXE_fixture").unwrap_or_else(|_| {
            std::env::current_exe()
                .expect("test binary path")
                .parent()
                .expect("test bin dir")
                .join("fixture")
                .to_string_lossy()
                .into_owned()
        }));
        assert!(bin.exists(), "fixture binary missing at {}", bin.display());
        let mut child = Command::new(&bin)
            .arg(name)
            .arg(cwd)
            .env("RPI_REMOTE_HOME", dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap_or_else(|error| panic!("spawn {}: {error}", bin.display()));
        let stdout = child.stdout.take().expect("child stdout");
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut lines = BufReader::new(stdout).lines();
            while let Some(Ok(line)) = lines.next() {
                if tx.send(line).is_err() {
                    return;
                }
            }
        });
        Self { child, rx }
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    fn is_running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// Sends a command and waits for the reply line.
    fn command(&mut self, line: &str) -> String {
        {
            let stdin = self.child.stdin.as_mut().expect("child stdin");
            stdin.write_all(line.as_bytes()).expect("write command");
            stdin.write_all(b"\n").expect("write newline");
            let _ = stdin.flush();
        }
        self.next_line(Duration::from_secs(5))
            .unwrap_or_else(|| "ERROR\tno reply".to_owned())
    }

    /// Waits for one marker line, bounded by `timeout`.
    ///
    /// The lines arrive over a channel fed by a dedicated reader thread, so a
    /// child that *hangs* surfaces as a timeout instead of hanging this test —
    /// which is the whole point of these tests.
    fn next_line(&mut self, timeout: Duration) -> Option<String> {
        match self.rx.recv_timeout(timeout) {
            Ok(line) => Some(line),
            Err(_) => None,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.kill();
    }
}

/// A private scratch root shared by one test's children.
fn scratch(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("rpifx-{}-{tag}", std::process::id() % 100_000));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

/// The folder the peers share, isolated per test.
///
/// Deliberately short: a Unix-domain socket path is capped at `SUN_LEN` (~104
/// bytes) and the agent's own state root already consumes part of that budget.
/// It must also be unique per test, or parallel runs see each other's peers.
fn project(dir: &std::path::Path) -> String {
    let path = dir.join("proj");
    std::fs::create_dir_all(&path).expect("create project dir");
    path.to_string_lossy().into_owned()
}

/// After the leader dies, a surviving follower must be able to send again.
#[test]
fn a_surviving_peer_recovers_after_the_leader_dies() {
    let dir = scratch("recover");
    let project = project(&dir);

    let mut leader = Fixture::spawn(&dir, "leader", &project);
    let mut follower = Fixture::spawn(&dir, "follower", &project);

    assert!(
        leader.next_line(Duration::from_secs(15)).is_some(),
        "leader never started"
    );
    assert!(
        follower.next_line(Duration::from_secs(15)).is_some(),
        "follower never started"
    );

    // Baseline: the follower reaches the broker by asking for its own roster.
    let before = follower.command("PEERS\n");
    assert!(
        before.starts_with("PEERS"),
        "baseline peers query failed: {before}"
    );
    assert!(
        before.contains("@follower"),
        "roster is missing us: {before}"
    );
    assert!(
        before.contains("@leader"),
        "roster is missing the leader: {before}"
    );

    leader.kill();
    // Give the follower a moment to observe the closed socket. If it never does,
    // the next query blocks until its own timeout and reports it.
    std::thread::sleep(Duration::from_millis(500));

    let after = follower.command("PEERS\n");
    assert!(
        after.starts_with("PEERS"),
        "follower did not recover after the leader died (got `{after}`); \
         this is the missing-failover bug"
    );
    assert!(
        after.contains("@follower"),
        "recovered roster is missing us: {after}"
    );

    drop(follower);
    std::fs::remove_dir_all(&dir).ok();
}

/// A fresh process must be able to re-lead the mesh after the leader dies,
/// rather than hanging against a broker that no longer serves anyone.
#[test]
fn a_fresh_peer_becomes_the_new_leader_after_the_old_one_dies() {
    let dir = scratch("relead");
    let project = project(&dir);

    let mut leader = Fixture::spawn(&dir, "leader", &project);
    assert!(leader.next_line(Duration::from_secs(15)).is_some());

    leader.kill();
    std::thread::sleep(Duration::from_millis(300));

    let mut successor = Fixture::spawn(&dir, "successor", &project);
    let joined = successor.next_line(Duration::from_secs(15));
    assert!(joined.is_some(), "successor never started");
    let reply = successor.command("PEERS\n");
    assert!(
        reply.starts_with("PEERS"),
        "successor could not query the mesh after re-leading (got `{reply}`)"
    );
    assert!(
        reply.contains("@successor"),
        "roster is missing us: {reply}"
    );
    assert!(
        successor.is_running(),
        "successor must stay up as the new leader"
    );

    drop(successor);
    std::fs::remove_dir_all(&dir).ok();
}

/// Leaving must close the connection, not just stop writing: a peer the broker
/// still lists is a ghost that later sends will wait on forever.
#[test]
fn leaving_notifies_the_broker_so_no_ghost_peer_remains() {
    let dir = scratch("ghost");
    let project = project(&dir);

    let mut leader = Fixture::spawn(&dir, "leader", &project);
    assert!(leader.next_line(Duration::from_secs(15)).is_some());
    let mut gone = Fixture::spawn(&dir, "gone", &project);
    assert!(gone.next_line(Duration::from_secs(15)).is_some());

    // `LEAVE` drops the peer's session and closes the connection.
    let reply = gone.command("LEAVE\n");
    assert!(reply.starts_with("OK"), "leave failed: {reply}");
    gone.kill();

    let peers = leader.command("PEERS\n");
    assert!(
        !peers.contains("@gone"),
        "the broker still lists a peer that left: {peers}"
    );
    assert!(peers.contains("@leader"), "roster lost the leader: {peers}");

    drop(leader);
    std::fs::remove_dir_all(&dir).ok();
}
