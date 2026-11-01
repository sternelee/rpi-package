//! Regression tests for the leader-lifecycle bugs the failover tests exposed.
//!
//! These run in-process (cheaper than `failover.rs`, which spawns real child
//! processes) but cover the same defects: a dropped leader must release its
//! endpoint, and a surviving follower must be able to take the mesh over.

#![cfg(unix)]

use std::time::Duration;

use rpi_remote::testing::MeshHarness;

/// After the leader's session is dropped, its endpoint must be released so a
/// survivor can win the election — not left bound to a broker serving nobody.
#[test]
fn a_dropped_leader_releases_the_endpoint_for_a_survivor() {
    let mesh = MeshHarness::new("releas");
    let leader = mesh.join("/tmp/rl-project", "leader");
    let follower = mesh.join("/tmp/rl-project", "follower");
    assert!(leader.is_leader(), "the first joiner must lead");
    assert!(!follower.is_leader());

    // Dropping the leader's session must stop the broker it hosted.
    drop(leader);

    // Give the survivor's rejoin a moment; then it must be serving the mesh.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if follower.is_leader() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the survivor never took over after the leader was dropped"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // And it must be usable: the roster is only obtainable from a live broker.
    let roster = follower.list_peers();
    assert!(
        roster.contains(&follower.address()),
        "the survivor's own roster entry is missing: {roster:?}"
    );
}

/// A re-led mesh answers `list_peers`, which is what `agent_send` relies on.
///
/// This is the regression for the bug where the survivor re-led successfully but
/// never *registered* on its own broker, so every `broker` request timed out.
#[test]
fn a_re_led_mesh_serves_the_survivor() {
    let mesh = MeshHarness::new("reled2");
    let leader = mesh.join("/tmp/r2-project", "leader");
    let survivor = mesh.join("/tmp/r2-project", "survivor");
    let before = survivor.list_peers();
    assert!(
        before.contains(&leader.address()),
        "baseline roster: {before:?}"
    );

    drop(leader);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if survivor.is_leader() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the survivor never re-led"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // The bug this guards against: leadership flips but `list_peers` times out.
    let after = survivor.list_peers();
    assert!(
        after.contains(&survivor.address()),
        "a re-led mesh must answer roster queries: {after:?}"
    );
}
