//! Leader election over the local IPC endpoint.
//!
//! Every participant races for the same endpoint: first it tries to *connect*
//! (someone else already leads), and only on failure does it try to *bind*
//! (become the leader). A stale endpoint left by a crashed leader is detected
//! because connecting to it fails, and is then unlinked so the bind succeeds.
//! The kernel owns the endpoint, so there is no check-then-claim race and no
//! PID file to go stale.

use std::time::Duration;

use crate::ipc::{self, Listener, Stream};

/// How many bind/connect rounds to attempt before giving up.
const MAX_ATTEMPTS: usize = 20;
/// Base backoff between attempts.
const BASE_BACKOFF_MS: u64 = 30;
/// Random jitter added to each backoff.
const JITTER_MS: u64 = 70;
/// How long a connect probe waits before declaring the endpoint dead.
const PROBE_TIMEOUT_MS: u64 = 500;

/// The outcome of the race: either we lead (we hold the listener) or we follow
/// (we hold a connection to the existing leader).
pub enum Election {
    Leader(Listener),
    Follower(Stream),
}

/// Races for `path`, returning the role we won.
pub fn join_or_lead(path: &str) -> std::io::Result<Election> {
    let mut last_error = None;
    for _ in 0..MAX_ATTEMPTS {
        if let Some(stream) = try_connect(path)? {
            return Ok(Election::Follower(stream));
        }
        remove_stale(path);
        match Listener::bind(path) {
            Ok(listener) => return Ok(Election::Leader(listener)),
            Err(error) => last_error = Some(error),
        }
        std::thread::sleep(Duration::from_millis(
            BASE_BACKOFF_MS + pseudo_random_jitter(),
        ));
    }
    Err(last_error.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
            format!("leader election failed after {MAX_ATTEMPTS} attempts: {path}"),
        )
    }))
}

/// Probes `path`, returning a live connection only when a listener answers.
pub fn try_connect(path: &str) -> std::io::Result<Option<Stream>> {
    // Any connect failure means "no live listener": the endpoint only exists
    // while its owner holds it, so there is no failure kind worth
    // distinguishing here.
    let Ok(stream) = ipc::connect(path) else {
        return Ok(None);
    };
    // A successful `connect` to a live endpoint is the whole signal: no byte
    // exchange is required, because the endpoint only exists while its owner
    // lives. Bound the probe so a half-open socket cannot hang startup.
    let _ = stream.set_read_timeout(Some(Duration::from_millis(PROBE_TIMEOUT_MS)));
    Ok(Some(stream))
}

/// Removes a leftover endpoint file that no live listener owns.
pub fn remove_stale(path: &str) {
    #[cfg(unix)]
    use std::os::unix::fs::FileTypeExt;

    if ipc::uses_named_pipe() {
        // Windows named pipes disappear with their owner; never unlink.
        return;
    }
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return;
    };
    if metadata.file_type().is_socket() {
        let _ = std::fs::remove_file(path);
    }
}

/// Cheap, non-cryptographic jitter. Election backoff does not need entropy —
/// it only needs to de-synchronize two processes that lost the same race.
fn pseudo_random_jitter() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.subsec_nanos() as u64)
        .unwrap_or(0);
    let mixed = nanos
        .wrapping_mul(6_364_136_223_846_793_005)
        .rotate_left(17)
        ^ (std::process::id() as u64);
    mixed % (JITTER_MS + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_participant_leads_and_the_second_follows() {
        let dir = std::env::temp_dir().join(format!("rpi-remote-election-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("broker.sock").to_string_lossy().to_string();

        let leader = join_or_lead(&path).unwrap();
        assert!(matches!(leader, Election::Leader(_)));

        let follower = join_or_lead(&path).unwrap();
        assert!(matches!(follower, Election::Follower(_)));

        // Once the leader's listener is gone the endpoint is stale and the next
        // participant takes over the same path.
        drop(leader);
        remove_stale(&path);
        let successor = join_or_lead(&path).unwrap();
        assert!(matches!(successor, Election::Leader(_)));

        std::fs::remove_dir_all(&dir).ok();
    }
}
