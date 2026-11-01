//! Per-`(cwd, name)` singleton lock for `/remote-pi`.
//!
//! At most one session may hold the lock for an identity; a second attempt is
//! refused. Implemented as a bound IPC endpoint rather than a PID file, because
//! the kernel releases the endpoint the instant the process dies — including on
//! `SIGKILL` — so there is no stale lock to clean up and no check-then-claim
//! race between "is it held?" and "claim it".

use crate::ipc::{self, Listener};
use crate::state;

/// The result of attempting to take the lock.
pub enum LockResult {
    /// We hold it. Dropping the guard releases the endpoint.
    Acquired(LockGuard),
    /// A live session already holds it.
    Refused { path: String },
}

/// Owns the bound endpoint for as long as the lock is held.
pub struct LockGuard {
    listener: Listener,
    path: String,
}

impl LockGuard {
    /// The endpoint the lock is held on.
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if !ipc::uses_named_pipe() {
            let _ = std::fs::remove_file(&self.path);
        }
        let _ = &self.listener;
    }
}

/// The endpoint a `(cwd, name)` identity locks under `root`.
///
/// Tests and [`MeshNode::join_with_endpoint`](crate::mesh_node::MeshNode) pass an
/// explicit root so they never touch the real state root; production callers use
/// [`acquire`], which supplies the process-wide one.
pub fn lock_path_in(root: &std::path::Path, cwd: &str, name: &str) -> String {
    let id = state::room_id_for(cwd, Some(name));
    ipc::ipc_address(
        &root
            .join("locks")
            .join(format!("{id}.sock"))
            .to_string_lossy(),
    )
}

/// Attempts to take the lock for `(cwd, name)` under the state root.
pub fn acquire(cwd: &str, name: &str) -> LockResult {
    acquire_at(&lock_path_in(&state::remote_root(), cwd, name))
}

/// Attempts to take the lock for an explicit endpoint path.
///
/// Order matters: bind first (the kernel only grants it when nothing holds the
/// endpoint), probe only on failure, and clear a stale endpoint before the
/// retry. Binding over an existing file would *steal* a live holder's lock.
pub fn acquire_at(path: &str) -> LockResult {
    if !ipc::uses_named_pipe() {
        if let Some(parent) = std::path::Path::new(path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
    }

    if let Ok(listener) = Listener::bind(path) {
        return LockResult::Acquired(LockGuard {
            listener,
            path: path.to_owned(),
        });
    }
    // The bind failed. A live listener answering means the lock is genuinely
    // held; otherwise the endpoint is stale and can be cleared before retrying.
    if let Ok(Some(stream)) = crate::election::try_connect(path) {
        drop(stream);
        return LockResult::Refused {
            path: path.to_owned(),
        };
    }
    crate::election::remove_stale(path);
    match Listener::bind(path) {
        Ok(listener) => LockResult::Acquired(LockGuard {
            listener,
            path: path.to_owned(),
        }),
        // Someone else bound between our unlink and retry: treat as held.
        Err(_) => LockResult::Refused {
            path: path.to_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_lock_lives_one_locks_directory_below_the_state_root() {
        // Regression: the endpoint is `<root>/locks/<id>.sock`. An earlier
        // version appended `locks` to an already-suffixed directory and wrote
        // `<root>/locks/locks/<id>.sock`, which still "worked" (the parent was
        // created recursively) while scattering state outside the documented
        // layout.
        let root = std::path::Path::new("/state");
        let path = lock_path_in(root, "/tmp/project", "alpha");
        assert!(path.starts_with("/state/locks/"), "unexpected path: {path}");
        assert!(
            !path.contains("locks/locks"),
            "the locks directory must not be nested twice: {path}"
        );
        assert!(path.ends_with(".sock"), "unexpected path: {path}");
    }

    #[test]
    fn a_second_acquire_is_refused_until_the_first_is_released() {
        // The test targets an explicit root, so it never touches (or depends
        // on) the process-wide state root. The name is kept short because a
        // Unix-domain socket path is capped at `SUN_LEN` (~104 bytes).
        let root = std::env::temp_dir().join(format!(
            "rpil-{}-{:?}",
            std::process::id() % 100_000,
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&root).unwrap();

        let cwd = if cfg!(windows) {
            r"C:\project"
        } else {
            "/tmp/project"
        };
        let alpha = lock_path_in(&root, cwd, "alpha");
        let beta = lock_path_in(&root, cwd, "beta");
        assert_ne!(alpha, beta, "distinct names must not share a lock");

        let first = acquire_at(&alpha);
        assert!(matches!(first, LockResult::Acquired(_)));
        assert!(matches!(acquire_at(&alpha), LockResult::Refused { .. }));
        // A different name in the same folder is a different identity.
        assert!(matches!(acquire_at(&beta), LockResult::Acquired(_)));

        drop(first);
        assert!(matches!(acquire_at(&alpha), LockResult::Acquired(_)));

        std::fs::remove_dir_all(&root).ok();
    }
}
