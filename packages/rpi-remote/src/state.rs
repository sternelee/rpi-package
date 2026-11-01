//! Filesystem roots, identity derivation, and the per-session runtime state.
//!
//! Every path is resolved at call time (never frozen in a `const`) so a test or
//! an operator can retarget the state root with `RPI_CODING_AGENT_DIR`. That
//! matters more here than in most extensions: the mesh endpoint, the cwd lock,
//! the audit log and the relay config all derive from this root, and a frozen
//! path would silently pin a session to the developer's real home directory.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use sha2::{Digest, Sha256};

/// Fixed local session name. The local mesh is a single per-machine session:
/// every process on the host shares one broker, matching the reference design
/// where multi-session support was removed after every install converged on one.
pub const LOCAL_SESSION_NAME: &str = "local";

/// Environment override for the rpi agent directory (mirrors the host).
const AGENT_DIR_ENV: &str = "RPI_CODING_AGENT_DIR";
/// Environment override for the mesh state root (tests and QA).
pub const REMOTE_HOME_ENV: &str = "RPI_REMOTE_HOME";

/// The base directory holding the host's agent state.
///
/// Precedence: `RPI_CODING_AGENT_DIR` → `$HOME/.rpi/agent`. The env var is the
/// same one the host honors, so an isolated session isolates the mesh too.
pub fn agent_dir() -> PathBuf {
    if let Some(value) = non_empty_env(AGENT_DIR_ENV) {
        return PathBuf::from(value);
    }
    home_dir().join(".rpi").join("agent")
}

/// rpi-remote's own state root: `<agent dir>/remote`, or `RPI_REMOTE_HOME`.
pub fn remote_root() -> PathBuf {
    if let Some(value) = non_empty_env(REMOTE_HOME_ENV) {
        return PathBuf::from(value);
    }
    agent_dir().join("remote")
}

/// `<state root>/sessions/<name>` — where a session's endpoint and audit live.
pub fn session_dir(name: &str) -> PathBuf {
    remote_root().join("sessions").join(name)
}

/// The IPC address of the session broker.
pub fn session_sock(name: &str) -> String {
    crate::ipc::ipc_address(&session_dir(name).join("broker.sock").to_string_lossy())
}

/// Path of the session's JSONL audit log.
pub fn session_audit(name: &str) -> PathBuf {
    session_dir(name).join("audit.jsonl")
}

/// `<state root>/skills` — where the bundled agent-network skill is deployed.
pub fn skills_dir() -> PathBuf {
    remote_root().join("skills")
}

/// The skills directory shipped with the installed extension, when present.
///
/// `rpi install` places the crate's resources next to the built library, so the
/// skill is discovered from the install location rather than copied into the
/// state root on every load.
pub fn bundled_skills_dir() -> Option<String> {
    // Alongside the installed cdylib: <extensions-dir>/skills. The state root is
    // the fallback, for a manually staged install.
    [installed_skills_dir(), Some(skills_dir())]
        .into_iter()
        .flatten()
        .find(|dir| dir.is_dir())
        .map(|dir| dir.to_string_lossy().into_owned())
}

/// `<dir holding the running executable>/skills`, where `rpi install` puts the
/// crate's resources.
fn installed_skills_dir() -> Option<PathBuf> {
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|dir| dir.join("skills")))
}

/// Copies the bundled agent-network skill into the state root, if a source copy
/// is reachable. Best-effort: a missing source is not an error.
///
/// Called on join as well as relied on via `resources_discover`, because some
/// hosts scan the state root for skills *before* plugins are loaded and would
/// otherwise miss a skill that only lives next to the installed library.
pub fn deploy_bundled_skill() {
    let Some(source) = installed_skills_dir().map(|dir| dir.join("agent-network")) else {
        return;
    };
    if !source.is_dir() {
        return;
    }
    let target = skills_dir().join("agent-network");
    let _ = std::fs::create_dir_all(&target);
    let _ = std::fs::copy(source.join("SKILL.md"), target.join("SKILL.md"));
}

/// Creates the per-session and skills directories.
pub fn ensure_dirs() -> std::io::Result<()> {
    std::fs::create_dir_all(session_dir(LOCAL_SESSION_NAME))?;
    std::fs::create_dir_all(skills_dir())?;
    Ok(())
}

/// Resolves `~`, preferring `HOME` then `USERPROFILE` (Windows).
fn home_dir() -> PathBuf {
    if let Some(value) = non_empty_env("HOME") {
        return PathBuf::from(value);
    }
    if let Some(value) = non_empty_env("USERPROFILE") {
        return PathBuf::from(value);
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

/// Canonicalizes `cwd` through `realpath` so symlinked paths map to one
/// identity. Falls back to the raw path when the directory is gone.
pub fn canonical_cwd(cwd: &str) -> String {
    std::fs::canonicalize(cwd)
        .map(|path| strip_unc_prefix(path.to_string_lossy().into_owned()))
        .unwrap_or_else(|_| cwd.to_owned())
}

/// Windows `canonicalize` returns `\\?\C:\...`; strip the verbatim prefix so
/// addresses and room ids match a non-canonical path's form.
fn strip_unc_prefix(path: String) -> String {
    if let Some(rest) = path.strip_prefix(r"\\?\") {
        rest.to_owned()
    } else {
        path
    }
}

/// The `(cwd, name)`-keyed room id.
///
/// Several agents in the **same** folder must land on distinct rooms, so the
/// name is part of the digest when it differs from the folder's default name.
/// When `name` is absent or equals the folder-derived default, the legacy
/// cwd-only id is returned, so a single unnamed agent's room is not re-keyed.
pub fn room_id_for(cwd: &str, name: Option<&str>) -> String {
    let canonical = canonical_cwd(cwd);
    match name {
        Some(name) if !name.is_empty() && name != default_agent_name(&canonical) => {
            // NUL separates cwd from name; it cannot occur in a POSIX path and
            // is stripped from any sanitized name, so the boundary is
            // unambiguous.
            first_12_of_hash(&format!("{canonical}\0{name}"))
        }
        _ => first_12_of_hash(&canonical),
    }
}

fn first_12_of_hash(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    base64_url_no_pad(&digest)[..12].to_owned()
}

/// The default display name of an agent in `cwd`: the folder's leaf name.
pub fn default_agent_name(cwd: &str) -> String {
    let path = Path::new(cwd);
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "agent".into())
}

/// URL-safe base64 without padding (room ids, key fingerprints).
pub fn base64_url_no_pad(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// The process's working directory, falling back to `.`.
pub fn current_cwd() -> String {
    std::env::current_dir()
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|_| ".".into())
}

/// The display name for this process's agent in `cwd`.
///
/// Reads `agentName` from `<cwd>/.rpi/remote.json` when present so several
/// agents can run in one folder under explicit names.
pub fn display_name(cwd: &str) -> String {
    project_config_name(cwd).unwrap_or_else(|| default_agent_name(cwd))
}

/// Reads `agentName` from the project's remote config, when set and usable.
pub fn project_config_name(cwd: &str) -> Option<String> {
    let path = Path::new(cwd).join(".rpi").join("remote.json");
    let raw = std::fs::read_to_string(path).ok()?;
    let value: serde_json::Value = serde_json::from_str(&raw).ok()?;
    let name = value.get("agentName")?.as_str()?;
    crate::envelope::sanitize_segment(name)
}

// ---------------------------------------------------------------------------
// Session runtime state
// ---------------------------------------------------------------------------

/// The live mesh for this process, if `/remote-pi start` has run.
///
/// Dropping the session releases the `(cwd, name)` lock: the `MeshNode` owns
/// the bound endpoint, so there is no separate guard to keep in sync.
pub struct Session {
    pub node: crate::mesh_node::MeshNode,
}

static SESSION: OnceLock<std::sync::Mutex<Option<std::sync::Arc<Session>>>> = OnceLock::new();

fn session_slot() -> &'static std::sync::Mutex<Option<std::sync::Arc<Session>>> {
    SESSION.get_or_init(|| std::sync::Mutex::new(None))
}

/// Replaces the live session, returning the previous one if any.
pub fn take_session() -> Option<std::sync::Arc<Session>> {
    session_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take()
}

/// The live session, when one has started.
pub fn current_session() -> Option<std::sync::Arc<Session>> {
    session_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

/// Stores the live session.
pub fn set_session(session: std::sync::Arc<Session>) {
    *session_slot()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(session);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn room_ids_are_stable_and_name_scoped() {
        let cwd = if cfg!(windows) {
            r"C:\projects\demo"
        } else {
            "/tmp/demo"
        };
        // The unnamed id *is* the cwd-only id, so this doubles as the
        // regression test for "an existing single agent is never re-keyed".
        let legacy = room_id_for(cwd, None);
        assert_eq!(legacy.len(), 12);
        // The default name reproduces the legacy id, so an unnamed single agent
        // is never re-keyed.
        assert_eq!(room_id_for(cwd, None), legacy);
        assert_eq!(room_id_for(cwd, Some("demo")), legacy);
        // A custom name is a distinct room.
        assert_ne!(room_id_for(cwd, Some("backend")), legacy);
        assert_ne!(
            room_id_for(cwd, Some("frontend")),
            room_id_for(cwd, Some("backend"))
        );
        assert_eq!(
            room_id_for(cwd, Some("backend")),
            room_id_for(cwd, Some("backend"))
        );
        assert_eq!(room_id_for(cwd, None).len(), 12);
    }

    #[test]
    fn default_agent_name_is_the_folder_leaf() {
        let cwd = if cfg!(windows) {
            r"C:\projects\demo"
        } else {
            "/projects/demo"
        };
        assert_eq!(default_agent_name(cwd), "demo");
        assert_eq!(default_agent_name("/"), "agent");
    }

    #[test]
    fn room_ids_use_url_safe_base64() {
        // A digest that maps to the two URL-safe-only characters proves the
        // alphabet, which is what makes an id safe inside a file name.
        assert_eq!(base64_url_no_pad(&[0xfb, 0xff]), "-_8");
    }
}
