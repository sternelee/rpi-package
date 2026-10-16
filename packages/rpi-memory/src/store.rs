//! Package-local atomic state-file write.
//!
//! This file is intentionally vendored so the crate can be published and built
//! independently before any other package in this workspace exists on crates.io.

use std::path::Path;

/// Write `text` to `path` via a temp sibling + rename.
///
/// `fs::write` truncates and then writes in place, so a shorter document written
/// over a longer one leaves the previous tail behind, and two rpi sessions
/// sharing a project can interleave their writes. Either way the state file
/// becomes unparsable. `rename` replaces atomically on Unix and on Windows
/// (`MoveFileEx` with `MOVEFILE_REPLACE_EXISTING`), so a reader only ever sees a
/// whole document. A failed rename removes the temp file instead of leaving a
/// stray sibling behind.
///
/// `label` names the store in error messages, e.g. `"memory store"`.
pub fn write_atomically(path: &Path, text: &str, label: &str) -> Result<(), String> {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| format!("{}.json", label.replace(' ', "-")));
    let tmp = path.with_file_name(format!("{name}.tmp-{}", std::process::id()));
    std::fs::write(&tmp, text).map_err(|e| format!("write {label}: {e}"))?;
    if let Err(error) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(format!("commit {label}: {error}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("rpi-store-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn overwrites_shorter_content_without_leaving_a_tail() {
        let dir = temp_dir("shorter");
        let path = dir.join("store.json");
        write_atomically(&path, r#"{"a":"a-longer-value"}"#, "test store").unwrap();
        write_atomically(&path, "{}", "test store").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn failed_commit_removes_the_temp_file() {
        let dir = temp_dir("failed");
        // A directory at `path` makes the rename fail, so the temp file must be
        // cleaned up rather than left behind.
        let path = dir.join("store.json");
        std::fs::create_dir(&path).unwrap();
        let error = write_atomically(&path, "{}", "test store").unwrap_err();
        assert!(error.contains("commit test store"), "{error}");
        let leftovers = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains(".tmp-"))
            .count();
        assert_eq!(leftovers, 0, "temp file left behind");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
