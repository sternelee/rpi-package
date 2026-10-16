use rpi_plugin_sdk::{
    register_entrypoint, FreeStringFn, PluginApi, RuntimeActionId, StableToolSchema, StbString,
    StbStringRef, StepHandle, StepResult, ToolPartialCb,
};
use serde_json::{json, Value};
use std::ffi::c_void;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

/// Host callbacks captured at registration time, so the slash command can
/// drive the session the same way the `plan` command does.
#[derive(Clone, Copy)]
struct Runtime {
    action: rpi_plugin_sdk::RuntimeActionFn,
    free_string: FreeStringFn,
    user_data: *mut c_void,
}
unsafe impl Send for Runtime {}
unsafe impl Sync for Runtime {}

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Test-only override so the command's `SendUserMessage` path is exercised
/// without a live host.
#[cfg(test)]
static TEST_RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn runtime() -> Result<Runtime, String> {
    #[cfg(test)]
    if let Some(runtime) = TEST_RUNTIME.get() {
        return Ok(*runtime);
    }
    RUNTIME
        .get()
        .copied()
        .ok_or_else(|| "simplify runtime is unavailable".to_string())
}

fn call_runtime(id: RuntimeActionId, args: Value) -> Result<Value, String> {
    let runtime = runtime()?;
    let input = args.to_string();
    let mut output = StbString::empty();
    let rc = (runtime.action)(
        id as u32,
        StbStringRef::from_str(&input),
        &mut output,
        runtime.user_data,
    );
    let text = output.to_string_lossy();
    (runtime.free_string)(output);
    if rc != 0 {
        return Err(if text.is_empty() {
            format!("runtime action {id:?} failed")
        } else {
            text
        });
    }
    if text.trim().is_empty() {
        Ok(Value::Null)
    } else {
        serde_json::from_str(&text).map_err(|e| format!("invalid runtime response: {e}"))
    }
}
struct Drive {
    params: Value,
    cancelled: AtomicBool,
    done: bool,
}
fn files(root: &Path, out: &mut Vec<PathBuf>, depth: usize) {
    if depth > 8 || out.len() >= 100 {
        return;
    }
    let Ok(rd) = fs::read_dir(root) else {
        return;
    };
    for e in rd.flatten() {
        let p = e.path();
        let n = p.file_name().and_then(|x| x.to_str()).unwrap_or("");
        if n == ".git" || n == "target" || n == "node_modules" || n == ".rpi" {
            continue;
        }
        if p.is_dir() {
            files(&p, out, depth + 1)
        } else if matches!(
            p.extension().and_then(|x| x.to_str()),
            Some(
                "rs" | "ts"
                    | "tsx"
                    | "js"
                    | "jsx"
                    | "py"
                    | "go"
                    | "java"
                    | "c"
                    | "h"
                    | "cpp"
                    | "hpp"
            )
        ) {
            out.push(p);
            if out.len() >= 100 {
                return;
            }
        }
    }
}
fn simplify(p: &Value) -> Result<String, String> {
    let root = PathBuf::from(p.get("root").and_then(Value::as_str).unwrap_or("."));
    if !root.exists() {
        return Err(format!("root does not exist: {}", root.display()));
    }
    let mut paths = Vec::new();
    if root.is_file() {
        paths.push(root.clone())
    } else {
        files(&root, &mut paths, 0)
    }
    let files_reviewed = paths.len();
    let mut reports = Vec::new();
    for path in paths {
        let Ok(text) = fs::read_to_string(&path) else {
            continue;
        };
        let lines: Vec<&str> = text.lines().collect();
        let long = lines.iter().filter(|l| l.chars().count() > 120).count();
        let todos = lines
            .iter()
            .filter(|l| l.contains("TODO") || l.contains("FIXME"))
            .count();
        let functions = lines
            .iter()
            .filter(|l| {
                l.contains("fn ")
                    || l.contains("function ")
                    || l.contains("def ")
                    || l.contains("func ")
            })
            .count();
        let mut dup = 0;
        for i in 1..lines.len() {
            if !lines[i].trim().is_empty() && lines[i].trim() == lines[i - 1].trim() {
                dup += 1;
            }
        }
        if long + todos + dup > 0 {
            reports.push(json!({"file":path,"lines":lines.len(),"longLines":long,"todoMarkers":todos,"adjacentDuplicates":dup,"functions":functions}));
        }
    }
    Ok(json!({"root":root,"filesReviewed":files_reviewed,"filesWithFindings":reports.len(),"findings":reports}).to_string())
}

/// Render the tool's JSON report as Markdown for the chat transcript.
fn render_report(report: &Value) -> String {
    let root = report.get("root").and_then(Value::as_str).unwrap_or(".");
    let reviewed = report
        .get("filesReviewed")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let with_findings = report
        .get("filesWithFindings")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let mut text = format!(
        "**simplify** — reviewed {reviewed} file(s), {with_findings} with findings under `{root}`.\n"
    );
    let findings = report
        .get("findings")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if findings.is_empty() {
        text.push_str("\nNo long lines, adjacent duplicate lines, or TODO/FIXME markers found.");
        return text;
    }
    text.push_str("\n| file | lines | long | dup | TODO |\n| --- | ---: | ---: | ---: | ---: |\n");
    for finding in findings {
        let file = finding.get("file").and_then(Value::as_str).unwrap_or("?");
        let field = |key: &str| finding.get(key).and_then(Value::as_u64).unwrap_or(0);
        text.push_str(&format!(
            "| `{file}` | {} | {} | {} | {} |\n",
            field("lines"),
            field("longLines"),
            field("adjacentDuplicates"),
            field("todoMarkers"),
        ));
    }
    text
}

/// `/simplify [path]` — run the deterministic scan, then hand the findings to
/// the model for an actual review.
///
/// The prompt reaches the session through [`RuntimeActionId::SetEditorText`],
/// never [`RuntimeActionId::SendUserMessage`]. A command handler runs on the
/// TUI's key-loop thread, and `SendUserMessage` awaits the whole agent turn
/// while the bridge parks the calling thread — so using it here freezes the
/// editor and makes Esc unable to cancel the run. `SetEditorText` only queues a
/// draft (host housekeeping, no harness call), and `autoSendMs` submits it
/// afterwards through the editor's normal path, which also renders a real user
/// bubble. Same reasoning as `rpi-voice`'s default output mode.
extern "C" fn simplify_command(args_json: StbStringRef, out: *mut StbString, _: *mut c_void) -> i32 {
    if out.is_null() {
        return 1;
    }
    let raw = unsafe { args_json.as_str().to_owned() };
    let envelope: Value = serde_json::from_str(&raw).unwrap_or(Value::Null);
    let root = envelope
        .get("args")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    let target = if root.is_empty() { "." } else { &root };
    let params = json!({ "root": target });
    let text = match simplify(&params) {
        Ok(report) => {
            let parsed: Value = serde_json::from_str(&report).unwrap_or(Value::Null);
            let scan = render_report(&parsed);
            let prompt = review_prompt(target, &scan);
            match submit_prompt(prompt) {
                Ok(()) => format!(
                    "Simplify review queued for `{target}`; press Enter to start it now, or edit the prompt first.\n\n{scan}"
                ),
                Err(error) => format!("Unable to queue the simplify review: {error}\n\n{scan}"),
            }
        }
        Err(error) => format!("simplify failed: {error}"),
    };
    unsafe {
        *out = StbString::from_string(json!({"kind":"message","text":text}).to_string());
    }
    0
}

/// Hand the review prompt to the prompt editor instead of sending it directly.
///
/// `mode: "replace"` keeps repeated `/simplify` runs from stacking prompts, and
/// the auto-send delay gives the user a window to cancel with Esc or edit the
/// text before it is submitted.
fn submit_prompt(prompt: String) -> Result<(), String> {
    call_runtime(
        RuntimeActionId::SetEditorText,
        json!({"text": prompt, "mode": "replace", "autoSendMs": AUTO_SEND_MS}),
    )
    .map(|_| ())
}

/// Grace period before the queued draft is submitted by the host.
const AUTO_SEND_MS: u64 = 3000;

/// Prompt handed to the model so `/simplify` reads the code rather than only
/// echoing the heuristic scan.
fn review_prompt(root: &str, scan: &str) -> String {
    format!(
        "Review the code under `{root}` for genuine simplification opportunities.\n\n\
         Read the relevant files yourself before judging: the scan below is a cheap heuristic \
         (long lines, adjacent duplicate lines, TODO/FIXME markers) and is not a review.\n\n\
         Report concrete, actionable findings only — dead code, duplicated logic worth \
         extracting, over-complex control flow, redundant abstractions. For each finding, cite \
         `path:line` and describe the smallest change that simplifies it. Call out false \
         positives from the scan and say so when the code is already simple. Do not edit files.\n\n\
         Heuristic scan:\n{scan}"
    )
}

extern "C" fn execute(
    _: StbStringRef,
    params: StbString,
    free: Option<FreeStringFn>,
) -> StepHandle {
    let t = params.to_string_lossy();
    params.free_with(free);
    Box::into_raw(Box::new(Drive {
        params: serde_json::from_str(&t).unwrap_or(Value::Null),
        cancelled: AtomicBool::new(false),
        done: false,
    })) as StepHandle
}
extern "C" fn poll(h: StepHandle, _: Option<ToolPartialCb>, _: *mut c_void) -> StepResult {
    if h.is_null() {
        return StepResult::err(StbString::from_string("null simplify handle".into()));
    }
    let d = unsafe { &mut *(h as *mut Drive) };
    if d.cancelled.load(Ordering::SeqCst) {
        return StepResult::err(StbString::from_string("simplify cancelled".into()));
    }
    if d.done {
        return StepResult::err(StbString::from_string(
            "simplify polled after completion".into(),
        ));
    }
    d.done = true;
    match simplify(&d.params) {
        Ok(t) => StepResult::done(StbString::from_string(
            json!({"content":[{"type":"text","text":t}]}).to_string(),
        )),
        Err(e) => StepResult::err(StbString::from_string(e)),
    }
}
extern "C" fn cancel(h: StepHandle) {
    if !h.is_null() {
        unsafe {
            (&*(h as *mut Drive))
                .cancelled
                .store(true, Ordering::SeqCst);
        }
    }
}
extern "C" fn destroy(h: StepHandle) {
    if !h.is_null() {
        unsafe {
            drop(Box::from_raw(h as *mut Drive));
        }
    }
}
extern "C" fn free_string(s: StbString) {
    if !s.is_empty() && !s.ptr.is_null() {
        unsafe {
            let b = std::slice::from_raw_parts(s.ptr as *const u8, s.len);
            let _ = Box::from_raw(b as *const [u8] as *mut [u8]);
        }
    }
}
#[no_mangle]
pub extern "C" fn rpi_plugin_register(api: *const PluginApi) -> i32 {
    unsafe {
        register_entrypoint(api, |api| {
            let _ = RUNTIME.set(Runtime {
                action: api.runtime_action,
                free_string: api.free_string,
                user_data: api.user_data,
            });
            let Some(register) = api.register_tool else {
                return 1;
            };
            let schema = Box::new(StableToolSchema {
                name: StbString::from_string("simplify".into()),
                description: StbString::from_string(
                    "Review local source for simple complexity and duplication findings.".into(),
                ),
                parameters: StbString::from_string(
                    r#"{"type":"object","properties":{"root":{"type":"string"}},"required":[]}"#
                        .into(),
                ),
            });
            let rc = register(&*schema, execute, poll, cancel, destroy, free_string);
            drop(schema);
            if rc != 0 {
                return rc;
            }
            if let Some(register_command) = api.register_command {
                let _ = register_command(
                    StbStringRef::from_str("simplify"),
                    StbStringRef::from_str(
                        "Review the project for simplification opportunities: /simplify [path]",
                    ),
                    simplify_command,
                );
            }
            0
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    thread_local! {
        /// Records every runtime action this test thread dispatched, so parallel
        /// tests never see each other's calls.
        static SENT: std::cell::RefCell<Vec<(u32, Value)>> = const { std::cell::RefCell::new(Vec::new()) };
    }

    fn sent_actions() -> Vec<(u32, Value)> {
        SENT.with(|sent| sent.borrow().clone())
    }

    extern "C" fn fake_action(
        action_id: u32,
        args_json: StbStringRef,
        out: *mut StbString,
        _: *mut c_void,
    ) -> i32 {
        if out.is_null() {
            return 1;
        }
        let raw = unsafe { args_json.as_str().to_owned() };
        let value: Value = serde_json::from_str(&raw).unwrap_or(Value::Null);
        SENT.with(|sent| sent.borrow_mut().push((action_id, value)));
        unsafe { *out = StbString::from_string("{}".to_string()) }
        0
    }

    extern "C" fn fake_free_string(s: StbString) {
        if !s.is_empty() && !s.ptr.is_null() {
            unsafe {
                drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                    s.ptr as *mut u8,
                    s.len,
                )));
            }
        }
    }

    /// Install the fake host callbacks once for the whole test binary.
    fn install_test_runtime() {
        let _ = TEST_RUNTIME.set(Runtime {
            action: fake_action,
            free_string: fake_free_string,
            user_data: std::ptr::null_mut(),
        });
    }

    /// Run `/simplify <args>` and return the reply text.
    fn run_command(args: &str) -> String {
        install_test_runtime();
        let input_json = json!({"args":args,"command":"simplify"}).to_string();
        let input = StbStringRef::from_str(&input_json);
        let mut out = StbString::empty();
        assert_eq!(simplify_command(input, &mut out, std::ptr::null_mut()), 0);
        let value: Value = serde_json::from_str(&out.to_string_lossy()).unwrap();
        assert_eq!(value["kind"], "message");
        let text = value["text"].as_str().unwrap().to_string();
        unsafe {
            drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                out.ptr as *mut u8,
                out.len,
            )));
        }
        text
    }

    #[test]
    fn missing_root_is_error() {
        assert!(simplify(&json!({"root":"x-does-not-exist"})).is_err());
    }

    #[test]
    fn renders_findings_as_a_markdown_table() {
        let report = simplify(&json!({"root": "."})).unwrap();
        let text = render_report(&serde_json::from_str(&report).unwrap());
        assert!(text.contains("**simplify**"), "{text}");
        assert!(text.contains("reviewed"), "{text}");
    }

    #[test]
    fn command_queues_a_review_draft_without_blocking() {
        let text = run_command("");
        assert!(text.contains("review queued"), "{text}");
        assert!(text.contains("**simplify**"), "{text}");
        let sent = sent_actions();
        assert_eq!(sent.len(), 1, "expected exactly one action: {sent:?}");
        let (action_id, args) = &sent[0];
        // `SetEditorText` (19), never `SendUserMessage` (1): the latter would
        // park the TUI key-loop thread for the whole agent turn.
        assert_eq!(*action_id, 19, "must not send the prompt directly");
        assert_eq!(args["mode"], "replace");
        assert!(args["autoSendMs"].as_u64().unwrap() > 0);
        let prompt = args["text"].as_str().unwrap();
        assert!(prompt.contains("Review the code under"), "{prompt}");
        assert!(prompt.contains("Do not edit files"), "{prompt}");
    }

    #[test]
    fn submit_prompt_targets_the_editor() {
        install_test_runtime();
        submit_prompt("draft body".to_string()).unwrap();
        let sent = sent_actions();
        let (action_id, args) = sent.last().unwrap();
        assert_eq!(*action_id, u32::from(RuntimeActionId::SetEditorText));
        assert_eq!(args["text"], "draft body");
    }

    #[test]
    fn command_reports_a_missing_root_instead_of_failing() {
        let text = run_command("x-does-not-exist");
        assert!(text.contains("simplify failed"), "{text}");
    }

    #[test]
    fn review_prompt_uses_the_requested_root() {
        let prompt = review_prompt("src", "scan body");
        assert!(prompt.contains("`src`"), "{prompt}");
        assert!(prompt.contains("scan body"), "{prompt}");
    }
}
