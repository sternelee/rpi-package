use std::ffi::c_void;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

mod store;

use rpi_plugin_sdk::{
    register_entrypoint, FreeStringFn, PluginApi, StbString, StbStringRef, StepHandle,
    StepResult, ToolPartialCb,
};
use serde_json::{json, Value};

struct Drive {
    params: Value,
    cancelled: AtomicBool,
    completed: bool,
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn path(params: &Value) -> PathBuf {
    let root = params
        .get("cwd")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    root.join(".rpi").join("memory.json")
}
fn load(file: &PathBuf) -> Result<Vec<Value>, String> {
    if !file.exists() {
        return Ok(Vec::new());
    }
    serde_json::from_str(&fs::read_to_string(file).map_err(|e| format!("read memory store: {e}"))?)
        .map_err(|e| format!("parse memory store: {e}"))
}
fn save(file: &PathBuf, items: &[Value]) -> Result<(), String> {
    if let Some(parent) = file.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create memory directory: {e}"))?;
    }
    let text = serde_json::to_string_pretty(items).map_err(|e| e.to_string())?;
    store::write_atomically(file, &text, "memory store")
}

fn words(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|v| v.len() > 1)
        .map(str::to_string)
        .collect()
}
fn memory(params: &Value) -> Result<String, String> {
    let file = path(params);
    let mut items = load(&file)?;
    let action = params
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or("list");
    match action {
        "remember" => {
            let text = params
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim();
            if text.is_empty() || text.len() > 10000 {
                return Err("text must contain 1-10000 characters".into());
            }
            let id = items
                .iter()
                .filter_map(|v| v.get("id").and_then(Value::as_u64))
                .max()
                .unwrap_or(0)
                + 1;
            let tags = params
                .get("tags")
                .cloned()
                .filter(|v| v.is_array())
                .unwrap_or_else(|| json!([]));
            items
                .push(json!({"id":id,"text":text,"tags":tags,"createdAt":now(),"updatedAt":now()}));
            save(&file, &items)?;
            Ok(json!({"action":"remember","id":id}).to_string())
        }
        "recall" => {
            let query = params.get("query").and_then(Value::as_str).unwrap_or("");
            if query.trim().is_empty() {
                return Err("query is required".into());
            }
            let q = words(query);
            let mut scored: Vec<(usize, Value)> = items
                .into_iter()
                .filter_map(|item| {
                    let text = item.get("text").and_then(Value::as_str).unwrap_or("");
                    let hay = words(text);
                    let score = q.iter().filter(|word| hay.contains(word)).count();
                    (score > 0).then_some((score, item))
                })
                .collect();
            scored.sort_by_key(|(score, _)| std::cmp::Reverse(*score));
            let limit = params
                .get("limit")
                .and_then(Value::as_u64)
                .unwrap_or(10)
                .clamp(1, 50) as usize;
            let results: Vec<Value> = scored
                .into_iter()
                .take(limit)
                .map(|(score, mut item)| {
                    item["score"] = json!(score);
                    item
                })
                .collect();
            Ok(json!({"action":"recall","query":query,"results":results}).to_string())
        }
        "forget" => {
            let id = params
                .get("id")
                .and_then(Value::as_u64)
                .ok_or("id is required")?;
            let before = items.len();
            items.retain(|v| v.get("id").and_then(Value::as_u64) != Some(id));
            if before == items.len() {
                return Err("memory item not found".into());
            }
            save(&file, &items)?;
            Ok(json!({"action":"forget","id":id}).to_string())
        }
        "clear" => {
            items.clear();
            save(&file, &items)?;
            Ok(json!({"action":"clear"}).to_string())
        }
        "list" => Ok(json!({"action":"list","items":items}).to_string()),
        _ => Err("action must be one of: remember, recall, list, forget, clear".into()),
    }
}

/// Render one memory item list as a Markdown bullet list.
fn render_items(items: &[Value]) -> String {
    if items.is_empty() {
        return "_No memories stored._\n".to_string();
    }
    let mut text = String::new();
    for item in items {
        let id = item.get("id").and_then(Value::as_u64).unwrap_or(0);
        let content = item.get("text").and_then(Value::as_str).unwrap_or("");
        text.push_str(&format!("- **#{id}** {content}"));
        let tags: Vec<&str> = item
            .get("tags")
            .and_then(Value::as_array)
            .map(|tags| tags.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        if !tags.is_empty() {
            text.push_str(&format!("  _(tags: {})_", tags.join(", ")));
        }
        text.push('\n');
    }
    text
}

/// Turn a `memory` result document into chat-friendly Markdown.
fn render_memory(report: &Value) -> String {
    match report.get("action").and_then(Value::as_str).unwrap_or("") {
        "remember" => format!(
            "Remembered as **#{}**.",
            report.get("id").and_then(Value::as_u64).unwrap_or(0)
        ),
        "recall" => {
            let query = report.get("query").and_then(Value::as_str).unwrap_or("");
            let results: Vec<Value> = report
                .get("results")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if results.is_empty() {
                format!("No memories matched `{query}`.")
            } else {
                format!("**recall `{query}`** — {} match(es)\n{}", results.len(), render_items(&results))
            }
        }
        "list" => {
            let items: Vec<Value> = report
                .get("items")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            format!("**memory store** — {} item(s)\n{}", items.len(), render_items(&items))
        }
        "forget" => format!(
            "Forgot **#{}**.",
            report.get("id").and_then(Value::as_u64).unwrap_or(0)
        ),
        "clear" => "Memory store cleared.".to_string(),
        _ => report.to_string(),
    }
}

const COMMAND_USAGE: &str =
    "Usage: /memory [list | remember <text> | recall <query> | forget <id> | clear]";

fn command_reply(out: *mut StbString, text: String) -> i32 {
    if out.is_null() {
        return 1;
    }
    unsafe {
        *out = StbString::from_string(json!({"kind":"message","text":text}).to_string());
    }
    0
}

/// `/memory` — the same store the tool uses, driven from the TUI.
extern "C" fn memory_command(args_json: StbStringRef, out: *mut StbString, _: *mut c_void) -> i32 {
    if out.is_null() {
        return 1;
    }
    let raw = unsafe { args_json.as_str().to_owned() };
    let envelope: Value = serde_json::from_str(&raw).unwrap_or(Value::Null);
    let args = envelope
        .get("args")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    let mut tokens = args.splitn(2, char::is_whitespace);
    let sub = tokens.next().unwrap_or("").to_lowercase();
    let rest = tokens.next().unwrap_or("").trim();
    let params = match sub.as_str() {
        "" | "list" => json!({"action": "list"}),
        "remember" | "add" => json!({"action": "remember", "text": rest}),
        "recall" | "search" => json!({"action": "recall", "query": rest}),
        "forget" | "remove" => match rest.parse::<u64>() {
            Ok(id) => json!({"action": "forget", "id": id}),
            Err(_) => {
                let text = format!("Usage: /memory forget <id> (got `{rest}`)\n\n{COMMAND_USAGE}");
                return command_reply(out, text);
            }
        },
        "clear" => json!({"action": "clear"}),
        _ => return command_reply(out, format!("Unknown subcommand `{sub}`.\n\n{COMMAND_USAGE}")),
    };
    let text = match memory(&params) {
        Ok(report) => render_memory(&serde_json::from_str(&report).unwrap_or(Value::Null)),
        Err(error) => format!("memory failed: {error}"),
    };
    command_reply(out, text)
}

extern "C" fn execute(
    _: StbStringRef,
    params: StbString,
    free: Option<FreeStringFn>,
) -> StepHandle {
    let text = params.to_string_lossy();
    params.free_with(free);
    Box::into_raw(Box::new(Drive {
        params: serde_json::from_str(&text).unwrap_or(Value::Null),
        cancelled: AtomicBool::new(false),
        completed: false,
    })) as StepHandle
}
extern "C" fn poll(handle: StepHandle, _: Option<ToolPartialCb>, _: *mut c_void) -> StepResult {
    if handle.is_null() {
        return StepResult::err(StbString::from_string("null memory handle".into()));
    }
    let drive = unsafe { &mut *(handle as *mut Drive) };
    if drive.cancelled.load(Ordering::SeqCst) {
        return StepResult::err(StbString::from_string("memory cancelled".into()));
    }
    if drive.completed {
        return StepResult::err(StbString::from_string(
            "memory polled after completion".into(),
        ));
    }
    drive.completed = true;
    match memory(&drive.params) {
        Ok(text) => StepResult::done(StbString::from_string(
            json!({"content":[{"type":"text","text":text}]}).to_string(),
        )),
        Err(e) => StepResult::err(StbString::from_string(e)),
    }
}
extern "C" fn cancel(handle: StepHandle) {
    if !handle.is_null() {
        unsafe {
            (&*(handle as *mut Drive))
                .cancelled
                .store(true, Ordering::SeqCst);
        }
    }
}
extern "C" fn destroy(handle: StepHandle) {
    if !handle.is_null() {
        unsafe {
            drop(Box::from_raw(handle as *mut Drive));
        }
    }
}
extern "C" fn free_string(s: StbString) {
    if !s.is_empty() && !s.ptr.is_null() {
        unsafe {
            let slice = std::slice::from_raw_parts(s.ptr as *const u8, s.len);
            let _ = Box::from_raw(slice as *const [u8] as *mut [u8]);
        }
    }
}

#[no_mangle]
pub extern "C" fn rpi_plugin_register(api: *const PluginApi) -> i32 {
    unsafe { register_entrypoint(api, |api| {
        let Some(register) = api.register_tool else {
            return 1;
        };
        let schema=Box::new(rpi_plugin_sdk::StableToolSchema{name:StbString::from_string("memory".into()),description:StbString::from_string("Remember and recall project-local facts.".into()),parameters:StbString::from_string(r#"{"type":"object","properties":{"action":{"type":"string","enum":["remember","recall","list","forget","clear"]},"text":{"type":"string"},"query":{"type":"string"},"id":{"type":"integer"},"tags":{"type":"array"},"limit":{"type":"integer"},"cwd":{"type":"string"}}}"#.into())});
        let rc = register(&*schema, execute, poll, cancel, destroy, free_string);
        drop(schema);
        if rc != 0 {
            return rc;
        }
        if let Some(register_command) = api.register_command {
            let _ = register_command(
                StbStringRef::from_str("memory"),
                StbStringRef::from_str(
                    "Remember and recall project-local facts in `.rpi/memory.json`",
                ),
                memory_command,
            );
        }
        0
    }) }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scores_overlap() {
        assert_eq!(words("Rust memory"), vec!["rust", "memory"]);
        assert!(memory(&json!({"action":"wat","cwd":"."})).is_err());
    }

    /// Run `/memory <args>` against a temp project and return the reply text.
    fn run_command(cwd: &str, args: &str) -> String {
        let input_json = json!({"args":args,"command":"memory"}).to_string();
        let input = StbStringRef::from_str(&input_json);
        let mut out = StbString::empty();
        assert_eq!(memory_command(input, &mut out, std::ptr::null_mut()), 0);
        let value: Value = serde_json::from_str(&out.to_string_lossy()).unwrap();
        assert_eq!(value["kind"], "message");
        let text = value["text"].as_str().unwrap().to_string();
        unsafe {
            drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                out.ptr as *mut u8,
                out.len,
            )));
        }
        let _ = cwd;
        text
    }

    #[test]
    fn command_round_trips_through_the_store() {
        let dir = std::env::temp_dir().join(format!("rpi-memory-cmd-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let previous = std::env::current_dir().unwrap();
        std::env::set_current_dir(&dir).unwrap();

        let remembered = run_command(".", "remember the deploy script lives in scripts/deploy.sh");
        assert!(remembered.contains("Remembered as **#1**"), "{remembered}");

        let recalled = run_command(".", "recall deploy");
        assert!(recalled.contains("deploy script"), "{recalled}");

        let listed = run_command(".", "list");
        assert!(listed.contains("1 item(s)"), "{listed}");

        let forgotten = run_command(".", "forget 1");
        assert!(forgotten.contains("Forgot **#1**"), "{forgotten}");

        let cleared = run_command(".", "clear");
        assert!(cleared.contains("cleared"), "{cleared}");

        std::env::set_current_dir(previous).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn command_reports_usage_errors() {
        assert!(run_command(".", "bogus").contains("Unknown subcommand"));
        assert!(run_command(".", "forget not-a-number").contains("Usage: /memory forget"));
        assert!(run_command(".", "remember").contains("memory failed"));
    }
}
