//! `/mcp` command surface: inspect and manage configured MCP servers.
//!
//! The host registers extension commands as `/name`, so this module backs both
//! `/mcp` and its `/mcp-adapter` alias. A command handler is synchronous and
//! returns one UI response object to the host's TUI:
//!
//! - `{"kind":"message","text":"..."}` renders a note;
//! - `{"kind":"selector","title","items":[{value,label,description}]}` opens a
//!   picker whose selection re-invokes the handler with
//!   `{"action":"select","value":"<the item value>"}`;
//! - `{"kind":"editor","title","initialText"}` opens the multiline editor, whose
//!   submission re-invokes the handler with `{"action":"edit","text":"..."}`.
//!
//! Selector values encode the follow-up subcommand (`"tools docs"`), so a
//! continuation needs no server-side state. The one stateful flow, `/mcp edit`,
//! records its target path in [`PENDING_EDIT`] before opening the editor.
//!
//! Config discovery matches the reference adapter's spirit but stays close to
//! rpi's `.rpi/` layout: an explicit `--config`/`$RPI_MCP_CONFIG` wins, then
//! `<cwd>/.rpi/mcp.json`, `<cwd>/mcp.json`, `<cwd>/.mcp.json`, then the global
//! `~/.config/mcp/mcp.json`, `~/.rpi/agent/mcp.json`, `~/.rpi/mcp.json`.

use crate::control::Control;
use crate::oauth::FlowView;
use crate::runtime;
use rpi_plugin_sdk::callbacks::Output;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

// `HOST_CONTEXT_KEY` is the key the tools read (`cwd`, `sessionId`). Commands do
// not receive it, so [`host_params`] synthesizes it from the process environment.
use rpi_plugin_sdk::HOST_CONTEXT_KEY;

/// Path an `/mcp edit` submission should be written back to. Set when the
/// editor opens; `None` (or a mismatched editor) is treated as a cancellation.
static PENDING_EDIT: Mutex<Option<PathBuf>> = Mutex::new(None);

/// Config a selector/input continuation should run against. A picker's items
/// encode only `"<sub> <server>"`, so the resolved path is carried here from
/// [`server_picker`] to the follow-up [`run_selection`].
static PENDING_CONFIG: Mutex<Option<PathBuf>> = Mutex::new(None);

const USAGE: &str = "\
MCP commands:
  /mcp                     show configured servers and their state
  /mcp list                list configured server names
  /mcp tools [server]      list a server's tools and argument schemas
  /mcp resources [server]  list a server's resources
  /mcp prompts [server]    list a server's prompt templates
  /mcp prompt <server> <name> [key=value ...]   load a prompt into the editor
  /mcp reconnect [server]  drop a connection so the next call reconnects
  /mcp close <server>      release a server connection now
  /mcp enable <server>     enable a server in the config
  /mcp disable <server>    disable a server in the config
  /mcp add <name> <command> [args ...]   add a stdio server
  /mcp add-url <name> <url>              add a streamable HTTP server
  /mcp remove <server>     delete a server from the config
  /mcp edit                open the config file in the editor and save on submit
  /mcp setup               create an empty config if none exists
  /mcp help                show this message
Options:
  --config <path>   use a specific config file (also $RPI_MCP_CONFIG)";

// ---------------------------------------------------------------------------
// ABI handler
// ---------------------------------------------------------------------------

rpi_plugin_sdk::string_handler! {
/// `/mcp` command handler. `raw` is the host envelope
/// `{"command":"/mcp","args":"..."}` (the args string is itself JSON for an
/// interactive continuation).
pub(crate) fn mcp_command(raw, out, _data) {
    let envelope: Value = serde_json::from_str(raw).unwrap_or(Value::Null);
    let args = envelope
        .get("args")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    let response = run(args);
    write_output(out, response)
}
}

fn write_output(out: &mut Output<'_>, value: Value) -> i32 {
    if !out.is_available() {
        return 1;
    }
    out.write(value.to_string());
    0
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub(crate) fn run(args: &str) -> Value {
    // A continuation arrives as a JSON object string in `args`.
    if args.starts_with('{') {
        if let Ok(value) = serde_json::from_str::<Value>(args) {
            if let Some(action) = value.get("action").and_then(Value::as_str) {
                return continuation(action, &value);
            }
        }
    }
    let (config_override, rest) = split_config_option(args);
    let config = resolve_config(config_override.as_deref());
    dispatch(&config, &rest)
}

fn continuation(action: &str, value: &Value) -> Value {
    match action {
        "select" => run_selection(value.get("value").and_then(Value::as_str).unwrap_or("")),
        "input" => run_selection(
            value
                .get("value")
                .or_else(|| value.get("text"))
                .and_then(Value::as_str)
                .unwrap_or(""),
        ),
        "edit" => save_edited_config(value.get("text").and_then(Value::as_str).unwrap_or("")),
        "cancel" => {
            *PENDING_EDIT.lock().unwrap_or_else(|p| p.into_inner()) = None;
            message("MCP command cancelled.")
        }
        _ => usage(),
    }
}

/// Re-run a picker's encoded subcommand. The selection string (`"tools docs"`)
/// does not repeat `--config`, so the resolved path is taken from
/// [`PENDING_CONFIG`]; a `--config` inside the selection itself still wins.
fn run_selection(selected: &str) -> Value {
    let (selected_override, rest) = split_config_option(selected);
    let carried = PENDING_CONFIG
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take();
    let config = match selected_override {
        Some(path) => absolute(PathBuf::from(path)),
        None => carried.unwrap_or_else(|| resolve_config(None)),
    };
    // A picker opened by `/mcp-oauth` encodes its origin so the continuation
    // returns to the OAuth dispatcher instead of the `/mcp` one.
    if let Some(rest) = rest.strip_prefix(OAUTH_PREFIX) {
        return oauth_dispatch(&config, rest);
    }
    dispatch(&config, &rest)
}

fn dispatch(config: &Path, rest: &str) -> Value {
    let mut tokens = rest.split_whitespace();
    let sub = tokens.next().unwrap_or("").to_lowercase();
    let positional: Vec<String> = tokens.map(str::to_string).collect();
    match sub.as_str() {
        "" | "status" | "panel" => show_status(config),
        "list" | "ls" | "servers" => list_servers(config),
        "tools" => show_tools(config, positional.first().map(String::as_str)),
        "resources" | "resource" => show_resources(config, positional.first().map(String::as_str)),
        "prompts" => show_prompts(config, positional.first().map(String::as_str)),
        "prompt" => load_prompt(config, &positional),
        "reconnect" | "close" => reconnect(config, positional.first().map(String::as_str), &sub),
        "enable" | "disable" => set_enabled(
            config,
            positional.first().map(String::as_str),
            sub == "enable",
        ),
        "add" => add_stdio(config, &positional),
        "add-url" | "addurl" => add_http(config, &positional),
        "remove" | "rm" | "delete" => remove_server(config, positional.first().map(String::as_str)),
        "edit" => edit_config(config),
        "setup" | "init" => setup(config),
        "help" | "-h" | "--help" => usage(),
        other => message(format!("Unknown MCP subcommand `{other}`.\n\n{USAGE}")),
    }
}

// ---------------------------------------------------------------------------
// Subcommands
// ---------------------------------------------------------------------------

fn show_status(config: &Path) -> Value {
    if !config.is_file() {
        return message(format!(
            "No MCP config at {}. Run `/mcp setup` or `/mcp add <name> <command>`.",
            config.display()
        ));
    }
    let params = host_params(config, None);
    match runtime::status(&params) {
        Ok(status) => {
            let path = status
                .get("config")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let servers = status
                .get("servers")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if servers.is_empty() {
                return message(format!(
                    "No MCP servers configured in {path}.\nRun `/mcp add <name> <command>` or `/mcp setup`."
                ));
            }
            let mut text = format!("MCP servers ({path})");
            for server in servers {
                let name = server.get("server").and_then(Value::as_str).unwrap_or("?");
                let transport = server
                    .get("transport")
                    .and_then(Value::as_str)
                    .unwrap_or("?");
                let enabled = server
                    .get("enabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let connected = server
                    .get("connected")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let state = if !enabled {
                    "disabled"
                } else if connected {
                    "connected"
                } else {
                    "idle"
                };
                text.push_str(&format!("\n  {name:<20} {transport:<6} {state}"));
            }
            text.push_str("\n\nRun `/mcp tools <server>` to inspect a server's tools.");
            message(text)
        }
        Err(error) => message(error),
    }
}

fn list_servers(config: &Path) -> Value {
    match config_server_names(config) {
        Ok(names) if names.is_empty() => message(format!(
            "No MCP servers configured in {}.",
            config.display()
        )),
        Ok(names) => message(format!(
            "MCP servers in {}:\n  {}",
            config.display(),
            names.join("\n  ")
        )),
        Err(error) => message(error),
    }
}

fn show_tools(config: &Path, server: Option<&str>) -> Value {
    let Some(server) = server else {
        return server_picker(config, "tools", "Select a server to list tools");
    };
    let params = host_params(config, Some(server));
    match runtime::list(&params, &Control::default()) {
        Ok(value) => {
            let tools = value
                .get("tools")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if tools.is_empty() {
                return message(format!("Server `{server}` exposes no tools."));
            }
            let mut text = format!("Tools on `{server}` ({}):", tools.len());
            for tool in tools {
                let name = tool.get("name").and_then(Value::as_str).unwrap_or("?");
                let description = tool
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .lines()
                    .next()
                    .unwrap_or("");
                let params = describe_parameters(tool.get("inputSchema"));
                text.push_str(&format!("\n  {name}"));
                if !description.is_empty() {
                    text.push_str(&format!(" — {description}"));
                }
                if !params.is_empty() {
                    text.push_str(&format!("\n      params: {params}"));
                }
            }
            message(text)
        }
        Err(error) => message(error),
    }
}

/// Render an MCP tool input schema's parameters compactly: `name*: type`,
/// required marked with `*`, enum members in parentheses and defaults appended.
fn describe_parameters(schema: Option<&Value>) -> String {
    let Some(properties) = schema
        .and_then(|schema| schema.get("properties"))
        .and_then(Value::as_object)
    else {
        return String::new();
    };
    let required: Vec<&str> = schema
        .and_then(|schema| schema.get("required"))
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let mut parts: Vec<String> = properties
        .iter()
        .map(|(name, spec)| {
            let mut text = name.clone();
            if required.contains(&name.as_str()) {
                text.push('*');
            }
            if let Some(kind) = schema_type(spec) {
                text.push_str(&format!(": {kind}"));
            }
            if let Some(values) = spec.get("enum").and_then(Value::as_array) {
                let joined = values
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .map(str::to_string)
                            .unwrap_or_else(|| value.to_string())
                    })
                    .collect::<Vec<_>>()
                    .join("|");
                text.push_str(&format!(" ({joined})"));
            }
            if let Some(default) = spec.get("default") {
                text.push_str(&format!(" = {}", compact_value(default)));
            }
            text
        })
        .collect();
    parts.sort();
    parts.join(", ")
}

/// The declared type, or the union of `anyOf`/`oneOf` branch types.
fn schema_type(spec: &Value) -> Option<String> {
    if let Some(kind) = spec.get("type").and_then(Value::as_str) {
        return Some(kind.to_string());
    }
    for key in ["anyOf", "oneOf"] {
        let Some(options) = spec.get(key).and_then(Value::as_array) else {
            continue;
        };
        let kinds: Vec<String> = options
            .iter()
            .filter_map(|option| {
                option
                    .get("type")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .collect();
        if !kinds.is_empty() {
            return Some(kinds.join("|"));
        }
    }
    None
}

fn compact_value(value: &Value) -> String {
    value
        .as_str()
        .map(|text| format!("{text:?}"))
        .unwrap_or_else(|| value.to_string())
}

fn show_resources(config: &Path, server: Option<&str>) -> Value {
    let Some(server) = server else {
        return server_picker(config, "resources", "Select a server to list resources");
    };
    let mut params = host_params(config, Some(server));
    params["action"] = json!("list");
    match runtime::resources(&params, &Control::default()) {
        Ok(value) => {
            let resources = value
                .get("resources")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if resources.is_empty() {
                return message(format!("Server `{server}` exposes no resources."));
            }
            let mut text = format!("Resources on `{server}` ({}):", resources.len());
            for resource in resources {
                let uri = resource.get("uri").and_then(Value::as_str).unwrap_or("?");
                let name = resource.get("name").and_then(Value::as_str).unwrap_or("");
                text.push_str(&format!("\n  {uri}"));
                if !name.is_empty() {
                    text.push_str(&format!(" — {name}"));
                }
            }
            message(text)
        }
        Err(error) => message(error),
    }
}

fn show_prompts(config: &Path, server: Option<&str>) -> Value {
    let Some(server) = server else {
        return server_picker(config, "prompts", "Select a server to list prompts");
    };
    let params = host_params(config, Some(server));
    match runtime::prompts(&params, &Control::default()) {
        Ok(value) => {
            let prompts = value
                .get("prompts")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if prompts.is_empty() {
                return message(format!("Server `{server}` exposes no prompts."));
            }
            let mut text = format!("Prompts on `{server}` ({}):", prompts.len());
            for prompt in prompts {
                let name = prompt.get("name").and_then(Value::as_str).unwrap_or("?");
                let description = prompt
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .lines()
                    .next()
                    .unwrap_or("");
                text.push_str(&format!("\n  {name}"));
                if !description.is_empty() {
                    text.push_str(&format!(" — {description}"));
                }
            }
            text.push_str(&format!(
                "\n\nLoad one with `/mcp prompt {server} <name> [key=value ...]`."
            ));
            message(text)
        }
        Err(error) => message(error),
    }
}

fn load_prompt(config: &Path, positional: &[String]) -> Value {
    let Some(server) = positional.first() else {
        return server_picker(config, "prompts", "Select a server to load a prompt from");
    };
    let Some(name) = positional.get(1) else {
        return message("Usage: /mcp prompt <server> <name> [key=value ...]");
    };
    let mut arguments = Map::new();
    for pair in &positional[2..] {
        match pair.split_once('=') {
            Some((key, value)) => {
                arguments.insert(key.to_string(), Value::String(value.to_string()));
            }
            None => return message(format!("Prompt argument `{pair}` must be key=value.")),
        }
    }
    let mut params = host_params(config, Some(server));
    params["prompt"] = json!(name);
    params["arguments"] = Value::Object(arguments);
    match runtime::prompt_get(&params, &Control::default()) {
        Ok(value) => {
            let result = value.get("result").cloned().unwrap_or(Value::Null);
            let text = prompt_to_text(&result);
            match crate::call_runtime(
                rpi_plugin_sdk::RuntimeActionId::SetEditorText,
                json!({"text": text, "mode": "replace"}),
            ) {
                Ok(_) => message(format!(
                    "Loaded prompt `{name}` from `{server}` into the editor."
                )),
                Err(error) => {
                    // The host has no editor (headless); show the text instead.
                    message(format!(
                        "Prompt `{name}` from `{server}`:\n\n{text}\n\n({error})"
                    ))
                }
            }
        }
        Err(error) => message(error),
    }
}

fn prompt_to_text(result: &Value) -> String {
    let mut text = String::new();
    if let Some(description) = result.get("description").and_then(Value::as_str) {
        text.push_str(description);
        text.push_str("\n\n");
    }
    if let Some(messages) = result.get("messages").and_then(Value::as_array) {
        for item in messages {
            let role = item.get("role").and_then(Value::as_str).unwrap_or("user");
            let content = item.get("content");
            let body = match content {
                Some(Value::Object(object)) => object
                    .get("text")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                Some(Value::Array(blocks)) => Some(
                    blocks
                        .iter()
                        .filter_map(|block| block.get("text").and_then(Value::as_str))
                        .collect::<Vec<_>>()
                        .join("\n"),
                ),
                _ => None,
            }
            .or_else(|| content.map(Value::to_string))
            .unwrap_or_default();
            text.push_str(&format!("[{role}] {body}\n"));
        }
    }
    text.trim_end().to_string()
}

fn reconnect(config: &Path, server: Option<&str>, sub: &str) -> Value {
    if let Some(server) = server {
        let params = host_params(config, Some(server));
        return match runtime::reconnect(&params, &Control::default()) {
            Ok(_) if sub == "close" => message(format!("Closed connection to `{server}`.")),
            Ok(_) => message(format!("Reconnecting `{server}` on its next use.")),
            Err(error) => message(error),
        };
    }
    // No server: close every configured, enabled connection for this session.
    match config_server_names(config) {
        Ok(names) => {
            let mut closed = 0;
            for name in &names {
                let params = host_params(config, Some(name));
                if runtime::reconnect(&params, &Control::default()).is_ok() {
                    closed += 1;
                }
            }
            message(format!(
                "Closed {closed} of {} configured server connection(s).",
                names.len()
            ))
        }
        Err(error) => message(error),
    }
}

fn set_enabled(config: &Path, server: Option<&str>, enabled: bool) -> Value {
    let Some(server) = server else {
        let verb = if enabled { "enable" } else { "disable" };
        return message(format!("Usage: /mcp {verb} <server>"));
    };
    match mutate_config(config, |value| {
        let servers = servers_mut(value)?;
        let Some(entry) = servers.get_mut(server).and_then(Value::as_object_mut) else {
            return Err(format!("MCP server `{server}` is not configured."));
        };
        entry.insert("enabled".into(), json!(enabled));
        Ok(())
    }) {
        Ok(()) => {
            let verb = if enabled { "Enabled" } else { "Disabled" };
            message(format!("{verb} `{server}` in {}.", config.display()))
        }
        Err(error) => message(error),
    }
}

fn add_stdio(config: &Path, positional: &[String]) -> Value {
    let Some(name) = positional.first() else {
        return message("Usage: /mcp add <name> <command> [args ...]");
    };
    let Some(command) = positional.get(1) else {
        return message("Usage: /mcp add <name> <command> [args ...]");
    };
    if let Err(error) = crate::config::validate_name(name) {
        return message(error);
    }
    let args = positional[2..].to_vec();
    let entry = json!({
        "command": command,
        "args": args,
        "enabled": true,
        "timeout": 60,
    });
    upsert(config, name, entry)
}

fn add_http(config: &Path, positional: &[String]) -> Value {
    let Some(name) = positional.first() else {
        return message("Usage: /mcp add-url <name> <url>");
    };
    let Some(url) = positional.get(1) else {
        return message("Usage: /mcp add-url <name> <url>");
    };
    if let Err(error) = crate::config::validate_name(name) {
        return message(error);
    }
    let entry = json!({"url": url, "enabled": true, "timeout": 60});
    upsert(config, name, entry)
}

fn upsert(config: &Path, name: &str, entry: Value) -> Value {
    let existed = config_server_names(config)
        .map(|names| names.iter().any(|n| n == name))
        .unwrap_or(false);
    match mutate_config(config, |value| {
        servers_mut(value)?.insert(name.to_string(), entry.clone());
        Ok(())
    }) {
        Ok(()) => {
            let verb = if existed { "Updated" } else { "Added" };
            message(format!(
                "{verb} MCP server `{name}` in {}.",
                config.display()
            ))
        }
        Err(error) => message(error),
    }
}

fn remove_server(config: &Path, server: Option<&str>) -> Value {
    let Some(server) = server else {
        return message("Usage: /mcp remove <server>");
    };
    match mutate_config(config, |value| {
        if servers_mut(value)?.remove(server).is_none() {
            return Err(format!("MCP server `{server}` is not configured."));
        }
        Ok(())
    }) {
        Ok(()) => message(format!("Removed `{server}` from {}.", config.display())),
        Err(error) => message(error),
    }
}

fn edit_config(config: &Path) -> Value {
    let text = match std::fs::read_to_string(config) {
        Ok(text) => text,
        Err(error) => {
            return message(format!(
                "Cannot read {}: {error}. Run `/mcp setup` first.",
                config.display()
            ))
        }
    };
    *PENDING_EDIT.lock().unwrap_or_else(|p| p.into_inner()) = Some(config.to_path_buf());
    json!({
        "kind": "editor",
        "title": format!("Editing {}", config.display()),
        "initialText": text,
        "placeholder": "{\"mcpServers\": { ... }}",
    })
}

fn save_edited_config(text: &str) -> Value {
    let path = PENDING_EDIT
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take();
    let Some(path) = path else {
        return message("No MCP config edit in progress.");
    };
    if text.trim().is_empty() {
        return message("Edit cancelled: empty config.");
    }
    let parsed: Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(error) => return message(format!("Invalid JSON: {error}")),
    };
    if !parsed.is_object() {
        return message("Config must be a JSON object.");
    }
    // Validate through the same parser the tools use, so an unloadable config
    // (bad server name, or an entry with neither `command` nor `url`) is never
    // written over a working one.
    if let Err(error) = crate::McpConfig::parse(text) {
        return message(format!("Invalid config: {error}"));
    }
    match write_json(&path, &parsed) {
        Ok(()) => message(format!("Saved {}.", path.display())),
        Err(error) => message(error),
    }
}

fn setup(config: &Path) -> Value {
    if config.is_file() {
        let count = config_server_names(config).map(|n| n.len()).unwrap_or(0);
        return message(format!(
            "Config already exists at {} ({count} server(s)). Use `/mcp edit` to change it.",
            config.display()
        ));
    }
    let value = json!({"mcpServers": {}});
    match write_json(config, &value) {
        Ok(()) => message(format!(
            "Created {}. Add a server with `/mcp add <name> <command>`.",
            config.display()
        )),
        Err(error) => message(error),
    }
}

// ---------------------------------------------------------------------------
// Config helpers
// ---------------------------------------------------------------------------

fn usage() -> Value {
    message(USAGE)
}

fn message(text: impl Into<String>) -> Value {
    json!({"kind": "message", "text": text.into()})
}

fn selector(title: &str, items: Vec<Value>) -> Value {
    json!({"kind": "selector", "title": title, "items": items})
}

fn server_picker(config: &Path, sub: &str, title: &str) -> Value {
    match config_server_names(config) {
        Ok(names) if names.is_empty() => message(format!(
            "No MCP servers configured in {}. Run `/mcp add <name> <command>`.",
            config.display()
        )),
        Ok(names) => {
            // Carry the resolved path into the follow-up selection, which only
            // encodes `"<sub> <server>"`.
            *PENDING_CONFIG.lock().unwrap_or_else(|p| p.into_inner()) = Some(config.to_path_buf());
            selector(
                title,
                names
                    .into_iter()
                    .map(|name| json!({"value": format!("{sub} {name}"), "label": name}))
                    .collect(),
            )
        }
        Err(error) => message(error),
    }
}

/// Split `--config <path>` / `--config=<path>` out of the raw args.
fn split_config_option(args: &str) -> (Option<String>, String) {
    let mut tokens = args.split_whitespace();
    let mut config = None;
    let mut rest = Vec::new();
    while let Some(token) = tokens.next() {
        if token == "--config" {
            config = tokens.next().map(str::to_string);
        } else if let Some(value) = token.strip_prefix("--config=") {
            config = Some(value.to_string());
        } else {
            rest.push(token);
        }
    }
    (config, rest.join(" "))
}

fn resolve_config(explicit: Option<&str>) -> PathBuf {
    if let Some(path) = explicit {
        return absolute(PathBuf::from(path));
    }
    if let Ok(path) = std::env::var("RPI_MCP_CONFIG") {
        if !path.trim().is_empty() {
            return absolute(PathBuf::from(path));
        }
    }
    if let Some(path) = discover_config() {
        return path;
    }
    // No config yet: default to the project-local path so add/setup have a home.
    absolute(cwd().join(".rpi/mcp.json"))
}

fn discover_config() -> Option<PathBuf> {
    let cwd = cwd();
    for relative in [".rpi/mcp.json", "mcp.json", ".mcp.json"] {
        let candidate = cwd.join(relative);
        if candidate.is_file() {
            return Some(absolute(candidate));
        }
    }
    if let Some(home) = home_dir() {
        for relative in [
            ".config/mcp/mcp.json",
            ".rpi/agent/mcp.json",
            ".rpi/mcp.json",
        ] {
            let candidate = home.join(relative);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

fn cwd() -> PathBuf {
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
}

fn absolute(path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        cwd().join(path)
    }
}

/// Synthetic host context so command-driven connections share the tools' scope
/// (`cwd`/`sessionId`) and therefore the same persistent connections.
fn host_params(config: &Path, server: Option<&str>) -> Value {
    let mut context = Map::new();
    context.insert("cwd".into(), json!(cwd().to_string_lossy()));
    if let Ok(session) = std::env::var(rpi_plugin_sdk::SESSION_ID_ENV) {
        if !session.is_empty() {
            context.insert("sessionId".into(), json!(session));
        }
    }
    let mut params = Map::new();
    params.insert("configPath".into(), json!(config.to_string_lossy()));
    params.insert(HOST_CONTEXT_KEY.into(), Value::Object(context));
    if let Some(server) = server {
        params.insert("server".into(), json!(server));
    }
    Value::Object(params)
}

fn config_server_names(config: &Path) -> Result<Vec<String>, String> {
    if !config.is_file() {
        return Ok(Vec::new());
    }
    let value = read_json(config)?;
    Ok(value
        .get("mcpServers")
        .and_then(Value::as_object)
        .map(|servers| servers.keys().cloned().collect())
        .unwrap_or_default())
}

fn read_json(path: &Path) -> Result<Value, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let value: Value =
        serde_json::from_str(&text).map_err(|e| format!("parse {}: {e}", path.display()))?;
    if !value.is_object() {
        return Err(format!("{} must contain a JSON object", path.display()));
    }
    Ok(value)
}

fn servers_mut(value: &mut Value) -> Result<&mut Map<String, Value>, String> {
    let object = value
        .as_object_mut()
        .ok_or_else(|| "config must be a JSON object".to_string())?;
    if !object.contains_key("mcpServers") {
        object.insert("mcpServers".into(), Value::Object(Map::new()));
    }
    object
        .get_mut("mcpServers")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| "config `mcpServers` must be an object".to_string())
}

/// Read-modify-write the config file, creating parent directories as needed.
/// Unknown top-level fields are preserved because the edit works on raw JSON.
fn mutate_config(
    path: &Path,
    edit: impl FnOnce(&mut Value) -> Result<(), String>,
) -> Result<(), String> {
    let mut value = if path.is_file() {
        read_json(path)?
    } else {
        json!({"mcpServers": {}})
    };
    edit(&mut value)?;
    write_json(path, &value)
}

fn write_json(path: &Path, value: &Value) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create {}: {e}", parent.display()))?;
        }
    }
    let mut text =
        serde_json::to_string_pretty(value).map_err(|e| format!("serialize config: {e}"))?;
    text.push('\n');
    std::fs::write(path, text).map_err(|e| format!("write {}: {e}", path.display()))
}

// ---------------------------------------------------------------------------
// `/mcp-oauth`: sign-in management for remote (HTTP) servers
// ---------------------------------------------------------------------------

/// Leading token a `/mcp-oauth` picker prepends to its selection so the
/// continuation can tell the two command surfaces apart.
const OAUTH_PREFIX: &str = "/mcp-oauth ";

const OAUTH_USAGE: &str = "\
MCP OAuth commands:
  /mcp-oauth                    show sign-in state for every remote server
  /mcp-oauth login <server>     sign in (opens the browser; PKCE + loopback)
  /mcp-oauth logout <server>    forget the stored credential
  /mcp-oauth cancel <server>    stop a sign-in that is already in progress
  /mcp-oauth help               show this message
Options:
  --config <path>   use a specific config file (also $RPI_MCP_CONFIG)";

rpi_plugin_sdk::string_handler! {
/// `/mcp-oauth` command handler. Same host envelope as
/// [`mcp_command`](mcp_command): `{"command":"/mcp-oauth","args":"..."}`.
pub(crate) fn oauth_command(raw, out, _data) {
    let envelope: Value = serde_json::from_str(raw).unwrap_or(Value::Null);
    let args = envelope
        .get("args")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    let response = run_oauth(args);
    write_output(out, response)
}
}

/// Run the `/mcp-oauth` surface without a TUI (embedders and tests). `args` is
/// the argument string after `/mcp-oauth`.
pub fn run_oauth(args: &str) -> Value {
    // A continuation arrives as a JSON object string in `args`.
    if args.starts_with('{') {
        if let Ok(value) = serde_json::from_str::<Value>(args) {
            if let Some(action) = value.get("action").and_then(Value::as_str) {
                return continuation(action, &value);
            }
        }
    }
    let (config_override, rest) = split_config_option(args);
    let config = resolve_config(config_override.as_deref());
    oauth_dispatch(&config, &rest)
}

fn oauth_dispatch(config: &Path, rest: &str) -> Value {
    let mut tokens = rest.split_whitespace();
    let sub = tokens.next().unwrap_or("").to_lowercase();
    let server = tokens.next();
    match sub.as_str() {
        "" | "status" | "list" => oauth_status(config),
        "login" | "auth" | "signin" => oauth_login(config, server),
        "logout" | "signout" => oauth_logout(config, server),
        "cancel" => oauth_cancel(config, server),
        "help" | "-h" | "--help" => message(OAUTH_USAGE),
        other => message(format!(
            "Unknown MCP OAuth subcommand `{other}`.\n\n{OAUTH_USAGE}"
        )),
    }
}

/// The configured remote servers (`url` instead of `command`), which are the
/// only ones that can carry an OAuth credential. Ordered by name so listings
/// are stable.
fn http_servers(config: &Path) -> Result<Vec<(String, String)>, String> {
    if !config.is_file() {
        return Ok(Vec::new());
    }
    let value = read_json(config)?;
    let mut servers = Vec::new();
    if let Some(map) = value.get("mcpServers").and_then(Value::as_object) {
        for (name, entry) in map {
            if let Some(url) = entry.get("url").and_then(Value::as_str) {
                servers.push((name.clone(), url.to_string()));
            }
        }
    }
    servers.sort();
    Ok(servers)
}

/// The URL of the configured remote server `server`, read from `config` (or the
/// discovered config when `config` is `None`).
pub(crate) fn server_url_in(config: Option<&Path>, server: &str) -> Result<String, String> {
    let config = match config {
        Some(path) => path.to_path_buf(),
        None => resolve_config(None),
    };
    let servers = http_servers(&config)?;
    servers
        .iter()
        .find(|(name, _)| name == server)
        .map(|(_, url)| url.clone())
        .ok_or_else(|| {
            format!(
                "MCP server `{server}` is not a remote server in {}; OAuth applies to `url` servers",
                config.display()
            )
        })
}

/// The URL of a configured remote server in the discovered config. Used by the
/// OAuth module, which has no config argument of its own.
pub(crate) fn server_url(server: &str) -> Result<String, String> {
    server_url_in(None, server)
}

/// Names of the configured remote servers, in a stable order. Used at plugin
/// load to offer one OAuth provider per server.
pub(crate) fn oauth_servers() -> Vec<String> {
    http_servers(&resolve_config(None))
        .map(|servers| servers.into_iter().map(|(name, _)| name).collect())
        .unwrap_or_default()
}

/// `12s`, `3m 20s`, `2h 5m` — coarse units, enough to judge a token's lifetime.
fn human_duration(seconds: i64) -> String {
    let seconds = seconds.max(0);
    let (hours, minutes, secs) = (seconds / 3600, (seconds % 3600) / 60, seconds % 60);
    if hours > 0 {
        format!("{hours}h {minutes}m")
    } else if minutes > 0 {
        format!("{minutes}m {secs}s")
    } else {
        format!("{secs}s")
    }
}

/// One line of credential state for `server`, from the host credential store.
fn credential_line(server: &str) -> String {
    match crate::oauth::credential_state(server) {
        (false, _) => format!("not signed in — run `/mcp-oauth login {server}`"),
        (true, None) => "signed in".to_string(),
        (true, Some(expires)) => {
            let remaining = expires - crate::oauth::now_epoch();
            if remaining > 0 {
                format!("signed in, expires in {}", human_duration(remaining))
            } else {
                "signed in, access token expired (it refreshes on the next call)".to_string()
            }
        }
    }
}

/// Lines describing a live or finished sign-in, appended under its server.
fn flow_lines(flow: &FlowView) -> Vec<String> {
    if flow.result.is_none() {
        let mut lines = vec![format!("sign-in in progress: {}", flow.stage)];
        if let Some(url) = &flow.url {
            lines.push(format!("open this URL if the browser did not: {url}"));
        }
        if flow.pending_input {
            lines.push("a code is needed; run `rpi auth login <server>` to enter it".to_string());
        }
        return lines;
    }
    match (&flow.failed, &flow.result) {
        (true, Some(reason)) => vec![format!("last sign-in failed: {reason}")],
        _ => vec!["last sign-in succeeded".to_string()],
    }
}

fn oauth_status(config: &Path) -> Value {
    let servers = match http_servers(config) {
        Ok(servers) => servers,
        Err(error) => return message(error),
    };
    if servers.is_empty() {
        return message(format!(
            "No remote (HTTP) MCP servers are configured in {}. Add one with `/mcp add-url <name> <url>`.",
            config.display()
        ));
    }
    let flows: BTreeMap<String, FlowView> = crate::oauth::statuses().into_iter().collect();
    let mut lines = vec![format!("MCP OAuth state ({})", config.display())];
    for (name, url) in &servers {
        lines.push(format!("  {name}  {}", credential_line(name)));
        lines.push(format!("    {url}"));
        if let Some(flow) = flows.get(name) {
            lines.extend(flow_lines(flow).into_iter().map(|line| format!("    {line}")));
        }
    }
    message(lines.join("\n"))
}

/// Start a sign-in, waiting briefly for the authorization URL so the message can
/// include it. The wait is bounded (1.5 s) because the host runs command
/// handlers on its TUI thread; the flow itself continues in the background.
fn oauth_login(config: &Path, server: Option<&str>) -> Value {
    let Some(server) = server else {
        return oauth_picker(config, "login", "Select an MCP server to sign in to");
    };
    if let Err(error) = server_url_in(Some(config), server) {
        return message(error);
    }
    if let Err(error) = crate::oauth::start(Some(config), server, true) {
        return message(error);
    }
    let url = wait_for_url(server, Duration::from_millis(1500));
    let mut text = format!(
        "Signing in to {server}. Complete the sign-in in your browser, then check `/mcp-oauth status`."
    );
    if let Some(url) = url {
        text.push_str(&format!("\nIf the browser did not open, visit: {url}"));
    }
    message(text)
}

/// Poll the flow registry until the authorization URL is known or the deadline
/// passes.
fn wait_for_url(server: &str, limit: Duration) -> Option<String> {
    let deadline = std::time::Instant::now() + limit;
    loop {
        if let Some(url) = crate::oauth::status(server).and_then(|flow| flow.url) {
            return Some(url);
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn oauth_logout(config: &Path, server: Option<&str>) -> Value {
    let Some(server) = server else {
        return oauth_picker(config, "logout", "Select an MCP server to sign out of");
    };
    match crate::oauth::logout(server) {
        Ok(text) => message(text),
        Err(error) => message(error),
    }
}

fn oauth_cancel(config: &Path, server: Option<&str>) -> Value {
    let Some(server) = server else {
        return oauth_picker(config, "cancel", "Select the sign-in to stop");
    };
    if crate::oauth::cancel(server) {
        message(format!("Stopped the sign-in to {server}."))
    } else {
        message(format!("No sign-in to {server} is in progress."))
    }
}

/// A picker over the configured remote servers, carrying the originating
/// command so the continuation does not have to guess it.
fn oauth_picker(config: &Path, sub: &str, title: &str) -> Value {
    match http_servers(config) {
        Err(error) => message(error),
        Ok(servers) if servers.is_empty() => message(format!(
            "No remote (HTTP) MCP servers are configured in {}. Add one with `/mcp add-url <name> <url>`.",
            config.display()
        )),
        Ok(servers) => {
            *PENDING_CONFIG.lock().unwrap_or_else(|p| p.into_inner()) = Some(config.to_path_buf());
            selector(
                title,
                servers
                    .into_iter()
                    .map(|(name, _)| {
                        json!({
                            "value": format!("{OAUTH_PREFIX}{sub} {name}"),
                            "label": name,
                        })
                    })
                    .collect(),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_config(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("rpi-mcp-cmd-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("mcp.json")
    }

    #[test]
    fn split_config_option_extracts_override() {
        let (config, rest) = split_config_option("--config /tmp/x.json tools docs");
        assert_eq!(config.as_deref(), Some("/tmp/x.json"));
        assert_eq!(rest, "tools docs");
        let (config, rest) = split_config_option("tools --config=/tmp/y.json docs");
        assert_eq!(config.as_deref(), Some("/tmp/y.json"));
        assert_eq!(rest, "tools docs");
    }

    #[test]
    fn add_then_disable_then_remove_round_trips() {
        let path = temp_config("roundtrip");
        let response = add_stdio(&path, &["docs".into(), "node".into(), "server.js".into()]);
        assert_eq!(response["kind"], "message");
        assert!(response["text"].as_str().unwrap().contains("Added"));
        let value = read_json(&path).unwrap();
        assert_eq!(value["mcpServers"]["docs"]["command"], "node");
        assert_eq!(value["mcpServers"]["docs"]["args"][0], "server.js");

        let response = set_enabled(&path, Some("docs"), false);
        assert!(response["text"].as_str().unwrap().contains("Disabled"));
        let value = read_json(&path).unwrap();
        assert_eq!(value["mcpServers"]["docs"]["enabled"], false);

        let response = remove_server(&path, Some("docs"));
        assert!(response["text"].as_str().unwrap().contains("Removed"));
        assert!(config_server_names(&path).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn add_http_preserves_unknown_fields() {
        let path = temp_config("preserve");
        write_json(&path, &json!({"mcpServers": {}, "other": 1})).unwrap();
        add_http(&path, &["remote".into(), "https://example.com/mcp".into()]);
        let value = read_json(&path).unwrap();
        assert_eq!(value["other"], 1);
        assert_eq!(
            value["mcpServers"]["remote"]["url"],
            "https://example.com/mcp"
        );
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn server_picker_encodes_the_follow_up_subcommand() {
        let path = temp_config("picker");
        add_stdio(&path, &["docs".into(), "node".into()]);
        let response = server_picker(&path, "tools", "Select a server");
        assert_eq!(response["kind"], "selector");
        assert_eq!(response["items"][0]["value"], "tools docs");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn continuation_select_replays_the_encoded_command() {
        let _serial = test_guard();
        // `tools missing` reaches runtime and fails cleanly with a message.
        let response = continuation("select", &json!({"value": "unknown-sub docs"}));
        assert_eq!(response["kind"], "message");
    }

    #[test]
    fn picker_carries_its_config_into_the_selection() {
        let _serial = test_guard();
        let path = temp_config("carry");
        add_stdio(&path, &["docs".into(), "node".into()]);
        let picker = server_picker(&path, "list", "Select a server");
        assert_eq!(picker["kind"], "selector");
        // The picker records the resolved path for the follow-up, which only
        // encodes `"<sub> <server>"`.
        assert_eq!(
            PENDING_CONFIG.lock().unwrap().as_deref(),
            Some(path.as_path())
        );
        // A continuation consumes it (so a later unrelated command cannot inherit it).
        let _ = run_selection("help");
        assert!(PENDING_CONFIG.lock().unwrap().is_none());
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn unknown_subcommand_and_help_are_messages() {
        let response = dispatch(Path::new("/nonexistent/mcp.json"), "nonsense");
        assert!(response["text"]
            .as_str()
            .unwrap()
            .contains("Unknown MCP subcommand"));
        let response = dispatch(Path::new("/nonexistent/mcp.json"), "help");
        assert!(response["text"].as_str().unwrap().contains("MCP commands"));
    }

    #[test]
    fn argv_without_tokens_defaults_to_status() {
        let path = temp_config("empty");
        let response = dispatch(&path, "");
        assert!(response["text"].as_str().unwrap().contains("No MCP config"));
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn describe_parameters_summarizes_types_enums_and_defaults() {
        let schema = json!({
            "type": "object",
            "required": ["path"],
            "properties": {
                "path": {"type": "string"},
                "format": {"type": "string", "enum": ["png", "jpeg"], "default": "png"},
                "size": {"anyOf": [{"type": "integer"}, {"type": "null"}]}
            }
        });
        let text = describe_parameters(Some(&schema));
        assert!(text.contains("path*: string"), "{text}");
        assert!(
            text.contains("format: string (png|jpeg) = \"png\""),
            "{text}"
        );
        assert!(text.contains("size: integer|null"), "{text}");
        assert_eq!(describe_parameters(None), "");
        assert_eq!(describe_parameters(Some(&json!({"type": "object"}))), "");
    }

    #[test]
    fn reconnect_reports_missing_and_disabled_servers() {
        let path = temp_config("reconnect");
        write_json(
            &path,
            &json!({"mcpServers": {"docs": {"command": "node", "enabled": false}}}),
        )
        .unwrap();
        let missing = reconnect(&path, Some("ghost"), "close");
        assert!(missing["text"].as_str().unwrap().contains("not configured"));
        let disabled = reconnect(&path, Some("docs"), "close");
        assert!(disabled["text"].as_str().unwrap().contains("disabled"));
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn add_twice_updates_the_existing_entry() {
        let path = temp_config("upsert");
        assert!(add_stdio(&path, &["docs".into(), "node".into()])["text"]
            .as_str()
            .unwrap()
            .contains("Added"));
        let updated = add_stdio(&path, &["docs".into(), "bun".into()]);
        assert!(updated["text"].as_str().unwrap().contains("Updated"));
        assert_eq!(
            read_json(&path).unwrap()["mcpServers"]["docs"]["command"],
            "bun"
        );
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn invalid_and_malformed_configs_surface_as_messages() {
        let path = temp_config("invalid");
        write_json(
            &path,
            &json!({"mcpServers": {"bad.name": {"command": "x"}}}),
        )
        .unwrap();
        let response = show_status(&path);
        assert!(
            response["text"].as_str().unwrap().contains("invalid"),
            "{response}"
        );

        std::fs::write(&path, "{not json").unwrap();
        // `show_status` goes through `McpConfig::parse`, whose error says the
        // config is invalid; `list_servers` reads raw JSON and reports a parse error.
        let response = show_status(&path);
        assert!(
            response["text"]
                .as_str()
                .unwrap()
                .contains("invalid mcp config"),
            "{response}"
        );
        let response = list_servers(&path);
        assert!(
            response["text"].as_str().unwrap().contains("parse"),
            "{response}"
        );
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn rpi_mcp_config_overrides_discovery() {
        let _serial = test_guard();
        let path = temp_config("env-override");
        let previous = std::env::var_os("RPI_MCP_CONFIG");
        std::env::set_var("RPI_MCP_CONFIG", path.to_string_lossy().as_ref());
        assert_eq!(resolve_config(None), path);
        match previous {
            Some(value) => std::env::set_var("RPI_MCP_CONFIG", value),
            None => std::env::remove_var("RPI_MCP_CONFIG"),
        }
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn edit_rejects_a_config_the_adapter_cannot_load() {
        let _serial = test_guard();
        let path = temp_config("edit-invalid");
        write_json(&path, &json!({"mcpServers": {}})).unwrap();
        let _ = edit_config(&path);
        // A server entry with neither `command` nor `url` cannot be loaded, so
        // the save is refused rather than replacing a working config.
        let response = save_edited_config(r#"{"mcpServers":{"x":{}}}"#);
        assert!(
            response["text"]
                .as_str()
                .unwrap()
                .contains("Invalid config"),
            "{response}"
        );
        assert!(config_server_names(&path).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    fn test_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The exact host path: an envelope in a borrowed `StbStringRef`, an
    /// owning output slot out. This exercises the `string_handler!` boundary
    /// (panic containment + `Output`) that `command_response` bypasses.
    #[test]
    fn abi_handler_parses_the_envelope_and_writes_the_ui_response() {
        let input = json!({"command": "/mcp", "args": "help"}).to_string();
        let mut out = rpi_plugin_sdk::StbString::empty();
        let rc = mcp_command(
            rpi_plugin_sdk::StbStringRef::from_str(&input),
            &mut out as *mut rpi_plugin_sdk::StbString,
            std::ptr::null_mut(),
        );
        assert_eq!(rc, 0);
        let text = out.to_string_lossy();
        rpi_plugin_sdk::callbacks::free_string(out);
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["kind"], "message");
        assert!(value["text"].as_str().unwrap().contains("MCP commands"));
    }

    #[test]
    fn abi_handler_rejects_a_null_output_slot() {
        let input = json!({"command": "/mcp", "args": "help"}).to_string();
        let rc = mcp_command(
            rpi_plugin_sdk::StbStringRef::from_str(&input),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );
        assert_eq!(rc, 1);
    }

    #[test]
    fn cancel_clears_a_pending_edit() {
        let _serial = test_guard();
        let path = temp_config("cancel");
        write_json(&path, &json!({"mcpServers": {}})).unwrap();
        let editor = edit_config(&path);
        assert_eq!(editor["kind"], "editor");
        assert!(PENDING_EDIT.lock().unwrap().is_some());
        let response = continuation("cancel", &json!({}));
        assert!(PENDING_EDIT.lock().unwrap().is_none());
        assert!(response["text"].as_str().unwrap().contains("cancelled"));
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn edit_continuation_without_a_pending_edit_is_reported() {
        let _serial = test_guard();
        *PENDING_EDIT.lock().unwrap() = None;
        let response = continuation("edit", &json!({"text": "{}"}));
        assert!(response["text"]
            .as_str()
            .unwrap()
            .contains("No MCP config edit"));
    }

    #[test]
    fn enable_and_disable_round_trip_through_the_config() {
        let path = temp_config("toggle");
        add_stdio(&path, &["docs".into(), "node".into()]);
        assert!(set_enabled(&path, Some("docs"), false)["text"]
            .as_str()
            .unwrap()
            .contains("Disabled"));
        assert_eq!(
            read_json(&path).unwrap()["mcpServers"]["docs"]["enabled"],
            false
        );
        // Enabling an unknown server reports rather than silently creating it.
        let missing = set_enabled(&path, Some("nope"), true);
        assert!(missing["text"].as_str().unwrap().contains("not configured"));
        assert!(read_json(&path).unwrap()["mcpServers"]["nope"].is_null());
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn remove_unknown_server_does_not_write_the_config() {
        let path = temp_config("remove-unknown");
        write_json(&path, &json!({"mcpServers": {}})).unwrap();
        let response = remove_server(&path, Some("ghost"));
        assert!(response["text"]
            .as_str()
            .unwrap()
            .contains("not configured"));
        assert!(config_server_names(&path).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn edit_saves_valid_json_and_rejects_invalid() {
        let _serial = test_guard();
        let path = temp_config("edit");
        write_json(&path, &json!({"mcpServers": {}})).unwrap();
        let _ = edit_config(&path);
        let response = save_edited_config(r#"{"mcpServers":{"x":{"command":"n"}}}"#);
        assert!(response["text"].as_str().unwrap().contains("Saved"));
        assert!(read_json(&path).unwrap()["mcpServers"]["x"].is_object());

        let _ = edit_config(&path);
        let response = save_edited_config("{not json");
        assert!(response["text"].as_str().unwrap().contains("Invalid JSON"));
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn setup_creates_and_reports_existing() {
        let path = temp_config("setup");
        let response = setup(&path);
        assert!(response["text"].as_str().unwrap().contains("Created"));
        assert!(path.is_file());
        let response = setup(&path);
        assert!(response["text"]
            .as_str()
            .unwrap()
            .contains("already exists"));
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn add_rejects_invalid_name() {
        let path = temp_config("badname");
        let response = add_stdio(&path, &["bad.name".into(), "node".into()]);
        assert!(response["text"].as_str().unwrap().contains("invalid"));
    }

    #[test]
    fn status_without_a_config_points_at_setup() {
        let path = temp_config("nostatus");
        let response = show_status(&path);
        assert_eq!(response["kind"], "message");
        assert!(response["text"].as_str().unwrap().contains("No MCP config"));
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    // --- /mcp-oauth -------------------------------------------------------

    /// A config with two remote servers and one stdio server.
    fn oauth_config(name: &str) -> PathBuf {
        let path = temp_config(name);
        write_json(
            &path,
            &json!({"mcpServers": {
                "docs": {"command": "node"},
                "remote-b": {"url": "https://b.example.com/mcp"},
                "remote-a": {"url": "https://a.example.com/mcp"},
            }}),
        )
        .unwrap();
        path
    }

    #[test]
    fn oauth_status_lists_remote_servers_and_skips_stdio_ones() {
        let path = oauth_config("oauth-status");
        let response = oauth_status(&path);
        assert_eq!(response["kind"], "message");
        let text = response["text"].as_str().unwrap().to_string();
        assert!(text.contains("remote-a"), "{text}");
        assert!(text.contains("remote-b"), "{text}");
        assert!(text.contains("https://a.example.com/mcp"), "{text}");
        // Only `url` servers can hold an OAuth credential.
        assert!(!text.contains("docs"), "{text}");
        // Listing order is by server name, not by config order.
        assert!(text.find("remote-a") < text.find("remote-b"), "{text}");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn oauth_status_without_remote_servers_points_at_add_url() {
        let path = temp_config("oauth-none");
        write_json(&path, &json!({"mcpServers": {"docs": {"command": "node"}}})).unwrap();
        let response = oauth_status(&path);
        assert_eq!(response["kind"], "message");
        let text = response["text"].as_str().unwrap();
        assert!(text.contains("No remote (HTTP) MCP servers"), "{text}");
        assert!(text.contains("/mcp add-url"), "{text}");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn oauth_help_and_unknown_subcommands_are_messages() {
        let path = oauth_config("oauth-help");
        let help = oauth_dispatch(&path, "help");
        assert_eq!(help["kind"], "message");
        let help = help["text"].as_str().unwrap();
        assert!(help.contains("/mcp-oauth login <server>"), "{help}");
        assert!(help.contains("PKCE + loopback"), "{help}");

        let unknown = oauth_dispatch(&path, "bogus");
        assert!(unknown["text"]
            .as_str()
            .unwrap()
            .contains("Unknown MCP OAuth subcommand `bogus`"));
        // The status aliases reach the same state view as the bare command.
        assert!(oauth_dispatch(&path, "status")["text"]
            .as_str()
            .unwrap()
            .contains("MCP OAuth state"));
        assert!(oauth_dispatch(&path, "list")["text"]
            .as_str()
            .unwrap()
            .contains("MCP OAuth state"));
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn run_oauth_accepts_a_config_override() {
        let path = temp_config("oauth-run");
        write_json(&path, &json!({"mcpServers": {"remote": {"url": "https://x.example.com/mcp"}}})).unwrap();
        let response = run_oauth(&format!("--config {} status", path.display()));
        assert_eq!(response["kind"], "message");
        let text = response["text"].as_str().unwrap();
        // The override is honoured, not the discovered config.
        assert!(text.contains(&path.display().to_string()), "{text}");
        assert!(text.contains("remote"), "{text}");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn server_url_in_only_accepts_remote_servers() {
        let path = oauth_config("oauth-url");
        assert_eq!(
            server_url_in(Some(&path), "remote-a").unwrap(),
            "https://a.example.com/mcp"
        );
        let stdio = server_url_in(Some(&path), "docs").unwrap_err();
        assert!(stdio.contains("not a remote server"), "{stdio}");
        assert!(server_url_in(Some(&path), "absent").is_err());
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn oauth_servers_lists_only_remote_names_in_order() {
        let path = oauth_config("oauth-servers");
        let names: Vec<String> = http_servers(&path)
            .unwrap()
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(names, vec!["remote-a", "remote-b"]);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn oauth_picker_encodes_the_origin_of_the_selection() {
        let path = oauth_config("oauth-picker");
        let response = oauth_picker(&path, "login", "Select a server to sign in");
        assert_eq!(response["kind"], "selector");
        assert_eq!(response["items"][0]["value"], "/mcp-oauth login remote-a");
        assert_eq!(response["items"][0]["label"], "remote-a");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn run_selection_routes_an_oauth_selection_back_to_its_dispatcher() {
        let _serial = test_guard();
        let path = oauth_config("oauth-route");
        // A `/mcp-oauth` picker records the config; the selection replays the
        // encoded subcommand through the OAuth dispatcher, not `/mcp`.
        let _ = oauth_picker(&path, "help", "Select a server");
        let response = run_selection("/mcp-oauth help");
        assert_eq!(response["kind"], "message");
        assert!(response["text"]
            .as_str()
            .unwrap()
            .contains("/mcp-oauth logout <server>"));
        // The carried config is consumed, so a later command cannot inherit it.
        assert!(PENDING_CONFIG.lock().unwrap().is_none());
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn human_duration_uses_coarse_units() {
        assert_eq!(human_duration(0), "0s");
        assert_eq!(human_duration(12), "12s");
        assert_eq!(human_duration(200), "3m 20s");
        assert_eq!(human_duration(7500), "2h 5m");
        // A negative remaining lifetime never renders as a negative duration.
        assert_eq!(human_duration(-5), "0s");
    }

    #[test]
    fn oauth_command_handler_parses_the_envelope() {
        let path = temp_config("oauth-envelope");
        write_json(&path, &json!({"mcpServers": {}})).unwrap();
        let input = json!({
            "command": "/mcp-oauth",
            "args": format!("--config {} status", path.display()),
        })
        .to_string();
        let mut out = rpi_plugin_sdk::StbString::empty();
        let rc = oauth_command(
            rpi_plugin_sdk::StbStringRef::from_str(&input),
            &mut out as *mut rpi_plugin_sdk::StbString,
            std::ptr::null_mut(),
        );
        assert_eq!(rc, 0);
        let text = out.to_string_lossy();
        rpi_plugin_sdk::callbacks::free_string(out);
        let response: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(response["kind"], "message");
        assert!(response["text"]
            .as_str()
            .unwrap()
            .contains("No remote (HTTP) MCP servers"));
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }
}
