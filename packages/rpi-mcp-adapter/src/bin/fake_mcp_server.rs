#![forbid(unsafe_code)]

use serde_json::{json, Value};
use std::io::{self, BufRead, Write};
fn main() {
    let modes: Vec<String> = std::env::args().collect();
    let mode = |name: &str| modes.iter().any(|arg| arg == name);
    if mode("--no-read") {
        std::thread::sleep(std::time::Duration::from_secs(60));
        return;
    }
    let mut descendant = None;
    if mode("--spawn-child") {
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--no-read")
            .stdin(std::process::Stdio::null())
            .spawn()
            .unwrap();
        eprintln!("descendant-pid={}", child.id());
        descendant = Some(child);
    }
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    let mut calls = 0;
    let mut subscribed = std::collections::BTreeSet::new();
    loop {
        let mut line = String::new();
        if input.read_line(&mut line).unwrap_or(0) == 0 {
            break;
        }
        let Ok(request) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let Some(id) = request.get("id").cloned() else {
            continue;
        };
        let Some(method) = request.get("method").and_then(Value::as_str) else {
            continue;
        };
        if mode("--hang")
            || mode("--spawn-child")
            || (mode("--hang-call") && method == "tools/call")
        {
            eprintln!("fake server deliberately blocked in {method}");
            std::thread::sleep(std::time::Duration::from_secs(60));
        }
        if mode("--oversize") {
            stdout.write_all(&vec![b'x'; 16 * 1024 * 1024 + 1]).unwrap();
            stdout.flush().unwrap();
            continue;
        }
        if mode("--notify") {
            writeln!(
                stdout,
                "{}",
                json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed"})
            )
            .unwrap();
            writeln!(stdout, "{}", json!({"jsonrpc":"2.0","id":999,"result":{}})).unwrap();
        }
        if mode("--ping") {
            writeln!(
                stdout,
                "{}",
                json!({"jsonrpc":"2.0","id":"server-ping","method":"ping"})
            )
            .unwrap();
            stdout.flush().unwrap();
            let mut reply = String::new();
            input.read_line(&mut reply).unwrap();
            let reply: Value = serde_json::from_str(&reply).unwrap();
            assert_eq!(reply["id"], "server-ping");
            assert_eq!(reply["result"], json!({}));
        }
        let result = match method {
            "initialize" => {
                json!({"protocolVersion":"2025-06-18","capabilities":{"tools":{},"resources":{},"prompts":{}},"serverInfo":{"name":"rpi-fake-mcp","version":"1"}})
            }
            "resources/list" => {
                if let Some(pages) = modes.iter().find_map(|arg| {
                    arg.strip_prefix("--resource-pages=")
                        .and_then(|count| count.parse::<usize>().ok())
                }) {
                    let page = request["params"]["cursor"]
                        .as_str()
                        .unwrap_or("0")
                        .parse::<usize>()
                        .unwrap();
                    let mut result = json!({"resources":[{"uri":format!("mem://page-{page}"),"name":format!("page-{page}")}]});
                    if page + 1 < pages {
                        result["nextCursor"] = json!((page + 1).to_string());
                    }
                    result
                } else {
                    json!({"resources":[{"uri":"mem://alpha","name":"alpha"}]})
                }
            }
            "resources/read" => {
                if mode("--drop-first-resource-read") {
                    let marker = std::env::var("RPI_FAKE_RESOURCE_MARKER").unwrap();
                    if !std::path::Path::new(&marker).exists() {
                        std::fs::write(marker, "dropped").unwrap();
                        break;
                    }
                }
                if mode("--hang-read") {
                    eprintln!("fake server deliberately blocked in resources/read");
                    std::thread::sleep(std::time::Duration::from_secs(60));
                }
                let uri = request["params"]["uri"].as_str().unwrap_or("mem://alpha");
                json!({"contents":[{"uri":uri,"text":format!("value-of-{uri}")}]})
            }
            "resources/subscribe" => {
                if mode("--reject-duplicate-subscribe")
                    && subscribed
                        .contains(request["params"]["uri"].as_str().unwrap_or("mem://alpha"))
                {
                    writeln!(stdout, "{}", json!({"jsonrpc":"2.0","id":id,"error":{"code":-32602,"message":"already subscribed"}})).unwrap();
                    stdout.flush().unwrap();
                    continue;
                }
                if mode("--reject-restored-subscribe") {
                    let marker = std::env::var("RPI_FAKE_RESOURCE_MARKER").unwrap();
                    if std::path::Path::new(&marker).exists() {
                        writeln!(stdout, "{}", json!({"jsonrpc":"2.0","id":id,"error":{"code":-32602,"message":"resource no longer exists"}})).unwrap();
                        stdout.flush().unwrap();
                        continue;
                    }
                    std::fs::write(marker, "subscribed").unwrap();
                }
                let uri = request["params"]["uri"].as_str().unwrap_or("mem://alpha");
                subscribed.insert(uri.to_string());
                // Push the first update *before* the subscribe response, which is
                // the ordering that used to lose it (the local subscription mark
                // was only written after the reply arrived).
                if mode("--notify-early") {
                    let uri = request["params"]["uri"].as_str().unwrap_or("mem://alpha");
                    writeln!(
                        stdout,
                        "{}",
                        json!({"jsonrpc":"2.0","method":"notifications/resources/updated","params":{"uri":uri}})
                    )
                    .unwrap();
                    stdout.flush().unwrap();
                }
                json!({})
            }
            "resources/unsubscribe" => {
                if mode("--reject-unsubscribe") {
                    writeln!(stdout, "{}", json!({"jsonrpc":"2.0","id":id,"error":{"code":-32603,"message":"unsubscribe temporarily failed"}})).unwrap();
                    stdout.flush().unwrap();
                    continue;
                }
                if let Some(uri) = request["params"]["uri"].as_str() {
                    subscribed.remove(uri);
                }
                json!({})
            }
            "tools/list" => {
                let echo = json!({"name":"echo","description":"Echo one value","inputSchema":{"type":"object","properties":{"value":{"type":"string"}}}});
                if mode("--paged") {
                    if request["params"]["cursor"] == "page2" {
                        json!({"tools":[{"name":"counter","inputSchema":{"type":"object"}},{"name":"fail","inputSchema":{"type":"object"}},{"name":"image","inputSchema":{"type":"object"}}]})
                    } else {
                        json!({"tools":[echo],"nextCursor":"page2"})
                    }
                } else if mode("--bad-cursor") {
                    json!({"tools":[echo],"nextCursor":"same"})
                } else {
                    json!({"tools":[echo]})
                }
            }
            "prompts/list" => {
                json!({"prompts":[{"name":"greet","description":"Greet someone","arguments":[{"name":"who","required":true}]}]})
            }
            "prompts/get" => {
                let who = request["params"]["arguments"]["who"]
                    .as_str()
                    .unwrap_or("world");
                json!({"description":"Greeting","messages":[{"role":"user","content":{"type":"text","text":format!("Hello {who}")}}]})
            }
            "tools/call" => {
                if mode("--stateful-resources") {
                    if request["params"]["arguments"]["value"] == "disconnect" {
                        break;
                    }
                    for uri in &subscribed {
                        writeln!(stdout, "{}", json!({"jsonrpc":"2.0","method":"notifications/resources/updated","params":{"uri":uri}})).unwrap();
                    }
                    stdout.flush().unwrap();
                }
                calls += 1;
                match request["params"]["name"].as_str().unwrap_or("") {
                    "counter" => {
                        json!({"content":[{"type":"text","text":format!("{}:{calls}", std::process::id())}]})
                    }
                    "fail" => {
                        json!({"isError":true,"content":[{"type":"text","text":"intentional failure"}]})
                    }
                    "image" => {
                        json!({"content":[{"type":"image","data":"aGVsbG8=","mimeType":"image/png"}],"structuredContent":{"ok":true}})
                    }
                    _ => {
                        json!({"content":[{"type":"text","text":format!("echo:{}",request["params"]["arguments"]["value"].as_str().unwrap_or(""))}]})
                    }
                }
            }
            _ => json!({}),
        };
        if writeln!(
            stdout,
            "{}",
            json!({"jsonrpc":"2.0","id":id,"result":result})
        )
        .and_then(|_| stdout.flush())
        .is_err()
        {
            break;
        }
    }
    if let Some(mut child) = descendant {
        let _ = child.kill();
        let _ = child.wait();
    }
}
