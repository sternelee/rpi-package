#![forbid(unsafe_code)]

use rpi_mcp_adapter::{call_from_config, discover_from_config};
use serde_json::json;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A unique config path per call. The system clock alone is not enough: parallel
/// tests in one process can read the same coarse timestamp, and then one test's
/// cleanup (`remove_file`) deletes another test's config.
fn config_path() -> std::path::PathBuf {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "rpi-mcp-test-{}-{suffix}-{sequence}.json",
        std::process::id()
    ))
}

fn write_config(path: &std::path::Path) {
    let command = env!("CARGO_BIN_EXE_fake_mcp_server");
    let config = json!({
        "mcpServers": {
            "fake": {
                "command": command,
                "enabled": true,
                "timeout": 5
            }
        }
    });
    fs::write(path, config.to_string()).unwrap();
}

#[test]
fn stdio_runtime_discovers_and_calls_tool() {
    let path = config_path();
    write_config(&path);

    let discovered = discover_from_config(path.to_str().unwrap(), "fake").unwrap();
    assert_eq!(discovered.name, "fake");
    assert_eq!(discovered.info.name, "rpi-fake-mcp");
    assert_eq!(discovered.tools.len(), 1);
    assert_eq!(discovered.tools[0].name, "echo");

    let result = call_from_config(
        path.to_str().unwrap(),
        "fake",
        "echo",
        json!({"value":"hello"}),
    )
    .unwrap();
    assert_eq!(result, "echo:hello");

    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    let _ = fs::remove_file(path);
}

#[test]
fn stdio_runtime_rejects_unknown_tool() {
    let path = config_path();
    write_config(&path);
    let error = call_from_config(path.to_str().unwrap(), "fake", "missing", json!({})).unwrap_err();
    assert!(error.contains("does not expose tool"));
    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    let _ = fs::remove_file(path);
}

#[test]
fn stdio_timeout_bounds_unresponsive_server() {
    let mut transport = rpi_mcp_adapter::StdioTransport::start(
        env!("CARGO_BIN_EXE_fake_mcp_server"),
        &["--hang".into()],
        &Default::default(),
        None,
        None,
    )
    .unwrap();
    transport.set_timeout(1);
    let started = std::time::Instant::now();
    let error = transport
        .request(&rpi_mcp_adapter::tools_list_request(1))
        .unwrap_err();
    assert!(
        error.contains("tools/list timed out after 1 seconds"),
        "{error}"
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
}

#[test]
fn stdio_skips_notifications_before_response() {
    let mut transport = rpi_mcp_adapter::StdioTransport::start(
        env!("CARGO_BIN_EXE_fake_mcp_server"),
        &["--notify".into()],
        &Default::default(),
        None,
        None,
    )
    .unwrap();
    transport.set_timeout(2);
    let response = transport
        .request(&rpi_mcp_adapter::tools_list_request(7))
        .unwrap();
    assert_eq!(response.id, 7);
}

fn config_with_mode(mode: &str) -> std::path::PathBuf {
    let path = config_path();
    fs::write(&path, json!({"mcpServers":{"fake":{"command":env!("CARGO_BIN_EXE_fake_mcp_server"),"args":[mode],"timeout":2}}}).to_string()).unwrap();
    path
}

fn config_with_modes(modes: &[&str]) -> std::path::PathBuf {
    config_with_modes_and_timeout(modes, 2)
}

fn config_with_modes_and_timeout(modes: &[&str], timeout: u64) -> std::path::PathBuf {
    let path = config_path();
    fs::write(&path, json!({"mcpServers":{"fake":{"command":env!("CARGO_BIN_EXE_fake_mcp_server"),"args":modes,"timeout":timeout}}}).to_string()).unwrap();
    path
}

/// The pending queue is process-global, so tests that inspect it must not run
/// concurrently with each other.
static RESOURCE_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn drain_updates_until(within: std::time::Duration) -> Vec<rpi_mcp_adapter::ResourceUpdate> {
    let deadline = std::time::Instant::now() + within;
    loop {
        let updates = rpi_mcp_adapter::take_resource_updates(None);
        if !updates.is_empty() || std::time::Instant::now() >= deadline {
            return updates;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// Regression: the local subscription mark used to be written *after* the
/// `subscribe` response, so a server that pushed the first
/// `notifications/resources/updated` while the handshake was still in flight had
/// that update dropped by the dispatcher.
#[test]
fn subscribe_keeps_an_update_pushed_before_the_response() {
    let _serial = RESOURCE_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let path = config_with_modes(&["--notify-early"]);
    let _ = rpi_mcp_adapter::take_resource_updates(None);

    let subscribed = rpi_mcp_adapter::resources_from_config(
        path.to_str().unwrap(),
        "fake",
        "subscribe",
        Some("mem://alpha"),
    )
    .unwrap();
    assert_eq!(subscribed["subscribed"], true);

    let updates = drain_updates_until(std::time::Duration::from_secs(5));
    assert_eq!(
        updates.len(),
        1,
        "the update pushed before the subscribe response was lost: {updates:?}"
    );
    assert!(updates[0].text.contains("value-of-mem://alpha"));

    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    let _ = fs::remove_file(path);
}

#[test]
fn unsubscribe_and_resubscribe_keep_resource_delivery_consistent() {
    let _serial = RESOURCE_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let path = config_with_modes(&["--notify-early"]);
    let _ = rpi_mcp_adapter::take_resource_updates(None);
    let resource = |action| {
        rpi_mcp_adapter::resources_from_config(
            path.to_str().unwrap(),
            "fake",
            action,
            Some("mem://alpha"),
        )
        .unwrap()
    };
    resource("subscribe");
    assert_eq!(
        drain_updates_until(std::time::Duration::from_secs(5)).len(),
        1
    );
    assert_eq!(resource("unsubscribe")["unsubscribed"], true);
    assert!(rpi_mcp_adapter::take_resource_updates(None).is_empty());
    assert_eq!(resource("subscribe")["subscribed"], true);
    let updates = drain_updates_until(std::time::Duration::from_secs(5));
    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    let _ = fs::remove_file(path);
    assert_eq!(
        updates.len(),
        1,
        "new subscription failed to deliver: {updates:?}"
    );
}

#[test]
fn resource_pagination_does_not_report_partial_results_as_complete() {
    for pages in [100, 101] {
        let mode = format!("--resource-pages={pages}");
        let path = config_with_mode(&mode);
        let result =
            rpi_mcp_adapter::resources_from_config(path.to_str().unwrap(), "fake", "list", None);
        rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
        let _ = fs::remove_file(path);
        if pages == 100 {
            let resources = result.unwrap()["resources"].as_array().unwrap().clone();
            assert_eq!(resources.len(), 100);
            assert_eq!(resources[99]["uri"], "mem://page-99");
        } else {
            assert!(result.unwrap_err().contains("exceeded 100 pages"));
        }
    }
}

#[test]
fn reconnect_restores_resource_subscriptions_without_replaying_failed_tools() {
    let _serial = RESOURCE_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let path = config_with_mode("--stateful-resources");
    let _ = rpi_mcp_adapter::take_resource_updates(None);
    rpi_mcp_adapter::resources_from_config(
        path.to_str().unwrap(),
        "fake",
        "subscribe",
        Some("mem://alpha"),
    )
    .unwrap();
    call_from_config(
        path.to_str().unwrap(),
        "fake",
        "echo",
        json!({"value":"tick"}),
    )
    .unwrap();
    assert_eq!(
        drain_updates_until(std::time::Duration::from_secs(3)).len(),
        1
    );

    // The server exits before replying. The failed tool is not retried, and the
    // next call must reconnect and restore the existing resource subscription.
    let disconnected = call_from_config(
        path.to_str().unwrap(),
        "fake",
        "echo",
        json!({"value":"disconnect"}),
    );
    assert!(disconnected.is_err());
    let result = call_from_config(
        path.to_str().unwrap(),
        "fake",
        "echo",
        json!({"value":"tick"}),
    );
    let updates = drain_updates_until(std::time::Duration::from_secs(1));
    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    let _ = fs::remove_file(path);
    assert_eq!(result.unwrap(), "echo:tick");
    assert_eq!(
        updates.len(),
        1,
        "connection recovered but resource subscription was lost"
    );
}

fn resource_echo(path: &std::path::Path, value: &str) -> Result<String, String> {
    call_from_config(
        path.to_str().unwrap(),
        "fake",
        "echo",
        json!({"value":value}),
    )
}

#[test]
fn repeated_subscribe_reuses_the_active_remote_subscription() {
    let _serial = RESOURCE_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let path = config_with_modes(&["--stateful-resources", "--reject-duplicate-subscribe"]);
    let _ = rpi_mcp_adapter::take_resource_updates(None);
    subscribe_alpha(&path);
    resource_echo(&path, "tick").unwrap();
    assert_eq!(
        drain_updates_until(std::time::Duration::from_secs(3)).len(),
        1
    );
    let repeated = rpi_mcp_adapter::resources_from_config(
        path.to_str().unwrap(),
        "fake",
        "subscribe",
        Some("mem://alpha"),
    );
    let tick = resource_echo(&path, "tick");
    let updates = drain_updates_until(std::time::Duration::from_secs(1));
    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    let _ = fs::remove_file(path);
    assert!(
        repeated.is_ok(),
        "active subscription was sent twice: {repeated:?}"
    );
    assert_eq!(tick.unwrap(), "echo:tick");
    assert_eq!(
        updates.len(),
        1,
        "repeated subscribe stopped resource delivery"
    );
}

#[test]
fn failed_remote_unsubscribe_does_not_prevent_resubscribing() {
    let _serial = RESOURCE_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let path = config_with_modes(&[
        "--stateful-resources",
        "--reject-duplicate-subscribe",
        "--reject-unsubscribe",
    ]);
    let _ = rpi_mcp_adapter::take_resource_updates(None);
    subscribe_alpha(&path);
    rpi_mcp_adapter::resources_from_config(
        path.to_str().unwrap(),
        "fake",
        "unsubscribe",
        Some("mem://alpha"),
    )
    .unwrap();
    let subscribed = rpi_mcp_adapter::resources_from_config(
        path.to_str().unwrap(),
        "fake",
        "subscribe",
        Some("mem://alpha"),
    );
    let tick = resource_echo(&path, "tick");
    let updates = drain_updates_until(std::time::Duration::from_secs(3));
    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    let _ = fs::remove_file(path);
    assert!(
        subscribed.is_ok(),
        "remote unsubscribe failure stranded the subscription: {subscribed:?}"
    );
    assert_eq!(tick.unwrap(), "echo:tick");
    assert_eq!(updates.len(), 1, "resubscribed resource stopped updating");
}

#[test]
fn failed_remote_unsubscribe_preserves_other_subscriptions_on_reconnect() {
    let _serial = RESOURCE_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let path = config_with_modes(&[
        "--stateful-resources",
        "--reject-duplicate-subscribe",
        "--reject-unsubscribe",
    ]);
    let _ = rpi_mcp_adapter::take_resource_updates(None);
    subscribe_alpha(&path);
    rpi_mcp_adapter::resources_from_config(
        path.to_str().unwrap(),
        "fake",
        "subscribe",
        Some("mem://beta"),
    )
    .unwrap();
    rpi_mcp_adapter::resources_from_config(
        path.to_str().unwrap(),
        "fake",
        "unsubscribe",
        Some("mem://alpha"),
    )
    .unwrap();
    let tick = resource_echo(&path, "tick");
    let updates = drain_updates_until(std::time::Duration::from_secs(3));
    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    let _ = fs::remove_file(path);
    assert_eq!(tick.unwrap(), "echo:tick");
    assert_eq!(updates.len(), 1, "unrelated subscription was lost");
    assert_eq!(updates[0].uri, "mem://beta");
}

#[test]
fn explicit_resubscribe_after_disconnect_keeps_the_early_update() {
    let _serial = RESOURCE_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let path = config_with_modes(&["--stateful-resources", "--notify-early"]);
    let _ = rpi_mcp_adapter::take_resource_updates(None);
    subscribe_alpha(&path);
    assert_eq!(
        drain_updates_until(std::time::Duration::from_secs(3)).len(),
        1
    );
    assert!(resource_echo(&path, "disconnect").is_err());
    let subscribed = rpi_mcp_adapter::resources_from_config(
        path.to_str().unwrap(),
        "fake",
        "subscribe",
        Some("mem://alpha"),
    );
    let updates = drain_updates_until(std::time::Duration::from_secs(3));
    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    let _ = fs::remove_file(path);
    subscribed.unwrap();
    assert_eq!(
        updates.len(),
        1,
        "the first update on the new transport was lost"
    );
}

#[test]
fn explicit_resubscribe_after_disconnect_does_not_subscribe_twice() {
    let _serial = RESOURCE_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let path = config_with_modes(&["--stateful-resources", "--reject-duplicate-subscribe"]);
    let _ = rpi_mcp_adapter::take_resource_updates(None);
    subscribe_alpha(&path);
    assert!(resource_echo(&path, "disconnect").is_err());
    let result = rpi_mcp_adapter::resources_from_config(
        path.to_str().unwrap(),
        "fake",
        "subscribe",
        Some("mem://alpha"),
    );
    let tick = resource_echo(&path, "tick");
    let updates = drain_updates_until(std::time::Duration::from_secs(3));
    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    let _ = fs::remove_file(path);
    assert_eq!(result.unwrap()["subscribed"], true);
    assert_eq!(tick.unwrap(), "echo:tick");
    assert_eq!(updates.len(), 1);
}

fn subscribe_alpha(path: &std::path::Path) {
    rpi_mcp_adapter::resources_from_config(
        path.to_str().unwrap(),
        "fake",
        "subscribe",
        Some("mem://alpha"),
    )
    .unwrap();
}

#[test]
fn changed_server_config_does_not_inherit_subscriptions_after_a_disconnect() {
    let _serial = RESOURCE_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let path = config_with_mode("--stateful-resources");
    let _ = rpi_mcp_adapter::take_resource_updates(None);
    subscribe_alpha(&path);
    assert!(resource_echo(&path, "disconnect").is_err());
    let mut config: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    config["mcpServers"]["fake"]["args"] = json!(["--stateful-resources", "--new-server-config"]);
    fs::write(&path, config.to_string()).unwrap();
    let result = resource_echo(&path, "tick");
    let updates = drain_updates_until(std::time::Duration::from_millis(250));
    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    let _ = fs::remove_file(path);
    assert_eq!(result.unwrap(), "echo:tick");
    assert!(
        updates.is_empty(),
        "subscriptions crossed a server configuration change"
    );
}

fn resource_config_with_marker(mode: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let path = config_path();
    let marker = path.with_extension("marker");
    fs::write(
        &path,
        json!({"mcpServers":{"fake":{
            "command":env!("CARGO_BIN_EXE_fake_mcp_server"),
            "args":["--stateful-resources", mode], "timeout":2,
            "env":{"RPI_FAKE_RESOURCE_MARKER":marker}
        }}})
        .to_string(),
    )
    .unwrap();
    (path, marker)
}

#[test]
fn rejected_restore_is_reported_without_permanently_blocking_tools() {
    let _serial = RESOURCE_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let (path, marker) = resource_config_with_marker("--reject-restored-subscribe");
    let _ = rpi_mcp_adapter::take_resource_updates(None);
    subscribe_alpha(&path);
    assert!(resource_echo(&path, "disconnect").is_err());
    let restored = resource_echo(&path, "tick");
    let next = resource_echo(&path, "tick");
    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    let updates = rpi_mcp_adapter::take_resource_updates(None);
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(marker);
    let error = restored.unwrap_err();
    assert!(error.contains("could not be restored"), "{error}");
    assert!(error.contains("subscriptions cleared"), "{error}");
    assert_eq!(next.unwrap(), "echo:tick");
    assert!(updates.is_empty());
}

#[test]
fn failed_background_resource_read_recovers_on_the_next_tool_call() {
    let _serial = RESOURCE_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let (path, marker) = resource_config_with_marker("--drop-first-resource-read");
    let _ = rpi_mcp_adapter::take_resource_updates(None);
    subscribe_alpha(&path);
    resource_echo(&path, "tick").unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    while !marker.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let dropped = marker.exists();
    let result = resource_echo(&path, "tick");
    let updates = drain_updates_until(std::time::Duration::from_secs(3));
    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(marker);
    assert!(dropped, "background read fixture did not disconnect");
    assert_eq!(result.unwrap(), "echo:tick");
    assert_eq!(
        updates.len(),
        1,
        "background disconnect lost the subscription"
    );
}

/// Regression: the background `resources/read` used a standalone control, so
/// `close` waited for its timeout (up to 120 s) instead of cancelling it.
#[test]
fn close_cancels_a_blocked_background_read() {
    let _serial = RESOURCE_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    // A long client timeout: without the fix, close would wait for the blocked
    // read to time out (30 s) instead of cancelling it.
    let path = config_with_modes_and_timeout(&["--notify-early", "--hang-read"], 30);
    let _ = rpi_mcp_adapter::take_resource_updates(None);

    rpi_mcp_adapter::resources_from_config(
        path.to_str().unwrap(),
        "fake",
        "subscribe",
        Some("mem://alpha"),
    )
    .unwrap();
    // Give the dispatcher a moment to enter the (blocking) read.
    std::thread::sleep(std::time::Duration::from_millis(200));

    let started = std::time::Instant::now();
    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < std::time::Duration::from_secs(3),
        "close waited for the blocked read instead of cancelling it: {elapsed:?}"
    );
    let _ = fs::remove_file(path);
}

/// Regression: closing a connection must leave no queued updates behind, so a
/// later session cannot be shown a released connection's resource contents.
///
/// Deliberately per-connection (`close_from_config`) rather than the global
/// `shutdown_connections`, which would tear down every other test's session.
#[test]
fn close_drops_queued_resource_updates() {
    let _serial = RESOURCE_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let path = config_with_modes(&["--notify-early"]);
    let _ = rpi_mcp_adapter::take_resource_updates(None);

    let subscribe = || {
        rpi_mcp_adapter::resources_from_config(
            path.to_str().unwrap(),
            "fake",
            "subscribe",
            Some("mem://alpha"),
        )
        .unwrap()
    };

    // The pipeline works: the update pushed before the response is queued.
    subscribe();
    let first = drain_updates_until(std::time::Duration::from_secs(5));
    assert_eq!(first.len(), 1, "expected one queued update, got {first:?}");

    // A new URI triggers another early update. Repeating the existing URI
    // reuses its remote registration and would leave this check with no update.
    rpi_mcp_adapter::resources_from_config(
        path.to_str().unwrap(),
        "fake",
        "subscribe",
        Some("mem://beta"),
    )
    .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(200));
    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();

    assert!(
        rpi_mcp_adapter::take_resource_updates(None).is_empty(),
        "close left resource updates queued for a later session"
    );
    let _ = fs::remove_file(path);
}
#[test]
fn persistent_session_and_paginated_tools() {
    let path = config_with_mode("--paged");
    let discovered = discover_from_config(path.to_str().unwrap(), "fake").unwrap();
    assert_eq!(discovered.tools.len(), 4);
    let first = call_from_config(path.to_str().unwrap(), "fake", "counter", json!({})).unwrap();
    let second = call_from_config(path.to_str().unwrap(), "fake", "counter", json!({})).unwrap();
    let (pid, count) = first.split_once(':').unwrap();
    assert_eq!(count, "1");
    assert_eq!(second, format!("{pid}:2"));
    assert!(
        call_from_config(path.to_str().unwrap(), "fake", "fail", json!({}))
            .unwrap_err()
            .contains("intentional failure")
    );
    // A tool error must not discard the healthy session.
    assert_eq!(
        call_from_config(path.to_str().unwrap(), "fake", "counter", json!({})).unwrap(),
        format!("{pid}:4")
    );
    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    let fresh = call_from_config(path.to_str().unwrap(), "fake", "counter", json!({})).unwrap();
    assert!(fresh.ends_with(":1"));
    assert_ne!(fresh.split(':').next().unwrap(), pid);
    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    fs::remove_file(path).unwrap();
}
#[test]
fn pagination_rejects_repeated_cursor() {
    let path = config_with_mode("--bad-cursor");
    assert!(discover_from_config(path.to_str().unwrap(), "fake")
        .unwrap_err()
        .contains("repeated"));
    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    fs::remove_file(path).unwrap();
}
#[test]
fn timeout_covers_a_full_stdin_pipe() {
    let mut transport = rpi_mcp_adapter::StdioTransport::start(
        env!("CARGO_BIN_EXE_fake_mcp_server"),
        &["--no-read".into()],
        &Default::default(),
        None,
        None,
    )
    .unwrap();
    transport.set_timeout(1);
    let start = std::time::Instant::now();
    let request = rpi_mcp_adapter::Request::new(
        1,
        "large",
        Some(json!({"data":"x".repeat(2 * 1024 * 1024)})),
    );
    assert!(transport
        .request(&request)
        .unwrap_err()
        .contains("timed out"));
    assert!(start.elapsed().as_secs_f32() < 3.0);
}
#[test]
fn cancellation_interrupts_pipe_write_and_cleans_workers() {
    let mut transport = rpi_mcp_adapter::StdioTransport::start(
        env!("CARGO_BIN_EXE_fake_mcp_server"),
        &["--no-read".into()],
        &Default::default(),
        None,
        None,
    )
    .unwrap();
    transport.set_timeout(60);
    let control = rpi_mcp_adapter::Control::default();
    let cancel = control.clone();
    let worker = std::thread::spawn(move || {
        let request = rpi_mcp_adapter::Request::new(
            1,
            "large",
            Some(json!({"data":"x".repeat(2 * 1024 * 1024)})),
        );
        transport.request_control(&request, &control)
    });
    std::thread::sleep(std::time::Duration::from_millis(100));
    let start = std::time::Instant::now();
    cancel.cancel();
    assert!(worker.join().unwrap().unwrap_err().contains("cancelled"));
    assert!(start.elapsed().as_secs_f32() < 2.0);
}
#[test]
fn server_ping_is_answered() {
    let path = config_with_mode("--ping");
    assert_eq!(
        call_from_config(
            path.to_str().unwrap(),
            "fake",
            "echo",
            json!({"value":"ping"})
        )
        .unwrap(),
        "echo:ping"
    );
    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    fs::remove_file(path).unwrap();
}
#[test]
fn oversize_unterminated_stdout_is_rejected() {
    let mut transport = rpi_mcp_adapter::StdioTransport::start(
        env!("CARGO_BIN_EXE_fake_mcp_server"),
        &["--oversize".into()],
        &Default::default(),
        None,
        None,
    )
    .unwrap();
    transport.set_timeout(5);
    assert!(transport
        .request(&rpi_mcp_adapter::tools_list_request(1))
        .unwrap_err()
        .contains("16 MiB"));
}
#[test]
fn timeout_error_includes_server_stderr() {
    let path = config_with_mode("--hang-call");
    let error = call_from_config(path.to_str().unwrap(), "fake", "echo", json!({})).unwrap_err();
    assert!(error.contains("tools/call timed out"), "{error}");
    assert!(error.contains("deliberately blocked"), "{error}");
    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    fs::remove_file(path).unwrap();
}

#[cfg(windows)]
#[test]
fn timeout_kills_descendants_holding_inherited_pipes() {
    let mut transport = rpi_mcp_adapter::StdioTransport::start(
        env!("CARGO_BIN_EXE_fake_mcp_server"),
        &["--spawn-child".into()],
        &Default::default(),
        None,
        None,
    )
    .unwrap();
    transport.set_timeout(1);
    let start = std::time::Instant::now();
    let error = transport
        .request(&rpi_mcp_adapter::tools_list_request(1))
        .unwrap_err();
    assert!(start.elapsed() < std::time::Duration::from_secs(3));
    let pid: u32 = error
        .split("descendant-pid=")
        .nth(1)
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(
        !rpi_plugin_sdk::process::process_is_running(pid).unwrap(),
        "MCP descendant survived transport shutdown"
    );
}

#[test]
fn command_status_reports_configured_servers() {
    let path = config_path();
    write_config(&path);
    let response =
        rpi_mcp_adapter::command_response(&format!("--config {} status", path.display()));
    assert_eq!(response["kind"], "message");
    let text = response["text"].as_str().unwrap();
    assert!(text.contains("fake"), "{text}");
    assert!(text.contains("stdio"), "{text}");
    assert!(text.contains("idle"), "{text}");
    fs::remove_file(path).unwrap();
}

#[test]
fn command_tools_connects_and_lists_discovered_tools() {
    let path = config_path();
    write_config(&path);
    // No explicit server -> a selector is returned; selecting its item re-invokes
    // the handler with the encoded subcommand and no `--config`, so the picker
    // must carry the resolved path into the continuation.
    let picker = rpi_mcp_adapter::command_response(&format!("--config {} tools", path.display()));
    assert_eq!(picker["kind"], "selector");
    let selected = picker["items"][0]["value"].as_str().unwrap().to_string();
    assert_eq!(selected, "tools fake");
    let continuation = json!({"action": "select", "value": selected}).to_string();
    let response = rpi_mcp_adapter::command_response(&continuation);
    assert_eq!(response["kind"], "message", "{response}");
    let text = response["text"].as_str().unwrap();
    assert!(text.contains("echo"), "{text}");
    assert!(text.contains("value"), "{text}");

    // The connection opened by `tools` is visible to `status` through the same
    // `(scope, identity, name)` key.
    let status = rpi_mcp_adapter::command_response(&format!("--config {} status", path.display()));
    assert!(
        status["text"].as_str().unwrap().contains("connected"),
        "{status}"
    );

    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    fs::remove_file(path).unwrap();
}

#[test]
fn command_add_and_remove_edit_the_config() {
    let path = config_path();
    fs::write(&path, r#"{"mcpServers":{}}"#).unwrap();
    let added = rpi_mcp_adapter::command_response(&format!(
        "--config {} add docs node server.js",
        path.display()
    ));
    assert!(added["text"].as_str().unwrap().contains("Added"), "{added}");
    let configured = fs::read_to_string(&path).unwrap();
    assert!(configured.contains("\"docs\""));
    assert!(configured.contains("server.js"));

    let removed =
        rpi_mcp_adapter::command_response(&format!("--config {} remove docs", path.display()));
    assert!(
        removed["text"].as_str().unwrap().contains("Removed"),
        "{removed}"
    );
    fs::remove_file(path).unwrap();
}

#[test]
fn command_resources_lists_server_resources() {
    let path = config_path();
    write_config(&path);
    let response =
        rpi_mcp_adapter::command_response(&format!("--config {} resources fake", path.display()));
    let text = response["text"].as_str().unwrap();
    assert!(text.contains("mem://alpha"), "{text}");
    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    fs::remove_file(path).unwrap();
}

#[test]
fn command_prompts_lists_and_loads_a_template() {
    let path = config_path();
    write_config(&path);
    let listed =
        rpi_mcp_adapter::command_response(&format!("--config {} prompts fake", path.display()));
    let text = listed["text"].as_str().unwrap();
    assert!(text.contains("greet"), "{text}");

    // `prompt` fetches prompts/get and reaches for the host editor. In this
    // test there is no host runtime, so it falls back to showing the text.
    let loaded = rpi_mcp_adapter::command_response(&format!(
        "--config {} prompt fake greet who=Ada",
        path.display()
    ));
    let text = loaded["text"].as_str().unwrap();
    assert!(text.contains("Hello Ada"), "{text}");
    rpi_mcp_adapter::close_from_config(path.to_str().unwrap(), "fake").unwrap();
    fs::remove_file(path).unwrap();
}

/// A config with one remote (`url`) server and one stdio server, so tests can
/// assert that OAuth only ever offers the remote one.
fn write_oauth_config(path: &std::path::Path) {
    let command = env!("CARGO_BIN_EXE_fake_mcp_server");
    let config = json!({
        "mcpServers": {
            "fake": { "command": command, "enabled": true },
            "remote": {
                "url": "https://mcp.example.com/mcp",
                "enabled": true,
                "timeout": 5
            }
        }
    });
    fs::write(path, config.to_string()).unwrap();
}

#[test]
fn command_oauth_status_lists_remote_servers_only() {
    let path = config_path();
    write_oauth_config(&path);
    let response =
        rpi_mcp_adapter::oauth_command_response(&format!("--config {} status", path.display()));
    assert_eq!(response["kind"], "message", "{response}");
    let text = response["text"].as_str().unwrap();
    assert!(text.contains("MCP OAuth state"), "{text}");
    assert!(text.contains("remote"), "{text}");
    assert!(text.contains("https://mcp.example.com/mcp"), "{text}");
    // A stdio server can never hold an OAuth credential.
    assert!(!text.contains("fake"), "{text}");
    fs::remove_file(path).unwrap();
}

#[test]
fn command_oauth_help_documents_every_subcommand() {
    let path = config_path();
    write_oauth_config(&path);
    let response =
        rpi_mcp_adapter::oauth_command_response(&format!("--config {} help", path.display()));
    assert_eq!(response["kind"], "message", "{response}");
    let text = response["text"].as_str().unwrap();
    for sub in ["/mcp-oauth login", "/mcp-oauth logout", "/mcp-oauth cancel"] {
        assert!(text.contains(sub), "missing {sub} in {text}");
    }
    fs::remove_file(path).unwrap();
}

#[test]
fn command_oauth_picker_routes_its_selection_back_to_oauth() {
    let path = config_path();
    write_oauth_config(&path);
    // No server given -> a selector over the remote servers only. `cancel` is
    // used because it is the one subcommand that touches nothing outside the
    // process: login would start a real network flow.
    let picker =
        rpi_mcp_adapter::oauth_command_response(&format!("--config {} cancel", path.display()));
    assert_eq!(picker["kind"], "selector", "{picker}");
    let items = picker["items"].as_array().unwrap();
    assert_eq!(items.len(), 1, "{picker}");
    let selected = items[0]["value"].as_str().unwrap().to_string();
    // The value carries the originating command so the continuation can route
    // itself without the `/mcp` dispatcher guessing.
    assert_eq!(selected, "/mcp-oauth cancel remote");

    // Carry the config explicitly too, so this assertion does not depend on the
    // process-global picker state (other tests use the same slot).
    let continuation = json!({
        "action": "select",
        "value": format!("--config {} {selected}", path.display()),
    })
    .to_string();
    let response = rpi_mcp_adapter::command_response(&continuation);
    assert_eq!(response["kind"], "message", "{response}");
    assert!(
        response["text"]
            .as_str()
            .unwrap()
            .contains("No sign-in to remote is in progress."),
        "{response}"
    );
    fs::remove_file(path).unwrap();
}

#[test]
fn command_oauth_login_rejects_a_server_without_a_url() {
    let path = config_path();
    write_oauth_config(&path);
    // `fake` is a stdio server, so there is nothing to authorize against and the
    // failure is reported before any network work starts.
    let response =
        rpi_mcp_adapter::oauth_command_response(&format!("--config {} login fake", path.display()));
    assert_eq!(response["kind"], "message", "{response}");
    assert!(
        response["text"]
            .as_str()
            .unwrap()
            .contains("not a remote server"),
        "{response}"
    );
    fs::remove_file(path).unwrap();
}
