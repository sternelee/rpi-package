//! `rpi-remote` — local agent mesh and (later) remote control for the rpi
//! coding agent, modelled on the `remote_pi`/`pi-extension` design.
//!
//! This build implements the **local agent mesh**: several `rpi` processes on
//! one machine discover each other through a per-machine broker over a
//! Unix-domain socket and exchange structured messages. It registers:
//!
//! - `agent_send` — unicast with a delivery ACK, or fire-and-forget
//!   broadcast/multicast;
//! - `list_peers` — the current routing addresses in this session;
//! - `agent_request` — deprecated synchronous request/reply;
//! - `/remote-pi` — `start`, `stop`, `status`, `peers`, `rename`.
//!
//! Remote control over the relay (QR pairing, an Ed25519 identity, streaming a
//! session to a phone) is a later stage and does **not** exist yet; see the
//! README for the staged plan and the documented differences from the reference
//! project.

// The plugin ABI boundary requires `unsafe` for the host-owned pointer/output
// slots that the SDK's macros already warrant; every such block is contained in
// `guard_or`/`event_handler!` so a panic cannot cross the boundary. Denying the
// lints that indicate genuinely sloppy code keeps the rest of the crate honest.
#![deny(unsafe_op_in_unsafe_fn)]

mod broker;
mod cwd_lock;
mod election;
mod envelope;
mod events;
mod ipc;
mod mesh_node;
mod peer;
mod state;
mod tools;

/// Test support used by `tests/`. Contains no production logic.
pub mod testing;

use rpi_plugin_sdk::{CommandHandlerFn, PluginApi, StbStringRef};

/// The slash command name.
pub const COMMAND_NAME: &str = "remote-pi";

/// Captured host runtime, used by the command handler and the tools.
static HOST: std::sync::OnceLock<PluginApi> = std::sync::OnceLock::new();

/// The captured host API, when registration already happened.
pub fn host() -> Option<&'static PluginApi> {
    HOST.get()
}

rpi_plugin_sdk::string_handler! {
fn remote_pi_command(args_json, out, _user_data) {
    let envelope: serde_json::Value =
        serde_json::from_str(args_json).unwrap_or(serde_json::Value::Null);
    let args = envelope
        .get("args")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .trim();
    let text = tools::run_command(args);
    if !out.write(text) {
        return 1;
    }
    0
}
}

rpi_plugin_sdk::export_plugin!(|api| {
    if let Some(declare) = api.declare {
        let status = declare(StbStringRef::from_str(
            r#"{"priority":55,"platforms":["linux","macos","windows"]}"#,
        ));
        if status != 0 {
            return status;
        }
    }
    let _ = HOST.set(*api);

    // The command and the tools are registered independently: a host that
    // lacks one slot should still expose the other rather than failing to load.
    if let Some(register_command) = api.register_command {
        let _ = register_command(
            StbStringRef::from_str(COMMAND_NAME),
            StbStringRef::from_str("Local agent mesh: start, stop, status, peers, rename"),
            remote_pi_command as CommandHandlerFn,
        );
    }

    // Session-observing handlers (tool-activity broadcast, shutdown teardown,
    // system-prompt guidance). A host without the event or prompt-transform
    // slots still gets the tools and the command.
    if api.register_event_handler.is_some() && api.register_before_agent_start.is_some() {
        let status = events::register_handlers(api);
        if status != 0 {
            return status;
        }
    }

    // The inbound pump is idle until a session joins, so starting it at load
    // costs one sleeping thread and nothing else.
    events::start_pump();

    tools::register_tools(api)
});
