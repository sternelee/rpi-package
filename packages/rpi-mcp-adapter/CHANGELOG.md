# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- *(mcp)* `/mcp` (and `/mcp-adapter`) slash commands: `status`, `list`, `tools`,
  `resources`, `prompts`, `prompt`, `reconnect`, `close`, `enable`, `disable`,
  `add`, `add-url`, `remove`, `edit`, `setup`, `help`, with a server picker when
  the server is omitted and `--config`/`$RPI_MCP_CONFIG` overrides
- *(mcp)* `prompts/list` and `prompts/get` connection support
- *(mcp)* `rpi_mcp_adapter::command_response` for host embedders and tests
- *(mcp)* `/mcp-oauth` slash commands (`status`, `login`, `logout`, `cancel`,
  `help`) implementing the MCP authorization spec: `WWW-Authenticate` challenge,
  RFC 9728 protected-resource metadata, RFC 8414 (or OpenID Connect)
  authorization-server metadata, RFC 7591 dynamic client registration, and PKCE
  `S256` — no per-server auth configuration
- *(mcp)* browser sign-in for remote servers: a `127.0.0.1` loopback callback
  catches the redirect, `state` is verified, the browser opens automatically, and
  the flow runs off the host's command thread so the TUI stays responsive
- *(mcp)* remote credentials stored in the host's `~/.rpi/agent/auth.json` under
  `mcp:<server>`, so `/mcp-oauth` and `rpi auth` share one token; discovery state
  is cached separately in `mcp-oauth.json`
- *(mcp)* `Authorization: Bearer` attached to a remote server's requests, with a
  background refresh when the access token is near expiry; a configured
  `Authorization` header still wins
- *(mcp)* one host OAuth provider registered per remote server, so
  `rpi auth login|status|refresh|logout mcp:<server>` drives the same flow
- *(mcp)* `rpi_mcp_adapter::oauth_command_response` for host embedders and tests

### Fixed

- *(mcp)* validate a `/mcp edit` submission with the same parser the tools use,
  so an unloadable config cannot replace a working one
- *(mcp)* serialize the session-scope-latching connection tests with the queue
  tests so the parallel lib suite no longer races on the process-global scope
- *(mcp)* a 401 from a remote server now reports the command that fixes it
  (`run /mcp-oauth login <server>`) instead of only the status code
- *(mcp)* allow `clippy::not_unsafe_ptr_arg_deref` around the generated plugin
  entrypoint so the documented `cargo clippy --all-targets -- -D warnings`
  verification passes (a pre-existing, workspace-wide false positive on the
  fixed `rpi_plugin_register` ABI signature, already allowed in two sibling
  crates)

## [0.2.4](https://github.com/pi-rust/rpi-package/compare/rpi-mcp-adapter-v0.2.3...rpi-mcp-adapter-v0.2.4) - 2026-10-16

### Fixed

- *(mcp)* reconnect after a rejected resource unsubscribe

## [0.2.3](https://github.com/pi-rust/rpi-package/compare/rpi-mcp-adapter-v0.2.2...rpi-mcp-adapter-v0.2.3) - 2026-10-16

### Fixed

- *(mcp)* preserve active subscriptions on repeated subscribe

## [0.2.2](https://github.com/pi-rust/rpi-package/compare/rpi-mcp-adapter-v0.2.1...rpi-mcp-adapter-v0.2.2) - 2026-10-16

### Added

- *(mcp-adapter)* MCP resources — list/read/subscribe with updates pushed to the model

### Fixed

- *(mcp-adapter)* retry resource delivery without reviving stale content
- *(mcp-adapter)* serialize connection teardown with active operations
- *(mcp-adapter)* restore resource subscriptions after reconnect
- *(mcp)* enforce resource pagination and text bounds
- *(mcp)* discard resource reads from retired subscriptions
- *(mcp)* resolve active session at resource injection boundary
- *(mcp-adapter)* isolate resource updates per session, and close without waiting
- *(mcp-adapter)* bound `mcp_resource read`, and reject HTTP subscribe

## [0.2.1](https://github.com/pi-rust/rpi-package/compare/rpi-mcp-adapter-v0.2.0...rpi-mcp-adapter-v0.2.1) - 2026-10-14

### Added

- *(webui)* integrate Open WebUI and publish versioned extensions ([#4](https://github.com/pi-rust/rpi-package/pull/4))
