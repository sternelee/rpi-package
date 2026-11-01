# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `agent_send` no longer re-sends the message while waiting for the delivery
  ACK. The wait was sliced for cancellation, and each slice built a fresh
  envelope with a new id, so a slow peer received duplicate copies under
  different ids and any reply's `re` pointed at a copy the sender never tracked.
  Cancellation is now observed inside the single send instead.
- Tool activity is no longer broadcast to peers. Nothing consumed the
  announcement, and because it arrived as ordinary mesh content it made every
  peer spend a model turn on a synthetic message it had to reply to.

### Fixed (failover)

- A dropped or killed leader no longer leaves its broker running behind it: the
  stop flag is tied to the leader's session, so the endpoint is released and a
  survivor can win the election.
- A surviving peer now re-joins on connection loss — re-running the election, so
  it either follows a re-led peer or leads itself — instead of hanging until
  every request times out.
- A re-led peer now *registers* on its own broker. Previously it flipped to the
  leader role but appeared on no roster, so `list_peers` and every
  `broker`-addressed request timed out.
- The reader thread no longer holds a duplicate socket handle past `leave`/drop,
  which kept the connection open and left a ghost peer in the broker's roster.
- The `(cwd, name)` lock no longer lands in `<root>/locks/locks/`; it lives one
  `locks` directory below the state root, as documented.

### Changed

- Lock path derivation has one implementation (`lock_path_in`) instead of two
  that differed only in their root.

### Removed

- Dead surface: `PendingMap`, `SessionPeer::is_leaving`, `is_connected`,
  `generation`, `room_id_for_cwd`, `base64_standard`, `MeshNode::locked_name`,
  `testing::Peer::session_peer`, and the inert `takeover` registration flag.
  All `#[allow(dead_code)]` markers are gone.
- `Drive` no longer duplicates the cancel flag; it uses the one `ToolHandles`
  already owns and exposes.

## [0.1.0](https://github.com/pi-rust/rpi-package/releases/tag/rpi-remote-v0.1.0) - 2026-10-18

### Added

- Local agent mesh for rpi, ported from the `remote_pi` / `pi-extension` design:
  a per-machine broker over a Unix-domain socket with bind-or-connect leader
  election and stale-endpoint recovery.
- `agent_send` (unicast with a broker delivery ACK; fire-and-forget broadcast and
  multicast), `list_peers`, and the deprecated synchronous `agent_request`.
- `/remote-pi start | stop | status | peers | rename`.
- Inbound mesh messages are delivered into the live session as labelled user
  turns.
- Leader failover: a dropped or killed leader releases its endpoint, and a
  surviving peer re-runs the election and re-registers, so the mesh keeps
  serving after its leader dies.
- A per-`(cwd, name)` singleton lock so two sessions cannot claim one identity,
  while several named agents may share one folder.
- Mesh usage guidance is appended to the system prompt while a session is on the
  mesh, so the model knows the reply contract without any persisted state.
- End-to-end tests that run real brokers over real sockets: unicast + ACK,
  broadcast, reply correlation, request/reply, lock contention, multi-folder
  sessions.

### Notes

- The local mesh requires a Unix-domain socket; on Windows use `rpi --server` as
  the broker transport until the named-pipe path lands.
- Remote control over a relay (QR pairing, Ed25519 identity, streaming to a
  client) is not part of this release.
