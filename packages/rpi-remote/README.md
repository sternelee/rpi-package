# rpi-remote

Local agent mesh for the `rpi` coding agent, modelled on the
[`remote_pi` / `pi-extension`](https://github.com/jacobaraujo7/remote_pi) design:
several `rpi` processes on one machine discover each other and exchange
structured messages, so two agents in two terminals can split work
(`backend`, `frontend`, `tests`, …) and coordinate.

> **Stage 1 of a staged port.** This build implements the local mesh. Remote
> control over a relay — QR pairing, an Ed25519 machine identity, streaming a
> session to a phone — is **not implemented yet**. See
> [Roadmap](#roadmap) for what each stage adds and
> [Differences from remote_pi](#differences-from-remotepi) for the constraints
> that shape the port.

## What it gives you

- **`agent_send`** — send a message to another agent. A unicast send waits for
  the broker's delivery ACK, so `received` means the message is safely queued
  for the peer; `timeout` means it is offline. Broadcast and multicast are
  fire-and-forget.
- **`list_peers`** — the routing addresses currently reachable in this session.
- **`agent_request`** — deprecated synchronous request/reply, kept for parity.
- **`/remote-pi`** — `start`, `stop`, `status`, `peers`, `rename`.

An incoming message is injected into the receiving session as a labelled user
message, so the peer's model sees it on its next turn and can answer with
`agent_send` + `re`. That is the whole protocol: the *agent* handles a message,
not a handler.

## Install

```bash
rpi install rpi-remote
```

Then, in each terminal you want on the mesh:

```text
/remote-pi start
```

## Try it in 30 seconds

Open two `rpi` sessions in the **same** directory and run `/remote-pi start` in
both. Then ask the model in the first one:

```text
Who else is connected in our agent session? List them.
```

The model calls `list_peers` and reports the addresses it sees. Then:

```text
Send a ping to the other agent and ask it to reply later.
```

The model calls `agent_send({to: "<address from list_peers>", body: {type: "ping"}})`,
which returns `received` as soon as the broker delivered it. The second terminal
sees the message as a user turn and answers with its own `agent_send`, setting
`re` to the original `id`.

Copy an address exactly as `list_peers` printed it. Do not build, parse or
normalize one.

## How it works

```text
┌────────────────────────── one machine ──────────────────────────┐
│                                                                 │
│  rpi #1 (leader)              rpi #2 (follower)                 │
│  ┌────────────┐               ┌────────────┐                    │
│  │ SessionPeer│◀── JSONL ────▶│ SessionPeer│                    │
│  └─────┬──────┘   over UDS    └────────────┘                    │
│        │ hosts                                                  │
│  ┌─────▼──────┐                                                 │
│  │   Broker   │  routes envelopes, ACKs unicast, audits         │
│  └────────────┘                                                 │
│                                                                 │
│  endpoint: ~/.rpi/agent/remote/sessions/local/broker.sock       │
└─────────────────────────────────────────────────────────────────┘
```

- The **first** process to reach the endpoint binds it and becomes the
  **leader**, hosting the broker; everyone else connects as a **follower**.
- A dead leader's endpoint is detected (connect fails), cleared and re-bound by
  the next process, so failover needs no cleanup step.
- The **envelope** is five fields:
  `{from, to, id, re, body}` — `to` is one address, an array, or `"broadcast"`.
- The **broker owns `from`**: a peer cannot claim another agent's identity.
- A per-`(cwd, name)` **lock** prevents two sessions from claiming one identity.
  Several agents may share a folder as long as their names differ.

### State and addresses

| Path | Contents |
| --- | --- |
| `~/.rpi/agent/remote/sessions/local/broker.sock` | the broker endpoint |
| `~/.rpi/agent/remote/sessions/local/audit.jsonl` | every routed envelope |
| `~/.rpi/agent/remote/locks/<room>.sock` | one `(cwd, name)` lock |
| `<cwd>/.rpi/remote.json` | optional `{ "agentName": "backend" }` |

`RPI_CODING_AGENT_DIR` relocates the whole root, which is how the tests stay out
of your real `~/.rpi`.

An address is `<cwd>@<name>`, e.g. `/Users/me/project@backend`. It is an opaque
routing key: echo it, never construct it.

### Commands

| Command | Effect |
| --- | --- |
| `/remote-pi start` | Join the local mesh |
| `/remote-pi stop` | Leave it |
| `/remote-pi status` | Session, assigned address, peer count, lock path |
| `/remote-pi peers` | List addresses, marking yours |
| `/remote-pi rename <name>` | Re-join under a new name |

### An agent can be renamed per folder

Drop a `remote.json` next to your project to give the agent an explicit name,
which is what lets two agents share one directory:

```json
{ "agentName": "backend" }
```

## Platform support

The mesh uses a Unix-domain socket. On **Windows** the local mesh is not
available in this build: `std` has no portable Unix-domain socket type, and the
crate deliberately avoids a large cross-platform IPC dependency for it. Use
`rpi --server` (TCP + auth token) as the transport on Windows until the
named-pipe path lands. Everything else — the tools, the command, the protocol
module — is platform-neutral and compiles on all three platforms.

## Differences from remote_pi

The reference project is a TypeScript extension for Pi; this is a Rust `cdylib`
for rpi. Some things are genuinely different rather than merely rewritten:

| | remote_pi | rpi-remote |
| --- | --- | --- |
| Extension model | TS, loaded by the Pi SDK | Rust `cdylib` behind the stable rpi plugin ABI |
| Tool approval | `pi.on("tool_call", …)` can block a tool | Plugin **event handlers cannot veto a tool call** in this rpi build (`before_tool_call` is `None`), so remote approval must go through an `rpi --mode rpc` child process — a later stage |
| Model catalogue | SDK `ModelRegistry` | No plugin-visible registry; a remote model picker is deferred rather than faked |
| Cross-machine | Relay verifies owner-signed membership | Not implemented; auth will be per-connection only unless the relay verifies signatures |
| Service install | Bundled systemd/launchd/Task Scheduler installer | Configuration + documented templates; installation stays with the user |

Two deliberate omissions from stage 1: there is no `agent#N` collision suffix
(a second session with the same `(cwd, name)` is **refused**, because silently
renaming a session the user named is worse than an explicit error), and the
cwd-only room id is preserved when the name equals the folder's default, so an
existing single-agent setup is not re-keyed.

## Roadmap

| Stage | Contents |
| --- | --- |
| **1 — done** | Local mesh: UDS broker, leader election **with failover** (a survivor re-leads and re-registers when the leader dies), envelope + ACK protocol, `agent_send` / `list_peers` / `agent_request`, `/remote-pi`, inbound delivery |
| 2 | Relay client + Ed25519 identity (keyring with a `0600` file fallback), QR pairing, wire protocol mirroring `PROTOCOL.md`, streaming the session to a paired client, app actions (compact / new session / model / thinking), footer status |
| 3 | Remote approval via an `rpi --mode rpc` child (`ui_dialog_*`), daemon registry, scheduled prompts, cross-machine envelope forwarding |

## Development

```bash
cargo test -p rpi-remote      # unit + end-to-end mesh tests
cargo build --release -p rpi-remote
```

The integration tests in `tests/mesh.rs` start **real brokers** over real
sockets and cover unicast + ACK, broadcast, reply correlation, request/reply,
lock contention and multi-folder sessions. They use private temp roots, so they
never touch your `~/.rpi` state.

```
src/
├── lib.rs        plugin entrypoint: registers the tools, the command, handlers
├── envelope.rs   the 5-field envelope, its codec, ACK/transport-error shapes
├── ipc.rs        the UDS/named-pipe transport seam
├── broker.rs     the leader-hosted broker: register → route → ACK → audit
├── election.rs   bind-or-connect race with stale-endpoint recovery
├── cwd_lock.rs   per-`(cwd, name)` singleton lock
├── peer.rs       `SessionPeer`: the leader/follower-symmetric client
├── mesh_node.rs  composition: lock + peer + session directories
├── events.rs     mesh ↔ session bridge (delivery, shutdown teardown, prompt hint)
├── state.rs      roots, room ids, per-process session slot
├── tools.rs      the three tools and `/remote-pi`
└── testing.rs    test harness used by `tests/`
```

## License

MIT.
