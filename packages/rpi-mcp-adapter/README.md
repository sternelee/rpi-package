# rpi-mcp-adapter

MCP tools for the RPI Rust extension ABI, with persistent stdio and Streamable
HTTP connections. The host registers four stable tools:

- `mcp_list`: list configured servers or discover a server's tools and schemas;
- `mcp_call`: call a discovered tool, preserving text/images and structured results;
- `mcp_resource`: list, read, subscribe to, or unsubscribe from a server's resources;
- `mcp_request`: send a raw HTTP JSON-RPC request for compatibility/diagnostics.
  This low-level tool does not manage initialization; use the other tools for normal MCP calls.

## Slash commands

The host also registers `/mcp` (and the `/mcp-adapter` alias) for managing
servers from the interactive session. Commands resolve their config the same way
the tools do, but infer the path when one is not given: an explicit
`--config <path>` (or `$RPI_MCP_CONFIG`) wins, then `<cwd>/.rpi/mcp.json`,
`<cwd>/mcp.json`, `<cwd>/.mcp.json`, then the global `~/.config/mcp/mcp.json`,
`~/.rpi/agent/mcp.json`, `~/.rpi/mcp.json`.

| Command | What it does |
| --- | --- |
| `/mcp` | Show every configured server's transport, enabled flag and connection state |
| `/mcp list` | List configured server names |
| `/mcp tools [server]` | List a server's tools with `name*: type` params (required marked, enums/defaults included) |
| `/mcp resources [server]` | List a server's resources |
| `/mcp prompts [server]` | List a server's prompt templates |
| `/mcp prompt <server> <name> [key=value ...]` | Fetch a prompt and load it into the editor |
| `/mcp reconnect [server]` | Drop a connection so the next call reconnects |
| `/mcp close <server>` | Release a connection now |
| `/mcp enable <server>` / `/mcp disable <server>` | Toggle `enabled` in the config |
| `/mcp add <name> <command> [args ...]` | Add a stdio server |
| `/mcp add-url <name> <url>` | Add a Streamable HTTP server |
| `/mcp remove <server>` | Delete a server from the config |
| `/mcp edit` | Open the config in the editor and save the valid JSON on submit |
| `/mcp setup` | Create an empty config if none exists |
| `/mcp help` | Show the usage summary |

Omitting the server on `tools`/`resources`/`prompts` opens a picker whose
selection runs the same subcommand. Config-only edits (`add`, `add-url`,
`disable`, `remove`, `edit`) preserve unknown top-level fields and never start a
server; `tools`/`resources`/`prompts`/`reconnect` reuse the same persistent
connections the tools do. Embedders without a TUI can call
`rpi_mcp_adapter::command_response(args)` for the same response object.

### `/mcp-oauth` — sign in to remote servers

Remote (`url`) servers that use the MCP authorization spec need a browser
sign-in. `/mcp-oauth` owns that flow; a stdio server has no credential and is
never offered.

| Command | What it does |
| --- | --- |
| `/mcp-oauth` | Show the sign-in state of every remote server (and any flow in progress) |
| `/mcp-oauth login <server>` | Start a sign-in: PKCE + loopback callback, browser opened for you |
| `/mcp-oauth logout <server>` | Revoke at the authorization server and forget the credential |
| `/mcp-oauth cancel <server>` | Stop a sign-in that is already in progress |
| `/mcp-oauth help` | Show the usage summary |

Omitting the server on `login`/`logout`/`cancel` opens the same kind of picker
as `/mcp`.

Discovery follows the MCP authorization specification with no extra
configuration: the server's `WWW-Authenticate` challenge (or a direct probe)
points at RFC 9728 protected-resource metadata, which names the RFC 8414
authorization server (OpenID Connect discovery is accepted as a fallback); the
client registers itself dynamically over RFC 7591 and authorizes with PKCE
(`S256`). The callback listens on `127.0.0.1:33418` (an ephemeral port if that
one is taken) so the registered `redirect_uri` stays reusable, and the `state`
parameter is verified before the code is exchanged.

The flow runs on a background thread because the host invokes command handlers
on its TUI thread: `login` returns immediately, and `/mcp-oauth status` reports
progress — including the authorization URL to open by hand if the browser did
not launch. The same flow is available without the TUI: `rpi auth login
mcp:<server>` runs it in the foreground (the browser still opens) and saves the
credential, and `rpi auth status`, `refresh`, and `logout` operate on that same
stored token.

Credentials live in `~/.rpi/agent/auth.json` (override the directory with
`RPI_CODING_AGENT_DIR`) under the provider id `mcp:<server>`, in the same shape
the host CLI writes — so `/mcp-oauth` and `rpi auth` share one credential and
neither has to sign in again. `Authorization: Bearer` is then attached to that
server's requests automatically, refreshed in the background when the access
token is near expiry; an `Authorization` header in the server config always
wins, and a 401 names the exact command to run. Discovery state (endpoints and
the registered client) is cached next to the credential in `mcp-oauth.json`,
never in the credential file itself.

The plugin also registers one OAuth provider per remote server with the host,
so the same sign-in is reachable from `rpi auth login|status|refresh|logout
mcp:<server>` and from any other host surface that lists providers. Embedders
without a TUI can call `rpi_mcp_adapter::oauth_command_response(args)`.

## Configuration and discovery

```json
{"mcpServers":{"playwright":{"command":"node","args":["server.js","--extension"],"timeout":60},"remote":{"url":"http://localhost:8931/mcp","headers":{}}}}
```

List server names without starting processes with `mcp_list`:

```json
{"configPath":".rpi/mcp.json"}
```

Discover all tool schemas, including paginated tool lists:

```json
{"configPath":".rpi/mcp.json","server":"playwright"}
```

Call a tool with `mcp_call`:

```json
{"configPath":".rpi/mcp.json","server":"playwright","tool":"browser_tabs","arguments":{"action":"list"},"timeoutSeconds":60}
```

Both tools also accept `url` instead of `configPath`/`server` for a direct HTTP
server. Loopback and private addresses are supported. HTTP redirects are disabled
so configured authentication headers are not forwarded to another endpoint.
Relative config paths resolve against the host project cwd. A stdio server starts
in that cwd unless `cwd` is configured; a relative configured cwd resolves against
the config file's directory. Environment settings are passed to the subprocess.

### Playwright extension screenshots

Playwright's action timeout is separate from the adapter's request timeout.
For slower screenshots, add `--timeout-action`, `15000` to the Playwright server
arguments and keep the outer `timeoutSeconds` at 60. Increasing only the outer
timeout does not override Playwright's default 5000 ms action timeout.

When using `--extension`, select the target tab before taking a screenshot:

```json
{"configPath":".rpi/mcp.json","server":"playwright","tool":"browser_tabs","arguments":{"action":"select","index":0},"timeoutSeconds":60}
```

Use the index returned by `browser_tabs` for the intended page. Selecting a tab
brings it to the front; a DOM snapshot can show updated state while a background
tab still has stale rendered content. Then call `browser_take_screenshot` with a
filename such as `.rpi/browser-output/lightbox.png`. In extension mode, a filename
may resolve against the workspace directory, even with `--output-dir` configured.

## Resources and live updates

`mcp_resource` covers the MCP resource surface (read-only by spec — writing goes
through a server's tools):

```json
{"configPath":".rpi/mcp.json","server":"docs","action":"list"}
{"configPath":".rpi/mcp.json","server":"docs","action":"read","uri":"file:///README.md"}
{"configPath":".rpi/mcp.json","server":"docs","action":"subscribe","uri":"file:///README.md"}
{"configPath":".rpi/mcp.json","server":"docs","action":"unsubscribe","uri":"file:///README.md"}
```

A subscribed server pushes `notifications/resources/updated`. The adapter reads
the resource and shows the new content to the model as a message at the next turn
boundary (`TurnEnd`) — never mid tool batch, so an assistant `tool_calls` message
stays adjacent to its `toolResult`s.

Updates are queued **per session scope**, not globally: only updates produced
under the session the host is currently serving are injected, and a session
switch drops the rest. The active session comes from the SDK environment, with
the latest tool call's `__rpi.sessionId` as a fallback for older embedders. Otherwise
switching sessions would leak one session's resource contents into another's
context. Closing a connection or unsubscribing a URI drops its queued updates
and prevents reads from that subscription lifetime from enqueueing late results.
Subscribing again starts a new lifetime, so an older read cannot reach it.
It also drops content already queued by the old lifetime. Repeating `subscribe`
for an active URI reuses the server registration; after reconnecting, that
registration is restored once before the call completes.
Only subscribed URIs trigger background reads.

Unsubscribing always stops local delivery. If the server rejects the unsubscribe,
the adapter closes that transport so a later subscribe can start cleanly. Other
active subscriptions are restored on the next connection.

If the host rejects an update message, the adapter retries at a later turn
boundary while the same session and subscription lifetime remain active. Newer
reads supersede older failed deliveries, including reads already delivered by
another boundary. Retries respect the 64-update queue limit and preserve newer
queued content when it is full.

A subscription is marked **before** its `subscribe` request goes out, so a server
that pushes the first update while the handshake is still in flight is not lost;
a failed subscribe rolls the mark back. Closing a connection cancels an in-flight
background read instead of waiting for its timeout, and joins the connection's
dispatcher thread. Closing or shutting down first drains in-flight operations,
then purges their subscriptions and queued updates. The stopped connection stays
registered until its dispatcher exits, so late work cannot reach a replacement.

This works for stdio servers today; HTTP/SSE push is not wired yet.

## Connections, bounds and cancellation

Connections are isolated by host session ID and canonical config path/server name
(or HTTP URL). Library callers without host context use a library-scoped fallback.
Initialization occurs once per connection. Config changes replace the connection.
Tool discovery refreshes before each call, so tool-list changes are reflected.
`mcp_list` with `action: "close"` releases an individual connection. Session shutdown
releases all connections in the extension instance. The host's reload flow must
finish old extension shutdown handlers before unloading DLLs.

The ABI executes on a background worker; `poll` is nonblocking. Cancellation and
per-request deadlines cover stdin writes, stdout reads, connection lock waits and
HTTP exchanges. The configured `timeout` defaults to 60 seconds; `timeoutSeconds`
overrides it for a call/list operation (1-300 seconds). Each handshake/list/call
request has its own deadline. Timed-out/cancelled stdio connections are stopped,
including their process tree and owned pipe workers. HTTP cancellations send a
best-effort cancellation notification. Failed transports are discarded and the
next invocation reinitializes; tool calls are never automatically replayed.

Reinitializing the same server restores its resource subscriptions before the
next operation. Transport failures or cancellation during restoration leave them
available for the next invocation to retry. If the server rejects restoration,
the adapter reports the error and clears the subscriptions; subscribe again to
resume updates. Changing server configuration also clears the old subscriptions
and queued updates instead of transferring them to the replacement server.

Requests/responses are limited to 16 MiB. Discovery is limited to 100 pages and
16384 tools, with repeated pagination cursors rejected. The connection registry is
limited to 64 entries; close unused entries to free capacity. The last 8 KiB of
server stderr is attached to stdio transport errors.

Streamable HTTP supports JSON and incremental SSE responses, initialized
notifications, negotiated protocol/session headers, session expiration and DELETE
cleanup. JSON-RPC responses are matched by ID. Server ping requests are answered;
other client requests return method-not-supported. Supported negotiated versions
are 2024-11-05, 2025-03-26 and 2025-06-18. Legacy HTTP+SSE endpoints (`/sse`) are
outside this adapter's scope; use a Streamable HTTP MCP endpoint (`/mcp`).

MCP `isError` results become host tool errors. Successful text and image blocks
cross the ABI as native host content. Other MCP blocks receive a text representation;
the original result, including structured content, is retained in result details.

## Scope and validation

The stable dispatcher avoids runtime tool registration. Prompts, sampling,
elicitation, OAuth, and automatic dynamic tool registration are not implemented;
resources are supported through `mcp_resource`, but resource updates over
HTTP/SSE push are not wired yet. `exposure` is parsed for configuration
compatibility; the stable dispatcher does not implement Pi's dynamic exposure
modes.

Run `cargo test -p rpi-mcp-adapter` and
`cargo clippy -p rpi-mcp-adapter --all-targets -- -D warnings`.
Tests exercise persistent processes, pagination, blocked writes, cancellation,
size limits, stderr diagnostics, HTTP JSON/SSE lifecycle, session headers,
expiration, ping replies and native images. The separate package smoke harness
also tests the built DLL through the real host ABI:

```powershell
$env:RPI_MCP_DLL = 'C:\path\to\rpi_mcp_adapter.dll'
$env:RPI_FAKE_MCP_SERVER = 'C:\path\to\fake_mcp_server.exe'
cargo test --manifest-path tools/rpi-package-smoke/Cargo.toml mcp_dll -- --ignored
```
