---
name: agent-mesh
description: Coordinate with other rpi agents running on this machine. Use when work should be split across several agents or sessions (backend, frontend, tests, orchestrator), when another agent may already be working in this folder, or when a message arrives labelled "[rpi agent mesh message]".
---

# Coordinating with other rpi agents

`/remote-pi start` joins a per-machine session that every other joined `rpi`
process on this machine shares. Each participant has a routing address of the
form `<cwd>@<name>`.

## Discover who is there

```
list_peers()
```

Returns the addresses currently reachable. It is a metadata query to the local
broker — a few milliseconds, not a turn of another agent — so call it freely
rather than caching a peer list across turns.

An address is **opaque**. Echo it exactly as printed; never build one from parts
or guess a name.

## Send a message

```
agent_send({ to: "/Users/me/project@backend", body: { type: "task", text: "..." } })
```

- A **unicast** `to` waits for the broker's delivery ACK:
  - `received` — delivered; the peer processes it in an upcoming turn. Delivery
    is reliable, so there is **no retry-on-busy**.
  - `denied` — the peer refused. Do not retry blindly.
  - `timeout` — no ACK within 5s: the peer is offline or the transport failed.
    Report it rather than pretending the message landed.
- `to: "broadcast"`, or an array of addresses, is **fire-and-forget** (no single
  recipient exists to ACK).
- Sending to your own address is refused. If the work is yours, just do it.

## Answer a message

An incoming message arrives as a user message labelled
`[rpi agent mesh message]`, carrying:

- `from` — the sender's address,
- `id` — the message id,
- and a hint naming the exact `agent_send` call to use.

To reply, send a normal message and set `re` to that `id`:

```
agent_send({ to: "<from>", body: { type: "result", text: "..." }, re: "<id>" })
```

The original sender correlates your answer through `re`. Replying is
asynchronous: the sender sees it in a later turn. Do not block waiting for a
reply — that is what the deprecated `agent_request` does, and it wastes the turn.

## Splitting work

A workable division when several agents share a repository:

1. One agent takes a **slice around one directory or one concern** — do not
   assign two agents overlapping files.
2. Announce the slice with `agent_send` so the others can see the boundary.
3. Report completion, or hand off the *artifact* (a file path, a commit, a test
   result), not a narration of what you did.
4. When a message asks for something outside your slice, forward it to the agent
   whose slice it belongs to instead of doing it yourself.

## Limits to respect

- The mesh is **local to one machine**. There is no cross-machine routing in
  this build.
- Peers are separate `rpi` processes and may be mid-turn; a message queued for
  their next turn is normal, not a failure.
- Do not use the mesh to talk to yourself or to persist state — it is a message
  channel between live agents, not storage.
