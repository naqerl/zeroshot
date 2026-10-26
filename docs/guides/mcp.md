# Serve Zeroshot over MCP

The hub daemon is the Model Context Protocol server. It serves Streamable HTTP on `POST /mcp` on
its normal API listener, alongside `/v1/hub/*`. Start it with `zeroshot hub daemon run` or install
it as a service with `zeroshot hub install`; there is no separate MCP process. Each tool runs the
same command the CLI would. Run events come from the durable stream `zeroshot watch` already
reads.

The default endpoint is `http://127.0.0.1:7383/mcp`. MCP uses the same bearer token as the rest of
the hub API: by default the contents of `~/.config/zeroshot/hub/api-token` (or the hub folder's
`api-token` file, or the `--token-file` passed to the daemon). The daemon heartbeat also records
the MCP URL as `mcp`.

```yaml
mcpServers:
  zeroshot:
    type: http
    url: http://127.0.0.1:7383/mcp
    headers:
      Authorization: Bearer $(cat ~/.config/zeroshot/hub/api-token)
```

Substitute the token file's contents if your MCP client does not run shell command substitution.
For example, a request can be made directly with:

```bash
curl -sS http://127.0.0.1:7383/mcp \
  -H "Authorization: Bearer $(cat ~/.config/zeroshot/hub/api-token)" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}'
```

Requests without a valid `Authorization: Bearer <api-token>` header fail with `401` and the
`hub.unauthorized` diagnostic. An HTTP client connects with JSON-RPC request bodies. An
`initialize` request without an `Mcp-Session-Id` header mints a new session and returns its id in
the `Mcp-Session-Id` response header. Later requests must send that id; a missing, unknown, or
expired id fails with `-32600` and `data.code` `mcp.session_required`. Calling `initialize` again
with an existing id resets that session so a client can recover after a dropped connection. Idle
sessions are discarded after 30 minutes. Notifications return `202 Accepted` without a JSON-RPC body.
`events/stream` returns `text/event-stream`; each SSE `data` line is one JSON-RPC notification or
the final response. Earlier event polls and ordinary requests return JSON. The MCP worker stops
with the hub daemon.

## Handshake

`initialize` answers with the negotiated protocol version, `serverInfo.name` `zeroshot`,
`capabilities.tools.listChanged` set to false, and the events extension
`io.modelcontextprotocol/events`. The client then sends `notifications/initialized`.
`ping` returns an empty object.

## Tools

`tools/list` returns one tool per command. The description is that command's help text. Argument
names are camelCase (`runId`, `runtimeConfig`, `noPrFeedback`). Unknown fields are rejected.
`tools/list` does not paginate: a `cursor` returns an empty page.

`tools/call` builds the CLI command from `arguments` and runs it. A successful command returns its
stdout as a text content block and, when that stdout is one JSON value or NDJSON, as
`structuredContent`. A failing command is still a JSON-RPC success, with `isError: true`.
`structuredContent.code` and `message` are the CLI diagnostic (`zeroshot.error/v1`). An unknown
tool name is a JSON-RPC invalid-params error.

`input`, `graph`, `runtimeConfig`, `uniformRuntimeConfig`, and `environment` are inline JSON
values, not file paths. `input` may be any JSON value; the other four are JSON objects. The server
writes each supplied value to a private temporary file for the duration of that command, then
removes it. On `run`, `hub_submit`, and `hub_enqueue`, `taskFrom` resolves a GitHub issue or
pull-request URL into the run input. It conflicts with `input`, `repository`, and
`sourceConnection`; the CLI enforces that conflict. For example:

```json
{
  "name": "run",
  "arguments": {
    "title": "Fix the parser",
    "template": "software-change",
    "input": { "task": "Fix the parser" },
    "runtimeConfig": {
      "agents": {
        "worker": { "harness": "codex", "provider": "openai", "model": "gpt-5" }
      }
    }
  }
}
```

| Tool                | Command                      | What it does                                        |
| ------------------- | ---------------------------- | --------------------------------------------------- |
| `run`               | `zeroshot run`               | Submit a run. `detach` returns after submission.    |
| `status`            | `zeroshot status`            | Read one run's status JSON.                         |
| `logs`              | `zeroshot logs`              | Follow one run's log stream until it ends.          |
| `watch`             | `zeroshot watch`             | Follow one run's event stream until it ends.        |
| `list`              | `zeroshot list`              | List runs as JSON.                                  |
| `resume`            | `zeroshot resume`            | Resume a failed run on its retained workspace.      |
| `force-stop`        | `zeroshot force-stop`        | Stop a run.                                         |
| `report`            | `zeroshot report`            | Print one lifecycle report from retained history.   |
| `hub_list`          | `zeroshot hub list`          | List hub members.                                   |
| `hub_add`           | `zeroshot hub add`           | Add a target or `local` as a hub member.            |
| `hub_update`        | `zeroshot hub update`        | Change member tags, slots, priority, or state.      |
| `hub_remove`        | `zeroshot hub remove`        | Remove a member from the hub.                       |
| `hub_ps`            | `zeroshot hub ps`            | List hub jobs.                                      |
| `hub_status`        | `zeroshot hub status`        | Read one hub job.                                   |
| `hub_submit`        | `zeroshot hub submit`        | Place a run on a free member.                       |
| `hub_enqueue`       | `zeroshot hub enqueue`       | Queue a run for later placement.                    |
| `hub_watch`         | `zeroshot hub watch`         | Follow a placed job's events until the stream ends. |
| `hub_dequeue`       | `zeroshot hub dequeue`       | Cancel a queued job or a recurring schedule.        |
| `hub_dispatch`      | `zeroshot hub dispatch`      | Run one dispatcher pass now and print its summary.  |
| `hub_force_stop`    | `zeroshot hub force-stop`    | Force a placed job's run to stop.                   |
| `hub_stats`         | `zeroshot hub stats`         | Aggregate job outcomes and token usage.             |
| `hub_logs`          | `zeroshot hub logs`          | Follow a placed job's logs until the stream ends.   |
| `hub_daemon_status` | `zeroshot hub daemon status` | Read the hub daemon heartbeat.                      |

`run` and `hub_submit` follow events until the run settles unless `detach` is true. `watch`,
`logs`, `hub_watch`, and `hub_logs` do the same and hold the MCP session until that stream ends.
For live run events, use the events extension instead of those tools.

Read-only tools are `status`, `logs`, `watch`, `list`, `report`, `hub_list`, `hub_ps`,
`hub_status`, `hub_watch`, `hub_stats`, `hub_logs`, and `hub_daemon_status`. `run`, `resume`,
`hub_submit`, and `hub_enqueue` submit work. `hub_add`, `hub_update`, and `hub_remove` manage
members; `hub_dequeue` cancels queued jobs or schedules; `hub_dispatch` requests a dispatcher pass;
`force-stop` and `hub_force_stop` stop runs.

## Run events

The server advertises `io.modelcontextprotocol/events` (SEP-3415). There is one event type,
`run.event`. Subscription arguments take exactly one of `runId` or `jobId`, and an optional
`target`; supplying both or neither is invalid params (`-32602`). The payload of a run event is
the durable watch notification: `subscriptionId`, `runId`, `title`, optional `source`, `size`,
`cursor`, and `status`.

A `jobId` that already has a run behaves exactly like that `runId`. A queued, scheduled, or
placing job can be followed before its run exists, so one subscription can follow a job from the
queue to its terminal result. The stream opens immediately and sends
`notifications/events/active` with `run: null` and the current hub `job` (including `state`,
`error`, and `errorKind`). It keeps sending heartbeats, reads the hub job record every
`nextPollMs` (1000 ms), and starts delivering ordinary `run.event` notifications as soon as the
job has a run id. If the job fails, is dequeued, or expires before it runs, the server sends one
terminal `notifications/events/event` carrying the job's `state`, `error`, and `errorKind`, then
answers the request with `resultType: complete`. Discovery-phase notifications use
`eventId` `<jobId>:<cursor>`. `events/poll` with a not-yet-placed job returns no events and
includes the current `job` document so a poller can observe queue state without opening a stream.

`events/list` reports `delivery` as `poll` and `push`, and `resultType` `complete`. Signed
webhooks are not offered. `events/subscribe` with a `runId` or `jobId` argument opens the same
long-lived stream as `events/stream`; `events/subscribe` without an `arguments` object, and
`events/unsubscribe`, returns JSON-RPC `Unsupported` (`-32026`) with `data.feature`
`deliveryMode` and `data.value` `webhook`.

`events/poll` returns occurrences after an opaque cursor:

- `cursor: null` means start from now. The response has no events and a cursor at the current head.
- A cursor is exclusive. The response cursor advances to the last delivered event.
- `truncated: true` means the retained window skipped events since the supplied cursor.
- `hasMore: true` means the batch hit `maxEvents` (default 50) and another event was waiting.
- `nextPollMs` is `1000`. `maxAgeMs` is accepted and not applied: watch records have no occurrence time.

`events/stream` is the same subscription as a long-lived request. The server sends
`notifications/events/active`, then one `notifications/events/event` per event, and
`notifications/events/heartbeat` about every 30 seconds. Every notification carries
`_meta["io.modelcontextprotocol/subscriptionId"]` set to the request id. A gap sends another
`notifications/events/active` with `truncated: true`, then continues. When the watch stream ends,
the server answers the request with `resultType: complete`. A queued job therefore ends either
when its run stream ends, or with the terminal job notification when it never runs.

Both modes open the same `run_watch` subscription `zeroshot watch` uses: the local ledger, or the
named target's watch transport. There is no second event pipeline. Job discovery reads the same
hub job record as `hub_status`; once a run id exists, following uses that run's watch.

## Stop polling

Keep the cursor from the last `events/poll`, then open `events/stream` with that cursor, the same
`name`, and the same `arguments`. The client receives events as notifications and does not call
`events/poll` again.

To stop the stream, send `notifications/cancelled` with `params.requestId` set to the
`events/stream` request id. The server stops delivery and releases the watch. It does not send a
final result for a cancelled stream.
