# Serve Zeroshot over MCP

The hub daemon is the Model Context Protocol server. It serves Streamable HTTP on `POST /mcp` on
its normal API listener, alongside `/v1/hub/*`. Start it with `zeroshot hub daemon run` or install
it as a service with `zeroshot hub install`; there is no separate MCP process. Each tool runs the
same command the CLI would. Run events come from the durable stream `zeroshot watch` already
reads, multiplexed with hub job, pool, and warning events on one cursor.

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
| `hub_jobs`          | `zeroshot hub jobs`          | List job history, newest first.                     |
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

`hub_submit` has no `wait` or `queue` option: it places the job now, parks it when a serving pool
will start a server for it, and fails when nothing can take it. Use `hub_enqueue` to queue a run.

Read-only tools are `status`, `logs`, `watch`, `list`, `report`, `hub_list`, `hub_ps`,
`hub_jobs`, `hub_status`, `hub_watch`, `hub_stats`, `hub_logs`, and `hub_daemon_status`. `run`,
`resume`, `hub_submit`, and `hub_enqueue` submit work. `hub_add`, `hub_update`, and `hub_remove`
manage members; `hub_dequeue` cancels queued jobs or schedules; `hub_dispatch` requests a
dispatcher pass; `force-stop` and `hub_force_stop` stop runs.

`hub_jobs` lists job history instead of live load: `state` (`all` by default), `label` (one or
more `key=value` pairs), `since` (`10m`, `2h`, `3d`), and `limit`. It returns at most a page of
the newest jobs, with `jobId`, `state`, `labels`, `pinnedRevision`, `attempts`, `lastCursor`,
`createdAt`, `updatedAt`, and `failureReason` only on a terminal failure. The page is
`{"schema": "zeroshot.hub.job-history/v1", "filters": …, "limit": …, "returned": …, "jobs": […]}`.
A filter that matches nothing is an empty `jobs` array, not an error.

## Hub events

The server advertises `io.modelcontextprotocol/events` (SEP-3415). One subscription covers the
whole hub. `events/list` reports one event type, `hub`, described as everything in the hub, with
`delivery` `poll` and `push`. It also lists the four `name` values a notification may carry:

| Name          | Data                                                                                             |
| ------------- | ------------------------------------------------------------------------------------------------ |
| `hub.job`     | `jobId`, `title`, `state`, `target`, `project`, `runId`, `attempts`, `pinnedRevision`, `labels`, `submittedAt`, `finishedAt`, `failureReason`, `failureClassification`, `errorKind`, `headBranch`, `headRevision`, `status`, `terminal`, `snapshot`, `reason`, `at`, `cursor`, `summary`, `replayed`, `sequence` |
| `hub.run`     | the durable run watch record: `runId`, `cursor`, `runCursor`, `status`, `summary`, `replayed`    |
| `hub.pool`    | `kind`, `pool`, `leaseId`, `serverId`, `memberId`, `machine`, `phase`, `previousPhase`, `reason`, `at`, `cursor`, `summary`, `replayed` |
| `hub.warning` | `jobId`, `runId`, `message`, `at`, `cursor`, `summary`, `replayed`                              |

A client that does not care about the kind reads the stream as it is; a client that cares
branches on `name`. The normal way to watch the hub is a request with no `name` field at all:

```
events/subscribe {}
```

```json
{"eventId": "hub:42", "name": "hub.run", "timestamp": "RFC3339", "data": {}, "_meta": {}}
```

`summary`, `replayed`, and the hub cursor live inside `data`. `data.cursor` is the same
`hub:<sequence>` value as `eventId`. The sequence is monotonic across jobs, runs, pools, and
warnings, so a client can keep one cursor and resume the whole feed. A `hub.run` record keeps its
original durable `v2:<sequence>` watch cursor as `data.runCursor`.

Every occurrence also carries `data.sequence`, a monotone counter owned by one subscription (or one
`events/poll` caller). It starts at 1 for the first delivered event, including bootstrap snapshots,
and increments for every delivered occurrence whether or not earlier occurrences were filtered out.
Use it to order a stream and to prove no event was skipped; keep `hub:<sequence>` for resume.

Subscription arguments are all optional filters:

- Omit `name` to subscribe to the whole hub. This is the normal form and needs no other field.
- `runId` narrows to that run, its job, and its warnings. `jobId` narrows to that job, its run,
  and that run's warnings. Supplying both is invalid params (`-32602`).
- `name` still selects one carried type: `hub.job`, `hub.run`, `hub.pool`, or `hub.warning`.
  `hub` and `hub.event` are aliases for every hub event; `run.event` is the legacy run-watch
  form. An unknown name is invalid params (`-32602`), like any other bad subscription field.
  There is no wildcard syntax.
- `target`, `kinds` (`hub.job`, `hub.run`, `hub.pool`, `hub.warning`), `states`, `phases`,
  `labels`, `nodes`, `terminalOnly`, and `verbose` further filter occurrences.
- `states` accepts `submitted`, `queued`, `scheduled`, `placing`, `running`, `succeeded`, `failed`,
  `dequeued`, and `cancelled`, and applies to `hub.job` records. Any other value is invalid params.
- `phases` accepts the run phases `admitted`, `running`, `stopping`, and `finished` plus every
  lease/pool phase below. Any other value is invalid params. `nodes` applies only to run records.
- `labels` is an object of string values; a job or run must carry all of them. An unknown state,
  phase, or non-string label is invalid params (`-32602`).
- `maxAgeMs` is accepted and not applied because watch records carry no occurrence time.

`events/poll` returns occurrences after an opaque `hub:<sequence>` cursor:

```json
{
  "resultType": "complete",
  "events": [],
  "cursor": "hub:42",
  "truncated": false,
  "hasMore": false,
  "nextPollMs": 1000
}
```

- `cursor: null` starts with a bootstrap snapshot and then the live feed. For every known job
  (queued, placing, running, or already attempted) the first batch contains one synthesized
  `hub.job` in `submittedAt` order with `data.snapshot: true` and the current state. Each record
  has a distinct `eventId`/`data.cursor` of
  `hub:snapshot:<delivered>:<start>`, where `<delivered>` counts the synthesized records already
  sent and `<start>` is the durable boundary the batch began from. It is never replayed, and is
  never reported by a later poll. After that, the feed carries each genuine occurrence once;
  bootstrap records replace remembered job replay so stale job state cannot be repeated. A
  bootstrap for an already-terminal job is that job's one durable terminal occurrence, so its
  publication moves the response cursor past itself and a resume cannot replay it. Run and pool
  replay still follows the retained window. There is no backfill of run history. If `maxEvents`
  clips the bootstrap, the returned cursor resumes after exactly the delivered snapshots with
  `hasMore: true`; it never drops the rest.
- A subscription opened with a cursor does not bootstrap; it resumes strictly after that cursor.
- A cursor is exclusive. The response cursor advances to the last delivered event and never moves
  backwards.
- `truncated` is `false` when nothing was lost. It is `{ "from": "hub:N", "to": "hub:M" }` only
  when replay shortfall or backpressure permanently lost events. Sparse `v2:` watch cursors are
  not evidence of loss, so a normal jump in a run's cursor never sets `truncated`.
- `hasMore: true` means the batch reached `maxEvents` (default 50, maximum 1000). Bootstrap
  snapshots count against that bound, so a hub that knows thousands of jobs cannot return them
  all in one frame; the returned cursor resumes after the last delivered record. A `maxEvents`
  clip is resumable and never reports `truncated`.
- `nextPollMs` is `1000`.

`events/stream` is the same subscription as a long-lived request. It sends
`notifications/events/active`, replays the available window, then sends one
`notifications/events/event` per occurrence and `notifications/events/heartbeat` about every five
seconds while idle. Every notification carries
`_meta["io.modelcontextprotocol/subscriptionId"]` set to the request id. `replayed` is `true`
only for records drained from the replay buffer at subscribe time; events delivered after a lag
or from the live broadcast are `false`. A real replay or backpressure shortfall that permanently
lost events sends another `notifications/events/active` with the `{from,to}` range before
continuing.

A terminal run or job is an ordinary event. Every job produces exactly one terminal `hub.job`
occurrence, including `succeeded`, `failed`, `force_stopped`, `dequeued`, `target_lost`, and jobs
found terminal after a daemon restart. It carries `data.terminal: true`, `finishedAt`,
`failureReason`, `errorKind`, and `headBranch`/`headRevision` when Git delivery landed. A snapshot
for a job that was already terminal is that terminal occurrence. The stream never completes on a
terminal event: it stays open for later jobs, later runs, pool changes, warnings, and idle
heartbeats. Only `events/unsubscribe`, MCP session shutdown, or request cancellation stops it.
Streams do not accept `maxEvents`.

`events/list` reports `resultType` `complete` with the single `hub` type. Signed webhooks are not
offered. `events/subscribe` opens the same long-lived stream as `events/stream`; with no arguments
it subscribes to the whole hub and is the ordinary way to watch jobs, runs, pool machines, and
warnings on one stream.

`hub.job` follows the same hub record as `hub_status`, including `submitted`, `queued`,
`scheduled`, `placing`, `running`, `succeeded`, `failed`, `dequeued`, and `cancelled`. A job that
fails or is cancelled before it runs still produces its one terminal `hub.job` event. Phase
transitions are not sampled away: every new run status transition publishes a `hub.run` with a
fresh active-execution set. A status is deduplicated only against the job's immediately previous
occurrence, using its attempt and complete active-execution set, so `{A, B}` to `{A}` still
publishes A's finish. `hub.pool`
reports each lease as `requested`, `creating`, `booting`, `preparing`, `ready`, `in_use`, `idle`,
`draining`, `deleted`, or `failed`, with `previousPhase` chaining its transitions. Pool-level
records use `kind: "pool"` and the phases `added`, `removed`, `paused`, `resumed`, `blocked`,
and `shortage`. See
[Watching a machine come up](hub-pools.md#watching-a-machine-come-up). A running job receives one
`hub.warning` per hour; a run-watch or stream failure also produces `hub.warning`, and the
subscription continues.

The same feed is available without MCP:

```bash
zeroshot hub events --json
zeroshot hub events --json --after hub:42 --states running,failed --phases running
```

`zeroshot hub events` prints one NDJSON occurrence per line with the same four names, fields,
bootstrap snapshots, sequence counter, cursor semantics, and `--job`/`--target`/`--label` filters.
`--json` is accepted for symmetry with other hub commands; NDJSON is the only format. A request
without `--after` prints the bootstrap snapshot and then follows live occurrences; a request with
`--after` resumes instead of bootstrapping.

## Resume and stop

Keep the `hub:<sequence>` cursor from `notifications/events/active`, `data.cursor` on an event,
or an `events/poll` response, then open `events/stream` or call `events/poll` with that cursor and
the same filters. The client receives only events after it. Because the cursor is hub-wide, a client
can close a stream after a terminal run and later resume the same feed for a new job or run.

Hub poll and stream cursors are interchangeable when the request uses hub events. The compatibility
form `name: "run.event"` with an explicit `runId` or `jobId` still polls the legacy per-run watch
and returns `v2:` cursors; do not feed a `hub:` cursor to that form or a `v2:` cursor to a hub
subscription. Omit `name` (or use `name: "hub.run"`) when one hub cursor must span poll and stream.

To stop a stream, send `notifications/cancelled` with `params.requestId` set to the stream's
request id, or call `events/unsubscribe` with `{"requestId": <stream request id>}`. The method
returns `{"resultType": "complete", "unsubscribed": true}` when it stopped that stream and
`unsubscribed: false` when no active stream has that id. A cancelled stream sends no final result.
