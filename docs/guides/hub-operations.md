# Operate a hub: metrics and evals

A hub daemon (`zeroshot hub daemon run`, installed with `zeroshot hub install`) serves the hub
API. This guide covers its Prometheus metrics, unconfirmed submissions, and the eval loop built on
top of hub placement.

## Prometheus metrics

`GET /metrics` on the hub API address returns the text exposition format (0.0.4). It needs the
same bearer token as every other hub API call. That token is the hub folder's `api-token`, or the
file given with `--token-file`.

For a Prometheus server or agent on the hub host itself, `--metrics-listen 127.0.0.1:PORT` also
serves `/metrics` without a token. The daemon refuses any address for that listener that is not
loopback. Remote scrapers use the API address with the token.

```console
zeroshot hub install --systemd --listen 0.0.0.0:7383 --metrics-listen 127.0.0.1:9383 \
  --metrics-label task
```

Scrape configuration for a remote Prometheus (copy the hub's `api-token` to the Prometheus host):

```yaml
scrape_configs:
  - job_name: zeroshot-hub
    scrape_interval: 30s
    metrics_path: /metrics
    authorization:
      type: Bearer
      credentials_file: /etc/prometheus/zeroshot-hub.token
    static_configs:
      - targets: ["lab-box:7383"]
```

For a local Prometheus or Grafana Agent on the hub host, no token is needed:

```yaml
scrape_configs:
  - job_name: zeroshot-hub-local
    static_configs:
      - targets: ["127.0.0.1:9383"]
```

### Metric reference

Job, queue, schedule, member, and token series come from hub state at scrape time. Jobs are never
deleted from state, so the counters derived from it only grow, and they survive daemon
restarts. Pass, placement, and API series are counted since the daemon started.

| Metric | Type | Labels |
|---|---|---|
| `zeroshot_hub_jobs` | gauge | `state` (queued, scheduled, running, succeeded, failed, dequeued, expired) |
| `zeroshot_hub_queue_depth` | gauge | `kind` (due, scheduled) |
| `zeroshot_hub_schedules` | gauge | active recurring schedules |
| `zeroshot_hub_cron_fires_total`, `zeroshot_hub_cron_skips_total` | counter | none |
| `zeroshot_hub_job_queue_wait_seconds` | histogram | `target` |
| `zeroshot_hub_job_run_duration_seconds` | histogram | `target`, `outcome` |
| `zeroshot_hub_member_slots`, `_slots_used`, `_slots_free` | gauge | `member` |
| `zeroshot_hub_member_up` | gauge | `member` (last load probe; absent until probed) |
| `zeroshot_hub_member_state` | gauge | `member`, `state` |
| `zeroshot_hub_member_capabilities_known` | gauge | `member` |
| `zeroshot_hub_placements_total` | counter | `member`, `outcome` (placed, unconfirmed, dispatch_failed) |
| `zeroshot_hub_placement_rejections_total` | counter | `reason` (no_capacity, no_matching_member, missing_capability, local_unsupported, remote_unsupported) |
| `zeroshot_hub_tokens_total` | counter | `target`, `model`, `project`, `kind` (input, output, cache_read, cache_creation) |
| `zeroshot_hub_label_tokens_total` | counter | `label`, `value`, `kind` for keys opted in with `--metrics-label KEY` |
| `zeroshot_hub_passes_total`, `zeroshot_hub_pass_errors_total` | counter | none |
| `zeroshot_hub_pass_duration_seconds` | histogram | none |
| `zeroshot_hub_api_requests_total` | counter | `route` (template, such as `/v1/hub/jobs/{id}`), `method`, `code` |
| `zeroshot_hub_api_request_duration_seconds` | histogram | `route`, `method` |
| `zeroshot_hub_eval_runs` | gauge | `complete` (true, false) |
| `zeroshot_hub_eval_samples` | gauge | `state` (pending, measured, judged, failed) |
| `zeroshot_hub_build_info`, `zeroshot_hub_start_time_seconds` | gauge | `version` |

Label cardinality is bounded:

- Job, run, schedule, and eval IDs never appear as labels.
- API routes use their templates, never the concrete path.
- Jobs placed on a member that has since been removed are labeled `_removed`.
- `model`, `project`, and opted-in label values keep the first 50 distinct values and collapse
  the rest into `_other`.

### Example queries

```promql
# Queue depth and how long due jobs wait before placement (p95, 1 h window)
zeroshot_hub_queue_depth{kind="due"}
histogram_quantile(0.95, sum by (le) (rate(zeroshot_hub_job_queue_wait_seconds_bucket[1h])))

# Free capacity per member, and members whose probes fail
zeroshot_hub_member_slots_free
zeroshot_hub_member_up == 0

# Output tokens per model over the last day
sum by (model) (increase(zeroshot_hub_tokens_total{kind="output"}[1d]))

# Refused submissions by reason
sum by (reason) (rate(zeroshot_hub_placement_rejections_total[15m]))
```

## Submitting a job

`zeroshot hub submit` places a run on a free member immediately when one can take it. When no free
member can but a pool serves the job's placement, submit parks the job exactly as `hub enqueue`
accepts it: the job is persisted as queued and returned without an error, because the pool
provisions a machine for it. `hub.no_capacity` (exit 3) is reserved for a placement that neither a
free member nor a pool can serve now. Use `hub enqueue` when no free member can take the job now
and no pool serves it.

## Unconfirmed submissions

When a target does not confirm a submission (the `zeroshot run` timed out, returned no receipt, or
lost its connection), the job is not failed. It stays placing on that member, running without a
run ID and with `errorKind` `submission_unconfirmed`, and every dispatcher pass submits it there
again with the same submission key, `hub-JOB_ID-ATTEMPT`. The target deduplicates by that key, so a
run it already accepted is not started twice. These retries are counted in the job's
`unconfirmedSubmissions`, apart from `attempts`, and the count survives daemon restarts.

After 30 unconfirmed submissions the job fails with `errorKind` `unconfirmed_limit`. Its error
names the member and the submission key, because a run with that key may still exist there. Set
another limit with `zeroshot hub daemon run --unconfirmed-limit N`; `hub install` passes the flag
through to the service.

`zeroshot hub dequeue JOB_ID` also removes a placing job, for example one whose target is down.
The job becomes `dequeued` with `errorKind` `dequeued_while_placing`, and the hub stops submitting
it. The target may still hold a run under the job's submission key; the error names that key, and
the hub never follows or adopts such a run. If a submission that was in flight when you dequeued
is then accepted, the error also names the run ID so you can stop it on the target.

## Evals

`zeroshot hub eval` reruns captured tasks under other topologies, models, or runtime settings,
measures each sample, judges it, and reports improvements and regressions against a baseline.
It is built for automated evolve loops: every command prints JSON, IDs are deterministic, and
`report --fail-on-regression` turns the verdict into an exit code.

```console
# 1. Capture: from a finished hub job, or from `zeroshot run` flags
zeroshot hub eval suite add parser --description "Parser fixes"
zeroshot hub eval case add --suite parser --from-job hj_0193f1c2
zeroshot hub eval case add --suite parser --name nested-lists \
  --title "Fix nested lists" --template single-worker --input task.json \
  --runtime-config runtime.json

# 2. Run: baseline (the case as captured) plus variants, 3 samples each, through hub placement
zeroshot hub eval run --suite parser --repeat 3 \
  --variant small=model=gpt-5-mini \
  --variant duo=template=duo,runtime-patch=effort-low.json \
  --judge cmd:judges/rubric.py --check tests="./check.sh" \
  --prices prices.json --key "parser-$(git rev-parse --short HEAD)"

# 3. Report: wait for every sample, print deltas, exit 6 on a regression
zeroshot hub eval report er_4c1d0e9a7b2f3c51 --wait 2h --fail-on-regression
zeroshot hub eval report er_4c1d0e9a7b2f3c51 --table
```

### Data model

Evals live in the hub state (`state.json`, under `evals`) and in the content-addressed blob
store `eval/blobs/SHA256` of the hub folder. They are served through the hub API
(`/v1/hub/eval/...`), so `--hub CONTEXT` works for every eval command.

- **Suite**: a name, an optional description, and its case IDs.
- **Case** (`ec_` + 16 hex digits): one captured task. It holds the run arguments, with every
  file argument (`--input`, `--graph`, `--runtime-config`, `--uniform-runtime-config`,
  `--environment`) stored as a blob. It also keeps the working folder, the pinned source revision,
  the placement, requirements, labels, project, models, and the source job.
    - The ID hashes the captured request. Names and times are not part of it, so capturing the
      same task twice returns the same case.
    - Capture copies the job's own labels, not its `eval.*` labels.
- **Run** (`er_` + 16 hex digits): the selected cases, the variants, the baseline, the repeat
  count, the placement override, the judge, checks, and prices.
    - The ID hashes the request and its `--key`. Repeating a request with the same key returns the
      same run and enqueues nothing new. Without `--key`, every request creates a new run.
- **Sample**: one case × variant × repeat. It is an ordinary queued hub job with a deterministic
  ID, labeled `eval.run`, `eval.case`, `eval.variant`, `eval.repeat` (and `eval.suite`). Placement,
  `hub ps --label eval.run=er_...`, logs, stats, and metrics work unchanged.
    - States: `pending` (job not finished), `measured` (outcome and checks recorded), `judged`, or
      `failed` (the job vanished).
    - A sample records its outcome (terminal state, failure reason, target, run ID, queue wait,
      duration, token usage, model), its check results, and the judge verdict.

### Variants

`--variant NAME=KEY=VALUE[,KEY=VALUE...]` rewrites the case's run request. Overrides apply in
order. File values are read by the client and uploaded with the request.

| Key | Effect |
|---|---|
| `graph=FILE`, `template=NAME` | Replace the topology. Each removes the other. |
| `runtime-config=FILE`, `uniform-runtime-config=FILE` | Replace the runtime file. Each removes the other. |
| `profile=NAME` | Replace the profile. |
| `environment=FILE` | Replace the environment and drop `--no-environment`. |
| `model=ID`, `harness=ID`, `provider=ID` | Set the field at the top level of the runtime file and on every node that already names it. |
| `runtime-patch=FILE` | Apply a JSON merge patch (RFC 7386) to the runtime file. |

`model`, `harness`, `provider`, and `runtime-patch` need a runtime file in the case, or an earlier
`runtime-config=` in the same variant. Model attribution and lane requirements are recomputed
from the final runtime file. `NAME=` with no overrides replays the case unchanged, as an A/A
control for run-to-run noise.

The baseline (`--baseline`) is one of these:

- a variant name: `baseline` by default, which is the case as captured and is added automatically;
- `hj_JOB`: an existing hub job's result is the baseline sample. This needs exactly one case;
- `none`: report absolute numbers only.

`--target` and `--tag` override every sample's placement. Without them, each sample keeps its
case's placement.

### Measurement and checks

The daemon runs an eval worker beside the dispatcher. Slow checks and judges never delay
placement.

When a sample's job finishes, the worker does the following on the hub host:

1. It writes `status.json` (the target's run status), `job.json`, `case.json`, and
   `outcome.json` to the work folder `eval/work/RUN/CASE/VARIANT-REPEAT/`.
2. It runs each `--check NAME=COMMAND` with `sh -c` in that folder, with a 10-minute limit. Exit 0
   passes. The last 4 KiB of output is kept.

Checks see these environment variables:

- `ZEROSHOT_EVAL_RUN`, `ZEROSHOT_EVAL_CASE`, `ZEROSHOT_EVAL_VARIANT`, `ZEROSHOT_EVAL_REPEAT`,
  `ZEROSHOT_EVAL_JOB`, `ZEROSHOT_EVAL_OUTCOME`;
- `ZEROSHOT_EVAL_TARGET` and `ZEROSHOT_EVAL_RUN_ID`: where the run happened;
- `ZEROSHOT_EVAL_STATUS`, `ZEROSHOT_EVAL_INPUTS` (the sample's spooled input files), and
  `ZEROSHOT_EVAL_WORKDIR`;
- `ZEROSHOT_HUB_EXECUTABLE`.

The run's workspace stays on its target. A check that needs the produced code fetches the
delivered branch, or calls `zeroshot logs`/`status` with the run ID itself.

A sample **passes** when its job succeeded, every check passed, and an absolute judge did not
answer `fail`. Cost is estimated from the token usage and the run's `--prices` table, in the same
format as `hub stats --prices`. `report --prices` can supply or replace the table later.

### Judge protocol

A judge receives one `zeroshot.hub.eval.judge-request/v1` document:

```json
{
  "schema": "zeroshot.hub.eval.judge-request/v1",
  "mode": "absolute",
  "runId": "er_4c1d0e9a7b2f3c51",
  "case": {"caseId": "ec_…", "name": "nested-lists", "title": "Fix nested lists",
           "labels": {"task": "parser"}, "pinnedRevision": "…",
           "files": {"--input": "/…/eval/cases/ec_…/task.json"}},
  "candidate": {"variant": "small", "repeat": 1, "jobId": "hj_…",
                "outcome": {"status": "succeeded", "durationMs": 81234, "tokenUsage": {…}, "model": "gpt-5-mini"},
                "checks": [{"name": "tests", "passed": true, "exitCode": 0, "output": "…"}],
                "workDir": "/…/eval/work/…", "status": "/…/status.json"},
  "baseline": null,
  "reply": "one JSON object {\"score\": 0..1, …}"
}
```

It answers with one JSON object:

```json
{"score": 0.8, "verdict": "pass", "rationale": "Handles nested lists; misses the empty case."}
```

- **`cmd:SCRIPT`**: the script is uploaded with the run and executed on the hub host (it needs a
  shebang). It reads the request on stdin, in the sample's work folder, with the check
  environment.
    - The last JSON object line on stdout is the answer, so logging before it is fine.
    - A non-zero exit, a timeout (10 minutes), an invalid answer, or exiting before reading the
      whole request is recorded as a judge error.
- **`zeroshot:GRAPH`**: the judge is a hub job running GRAPH. The request is its `--input`, with
  `--judge-runtime FILE` as its uniform runtime config. It uses the candidate's source flags and
  placement and is labeled `eval.role=judge`. Its terminal result output is the answer: an
  object, or a string holding one.
- **Modes**:
    - `--judge-mode absolute` (default): each sample gets a score from 0 to 1.
    - `--judge-mode pairwise`: each variant sample is compared with the baseline sample of the
      same case and repeat (or the single baseline sample). It is scored from -1 to 1, positive
      when the candidate is better. The baseline's own samples are recorded as `reference`.
- A score outside the mode's range, or a missing score, is a judge error. Nothing is guessed. The
  rationale is stored (up to 8 KiB) and returned by `hub eval status`.

Anyone holding the hub token can upload a judge script or check that runs on the hub host. This
is the same trust the token already grants for running work on hub members.

### Reports

`zeroshot hub eval report RUN` returns `zeroshot.hub.eval.report/v1`:

- `variants`: one aggregate row per variant over the cases the baseline covers.
- `cases`: the same rows per case.
- `samples`: counts by state, and `complete`.
- `improvements`, `regressions`, and `regressed`.

Each row has these fields:

- `aggregate`: per metric, the `mean`, the `ci95` interval, and `n`. The metrics are `passRate`,
  `judgeScore`, `tokens` (input + output), `costUsd`, and `durationMs`.
- `delta`: per metric, `absolute` and `relative` against the baseline.
- `flags`: the deltas that pass a threshold, each `improved` or `regressed`.
- `verdict`: `improved`, `regressed`, `mixed`, `unchanged`, or `insufficient` (nothing to
  compare). The baseline row has `role: baseline` instead.

| Metric | Better | Compared | Default threshold |
|---|---|---|---|
| `passRate` | higher | absolute difference | 0.05 |
| `judgeScore` | higher | absolute difference (pairwise: the mean pairwise score) | 0.05 |
| `tokens` | lower | relative change | 0.10 |
| `costUsd` | lower | relative change | 0.10 |
| `durationMs` | lower | relative change | 0.25 |

- `--threshold METRIC=VALUE` changes a threshold, and `METRIC=off` disables one.
- `--baseline VARIANT` compares against another variant of the run. `--baseline er_RUN[:VARIANT]`
  compares against another run's samples, matched by case. Use this to track a candidate against
  last week's champion.
- `--table` prints a fixed-width summary instead of JSON.
- `--wait[=DURATION]` polls until every sample is settled.

| Exit code | Meaning (with `--fail-on-regression`) |
|---|---|
| 0 | complete, and no variant is `regressed` or `mixed` |
| 3 | not complete yet; retry or use `--wait` |
| 6 | a variant regressed in aggregate |

### Out of scope

These are deliberately left to the loop that calls the hub:

- Significance testing beyond the 95% normal-approximation interval. There are no p-values,
  multiple-comparison corrections, or sequential tests, and with few repeats the interval is
  wide.
- A dataset UI, case editing, or deleting cases and runs.
- Fetching target workspaces to the hub host. Checks fetch what they need.
- Automatic variant search. An evolve loop proposes variants, calls `hub eval run`, and reads the
  report.


### Eval to-dos

Not implemented yet. These come from Lauren Tan's (@poteto)
[eval playbook](https://github.com/cursor/plugins/blob/main/pstack/skills/poteto-mode/playbooks/eval.md):

- **Blind the judge.** The judge request currently names the variant and the model and marks
  which side is the baseline. Give samples neutral labels (for example `A` and `B`), drop model
  and variant names from the judge input, and randomize their order. Map the labels back only
  after the verdict.
- **Judge both variants in one pass on one scale.** Have one judge call score the baseline and
  the candidate together, on the same scale, instead of scoring each sample separately.
  `--judge-mode pairwise` already compares a candidate with the baseline, but it is not blinded
  and gives one relative score rather than both samples on a shared scale.
- **Verify behavior from transcripts, not self-report.** Check what the agent actually did, such
  as tool calls, files read, and commands run, from the job's transcript and events. Do not
  trust its own summary or claims, and give the judge and checks that evidence.
