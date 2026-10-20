# Operate a hub: metrics and evals

A hub daemon (`zeroshot hub daemon run`, installed with `zeroshot hub install`) serves the hub
API. This guide covers its Prometheus metrics and the eval loop built on top of hub placement.

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
| `zeroshot_hub_placements_total` | counter | `member`, `outcome` (placed, dispatch_failed) |
| `zeroshot_hub_placement_rejections_total` | counter | `reason` (no_capacity, no_matching_member, missing_capability, local_unsupported, remote_unsupported) |
| `zeroshot_hub_tokens_total` | counter | `target`, `model`, `project`, `kind` (input, output, cache_read, cache_creation) |
| `zeroshot_hub_label_tokens_total` | counter | `label`, `value`, `kind` for keys opted in with `--metrics-label KEY` |
| `zeroshot_hub_passes_total`, `zeroshot_hub_pass_errors_total` | counter | none |
| `zeroshot_hub_pass_duration_seconds` | histogram | none |
| `zeroshot_hub_api_requests_total` | counter | `route` (template, such as `/v1/hub/jobs/{id}`), `method`, `code` |
| `zeroshot_hub_api_request_duration_seconds` | histogram | `route`, `method` |
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
