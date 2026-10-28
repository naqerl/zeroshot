# Run jobs on on-demand cloud servers

A hub pool creates a Hetzner Cloud server when a queued job needs one, joins it to the hub as a
member, and deletes it once it is idle. A running job keeps its server alive however long it
takes; there is no age limit or deadline. Every run is archived on the hub host before its server
is deleted.

## Add a pool

Choose the server types first. `hub pool types` lists the provider's current types with vCPUs,
memory, architecture, hourly price per location and where each is available. Prices change, so
check them there rather than in this guide.

```bash
zeroshot hub pool types
zeroshot hub pool add burst \
  --provider hetzner --type cpx31,cx43 --location fsn1,nbg1 --image docker-ce \
  --target-image ghcr.io/the-open-engine/zeroshot-target:1.2.3 \
  --max 3 --idle 10m --tag linux,burst \
  --ssh-key ~/.ssh/zeroshot-pool --ssh-from 203.0.113.7/32
zeroshot hub pool list
zeroshot hub pool remove burst   # only once the pool has no servers left
```

The type names above are only an example. There is no default type.

- `--type` takes server types, best first, comma-separated or repeated. It is required. Each
  type must be current in the provider's catalog. ARM types (`cax*`) are rejected unless the
  target image runs on arm64 and you pass `--multi-arch`.
- `--location` takes locations in the order to try them. Without it, the pool uses every
  location that offers one of its types.
- `--wait-max` expires a queued job that waited this long for a server (see below).
- `--image` is the server image: a snapshot ID or a system image such as `docker-ce`.
- `--target-image` is the zeroshot target container. It must be pinned: a digest or a tag other
  than `latest`.
- `--max` caps live servers. `--slots` (default 1) is concurrent runs per server.
- `--idle` (default 10m) is the only automatic teardown trigger: a server with no hub jobs for
  this long drains and is deleted. A busy server is never torn down by a clock.
- `--ssh-key` is a private key on the hub host. Only its `.pub` sibling goes to the server.
- `--ssh-from` is the hub's public address. The server firewall allows SSH from it and nothing
  else.

The pool's `.pub` key is also registered with the provider and attached to the server by its
provider key ID. Cloud-init alone is not enough: a key written only to `ssh_authorized_keys` leaves
the login account's password expired, so PAM refuses the key session with "Password change required
but no TTY available". The boot configuration also marks the password as non-expiring so the key
session works.

The daemon needs an API token, from `hub daemon run --hetzner-token-file FILE` or `HCLOUD_TOKEN`
in the daemon's environment. The token is sent only to the Hetzner API. It is never stored in hub
state, labels, user data, or logs.

## Choose a machine per job

A job can ask for a size when you submit it:

```bash
zeroshot hub submit --tag burst --min-vcpu 8 --min-ram 16G ...   # first listed type that fits
zeroshot hub submit --tag burst --server-type cx43 ...           # that type only
```

These flags are the same as the tags `min-vcpu=N`, `min-ram-gb=N` and `server-type=TYPE`. A job
whose size no listed type can meet is rejected when it is submitted.

### Preferred machines per tag

A tag profile names the machines a pool creates for jobs that carry a tag:

```bash
zeroshot hub pool tag set burst gpu-heavy --type cpx41,cx43 --location hel1,fsn1 \
  --min-vcpu 8 --min-ram 16 --slots 2
zeroshot hub pool tag list burst
zeroshot hub pool tag remove burst gpu-heavy
```

- `--type` lists the profile's types, best first. Each must meet `--min-vcpu`/`--min-ram`.
  Without `--type`, the profile uses the pool's types that meet the minimum.
- `--location` is the order of locations to try. It defaults to the pool's.
- `--slots` sets the concurrent runs on a server created for the tag. It defaults to the pool's
  `--slots`.
- `--priority` breaks ties between profiles (see below).
- A profile tag must be an ordinary tag, not a size tag. The pool's servers join with the pool's
  tags plus every profiled tag.
- Profiles are kept in hub state (schema v2). Setting or removing one retries any wait for that
  tag at once.

### How a job's machine is chosen

1. **The job's own size**, from `--server-type`, `--min-vcpu` or `--min-ram`. The hub tries
   the matched profile's types, then the pool's, then other profiles' types, keeping those that
   fit.
2. **The profile of one of its tags.** The hub tries the profile's types and locations.
3. **The pool's default**: its `--type` and `--location` lists.

If a job carries several profiled tags, the profile with the highest `--priority` wins. Ties go
to the larger minimum vCPU count, then the larger minimum memory. `tag set` rejects a profile
that would tie with another on all three, so the choice is never ambiguous. Give one of them a
higher `--priority`.

A profile's minimum is its `--min-vcpu`/`--min-ram`. Without them, it is the smallest vCPU
count and memory among its types.

- **Reuse.** A job may use any live, idle pool server that is at least that large, so a small
  job can reuse a bigger server before a new one is created.
- **No undersized servers.** A tagged job never lands on a server smaller than its profile
  needs. The hub creates the profile's preferred type instead.

In the hub API, the job's `spec` field shows the source (`job`, `tag` or `pool`),
the matched tag and the size. Members show their `machine` and `spec` too.

## When the provider has no capacity

To create a server, the hub tries each fitting type in each of its locations, in order: the
next location first, then the next type. Pairs that the catalog shows as unavailable are
skipped without a create call. What happens next depends on how the provider answers.

- **No capacity for any pair.** The job stays queued. Its `waiting` reason names what was
  tried, for example `waiting: tag gpu-heavy needs cpx41/cx43, no capacity in hel1/fsn1` or
  `waiting: no capacity for cpx31/cx43 in fsn1/nbg1`. Jobs needing that machine are retried after
  30 seconds; the wait doubles each round, up to 10 minutes. This backoff is separate from the
  boot-failure pause and is kept per machine choice, so other jobs of the pool go on.
- **Account limit or rejected request** (quota, invalid type or image). The pool is blocked
  with the provider's error, and jobs show `blocked: …`. Fix the cause, then run
  `zeroshot hub pool resume burst`. `resume` also clears capacity waits and the boot pause.
- **Rate limit, timeout or server error.** The hub retries the lease with its own backoff.

A refused create can leave a half-made resource behind. The hub deletes everything under the
lease's label once a create succeeds or the attempt is given up. The reaper is the backstop.

With `--wait-max 30m`, a queued job that the pool has not served within 30 minutes expires with
error kind `pool_wait_timeout`, and the message carries the last waiting reason. Without it, a
job waits until its own expiry.

## What happens

1. **Placement.** A pool with no live server still counts as eligible: `hub submit` parks a
   job that only a pool can serve, exactly as `hub enqueue` accepts it, and the daemon places it.
   Submitting never fails just because the pool has not provisioned a server yet.
2. **Scale up.** Each pass creates servers for queued jobs that no live or starting server can
   take, one per `--slots` jobs of the same machine choice, up to `--max`.
   The server is named `zs-<job>` and labelled `zeroshot/hub`, `zeroshot/lease`,
   `zeroshot/server-type` and `zeroshot/location`, with no secrets.
3. **Boot.** cloud-init runs the boot script: it installs Docker and btrfs-progs if needed, pulls
   the target image, mounts the btrfs data folder at `/mnt/zs`, and runs the target on
   `127.0.0.1:8080` with `/mnt/zs` as its state. Nothing on the server shuts it down on a timer.
4. **Join.** The hub opens `ssh -N -L` to the server and runs the data health check over SSH.
   Then it reads target discovery through the tunnel,
   registers the target, and adds it as a member with the pool's and its profiles' tags. A dropped tunnel is
   reopened. Host keys are trusted on first use into the pool's own `known_hosts`.
5. **Teardown.** When a server has been idle for `--idle`, the hub drains it. It
   waits until each run is terminal and its postmortem has finished, then archives the run. Next it
   removes the member and the target, closes the tunnel, deletes the server, waits for the delete
   action, and finally deletes the firewall.
6. **Reaper.** On start and every five minutes, the hub lists resources labelled
   `zeroshot/hub=<hub id>`. It deletes servers, legacy Volumes, primary IPs, and firewalls that state
   does not know about. It never deletes a resource whose lease state still holds.

A server whose boot fails or times out is deleted. Its pool then pauses for 5 minutes, and the
pause doubles on each further failure up to an hour, so a broken image does not keep creating
billed servers.

`zeroshot hub submit` does not require you to know whether a live member or a pool will take the
job. When no free member can take it but a pool can serve its placement, submit parks the job as
queued and returns it without an error, because that pool starts a server for it; exit 3 with
`hub.no_capacity` remains for a placement that neither a free member nor a pool can serve now.
Use `zeroshot hub enqueue` when no free member can take the job now and no pool serves it.

## Watching a machine come up

Every pool transition is recorded by the code that performs it and exposed as a `hub.pool`
event. One subscription receives those events together with jobs, runs, and warnings; see
[the MCP guide](mcp.md#hub-events). The lease fields are `leaseId`, `pool`, `serverId`,
`memberId`, `machine` (`cores`, `memoryMb`, `location`, `serverType`), `phase`, `previousPhase`,
`at`, and `reason`. A lease never records the same phase twice, and `previousPhase` chains the
transitions in the order the hub made them.

| Phase       | What the hub did                                                                 | `memberId` |
| ----------- | -------------------------------------------------------------------------------- | ---------- |
| `requested` | Created the lease for a queued job.                                               | no         |
| `creating`  | The provider accepted the request; `serverId` is known.                           | no         |
| `booting`   | The provider reports the server running.                                          | no         |
| `preparing` | Opened the tunnel and is checking the data folder, image, and target startup.     | no         |
| `ready`     | The target answered discovery and the server joined the hub as a member.          | yes        |
| `in_use`    | The hub placed a job on this member.                                              | yes        |
| `idle`      | The job finished; the idle timer is running.                                      | yes        |
| `draining`  | Teardown started: archive the runs, remove the member, close the tunnel.          | yes        |
| `deleted`   | The provider confirmed deletion; billing stopped.                                 | yes        |
| `failed`    | The lease stopped early; `reason` names `creating`, `booting`, or `preparing`.     | no         |

The provider API has no boot-progress signal before it reports the server running, so
`creating` covers that wait. `booting` is recorded at the provider's running report. The hub then
records `preparing` before it opens the tunnel, and that phase covers SSH readiness, the data
health check, image pull, and target startup; there are no probe-by-probe events inside it. A
lease that failed before it had a machine never records `deleted`, so its pool does not report
`removed`.

Every pass also reconciles leases the hub can no longer act on. A lease whose pool was removed
and whose server the provider no longer lists records `deleted` with a `reason` naming both
facts, even when a drain never finished: with no pool and no server there is nothing left to
archive, tunnel, or delete. A run still placing on that lease fails with `target_lost` and
`failureClassification: "provider_confirmed_absent"`, and the pool's `removed` event follows on
the next pass. Reconciliation never deletes a server the provider still lists, never touches a
lease whose pool still exists, and leaves a lease alone when the provider cannot confirm the
loss — an inconclusive answer is not a deleted server, so the lease keeps draining and the pass
reports the failed confirmation. A lease that never received a server is not reconciled either:
there is no lost resource to report, so it keeps its pool from claiming removal.

Pool-wide changes use `kind: "pool"` and the phases `added`, `removed`, `paused`, `resumed`,
`blocked`, and `shortage`. They carry the pool name and no lease, and a `reason` for every phase
except `added` and `removed`. `paused` follows a failed boot and says how many failures in a row
stopped new servers and for how long; `resumed` follows a server that joined or a
`hub pool resume` that cleared the pause, the block, and the shortages. `removed`
is recorded only after every lease of the pool has recorded `deleted`.

An example notification for a lease that just became ready:

```json
{
  "eventId": "hub:57",
  "name": "hub.pool",
  "timestamp": "2026-02-11T09:41:12.481Z",
  "data": {
    "kind": "lease",
    "pool": "burst",
    "leaseId": "zs-hj-42",
    "serverId": 41892345,
    "memberId": "zs-hj-42",
    "machine": {"cores": 4, "memoryMb": 8192, "location": "fsn1", "serverType": "cpx31"},
    "phase": "ready",
    "previousPhase": "preparing",
    "reason": null,
    "at": "2026-02-11T09:41:12.481Z",
    "cursor": "hub:57",
    "summary": {"phase": "ready", "node": null, "terminal": false, "reason": null},
    "replayed": false
  },
  "_meta": {}
}
```

## The btrfs data folder

The target keeps run workspaces, checkpoint staging and its Restic repository under its storage
folder, and copies workspaces with reflinks (`FICLONE`). Hetzner root disks are ext4, which has no
reflinks, so every pool server keeps its data on btrfs. zeroshot uses only reflinks: no
subvolumes, no `btrfs` command, and no extra container privileges.

The boot script is written by the hub into cloud-init user data, so any server image with
Docker or apt/dnf works. No custom image is needed. The script runs with `set -eu`:

- It allocates `/var/lib/zeroshot-data.btrfs` (mode 0600) with `fallocate`. It formats the file
  with `mkfs.btrfs` and loop-mounts it at `/mnt/zs`.
- It mounts with `noatime,compress=zstd:1` and adds a `nofail` line to `/etc/fstab`, so a reboot
  remounts the folder. `/mnt/zs` is made immutable before the mount, so nothing can write to the
  ext4 underneath.
- The script then checks that `/mnt/zs` is btrfs. It creates a subvolume, takes a read-only
  snapshot and makes a reflink copy, then starts the target. If any step fails, the target is not
  started.
- Rerunning the script never reformats: a mounted or formatted folder is reused. Its log and an
  `ok` or `failed` marker are kept in `/var/lib/zeroshot-boot/`.

Before the hub registers a server, it runs a health check as root over SSH:

- the boot script finished (`ok`);
- `stat -f` reports btrfs for `/mnt/zs`;
- a subvolume snapshot works;
- the running target can make a reflink copy inside its own `/var/lib/zeroshot`.

The member then shows the checked filesystem, mode `file` and size under
`data: {fs: "btrfs", mode: "file", sizeBytes: N}` in `zeroshot hub list`, and
`zeroshot hub pool list` reports the same observed data for the pool's servers.
While the boot script still runs, or SSH fails, the check is retried until the boot deadline. A
failed boot script or check counts as a failed boot: the server is deleted and the pool pauses.

## Archives

Before deletion, `hub/archive/<job id>/` receives `status.json`, `report.json`, `watch.ndjson`,
and `logs.ndjson`. The job records the folder, plus the run's `authored_graph_hash` when the
report has one. A server is never deleted while one of its runs is unarchived. A failed archive
keeps the server and is retried on the next pass. Archiving reads through the target's API,
so it does not depend on the data folder's filesystem. Object Storage upload is not built in.

## Metrics

- `zeroshot_hub_pool_capacity_unavailable_total{pool,server_type,location}` counts capacity
  refusals and catalog skips.
- `zeroshot_hub_pool_holding{pool,reason}` is 1 while a pool starts no servers, for the reasons
  `blocked`, `capacity` or `boot_failures`.
- `zeroshot_hub_member_machine_info{member,server_type,location,source,profile}` gives each pool
  member's machine and why it was chosen.
- `zeroshot_hub_pool_queued_jobs{pool,source,profile}` counts queued jobs by machine choice.
- `zeroshot_hub_pool_events_total{event}` counts pool pass outcomes by event: `started`,
  `teardown`, `archived`, `deleted`, `reconciled`, `reaped`, `shortage`, `blocked`, `wait_timeout`,
  or `error`. `reconciled` counts leases recorded as deleted because their server was gone and
  their pool removed.

## Cost

Each type's hourly price per location is listed in `zeroshot hub pool types`. Hetzner bills by
the hour, so a server is billed for at least one hour. `--max`, `--idle`, and `--wait-max` bound
the spend; a long-running job costs its server's hourly price until that job finishes.
