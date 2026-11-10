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
- `--wait-max` retires a job's own starting lease after this long without the target (see
  below); it does not bound time spent waiting for a free slot.
- `--image` is the server image: a snapshot ID or a system image such as `docker-ce`.
- `--target-image` is the zeroshot target container. It must be pinned: a digest or a tag other
  than `latest`.
- `--max` caps live servers. `--slots` (default 1) is concurrent runs per server.
- `--idle` (default 10m) is the only automatic teardown trigger: a server with no hub jobs for
  this long drains and is deleted, unless a deliberate `hub stop` holds the lease (see
  [Deliberate stops](#deliberate-stops-and-release)). A busy server is never torn down by a clock.
- `--ssh-key` is a private key on the hub host. Only its `.pub` sibling goes to the server.
- `--ssh-from` is the hub's public address. The server firewall allows SSH from it and nothing
  else.
- `--transport` picks the job data path. `ssh` (the default) keeps the `ssh -N -L` tunnel the
  hub has always used. `tailcat` runs a pinned
  [tailcat](https://github.com/tailscale/tailcat) server on the box and reaches the target over
  its WireGuard tunnel; SSH remains only for the one-time address read and control-plane
  scripts. See [Reach a target over tailcat](#reach-a-target-over-tailcat).

The pool's `.pub` key is also registered with the provider and attached to the server by its
provider key ID. Cloud-init alone is not enough: a key written only to `ssh_authorized_keys` leaves
the login account's password expired, so PAM refuses the key session with "Password change required
but no TTY available". The boot configuration also marks the password as non-expiring so the key
session works.

The daemon needs an API token, from `hub daemon run --hetzner-token-file FILE` or `HCLOUD_TOKEN`
in the daemon's environment. The token is sent only to the Hetzner API. It is never stored in hub
state, labels, user data, or logs.

## Reach a target over tailcat

`--transport tailcat` removes SSH from the job data path. The boot script downloads the pinned
`tailcat` v0.7.0 Linux binary for the server's architecture, verifies the release checksum, and
starts `tailcat serve 8080` in a container beside the target. tailcat writes its address to
`/run/zeroshot/tailcat.addr`; the hub reads that one file over the SSH channel it already has,
then forwards each lease's loopback port to port 8080 through `tailcat forward`. Job traffic never
crosses the SSH session.

The address is derived from the server's node key, pre-shared key, and DERP region, but deriving
it on the hub would mean reimplementing tailcat's unstable CBOR address format; reading the file
is the smaller surface. The server key lives on the box's root disk and its region is pinned once,
so a restarted tailcat server keeps the same address.

Requirements and fallbacks:

- The hub host needs the matching `tailcat` CLI on its `PATH` (v0.7.0). Without it, the lease
  logs a warning and uses the SSH tunnel.
- A server that publishes no address, for example because the download, checksum, key
  generation, or container start failed, also falls back to SSH. A tailcat problem never fails a
  lease that SSH can serve.
- `--transport ssh` is the default and behaves exactly as before. Existing pools and saved state
  load as `ssh`.

Smoke test the live path on a real hub with a provider token: create a pool with
`--transport tailcat`, submit one job, confirm the lease's `transport` is `tailcat` in
`hub pool list` (and the daemon log says so once for that lease), and confirm the run succeeds.
This repository's build has no Docker socket or Hetzner token, so the live path is unproven here.

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
- **Account limit, rejected request, or another provider refusal** (quota, invalid type or
  image, or an unclassified error). The pool is blocked with the provider's error, and jobs
  waiting on it report `errorKind: pool_blocked` with the pool name, reason, and attempt time in
  `failureReason`. The block expires on its own: 1 minute, then 2, 5, 15, and at most 30 minutes
  between consecutive failures. A successful lease resets the schedule. After expiry the job
  reports `errorKind: pool_retrying` with the next attempt time. `zeroshot hub pool resume burst`
  clears the block immediately and also clears capacity waits and the boot pause.
- **Held machine.** `zeroshot hub stop <JOB>` deliberately stops a job and holds its lease, so
  the idle rule does not delete the server. Give it back with
  `zeroshot hub pool release <LEASE>` (the lease name is in `hub pool list`); the server then
  follows the ordinary `--idle` rule. A held lease still counts against `--max` until it is
  released, because the server is still billed.
- **Rate limit, timeout or server error.** The hub retries the lease with its own backoff.

A refused create can leave a half-made resource behind. The hub deletes everything under the
lease's label once a create succeeds or the attempt is given up. The reaper is the backstop.

`--wait-max` bounds one lease attempt. With `--wait-max 30m`, a lease a job started is retired and
the job fails with error kind `pool_wait_timeout` when it has not reached the target 30 minutes
after the lease was created; its message names the lease and carries the last waiting reason.
A job with no lease of its own is waiting for a free slot, not for a server of its own, and is not
failed by `--wait-max`. While the pool runs its `--max`, such a job stays `queued` with `waiting`
naming the pool, its `--max`, and the job holding the slot. Queue time has its own 24-hour ceiling:
a job still `queued` or `scheduled` without a lease of its own after 24 hours fails with error kind
`queue_expired`, and its message names the wait. Without `--wait-max`, a lease attempt has no
limit; the queue ceiling still applies.

## What happens

1. **Placement.** A pool with no live server still counts as eligible: `hub submit` parks a
   job that only a pool can serve, exactly as `hub enqueue` accepts it, and the daemon places it.
   Submitting never fails just because the pool has not provisioned a server yet.
2. **Scale up.** Each pass creates servers for queued jobs that no live or starting server can
   take, one per `--slots` jobs of the same machine choice, up to `--max`.
   The server is named `zs-<job>` and labelled `zeroshot/hub`, `zeroshot/lease`,
   `zeroshot/server-type` and `zeroshot/location`, with no secrets.
3. **Boot.** cloud-init runs the boot script: it installs Docker and btrfs-progs if needed, pulls
   the target image, mounts the btrfs data folder at `/mnt/zs`, and runs the target on the server's
   host network bound to `127.0.0.1:8080` with `/mnt/zs` as its state, so the hub's tunnel reaches
   the target itself and not a Docker-published port. Nothing on the server shuts it down on a
   timer.
4. **Join.** The hub opens `ssh -N -L` to the server and runs the data health check over SSH.
   Then it reads target discovery through the tunnel,
   registers the target, and adds it as a member with the pool's and its profiles' tags. A dropped tunnel is
   reopened. Host keys are trusted on first use into the pool's own `known_hosts`.
5. **Teardown.** When a server has been idle for `--idle` and its lease is not held by a
   deliberate `hub stop`, the hub drains it. It
   waits until each run is terminal and its postmortem has finished, then archives the run. Next it
   removes the member and the target, closes the tunnel, deletes the server, waits for the delete
   action, and finally deletes the firewall.
6. **Reaper.** On start and every five minutes, the hub lists resources labelled
   `zeroshot/hub=<hub id>`. It deletes servers, legacy Volumes, primary IPs, and firewalls that state
   does not know about. It never deletes a resource whose lease state still holds.

The idle clock survives a restart. `idleSince` on the member record is the live observation, but
job records outlive it: when the member record is gone, or every job on the lease is terminal, the
clock starts at the newest `finishedAt` on that lease, and at the lease's creation time when it
never had a job. A restored lease therefore drains after `--idle` instead of being read as busy
forever because no member record carries its clock. A lease that still has a nonterminal job keeps
the member's own clock rather than a derived one, so a busy server is not torn down by age.

A restored lease also judges its runs by liveness rather than by the ledger. `state.json` can still
say `Running` for a run whose runtime is gone, so every pass checks the runs an `Active` lease holds
against the runs its own target lists. A run the target still lists answers for its job; a run the
target no longer lists is over, and the job fails `runtime_lost` with `failureClassification`
`run_unreachable` through the same settlement a deleted lease uses. A target that cannot be reached
at all proves nothing about its runs and stays with the
[lost member](hub-operations.md#lost-members-and-removing-a-member) path. Failing the run is what
starts the lease's idle clock, so the machine then drains after `--idle`. The settlement never
overrides `hold` or a provider-quota hold.

A server whose boot fails or times out is deleted. Its pool then pauses for 5 minutes, and the
pause doubles on each further failure up to an hour, so a broken image does not keep creating
billed servers.

`zeroshot hub submit` does not require you to know whether a live member or a pool will take the
job. When no free member can take it but a pool can serve its placement, submit parks the job as
queued and returns it without an error, because that pool starts a server for it; exit 3 with
`hub.no_capacity` remains for a placement that neither a free member nor a pool can serve now.
Use `zeroshot hub enqueue` when no free member can take the job now and no pool serves it.

## Deliberate stops and release

`zeroshot hub stop <JOB>` stops a job deliberately and marks it `stopped` rather than failed.
The lease that ran it is held (`hold: true` in `GET /v1/hub/pools`), so the idle rule does not
delete the machine while the operator still wants it. A new job placed on that member clears the
hold automatically. To give the machine back, run `zeroshot hub pool release <LEASE>`; the server
then follows the ordinary `--idle` rule and is drained and deleted. A held lease still counts
against the pool's `--max` because it is still billed.

This is separate from `hub force-stop`, which stops a run but leaves the idle rule in charge, and
from the daemon's `--max-stall` enforcement: a stalled job still fails with `stalled` and its
server is still deleted.

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
| `idle`      | The job finished or was deliberately stopped; the idle timer runs unless held.    | yes        |
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
the next pass.

A lease whose pool still exists is reconciled too. If it has made no progress for the pool's
`staleLeaseMs` and the provider no longer lists its server, the lease records `deleted` with a
`reason` naming the pool and the phase, and its slot is free for the next dispatch pass. A lease
that is still advancing phases inside the grace window is left alone. The API pool body defaults
`staleLeaseMs` to 30 minutes and is the way to tune it; `zeroshot hub pool add` uses that default.
The same path retires a lease stuck in `connecting`/`preparing` past the pool's `--wait-max`, or
in `draining` past the ten-minute drain deadline, even when the provider still lists the server.
The hub records the draining intent before asking the provider to delete that server; if the
delete fails, the lease stays `draining` with the provider's error and backs off before retrying
instead of leaking a running box. A running job on a lease retired by a bounded wait fails with
`failureClassification: "lease_retired"`, not `provider_confirmed_absent`.

Reconciliation leaves a lease alone when the provider cannot confirm the loss — an inconclusive
answer is not a deleted server, so the lease keeps draining and the pass reports the failed
confirmation. A lease that never received a server is not reconciled either: there is no lost
resource to report, so it keeps its pool from claiming removal. A lease already in `deleting`
confirms its own deletion and is never reconciled.

Every `hub.pool` record carries `kind: "hub.pool"`, like every hub event, and `scope`: `"lease"`
for a lease transition, `"pool"` for a pool-wide one. Pool-wide changes use `scope: "pool"` and the phases `added`, `removed`, `paused`, `resumed`,
`blocked`, `retrying`, and `shortage`. They carry the pool name and no lease, and a `reason` for
every phase except `added` and `removed`. `paused` follows a failed boot and says how many
failures in a row stopped new servers and for how long; `blocked` and `retrying` carry the
consecutive block `attempts` and the `untilMs` deadline; `resumed` follows an expired block, a
server that joined, or a `hub pool resume` that cleared the pause, the block, and the shortages.
`removed` is recorded only after every lease of the pool has recorded `deleted`.

An example notification for a lease that just became ready:

```json
{
  "eventId": "hub:57",
  "name": "hub",
  "timestamp": "2026-02-11T09:41:12.481Z",
  "data": {
    "kind": "hub.pool",
    "scope": "lease",
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
    "summary": {"phase": "ready", "node": null, "terminal": false, "reason": null}
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

- It allocates `/var/lib/zeroshot-data.btrfs` (mode 0600) with `fallocate`. The size is the free
  root disk minus an 8 GiB headroom, clamped to 5 GiB at the low end and 24 GiB at the high end,
  rounded down to whole GiB. It formats the file with `mkfs.btrfs` and loop-mounts it at
  `/mnt/zs`.
- The file is bounded because the root disk is ext4 and shared with Docker: the image, the
  container's writable layer, logs and any build the job runs outside `/var/lib/zeroshot` all
  live there. Sizing the file from the whole free disk (54 GiB on a 75 GB `ccx23`) left the
  build a few GiB, so the layer filled the disk and the run ledger failed with `database is
  full`. A bigger server type buys more root disk for that layer and more headroom, not a bigger
  data file: the 24 GiB cap stays.
- It mounts with `noatime,compress=zstd:1,discard` and adds a `nofail` line to `/etc/fstab`, so a
  reboot remounts the folder. `/mnt/zs` is made immutable before the mount, so nothing can write
  to the ext4 underneath. `discard` returns deleted extents to the loop file, and therefore to
  ext4, instead of letting the file grow until the disk is full.
- It runs `fstrim -av` once after the mount. On an ordinary drained teardown it removes the target
  container so its writable layer can be reclaimed, prunes unused images and volumes with `docker
  system prune -af --volumes`, trims the data folder, and only then deletes the server. A lease
  whose pool was already removed is deleted without this remote cleanup because the pool's SSH key
  is gone with the pool.
- After the mount it fails the boot with `disk_full` when `/` has less than 5 GiB free or the
  data folder has less than 3 GiB. The data floor is lower because a freshly formatted 5 GiB
  btrfs reports slightly under 5 GiB available. The hub reports `reason: "disk_full"` on the lease
  and counts it as a failed boot, so the pool pauses instead of silently recycling the box. The
  floor check is boot-only: a later fill of the 24 GiB data file, such as the run ledger or
  checkpoints, still surfaces as the opaque SQLite database-is-full error.
- The target starts with `CARGO_HOME=/var/lib/zeroshot/.cargo`,
  `CARGO_TARGET_DIR=/var/lib/zeroshot/.cargo-target` and `TMPDIR=/var/lib/zeroshot/tmp`, all
  created on the btrfs data folder first. A pool member executes submitted runs through the hosted
  capsule allocator, which pins those same three values under the capsule runtime home on btrfs
  and puts them in every lane's base environment. Codex keeps that `TMPDIR`; Claude, Pi and
  Copilot replace it with their own session scratch, which is also under the capsule storage root.
  Local CLI runs forward the container values through the local controller as well. `HOME` stays
  the container user's home, so a build that writes `$HOME/.rustup` remains in the container layer
  on ext4; the 8 GiB headroom covers that residual.
- The script then checks that `/mnt/zs` is btrfs. It creates a subvolume, takes a read-only
  snapshot and makes a reflink copy, then starts the target. If any step fails, the target is not
  started.
- Rerunning the script never reformats: a mounted or formatted folder is reused. Its log and an
  `ok` or `failed` marker are kept in `/var/lib/zeroshot-boot/`.

Provisioning a **btrfs root image** (`pool.image`) would remove the loop file entirely: the root
filesystem would already support reflinks, so there would be no ext4/Docker layer to protect and
no size arithmetic to get wrong. That is out of scope here; the bounded loop file is the fix for
current ext4 images.

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
  `queue_expired`, or `error`. `reconciled` counts leases recorded as deleted because their
  server was gone or a bounded wait expired, whether or not their pool still exists.

## Cost

Each type's hourly price per location is listed in `zeroshot hub pool types`. Hetzner bills by
the hour, so a server is billed for at least one hour. `--max` and `--idle` bound how many servers
run and how long an idle one survives; `--wait-max` bounds a stuck lease attempt, not a queue wait.
A long-running job costs its server's hourly price until that job finishes.
