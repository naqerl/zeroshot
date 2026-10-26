# Run jobs on on-demand cloud servers

A hub pool creates a Hetzner Cloud server when a queued job needs one, joins it to the hub as a
member, and deletes it when it has been idle or has reached its TTL. Every run is archived on the
hub host before its server is deleted.

## Add a pool

Choose the server types first. `hub pool types` lists the provider's current types with vCPUs,
memory, architecture, hourly price per location and where each is available. Prices change, so
check them there rather than in this guide.

```bash
zeroshot hub pool types
zeroshot hub pool add burst \
  --provider hetzner --type cpx31,cx43 --location fsn1,nbg1 --image docker-ce \
  --target-image ghcr.io/the-open-engine/zeroshot-target:1.2.3 \
  --max 3 --idle 10m --ttl 2h --tag linux,burst \
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
- `--ssh-key` is a private key on the hub host. Only its `.pub` sibling goes to the server.
- `--ssh-from` is the hub's public address. The server firewall allows SSH from it and nothing
  else.

The pool's `.pub` key is also registered with the provider and attached to the server by its
provider key ID. Cloud-init alone is not enough: a key written only to `ssh_authorized_keys` leaves
the login account's password expired, so PAM refuses the key session with "Password change required
but no TTY available". The boot configuration also marks the password as non-expiring so the key
session works.

- `--data-fs` is the data folder's filesystem. Only `btrfs` (the default) is supported; see
  [The btrfs data folder](#the-btrfs-data-folder).
- `--data-size GB` sizes the btrfs data file on the server's local disk. Without it the file takes
  the free disk after the target image, less 8 GB of headroom.
- `--data-volume GB` puts the data folder on a Hetzner Volume of that size (10 to 10240 GB)
  instead, created and deleted with the server. It excludes `--data-size`.

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

1. **Placement.** A pool with no live server still counts as eligible. A job that only a pool
   matches queues (exit 3 without `--queue`) instead of failing with exit 4.
2. **Scale up.** Each pass creates servers for queued jobs that no live or starting server can
   take, one per `--slots` jobs of the same machine choice, up to `--max`.
   The server is named `zs-<job>` and labelled `zeroshot/hub`, `zeroshot/lease`,
   `zeroshot/expires`, `zeroshot/server-type` and `zeroshot/location`, with no secrets.
3. **Boot.** cloud-init schedules `shutdown -h` shortly after the TTL as a backstop, then runs
   the boot script: it installs Docker and btrfs-progs if needed, pulls the target image,
   mounts the btrfs data folder at `/mnt/zs`, and runs the target on `127.0.0.1:8080` with
   `/mnt/zs` as its state.
4. **Join.** The hub opens `ssh -N -L` to the server and runs the data health check over SSH.
   Then it reads target discovery through the tunnel,
   registers the target, and adds it as a member with the pool's and its profiles' tags. A dropped tunnel is
   reopened. Host keys are trusted on first use into the pool's own `known_hosts`.
5. **Teardown.** When a server is idle for `--idle` or reaches `--ttl`, the hub drains it. It
   waits until each run is terminal and its postmortem has finished, then archives the run. Next it
   removes the member and the target, closes the tunnel, deletes the server, waits for the delete
   action, and finally deletes the Volume, if any, and the firewall.
6. **Reaper.** On start and every five minutes, the hub lists resources labelled
   `zeroshot/hub=<hub id>`. It deletes servers, Volumes, primary IPs, and firewalls that state does not
   know about or that are past `zeroshot/expires`.

A server whose boot fails or times out is deleted. Its pool then pauses for 5 minutes, and the
pause doubles on each further failure up to an hour, so a broken image does not keep creating
billed servers.

## The btrfs data folder

The target keeps run workspaces, checkpoint staging and its Restic repository under its storage
folder, and copies workspaces with reflinks (`FICLONE`). Hetzner root disks are ext4, which has no
reflinks, so every pool server keeps its data on btrfs. zeroshot uses only reflinks: no
subvolumes, no `btrfs` command, and no extra container privileges.

The boot script is written by the hub into cloud-init user data, so any server image with
Docker or apt/dnf works. No custom image is needed. The script runs with `set -eu`:

- **File mode** (the default) allocates `/var/lib/zeroshot-data.btrfs` (mode 0600) with
  `fallocate`. It formats the file with `mkfs.btrfs` and loop-mounts it at `/mnt/zs`.
- **Volume mode** (`--data-volume`) waits for the attached Volume under
  `/dev/disk/by-id/scsi-0HC_Volume_*`. It formats the Volume only if it holds no filesystem, and
  fails if it holds a filesystem other than btrfs.
- Both mount with `noatime,compress=zstd:1` and add a `nofail` line to `/etc/fstab`, so a reboot
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

The member then shows the checked filesystem, mode and size under `data` in `zeroshot hub list`.
While the boot script still runs, or SSH fails, the check is retried until the boot deadline. A
failed boot script or check counts as a failed boot: the server is deleted and the pool pauses.

A Volume is created unformatted, in the server's location, with the same labels. It is attached
when the server is created and deleted after the server, and the reaper deletes it if the hub no
longer holds it. A Volume makes boots slower and is billed separately; see the provider's
prices. The file mode needs no extra resource.

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

## Cost

Each type's hourly price per location is listed in `zeroshot hub pool types`. Hetzner bills by
the hour, so a server is billed for at least one hour. `--max`, `--idle`, `--ttl`, `--wait-max`,
and the self-shutdown backstop bound the spend.
