# Run jobs on on-demand cloud servers

A hub pool creates a Hetzner Cloud server when a queued job needs one, joins it to the hub as a
member, and deletes it when it has been idle or has reached its TTL. Every run is archived on the
hub host before its server is deleted.

## Add a pool

```bash
zeroshot hub pool add burst \
  --provider hetzner --type cx33 --location fsn1 --image docker-ce \
  --target-image ghcr.io/the-open-engine/zeroshot-target:1.2.3 \
  --max 3 --idle 10m --ttl 2h --tag linux,burst \
  --ssh-key ~/.ssh/zeroshot-pool --ssh-from 203.0.113.7/32
zeroshot hub pool list
zeroshot hub pool remove burst   # only once the pool has no servers left
```

- `--image` is the server image: a snapshot ID or a system image such as `docker-ce`.
- `--target-image` is the zeroshot target container. It must be pinned: a digest or a tag other
  than `latest`.
- `--max` caps live servers. `--slots` (default 1) is concurrent runs per server.
- `--ssh-key` is a private key on the hub host. Only its `.pub` sibling goes to the server.
- `--ssh-from` is the hub's public address. The server firewall allows SSH from it and nothing
  else.

The daemon needs an API token, from `hub daemon run --hetzner-token-file FILE` or `HCLOUD_TOKEN`
in the daemon's environment. The token is sent only to the Hetzner API. It is never stored in hub
state, labels, user data, or logs.

## What happens

1. **Placement.** A pool with no live server still counts as eligible. A job that only a pool
   matches queues (exit 3 without `--queue`) instead of failing with exit 4.
2. **Scale up.** Each pass creates one server per queued job that needs the pool, up to `--max`.
   The server is named `zs-<job>` and labelled `zeroshot/hub`, `zeroshot/lease`, and
   `zeroshot/expires`, with IDs and times only.
3. **Boot.** cloud-init installs Docker if needed and runs the target image on `127.0.0.1:8080`
   with `/mnt/zs` mounted as its state. It also schedules `shutdown -h` shortly after the TTL as a
   backstop.
4. **Join.** The hub opens `ssh -N -L` to the server, reads target discovery through the tunnel,
   registers the target, and adds it as a member with the pool's tags. A dropped tunnel is
   reopened. Host keys are trusted on first use into the pool's own `known_hosts`.
5. **Teardown.** When a server is idle for `--idle` or reaches `--ttl`, the hub drains it. It
   waits until each run is terminal and its postmortem has finished, then archives the run. Next it
   removes the member and the target, closes the tunnel, deletes the server, waits for the delete
   action, and finally deletes the firewall.
6. **Reaper.** On start and every five minutes, the hub lists resources labelled
   `zeroshot/hub=<hub id>`. It deletes servers, primary IPs, and firewalls that state does not
   know about or that are past `zeroshot/expires`.

A server whose boot fails or times out is deleted. Its pool then pauses for 5 minutes, and the
pause doubles on each further failure up to an hour, so a broken image does not keep creating
billed servers.

## Archives

Before deletion, `hub/archive/<job id>/` receives `status.json`, `report.json`, `watch.ndjson`,
and `logs.ndjson`. The job records the folder, plus the run's `authored_graph_hash` when the
report has one. A server is never deleted while one of its runs is unarchived. A failed archive
keeps the server and is retried on the next pass. Object Storage upload is not built in.

## Cost

A CX33 costs about €0.0136 per hour. Hetzner bills by the hour, so a server is billed for at
least one hour. `--max`, `--idle`, `--ttl`, and the self-shutdown backstop bound the spend.
