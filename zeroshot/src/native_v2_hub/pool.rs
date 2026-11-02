//! On-demand pool targets: the hub creates a cloud server when queued jobs need a pool, runs
//! the stock target image on it, registers it as a member, and deletes it once it is idle,
//! after archiving every run it held.
//!
//! This module owns the pool and lease records and the pure decisions (which jobs a pool serves,
//! how many servers to start, which leases to tear down, which provider resources are orphans).
//! [`super::lease`] performs the provider, tunnel, registry and archive effects.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::capacity::{self, Machine, PoolType, Shortage};
use super::health::{DataHealth, FILE_MODE};
use super::profile::{self, Spec, TagProfile};
use super::clock::rfc3339;
use super::model::{HubState, Job, JobState, Member, PlacementSpec, Requirements};

/// Label naming the hub that owns a provider resource.
pub(crate) const LABEL_HUB: &str = "zeroshot/hub";
/// Label naming the lease a provider resource belongs to.
pub(crate) const LABEL_LEASE: &str = "zeroshot/lease";
/// Label naming the server type a lease's server was created as.
pub(crate) const LABEL_SERVER_TYPE: &str = "zeroshot/server-type";
/// Label naming the location a lease's server was created in.
pub(crate) const LABEL_LOCATION: &str = "zeroshot/location";
/// How long a new server may take to boot and answer target discovery.
pub(crate) const BOOT_DEADLINE_MS: u64 = 15 * 60_000;
/// Longest wait between two provider polls of one lease.
const MAX_BACKOFF_MS: u64 = 60_000;
/// Pool lifecycle records kept for the `events/list` stream and `hub pool list`.
pub(crate) const POOL_EVENT_LIMIT: usize = 1024;

/// One observed pool or lease transition. Lease phases chain through `previous_phase`; a
/// pool-level change such as `added` or `removed` carries no lease.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PoolPhase {
    Requested,
    Creating,
    Booting,
    Preparing,
    Ready,
    InUse,
    Idle,
    Draining,
    Deleted,
    Failed,
    Added,
    Removed,
    Paused,
    Resumed,
    Blocked,
    Shortage,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PoolEventKind {
    Lease,
    Pool,
}

/// One recorded transition, in the order the hub performed it.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PoolEventRecord {
    pub sequence: u64,
    pub kind: PoolEventKind,
    pub pool: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<Machine>,
    pub phase: PoolPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_phase: Option<PoolPhase>,
    pub at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Provider {
    Hetzner,
}

/// One `hub pool add` configuration.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Pool {
    pub name: String,
    pub provider: Provider,
    /// Server types to create, best first, each with the pool's locations that offer it.
    pub server_types: Vec<PoolType>,
    /// Locations to try, in order, for each type.
    pub locations: Vec<String>,
    /// Whether the target image runs on ARM too, so ARM server types are allowed.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub multi_arch: bool,
    /// A queued job this pool serves expires after waiting this long for a server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_max_ms: Option<u64>,
    /// Provider image: a snapshot ID or an image name such as `docker-ce`.
    pub image: String,
    /// Pinned `zeroshot-target` image reference the server runs.
    pub target_image: String,
    /// Most servers this pool runs at once, in every lease phase.
    pub max: u32,
    pub slots: u32,
    pub idle_ms: u64,
    pub tags: Vec<String>,
    /// Private key the hub opens SSH tunnels with; its `.pub` sibling is installed on servers.
    pub ssh_key: PathBuf,
    /// Source CIDRs the server firewall admits on port 22: the hub's address.
    pub ssh_from: Vec<String>,
    pub created_at_ms: u64,
    /// Boots that failed or timed out in a row; reset when a server joins.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub boot_failures: u32,
    /// No server starts before this time after failed boots, so a broken image or location
    /// cannot start, and bill, a fresh server every pass.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused_until_ms: Option<u64>,
    /// Account limit or rejected request: no server starts until `hub pool resume`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked: Option<String>,
    /// Machines every candidate was out of capacity for, by [`Spec::key`]: jobs needing them
    /// retry with backoff.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub shortages: BTreeMap<String, Shortage>,
    /// Machines for jobs with a given tag (`hub pool tag set`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub profiles: Vec<TagProfile>,
    /// Capacity refusals by server type and location.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub unavailable: BTreeMap<String, BTreeMap<String, u64>>,
}

/// First pause after a failed boot; it doubles per further failure up to [`MAX_PAUSE_MS`].
pub(crate) const FIRST_PAUSE_MS: u64 = 5 * 60_000;
pub(crate) const MAX_PAUSE_MS: u64 = 60 * 60_000;

/// Record how a pool's boot ended: a failure pauses the pool's scale-up, with doubling pauses;
/// a server that joined resets it. Both are recorded as pool events, so a caller sees why no
/// server started instead of polling `hub pool ls`.
pub(crate) fn record_boot(state: &mut HubState, pool: &str, failed: bool, now: u64) {
    let Some(index) = state.pools.iter().position(|slot| slot.name == pool) else {
        return;
    };
    let change = {
        let slot = &mut state.pools[index];
        let held = slot.paused_until_ms.is_some() || slot.blocked.is_some();
        if failed {
            slot.boot_failures = slot.boot_failures.saturating_add(1);
            let doublings = slot.boot_failures.saturating_sub(1).min(8);
            let pause = FIRST_PAUSE_MS
                .saturating_mul(1 << doublings)
                .min(MAX_PAUSE_MS);
            slot.paused_until_ms = Some(now.saturating_add(pause));
            Some((
                PoolPhase::Paused,
                format!(
                    "{} failed boot(s) in a row; no server starts for {}s",
                    slot.boot_failures,
                    pause / 1000
                ),
            ))
        } else {
            slot.boot_failures = 0;
            slot.paused_until_ms = None;
            held.then(|| (PoolPhase::Resumed, "a server joined the hub".to_owned()))
        }
    };
    if let Some((phase, reason)) = change {
        record_pool_phase(
            state,
            pool,
            PoolPhaseChange {
                phase,
                reason: Some(reason),
                at: now,
            },
        );
    }
}

impl Pool {
    /// Whether this pool can run a job: the job names at least one tag besides size requests,
    /// every one of them is a pool tag or a profiled tag, a server type fits its machine (see
    /// [`profile::resolve_spec`]), it pins no target, and named targets can run it.
    pub(crate) fn serves(&self, spec: &PlacementSpec, requirements: &Requirements) -> bool {
        let known = profile::member_tags(self);
        let mut tags = capacity::ordinary_tags(&spec.tags).peekable();
        tags.peek().is_some()
            && spec.target.is_none()
            && requirements.remote_unsupported.is_none()
            && tags.all(|tag| known.contains(tag))
            && !capacity::candidates(self, &profile::resolve_spec(self, &spec.tags)).is_empty()
    }

    /// The data folder the pool's servers report: the last health check's filesystem, mode
    /// and size. A pool with no joined server yet reports the fixed layout it will use.
    fn data(&self, members: &[Member]) -> Value {
        members
            .iter()
            .filter(|member| member.pool.as_deref() == Some(self.name.as_str()))
            .find_map(|member| member.data.as_ref())
            .map_or_else(
                || json!({"fs": "btrfs", "mode": FILE_MODE, "sizeBytes": null}),
                DataHealth::view,
            )
    }

    pub(crate) fn view(&self, leases: &[Lease], members: &[Member], now: u64) -> Value {
        let mine = leases
            .iter()
            .filter(|lease| lease.pool == self.name)
            .map(Lease::view)
            .collect::<Vec<_>>();
        json!({
            "name": self.name,
            "provider": self.provider,
            "serverTypes": self.server_types,
            "locations": self.locations,
            "multiArch": self.multi_arch,
            "waitMaxMs": self.wait_max_ms,
            "image": self.image,
            "targetImage": self.target_image,
            "max": self.max,
            "slots": self.slots,
            "idleMs": self.idle_ms,
            "tags": self.tags,
            "sshKey": self.ssh_key,
            "sshFrom": self.ssh_from,
            "createdAt": rfc3339(self.created_at_ms),
            "bootFailures": self.boot_failures,
            "pausedUntil": self.paused_until_ms.map(rfc3339),
            "blocked": self.blocked,
            "shortages": self.shortages,
            "waiting": capacity::hold_reason(self, None, now),
            "profiles": self.profiles.iter().map(TagProfile::view).collect::<Vec<_>>(),
            "unavailable": self.unavailable,
            "data": self.data(members),
            "leases": mine,
        })
    }
}

/// The first pool that can run a job, in creation order.
pub(crate) fn serving<'a>(
    pools: &'a [Pool],
    spec: &PlacementSpec,
    requirements: &Requirements,
) -> Option<&'a Pool> {
    pools.iter().find(|pool| pool.serves(spec, requirements))
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LeasePhase {
    /// Decided; the server is not created yet.
    Creating,
    /// Created; waiting for the provider to report it running.
    Booting,
    /// Running; opening the tunnel and waiting for target discovery.
    Connecting,
    /// A hub member.
    Active,
    /// Draining: no new jobs; waiting for runs to finish and be archived.
    Draining,
    /// Member removed; waiting for the provider to confirm deletion.
    Deleting,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TeardownReason {
    Idle,
    BootTimeout,
    /// The provider reported the server failed or gone while it booted.
    BootFailed,
    PoolRemoved,
}

/// One pool server, from the scale-up decision until its deletion is confirmed.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Lease {
    /// Lease ID: the `zeroshot/lease` label value.
    pub id: String,
    /// Server, target and member name: `zs-ID`.
    pub name: String,
    pub pool: String,
    pub phase: LeasePhase,
    /// Hub-side port of the SSH tunnel to the target.
    pub local_port: u16,
    pub created_at_ms: u64,
    /// When the lease entered draining, so archive retries are bounded even when a target
    /// never answers again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drain_started_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub firewall_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ipv4: Option<String>,
    /// Provider action being awaited (create or delete).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<TeardownReason>,
    /// Consecutive failed or pending steps, for backoff.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_attempt_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// The machine the job that started this lease needs, and why.
    #[serde(default)]
    pub spec: Spec,
    /// The server type and location the provider accepted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<Machine>,
    /// Furthest pool phase already recorded for this lease; kept so a repeated observation
    /// never becomes a second event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_phase: Option<PoolPhase>,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_zero(value: &u32) -> bool {
    *value == 0
}

/// The setup chain a lease follows before it can take jobs. Later phases (`in_use`, `idle`)
/// depend on placement and are recorded by the callers that perform them.
const SETUP_PHASES: &[PoolPhase] = &[
    PoolPhase::Requested,
    PoolPhase::Creating,
    PoolPhase::Booting,
    PoolPhase::Preparing,
    PoolPhase::Ready,
];

fn setup_index(phase: PoolPhase) -> Option<usize> {
    SETUP_PHASES
        .iter()
        .position(|candidate| *candidate == phase)
}

/// One transition to record, with the time the hub performed it.
pub(crate) struct PoolPhaseChange {
    pub phase: PoolPhase,
    pub reason: Option<String>,
    pub at: u64,
}

/// Record one pool-level change: `added`, `removed`, `paused`, `resumed`, `blocked`, or
/// `shortage`.
pub(crate) fn record_pool_phase(state: &mut HubState, pool: &str, change: PoolPhaseChange) {
    state.push_pool_event(PoolEventRecord {
        sequence: 0,
        kind: PoolEventKind::Pool,
        pool: pool.to_owned(),
        lease_id: None,
        server_id: None,
        member_id: None,
        machine: None,
        phase: change.phase,
        previous_phase: None,
        at: rfc3339(change.at),
        reason: change.reason,
    });
}

/// Record one lease change once, chaining every setup phase it passed through so a fast boot
/// still reports `creating`, `booting`, and `preparing` in order. Returns whether this call
/// emitted anything.
pub(crate) fn record_lease_phase(
    state: &mut HubState,
    lease: &mut Lease,
    change: PoolPhaseChange,
) -> bool {
    if lease.event_phase == Some(change.phase) {
        return false;
    }
    let previous = lease.event_phase;
    let chain = match (previous.and_then(setup_index), setup_index(change.phase)) {
        (Some(from), Some(to)) if to > from => SETUP_PHASES[from + 1..=to].to_vec(),
        _ => vec![change.phase],
    };
    let member = matches!(
        change.phase,
        PoolPhase::Ready
            | PoolPhase::InUse
            | PoolPhase::Idle
            | PoolPhase::Draining
            | PoolPhase::Deleted
            | PoolPhase::Failed
    )
    .then(|| lease.name.clone());
    let mut prior = previous;
    for (index, step) in chain.iter().copied().enumerate() {
        let last = index + 1 == chain.len();
        let step_member = if matches!(
            step,
            PoolPhase::Ready
                | PoolPhase::InUse
                | PoolPhase::Idle
                | PoolPhase::Draining
                | PoolPhase::Deleted
        ) {
            member.clone()
        } else {
            None
        };
        state.push_pool_event(PoolEventRecord {
            sequence: 0,
            kind: PoolEventKind::Lease,
            pool: lease.pool.clone(),
            lease_id: Some(lease.id.clone()),
            server_id: lease.server_id,
            member_id: step_member,
            machine: lease.machine.clone(),
            phase: step,
            previous_phase: prior,
            at: rfc3339(change.at),
            reason: last.then(|| change.reason.clone()).flatten(),
        });
        prior = Some(step);
    }
    lease.event_phase = Some(change.phase);
    true
}

/// Whether every lease that ever recorded an event for `pool` also recorded `deleted`.
/// A lease that failed before it had a machine never records `deleted`, so its pool is not
/// reported removed even though no server remains.
pub(crate) fn every_lease_deleted(state: &HubState, pool: &str) -> bool {
    let mut leases: Vec<&str> = Vec::new();
    let mut deleted: Vec<&str> = Vec::new();
    for event in &state.pool_events {
        if event.kind != PoolEventKind::Lease || event.pool != pool {
            continue;
        }
        let Some(id) = event.lease_id.as_deref() else {
            continue;
        };
        if !leases.contains(&id) {
            leases.push(id);
        }
        if event.phase == PoolPhase::Deleted && !deleted.contains(&id) {
            deleted.push(id);
        }
    }
    leases.iter().all(|id| deleted.contains(id))
}

/// Record `phase` for the lease that owns `member`, if any: placement (`in_use`) and
/// idleness (`idle`) happen outside a lease step.
pub(crate) fn record_member_phase(
    state: &mut HubState,
    member: &str,
    phase: PoolPhase,
    at: u64,
) -> bool {
    let Some(index) = state.leases.iter().position(|lease| lease.name == member) else {
        return false;
    };
    let mut lease = state.leases[index].clone();
    let changed = record_lease_phase(
        state,
        &mut lease,
        PoolPhaseChange {
            phase,
            reason: None,
            at,
        },
    );
    state.leases[index] = lease;
    changed
}

impl Lease {
    /// A lease decided at `now` for `job_id`, tunnelled through `local_port`.
    pub(crate) fn new(pool: &Pool, job_id: &str, local_port: u16, now: u64) -> Self {
        let id = lease_id(job_id);
        Self {
            name: format!("zs-{id}"),
            id,
            pool: pool.name.clone(),
            phase: LeasePhase::Creating,
            local_port,
            created_at_ms: now,
            drain_started_ms: None,
            server_id: None,
            firewall_id: None,
            ipv4: None,
            action_id: None,
            reason: None,
            attempts: 0,
            next_attempt_ms: None,
            error: None,
            spec: Spec::default(),
            machine: None,
            event_phase: None,
        }
    }

    /// Labels for every provider resource of this lease. IDs only, never secrets.
    pub(crate) fn labels(&self, hub_id: &str) -> BTreeMap<String, String> {
        BTreeMap::from([
            (LABEL_HUB.to_owned(), hub_id.to_owned()),
            (LABEL_LEASE.to_owned(), self.id.clone()),
        ])
    }

    pub(crate) fn due(&self, now: u64) -> bool {
        self.next_attempt_ms.is_none_or(|at| at <= now)
    }

    /// Record a step that must be retried later, with exponential backoff.
    pub(crate) fn retry(&mut self, now: u64, error: Option<String>) {
        self.attempts = self.attempts.saturating_add(1);
        let delay = 2_000_u64
            .saturating_mul(1 << self.attempts.min(16))
            .min(MAX_BACKOFF_MS);
        self.next_attempt_ms = Some(now.saturating_add(delay));
        if error.is_some() {
            self.error = error;
        }
    }

    /// Enter `phase` with a fresh backoff.
    pub(crate) fn advance(&mut self, phase: LeasePhase) {
        self.phase = phase;
        self.attempts = 0;
        self.next_attempt_ms = None;
        self.error = None;
    }

    pub(crate) fn view(&self) -> Value {
        json!({
            "id": self.id,
            "name": self.name,
            "phase": self.phase,
            "eventPhase": self.event_phase,
            "serverId": self.server_id,
            "localPort": self.local_port,
            "createdAt": rfc3339(self.created_at_ms),
            "reason": self.reason,
            "attempts": self.attempts,
            "error": self.error,
            "spec": self.spec,
            "machine": self.machine.as_ref().map(Machine::view),
        })
    }
}

/// Lease ID for a job: its ID without the `hj_` prefix, lowercased and reduced to a valid
/// hostname label, or a fresh UUID when nothing usable remains.
pub(crate) fn lease_id(job_id: &str) -> String {
    let id = job_id
        .strip_prefix("hj_")
        .unwrap_or(job_id)
        .to_ascii_lowercase()
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '-')
        .take(56)
        .collect::<String>();
    let id = id.trim_matches('-').to_owned();
    if id.is_empty() {
        uuid::Uuid::now_v7().simple().to_string()
    } else {
        id
    }
}

/// A server to start: its pool, the job it is for, and the machine that job needs.
pub(crate) type Decision = (String, String, Spec);

/// Servers to start. Demand is the queued, due jobs the pool serves (each counted for its first
/// serving pool only) that are still queued after placement, in slots, per machine spec;
/// supply is leases of that spec still starting. Every lease the pool holds counts toward `max`,
/// since every one of them is billed. A blocked or boot-paused pool starts none; a capacity
/// shortage holds only the jobs of its spec.
pub(crate) fn scale_up(state: &HubState, now: u64) -> Vec<Decision> {
    let mut decisions = Vec::new();
    for pool in &state.pools {
        if !capacity::may_start(pool, now) {
            continue;
        }
        let waiting = state
            .jobs
            .iter()
            .filter(|job| job.state == JobState::Queued)
            .filter(|job| job.not_before_ms.is_none_or(|at| at <= now))
            .filter(|job| {
                serving(&state.pools, &job.placement, &job.requirements)
                    .is_some_and(|first| first.name == pool.name)
            })
            .collect::<Vec<&Job>>();
        let held = state.leases.iter().filter(|lease| lease.pool == pool.name);
        let starting = held.clone().filter(|lease| {
            matches!(
                lease.phase,
                LeasePhase::Creating | LeasePhase::Booting | LeasePhase::Connecting
            )
        });
        // Jobs without a lease of their own, and how many jobs starting leases already carry.
        let mut specs = BTreeMap::<Spec, (Vec<&Job>, u64)>::new();
        for job in waiting {
            let spec = profile::resolve_spec(pool, &job.placement.tags);
            if capacity::short(pool, &spec, now) {
                continue;
            }
            let entry = specs.entry(spec).or_default();
            if starting
                .clone()
                .any(|lease| lease.id == lease_id(&job.job_id))
            {
                entry.1 += 1;
            } else if !held.clone().any(|lease| lease.id == lease_id(&job.job_id)) {
                entry.0.push(job);
            }
        }
        let mut room = u64::from(pool.max).saturating_sub(held.clone().count() as u64);
        for (spec, (jobs, carried)) in specs {
            let slots = u64::from(profile::slots_for(pool, &spec));
            let free = (starting.clone().filter(|lease| lease.spec == spec).count() as u64)
                .saturating_mul(slots)
                .saturating_sub(carried);
            let needed = (jobs.len() as u64)
                .saturating_sub(free)
                .div_ceil(slots)
                .min(room);
            room -= needed;
            decisions.extend(
                jobs.iter()
                    .step_by(usize::try_from(slots).unwrap_or(1))
                    .take(usize::try_from(needed).unwrap_or(0))
                    .map(|job| (pool.name.clone(), job.job_id.clone(), spec.clone())),
            );
        }
    }
    decisions
}

/// Track idleness of pool members: idle since the first pass that saw no hub job running or
/// placing on them.
pub(crate) fn update_idle(state: &mut HubState, now: u64) {
    let mut became_idle = Vec::new();
    {
        let jobs = &state.jobs;
        for member in state
            .members
            .iter_mut()
            .filter(|member| member.pool.is_some())
        {
            let busy = jobs.iter().any(|job| {
                job.state == JobState::Running
                    && job.target.as_deref() == Some(member.name.as_str())
            });
            if busy {
                member.idle_since_ms = None;
            } else if member.idle_since_ms.is_none() {
                member.idle_since_ms = Some(now);
                became_idle.push(member.name.clone());
            }
        }
    }
    for name in became_idle {
        let was_in_use = state
            .leases
            .iter()
            .find(|lease| lease.name == name)
            .is_some_and(|lease| lease.event_phase == Some(PoolPhase::InUse));
        if was_in_use {
            record_member_phase(state, &name, PoolPhase::Idle, now);
        }
    }
}

/// Leases that must start their teardown now, with the reason.
pub(crate) fn teardown_due(state: &HubState, now: u64) -> Vec<(String, TeardownReason)> {
    let mut due = Vec::new();
    for lease in &state.leases {
        let pool = state.pools.iter().find(|pool| pool.name == lease.pool);
        let reason = match lease.phase {
            LeasePhase::Draining | LeasePhase::Deleting => None,
            _ if pool.is_none() => Some(TeardownReason::PoolRemoved),
            LeasePhase::Creating | LeasePhase::Booting | LeasePhase::Connecting => (now
                >= lease.created_at_ms.saturating_add(BOOT_DEADLINE_MS))
            .then_some(TeardownReason::BootTimeout),
            LeasePhase::Active => {
                let idle_ms = pool.map_or(0, |pool| pool.idle_ms);
                state
                    .member(&lease.name)
                    .and_then(|member| member.idle_since_ms)
                    .filter(|since| now >= since.saturating_add(idle_ms))
                    .map(|_| TeardownReason::Idle)
            }
        };
        if let Some(reason) = reason {
            due.push((lease.name.clone(), reason));
        }
    }
    due
}

/// Jobs that ran on `member` and still need an archive before its server may be deleted:
/// every job with a run ID on it. A job not yet terminal blocks deletion too.
pub(crate) fn unarchived<'a>(jobs: &'a [Job], member: &str) -> Vec<&'a Job> {
    jobs.iter()
        .filter(|job| job.target.as_deref() == Some(member))
        .filter(|job| {
            job.run_id.is_some() && job.archive.is_none() && job.archive_skipped.is_none()
        })
        .collect()
}

/// Whether every hub job on `member` has finished: nothing running or placing there.
pub(crate) fn drained(jobs: &[Job], member: &str) -> bool {
    !jobs
        .iter()
        .any(|job| job.state == JobState::Running && job.target.as_deref() == Some(member))
}

/// A labelled provider resource found by the reaper.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Resource {
    pub kind: ResourceKind,
    pub id: u64,
    pub name: String,
    pub labels: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ResourceKind {
    Server,
    /// A Hetzner Volume left by an older pool; the reaper deletes those it no longer holds.
    Volume,
    PrimaryIp,
    Firewall,
}

/// Resources the reaper deletes: those whose lease the state does not hold. A resource of a
/// held lease is kept until the lease's own teardown removes it: nothing is deleted before its
/// runs are archived.
pub(crate) fn orphans<'a>(resources: &'a [Resource], state: &HubState) -> Vec<&'a Resource> {
    resources
        .iter()
        .filter(|resource| {
            !resource
                .labels
                .get(LABEL_LEASE)
                .is_some_and(|id| state.leases.iter().any(|lease| lease.id == *id))
        })
        .collect()
}
