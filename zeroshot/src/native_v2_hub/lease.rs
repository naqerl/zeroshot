//! The effects of pool leases, one step per lease and pass: create the server, wait for it to
//! boot, tunnel to it and register it as a member, keep its tunnel alive, and on idle or TTL
//! drain it, archive every run it held, remove it from the hub and the target registry, close
//! its tunnel, delete the server and its firewall, and confirm the deletion. The reaper deletes
//! labelled provider resources that the state does not hold or that outlived their deadline.
//!
//! Every provider failure is retried with backoff; an unconfirmed step never counts as done.
//! Nothing is deleted while a run on it is unarchived.

use std::path::PathBuf;
use std::sync::Arc;

use serde_json::{Value, json};

use super::archive;
use super::capacity::{self, Machine, PoolType};
use super::profile::{self, Spec};
use super::clock::now_ms;
use super::error::HubError;
use super::model::{HubState, Member, MemberState};
use super::pool::{
    self, LABEL_HUB, LABEL_LEASE, LABEL_LOCATION, LABEL_SERVER_TYPE, Lease, LeasePhase, Pool,
    Resource, ResourceKind, TeardownReason,
};
use super::provision::{self, Catalog, Progress, Provisioner, Refusal, ServerRequest};
use super::service::HubService;
use super::tunnel::{self, TunnelSpec, Tunnels};
use super::wire::AddPool;
use crate::native_v2_target::TargetRegistry;

/// Least time between reaper passes after the first: three list calls each, well inside the
/// provider's hourly request budget.
pub(crate) const REAP_INTERVAL_MS: u64 = 5 * 60_000;

/// Provider and tunnel effects of a hub's pools.
pub(crate) struct PoolRuntime {
    provisioner: Arc<dyn Provisioner>,
    tunnels: Arc<dyn Tunnels>,
    reaped_at: std::sync::Mutex<Option<u64>>,
}

impl PoolRuntime {
    pub(crate) fn new(provisioner: Arc<dyn Provisioner>, tunnels: Arc<dyn Tunnels>) -> Self {
        Self {
            provisioner,
            tunnels,
            reaped_at: std::sync::Mutex::new(None),
        }
    }

    /// Whether a reaper pass is due, and if so record it as started at `now`.
    fn reap_due(&self, now: u64) -> bool {
        let mut reaped_at = self
            .reaped_at
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if reaped_at.is_some_and(|at| now < at.saturating_add(REAP_INTERVAL_MS)) {
            return false;
        }
        *reaped_at = Some(now);
        true
    }
}

/// The daemon's runtime: Hetzner with the configured token and SSH tunnels; `None` when no
/// token is configured, so a hub without pools never needs one.
pub(crate) fn production_runtime(
    api: &zeroshot_engine::native_v2_cli::hub::HubApiOptions,
) -> Result<Option<PoolRuntime>, HubError> {
    let file = api
        .hetzner_token_file
        .as_deref()
        .map(super::paths::absolute)
        .transpose()?;
    let Some(token) = super::hetzner::Token::load(file.as_deref())? else {
        return Ok(None);
    };
    let provisioner = super::hetzner::Hetzner::new(super::hetzner::API, token)?;
    Ok(Some(PoolRuntime::new(
        Arc::new(provisioner),
        Arc::new(tunnel::SshTunnels::default()),
    )))
}

/// What one pool pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PoolReport {
    pub started: Vec<String>,
    pub teardowns: Vec<(String, TeardownReason)>,
    pub archived: Vec<String>,
    pub deleted: Vec<String>,
    pub reaped: Vec<(ResourceKind, u64)>,
    /// Leases given up because no candidate had capacity: `(lease, reason)`.
    pub shortages: Vec<(String, String)>,
    /// Pools stopped on an account limit or a rejected request: `(pool, error)`.
    pub blocked: Vec<(String, String)>,
    /// Jobs that waited longer than their pool's `--wait-max`.
    pub expired: Vec<String>,
    pub errors: Vec<String>,
}

impl PoolReport {
    pub(crate) fn view(&self) -> Value {
        json!({
            "started": self.started,
            "teardowns": self.teardowns.iter().map(|(name, reason)| json!({
                "lease": name,
                "reason": reason,
            })).collect::<Vec<_>>(),
            "archived": self.archived,
            "deleted": self.deleted,
            "reaped": self.reaped.iter().map(|(kind, id)| json!({
                "kind": format!("{kind:?}"),
                "id": id,
            })).collect::<Vec<_>>(),
            "shortages": self.shortages.iter().map(|(lease, reason)| json!({
                "lease": lease,
                "reason": reason,
            })).collect::<Vec<_>>(),
            "blocked": self.blocked.iter().map(|(pool, error)| json!({
                "pool": pool,
                "error": error,
            })).collect::<Vec<_>>(),
            "expired": self.expired,
            "errors": self.errors,
        })
    }
}

/// Outcome of one lease step.
enum Step {
    /// Replace the lease with this record.
    Update(Box<Lease>),
    /// The lease is finished and removed, after cleaning up anything a refused create attempt
    /// left under its label.
    Done(Box<Lease>),
    /// The lease never got a server; it is removed after cleaning up what its attempts left.
    Abandoned(Box<Lease>),
}

/// How creating a lease's server ended.
enum Creation {
    /// The provider accepted a server, or an earlier unconfirmed create left one; `true` when
    /// refused attempts came first and may have left resources behind.
    Created(bool),
    /// No candidate had capacity; these `(type, location)` pairs were tried.
    Short(Vec<(String, String)>),
    /// The provider refused for a reason a retry cannot fix.
    Blocked(String),
}

impl HubService {
    /// One pool pass: reap, track idleness, scale up, start due teardowns and step every due
    /// lease. Runs outside the placement lock except where it changes what placement may use.
    pub(crate) async fn pool_pass(&self) -> Result<PoolReport, HubError> {
        let mut report = PoolReport::default();
        let state = self.snapshot().await;
        if state.pools.is_empty() && state.leases.is_empty() {
            return Ok(report);
        }
        let Some(runtime) = self.pool_runtime() else {
            report
                .errors
                .push("hub pools need a provider token; set HCLOUD_TOKEN".to_owned());
            return Ok(report);
        };
        self.reap(runtime, &mut report).await;
        let now = now_ms();
        let (started, expired) = self.scale(now).await?;
        report.started = started;
        report.expired = expired;
        report.teardowns = self.begin_teardowns(now).await?;
        for lease in self.snapshot().await.leases {
            if !lease.due(now_ms()) {
                continue;
            }
            let name = lease.name.clone();
            let phase = lease.phase;
            let failed_boot = matches!(
                lease.reason,
                Some(TeardownReason::BootFailed | TeardownReason::BootTimeout)
            );
            let pool_name = lease.pool.clone();
            match self.step(runtime, lease, &mut report).await {
                Ok(Step::Update(next)) => self.store_lease(*next).await?,
                Ok(Step::Done(lease)) => {
                    self.sweep(runtime, &lease, &mut report).await;
                    self.mutate(|state| {
                        state.leases.retain(|lease| lease.name != name);
                        if failed_boot {
                            pool::record_boot(state, &pool_name, true, now_ms());
                        }
                        Ok(())
                    })
                    .await?;
                    report.deleted.push(name);
                }
                Ok(Step::Abandoned(lease)) => {
                    self.sweep(runtime, &lease, &mut report).await;
                    self.mutate(|state| {
                        state.leases.retain(|slot| slot.name != name);
                        Ok(())
                    })
                    .await?;
                }
                Err(error) => {
                    report.errors.push(format!("{name}: {error}"));
                    self.retry_lease(&name, phase, error.to_string()).await?;
                }
            }
        }
        Ok(report)
    }

    async fn store_lease(&self, next: Lease) -> Result<(), HubError> {
        self.mutate(|state| {
            if let Some(slot) = state.leases.iter_mut().find(|slot| slot.name == next.name) {
                *slot = next;
            }
            Ok(())
        })
        .await
    }

    async fn retry_lease(
        &self,
        name: &str,
        phase: LeasePhase,
        error: String,
    ) -> Result<(), HubError> {
        let now = now_ms();
        self.mutate(|state| {
            if let Some(lease) = state
                .leases
                .iter_mut()
                .find(|lease| lease.name == name && lease.phase == phase)
            {
                lease.retry(now, Some(error));
            }
            Ok(())
        })
        .await
    }

    /// Track idleness, record a lease for every server the queue needs, and tell queued pool
    /// jobs why they wait, expiring those past their pool's `--wait-max`. Returns the started
    /// leases and the expired jobs.
    async fn scale(&self, now: u64) -> Result<(Vec<String>, Vec<String>), HubError> {
        let mut ports = Vec::new();
        for _ in pool::scale_up(&self.snapshot().await, now) {
            ports.push(tunnel::free_port()?);
        }
        let _placing = self.placement_lock().await;
        self.mutate(|state| {
            pool::update_idle(state, now);
            let expired = capacity::note_waiting(state, now);
            let mut started = Vec::new();
            for ((pool_name, job_id, spec), port) in
                pool::scale_up(state, now).into_iter().zip(ports)
            {
                let Some(pool) = state.pools.iter().find(|pool| pool.name == pool_name) else {
                    continue;
                };
                let lease = Lease {
                    spec,
                    ..Lease::new(pool, &job_id, port, now)
                };
                started.push(lease.name.clone());
                state.leases.push(lease);
            }
            Ok((started, expired))
        })
        .await
    }

    /// Drain every lease due for teardown, so placement stops using it.
    async fn begin_teardowns(&self, now: u64) -> Result<Vec<(String, TeardownReason)>, HubError> {
        let _placing = self.placement_lock().await;
        self.mutate(|state| {
            let due = pool::teardown_due(state, now);
            for (name, reason) in &due {
                begin_teardown(state, name, *reason);
            }
            Ok(due)
        })
        .await
    }

    async fn step(
        &self,
        runtime: &PoolRuntime,
        mut lease: Lease,
        report: &mut PoolReport,
    ) -> Result<Step, HubError> {
        let state = self.snapshot().await;
        let pool = state.pools.iter().find(|pool| pool.name == lease.pool);
        match lease.phase {
            LeasePhase::Creating => {
                let pool = pool.ok_or_else(|| removed(&lease))?;
                // Without the hub ID the reaper could never find the server again.
                let hub_id = state.hub_id.as_deref().ok_or_else(|| {
                    HubError::InvalidState("hub state has no hub ID for pool labels".to_owned())
                })?;
                match self.create(runtime, (pool, hub_id), &mut lease).await? {
                    Creation::Created(false) => {}
                    Creation::Created(true) => self.sweep(runtime, &lease, report).await,
                    Creation::Short(tried) => {
                        let reason = self
                            .hold(&lease.pool, Hold::Short(&lease.spec, &tried))
                            .await?;
                        report.shortages.push((lease.name.clone(), reason));
                        return Ok(Step::Abandoned(Box::new(lease)));
                    }
                    Creation::Blocked(error) => {
                        self.hold(&lease.pool, Hold::Blocked(&error)).await?;
                        report.blocked.push((lease.pool.clone(), error));
                        return Ok(Step::Abandoned(Box::new(lease)));
                    }
                }
            }
            LeasePhase::Booting => boot(runtime, &mut lease).await?,
            LeasePhase::Connecting => {
                let pool = pool.ok_or_else(|| removed(&lease))?;
                self.connect(runtime, pool, &mut lease).await?;
            }
            LeasePhase::Active => {
                if !runtime.tunnels.is_open(&lease.name) {
                    let pool = pool.ok_or_else(|| removed(&lease))?;
                    runtime.tunnels.open(&self.tunnel_spec(pool, &lease)?)?;
                }
            }
            LeasePhase::Draining => {
                if !self.drain(runtime, &lease, report).await? {
                    return Ok(Step::Update(Box::new(lease)));
                }
                // A boot that failed or timed out may still hold its creation action; only a
                // deletion action may confirm the server gone.
                lease.action_id = None;
                lease.advance(LeasePhase::Deleting);
            }
            LeasePhase::Deleting => {
                if delete(runtime, &mut lease).await? {
                    return Ok(Step::Done(Box::new(lease)));
                }
            }
        }
        Ok(Step::Update(Box::new(lease)))
    }

    /// Create the lease's server, or adopt the one an earlier unconfirmed create made. Tries
    /// each fitting type of the pool in each of its locations, best first, skipping those the
    /// catalog shows unavailable and moving on after each capacity refusal. Account limits and
    /// rejected requests block the pool; rate limits, timeouts and server errors are retried.
    async fn create(
        &self,
        runtime: &PoolRuntime,
        (pool, hub_id): (&Pool, &str),
        lease: &mut Lease,
    ) -> Result<Creation, HubError> {
        let existing = runtime
            .provisioner
            .list_by_label(&lease_selector(hub_id, lease))
            .await?;
        if let Some(server) = existing
            .iter()
            .find(|resource| resource.kind == ResourceKind::Server)
        {
            lease.server_id = Some(server.id);
            let held = |kind| {
                existing
                    .iter()
                    .find(|resource| resource.kind == kind)
                    .map(|resource| resource.id)
            };
            lease.firewall_id = held(ResourceKind::Firewall);
            lease.volume_id = held(ResourceKind::Volume);
            lease.machine = adopted_machine(pool, server);
            lease.advance(LeasePhase::Booting);
            return Ok(Creation::Created(false));
        }
        let catalog = runtime.provisioner.catalog().await?;
        let public_key = read_public_key(pool)?;
        let mut tried = Vec::new();
        let mut refused = false;
        for (server_type, location) in capacity::candidates(pool, &lease.spec) {
            if !available(&catalog, &server_type.name, location) {
                tried.push((server_type.name.clone(), location.to_owned()));
                continue;
            }
            let mut labels = lease.labels(hub_id);
            labels.insert(LABEL_SERVER_TYPE.to_owned(), server_type.name.clone());
            labels.insert(LABEL_LOCATION.to_owned(), location.to_owned());
            let request = ServerRequest {
                name: lease.name.clone(),
                server_type: server_type.name.clone(),
                location: location.to_owned(),
                image: pool.image.clone(),
                user_data: provision::user_data(pool, lease, &public_key)?,
                labels,
                ssh_from: pool.ssh_from.clone(),
                volume_gb: pool.data.volume_gb,
            };
            match runtime.provisioner.create(&request).await {
                Ok(created) => {
                    lease.server_id = Some(created.server_id);
                    lease.firewall_id = created.firewall_id;
                    lease.volume_id = created.volume_id;
                    lease.action_id = created.action_id;
                    lease.machine = Some(machine(server_type, location));
                    lease.advance(LeasePhase::Booting);
                    self.hold(&pool.name, Hold::Clear(&lease.spec, &tried))
                        .await?;
                    return Ok(Creation::Created(refused));
                }
                Err(failure) => {
                    refused = true;
                    match failure.refusal {
                        Refusal::Capacity => {
                            tried.push((server_type.name.clone(), location.to_owned()));
                        }
                        Refusal::Quota | Refusal::Invalid => {
                            return Ok(Creation::Blocked(format!(
                                "{} in {location}: {}",
                                server_type.name, failure.message
                            )));
                        }
                        Refusal::RateLimited | Refusal::Transient => {
                            self.hold(&pool.name, Hold::Count(&tried)).await?;
                            return Err(failure.into());
                        }
                    }
                }
            }
        }
        Ok(Creation::Short(tried))
    }

    /// Record what a create attempt learned about the pool's capacity, under the state lock.
    /// Returns the shortage reason for [`Hold::Short`].
    async fn hold(&self, pool_name: &str, hold: Hold<'_>) -> Result<String, HubError> {
        let now = now_ms();
        self.mutate(|state| {
            let Some(pool) = state.pools.iter_mut().find(|pool| pool.name == pool_name) else {
                return Ok(String::new());
            };
            match hold {
                Hold::Clear(spec, tried) => {
                    capacity::count_unavailable(pool, tried);
                    pool.shortages.remove(&spec.key());
                }
                Hold::Count(tried) => capacity::count_unavailable(pool, tried),
                Hold::Short(spec, tried) => {
                    capacity::count_unavailable(pool, tried);
                    capacity::record_shortage(pool, spec, tried, now);
                    return Ok(pool
                        .shortages
                        .get(&spec.key())
                        .map(|shortage| shortage.reason.clone())
                        .unwrap_or_default());
                }
                Hold::Blocked(error) => pool.blocked = Some(error.to_owned()),
            }
            Ok(String::new())
        })
        .await
    }

    /// Delete whatever refused create attempts of a lease left behind, servers first, keeping
    /// the server, Volume and firewall the lease holds. What fails here stays labelled for the reaper.
    async fn sweep(&self, runtime: &PoolRuntime, lease: &Lease, report: &mut PoolReport) {
        let Some(hub_id) = self.snapshot().await.hub_id else {
            return;
        };
        let mut left = match runtime
            .provisioner
            .list_by_label(&lease_selector(&hub_id, lease))
            .await
        {
            Ok(left) => left,
            Err(error) => {
                report
                    .errors
                    .push(format!("{}: cleanup: {error}", lease.name));
                return;
            }
        };
        left.retain(|resource| {
            let held = match resource.kind {
                ResourceKind::Server => lease.server_id,
                ResourceKind::Firewall => lease.firewall_id,
                ResourceKind::Volume => lease.volume_id,
                ResourceKind::PrimaryIp => None,
            };
            held != Some(resource.id)
        });
        left.sort_by_key(|resource| resource.kind);
        for resource in &left {
            match runtime.provisioner.destroy(resource).await {
                Ok(_) => report.reaped.push((resource.kind, resource.id)),
                Err(error) => report.errors.push(format!(
                    "{}: cleanup of {:?} {}: {error}",
                    lease.name, resource.kind, resource.id
                )),
            }
        }
    }

    /// Tunnel to a booted server, register it as a direct target, check discovery through the
    /// tunnel, and add it as a member with the pool's tags.
    async fn connect(
        &self,
        runtime: &PoolRuntime,
        pool: &Pool,
        lease: &mut Lease,
    ) -> Result<(), HubError> {
        runtime.tunnels.open(&self.tunnel_spec(pool, lease)?)?;
        let origin = format!("http://127.0.0.1:{}", lease.local_port);
        match self.registry().get(&lease.name) {
            Ok(record) if record.origin.trim_end_matches('/') == origin => {}
            Ok(_) => {
                self.registry()
                    .remove(&lease.name)
                    .map_err(target_registry)?;
                self.registry()
                    .add_direct(&lease.name, &origin)
                    .map_err(target_registry)?;
            }
            Err(_) => self
                .registry()
                .add_direct(&lease.name, &origin)
                .map_err(target_registry)?,
        }
        let capabilities = self.driver().capabilities(&lease.name).await?;
        let now = now_ms();
        let member = Member {
            tags: profile::member_tags(pool),
            machine: lease.machine.clone(),
            spec: Some(lease.spec.clone()),
            slots: profile::slots_for(pool, &lease.spec),
            pool: Some(pool.name.clone()),
            server_id: lease.server_id,
            expires_at_ms: Some(lease.expires_at_ms),
            idle_since_ms: Some(now),
            capabilities: Some(capabilities),
            capabilities_at_ms: Some(now),
            ..Member::new(lease.name.clone(), now)
        };
        self.mutate(|state| {
            state
                .members
                .retain(|existing| existing.name != member.name);
            state.members.push(member);
            pool::record_boot(state, &pool.name, false, now);
            Ok(())
        })
        .await?;
        lease.advance(LeasePhase::Active);
        Ok(())
    }

    /// Once nothing runs on the lease's member, archive every run it held, then remove the
    /// member, its target and its tunnel. Returns whether the lease may be deleted.
    async fn drain(
        &self,
        runtime: &PoolRuntime,
        lease: &Lease,
        report: &mut PoolReport,
    ) -> Result<bool, HubError> {
        let state = self.snapshot().await;
        if !pool::drained(&state.jobs, &lease.name) {
            return Ok(false);
        }
        let root = self.store().directory().join("archive");
        for job in pool::unarchived(&state.jobs, &lease.name) {
            if !runtime.tunnels.is_open(&lease.name)
                && let Some(pool) = state.pools.iter().find(|pool| pool.name == lease.pool)
            {
                runtime.tunnels.open(&self.tunnel_spec(pool, lease)?)?;
            }
            let archived = archive::archive(self.driver(), &root, &lease.name, job).await?;
            let job_id = job.job_id.clone();
            self.mutate(|state| {
                if let Some(slot) = state.jobs.iter_mut().find(|slot| slot.job_id == job_id) {
                    slot.archive = Some(archived.folder.clone());
                    slot.graph_hash.clone_from(&archived.graph_hash);
                }
                Ok(())
            })
            .await?;
            report.archived.push(job.job_id.clone());
        }
        let _placing = self.placement_lock().await;
        let removed = self
            .mutate(|state| {
                if !pool::drained(&state.jobs, &lease.name)
                    || !pool::unarchived(&state.jobs, &lease.name).is_empty()
                {
                    return Ok(false);
                }
                state.members.retain(|member| member.name != lease.name);
                Ok(true)
            })
            .await?;
        if removed {
            self.registry()
                .remove(&lease.name)
                .map_err(target_registry)?;
            runtime.tunnels.close(&lease.name);
        }
        Ok(removed)
    }

    fn tunnel_spec(&self, pool: &Pool, lease: &Lease) -> Result<TunnelSpec, HubError> {
        let host = lease.ipv4.clone().ok_or_else(|| {
            HubError::InvalidState(format!("{} has no public address yet", lease.name))
        })?;
        Ok(TunnelSpec {
            name: lease.name.clone(),
            host,
            local_port: lease.local_port,
            key: pool.ssh_key.clone(),
            known_hosts: self.store().directory().join("pool").join("known_hosts"),
        })
    }

    /// Delete labelled provider resources this hub does not hold or that outlived their lease.
    async fn reap(&self, runtime: &PoolRuntime, report: &mut PoolReport) {
        let Some(hub_id) = self.snapshot().await.hub_id else {
            return;
        };
        if !runtime.reap_due(now_ms()) {
            return;
        }
        let resources = match runtime
            .provisioner
            .list_by_label(&format!("{LABEL_HUB}={hub_id}"))
            .await
        {
            Ok(resources) => resources,
            Err(error) => {
                report.errors.push(format!("reaper: {error}"));
                return;
            }
        };
        let state = self.snapshot().await;
        let mut orphans = pool::orphans(&resources, &state, now_ms());
        // Servers first: a Volume or firewall is deletable only once its server is gone.
        orphans.sort_by_key(|resource| resource.kind);
        for resource in orphans {
            match runtime.provisioner.destroy(resource).await {
                Ok(_) => report.reaped.push((resource.kind, resource.id)),
                Err(error) => report.errors.push(format!(
                    "reaper: {:?} {} ({}): {error}",
                    resource.kind, resource.id, resource.name
                )),
            }
        }
    }
}

fn begin_teardown(state: &mut HubState, name: &str, reason: TeardownReason) {
    if let Some(lease) = state.leases.iter_mut().find(|lease| lease.name == name) {
        lease.reason = Some(reason);
        lease.advance(LeasePhase::Draining);
    }
    if let Some(member) = state.members.iter_mut().find(|member| member.name == name) {
        member.state = MemberState::Drain;
    }
}

async fn boot(runtime: &PoolRuntime, lease: &mut Lease) -> Result<(), HubError> {
    let server_id = lease
        .server_id
        .ok_or_else(|| HubError::InvalidState(format!("{} has no server", lease.name)))?;
    match runtime
        .provisioner
        .wait_ready(server_id, lease.action_id)
        .await?
    {
        Progress::Done(Some(ipv4)) => {
            lease.ipv4 = Some(ipv4);
            lease.action_id = None;
            lease.advance(LeasePhase::Connecting);
        }
        Progress::Done(None) | Progress::Pending => lease.retry(now_ms(), None),
        Progress::Failed(error) => {
            lease.reason = Some(TeardownReason::BootFailed);
            lease.advance(LeasePhase::Draining);
            lease.error = Some(error);
        }
    }
    Ok(())
}

/// Delete the server, await its action, then delete its Volume, which the deletion detached,
/// and its firewall. Returns whether every
/// resource of the lease is confirmed gone.
async fn delete(runtime: &PoolRuntime, lease: &mut Lease) -> Result<bool, HubError> {
    if let Some(action_id) = lease.action_id {
        match runtime.provisioner.action(action_id).await? {
            Progress::Done(_) => {
                lease.action_id = None;
                lease.server_id = None;
            }
            Progress::Pending => {
                lease.retry(now_ms(), None);
                return Ok(false);
            }
            Progress::Failed(error) => {
                lease.action_id = None;
                lease.retry(now_ms(), Some(format!("deletion failed: {error}")));
                return Ok(false);
            }
        }
    }
    if let Some(server_id) = lease.server_id {
        let resource = resource(ResourceKind::Server, server_id, lease);
        match runtime.provisioner.destroy(&resource).await? {
            Some(action_id) => {
                lease.action_id = Some(action_id);
                lease.retry(now_ms(), None);
                return Ok(false);
            }
            None => lease.server_id = None,
        }
    }
    if let Some(volume_id) = lease.volume_id {
        let resource = resource(ResourceKind::Volume, volume_id, lease);
        runtime.provisioner.destroy(&resource).await?;
        lease.volume_id = None;
    }
    if let Some(firewall_id) = lease.firewall_id {
        let resource = resource(ResourceKind::Firewall, firewall_id, lease);
        runtime.provisioner.destroy(&resource).await?;
        lease.firewall_id = None;
    }
    Ok(true)
}

/// What a create attempt learned about a pool.
enum Hold<'a> {
    /// A server of this spec was created after these candidates were out of capacity.
    Clear(&'a Spec, &'a [(String, String)]),
    /// These candidates were out of capacity before a retryable error.
    Count(&'a [(String, String)]),
    /// Every candidate of this spec was out of capacity.
    Short(&'a Spec, &'a [(String, String)]),
    /// The provider refused for a reason only the operator can fix.
    Blocked(&'a str),
}

fn lease_selector(hub_id: &str, lease: &Lease) -> String {
    format!("{LABEL_HUB}={hub_id},{LABEL_LEASE}={}", lease.id)
}

/// Whether the catalog shows `server_type` creatable in `location` now. A location missing
/// from the catalog is tried anyway; the provider has the last word.
fn available(catalog: &Catalog, server_type: &str, location: &str) -> bool {
    catalog
        .location(location)
        .is_none_or(|location| location.available.contains(server_type))
}

fn machine(server_type: &PoolType, location: &str) -> Machine {
    Machine {
        server_type: server_type.name.clone(),
        location: location.to_owned(),
        cores: server_type.cores,
        memory_mb: server_type.memory_mb,
    }
}

/// The machine of a server an earlier, unconfirmed create left, from its labels.
fn adopted_machine(pool: &Pool, server: &Resource) -> Option<Machine> {
    let server_type = server.labels.get(LABEL_SERVER_TYPE)?;
    let location = server.labels.get(LABEL_LOCATION)?;
    pool.server_types
        .iter()
        .chain(
            pool.profiles
                .iter()
                .flat_map(|profile| &profile.server_types),
        )
        .find(|known| known.name == *server_type)
        .map(|known| machine(known, location))
}

fn resource(kind: ResourceKind, id: u64, lease: &Lease) -> Resource {
    Resource {
        kind,
        id,
        name: lease.name.clone(),
        labels: std::collections::BTreeMap::new(),
    }
}

fn read_public_key(pool: &Pool) -> Result<String, HubError> {
    let mut path: PathBuf = pool.ssh_key.clone().into_os_string().into();
    path.as_mut_os_string().push(".pub");
    std::fs::read_to_string(&path)
        .map_err(|error| HubError::storage(&format!("{} could not be read", path.display()), error))
}

fn removed(lease: &Lease) -> HubError {
    HubError::InvalidState(format!("pool {} of {} was removed", lease.pool, lease.name))
}

fn target_registry(error: impl std::fmt::Display) -> HubError {
    HubError::storage("target registry", error)
}

impl HubService {
    /// Add a pool after checking its server types and locations against the provider's
    /// catalog, which needs the provider token.
    pub(crate) async fn add_pool(&self, request: AddPool) -> Result<Value, HubError> {
        validate_pool(&request)?;
        let catalog = self.catalog().await?;
        let (server_types, locations) = capacity::resolve(
            (&request.server_types, &request.locations),
            request.multi_arch,
            &catalog,
        )?;
        let pool = Pool {
            name: request.name,
            provider: request.provider,
            server_types,
            locations,
            multi_arch: request.multi_arch,
            wait_max_ms: request.wait_max_ms,
            image: request.image,
            target_image: request.target_image,
            max: request.max,
            slots: request.slots.max(1),
            idle_ms: request.idle_ms,
            ttl_ms: request.ttl_ms,
            tags: request.tags,
            ssh_key: request.ssh_key,
            ssh_from: request.ssh_from,
            created_at_ms: now_ms(),
            boot_failures: 0,
            paused_until_ms: None,
            blocked: None,
            shortages: std::collections::BTreeMap::new(),
            profiles: Vec::new(),
            unavailable: std::collections::BTreeMap::new(),
            data: request.data,
        };
        read_public_key(&pool)?;
        self.mutate(|state| {
            if state
                .pools
                .iter()
                .any(|existing| existing.name == pool.name)
            {
                return Err(HubError::Conflict(format!(
                    "pool {} already exists; remove it first",
                    pool.name
                )));
            }
            if state.hub_id.is_none() {
                state.hub_id = Some(uuid::Uuid::now_v7().simple().to_string());
            }
            let document = pool_document(&pool, &state.leases);
            state.pools.push(pool);
            Ok(document)
        })
        .await
    }

    pub(crate) async fn list_pools(&self) -> Value {
        let state = self.snapshot().await;
        let now = now_ms();
        json!({
            "schema": "zeroshot.hub.pools/v1",
            "hubId": state.hub_id,
            "pools": state
                .pools
                .iter()
                .map(|pool| pool.view(&state.leases, now))
                .collect::<Vec<_>>(),
        })
    }

    /// The provider's server types, prices and locations.
    pub(crate) async fn pool_types(&self) -> Result<Value, HubError> {
        Ok(capacity::types_document(&self.catalog().await?))
    }

    pub(super) async fn catalog(&self) -> Result<Catalog, HubError> {
        let runtime = self.pool_runtime().ok_or_else(|| {
            HubError::Usage("hub pools need a provider token; set HCLOUD_TOKEN".to_owned())
        })?;
        runtime.provisioner.catalog().await
    }

    /// Let a pool start servers again: clear an account or request block, a capacity
    /// shortage and a failed-boot pause.
    pub(crate) async fn resume_pool(&self, name: &str) -> Result<Value, HubError> {
        self.mutate(|state| {
            let pool = state
                .pools
                .iter_mut()
                .find(|pool| pool.name == name)
                .ok_or_else(|| HubError::Usage(format!("pool {name} does not exist")))?;
            pool.blocked = None;
            pool.shortages.clear();
            pool.boot_failures = 0;
            pool.paused_until_ms = None;
            let pool = pool.clone();
            Ok(pool_document(&pool, &state.leases))
        })
        .await
    }

    /// Remove a pool. Its servers are drained, archived and deleted by the next passes.
    pub(crate) async fn remove_pool(&self, name: &str) -> Result<Value, HubError> {
        self.mutate(|state| {
            let before = state.pools.len();
            state.pools.retain(|pool| pool.name != name);
            if state.pools.len() == before {
                return Err(HubError::Usage(format!("pool {name} does not exist")));
            }
            let leases = state
                .leases
                .iter()
                .filter(|lease| lease.pool == name)
                .count();
            Ok(json!({
                "schema": "zeroshot.hub.pool/v1",
                "removed": name,
                "leasesToTearDown": leases,
            }))
        })
        .await
    }
}

fn pool_document(pool: &Pool, leases: &[Lease]) -> Value {
    json!({ "schema": "zeroshot.hub.pool/v1", "pool": pool.view(leases, now_ms()) })
}

fn validate_pool(request: &AddPool) -> Result<(), HubError> {
    let usage = |message: String| Err(HubError::Usage(message));
    let valid_name = !request.name.is_empty()
        && request.name.len() <= 32
        && request.name.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        });
    if !valid_name {
        return usage(format!(
            "pool name {:?} must be 1-32 lowercase letters, digits or dashes",
            request.name
        ));
    }
    if request.tags.is_empty() {
        return usage("a pool needs at least one --tags value; jobs reach it by tag".to_owned());
    }
    if request.max == 0 {
        return usage("--max must be at least 1".to_owned());
    }
    if request.ttl_ms == 0 || request.idle_ms == 0 {
        return usage("--idle and --ttl must be positive".to_owned());
    }
    if !provision::pinned(&request.target_image) {
        return usage(format!(
            "--target-image {:?} must be pinned to a tag other than latest or to a digest",
            request.target_image
        ));
    }
    if !request.ssh_key.is_absolute() {
        return usage("--ssh-key must be an absolute path on the hub host".to_owned());
    }
    if request.ssh_from.is_empty() || !request.ssh_from.iter().all(|cidr| valid_cidr(cidr)) {
        return usage(
            "--ssh-from needs the hub's public address as CIDRs, such as 203.0.113.7/32".to_owned(),
        );
    }
    let identifiers = request
        .server_types
        .iter()
        .map(|value| ("--type", value))
        .chain(request.locations.iter().map(|value| ("--location", value)))
        .chain([("--image", &request.image)]);
    for (flag, value) in identifiers {
        if value.is_empty() || !value.chars().all(|character| character.is_ascii_graphic()) {
            return usage(format!("{flag} {value:?} is not a provider identifier"));
        }
    }
    if let Some(0) = request.wait_max_ms {
        return usage("--wait-max must be positive".to_owned());
    }
    request.data.validate()
}

fn valid_cidr(cidr: &str) -> bool {
    let Some((address, prefix)) = cidr.split_once('/') else {
        return false;
    };
    match address.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(_)) => prefix.parse::<u8>().is_ok_and(|prefix| prefix <= 32),
        Ok(std::net::IpAddr::V6(_)) => prefix.parse::<u8>().is_ok_and(|prefix| prefix <= 128),
        Err(_) => false,
    }
}
