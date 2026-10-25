//! On-demand pool targets against a fake provider and fake tunnels: no cloud is ever called.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::json;

use super::super::archive::{self, FILES};
use super::super::capacity;
use super::super::clock::now_ms;
use super::super::data::DataSettings;
use super::super::error::{EXIT_NO_CAPACITY, EXIT_NO_ELIGIBLE_TARGET, HubError};
use super::super::lease::{PoolRuntime, REAP_INTERVAL_MS, production_runtime};
use super::super::model::{HubState, Job, JobState, MemberState, STATE_VERSION, Strategy};
use super::super::pool::{
    self, BACKSTOP_GRACE_MS, BOOT_DEADLINE_MS, FIRST_PAUSE_MS, MAX_PAUSE_MS, LABEL_EXPIRES,
    LABEL_HUB, LABEL_LEASE, LABEL_LOCATION, LABEL_SERVER_TYPE, Lease, LeasePhase, Pool, Provider,
    Resource, ResourceKind, TeardownReason,
};
use super::super::provision::{
    self, Architecture, Catalog, Created, Failure, Location, Progress, Provisioner, Refusal,
    ServerRequest, ServerType,
};
use super::super::service::HubService;
use super::super::store::{HubStore, migrate};
use super::super::tunnel::{TunnelSpec, Tunnels, ssh_arguments};
use super::super::wire::AddPool;
use super::{FakeDriver, job, spec};
use crate::native_v2_target::{FileTargetRegistry, TargetRegistry};

pub(super) const TAG: &str = "hetzner";
pub(super) const IMAGE: &str = "ghcr.io/the-open-engine/zeroshot-target:0.9.0";

/// A provider in memory: resources by kind and ID, plus every call it saw.
#[derive(Default)]
pub(super) struct FakeCloud {
    inner: Mutex<Cloud>,
}

#[derive(Default)]
pub(super) struct Cloud {
    pub(super) next_id: u64,
    pub(super) resources: BTreeMap<(ResourceKind, u64), Resource>,
    pub(super) created: Vec<ServerRequest>,
    pub(super) destroyed: Vec<(ResourceKind, u64)>,
    pub(super) lists: Vec<String>,
    /// Readiness answer; `None` means running with an address.
    pub(super) ready: Option<Progress>,
    /// Refuse every create with this.
    pub(super) refuse_all: Option<Refusal>,
    /// Refuse creates of a server type in a location.
    pub(super) refuse: BTreeMap<(String, String), Refusal>,
    /// A refused create leaves its labelled firewall behind, as a half-done create can.
    pub(super) leave_partial: bool,
    /// Every create attempted, as (server type, location), refused or not.
    pub(super) attempts: Vec<(String, String)>,
    /// The catalog; `None` serves [`catalog_fixture`].
    pub(super) catalog: Option<Catalog>,
    pub(super) catalog_fails: bool,
    /// Delete actions still pending before they succeed.
    pub(super) pending_deletes: u32,
}

impl FakeCloud {
    pub(super) fn with<R>(&self, read: impl FnOnce(&mut Cloud) -> R) -> R {
        read(&mut self.inner.lock().expect("cloud lock"))
    }

    pub(super) fn servers(&self) -> usize {
        self.with(|cloud| {
            cloud
                .resources
                .keys()
                .filter(|(kind, _)| *kind == ResourceKind::Server)
                .count()
        })
    }

    pub(super) fn holds(&self, kind: ResourceKind, id: u64) -> bool {
        self.with(|cloud| cloud.resources.contains_key(&(kind, id)))
    }

    pub(super) fn plant(&self, kind: ResourceKind, labels: &[(&str, &str)]) -> u64 {
        self.with(|cloud| {
            cloud.next_id += 1;
            let id = cloud.next_id;
            let labels = labels
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect();
            cloud.resources.insert(
                (kind, id),
                Resource {
                    kind,
                    id,
                    name: format!("planted-{id}"),
                    labels,
                },
            );
            id
        })
    }
}

#[async_trait]
impl Provisioner for FakeCloud {
    async fn create(&self, request: &ServerRequest) -> Result<Created, Failure> {
        self.with(|cloud| {
            let place = (request.server_type.clone(), request.location.clone());
            cloud.attempts.push(place.clone());
            if let Some(refusal) = cloud
                .refuse_all
                .or_else(|| cloud.refuse.get(&place).copied())
            {
                if cloud.leave_partial {
                    cloud.next_id += 1;
                    let id = cloud.next_id;
                    cloud.resources.insert(
                        (ResourceKind::Firewall, id),
                        Resource {
                            kind: ResourceKind::Firewall,
                            id,
                            name: request.name.clone(),
                            labels: request.labels.clone(),
                        },
                    );
                }
                return Err(Failure::new(
                    refusal,
                    format!("create refused: {refusal:?} for {} in {}", place.0, place.1),
                ));
            }
            cloud.created.push(request.clone());
            let mut ids = Vec::new();
            for kind in [ResourceKind::Firewall, ResourceKind::Server] {
                cloud.next_id += 1;
                ids.push(cloud.next_id);
                cloud.resources.insert(
                    (kind, cloud.next_id),
                    Resource {
                        kind,
                        id: cloud.next_id,
                        name: request.name.clone(),
                        labels: request.labels.clone(),
                    },
                );
            }
            Ok(Created {
                server_id: ids[1],
                firewall_id: Some(ids[0]),
                action_id: Some(ids[1] + 1000),
            })
        })
    }

    async fn catalog(&self) -> Result<Catalog, HubError> {
        self.with(|cloud| {
            if cloud.catalog_fails {
                return Err(HubError::Provider("catalog unavailable".to_owned()));
            }
            Ok(cloud.catalog.clone().unwrap_or_else(catalog_fixture))
        })
    }

    async fn wait_ready(&self, _: u64, _: Option<u64>) -> Result<Progress, HubError> {
        Ok(self.with(|cloud| {
            cloud
                .ready
                .clone()
                .unwrap_or_else(|| Progress::Done(Some("203.0.113.10".to_owned())))
        }))
    }

    async fn destroy(&self, resource: &Resource) -> Result<Option<u64>, HubError> {
        Ok(self.with(|cloud| {
            cloud.destroyed.push((resource.kind, resource.id));
            let existed = cloud
                .resources
                .remove(&(resource.kind, resource.id))
                .is_some();
            (existed && resource.kind == ResourceKind::Server).then_some(resource.id + 2000)
        }))
    }

    async fn action(&self, _: u64) -> Result<Progress, HubError> {
        Ok(self.with(|cloud| {
            if cloud.pending_deletes > 0 {
                cloud.pending_deletes -= 1;
                Progress::Pending
            } else {
                Progress::Done(None)
            }
        }))
    }

    async fn list_by_label(&self, selector: &str) -> Result<Vec<Resource>, HubError> {
        let wanted = selector
            .split(',')
            .filter_map(|pair| pair.split_once('='))
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect::<Vec<_>>();
        Ok(self.with(|cloud| {
            cloud.lists.push(selector.to_owned());
            cloud
                .resources
                .values()
                .filter(|resource| {
                    wanted
                        .iter()
                        .all(|(key, value)| resource.labels.get(key) == Some(value))
                })
                .cloned()
                .collect()
        }))
    }
}

pub(super) fn offered(
    name: &str,
    cores: u32,
    memory_gb: u64,
    architecture: Architecture,
) -> ServerType {
    ServerType {
        name: name.to_owned(),
        cores,
        memory_mb: memory_gb * 1024,
        disk_gb: memory_gb * 10,
        architecture,
        deprecated: false,
        hourly: ["fsn1", "nbg1", "hel1"]
            .into_iter()
            .map(|location| (location.to_owned(), format!("0.{:04}", cores * 37)))
            .collect(),
    }
}

/// Four current types and one deprecated, all offered and available in three locations.
pub(super) fn catalog_fixture() -> Catalog {
    let mut old = offered("cx11", 1, 2, Architecture::X86);
    old.deprecated = true;
    let types = vec![
        offered("cx23", 2, 4, Architecture::X86),
        offered("cpx31", 4, 8, Architecture::X86),
        offered("cx43", 8, 16, Architecture::X86),
        offered("cax21", 4, 8, Architecture::Arm),
        old,
    ];
    let names = types
        .iter()
        .map(|server_type| server_type.name.clone())
        .collect::<BTreeSet<_>>();
    Catalog {
        locations: ["fsn1", "nbg1", "hel1"]
            .into_iter()
            .map(|name| Location {
                name: name.to_owned(),
                supported: names.clone(),
                available: names.clone(),
            })
            .collect(),
        types,
    }
}

/// Tunnels that only record what is open.
#[derive(Default)]
pub(super) struct FakeTunnels {
    open: Mutex<BTreeSet<String>>,
    opened: Mutex<Vec<TunnelSpec>>,
}

impl Tunnels for FakeTunnels {
    fn open(&self, spec: &TunnelSpec) -> Result<(), HubError> {
        self.open
            .lock()
            .expect("tunnel lock")
            .insert(spec.name.clone());
        self.opened.lock().expect("tunnel lock").push(spec.clone());
        Ok(())
    }

    fn is_open(&self, name: &str) -> bool {
        self.open.lock().expect("tunnel lock").contains(name)
    }

    fn close(&self, name: &str) {
        self.open.lock().expect("tunnel lock").remove(name);
    }
}

pub(super) struct Rig {
    pub(super) service: HubService,
    pub(super) driver: Arc<FakeDriver>,
    pub(super) cloud: Arc<FakeCloud>,
    pub(super) tunnels: Arc<FakeTunnels>,
    pub(super) directory: tempfile::TempDir,
}

pub(super) fn rig_with(cloud: FakeCloud) -> Rig {
    let directory = tempfile::tempdir().expect("tempdir");
    let driver = Arc::new(FakeDriver {
        discover_any: Some(Vec::new()),
        ..FakeDriver::default()
    });
    let cloud = Arc::new(cloud);
    let tunnels = Arc::new(FakeTunnels::default());
    let service = HubService::open(
        HubStore::new(directory.path().join("hub")),
        driver.clone(),
        FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service")
    .with_pools(PoolRuntime::new(cloud.clone(), tunnels.clone()));
    Rig {
        service,
        driver,
        cloud,
        tunnels,
        directory,
    }
}

pub(super) fn rig() -> Rig {
    rig_with(FakeCloud::default())
}

pub(super) fn add_request(folder: &Path, max: u32) -> AddPool {
    let key = folder.join("id_ed25519");
    std::fs::write(&key, "private").expect("key");
    std::fs::write(
        folder.join("id_ed25519.pub"),
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFake hub@zeroshot\n",
    )
    .expect("public key");
    AddPool {
        name: "burst".to_owned(),
        provider: Provider::Hetzner,
        server_types: vec!["cpx31".to_owned(), "cx43".to_owned()],
        locations: vec!["fsn1".to_owned(), "nbg1".to_owned()],
        multi_arch: false,
        wait_max_ms: None,
        image: "docker-ce".to_owned(),
        target_image: IMAGE.to_owned(),
        max,
        slots: 1,
        idle_ms: 600_000,
        ttl_ms: 7_200_000,
        tags: vec![TAG.to_owned()],
        ssh_key: key,
        ssh_from: vec!["198.51.100.7/32".to_owned()],
        data: DataSettings::default(),
    }
}

pub(super) fn tagged(id: &str) -> Job {
    let mut queued = job(id, 1);
    queued.placement = spec(None, &[TAG], Strategy::FirstFree);
    queued
}

impl Rig {
    pub(super) async fn add_pool(&self, max: u32) {
        self.service
            .add_pool(add_request(self.directory.path(), max))
            .await
            .expect("add pool");
    }

    pub(super) async fn enqueue(&self, id: &str) {
        self.service
            .enqueue(tagged(id), Vec::new())
            .await
            .expect("enqueue");
    }

    /// One daemon pass with every lease's backoff elapsed.
    pub(super) async fn pass(&self) -> serde_json::Value {
        self.set(|state| {
            for lease in &mut state.leases {
                lease.next_attempt_ms = None;
            }
        })
        .await;
        self.service.pass().await.expect("pass").1
    }

    pub(super) async fn state(&self) -> HubState {
        self.service.snapshot().await
    }

    pub(super) async fn lease(&self) -> Lease {
        self.state().await.leases.first().cloned().expect("a lease")
    }

    pub(super) async fn set(&self, change: impl FnOnce(&mut HubState)) {
        self.service
            .mutate(|state| {
                change(state);
                Ok(())
            })
            .await
            .expect("mutate");
    }

    /// Enqueue `hj_one`, bring its server up and place the job there.
    pub(super) async fn running_job_on_server(&self) -> Lease {
        self.add_pool(1).await;
        self.enqueue("hj_one").await;
        for _ in 0..4 {
            self.pass().await;
        }
        let lease = self.lease().await;
        assert_eq!(lease.phase, LeasePhase::Active, "{lease:?}");
        let state = self.state().await;
        assert_eq!(
            job_named(&state, "hj_one").target.as_deref(),
            Some(lease.name.as_str())
        );
        lease
    }

    /// As [`Self::running_job_on_server`], then let the run finish.
    pub(super) async fn finished_job_on_server(&self) -> Lease {
        let lease = self.running_job_on_server().await;
        self.driver.finish_all();
        self.pass().await;
        assert_eq!(
            job_named(&self.state().await, "hj_one").state,
            JobState::Succeeded
        );
        lease
    }
}

pub(super) fn job_named<'a>(state: &'a HubState, id: &str) -> &'a Job {
    state
        .jobs
        .iter()
        .find(|job| job.job_id == id)
        .expect("job present")
}

pub(super) fn first_error(view: &serde_json::Value) -> String {
    view["pools"]["errors"][0]
        .as_str()
        .unwrap_or_default()
        .to_owned()
}

#[tokio::test]
async fn a_pool_with_no_servers_makes_tagged_jobs_queue_instead_of_failing() {
    let rig = rig();
    let error = rig
        .service
        .enqueue(tagged("hj_early"), Vec::new())
        .await
        .expect_err("no member and no pool");
    assert_eq!(error.exit_code(), EXIT_NO_ELIGIBLE_TARGET);

    rig.add_pool(1).await;
    let error = rig
        .service
        .submit(tagged("hj_early"), false, Vec::new())
        .await
        .expect_err("nothing free yet");
    assert_eq!(error.exit_code(), EXIT_NO_CAPACITY, "{error:?}");
    rig.service
        .submit(tagged("hj_early"), true, Vec::new())
        .await
        .expect("queued for the pool");
    assert_eq!(
        job_named(&rig.state().await, "hj_early").state,
        JobState::Queued
    );

    let mut pinned = job("hj_pinned", 1);
    pinned.placement = spec(Some("elsewhere"), &[TAG], Strategy::FirstFree);
    assert!(rig.service.enqueue(pinned, Vec::new()).await.is_err());
}

#[tokio::test]
async fn a_queued_job_creates_a_server_that_joins_as_a_member_and_runs_it() {
    let rig = rig();
    rig.add_pool(1).await;
    rig.enqueue("hj_one").await;

    let view = rig.pass().await;
    assert_eq!(view["pools"]["started"], json!(["zs-one"]));
    assert_eq!(rig.lease().await.phase, LeasePhase::Booting);
    let request = rig.cloud.with(|cloud| cloud.created[0].clone());
    assert_eq!(request.name, "zs-one");
    assert_eq!(
        (request.server_type.as_str(), request.location.as_str()),
        ("cpx31", "fsn1")
    );
    assert_eq!(request.labels[LABEL_SERVER_TYPE], "cpx31");
    assert_eq!(request.labels[LABEL_LOCATION], "fsn1");
    assert_eq!(request.ssh_from, ["198.51.100.7/32"]);
    let hub_id = rig.state().await.hub_id.expect("hub id");
    assert_eq!(request.labels[LABEL_HUB], hub_id);
    assert_eq!(request.labels[LABEL_LEASE], "one");
    assert!(request.labels.contains_key(LABEL_EXPIRES));
    assert!(request.user_data.contains(IMAGE));
    assert!(request.user_data.contains("127.0.0.1:8080:8080"));
    assert!(request.user_data.contains("DATA='/mnt/zs'"));
    assert!(request.user_data.contains("\"$DATA:/var/lib/zeroshot\""));
    assert!(request.user_data.contains("[shutdown, -h, \"+"));

    rig.pass().await;
    assert_eq!(rig.lease().await.phase, LeasePhase::Connecting);
    rig.pass().await;
    let lease = rig.lease().await;
    assert_eq!(lease.phase, LeasePhase::Active);
    assert!(rig.tunnels.is_open("zs-one"));
    let tunnel = rig.tunnels.opened.lock().expect("lock")[0].clone();
    assert_eq!(tunnel.host, "203.0.113.10");
    assert_eq!(tunnel.local_port, lease.local_port);
    let record = rig.service.registry().get("zs-one").expect("registered");
    assert_eq!(
        record.origin.trim_end_matches('/'),
        format!("http://127.0.0.1:{}", lease.local_port)
    );
    let state = rig.state().await;
    let member = state.member("zs-one").expect("member");
    assert_eq!(member.pool.as_deref(), Some("burst"));
    assert_eq!(member.server_id, lease.server_id);
    assert_eq!(member.expires_at_ms, Some(lease.expires_at_ms));
    assert_eq!(member.tags, [TAG]);
    let machine = member.machine.as_ref().expect("machine");
    assert_eq!((machine.cores, machine.memory_mb), (4, 8192));
    assert_eq!(member.view()["machine"]["serverType"], json!("cpx31"));
    assert_eq!(member.view()["machine"]["location"], json!("fsn1"));

    rig.pass().await;
    let state = rig.state().await;
    assert_eq!(
        job_named(&state, "hj_one").target.as_deref(),
        Some("zs-one")
    );
    assert_eq!(job_named(&state, "hj_one").state, JobState::Running);
}

#[tokio::test]
async fn scale_up_stops_at_the_pool_maximum() {
    let rig = rig();
    rig.add_pool(2).await;
    for id in ["hj_a", "hj_b", "hj_c"] {
        rig.enqueue(id).await;
    }
    let view = rig.pass().await;
    assert_eq!(view["pools"]["started"], json!(["zs-a", "zs-b"]));
    rig.pass().await;
    assert_eq!(rig.state().await.leases.len(), 2);
    assert_eq!(rig.cloud.servers(), 2);
}

#[tokio::test]
async fn an_idle_server_is_archived_removed_and_deleted() {
    let rig = rig();
    let lease = rig.finished_job_on_server().await;
    rig.set(|state| {
        let member = state.members.iter_mut().find(|m| m.name == lease.name);
        member.expect("member").idle_since_ms = Some(1);
    })
    .await;

    let view = rig.pass().await;
    assert_eq!(view["pools"]["teardowns"][0]["reason"], json!("idle"));
    assert_eq!(view["pools"]["archived"], json!(["hj_one"]));
    let state = rig.state().await;
    let archived = job_named(&state, "hj_one");
    let folder = archived.archive.clone().expect("archive recorded");
    assert_eq!(
        folder,
        rig.service
            .store()
            .directory()
            .join("archive")
            .join("hj_one")
    );
    for file in FILES {
        assert!(folder.join(file).is_file(), "{file}");
    }
    assert_eq!(archived.graph_hash.as_deref(), Some("sha256:graph"));
    let captured = rig.driver.captures.lock().expect("lock").clone();
    let commands = captured
        .iter()
        .map(|(_, arguments)| arguments[0].as_str())
        .collect::<Vec<_>>();
    assert_eq!(commands, ["report", "watch", "logs"]);
    assert!(state.member(&lease.name).is_none());
    assert!(rig.service.registry().get(&lease.name).is_err());
    assert!(!rig.tunnels.is_open(&lease.name));
    assert_eq!(rig.lease().await.phase, LeasePhase::Deleting);
    assert!(rig.cloud.with(|cloud| cloud.destroyed.is_empty()));

    rig.pass().await;
    assert!(rig.lease().await.action_id.is_some());
    let view = rig.pass().await;
    assert_eq!(view["pools"]["deleted"], json!([lease.name]));
    assert!(rig.state().await.leases.is_empty());
    assert_eq!(rig.cloud.servers(), 0);
    let destroyed = rig.cloud.with(|cloud| cloud.destroyed.clone());
    assert_eq!(
        destroyed,
        [
            (ResourceKind::Server, lease.server_id.expect("server")),
            (ResourceKind::Firewall, lease.firewall_id.expect("firewall")),
        ]
    );
}

#[tokio::test]
async fn ttl_drains_a_busy_server_and_deletes_it_only_after_its_run_finishes() {
    let rig = rig();
    let lease = rig.running_job_on_server().await;
    rig.set(|state| state.leases[0].expires_at_ms = 1).await;

    let view = rig.pass().await;
    assert_eq!(view["pools"]["teardowns"][0]["reason"], json!("ttl"));
    let state = rig.state().await;
    assert_eq!(
        state.member(&lease.name).expect("member").state,
        MemberState::Drain
    );
    assert_eq!(state.leases[0].phase, LeasePhase::Draining);
    rig.pass().await;
    assert_eq!(rig.lease().await.phase, LeasePhase::Draining);
    assert!(rig.cloud.with(|cloud| cloud.destroyed.is_empty()));

    rig.driver.finish_all();
    rig.pass().await;
    assert!(job_named(&rig.state().await, "hj_one").archive.is_some());
    assert_eq!(rig.lease().await.phase, LeasePhase::Deleting);
    rig.pass().await;
    rig.pass().await;
    assert!(rig.state().await.leases.is_empty());
    assert_eq!(rig.cloud.servers(), 0);
}

#[tokio::test]
async fn a_failed_archive_keeps_the_server_until_a_later_pass_archives_it() {
    let rig = rig();
    let lease = rig.finished_job_on_server().await;
    rig.set(|state| state.leases[0].expires_at_ms = 1).await;
    rig.driver.capture_fails.store(true, Ordering::SeqCst);

    let view = rig.pass().await;
    assert!(first_error(&view).contains("capture failed"), "{view}");
    rig.pass().await;
    let state = rig.state().await;
    assert_eq!(state.leases[0].phase, LeasePhase::Draining);
    assert!(state.leases[0].error.is_some());
    assert!(job_named(&state, "hj_one").archive.is_none());
    assert!(state.member(&lease.name).is_some());
    assert!(rig.cloud.with(|cloud| cloud.destroyed.is_empty()));

    rig.driver.capture_fails.store(false, Ordering::SeqCst);
    rig.pass().await;
    assert!(job_named(&rig.state().await, "hj_one").archive.is_some());
    assert_eq!(rig.lease().await.phase, LeasePhase::Deleting);
}

#[tokio::test]
async fn an_unfinished_run_is_never_archived() {
    let driver = FakeDriver::default();
    let directory = tempfile::tempdir().expect("tempdir");
    let mut running = job("hj_live", 1);
    running.run_id = Some("zs-x-run-0".to_owned());
    driver.runs.lock().expect("lock").insert(
        "zs-x".to_owned(),
        vec![("zs-x-run-0".to_owned(), "running")],
    );
    let error = archive::archive(&driver, directory.path(), "zs-x", &running)
        .await
        .expect_err("still running");
    assert!(error.to_string().contains("not finished"), "{error}");
    assert!(driver.captures.lock().expect("lock").is_empty());
    running.run_id = None;
    assert!(
        archive::archive(&driver, directory.path(), "zs-x", &running)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn the_reaper_deletes_orphans_and_expired_resources_but_keeps_live_leases() {
    let rig = rig();
    rig.add_pool(1).await;
    let state = rig.state().await;
    let hub_id = state.hub_id.clone().expect("hub id");
    let held = Lease {
        phase: LeasePhase::Active,
        ipv4: Some("203.0.113.11".to_owned()),
        ..Lease::new(&state.pools[0], "hj_held", 40000, now_ms())
    };
    let held_id = held.id.clone();
    rig.set(|state| state.leases.push(held)).await;
    let future = (now_ms() / 1000 + 3600).to_string();
    let hub = hub_id.as_str();
    let orphan = rig.cloud.plant(
        ResourceKind::Server,
        &[
            (LABEL_HUB, hub),
            (LABEL_LEASE, "gone"),
            (LABEL_EXPIRES, &future),
        ],
    );
    let orphan_ip = rig.cloud.plant(
        ResourceKind::PrimaryIp,
        &[(LABEL_HUB, hub), (LABEL_LEASE, "gone")],
    );
    let live = rig.cloud.plant(
        ResourceKind::Server,
        &[
            (LABEL_HUB, hub),
            (LABEL_LEASE, &held_id),
            (LABEL_EXPIRES, &future),
        ],
    );
    let expired = rig.cloud.plant(
        ResourceKind::Firewall,
        &[
            (LABEL_HUB, hub),
            (LABEL_LEASE, &held_id),
            (LABEL_EXPIRES, "1"),
        ],
    );
    let foreign = rig.cloud.plant(
        ResourceKind::Server,
        &[(LABEL_HUB, "another-hub"), (LABEL_LEASE, "gone")],
    );

    let view = rig.pass().await;
    assert_eq!(
        rig.cloud.with(|cloud| cloud.destroyed.clone()),
        [
            (ResourceKind::Server, orphan),
            (ResourceKind::PrimaryIp, orphan_ip),
            (ResourceKind::Firewall, expired),
        ]
    );
    assert_eq!(view["pools"]["reaped"].as_array().map(Vec::len), Some(3));
    assert!(rig.cloud.holds(ResourceKind::Server, live));
    assert!(rig.cloud.holds(ResourceKind::Server, foreign));
    assert_eq!(
        rig.cloud.with(|cloud| cloud.lists.clone()),
        [format!("{LABEL_HUB}={hub_id}")]
    );

    rig.pass().await;
    assert_eq!(
        rig.cloud.with(|cloud| cloud.lists.len()),
        1,
        "the reaper runs at most every {REAP_INTERVAL_MS} ms"
    );
}

#[test]
fn orphans_keep_an_expired_lease_whose_runs_are_unarchived() {
    let lease = Lease::new(&pool_fixture(), "hj_keep", 1, 0);
    let mut ran = job("hj_keep", 0);
    ran.target = Some(lease.name.clone());
    ran.run_id = Some("run".to_owned());
    ran.state = JobState::Succeeded;
    let state = HubState {
        leases: vec![lease.clone()],
        jobs: vec![ran],
        ..HubState::default()
    };
    let resource = Resource {
        kind: ResourceKind::Server,
        id: 7,
        name: lease.name.clone(),
        labels: lease.labels("hub"),
    };
    let late = lease.hard_deadline_ms() + 1000;
    let resources = std::slice::from_ref(&resource);
    assert!(pool::orphans(resources, &state, late).is_empty());
    let archived = HubState {
        jobs: Vec::new(),
        ..state
    };
    assert_eq!(pool::orphans(resources, &archived, late).len(), 1);
    assert!(pool::orphans(resources, &archived, 0).is_empty());
}

#[tokio::test]
async fn an_unconfirmed_create_is_adopted_instead_of_duplicated() {
    let rig = rig();
    rig.add_pool(1).await;
    let hub_id = rig.state().await.hub_id.expect("hub id");
    let existing = rig.cloud.plant(
        ResourceKind::Server,
        &[(LABEL_HUB, &hub_id), (LABEL_LEASE, "one")],
    );
    rig.set(|state| {
        let pool = state.pools[0].clone();
        state
            .leases
            .push(Lease::new(&pool, "hj_one", 40001, now_ms()));
    })
    .await;
    rig.enqueue("hj_one").await;
    rig.pass().await;
    let lease = rig.lease().await;
    assert_eq!(lease.server_id, Some(existing));
    assert_eq!(lease.phase, LeasePhase::Booting);
    assert!(rig.cloud.with(|cloud| cloud.created.is_empty()));
}

#[tokio::test]
async fn provider_failures_back_off_and_a_failed_boot_tears_down() {
    let cloud = FakeCloud::default();
    cloud.with(|cloud| cloud.refuse_all = Some(Refusal::Transient));
    let rig = rig_with(cloud);
    rig.add_pool(1).await;
    rig.enqueue("hj_one").await;
    let view = rig.service.pass().await.expect("pass").1;
    assert!(first_error(&view).contains("create refused"), "{view}");
    let lease = rig.lease().await;
    assert_eq!(lease.phase, LeasePhase::Creating);
    assert_eq!(lease.attempts, 1);
    assert!(lease.next_attempt_ms.is_some());
    rig.service.pass().await.expect("pass");
    assert_eq!(rig.lease().await.attempts, 1, "backoff not elapsed");

    rig.cloud.with(|cloud| {
        cloud.refuse_all = None;
        cloud.ready = Some(Progress::Failed("server error".to_owned()));
    });
    rig.pass().await;
    rig.pass().await;
    let lease = rig.lease().await;
    assert_eq!(lease.reason, Some(TeardownReason::BootFailed));
    assert!(matches!(
        lease.phase,
        LeasePhase::Draining | LeasePhase::Deleting
    ));
    for _ in 0..3 {
        rig.pass().await;
    }
    let state = rig.state().await;
    assert!(state.leases.is_empty(), "{:?}", state.leases);
    assert_eq!(rig.cloud.servers(), 0);
    assert_eq!(state.pools[0].boot_failures, 1);
    let paused = state.pools[0].paused_until_ms.expect("paused");
    assert!(paused > now_ms() + FIRST_PAUSE_MS - 60_000);
    rig.pass().await;
    assert!(
        rig.state().await.leases.is_empty(),
        "no server while paused"
    );
    assert_eq!(rig.cloud.with(|cloud| cloud.created.len()), 1);

    rig.set(|state| state.pools[0].paused_until_ms = Some(1))
        .await;
    rig.cloud.with(|cloud| cloud.ready = None);
    for _ in 0..3 {
        rig.pass().await;
    }
    let state = rig.state().await;
    assert_eq!(state.leases[0].phase, LeasePhase::Active);
    assert_eq!(state.pools[0].boot_failures, 0);
    assert_eq!(state.pools[0].paused_until_ms, None);
}

#[test]
fn repeated_boot_failures_double_the_pause_up_to_an_hour() {
    let mut state = HubState {
        pools: vec![pool_fixture()],
        ..HubState::default()
    };
    let mut pauses = Vec::new();
    for _ in 0..6 {
        pool::record_boot(&mut state, "burst", true, 0);
        pauses.push(state.pools[0].paused_until_ms.expect("paused"));
    }
    assert_eq!(pauses[0], FIRST_PAUSE_MS);
    assert_eq!(pauses[1], 2 * FIRST_PAUSE_MS);
    assert_eq!(pauses[5], MAX_PAUSE_MS);
    pool::record_boot(&mut state, "absent", true, 0);
    assert_eq!(state.pools[0].boot_failures, 6);
}

#[tokio::test]
async fn a_pending_boot_retries_and_a_dropped_tunnel_reopens() {
    let rig = rig();
    rig.add_pool(1).await;
    rig.enqueue("hj_one").await;
    rig.cloud
        .with(|cloud| cloud.ready = Some(Progress::Pending));
    rig.pass().await;
    rig.pass().await;
    assert_eq!(rig.lease().await.phase, LeasePhase::Booting);
    assert_eq!(rig.lease().await.attempts, 1);
    rig.cloud.with(|cloud| cloud.ready = None);
    rig.pass().await;
    rig.pass().await;
    assert_eq!(rig.lease().await.phase, LeasePhase::Active);
    rig.tunnels.close("zs-one");
    rig.pass().await;
    assert!(rig.tunnels.is_open("zs-one"));
}

#[tokio::test]
async fn a_boot_timeout_and_a_removed_pool_tear_leases_down() {
    let rig = rig();
    rig.add_pool(1).await;
    rig.enqueue("hj_one").await;
    rig.cloud
        .with(|cloud| cloud.ready = Some(Progress::Pending));
    rig.pass().await;
    rig.set(|state| state.leases[0].created_at_ms = 0).await;
    let view = rig.pass().await;
    assert_eq!(
        view["pools"]["teardowns"][0]["reason"],
        json!("boot_timeout")
    );

    let orphaned = HubState {
        leases: vec![Lease::new(&pool_fixture(), "hj_x", 1, 0)],
        ..HubState::default()
    };
    assert_eq!(
        pool::teardown_due(&orphaned, BOOT_DEADLINE_MS - 1),
        [("zs-x".to_owned(), TeardownReason::PoolRemoved)]
    );
    let removed = rig.service.remove_pool("burst").await.expect("remove");
    assert_eq!(removed["leasesToTearDown"], json!(1));
    assert!(rig.service.remove_pool("burst").await.is_err());
}

#[tokio::test]
async fn deletion_waits_for_the_provider_action() {
    let rig = rig();
    let lease = rig.finished_job_on_server().await;
    rig.set(|state| state.leases[0].expires_at_ms = 1).await;
    rig.cloud.with(|cloud| cloud.pending_deletes = 2);
    rig.pass().await;
    rig.pass().await;
    for _ in 0..2 {
        rig.pass().await;
        assert_eq!(rig.lease().await.phase, LeasePhase::Deleting);
    }
    let view = rig.pass().await;
    assert_eq!(view["pools"]["deleted"], json!([lease.name]));
}

#[tokio::test]
async fn pool_config_is_validated_listed_and_unique() {
    let rig = rig();
    let folder = rig.directory.path();
    type Case = (&'static str, fn(&mut AddPool));
    let cases: [Case; 9] = [
        ("pool name", |request| request.name = "Bad_Name".to_owned()),
        ("--tags", |request| request.tags.clear()),
        ("--max", |request| request.max = 0),
        ("--idle", |request| request.idle_ms = 0),
        ("pinned", |request| {
            request.target_image = "ghcr.io/x/zeroshot-target:latest".to_owned();
        }),
        ("absolute", |request| request.ssh_key = PathBuf::from("key")),
        ("--ssh-from", |request| {
            request.ssh_from = vec!["0.0.0.0".to_owned()];
        }),
        ("--ssh-from", |request| {
            request.ssh_from = vec!["::1/129".to_owned()];
        }),
        ("--location", |request| {
            request.locations = vec!["f sn".to_owned()]
        }),
    ];
    for (message, change) in cases {
        let mut request = add_request(folder, 1);
        change(&mut request);
        let error = rig.service.add_pool(request).await.expect_err(message);
        assert!(error.to_string().contains(message), "{message}: {error}");
    }
    let mut missing = add_request(folder, 1);
    missing.ssh_key = folder.join("absent");
    assert!(rig.service.add_pool(missing).await.is_err());

    let mut v6 = add_request(folder, 1);
    v6.ssh_from = vec!["2001:db8::1/128".to_owned()];
    rig.service.add_pool(v6).await.expect("ipv6 cidr");
    let error = rig
        .service
        .add_pool(add_request(folder, 1))
        .await
        .expect_err("duplicate");
    assert!(error.to_string().contains("already exists"), "{error}");
    let listed = rig.service.list_pools().await;
    assert_eq!(listed["schema"], json!("zeroshot.hub.pools/v1"));
    assert_eq!(listed["pools"][0]["name"], json!("burst"));
    assert_eq!(listed["pools"][0]["targetImage"], json!(IMAGE));
    let metrics = rig.service.render_metrics().await;
    assert!(
        metrics.contains(r#"zeroshot_hub_pool_leases{pool="burst",phase="active"} 0"#),
        "{metrics}"
    );
}

#[tokio::test]
async fn a_hub_without_a_provider_token_reports_its_pools_as_stuck() {
    let directory = tempfile::tempdir().expect("tempdir");
    let service = HubService::open(
        HubStore::new(directory.path().join("hub")),
        Arc::new(FakeDriver::default()),
        FileTargetRegistry::new(directory.path().join("targets.json")),
    )
    .expect("service");
    assert_eq!(service.pool_pass().await.expect("no pools").errors.len(), 0);
    let error = service
        .add_pool(add_request(directory.path(), 1))
        .await
        .expect_err("the catalog needs the token");
    assert!(error.to_string().contains("HCLOUD_TOKEN"), "{error}");
    assert!(service.pool_types().await.is_err());
    service
        .mutate(|state| {
            state.pools.push(pool_fixture());
            Ok(())
        })
        .await
        .expect("pool added before the token went away");
    let report = service.pool_pass().await.expect("pass");
    assert!(report.errors[0].contains("HCLOUD_TOKEN"));
    if std::env::var_os("HCLOUD_TOKEN").is_none() {
        let runtime = production_runtime(&Default::default()).expect("no token is no error");
        assert!(runtime.is_none());
    }
}

#[test]
fn version_one_state_migrates_with_empty_pools() {
    let v1 = json!({"version": 1, "members": [], "jobs": []});
    let state = migrate(v1.to_string().as_bytes()).expect("v1 migrates");
    assert_eq!(state.version, STATE_VERSION);
    assert!(state.pools.is_empty() && state.leases.is_empty() && state.hub_id.is_none());
    let mut v1_with_member = serde_json::to_value(HubState {
        members: vec![super::member("box", &["linux"], 1, 0)],
        ..HubState::default()
    })
    .expect("state json");
    v1_with_member["version"] = json!(1);
    let members = v1_with_member["members"].as_array_mut().expect("members");
    for member in members {
        let object = member.as_object_mut().expect("member object");
        object.retain(|key, _| {
            !matches!(
                key.as_str(),
                "pool" | "serverId" | "idleSinceMs" | "expiresAtMs"
            )
        });
    }
    let migrated = migrate(v1_with_member.to_string().as_bytes()).expect("member migrates");
    assert_eq!(migrated.members[0].pool, None);
    assert!(migrate(br#"{"version": 99}"#).is_err());
    assert!(migrate(b"not json").is_err());
}

#[test]
fn user_data_pins_the_image_and_carries_no_secrets() {
    let pool = pool_fixture();
    let lease = Lease::new(&pool, "hj_one", 40123, 0);
    let document = provision::user_data(&pool, &lease, "ssh-ed25519 AAAA hub").expect("user data");
    assert!(document.starts_with("#cloud-config"), "{document}");
    assert!(document.contains("ssh-ed25519 AAAA hub"));
    let backstop = (pool.ttl_ms + BACKSTOP_GRACE_MS) / 60_000;
    assert!(
        document.contains(&format!("[shutdown, -h, \"+{backstop}\"]")),
        "{document}"
    );
    assert!(document.contains("ORIGIN='http://127.0.0.1:40123'"));
    assert!(!document.to_lowercase().contains("token"));
    assert!(document.len() <= provision::USER_DATA_LIMIT);
    assert!(provision::user_data(&pool, &lease, "two\nlines").is_err());
    let huge = format!("ssh-ed25519 {}", "k".repeat(provision::USER_DATA_LIMIT));
    assert!(provision::user_data(&pool, &lease, &huge).is_err());
    assert!(provision::pinned("ghcr.io/x/target@sha256:abc"));
    assert!(provision::pinned("ghcr.io/x/target:1.2"));
    assert!(provision::pinned("localhost:5000/target:1.2"));
    assert!(!provision::pinned("ghcr.io/x/target"));
    assert!(!provision::pinned("localhost:5000/target"));
    assert!(!provision::pinned("ghcr.io/x/target:latest"));
}

#[test]
fn lease_ids_are_hostname_labels_and_backoff_is_capped() {
    assert_eq!(pool::lease_id("hj_0190ABcd"), "0190abcd");
    assert_eq!(pool::lease_id("hj___").len(), 32);
    let mut lease = Lease::new(&pool_fixture(), "hj_x", 1, 0);
    for _ in 0..20 {
        lease.retry(0, None);
    }
    let capped = lease.next_attempt_ms.expect("backoff");
    lease.retry(0, Some("boom".to_owned()));
    assert_eq!(lease.next_attempt_ms, Some(capped));
    assert_eq!(lease.error.as_deref(), Some("boom"));
    assert!(!lease.due(capped - 1));
    assert!(lease.due(capped));
}

#[test]
fn ssh_tunnels_forward_loopback_only_with_pinned_host_keys() {
    let spec = TunnelSpec {
        name: "zs-one".to_owned(),
        host: "203.0.113.10".to_owned(),
        local_port: 40123,
        key: PathBuf::from("/hub/id_ed25519"),
        known_hosts: PathBuf::from("/hub/pool/known_hosts"),
    };
    let arguments = ssh_arguments(&spec);
    assert!(arguments.contains(&"-N".to_owned()), "{arguments:?}");
    assert!(arguments.contains(&"127.0.0.1:40123:127.0.0.1:8080".to_owned()));
    assert!(arguments.contains(&"BatchMode=yes".to_owned()));
    assert!(arguments.contains(&"UserKnownHostsFile=/hub/pool/known_hosts".to_owned()));
    assert_eq!(
        arguments.last().map(String::as_str),
        Some("root@203.0.113.10")
    );
}

pub(super) fn pool_fixture() -> Pool {
    Pool {
        name: "burst".to_owned(),
        provider: Provider::Hetzner,
        server_types: capacity::resolve(
            (&["cpx31".to_owned()], &["fsn1".to_owned()]),
            false,
            &catalog_fixture(),
        )
        .expect("cpx31 in fsn1")
        .0,
        locations: vec!["fsn1".to_owned()],
        multi_arch: false,
        wait_max_ms: None,
        image: "docker-ce".to_owned(),
        target_image: IMAGE.to_owned(),
        max: 1,
        slots: 1,
        idle_ms: 600_000,
        ttl_ms: 7_200_000,
        tags: vec![TAG.to_owned()],
        ssh_key: PathBuf::from("/hub/id_ed25519"),
        ssh_from: vec!["198.51.100.7/32".to_owned()],
        created_at_ms: 0,
        boot_failures: 0,
        paused_until_ms: None,
        blocked: None,
        shortages: std::collections::BTreeMap::new(),
        profiles: Vec::new(),
        unavailable: BTreeMap::new(),
        data: DataSettings::default(),
    }
}

#[cfg(unix)]
#[test]
fn ssh_tunnels_start_restart_and_stop_their_process() {
    use super::super::tunnel::{SshTunnels, free_port};
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().expect("tempdir");
    let fake = directory.path().join("fake-ssh");
    let marker = directory.path().join("starts");
    std::fs::write(
        &fake,
        format!(
            "#!/bin/sh\necho \"$@\" >> '{}'\nexec sleep 30\n",
            marker.display()
        ),
    )
    .expect("fake ssh");
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o700)).expect("mode");
    let spec = TunnelSpec {
        name: "zs-one".to_owned(),
        host: "203.0.113.10".to_owned(),
        local_port: free_port().expect("port"),
        key: directory.path().join("key"),
        known_hosts: directory.path().join("pool/known_hosts"),
    };
    let started = |count: usize| {
        for _ in 0..500 {
            let lines = std::fs::read_to_string(&marker).unwrap_or_default();
            if lines.lines().count() >= count {
                return lines;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("fake ssh did not start {count} times");
    };
    let tunnels = SshTunnels::with_program(fake);
    assert!(!tunnels.is_open("zs-one"));
    tunnels.open(&spec).expect("opened");
    assert!(tunnels.is_open("zs-one"));
    assert!(directory.path().join("pool").is_dir());
    started(1);
    tunnels.open(&spec).expect("still open");
    tunnels.close("zs-one");
    assert!(!tunnels.is_open("zs-one"));
    tunnels.close("zs-one");
    tunnels.open(&spec).expect("reopened");
    let starts = started(2);
    drop(tunnels);
    assert_eq!(starts.lines().count(), 2, "{starts}");
    assert!(starts.contains("root@203.0.113.10"));

    let missing = SshTunnels::with_program(directory.path().join("absent"));
    let error = missing.open(&spec).expect_err("no program");
    assert!(
        error.to_string().contains("ssh could not be started"),
        "{error}"
    );
}
