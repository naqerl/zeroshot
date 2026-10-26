//! Which server a pool job needs, where to try creating it, and what a pool is waiting for.
//!
//! A pool holds an ordered list of server types and locations, and tag profiles may hold their
//! own (see [`super::profile`]). A job may ask for a size with the tags `server-type=NAME`,
//! `min-vcpu=N` and `min-ram-gb=N` (the `hub submit` flags `--server-type`, `--min-vcpu` and
//! `--min-ram`); the pool tries the first type that fits, in each of its locations in order,
//! then the next type. Capacity refusals move on to the next candidate; when none is left, jobs
//! asking for the same machines wait with backoff, apart from the pause after failed boots,
//! since a shortage is not a broken image.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::clock::rfc3339;
use super::error::HubError;
use super::model::{HubState, JobError, JobErrorKind, JobState};
use super::pool::{Lease, Pool, serving};
use super::profile::{self, Spec};
use super::provision::{Architecture, Catalog};

const TAG_SERVER_TYPE: &str = "server-type=";
const TAG_MIN_CPU: &str = "min-vcpu=";
const TAG_MIN_RAM_GB: &str = "min-ram-gb=";

/// First wait after every candidate was out of capacity; it doubles up to [`MAX_SHORTAGE_MS`].
pub(crate) const FIRST_SHORTAGE_MS: u64 = 30_000;
pub(crate) const MAX_SHORTAGE_MS: u64 = 10 * 60_000;

/// The server a job asks for. Empty asks for nothing beyond the pool's first type.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct SizeRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_cores: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_memory_gb: Option<u32>,
}

impl SizeRequest {
    /// The size a job's tags ask for; a later tag of the same kind wins.
    pub(crate) fn from_tags(tags: &[String]) -> Self {
        let mut want = Self::default();
        for tag in tags {
            want.absorb(tag);
        }
        want
    }

    /// Fold one tag in; returns whether it was a size tag.
    fn absorb(&mut self, tag: &str) -> bool {
        let count = |value: &str| value.parse::<u32>().ok().filter(|count| *count > 0);
        if let Some(name) = tag
            .strip_prefix(TAG_SERVER_TYPE)
            .filter(|name| identifier(name))
        {
            self.server_type = Some(name.to_owned());
        } else if let Some(cores) = tag.strip_prefix(TAG_MIN_CPU).and_then(count) {
            self.min_cores = Some(cores);
        } else if let Some(memory) = tag.strip_prefix(TAG_MIN_RAM_GB).and_then(count) {
            self.min_memory_gb = Some(memory);
        } else {
            return false;
        }
        true
    }

    pub(crate) fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// The request as size tags, such as `min-vcpu=8,min-ram-gb=16`.
    pub(crate) fn describe(&self) -> String {
        let mut parts = Vec::new();
        if let Some(name) = &self.server_type {
            parts.push(format!("{TAG_SERVER_TYPE}{name}"));
        }
        if let Some(cores) = self.min_cores {
            parts.push(format!("{TAG_MIN_CPU}{cores}"));
        }
        if let Some(memory) = self.min_memory_gb {
            parts.push(format!("{TAG_MIN_RAM_GB}{memory}"));
        }
        parts.join(",")
    }

    /// Whether a server of this type, size and memory is enough.
    pub(crate) fn fits(&self, name: &str, cores: u32, memory_mb: u64) -> bool {
        self.server_type
            .as_deref()
            .is_none_or(|wanted| wanted == name)
            && self.min_cores.is_none_or(|least| cores >= least)
            && self
                .min_memory_gb
                .is_none_or(|least| memory_mb >= u64::from(least) * 1024)
    }
}

/// Whether a tag asks for a server size rather than naming a member tag.
pub(crate) fn is_size_tag(tag: &str) -> bool {
    SizeRequest::default().absorb(tag)
}

/// A job's tags that are not size requests.
pub(crate) fn ordinary_tags(tags: &[String]) -> impl Iterator<Item = &String> {
    tags.iter().filter(|tag| !is_size_tag(tag))
}

fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
}

/// One server type a pool may create, with what the catalog said about it when it was added.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct PoolType {
    pub name: String,
    pub cores: u32,
    pub memory_mb: u64,
    pub architecture: Architecture,
    /// The pool's locations that offer this type, in the pool's order.
    pub locations: Vec<String>,
    /// Gross hourly price by location.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub hourly: BTreeMap<String, String>,
}

/// The server a pool member runs on.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Machine {
    pub server_type: String,
    pub location: String,
    pub cores: u32,
    pub memory_mb: u64,
}

impl Machine {
    /// Whether this machine meets a size tag; `false` for any other tag.
    pub(crate) fn meets(&self, tag: &str) -> bool {
        let mut want = SizeRequest::default();
        want.absorb(tag) && want.fits(&self.server_type, self.cores, self.memory_mb)
    }

    pub(crate) fn view(&self) -> Value {
        json!({
            "serverType": self.server_type,
            "location": self.location,
            "cores": self.cores,
            "memoryMb": self.memory_mb,
        })
    }
}

/// Where to try a job's server, best first: each type of its spec in order (see
/// [`profile::types_for`]), in each of that type's locations in order.
pub(crate) fn candidates<'a>(pool: &'a Pool, spec: &Spec) -> Vec<(&'a PoolType, &'a str)> {
    profile::types_for(pool, spec)
        .into_iter()
        .flat_map(|server_type| {
            server_type
                .locations
                .iter()
                .map(move |location| (server_type, location.as_str()))
        })
        .collect()
}

/// Why a pool is not creating servers now, if it is not.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Shortage {
    /// `waiting: no capacity for TYPES in LOCATIONS`.
    pub reason: String,
    pub since_ms: u64,
    /// Shortages in a row, for the backoff.
    pub rounds: u32,
    pub retry_at_ms: u64,
}

/// Record that no candidate of a spec had capacity: jobs of that spec retry after a doubling
/// wait, while jobs needing other machines go on.
pub(crate) fn record_shortage(pool: &mut Pool, spec: &Spec, tried: &[(String, String)], now: u64) {
    let reason = shortage_reason(spec, tried);
    let earlier = pool.shortages.get(&spec.key());
    let rounds = earlier.map_or(0, |shortage| shortage.rounds) + 1;
    let wait = FIRST_SHORTAGE_MS
        .saturating_mul(1 << (rounds - 1).min(8))
        .min(MAX_SHORTAGE_MS);
    let since_ms = earlier.map_or(now, |shortage| shortage.since_ms);
    pool.shortages.insert(
        spec.key(),
        Shortage {
            reason,
            since_ms,
            rounds,
            retry_at_ms: now.saturating_add(wait),
        },
    );
}

/// Whether jobs of this spec wait out a shortage now.
pub(crate) fn short(pool: &Pool, spec: &Spec, now: u64) -> bool {
    pool.shortages
        .get(&spec.key())
        .is_some_and(|shortage| now < shortage.retry_at_ms)
}

/// Count capacity refusals per type and location, for metrics.
pub(crate) fn count_unavailable(pool: &mut Pool, tried: &[(String, String)]) {
    for (server_type, location) in tried {
        let count = pool
            .unavailable
            .entry(server_type.clone())
            .or_default()
            .entry(location.clone())
            .or_default();
        *count = count.saturating_add(1);
    }
}

fn shortage_reason(spec: &Spec, tried: &[(String, String)]) -> String {
    let mut types = Vec::new();
    let mut locations = Vec::new();
    for (server_type, location) in tried {
        if !types.contains(server_type) {
            types.push(server_type.clone());
        }
        if !locations.contains(location) {
            locations.push(location.clone());
        }
    }
    match spec.asker() {
        Some(asker) => format!(
            "waiting: {asker} needs {}, no capacity in {}",
            types.join("/"),
            locations.join("/")
        ),
        None => format!(
            "waiting: no capacity for {} in {}",
            types.join("/"),
            locations.join("/")
        ),
    }
}

/// Why the pool creates no server now for jobs of `spec` (any spec when `None`): stopped by an
/// account or request error, waiting out a capacity shortage, or paused after failed boots.
pub(crate) fn hold_reason(pool: &Pool, spec: Option<&Spec>, now: u64) -> Option<String> {
    if let Some(blocked) = &pool.blocked {
        return Some(format!(
            "blocked: {blocked}; fix it, then run `zeroshot hub pool resume {}`",
            pool.name
        ));
    }
    let shortages = pool
        .shortages
        .iter()
        .filter(|(key, _)| spec.is_none_or(|spec| spec.key() == **key))
        .map(|(_, shortage)| {
            format!(
                "{}; next try at {}",
                shortage.reason,
                rfc3339(shortage.retry_at_ms.max(now))
            )
        })
        .collect::<Vec<_>>();
    if !shortages.is_empty() {
        return Some(shortages.join("; "));
    }
    pool.paused_until_ms
        .filter(|until| now < *until)
        .map(|until| {
            format!(
                "paused after {} failed boot(s) until {}",
                pool.boot_failures,
                rfc3339(until)
            )
        })
}

/// Whether the pool may start servers now; a shortage holds only the jobs of its spec.
pub(crate) fn may_start(pool: &Pool, now: u64) -> bool {
    pool.blocked.is_none() && pool.paused_until_ms.is_none_or(|until| now >= until)
}

/// Resolve the machine of each queued pool job, show it why it waits, and expire those past
/// their pool's `--wait-max`. Returns the expired job IDs.
pub(crate) fn note_waiting(state: &mut HubState, now: u64) -> Vec<String> {
    let mut expired = Vec::new();
    let pools = state.pools.clone();
    let leases = state.leases.clone();
    for job in &mut state.jobs {
        if job.state != JobState::Queued {
            job.pool_wait = None;
            continue;
        }
        let Some(pool) = serving(&pools, &job.placement, &job.requirements) else {
            job.pool_wait = None;
            job.requirements.machine = None;
            continue;
        };
        let spec = profile::resolve_spec(pool, &job.placement.tags);
        let reason = hold_reason(pool, Some(&spec), now).or_else(|| at_max(pool, &leases));
        job.requirements.machine = Some(spec);
        let queued_since = job.not_before_ms.unwrap_or(0).max(job.submitted_at_ms);
        if let Some(limit) = pool.wait_max_ms
            && now >= queued_since.saturating_add(limit)
        {
            job.state = JobState::Failed;
            job.finished_at_ms = Some(now);
            job.error = Some(JobError::new(
                JobErrorKind::PoolWaitTimeout,
                format!(
                    "no server from pool {} within its --wait-max of {}s{}",
                    pool.name,
                    limit / 1000,
                    reason
                        .map(|reason| format!(" ({reason})"))
                        .unwrap_or_default()
                ),
            ));
            job.pool_wait = None;
            expired.push(job.job_id.clone());
            continue;
        }
        job.pool_wait = reason;
    }
    expired
}

fn at_max(pool: &Pool, leases: &[Lease]) -> Option<String> {
    let held = leases
        .iter()
        .filter(|lease| lease.pool == pool.name)
        .count();
    (held >= usize::try_from(pool.max).unwrap_or(usize::MAX)).then(|| {
        format!(
            "waiting: pool {} runs its --max of {} servers",
            pool.name, pool.max
        )
    })
}

/// The pool's server types and locations, checked against the provider's catalog: every type
/// must exist, be current, run the target image's architecture, and be offered in at least
/// one chosen location. Without `--location`, every location offering a chosen type is used,
/// in catalog order.
pub(crate) fn resolve(
    (types, locations): (&[String], &[String]),
    multi_arch: bool,
    catalog: &Catalog,
) -> Result<(Vec<PoolType>, Vec<String>), HubError> {
    let usage = |message: String| Err(HubError::Usage(message));
    if types.is_empty() {
        return usage(
            "--type needs at least one server type; see `zeroshot hub pool types`".to_owned(),
        );
    }
    if let Some(repeated) = repeated(types).or_else(|| repeated(locations)) {
        return usage(format!("{repeated} is listed twice"));
    }
    for location in locations {
        if catalog.location(location).is_none() {
            let known = catalog
                .locations
                .iter()
                .map(|location| location.name.as_str())
                .collect::<Vec<_>>();
            return usage(format!(
                "unknown location {location:?}; the provider has {}",
                known.join(", ")
            ));
        }
    }
    let locations = if locations.is_empty() {
        catalog
            .locations
            .iter()
            .filter(|location| types.iter().any(|name| location.supported.contains(name)))
            .map(|location| location.name.clone())
            .collect()
    } else {
        locations.to_vec()
    };
    let mut resolved = Vec::new();
    for name in types {
        resolved.push(pool_type(name, &locations, multi_arch, catalog)?);
    }
    Ok((resolved, locations))
}

fn pool_type(
    name: &str,
    locations: &[String],
    multi_arch: bool,
    catalog: &Catalog,
) -> Result<PoolType, HubError> {
    let usage = |message: String| Err(HubError::Usage(message));
    let Some(found) = catalog.server_type(name) else {
        return usage(format!(
            "unknown server type {name:?}; `zeroshot hub pool types` lists the valid ones"
        ));
    };
    if found.deprecated {
        return usage(format!(
            "server type {name} is deprecated; `zeroshot hub pool types` lists current ones"
        ));
    }
    if found.architecture == Architecture::Arm && !multi_arch {
        return usage(format!(
            "server type {name} is ARM (arm64) but the target image is amd64-only; drop it, or \
             pass --multi-arch if --target-image is a multi-arch image"
        ));
    }
    let offered = locations
        .iter()
        .filter(|location| {
            catalog
                .location(location)
                .is_some_and(|location| location.supported.contains(name))
        })
        .cloned()
        .collect::<Vec<_>>();
    if offered.is_empty() {
        return usage(format!(
            "server type {name} is offered in none of {}",
            locations.join(", ")
        ));
    }
    Ok(PoolType {
        name: name.to_owned(),
        cores: found.cores,
        memory_mb: found.memory_mb,
        architecture: found.architecture,
        hourly: found
            .hourly
            .iter()
            .filter(|(location, _)| offered.contains(location))
            .map(|(location, price)| (location.clone(), price.clone()))
            .collect(),
        locations: offered,
    })
}

fn repeated(values: &[String]) -> Option<&String> {
    let mut seen = BTreeSet::new();
    values.iter().find(|value| !seen.insert(*value))
}

/// The `hub pool types` document: current server types with size, architecture, hourly price
/// per location and where they can be created now.
pub(crate) fn types_document(catalog: &Catalog) -> Value {
    let types = catalog
        .types
        .iter()
        .filter(|server_type| !server_type.deprecated)
        .map(|server_type| {
            let available = catalog
                .locations
                .iter()
                .filter(|location| location.available.contains(&server_type.name))
                .map(|location| location.name.clone())
                .collect::<Vec<_>>();
            json!({
                "name": server_type.name,
                "cores": server_type.cores,
                "memoryMb": server_type.memory_mb,
                "diskGb": server_type.disk_gb,
                "architecture": server_type.architecture,
                "needsMultiArch": server_type.architecture == Architecture::Arm,
                "hourlyGross": server_type.hourly,
                "availableIn": available,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "schema": "zeroshot.hub.pool-types/v1",
        "types": types,
        "locations": catalog.locations.iter().map(|location| &location.name).collect::<Vec<_>>(),
    })
}
