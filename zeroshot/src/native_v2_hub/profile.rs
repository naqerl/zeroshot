//! Tag profiles: the machines a pool creates for jobs carrying a tag.
//!
//! A job's machine is resolved in this order: an explicit size on the job (`--server-type`,
//! `--min-vcpu`, `--min-ram`), then the profile of one of its tags, then the pool's own types.
//! When several of a job's tags have profiles, the one with the highest `--priority` wins,
//! then the one with the largest minimum vCPU, then the largest minimum memory. Profiles that
//! would tie on all three are rejected when they are set, so the choice is always unambiguous.
//!
//! A job placed by a profile needs a member at least as large as the profile's minimum (its
//! `--min-vcpu`/`--min-ram`, or else its smallest type), so a small job may reuse a bigger idle
//! server but a big job never lands on an undersized one.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::capacity::{self, PoolType, SizeRequest};
use super::error::HubError;
use super::model::{HubState, PlacementSpec, Requirements};
use super::pool::Pool;
use super::provision::Catalog;
use super::service::HubService;
use super::wire::SetTagProfile;

/// What machines a pool creates for jobs with one tag.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct TagProfile {
    pub tag: String,
    /// Types to try, best first, each with the locations to try it in.
    pub server_types: Vec<PoolType>,
    pub locations: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_cores: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_memory_gb: Option<u32>,
    /// Slots of a member created for this tag; the pool's `--slots` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slots: Option<u32>,
    /// Higher wins when a job carries several profiled tags.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub priority: u32,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_zero(value: &u32) -> bool {
    *value == 0
}

impl TagProfile {
    /// The least machine a job with this tag accepts: the profile's minimum, or the smallest
    /// vCPU count and memory among its types.
    pub(crate) fn minimum(&self) -> SizeRequest {
        let smallest_memory = self
            .server_types
            .iter()
            .map(|server_type| u32::try_from(server_type.memory_mb / 1024).unwrap_or(u32::MAX))
            .min();
        SizeRequest {
            server_type: None,
            min_cores: self.min_cores.or_else(|| {
                self.server_types
                    .iter()
                    .map(|server_type| server_type.cores)
                    .min()
            }),
            min_memory_gb: self.min_memory_gb.or(smallest_memory),
        }
    }

    /// The order among a job's profiled tags: priority, then minimum vCPU, then memory.
    pub(crate) fn rank(&self) -> (u32, u32, u32) {
        let minimum = self.minimum();
        (
            self.priority,
            minimum.min_cores.unwrap_or(0),
            minimum.min_memory_gb.unwrap_or(0),
        )
    }

    pub(crate) fn view(&self) -> Value {
        json!({
            "tag": self.tag,
            "serverTypes": self.server_types,
            "locations": self.locations,
            "minCores": self.min_cores,
            "minMemoryGb": self.min_memory_gb,
            "minimum": self.minimum(),
            "slots": self.slots,
            "priority": self.priority,
        })
    }
}

/// Where a job's machine requirement came from.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SpecSource {
    /// The job's own `--server-type`, `--min-vcpu` or `--min-ram`.
    Job,
    /// The profile of one of the job's tags.
    Tag,
    /// The pool's own server types.
    #[default]
    Pool,
}

/// The machine a pool job gets and why.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Spec {
    #[serde(default)]
    pub source: SpecSource,
    /// The tag whose profile matched, if one did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// The size the job needs: its own request, or its profile's minimum.
    #[serde(default, skip_serializing_if = "SizeRequest::is_empty")]
    pub want: SizeRequest,
}

impl Spec {
    /// The key a capacity shortage is tracked under: jobs with the same key try the same types.
    pub(crate) fn key(&self) -> String {
        match self.source {
            SpecSource::Pool => "pool".to_owned(),
            SpecSource::Tag => format!("tag:{}", self.tag.as_deref().unwrap_or_default()),
            SpecSource::Job => format!(
                "job:{}:{}",
                self.tag.as_deref().unwrap_or_default(),
                self.want.describe()
            ),
        }
    }

    /// Who asks for the machine, for waiting reasons: `tag gpu-heavy`, `min-vcpu=8`, or none.
    pub(crate) fn asker(&self) -> Option<String> {
        match self.source {
            SpecSource::Pool => None,
            SpecSource::Tag => Some(format!("tag {}", self.tag.as_deref().unwrap_or_default())),
            SpecSource::Job => Some(format!("job size {}", self.want.describe())),
        }
    }
}

/// The profile chosen among a job's tags, by [`TagProfile::rank`].
pub(crate) fn profile_for<'a>(pool: &'a Pool, tags: &[String]) -> Option<&'a TagProfile> {
    pool.profiles
        .iter()
        .filter(|profile| tags.contains(&profile.tag))
        .max_by_key(|profile| profile.rank())
}

/// Resolve a job's machine: its explicit size, else its tag profile, else the pool default.
pub(crate) fn resolve_spec(pool: &Pool, tags: &[String]) -> Spec {
    let explicit = SizeRequest::from_tags(tags);
    let profile = profile_for(pool, tags);
    let tag = profile.map(|profile| profile.tag.clone());
    if !explicit.is_empty() {
        return Spec {
            source: SpecSource::Job,
            tag,
            want: explicit,
        };
    }
    match profile {
        Some(profile) => Spec {
            source: SpecSource::Tag,
            tag,
            want: profile.minimum(),
        },
        None => Spec::default(),
    }
}

/// The types to try for a spec, best first. A profile's own list for a tag; for an explicit
/// size, every type the pool knows (the matched profile's first, then the pool's, then other
/// profiles'), filtered by the size; the pool's list otherwise.
pub(crate) fn types_for<'a>(pool: &'a Pool, spec: &Spec) -> Vec<&'a PoolType> {
    let profile = spec
        .tag
        .as_ref()
        .and_then(|tag| pool.profiles.iter().find(|profile| profile.tag == *tag));
    let listed: Vec<&PoolType> = match (spec.source, profile) {
        (SpecSource::Tag, Some(profile)) => profile.server_types.iter().collect(),
        (SpecSource::Job, _) => {
            let mut known: Vec<&PoolType> = Vec::new();
            let every = profile
                .into_iter()
                .flat_map(|profile| &profile.server_types)
                .chain(&pool.server_types)
                .chain(
                    pool.profiles
                        .iter()
                        .flat_map(|profile| &profile.server_types),
                );
            for server_type in every {
                if !known.iter().any(|seen| seen.name == server_type.name) {
                    known.push(server_type);
                }
            }
            known
        }
        _ => pool.server_types.iter().collect(),
    };
    listed
        .into_iter()
        .filter(|server_type| {
            spec.want
                .fits(&server_type.name, server_type.cores, server_type.memory_mb)
        })
        .collect()
}

/// Slots of a member created for a spec: its profile's, or the pool's.
pub(crate) fn slots_for(pool: &Pool, spec: &Spec) -> u32 {
    spec.tag
        .as_ref()
        .and_then(|tag| pool.profiles.iter().find(|profile| profile.tag == *tag))
        .and_then(|profile| profile.slots)
        .unwrap_or(pool.slots)
        .max(1)
}

/// Tags a pool member carries: the pool's, then every profiled tag, so a job of any profile
/// may reuse a member that is large enough.
pub(crate) fn member_tags(pool: &Pool) -> Vec<String> {
    let mut tags = pool.tags.clone();
    for profile in &pool.profiles {
        if !tags.contains(&profile.tag) {
            tags.push(profile.tag.clone());
        }
    }
    tags
}

/// Why a new profile cannot join a pool's: it ties with another profile on every rank, so a
/// job carrying both tags would have no defined machine.
pub(crate) fn tie(pool: &Pool, candidate: &TagProfile) -> Option<String> {
    pool.profiles
        .iter()
        .filter(|profile| profile.tag != candidate.tag)
        .find(|profile| profile.rank() == candidate.rank())
        .map(|profile| {
            let (priority, cores, memory) = candidate.rank();
            format!(
                "tags {} and {} would tie for a job carrying both (priority {priority}, \
                 {cores} vCPU, {memory} GB); give one a higher --priority",
                profile.tag, candidate.tag
            )
        })
}

/// The machine of a job a pool serves, or `None` when no pool serves it.
pub(crate) fn machine_for(
    pools: &[Pool],
    placement: &PlacementSpec,
    requirements: &Requirements,
) -> Option<Spec> {
    super::pool::serving(pools, placement, requirements)
        .map(|pool| resolve_spec(pool, &placement.tags))
}

impl HubService {
    /// Set (or replace) the machines a pool creates for jobs with `tag`, after checking the
    /// types and locations against the provider's catalog.
    pub(crate) async fn set_tag_profile(
        &self,
        (name, tag): (&str, &str),
        request: SetTagProfile,
    ) -> Result<Value, HubError> {
        check_tag(tag)?;
        let pool = self
            .snapshot()
            .await
            .pools
            .into_iter()
            .find(|pool| pool.name == name)
            .ok_or_else(|| missing_pool(name))?;
        let catalog = self.catalog().await?;
        let profile = build(&pool, tag, request, &catalog)?;
        self.mutate(|state| {
            let pool = state
                .pools
                .iter_mut()
                .find(|pool| pool.name == name)
                .ok_or_else(|| missing_pool(name))?;
            if let Some(reason) = tie(pool, &profile) {
                return Err(HubError::Usage(reason));
            }
            match pool
                .profiles
                .iter_mut()
                .find(|known| known.tag == profile.tag)
            {
                Some(known) => *known = profile.clone(),
                None => pool.profiles.push(profile.clone()),
            }
            pool.shortages.remove(&format!("tag:{tag}"));
            let pool = pool.clone();
            sync_members(state, &pool);
            Ok(profile.view())
        })
        .await
    }

    /// A pool's tag profiles.
    pub(crate) async fn list_tag_profiles(&self, name: &str) -> Result<Value, HubError> {
        let state = self.snapshot().await;
        let pool = state
            .pools
            .iter()
            .find(|pool| pool.name == name)
            .ok_or_else(|| missing_pool(name))?;
        Ok(json!({
            "schema": "zeroshot.hub.pool-tags/v1",
            "pool": pool.name,
            "profiles": pool.profiles.iter().map(TagProfile::view).collect::<Vec<_>>(),
        }))
    }

    /// Remove a tag profile: jobs with the tag fall back to the pool's own types.
    pub(crate) async fn remove_tag_profile(
        &self,
        name: &str,
        tag: &str,
    ) -> Result<Value, HubError> {
        self.mutate(|state| {
            let pool = state
                .pools
                .iter_mut()
                .find(|pool| pool.name == name)
                .ok_or_else(|| missing_pool(name))?;
            let before = pool.profiles.len();
            pool.profiles.retain(|profile| profile.tag != tag);
            if pool.profiles.len() == before {
                return Err(HubError::Usage(format!(
                    "pool {name} has no profile for tag {tag}"
                )));
            }
            pool.shortages.remove(&format!("tag:{tag}"));
            let pool = pool.clone();
            sync_members(state, &pool);
            Ok(json!({ "pool": name, "removed": tag }))
        })
        .await
    }
}

fn missing_pool(name: &str) -> HubError {
    HubError::Usage(format!("pool {name} does not exist"))
}

fn check_tag(tag: &str) -> Result<(), HubError> {
    if tag.is_empty()
        || tag.len() > 128
        || tag
            .chars()
            .any(|character| character.is_whitespace() || character == ',')
    {
        return Err(HubError::Usage(format!(
            "invalid tag {tag:?}: use 1-128 characters without whitespace or commas"
        )));
    }
    if capacity::is_size_tag(tag) {
        return Err(HubError::Usage(format!(
            "{tag:?} is a size request; profile an ordinary job tag"
        )));
    }
    Ok(())
}

/// The profile a request describes. Listed types must meet the minimum; types taken from the
/// pool are filtered by it.
fn build(
    pool: &Pool,
    tag: &str,
    request: SetTagProfile,
    catalog: &Catalog,
) -> Result<TagProfile, HubError> {
    let usage = |message: String| Err(HubError::Usage(message));
    if request.slots == Some(0) {
        return usage("--slots must be at least 1".to_owned());
    }
    let explicit = !request.server_types.is_empty();
    let names = if explicit {
        request.server_types
    } else {
        pool.server_types
            .iter()
            .map(|server_type| server_type.name.clone())
            .collect()
    };
    let locations = if request.locations.is_empty() {
        pool.locations.clone()
    } else {
        request.locations
    };
    let (types, locations) = capacity::resolve((&names, &locations), pool.multi_arch, catalog)?;
    let minimum = SizeRequest {
        server_type: None,
        min_cores: request.min_cores,
        min_memory_gb: request.min_memory_gb,
    };
    let fits = |server_type: &PoolType| {
        minimum.fits(&server_type.name, server_type.cores, server_type.memory_mb)
    };
    if explicit && let Some(small) = types.iter().find(|server_type| !fits(server_type)) {
        return usage(format!(
            "server type {} ({} vCPU, {} GB) is smaller than tag {tag} needs ({})",
            small.name,
            small.cores,
            small.memory_mb / 1024,
            minimum.describe()
        ));
    }
    let server_types = types.into_iter().filter(fits).collect::<Vec<_>>();
    if server_types.is_empty() {
        return usage(format!(
            "none of the pool's types meets {} for tag {tag}; list some with --type",
            minimum.describe()
        ));
    }
    Ok(TagProfile {
        tag: tag.to_owned(),
        server_types,
        locations,
        min_cores: request.min_cores,
        min_memory_gb: request.min_memory_gb,
        slots: request.slots,
        priority: request.priority,
    })
}

/// Give a pool's members the pool's and its profiles' tags.
fn sync_members(state: &mut HubState, pool: &Pool) {
    let tags = member_tags(pool);
    for member in &mut state.members {
        if member.pool.as_deref() == Some(pool.name.as_str()) {
            member.tags.clone_from(&tags);
        }
    }
}
