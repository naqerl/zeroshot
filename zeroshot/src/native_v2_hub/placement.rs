use std::collections::BTreeMap;

use super::model::{LOCAL_MEMBER, Member, MemberState, PlacementSpec, Requirements, Strategy};
use super::profile::SpecSource;

/// Observed reachability and active runs of one member.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Load {
    pub reachable: bool,
    pub active: u32,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Placement {
    /// Place on this member.
    Place(String),
    /// Members match and can run the job, but none is reachable with a free slot, or a
    /// matching member's capabilities are not discovered yet.
    Busy { matching: Vec<String> },
    /// No active member matches the target and tags, or every matching member is known not to
    /// support the job. `rejected` names each such member with its reason.
    NoEligible { rejected: Vec<Rejection> },
}

/// A selector-matching member that cannot run a job, with the reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Rejection {
    pub member: String,
    pub reason: String,
}

/// Whether one member can run a job with these requirements.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Fit {
    Supported,
    /// The target has not been discovered yet; retry after discovery.
    Unknown,
    Unsupported(String),
}

/// Active members that satisfy the pinned target and every required tag, in registration order.
pub(crate) fn matching_members<'a>(members: &'a [Member], spec: &PlacementSpec) -> Vec<&'a Member> {
    members
        .iter()
        .filter(|member| member.state == MemberState::Active)
        .filter(|member| {
            spec.target
                .as_ref()
                .is_none_or(|target| *target == member.name)
        })
        .filter(|member| spec.tags.iter().all(|tag| member.has_tag(tag)))
        .collect()
}

/// Capability match of one member. The local controller runs in-process and is not gated on
/// discovery markers; named targets must advertise every required discovery kind. A pool
/// member smaller than the job's tag profile asks for never fits; a bigger one does.
pub(crate) fn fit(member: &Member, requirements: &Requirements) -> Fit {
    if let Some(reason) = undersized(member, requirements) {
        return Fit::Unsupported(reason);
    }
    if member.name == LOCAL_MEMBER {
        return requirements
            .local_unsupported
            .clone()
            .map_or(Fit::Supported, Fit::Unsupported);
    }
    if let Some(reason) = &requirements.remote_unsupported {
        return Fit::Unsupported(reason.clone());
    }
    if requirements.capabilities.is_empty() {
        return Fit::Supported;
    }
    let Some(advertised) = &member.capabilities else {
        return Fit::Unknown;
    };
    let missing = requirements
        .capabilities
        .iter()
        .filter(|kind| !advertised.contains(kind))
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        Fit::Supported
    } else {
        Fit::Unsupported(format!("target does not advertise {}", missing.join(", ")))
    }
}

fn undersized(member: &Member, requirements: &Requirements) -> Option<String> {
    let spec = requirements
        .machine
        .as_ref()
        .filter(|spec| spec.source == SpecSource::Tag)?;
    let machine = member.machine.as_ref()?;
    (!spec
        .want
        .fits(&machine.server_type, machine.cores, machine.memory_mb))
    .then(|| {
        format!(
            "{} ({} vCPU, {} GB) is smaller than tag {} needs ({})",
            machine.server_type,
            machine.cores,
            machine.memory_mb / 1024,
            spec.tag.as_deref().unwrap_or_default(),
            spec.want.describe()
        )
    })
}

/// Members matching the selector that are known not to support the job. `None` when at least
/// one matching member supports it or might once discovered.
pub(crate) fn rejections(
    members: &[Member],
    spec: &PlacementSpec,
    requirements: &Requirements,
) -> Option<Vec<Rejection>> {
    let mut rejected = Vec::new();
    for member in matching_members(members, spec) {
        match fit(member, requirements) {
            Fit::Unsupported(reason) => rejected.push(Rejection {
                member: member.name.clone(),
                reason,
            }),
            Fit::Supported | Fit::Unknown => return None,
        }
    }
    Some(rejected)
}

/// Pick a free supported member. Members absent from `loads` are treated as unreachable.
pub(crate) fn place(
    members: &[Member],
    spec: &PlacementSpec,
    requirements: &Requirements,
    loads: &BTreeMap<String, Load>,
) -> Placement {
    if let Some(rejected) = rejections(members, spec, requirements) {
        return Placement::NoEligible { rejected };
    }
    let matching = matching_members(members, spec)
        .into_iter()
        .filter(|member| !matches!(fit(member, requirements), Fit::Unsupported(_)))
        .collect::<Vec<_>>();
    let mut free = matching
        .iter()
        .enumerate()
        .filter(|(_, member)| fit(member, requirements) == Fit::Supported)
        .filter_map(|(order, member)| {
            let load = loads.get(&member.name)?;
            (load.reachable && load.active < member.slots).then_some((order, *member, load.active))
        })
        .collect::<Vec<_>>();
    free.sort_by(|left, right| {
        let by_priority = right
            .1
            .priority
            .cmp(&left.1.priority)
            .then(left.0.cmp(&right.0));
        match spec.strategy {
            Strategy::FirstFree => by_priority,
            Strategy::LeastLoaded => {
                let left_ratio = u64::from(left.2) * u64::from(right.1.slots);
                let right_ratio = u64::from(right.2) * u64::from(left.1.slots);
                left_ratio.cmp(&right_ratio).then(by_priority)
            }
        }
    });
    match free.first() {
        Some((_, member, _)) => Placement::Place(member.name.clone()),
        None => Placement::Busy {
            matching: matching.iter().map(|member| member.name.clone()).collect(),
        },
    }
}

/// Human-readable placement for error messages.
pub(crate) fn describe(spec: &PlacementSpec) -> String {
    let mut parts = Vec::new();
    if let Some(target) = &spec.target {
        parts.push(format!("target {target}"));
    }
    if !spec.tags.is_empty() {
        parts.push(format!("tags [{}]", spec.tags.join(", ")));
    }
    if parts.is_empty() {
        "any member".to_owned()
    } else {
        parts.join(" and ")
    }
}
