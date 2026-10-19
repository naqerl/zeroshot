use std::collections::BTreeMap;

use super::model::{Member, MemberState, PlacementSpec, Strategy};

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
    /// Members match, but none is reachable with a free slot.
    Busy { matching: Vec<String> },
    /// No active member matches the target and tags.
    NoEligible,
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
        .filter(|member| spec.tags.iter().all(|tag| member.tags.contains(tag)))
        .collect()
}

/// Pick a free member. Members absent from `loads` are treated as unreachable.
pub(crate) fn place(
    members: &[Member],
    spec: &PlacementSpec,
    loads: &BTreeMap<String, Load>,
) -> Placement {
    let matching = matching_members(members, spec);
    if matching.is_empty() {
        return Placement::NoEligible;
    }
    let mut free = matching
        .iter()
        .enumerate()
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
