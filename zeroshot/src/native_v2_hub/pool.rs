//! Reconcile the pool's member leases, so capacity that no longer exists cannot wedge placement.
//!
//! One member is one pool lease: a durable definition (name, tags, slots, priority) plus the live
//! target behind it. A box deleted out of band leaves the definition with nothing that can serve
//! work. Such a lease must stop counting as capacity, and the hub must not fail jobs because of
//! it: the definition is retained, so the same member returns as soon as its target answers
//! again (a recreated box, or a recovered target).
//!
//! The pass runs at daemon start and before every placement decision, so neither a crash nor a
//! killed box survives a restart as capacity that is not there.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use super::clock::rfc3339;
use super::model::{LOCAL_MEMBER, Member, MemberState, StaleLease};
use super::placement::Load;

/// Reason code carried by every lease this pass drops, in the dispatch report and in `hub.pool`.
pub(crate) const RECONCILED_STALE_LEASE: &str = "reconciled-stale-lease";
/// A lease unreachable for this long cannot be live, once enough probes agree.
pub(crate) const STALE_LEASE_AFTER_MS: u64 = 2 * 60 * 1000;
/// Consecutive failed probes needed before an unreachable lease is reconciled away.
pub(crate) const STALE_LEASE_MIN_PROBES: u32 = 3;
/// Reason code for a retained definition that a successful probe made live capacity again.
pub(crate) const REARMED_LEASE: &str = "re-armed-lease";

/// One lease dropped by reconciliation, or re-armed by a successful probe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Reconciled {
    pub member: String,
    pub detail: String,
}

/// Whether a member name still resolves as a hub target. `None` means the registry could not be
/// read, which proves nothing about the lease and therefore never drops it.
pub(crate) type TargetPresence = Option<bool>;

/// Drop every active lease that cannot be live: its target is no longer registered on the hub
/// host (the box was deleted out of band), or it has stayed unreachable past the stale budget.
///
/// The definition is kept; only the live lease is reconciled away. `registered` answers whether
/// a member name still resolves as a hub target.
pub(crate) fn reconcile(
    members: &mut [Member],
    now_ms: u64,
    registered: impl Fn(&str) -> TargetPresence,
) -> Vec<Reconciled> {
    let mut reconciled = Vec::new();
    for member in members {
        if member.state != MemberState::Active || member.name == LOCAL_MEMBER {
            continue;
        }
        let detail = match registered(&member.name) {
            // The box behind this member is gone from the hub host's configuration.
            Some(false) => Some("registered as a member but not as a hub target".to_owned()),
            Some(true) => member
                .unreachable_since_ms
                .filter(|since| {
                    member.unreachable_probes >= STALE_LEASE_MIN_PROBES
                        && now_ms.saturating_sub(*since) > STALE_LEASE_AFTER_MS
                })
                .map(|since| {
                    format!(
                        "unreachable for {}ms across {} consecutive probes",
                        now_ms.saturating_sub(since),
                        member.unreachable_probes
                    )
                }),
            // An unreadable registry proves nothing; keep the lease as it is.
            None => None,
        };
        let Some(detail) = detail else {
            continue;
        };
        member.state = MemberState::Stale;
        member.stale = Some(StaleLease {
            reason: RECONCILED_STALE_LEASE.to_owned(),
            detail: detail.clone(),
            at_ms: now_ms,
        });
        reconciled.push(Reconciled {
            member: member.name.clone(),
            detail,
        });
    }
    reconciled
}

/// Re-arm reconciled definitions whose target answers again, so a recreated box heals placement
/// without an operator action. Only a successful probe of a still-registered target qualifies.
pub(crate) fn rearm(
    members: &mut [Member],
    loads: &BTreeMap<String, Load>,
    registered: impl Fn(&str) -> TargetPresence,
) -> Vec<Reconciled> {
    let mut rearmed = Vec::new();
    for member in members {
        if member.state != MemberState::Stale || registered(&member.name) != Some(true) {
            continue;
        }
        if !loads.get(&member.name).is_some_and(|load| load.reachable) {
            continue;
        }
        member.state = MemberState::Active;
        member.stale = None;
        member.unreachable_probes = 0;
        member.unreachable_since_ms = None;
        rearmed.push(Reconciled {
            member: member.name.clone(),
            detail: "target answers again; lease re-armed".to_owned(),
        });
    }
    rearmed
}

/// Report entries for one reconciliation pass. Dropped and re-armed leases are tagged, so a
/// `hub.pool` subscriber sees one record per lease change.
pub(crate) fn report(dropped: &[Reconciled], rearmed: &[Reconciled], now_ms: u64) -> Vec<Value> {
    let entry = |reconciled: &Reconciled, phase: &str, reason: &str| {
        json!({
            "member": reconciled.member,
            "phase": phase,
            "reason": reason,
            "detail": reconciled.detail,
            "at": rfc3339(now_ms),
        })
    };
    dropped
        .iter()
        .map(|reconciled| entry(reconciled, "failed", RECONCILED_STALE_LEASE))
        .chain(
            rearmed
                .iter()
                .map(|reconciled| entry(reconciled, "active", REARMED_LEASE)),
        )
        .collect()
}
