//! Gitea/Forgejo merge-policy reconstruction.
//!
//! GitHub's delivery path derives one "would GitHub merge this?" snapshot from a single GraphQL
//! policy query: merge state, review decision, required checks, allowed merge methods, and ref
//! rules. Gitea's REST surface exposes the same facts through separate endpoints, so this module
//! reconstructs the closest equivalent snapshot and documents every GitHub concept Gitea cannot
//! represent.
//!
//! Gitea has no ruleset concept, no required-linear-history rule, no merge queue, and no
//! per-branch merge-method restriction. None of those are emulated here; the code comments below
//! name each gap.

use super::{
    BranchProtectionWire, ForgeAuthorityError, ForgeChecks, ForgeReviewState, PullWire,
    RepositoryPolicyWire, ReviewWire, StatusWire, valid_revision,
};

/// A reconstructed merge-policy snapshot, mirroring GitHub's `PolicySnapshot`.
#[derive(Debug)]
pub(super) struct GiteaPolicySnapshot {
    pub(super) state: ForgeReviewState,
    pub(super) pull_request_ready: bool,
}

/// The strongest evidence Gitea offers for the required-status-check gate.
enum RequiredChecksEvidence {
    Absent,
    Passed,
    Pending,
    Failed { diagnostic: String },
}

/// GitHub merge methods mapped onto Gitea's `do` values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GiteaMergeMethod {
    Merge,
    Squash,
    Rebase,
}

impl GiteaMergeMethod {
    pub(super) fn do_value(self) -> &'static str {
        match self {
            Self::Merge => "merge",
            Self::Squash => "squash",
            Self::Rebase => "rebase",
        }
    }
}

/// Select the merge method exactly as GitHub's intersection does.
///
/// GitHub's merge-method set is `{merge, squash, rebase}`; the first allowed method wins in that
/// order. Gitea's `default_merge_style` is a UI preference rather than merge authority, so it is
/// deliberately ignored, and Gitea exposes no per-branch merge-method restriction to intersect
/// with. Gitea's extra `rebase-merge` style has no GitHub-equivalent method, so a repository that
/// allows only that style is refused rather than silently emulated.
pub(super) fn select_merge_method(
    policy: &RepositoryPolicyWire,
) -> Result<GiteaMergeMethod, ForgeAuthorityError> {
    let selected = [
        (policy.allow_merge_commits, GiteaMergeMethod::Merge),
        (policy.allow_squash_merge, GiteaMergeMethod::Squash),
        (policy.allow_rebase, GiteaMergeMethod::Rebase),
    ]
    .into_iter()
    .find_map(|(allowed, method)| allowed.then_some(method));
    if let Some(method) = selected {
        return Ok(method);
    }
    if policy.allow_rebase_explicit {
        return Err(ForgeAuthorityError::api(
            None,
            "This Gitea/Forgejo repository allows only rebase-merge, which has no \
             GitHub-equivalent merge method. Enable merge commits, squash, or rebase before \
             retrying delivery.",
        ));
    }
    Err(ForgeAuthorityError::api(
        None,
        "No merge method is enabled for this Gitea/Forgejo repository. Enable merge commits, \
         squash, or rebase before retrying delivery.",
    ))
}

/// Reconstruct the open-pull-request snapshot.
pub(super) fn classify_open_snapshot(
    pull: &PullWire,
    protection: Option<&BranchProtectionWire>,
    reviews: &[ReviewWire],
    status: &StatusWire,
) -> Result<GiteaPolicySnapshot, ForgeAuthorityError> {
    let evidence = classify_required_checks(protection, status);
    if let RequiredChecksEvidence::Failed { diagnostic } = evidence {
        return Ok(GiteaPolicySnapshot {
            state: ForgeReviewState::Open {
                checks: ForgeChecks::Failed { diagnostic },
            },
            pull_request_ready: false,
        });
    }
    if protection.is_some_and(|protection| protection.block_on_outdated_branch)
        && branch_is_behind(pull)
    {
        // Gitea can require the pull request head to be current with its base, but its API exposes
        // no operation to advance that head the way GitHub's `updatePullRequestBranch` does.
        // Merging a stale head would bypass the reviewed revision, so this is an explicit
        // unsupported-concept refusal rather than a silent emulation.
        return Err(ForgeAuthorityError::api(
            None,
            "Gitea/Forgejo requires the pull request head to be up to date with the base branch, \
             but Gitea exposes no API to advance the pull request head (GitHub's \
             updatePullRequestBranch). Rebase the delivery branch onto the target revision and \
             retry.",
        ));
    }
    let merge_ready = merge_gate_ready(pull, protection, reviews);
    let handoff = pull_request_ready(pull, protection, reviews);
    let policy_ready = merge_ready || handoff;
    let checks = match (policy_ready, &evidence) {
        (true, RequiredChecksEvidence::Absent) => ForgeChecks::NotRequired,
        (true, RequiredChecksEvidence::Passed) => ForgeChecks::Passed,
        _ => ForgeChecks::Pending,
    };
    let pull_request_ready = !matches!(evidence, RequiredChecksEvidence::Pending) && handoff;
    Ok(GiteaPolicySnapshot {
        state: ForgeReviewState::Open { checks },
        pull_request_ready,
    })
}

/// Gitea's equivalent of GitHub's merge gate (`mergeable` plus `mergeStateStatus`).
///
/// Gitea does not compute a merge-state string, so approval and rejected-review policy are folded
/// in here, exactly where GitHub would report `BLOCKED`.
fn merge_gate_ready(
    pull: &PullWire,
    protection: Option<&BranchProtectionWire>,
    reviews: &[ReviewWire],
) -> bool {
    !pull.draft
        && pull.mergeable == Some(true)
        && approvals_ok(protection, reviews)
        && !rejected_review_blocked(protection, reviews)
}

/// Gitea's equivalent of GitHub's "PR ready for approval handoff" state.
///
/// Mirrors GitHub's `pull_request_ready`: the pull request is technically mergeable but a review
/// decision still blocks it, and the base policy has a human approval gate. Gitea has no
/// code-owner-review or conversation-resolution concept, so only required approvals participate.
fn pull_request_ready(
    pull: &PullWire,
    protection: Option<&BranchProtectionWire>,
    reviews: &[ReviewWire],
) -> bool {
    let Some(protection) = protection else {
        return false;
    };
    !pull.draft
        && pull.mergeable == Some(true)
        && handoff_policy_ready(protection)
        && (!approvals_ok(Some(protection), reviews)
            || rejected_review_blocked(Some(protection), reviews))
}

/// Mirrors GitHub's `approval_handoff_policy_ready`.
///
/// Gitea has no code-owner reviews, no conversation-resolution rule, and no linear-history rule.
/// `require_signed_commits` disables the handoff path just as GitHub's `requires_signatures` does;
/// Gitea's server-side merge signing is not modeled, so the merge gate deliberately ignores it and
/// lets a real rejection flow through the failure classifier.
fn handoff_policy_ready(protection: &BranchProtectionWire) -> bool {
    protection.required_approvals > 0 && !protection.require_signed_commits
}

/// Whether the base branch requires the pull request head to be current.
///
/// Gitea does not expose `CommitsBehind`, but a merge base that differs from the base head proves
/// the head is behind. A missing or malformed merge base is treated as current.
fn branch_is_behind(pull: &PullWire) -> bool {
    pull.merge_base
        .as_deref()
        .filter(|base| valid_revision(base))
        .is_some_and(|base| base != pull.base.sha)
}

fn approvals_ok(protection: Option<&BranchProtectionWire>, reviews: &[ReviewWire]) -> bool {
    match protection {
        Some(protection) if protection.required_approvals > 0 => {
            distinct_approvers(reviews) >= protection.required_approvals as usize
        }
        _ => true,
    }
}

/// Count distinct approving users.
///
/// Gitea's own approval count counts review rows, but the task requires distinct approvers, which
/// is also GitHub's model. Only official, non-dismissed approvals count; stale approvals are left
/// counted because Gitea's default is `IgnoreStaleApprovals = false`.
fn distinct_approvers(reviews: &[ReviewWire]) -> usize {
    let mut logins: Vec<&str> = reviews
        .iter()
        .filter(|review| review.state == "APPROVED" && review.official && !review.dismissed)
        .map(|review| review.user.login.as_str())
        .collect();
    logins.sort_unstable();
    logins.dedup();
    logins.len()
}

fn rejected_review_blocked(
    protection: Option<&BranchProtectionWire>,
    reviews: &[ReviewWire],
) -> bool {
    protection.is_some_and(|protection| {
        protection.block_on_rejected_reviews
            && reviews.iter().any(|review| {
                review.state == "REQUEST_CHANGES" && review.official && !review.dismissed
            })
    })
}

/// Reconstruct the required-status-check gate.
///
/// With branch-protection contexts, Gitea matches them with glob patterns and uses the worst
/// matched state. This reconstruction uses exact context equality, so a repository that relies on
/// wildcard contexts may be reported as pending rather than merged. Without protection contexts,
/// Gitea falls back to its combined commit status, which is preserved here.
fn classify_required_checks(
    protection: Option<&BranchProtectionWire>,
    status: &StatusWire,
) -> RequiredChecksEvidence {
    let required = protection
        .filter(|protection| protection.enable_status_check)
        .map(|protection| protection.status_check_contexts.as_slice())
        .unwrap_or(&[]);
    if required.is_empty() {
        return match super::classify_checks(status) {
            ForgeChecks::NotRequired => RequiredChecksEvidence::Absent,
            ForgeChecks::Passed => RequiredChecksEvidence::Passed,
            ForgeChecks::Pending => RequiredChecksEvidence::Pending,
            ForgeChecks::Failed { diagnostic } => RequiredChecksEvidence::Failed { diagnostic },
        };
    }
    let mut pending = false;
    let mut diagnostic = None;
    for context in required {
        match status
            .statuses
            .iter()
            .find(|entry| &entry.context == context)
        {
            None => pending = true,
            Some(entry) => match entry.status.to_ascii_lowercase().as_str() {
                "success" => {}
                "failure" | "error" => {
                    diagnostic = Some(format!(
                        "Gitea required status check {context} is {}",
                        entry.status
                    ));
                }
                _ => pending = true,
            },
        }
    }
    if let Some(diagnostic) = diagnostic {
        return RequiredChecksEvidence::Failed { diagnostic };
    }
    if pending {
        RequiredChecksEvidence::Pending
    } else {
        RequiredChecksEvidence::Passed
    }
}
