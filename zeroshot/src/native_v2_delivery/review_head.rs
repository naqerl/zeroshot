use super::{ForgeReviewObservation, ForgeReviewReceipt, ForgeReviewState, valid_revision};

#[derive(Clone, Copy)]
pub struct ForgeHeadSynchronization<'a> {
    pub workspace: &'a std::path::Path,
    pub previous: &'a ForgeReviewReceipt,
    pub updated: &'a ForgeReviewReceipt,
}

impl ForgeReviewReceipt {
    pub(crate) fn observation(&self, state: ForgeReviewState) -> ForgeReviewObservation {
        self.observation_with_readiness(state, false, false)
    }

    pub(crate) fn observation_with_readiness(
        &self,
        state: ForgeReviewState,
        pull_request_ready: bool,
        head_update_required: bool,
    ) -> ForgeReviewObservation {
        ForgeReviewObservation {
            review_id: self.review_id.clone(),
            repository: self.repository.clone(),
            target_branch: self.target_branch.clone(),
            head_branch: self.head_branch.clone(),
            head_revision: self.head_revision.clone(),
            state,
            pull_request_ready,
            head_update_required,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ForgeMergeRequestOutcome {
    Accepted,
    Pending,
    HeadUpdateRequired,
    Conflict,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ForgeHeadUpdateOutcome {
    Updated(ForgeReviewReceipt),
    Pending,
    Conflict,
}

pub(super) fn valid_head_update(
    previous: &ForgeReviewReceipt,
    updated: &ForgeReviewReceipt,
) -> bool {
    updated.review_id == previous.review_id
        && updated.repository == previous.repository
        && updated.target_branch == previous.target_branch
        && updated.head_branch == previous.head_branch
        && valid_revision(&updated.head_revision)
        && updated.head_revision != previous.head_revision
}

/// Identity-constrained read before any delivery mutation.
#[derive(Clone, Copy)]
pub struct ForgeDeliveryRead<'a> {
    pub target: &'a super::DeliveryTarget,
    pub head_branch: &'a str,
    pub known_review: Option<&'a ForgeReviewReceipt>,
    pub include_review: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForgeDeliverySnapshot {
    pub review: Option<ForgeReviewObservation>,
    pub head_revision: Option<String>,
}

#[derive(Clone, Copy)]
pub struct ForgeHeadReconciliation<'a> {
    pub workspace: &'a std::path::Path,
    pub published: &'a ForgeReviewReceipt,
    pub observed: &'a ForgeReviewReceipt,
    pub commit_message: &'a str,
    pub authorized_update: bool,
    pub adopting_existing: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ForgeReconciliationOutcome {
    Unchanged,
    Adopted,
    NeedsWork(String),
    Refused(String),
}

/// Target integration retains the admitted source revision as provenance.
#[derive(Clone, Copy)]
pub struct ForgeTargetReconciliation<'a> {
    pub workspace: &'a std::path::Path,
    pub target: &'a super::DeliveryTarget,
    pub commit_message: &'a str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForgeTargetIntegration {
    pub target_revision: String,
    pub outcome: ForgeReconciliationOutcome,
}
