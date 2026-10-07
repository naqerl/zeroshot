use super::*;

pub(super) enum ReviewProgress {
    Merged(String),
    CiFailed(String),
    Behind,
    PullRequestReady,
    Mergeable,
    Pending,
    Conflict,
    Closed,
}

pub(super) enum ReviewStep {
    Continue,
    Complete(WorkerOutcome),
}

impl ReviewProgress {
    pub(super) fn from_observation(
        observation: ForgeReviewObservation,
    ) -> Result<Self, DeliveryStop> {
        match observation.state {
            ForgeReviewState::Merged { merge_revision } if valid_revision(&merge_revision) => {
                Ok(Self::Merged(merge_revision))
            }
            ForgeReviewState::Merged { .. } => {
                Err(DeliveryStop::Outcome(WorkerOutcome::malformed()))
            }
            ForgeReviewState::Open { checks } => Ok(Self::from_open(
                checks,
                observation.pull_request_ready,
                observation.head_update_required,
            )),
            ForgeReviewState::Conflict => Ok(Self::Conflict),
            ForgeReviewState::Closed => Ok(Self::Closed),
        }
    }

    fn from_open(checks: ForgeChecks, ready: bool, behind: bool) -> Self {
        match checks {
            ForgeChecks::Failed { diagnostic } => Self::CiFailed(diagnostic),
            ForgeChecks::Pending => Self::Pending,
            ForgeChecks::NotRequired | ForgeChecks::Passed if behind => Self::Behind,
            ForgeChecks::NotRequired | ForgeChecks::Passed if ready => Self::PullRequestReady,
            ForgeChecks::NotRequired | ForgeChecks::Passed => Self::Mergeable,
        }
    }
}

pub(super) async fn failed_review(
    control: &DriverControl,
    detail: &str,
) -> Result<ReviewStep, DeliveryStop> {
    let error = NodeRunnerError::DriverDetail(detail.to_owned());
    let _ = report_provider_error("Git delivery", &error, &[], control).await;
    Err(crash_outcome())
}

pub(super) fn crash_outcome() -> DeliveryStop {
    DeliveryStop::Outcome(WorkerOutcome::declared_failure(WorkerErrorCode::Crash))
}

pub(super) async fn review_completion(
    drive: &ReviewDrive<'_>,
    label: &'static str,
    diagnostic: &str,
    merge_revision: Option<&str>,
) -> Result<ReviewStep, DeliveryStop> {
    let diagnostic = drive.adapter.review_context(diagnostic);
    emit(drive.control, &diagnostic).await?;
    validate_delivery_contract(drive.mode, drive.response).map_err(DeliveryStop::Runner)?;
    delivery_outcome(
        DeliveryResult {
            mode: drive.mode,
            outcome: label,
            review: &drive.review,
            merge_revision,
        },
        &diagnostic,
    )
    .map(ReviewStep::Complete)
    .map_err(DeliveryStop::Runner)
}

#[cfg(test)]
#[path = "review/tests.rs"]
mod tests;
