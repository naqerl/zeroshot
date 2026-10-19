use serde_json::{Value, json};
use thiserror::Error;

use super::placement::Rejection;

/// Exit code for retryable placement outcomes: busy members or an expired --wait.
pub(crate) const EXIT_NO_CAPACITY: i32 = 3;
/// Exit code when no active member matches the placement at all.
pub(crate) const EXIT_NO_ELIGIBLE_TARGET: i32 = 4;

#[derive(Debug, Error)]
pub(crate) enum HubError {
    #[error("{0}")]
    Usage(String),
    #[error("{}", no_eligible_message(.placement, .rejected))]
    NoEligibleTarget {
        placement: String,
        rejected: Vec<Rejection>,
    },
    #[error("no free hub member for {0}; retry, or use --wait or --queue")]
    NoCapacity(String),
    #[error("timed out waiting for a free hub member for {0}")]
    WaitTimeout(String),
    #[error("hub job {0} was not found")]
    JobNotFound(String),
    #[error("{0}")]
    MemberNotFound(String),
    #[error("{0}")]
    InvalidState(String),
    #[error("{0}")]
    Conflict(String),
    #[error("hub storage {0}")]
    Storage(String),
    #[error("{message}")]
    Target {
        message: String,
        diagnostic: Option<Value>,
    },
}

impl HubError {
    pub(crate) const fn exit_code(&self) -> i32 {
        match self {
            Self::NoCapacity(_) | Self::WaitTimeout(_) => EXIT_NO_CAPACITY,
            Self::NoEligibleTarget { .. } => EXIT_NO_ELIGIBLE_TARGET,
            _ => 1,
        }
    }

    const fn code(&self) -> (&'static str, &'static str) {
        match self {
            Self::Usage(_) => ("invalid_request", "hub.invalid_request"),
            Self::NoEligibleTarget { .. } => ("target", "hub.no_eligible_target"),
            Self::NoCapacity(_) => ("target", "hub.no_capacity"),
            Self::WaitTimeout(_) => ("target", "hub.wait_timeout"),
            Self::JobNotFound(_) => ("invalid_request", "hub.job_not_found"),
            Self::MemberNotFound(_) => ("invalid_request", "hub.member_not_found"),
            Self::InvalidState(_) => ("invalid_request", "hub.invalid_state"),
            Self::Conflict(_) => ("invalid_request", "hub.conflict"),
            Self::Storage(_) => ("target", "hub.storage"),
            Self::Target { .. } => ("target", "hub.target"),
        }
    }

    /// `zeroshot.error/v1` diagnostic for `ZEROSHOT_ERROR_FORMAT=json`.
    pub(crate) fn diagnostic(&self) -> Value {
        let (kind, code) = self.code();
        let details = match self {
            Self::Target {
                diagnostic: Some(diagnostic),
                ..
            } => json!({ "target": diagnostic }),
            Self::NoEligibleTarget { rejected, .. } => json!({
                "rejected": rejected
                    .iter()
                    .map(|rejection| json!({ "member": rejection.member, "reason": rejection.reason }))
                    .collect::<Vec<_>>(),
            }),
            _ => json!({}),
        };
        json!({
            "schema": "zeroshot.error/v1",
            "kind": kind,
            "code": code,
            "message": self.to_string(),
            "path": null,
            "node": null,
            "details": details,
        })
    }

    pub(crate) fn no_eligible(placement: String, rejected: Vec<Rejection>) -> Self {
        Self::NoEligibleTarget {
            placement,
            rejected,
        }
    }

    pub(crate) fn storage(action: &str, error: impl std::fmt::Display) -> Self {
        Self::Storage(format!("{action}: {error}"))
    }
}

fn no_eligible_message(placement: &str, rejected: &[Rejection]) -> String {
    if rejected.is_empty() {
        return format!("no active hub member matches {placement}");
    }
    let reasons = rejected
        .iter()
        .map(|rejection| format!("{}: {}", rejection.member, rejection.reason))
        .collect::<Vec<_>>()
        .join("; ");
    format!("no hub member matching {placement} supports this run ({reasons})")
}
