use serde_json::{Value, json};
use thiserror::Error;

use super::placement::Rejection;

/// Exit code for retryable placement outcomes: busy members or an expired --wait.
pub(crate) const EXIT_NO_CAPACITY: i32 = 3;
/// Exit code when no active member matches the placement at all.
pub(crate) const EXIT_NO_ELIGIBLE_TARGET: i32 = 4;
/// Exit code when the selected hub context does not answer.
pub(crate) const EXIT_UNREACHABLE: i32 = 5;
/// Exit code of `hub eval report --fail-on-regression` when a variant regressed.
pub(crate) const EXIT_REGRESSION: i32 = 6;

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
    EvalNotFound(String),
    #[error("{0}")]
    InvalidState(String),
    #[error("{0}")]
    Conflict(String),
    #[error("hub storage {0}")]
    Storage(String),
    /// The selected hub context's API did not answer.
    #[error("hub context {context} ({url}) is unreachable: {cause}{}", unreachable_hint(.context))]
    Unreachable {
        context: String,
        url: String,
        cause: String,
    },
    /// The hub API rejected the context's bearer token.
    #[error("hub context {context} ({url}) rejected the bearer token: {message}")]
    Unauthorized {
        context: String,
        url: String,
        message: String,
    },
    /// Another daemon already owns the hub folder.
    #[error("{0}")]
    DaemonRunning(String),
    /// An error document returned by a hub daemon, replayed verbatim with its exit code.
    #[error("{message}")]
    Remote {
        message: String,
        diagnostic: Value,
        exit_code: i32,
    },
    #[error("{message}")]
    Target {
        message: String,
        diagnostic: Option<Value>,
    },
    /// The target gave no usable answer (a timeout, a lost connection, or no receipt), so
    /// whether it acted is unknown.
    #[error("{0}")]
    Unanswered(String),
}

impl HubError {
    pub(crate) const fn exit_code(&self) -> i32 {
        match self {
            Self::NoCapacity(_) | Self::WaitTimeout(_) => EXIT_NO_CAPACITY,
            Self::NoEligibleTarget { .. } => EXIT_NO_ELIGIBLE_TARGET,
            Self::Unreachable { .. } => EXIT_UNREACHABLE,
            Self::Remote { exit_code, .. } => *exit_code,
            _ => 1,
        }
    }

    /// HTTP status the hub API answers this error with; the body is the diagnostic.
    pub(crate) const fn http_status(&self) -> u16 {
        match self {
            Self::Usage(_) => 400,
            Self::JobNotFound(_) | Self::MemberNotFound(_) | Self::EvalNotFound(_) => 404,
            Self::InvalidState(_) | Self::Conflict(_) | Self::DaemonRunning(_) => 409,
            Self::NoEligibleTarget { .. } => 422,
            Self::NoCapacity(_) | Self::WaitTimeout(_) => 503,
            Self::Unauthorized { .. } => 401,
            Self::Target { .. } | Self::Unreachable { .. } | Self::Unanswered(_) => 502,
            Self::Storage(_) | Self::Remote { .. } => 500,
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
            Self::EvalNotFound(_) => ("invalid_request", "hub.eval_not_found"),
            Self::InvalidState(_) => ("invalid_request", "hub.invalid_state"),
            Self::Conflict(_) => ("invalid_request", "hub.conflict"),
            Self::Storage(_) => ("target", "hub.storage"),
            Self::Unreachable { .. } => ("target", "hub.unreachable"),
            Self::Unauthorized { .. } => ("target", "hub.unauthorized"),
            Self::DaemonRunning(_) => ("invalid_request", "hub.daemon_running"),
            Self::Remote { .. } => ("target", "hub.remote"),
            Self::Target { .. } => ("target", "hub.target"),
            Self::Unanswered(_) => ("target", "hub.target_unanswered"),
        }
    }

    /// `zeroshot.error/v1` diagnostic for `ZEROSHOT_ERROR_FORMAT=json`.
    pub(crate) fn diagnostic(&self) -> Value {
        if let Self::Remote { diagnostic, .. } = self {
            return diagnostic.clone();
        }
        let (kind, code) = self.code();
        let details = match self {
            Self::Target {
                diagnostic: Some(diagnostic),
                ..
            } => json!({ "target": diagnostic }),
            Self::Unreachable { context, url, .. } | Self::Unauthorized { context, url, .. } => {
                json!({ "context": context, "url": url })
            }
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

fn unreachable_hint(context: &str) -> &'static str {
    if context == "local" {
        "; start this machine's hub with `zeroshot hub install --systemd`, `zeroshot hub install \
         --docker`, or `zeroshot hub daemon run`"
    } else {
        "; check the URL and that its daemon runs, or pick another hub with --hub"
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
