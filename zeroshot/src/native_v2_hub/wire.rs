//! Request bodies of the hub API, shared by the daemon and its clients.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::model::{Job, MemberState, Schedule};
use super::pool::Provider;

/// `POST /v1/hub/members`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct AddMember {
    pub name: String,
    #[serde(default)]
    pub tags: Vec<String>,
    pub slots: u32,
    #[serde(default)]
    pub priority: i32,
}

/// `POST /v1/hub/pools`. Paths are absolute paths on the hub host.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct AddPool {
    pub name: String,
    pub provider: Provider,
    /// Server types, best first.
    pub server_types: Vec<String>,
    /// Locations, in order; empty means every location offering one of the types.
    #[serde(default)]
    pub locations: Vec<String>,
    #[serde(default)]
    pub multi_arch: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_max_ms: Option<u64>,
    pub image: String,
    pub target_image: String,
    pub max: u32,
    pub slots: u32,
    pub idle_ms: u64,
    pub tags: Vec<String>,
    pub ssh_key: std::path::PathBuf,
    pub ssh_from: Vec<String>,
    /// The servers' btrfs data folder; default: a file on the local disk.
    #[serde(default)]
    pub data: super::data::DataSettings,
}

/// `PUT /v1/hub/pools/{name}/tags/{tag}`: the machines a pool creates for jobs with a tag.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct SetTagProfile {
    /// Server types, best first; empty means the pool's types that meet the minimum.
    #[serde(default)]
    pub server_types: Vec<String>,
    /// Locations in order; empty means the pool's.
    #[serde(default)]
    pub locations: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_cores: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_memory_gb: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slots: Option<u32>,
    #[serde(default)]
    pub priority: u32,
}

/// `PATCH /v1/hub/members/{name}`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct UpdateMember {
    #[serde(default)]
    pub add_tags: Vec<String>,
    #[serde(default)]
    pub remove_tags: Vec<String>,
    #[serde(default)]
    pub slots: Option<u32>,
    #[serde(default)]
    pub priority: Option<i32>,
    #[serde(default)]
    pub state: Option<MemberState>,
}

/// `POST /v1/hub/members/query`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct MemberQuery {
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub probe: bool,
}

/// A file named by a run argument (`--input`, `--graph`, `--runtime-config`,
/// `--uniform-runtime-config`, `--environment`), read by the client so the hub host does not
/// need the client's files. `argument` is the exact argument value it replaces.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct JobFile {
    pub argument: String,
    pub content: String,
}

/// `POST /v1/hub/jobs`: place now; queue when busy only with `queue`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct SubmitJob {
    pub job: Job,
    #[serde(default)]
    pub queue: bool,
    #[serde(default)]
    pub files: Vec<JobFile>,
}

/// `POST /v1/hub/queue`: accept for later placement.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct EnqueueJob {
    pub job: Job,
    #[serde(default)]
    pub files: Vec<JobFile>,
}

/// `POST /v1/hub/schedules`: a recurring schedule built by the client.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct CreateSchedule {
    pub schedule: Schedule,
    #[serde(default)]
    pub files: Vec<JobFile>,
}

/// `POST /v1/hub/jobs/query`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct JobQuery {
    pub state: String,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub labels: Vec<(String, String)>,
    #[serde(default)]
    pub limit: Option<usize>,
}

/// `POST /v1/hub/stats`. Times are already resolved by the client.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct StatsRequest {
    #[serde(default)]
    pub by: Vec<String>,
    #[serde(default)]
    pub since_ms: Option<u64>,
    #[serde(default)]
    pub until_ms: Option<u64>,
    /// The `--prices` table as read by the client.
    #[serde(default)]
    pub prices: Option<Value>,
}

/// `POST /v1/hub/jobs/{id}/stream`: the observation or stop command to proxy.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "op")]
pub(crate) enum StreamRequest {
    Watch {
        #[serde(default)]
        after: Option<String>,
    },
    Logs {
        #[serde(default)]
        after: Option<String>,
        #[serde(default)]
        execution: Option<String>,
    },
    Attach {
        execution: String,
    },
    ForceStop,
}

/// One NDJSON frame of a proxied command: a stdout or stderr line, then its exit code.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", untagged)]
pub(crate) enum StreamFrame {
    Stdout { stdout: String },
    Stderr { stderr: String },
    Exit { exit: i32 },
}
