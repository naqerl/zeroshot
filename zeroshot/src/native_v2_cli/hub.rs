//! Parsed `zeroshot hub` requests.
//!
//! The hub is a client-side pool of registered targets. Parsing only checks syntax and keeps
//! the run arguments in the exact `zeroshot run` form; the shipped binary owns placement, queue
//! state, and the delegated target calls.

use std::path::PathBuf;

/// One parsed hub subcommand.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HubCommand {
    Add(HubMemberAdd),
    Update(HubMemberUpdate),
    Remove { name: String },
    List { tags: Vec<String>, probe: bool },
    Submit(Box<HubSubmit>),
    Enqueue(Box<HubEnqueue>),
    Ps(HubPs),
    Status { job: String },
    Watch { job: String, after: Option<String> },
    Logs(HubLogs),
    Attach { job: String, execution: String },
    ForceStop { job: String },
    Dequeue { job: String },
    Dispatch { repeat: bool, interval: String },
    Stats(HubStats),
}

/// Registration of an existing named target, or the built-in `local` controller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubMemberAdd {
    pub name: String,
    pub tags: Vec<String>,
    pub slots: u32,
    pub priority: i32,
}

/// Partial update of one hub member.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HubMemberUpdate {
    pub name: String,
    pub add_tags: Vec<String>,
    pub remove_tags: Vec<String>,
    pub slots: Option<u32>,
    pub priority: Option<i32>,
    pub state: Option<HubMemberState>,
}

/// Placement eligibility of a hub member.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HubMemberState {
    /// Accepts new placements.
    Active,
    /// Keeps running jobs but accepts no new placements.
    Drain,
    /// Ignored by placement and probes.
    Disabled,
}

/// Choice among free eligible members.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HubStrategy {
    /// Highest priority, then registration order.
    #[default]
    FirstFree,
    /// Lowest active-to-slots ratio.
    LeastLoaded,
}

/// A run request shared by `submit` and `enqueue`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubJobSpec {
    pub title: String,
    /// Arguments after `zeroshot run`, without target, submission key, or detach.
    pub run_args: Vec<String>,
    pub target: Option<String>,
    pub tags: Vec<String>,
    pub strategy: HubStrategy,
    pub labels: Vec<(String, String)>,
    /// Runtime files used to attribute token usage to a model.
    pub runtime_files: Vec<PathBuf>,
    pub repository: Option<String>,
    pub branch: Option<String>,
    pub revision: Option<String>,
    pub source_connection: Option<String>,
    /// An `--environment` file was given; local runs reject environment hooks.
    pub environment: bool,
    pub cwd: Option<PathBuf>,
    pub submission_key: Option<String>,
}

/// Immediate placement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubSubmit {
    pub job: HubJobSpec,
    pub detach: bool,
    pub wait: Option<Option<String>>,
    pub queue: bool,
}

/// Deferred placement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubEnqueue {
    pub job: HubJobSpec,
    pub at: Option<String>,
    pub priority: i32,
    pub expires: Option<String>,
}

/// Hub job listing filters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubPs {
    pub state: String,
    pub target: Option<String>,
    pub labels: Vec<(String, String)>,
    pub limit: Option<usize>,
}

/// Log passthrough for one hub job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubLogs {
    pub job: String,
    pub after: Option<String>,
    pub execution: Option<String>,
}

/// Token and outcome aggregation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HubStats {
    pub by: Vec<String>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub prices: Option<PathBuf>,
}
