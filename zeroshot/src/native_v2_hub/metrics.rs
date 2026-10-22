//! Prometheus metrics of the hub daemon, in the text exposition format (version 0.0.4).
//!
//! Job, queue, schedule, member, and token series are derived from hub state at scrape time.
//! Jobs are never deleted from state, so the derived counters only grow and survive daemon
//! restarts. Pass, placement, and API series are counted in memory since the daemon started.
//! Label values are bounded: no job, run, or schedule IDs appear as labels, API routes use
//! their templates, and models, projects, and opted-in job label values beyond the first
//! [`VALUE_CAP`] distinct values collapse into `_other`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::Mutex;

use super::eval::SampleState;
use super::model::{HubState, Job, JobState, MemberState, ScheduleState};
use super::placement::{Load, Rejection};
use super::usage::{TOKEN_KINDS, Tokens};

/// Distinct values kept per bounded label before the rest collapse into `_other`.
pub(crate) const VALUE_CAP: usize = 50;
/// Overflow value for bounded labels.
pub(crate) const OTHER: &str = "_other";
/// Content type of the text exposition format.
pub(crate) const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Seconds buckets for queue wait and run duration: 1 s to 6 h.
const JOB_BUCKETS: [f64; 12] = [
    1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0, 21600.0,
];
/// Seconds buckets for dispatcher passes.
const PASS_BUCKETS: [f64; 10] = [0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0];
/// Seconds buckets for API requests.
const API_BUCKETS: [f64; 10] = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1.0, 5.0, 30.0];

/// A cumulative histogram over fixed upper bounds.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Histogram {
    bounds: &'static [f64],
    counts: Vec<u64>,
    sum: f64,
    count: u64,
}

impl Histogram {
    pub(crate) fn new(bounds: &'static [f64]) -> Self {
        Self {
            bounds,
            counts: vec![0; bounds.len()],
            sum: 0.0,
            count: 0,
        }
    }

    pub(crate) fn observe(&mut self, value: f64) {
        for (bound, count) in self.bounds.iter().zip(self.counts.iter_mut()) {
            if value <= *bound {
                *count += 1;
            }
        }
        self.sum += value;
        self.count += 1;
    }
}

/// Builder for one exposition document.
#[derive(Default)]
pub(crate) struct Exposition {
    text: String,
}

impl Exposition {
    pub(crate) fn family(&mut self, name: &str, kind: &str, help: &str) {
        let _ = writeln!(self.text, "# HELP {name} {help}");
        let _ = writeln!(self.text, "# TYPE {name} {kind}");
    }

    pub(crate) fn sample(&mut self, name: &str, labels: &[(&str, &str)], value: f64) {
        self.text.push_str(name);
        if !labels.is_empty() {
            self.text.push('{');
            for (index, (key, label)) in labels.iter().enumerate() {
                if index > 0 {
                    self.text.push(',');
                }
                let _ = write!(self.text, "{key}=\"{}\"", escape(label));
            }
            self.text.push('}');
        }
        let _ = writeln!(self.text, " {}", number(value));
    }

    pub(crate) fn histogram(&mut self, name: &str, labels: &[(&str, &str)], histogram: &Histogram) {
        let bucket = format!("{name}_bucket");
        for (bound, count) in histogram.bounds.iter().zip(&histogram.counts) {
            let le = number(*bound);
            let mut with_le = labels.to_vec();
            with_le.push(("le", &le));
            #[allow(clippy::cast_precision_loss)]
            self.sample(&bucket, &with_le, *count as f64);
        }
        let mut with_le = labels.to_vec();
        with_le.push(("le", "+Inf"));
        #[allow(clippy::cast_precision_loss)]
        let count = histogram.count as f64;
        self.sample(&bucket, &with_le, count);
        self.sample(&format!("{name}_sum"), labels, histogram.sum);
        self.sample(&format!("{name}_count"), labels, count);
    }

    pub(crate) fn finish(self) -> String {
        self.text
    }
}

/// Escape a label value: backslash, double quote, and line feed.
pub(crate) fn escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            other => escaped.push(other),
        }
    }
    escaped
}

fn number(value: f64) -> String {
    if value.is_infinite() {
        return if value > 0.0 { "+Inf" } else { "-Inf" }.to_owned();
    }
    if value.fract() == 0.0 && value.abs() < 1e15 {
        #[allow(clippy::cast_possible_truncation)]
        return format!("{}", value as i64);
    }
    format!("{value}")
}

/// Keeps the first [`VALUE_CAP`] distinct values seen, in a stable order (jobs are only ever
/// appended), and maps every later value to [`OTHER`].
#[derive(Default)]
struct Bounded {
    kept: BTreeSet<String>,
}

impl Bounded {
    fn admit(&mut self, value: &str) -> String {
        if self.kept.contains(value) {
            return value.to_owned();
        }
        if self.kept.len() < VALUE_CAP {
            self.kept.insert(value.to_owned());
            return value.to_owned();
        }
        OTHER.to_owned()
    }
}

/// Bounded rejection reason of one member that cannot run a job.
pub(crate) fn rejection_reason(rejection: &Rejection) -> &'static str {
    if rejection.reason.starts_with("target does not advertise") {
        "missing_capability"
    } else if rejection.member == super::model::LOCAL_MEMBER {
        "local_unsupported"
    } else {
        "remote_unsupported"
    }
}

#[derive(Default)]
struct Counters {
    passes: u64,
    pass_errors: u64,
    pass_duration: Option<Histogram>,
    placements: BTreeMap<(String, &'static str), u64>,
    rejections: BTreeMap<&'static str, u64>,
    requests: BTreeMap<(String, String, u16), u64>,
    latency: BTreeMap<(String, String), Histogram>,
    loads: BTreeMap<String, Load>,
}

/// In-memory counters of one daemon, plus the job label keys exported with token counters.
pub(crate) struct Metrics {
    counters: Mutex<Counters>,
    label_keys: Vec<String>,
    started_at_ms: u64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl Metrics {
    pub(crate) fn new(label_keys: Vec<String>) -> Self {
        Self {
            counters: Mutex::new(Counters::default()),
            label_keys,
            started_at_ms: super::clock::now_ms(),
        }
    }

    fn with<R>(&self, change: impl FnOnce(&mut Counters) -> R) -> Option<R> {
        self.counters
            .lock()
            .ok()
            .map(|mut counters| change(&mut counters))
    }

    pub(crate) fn record_pass(&self, seconds: f64, failed: bool) {
        self.with(|counters| {
            counters.passes += 1;
            if failed {
                counters.pass_errors += 1;
            }
            counters
                .pass_duration
                .get_or_insert_with(|| Histogram::new(&PASS_BUCKETS))
                .observe(seconds);
        });
    }

    pub(crate) fn record_placement(&self, member: &str, outcome: &'static str) {
        self.with(|counters| {
            *counters
                .placements
                .entry((member.to_owned(), outcome))
                .or_default() += 1;
        });
    }

    /// Count one refused placement: busy members, no matching member, or each distinct
    /// reason why matching members cannot run the job.
    pub(crate) fn record_rejection(&self, rejected: Option<&[Rejection]>) {
        let reasons = match rejected {
            None => BTreeSet::from(["no_capacity"]),
            Some([]) => BTreeSet::from(["no_matching_member"]),
            Some(rejected) => rejected.iter().map(rejection_reason).collect(),
        };
        self.with(|counters| {
            for reason in reasons {
                *counters.rejections.entry(reason).or_default() += 1;
            }
        });
    }

    pub(crate) fn record_loads(&self, loads: &BTreeMap<String, Load>) {
        self.with(|counters| {
            for (member, load) in loads {
                counters.loads.insert(member.clone(), load.clone());
            }
        });
    }

    /// Count one API request: `endpoint` is its route template and method.
    pub(crate) fn record_request(&self, endpoint: (&str, &str), status: u16, seconds: f64) {
        let (route, method) = endpoint;
        self.with(|counters| {
            *counters
                .requests
                .entry((route.to_owned(), method.to_owned(), status))
                .or_default() += 1;
            counters
                .latency
                .entry((route.to_owned(), method.to_owned()))
                .or_insert_with(|| Histogram::new(&API_BUCKETS))
                .observe(seconds);
        });
    }

    /// The exposition document for `state` and the counters so far.
    #[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
    pub(crate) fn render(&self, state: &HubState, now_ms: u64) -> String {
        let mut out = Exposition::default();
        out.family(
            "zeroshot_hub_build_info",
            "gauge",
            "Hub daemon build; always 1.",
        );
        out.sample(
            "zeroshot_hub_build_info",
            &[("version", env!("CARGO_PKG_VERSION"))],
            1.0,
        );
        out.family(
            "zeroshot_hub_start_time_seconds",
            "gauge",
            "Unix time the hub daemon started.",
        );
        out.sample(
            "zeroshot_hub_start_time_seconds",
            &[],
            self.started_at_ms as f64 / 1000.0,
        );
        render_jobs(&mut out, state, now_ms);
        render_members(&mut out, state, self);
        render_job_histograms(&mut out, state);
        render_tokens(&mut out, state, &self.label_keys);
        let Ok(counters) = self.counters.lock() else {
            return out.finish();
        };
        out.family(
            "zeroshot_hub_passes_total",
            "counter",
            "Dispatcher passes since the daemon started.",
        );
        out.sample("zeroshot_hub_passes_total", &[], counters.passes as f64);
        out.family(
            "zeroshot_hub_pass_errors_total",
            "counter",
            "Dispatcher passes that failed.",
        );
        out.sample(
            "zeroshot_hub_pass_errors_total",
            &[],
            counters.pass_errors as f64,
        );
        out.family(
            "zeroshot_hub_pass_duration_seconds",
            "histogram",
            "Duration of dispatcher passes, including target calls.",
        );
        let empty = Histogram::new(&PASS_BUCKETS);
        out.histogram(
            "zeroshot_hub_pass_duration_seconds",
            &[],
            counters.pass_duration.as_ref().unwrap_or(&empty),
        );
        out.family(
            "zeroshot_hub_placements_total",
            "counter",
            "Placement attempts by member and outcome (placed, unconfirmed, dispatch_failed).",
        );
        for ((member, outcome), count) in &counters.placements {
            out.sample(
                "zeroshot_hub_placements_total",
                &[("member", member), ("outcome", outcome)],
                *count as f64,
            );
        }
        out.family(
            "zeroshot_hub_placement_rejections_total",
            "counter",
            "Refused submissions by reason (no_capacity, no_matching_member, missing_capability, \
             local_unsupported, remote_unsupported).",
        );
        for (reason, count) in &counters.rejections {
            out.sample(
                "zeroshot_hub_placement_rejections_total",
                &[("reason", reason)],
                *count as f64,
            );
        }
        out.family(
            "zeroshot_hub_api_requests_total",
            "counter",
            "Hub API requests by route template, method, and status code.",
        );
        for ((route, method, status), count) in &counters.requests {
            out.sample(
                "zeroshot_hub_api_requests_total",
                &[
                    ("route", route),
                    ("method", method),
                    ("code", &status.to_string()),
                ],
                *count as f64,
            );
        }
        out.family(
            "zeroshot_hub_api_request_duration_seconds",
            "histogram",
            "Hub API latency to response headers by route template and method.",
        );
        for ((route, method), histogram) in &counters.latency {
            out.histogram(
                "zeroshot_hub_api_request_duration_seconds",
                &[("route", route), ("method", method)],
                histogram,
            );
        }
        out.finish()
    }

    fn load(&self, member: &str) -> Option<Load> {
        self.with(|counters| counters.loads.get(member).cloned())
            .flatten()
    }
}

const JOB_STATES: [&str; 7] = [
    "queued",
    "scheduled",
    "running",
    "succeeded",
    "failed",
    "dequeued",
    "expired",
];

#[allow(clippy::cast_precision_loss)]
fn render_jobs(out: &mut Exposition, state: &HubState, now_ms: u64) {
    let mut by_state = JOB_STATES
        .iter()
        .map(|name| (*name, 0_u64))
        .collect::<BTreeMap<_, _>>();
    for job in &state.jobs {
        *by_state.entry(job.display_state(now_ms)).or_default() += 1;
    }
    out.family("zeroshot_hub_jobs", "gauge", "Hub jobs by state.");
    for (name, count) in &by_state {
        out.sample("zeroshot_hub_jobs", &[("state", name)], *count as f64);
    }
    out.family(
        "zeroshot_hub_queue_depth",
        "gauge",
        "Queued jobs waiting for placement: due now, or scheduled for later.",
    );
    out.sample(
        "zeroshot_hub_queue_depth",
        &[("kind", "due")],
        by_state["queued"] as f64,
    );
    out.sample(
        "zeroshot_hub_queue_depth",
        &[("kind", "scheduled")],
        by_state["scheduled"] as f64,
    );
    let active = state
        .schedules
        .iter()
        .filter(|schedule| schedule.state == ScheduleState::Active)
        .count();
    out.family(
        "zeroshot_hub_schedules",
        "gauge",
        "Active recurring schedules.",
    );
    out.sample("zeroshot_hub_schedules", &[], active as f64);
    let fired = state
        .schedules
        .iter()
        .map(|schedule| schedule.fired)
        .sum::<u64>();
    let skipped = state
        .schedules
        .iter()
        .map(|schedule| schedule.skipped)
        .sum::<u64>();
    out.family(
        "zeroshot_hub_cron_fires_total",
        "counter",
        "Cron fires that queued a job.",
    );
    out.sample("zeroshot_hub_cron_fires_total", &[], fired as f64);
    out.family(
        "zeroshot_hub_cron_skips_total",
        "counter",
        "Cron fires skipped because the previous job was still queued.",
    );
    out.sample("zeroshot_hub_cron_skips_total", &[], skipped as f64);
    out.family(
        "zeroshot_hub_eval_runs",
        "gauge",
        "Eval runs, by whether every sample is settled.",
    );
    let complete = state.evals.runs.iter().filter(|run| run.complete()).count();
    out.sample(
        "zeroshot_hub_eval_runs",
        &[("complete", "true")],
        complete as f64,
    );
    out.sample(
        "zeroshot_hub_eval_runs",
        &[("complete", "false")],
        (state.evals.runs.len() - complete) as f64,
    );
    out.family(
        "zeroshot_hub_eval_samples",
        "gauge",
        "Eval samples by state: pending, measured, judged, failed.",
    );
    for (name, wanted) in [
        ("pending", SampleState::Pending),
        ("measured", SampleState::Measured),
        ("judged", SampleState::Judged),
        ("failed", SampleState::Failed),
    ] {
        let count = state
            .evals
            .runs
            .iter()
            .flat_map(|run| &run.samples)
            .filter(|sample| sample.state == wanted)
            .count();
        out.sample(
            "zeroshot_hub_eval_samples",
            &[("state", name)],
            count as f64,
        );
    }
}

#[allow(clippy::cast_precision_loss)]
fn render_members(out: &mut Exposition, state: &HubState, metrics: &Metrics) {
    out.family(
        "zeroshot_hub_member_slots",
        "gauge",
        "Concurrent runs the hub places on each member.",
    );
    for member in &state.members {
        out.sample(
            "zeroshot_hub_member_slots",
            &[("member", &member.name)],
            f64::from(member.slots),
        );
    }
    out.family(
        "zeroshot_hub_member_slots_used",
        "gauge",
        "Busy slots per member: the larger of hub-placed running jobs and the last probe.",
    );
    let used = |name: &str| {
        let own = state
            .jobs
            .iter()
            .filter(|job| job.state == JobState::Running && job.target.as_deref() == Some(name))
            .count();
        let probed = metrics.load(name).map_or(0, |load| load.active);
        u32::try_from(own).unwrap_or(u32::MAX).max(probed)
    };
    for member in &state.members {
        out.sample(
            "zeroshot_hub_member_slots_used",
            &[("member", &member.name)],
            f64::from(used(&member.name)),
        );
    }
    out.family(
        "zeroshot_hub_member_slots_free",
        "gauge",
        "Free slots per member.",
    );
    for member in &state.members {
        out.sample(
            "zeroshot_hub_member_slots_free",
            &[("member", &member.name)],
            f64::from(member.slots.saturating_sub(used(&member.name))),
        );
    }
    out.family(
        "zeroshot_hub_member_up",
        "gauge",
        "1 when the member's last load probe succeeded, 0 when it failed; absent until probed.",
    );
    for member in &state.members {
        if let Some(load) = metrics.load(&member.name) {
            out.sample(
                "zeroshot_hub_member_up",
                &[("member", &member.name)],
                if load.reachable { 1.0 } else { 0.0 },
            );
        }
    }
    out.family(
        "zeroshot_hub_member_state",
        "gauge",
        "1 for each member's placement state (active, drain, disabled).",
    );
    for member in &state.members {
        let name = match member.state {
            MemberState::Active => "active",
            MemberState::Drain => "drain",
            MemberState::Disabled => "disabled",
        };
        out.sample(
            "zeroshot_hub_member_state",
            &[("member", &member.name), ("state", name)],
            1.0,
        );
    }
    out.family(
        "zeroshot_hub_member_capabilities_known",
        "gauge",
        "1 when the member's discovery capabilities are known.",
    );
    for member in &state.members {
        out.sample(
            "zeroshot_hub_member_capabilities_known",
            &[("member", &member.name)],
            if member.capabilities.is_some() || member.name == super::model::LOCAL_MEMBER {
                1.0
            } else {
                0.0
            },
        );
    }
}

/// Target label of a job: a current member name, `_removed` for former members, `none`.
fn target_label(state: &HubState, job: &Job) -> String {
    match &job.target {
        Some(target) if state.member(target).is_some() => target.clone(),
        Some(_) => "_removed".to_owned(),
        None => "none".to_owned(),
    }
}

#[allow(clippy::cast_precision_loss)]
fn render_job_histograms(out: &mut Exposition, state: &HubState) {
    let mut waits: BTreeMap<String, Histogram> = BTreeMap::new();
    let mut durations: BTreeMap<(String, &'static str), Histogram> = BTreeMap::new();
    for job in &state.jobs {
        let Some(dispatched) = job.dispatched_at_ms else {
            continue;
        };
        let target = target_label(state, job);
        let ready = job.not_before_ms.unwrap_or(0).max(job.submitted_at_ms);
        waits
            .entry(target.clone())
            .or_insert_with(|| Histogram::new(&JOB_BUCKETS))
            .observe(dispatched.saturating_sub(ready) as f64 / 1000.0);
        if let Some(finished) = job.finished_at_ms.filter(|_| job.state.is_terminal()) {
            let outcome = if job.state == JobState::Succeeded {
                "succeeded"
            } else {
                "failed"
            };
            durations
                .entry((target, outcome))
                .or_insert_with(|| Histogram::new(&JOB_BUCKETS))
                .observe(finished.saturating_sub(dispatched) as f64 / 1000.0);
        }
    }
    out.family(
        "zeroshot_hub_job_queue_wait_seconds",
        "histogram",
        "Time from a job being due to its placement, by target.",
    );
    for (target, histogram) in &waits {
        out.histogram(
            "zeroshot_hub_job_queue_wait_seconds",
            &[("target", target)],
            histogram,
        );
    }
    out.family(
        "zeroshot_hub_job_run_duration_seconds",
        "histogram",
        "Time from placement to the run finishing, by target and outcome.",
    );
    for ((target, outcome), histogram) in &durations {
        out.histogram(
            "zeroshot_hub_job_run_duration_seconds",
            &[("target", target), ("outcome", outcome)],
            histogram,
        );
    }
}

/// Complete usage only: incomplete or invalid usage never enters the token counters.
fn token_counts(job: &Job) -> Option<[u64; 4]> {
    let usage = job.token_usage.as_ref()?.complete()?;
    Some(Tokens::of(usage).kinds())
}

/// Counter totals saturate rather than wrap: a counter must never go backwards.
fn add_counts(total: &mut [u64; 4], counts: [u64; 4]) {
    for (total, count) in total.iter_mut().zip(counts) {
        *total = total.saturating_add(count);
    }
}

#[allow(clippy::cast_precision_loss)]
fn render_tokens(out: &mut Exposition, state: &HubState, label_keys: &[String]) {
    let (mut models, mut projects) = (Bounded::default(), Bounded::default());
    let mut totals: BTreeMap<(String, String, String), [u64; 4]> = BTreeMap::new();
    let mut values: BTreeMap<&str, Bounded> = BTreeMap::new();
    let mut by_label: BTreeMap<(String, String), [u64; 4]> = BTreeMap::new();
    for job in &state.jobs {
        let Some(counts) = token_counts(job) else {
            continue;
        };
        let key = (
            target_label(state, job),
            models.admit(&job.model_key()),
            projects.admit(job.project.as_deref().unwrap_or("unknown")),
        );
        let entry = totals.entry(key).or_default();
        add_counts(entry, counts);
        for label in label_keys {
            let Some(value) = job.labels.get(label) else {
                continue;
            };
            let value = values.entry(label).or_default().admit(value);
            let entry = by_label.entry((label.clone(), value)).or_default();
            add_counts(entry, counts);
        }
    }
    out.family(
        "zeroshot_hub_tokens_total",
        "counter",
        "Provider-reported tokens of finished jobs by target, model, project, and kind.",
    );
    for ((target, model, project), counts) in &totals {
        for (kind, count) in TOKEN_KINDS.iter().zip(counts) {
            out.sample(
                "zeroshot_hub_tokens_total",
                &[
                    ("target", target),
                    ("model", model),
                    ("project", project),
                    ("kind", kind),
                ],
                *count as f64,
            );
        }
    }
    out.family(
        "zeroshot_hub_label_tokens_total",
        "counter",
        "Provider-reported tokens by an opted-in job label (daemon --metrics-label KEY).",
    );
    for ((label, value), counts) in &by_label {
        for (kind, count) in TOKEN_KINDS.iter().zip(counts) {
            out.sample(
                "zeroshot_hub_label_tokens_total",
                &[("label", label), ("value", value), ("kind", kind)],
                *count as f64,
            );
        }
    }
}
