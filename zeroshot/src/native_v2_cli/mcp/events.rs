use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde_json::{Map, Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::io::AsyncWrite;
use tokio::sync::{Mutex, broadcast};

use super::args::{
    bool_prop, object_schema, optional_string, reject_unknown, string_array_prop, string_prop,
};
use super::host::{parse_cli, McpFailure, McpHost, WatchStream};
use super::super::hub::{HubCommand, HubPoolCommand, HubRequest};
use super::super::{NativeV2CliCommand, RunWatchCommand};

pub(super) const EXTENSION_ID: &str = "io.modelcontextprotocol/events";
const EVENT_NAME: &str = "run.event";
const HUB_JOB: &str = "hub.job";
const HUB_RUN: &str = "hub.run";
const HUB_POOL: &str = "hub.pool";
const HUB_WARNING: &str = "hub.warning";
const HUB_EVENTS: &[&str] = &[HUB_JOB, HUB_RUN, HUB_POOL, HUB_WARNING];
const SUBSCRIPTION_META: &str = "io.modelcontextprotocol/subscriptionId";
const FIRST_WAIT: Duration = Duration::from_secs(2);
const IDLE_WAIT: Duration = Duration::from_millis(50);
const DEFAULT_MAX_EVENTS: usize = 50;
const MAX_EVENTS_LIMIT: u64 = 1000;
const NEXT_POLL_MS: u64 = 1000;
const DRAIN_LIMIT: usize = 10_000;
#[cfg(test)]
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(200);
#[cfg(not(test))]
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const HUB_POLL_INTERVAL: Duration = Duration::from_millis(250);
const HUB_WARNING_INTERVAL: Duration = Duration::from_secs(60 * 60);
const HUB_LOG_LIMIT: usize = 4096;
const HUB_BROADCAST_LIMIT: usize = 1024;
pub(crate) const PHASES: &[&str] = &["admitted", "running", "stopping", "finished"];
/// Phases a pool lease or pool record can carry; `phases` also filters these.
pub(crate) const POOL_PHASES: &[&str] = &[
    "requested",
    "creating",
    "booting",
    "preparing",
    "ready",
    "in_use",
    "idle",
    "draining",
    "deleted",
    "failed",
    "added",
    "removed",
    "paused",
    "resumed",
    "blocked",
    "shortage",
];
/// The states a `hub.job` occurrence can carry; `states` filters match this vocabulary.
pub(crate) const JOB_STATES: &[&str] = &[
    "submitted",
    "queued",
    "scheduled",
    "placing",
    "running",
    "succeeded",
    "failed",
    "dequeued",
    "cancelled",
];
/// The hub's built-in local member is the default local backend, not a named target.
const LOCAL_HUB_MEMBER: &str = "local";

type HubSnapshot = (
    broadcast::Receiver<Arc<HubEvent>>,
    Vec<Arc<HubEvent>>,
    Value,
    Option<String>,
);

pub(super) struct Prepared {
    subscription: HubSubscription,
    filters: Filters,
}

struct HubSubscription {
    feed: Rc<HubFeed>,
    receiver: broadcast::Receiver<Arc<HubEvent>>,
    replay: Vec<Arc<HubEvent>>,
    cursor: Option<String>,
    truncated: Value,
    selector: Selector,
    target: Option<String>,
    kinds: Option<Vec<String>>,
}

pub(super) enum EventError {
    Invalid(McpFailure),
    NotFound,
    Failed(McpFailure),
}

struct PollRequest {
    selector: Selector,
    target: Option<String>,
    cursor: Option<String>,
    max_events: usize,
    filters: Filters,
    hub: bool,
}

#[derive(Clone)]
enum Selector {
    Hub,
    Run(String),
    Job(String),
}

impl Selector {
    fn is_hub(&self) -> bool {
        matches!(self, Self::Hub)
    }
}

struct JobSnapshot {
    view: Value,
    run: Option<Value>,
    run_id: Option<String>,
    target: Option<String>,
}

#[derive(Clone, Default)]
struct Filters {
    phases: Option<Vec<String>>,
    states: Option<Vec<String>>,
    nodes: Option<Vec<String>>,
    kinds: Option<Vec<String>>,
    labels: Option<Vec<(String, String)>>,
    terminal_only: bool,
    verbose: bool,
}

impl Filters {
    fn accepts(&self, data: &Value, terminal: bool) -> bool {
        if self.terminal_only && !terminal {
            return false;
        }
        if let Some(phases) = &self.phases {
            let phase = data
                .pointer("/status/phase")
                .or_else(|| data.get("state"))
                .or_else(|| data.get("phase"))
                .and_then(Value::as_str);
            if !phase.is_some_and(|phase| phases.iter().any(|allowed| allowed == phase)) {
                return false;
            }
        }
        if let Some(states) = &self.states {
            let state = data.get("state").and_then(Value::as_str);
            if !state.is_some_and(|state| states.iter().any(|allowed| allowed == state)) {
                return false;
            }
        }
        if let Some(labels) = &self.labels {
            let present = data.get("labels").and_then(Value::as_object);
            if !labels.iter().all(|(key, value)| {
                present
                    .and_then(|labels| labels.get(key))
                    .and_then(Value::as_str)
                    == Some(value.as_str())
            }) {
                return false;
            }
        }
        if let Some(nodes) = &self.nodes {
            // A terminal record carries no active executions, so a node filter would
            // otherwise swallow the one event that tells the caller the run ended.
            if data.get("status").is_none() {
                return true;
            }
            let matched = data
                .pointer("/status/activeExecutions")
                .and_then(Value::as_array)
                .is_some_and(|executions| {
                    executions.iter().any(|execution| {
                        execution
                            .get("node")
                            .and_then(Value::as_str)
                            .is_some_and(|node| nodes.iter().any(|allowed| allowed == node))
                    })
                });
            if !matched && !terminal {
                return false;
            }
        }
        true
    }
}

struct Batch {
    events: Vec<Value>,
    cursor: Option<String>,
    truncated: Value,
    has_more: bool,
}

/// One event published on the hub-wide stream. The sequence is the hub cursor; it is
/// deliberately independent from the sparse per-run `v2:` watch cursors.
pub(super) struct HubEvent {
    sequence: u64,
    name: &'static str,
    timestamp: String,
    data: Value,
    run_id: Option<String>,
    job_id: Option<String>,
    target: Option<String>,
    phase: Option<String>,
    terminal: bool,
    /// A bootstrap state synthesized when a subscription starts without a cursor. It is sent
    /// before live work but is never appended to the durable log or counted as a new cursor.
    snapshot: bool,
}

struct HubLog {
    events: VecDeque<Arc<HubEvent>>,
    next_sequence: u64,
}

/// Resident hub event log shared by every subscription on one MCP worker. A slow
/// subscriber reads the bounded log after a broadcast lag instead of blocking dispatch.
pub(super) struct HubFeed {
    log: RefCell<HubLog>,
    snapshot: RefCell<Vec<Arc<HubEvent>>>,
    sender: broadcast::Sender<Arc<HubEvent>>,
    started: Cell<bool>,
    warnings: RefCell<WarningBook>,
    /// Latest durable run record per job, used to carry delivery facts into the one terminal
    /// `hub.job` occurrence.
    run_facts: RefCell<HashMap<String, Value>>,
    /// Labels of the latest job view, so every job-derived record can be label filtered.
    job_labels: RefCell<HashMap<String, Value>>,
    /// Jobs whose terminal occurrence this feed already published. A late subscriber gets that
    /// final state in its bootstrap snapshot instead of waiting for an event it missed.
    terminal_jobs: RefCell<HashSet<String>>,
    /// The immediately previous `hub.run` occurrence per job. Comparing against only that
    /// occurrence keeps a shrinking active set (A,B -> A) a real transition instead of a
    /// historical duplicate, and carries the job attempt into the identity.
    run_seen: RefCell<HashMap<String, String>>,
}

/// Facts the warning publisher keeps between intervals for each running job: the execution it
/// last observed and when that execution first appeared, plus the cursor the previous warning
/// announced. It lives beside the feed rather than on `Job`, so no hub state schema moves.
#[derive(Default)]
struct WarningBook {
    jobs: HashMap<String, WarningEntry>,
}

#[derive(Default)]
struct WarningEntry {
    node: Option<String>,
    execution: Option<String>,
    execution_since_ms: Option<u64>,
    announced_cursor: Option<String>,
}

impl WarningBook {
    /// Learn the current execution from a run event. An empty `activeExecutions` means the run
    /// is between executions, so the next one starts its own clock.
    fn observe(&mut self, job_id: &str, event: &Value, now: u64) {
        if event.pointer("/status/phase").and_then(Value::as_str) == Some("finished") {
            self.jobs.remove(job_id);
            return;
        }
        let Some(executions) = event
            .pointer("/status/activeExecutions")
            .and_then(Value::as_array)
        else {
            return;
        };
        let current = executions.first();
        let execution = current
            .and_then(|execution| execution.get("execution"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let node = current
            .and_then(|execution| execution.get("node"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let entry = self.jobs.entry(job_id.to_owned()).or_default();
        if entry.execution != execution {
            entry.execution_since_ms = execution.as_ref().map(|_| now);
            entry.execution = execution;
            entry.node = node;
        } else if node.is_some() {
            entry.node = node;
        }
    }

    fn retain_running(&mut self, running: &HashSet<String>) {
        self.jobs.retain(|job_id, _| running.contains(job_id));
    }
}

impl HubFeed {
    pub(super) fn new() -> Self {
        let (sender, _) = broadcast::channel(HUB_BROADCAST_LIMIT);
        Self {
            log: RefCell::new(HubLog {
                events: VecDeque::new(),
                next_sequence: 1,
            }),
            snapshot: RefCell::new(Vec::new()),
            sender,
            started: Cell::new(false),
            warnings: RefCell::new(WarningBook::default()),
            run_facts: RefCell::new(HashMap::new()),
            job_labels: RefCell::new(HashMap::new()),
            terminal_jobs: RefCell::new(HashSet::new()),
            run_seen: RefCell::new(HashMap::new()),
        }
    }

    pub(super) fn ensure_started<H>(self: &Rc<Self>, host: Rc<H>)
    where
        H: McpHost + 'static,
    {
        if self.started.replace(true) {
            return;
        }
        let feed = Rc::clone(self);
        tokio::task::spawn_local(async move {
            hub_poller(host, feed).await;
        });
    }

    fn publish(&self, mut event: HubEvent) {
        // Run and warning records describe a job, so `labels` filtering has the same
        // meaning on every job-derived kind instead of only on `hub.job`.
        if event.name != HUB_JOB
            && let Some(job_id) = &event.job_id
            && let Some(labels) = self.job_labels.borrow().get(job_id).cloned()
            && let Some(map) = event.data.as_object_mut()
        {
            map.entry("labels").or_insert(labels);
        }
        let mut log = self.log.borrow_mut();
        event.sequence = log.next_sequence;
        log.next_sequence = log.next_sequence.saturating_add(1);
        let event = Arc::new(event);
        log.events.push_back(Arc::clone(&event));
        while log.events.len() > HUB_LOG_LIMIT {
            log.events.pop_front();
        }
        drop(log);
        self.remember(Arc::clone(&event));
        let _ = self.sender.send(event);
    }

    /// Keep the latest state per job, run, and pool lease for a new subscription.
    fn remember(&self, event: Arc<HubEvent>) {
        let Some((kind, key)) = snapshot_key(&event) else {
            return;
        };
        let mut snapshot = self.snapshot.borrow_mut();
        snapshot.retain(|current| {
            snapshot_key(current).is_none_or(|(current_kind, current_key)| {
                current_kind != kind || current_key != key
            })
        });
        if !event.terminal {
            snapshot.push(event);
        }
    }

    fn head(&self) -> u64 {
        self.log.borrow().next_sequence.saturating_sub(1)
    }

    /// Subscribe before taking the replay snapshot. There is no await between the two,
    /// so a single-threaded worker cannot lose or duplicate an event at the boundary.
    fn snapshot(&self, after: Option<u64>) -> HubSnapshot {
        let log = self.log.borrow();
        let head = log.next_sequence.saturating_sub(1);
        let start = after.unwrap_or(head);
        let truncated = log.truncation_after(after);
        let replay = if after.is_none() {
            let mut replay = self.snapshot.borrow().clone();
            replay.sort_by_key(|event| event.sequence);
            replay
        } else {
            log.events
                .iter()
                .filter(|event| event.sequence > start)
                .cloned()
                .collect()
        };
        (
            self.sender.subscribe(),
            replay,
            truncated,
            Some(hub_cursor(start)),
        )
    }

    fn after(&self, sequence: u64) -> (Vec<Arc<HubEvent>>, Value) {
        let log = self.log.borrow();
        let truncated = log.truncation_after(Some(sequence));
        let replay = log
            .events
            .iter()
            .filter(|event| event.sequence > sequence)
            .cloned()
            .collect();
        (replay, truncated)
    }
}

fn snapshot_key(event: &HubEvent) -> Option<(&'static str, String)> {
    match event.name {
        HUB_JOB => event.job_id.clone().map(|key| ("job", key)),
        HUB_RUN => event.run_id.clone().map(|key| ("run", key)),
        HUB_POOL => event
            .data
            .get("leaseId")
            .and_then(Value::as_str)
            .map(|key| ("lease", key.to_owned()))
            .or_else(|| {
                event
                    .data
                    .get("pool")
                    .and_then(Value::as_str)
                    .map(|key| ("pool", key.to_owned()))
            }),
        _ => None,
    }
}

impl HubLog {
    fn truncation_after(&self, after: Option<u64>) -> Value {
        let Some(after) = after else {
            return json!(false);
        };
        let head = self.next_sequence.saturating_sub(1);
        let first = self
            .events
            .front()
            .map(|event| event.sequence)
            .unwrap_or(head.saturating_add(1));
        if after.saturating_add(1) < first {
            truncated_range(after.saturating_add(1), first.saturating_sub(1))
        } else {
            json!(false)
        }
    }
}

fn hub_cursor(sequence: u64) -> String {
    format!("hub:{sequence}")
}

fn cursor_sequence(cursor: &str) -> Option<u64> {
    cursor.rsplit_once(':')?.1.parse().ok()
}

fn truncated_range(from: u64, to: u64) -> Value {
    if from > to {
        return json!(false);
    }
    json!({"from": hub_cursor(from), "to": hub_cursor(to)})
}

/// A resume cursor from before a daemon restart can sit beyond the new feed head. Clamp it to
/// the head so live work is still delivered, and report the vanished range as truncation.
fn clamp_resume(feed: &HubFeed, after: Option<u64>) -> (Option<u64>, Value) {
    let head = feed.head();
    match after {
        Some(after) if after > head => (Some(head), truncated_range(head.saturating_add(1), after)),
        _ => (after, json!(false)),
    }
}

fn merge_truncated(current: Value, lost: Value) -> Value {
    match truncation_bounds(&lost) {
        (Some(from), Some(to)) => merge_truncation(current, from, to),
        _ => current,
    }
}

fn merge_truncation(current: Value, from: u64, to: u64) -> Value {
    if from > to {
        return current;
    }
    match current {
        Value::Object(mut map) => {
            let current_to = map
                .get("to")
                .and_then(Value::as_str)
                .and_then(|cursor| cursor.strip_prefix("hub:"))
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(to);
            map.insert("to".to_owned(), json!(hub_cursor(current_to.max(to))));
            Value::Object(map)
        }
        _ => truncated_range(from, to),
    }
}

pub(super) fn list(params: &Value) -> Result<Value, EventError> {
    let paged = match params {
        Value::Null => false,
        Value::Object(map) => {
            reject_unknown(map, &["cursor"]).map_err(EventError::Invalid)?;
            optional_string(map, "cursor")
                .map_err(EventError::Invalid)?
                .is_some()
        }
        _ => {
            return Err(EventError::Invalid(McpFailure::invalid(
                "params must be an object",
            )));
        }
    };
    if paged {
        return Ok(json!({"resultType": "complete", "events": []}));
    }
    let descriptions = [(
        "hub",
        "everything in the hub: jobs, runs, pool leases, and warnings. Each notification keeps \
         its own name, one of hub.job, hub.run, hub.pool, or hub.warning, so a client can branch \
         on the kind or ignore it and read the stream.",
    )];
    let events = descriptions
        .into_iter()
        .map(|(name, description)| {
            json!({
                "name": name,
                "description": description,
                "names": HUB_EVENTS,
                "delivery": ["poll", "push"],
                "inputSchema": input_schema(name),
                "payloadSchema": payload_schema(name),
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "resultType": "complete",
        "events": events,
    }))
}

pub(super) fn unsubscribe(params: &Value) -> Result<Value, EventError> {
    let map = match params {
        Value::Null => {
            return Err(EventError::Invalid(McpFailure::invalid(
                "requestId is required",
            )));
        }
        Value::Object(map) => map,
        _ => {
            return Err(EventError::Invalid(McpFailure::invalid(
                "params must be an object",
            )));
        }
    };
    reject_unknown(map, &["requestId"]).map_err(EventError::Invalid)?;
    let request_id = map
        .get("requestId")
        .filter(|value| !value.is_null())
        .ok_or_else(|| EventError::Invalid(McpFailure::invalid("requestId is required")))?;
    if !(request_id.is_number() || request_id.is_string()) {
        return Err(EventError::Invalid(McpFailure::invalid(
            "requestId must be a string or number",
        )));
    }
    Ok(request_id.clone())
}

pub(super) async fn poll<H>(
    host: &Rc<H>,
    feed: &Rc<HubFeed>,
    params: &Value,
    sequence: &Cell<u64>,
) -> Result<Value, EventError>
where
    H: McpHost + 'static,
{
    let request = parse_subscription(params, true)?;
    if request.hub {
        return hub_poll(host, feed, &request, sequence).await;
    }
    let (batch, job) = match request.selector.clone() {
        Selector::Hub => {
            return Err(EventError::Invalid(McpFailure::invalid(
                "a hub poll requires the hub path",
            )));
        }
        Selector::Run(run_id) => {
            let batch =
                collect(host.as_ref(), &request, &run_id, request.target.as_deref()).await?;
            (batch, None)
        }
        Selector::Job(job_id) => {
            let snapshot = job_status(host.as_ref(), &job_id).await?;
            if let Some(run_id) = snapshot.run_id.clone() {
                let target = request.target.clone().or_else(|| snapshot.target.clone());
                let batch = collect(host.as_ref(), &request, &run_id, target.as_deref()).await?;
                (batch, Some(snapshot.view))
            } else {
                (
                    Batch {
                        events: Vec::new(),
                        cursor: None,
                        truncated: json!(false),
                        has_more: false,
                    },
                    Some(snapshot.view),
                )
            }
        }
    };
    let mut result = json!({
        "resultType": "complete",
        "events": batch.events,
        "cursor": cursor_value(&batch.cursor),
        "truncated": batch.truncated,
        "hasMore": batch.has_more,
        "nextPollMs": NEXT_POLL_MS,
    });
    if let Some(job) = job {
        result["job"] = job;
    }
    Ok(result)
}

pub(super) async fn prepare<H>(
    host: &Rc<H>,
    feed: &Rc<HubFeed>,
    params: &Value,
) -> Result<Prepared, EventError>
where
    H: McpHost + 'static,
{
    let request = parse_subscription(params, false)?;
    prepare_hub(host, feed, &request).await
}

async fn prepare_hub<H>(
    host: &Rc<H>,
    feed: &Rc<HubFeed>,
    request: &PollRequest,
) -> Result<Prepared, EventError>
where
    H: McpHost + 'static,
{
    feed.ensure_started(Rc::clone(host));
    let after = match request.cursor.as_deref() {
        Some(cursor) => Some(parse_hub_cursor(cursor)?),
        None => None,
    };
    let (after, stale) = clamp_resume(feed, after);
    let start = after.unwrap_or_else(|| feed.head());
    let (receiver, mut replay, truncated, mut cursor) = feed.snapshot(after);
    let mut truncated = merge_truncated(truncated, stale);
    if after.is_none()
        && let Some(snapshots) = snapshot_events(host.as_ref(), feed, request).await
    {
        // Bootstrap records lead the batch. Everything published while this snapshot was
        // built still follows, so neither the bootstrap terminal's own publication nor an
        // unrelated live transition is skipped. The stream drops only repeated job states.
        let (live, lost) = feed.after(start);
        replay.retain(|event| event.sequence <= start);
        replay.extend(live);
        truncated = merge_truncated(truncated, lost);
        let mut combined = snapshots;
        combined.extend(replay);
        replay = combined;
        // A bootstrap for an already-terminal job publishes that job's one durable terminal
        // occurrence. Start the active cursor after it so a resume cannot replay it.
        cursor = Some(hub_cursor(feed.head()));
    }
    Ok(Prepared {
        subscription: HubSubscription {
            feed: Rc::clone(feed),
            receiver,
            replay,
            cursor,
            truncated,
            selector: request.selector.clone(),
            target: request.target.clone(),
            kinds: request.filters.kinds.clone(),
        },
        filters: request.filters.clone(),
    })
}

pub(super) struct StreamOutput<W> {
    id: Value,
    writer: Arc<Mutex<W>>,
    cancelled: Arc<AtomicBool>,
}

impl<W> StreamOutput<W> {
    pub(super) fn new(id: Value, writer: Arc<Mutex<W>>, cancelled: Arc<AtomicBool>) -> Self {
        Self {
            id,
            writer,
            cancelled,
        }
    }
}

pub(super) async fn drive<W>(prepared: Prepared, output: StreamOutput<W>) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    let Prepared {
        subscription,
        filters,
    } = prepared;
    drive_hub(subscription, filters, output).await
}

async fn drive_hub<W>(
    subscription: HubSubscription,
    filters: Filters,
    output: StreamOutput<W>,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    let HubSubscription {
        feed,
        mut receiver,
        replay,
        mut cursor,
        mut truncated,
        selector,
        target,
        kinds,
    } = subscription;
    let StreamOutput {
        id,
        writer,
        cancelled,
    } = output;
    if cancelled.load(Ordering::Relaxed) {
        return Ok(());
    }
    write_active(
        &writer,
        &id,
        &ActiveView {
            cursor: &cursor,
            run: None,
            truncated: truncated.clone(),
            job: None,
        },
    )
    .await?;
    let filter = HubFilter {
        selector: &selector,
        target: &target,
        kinds: &kinds,
        filters: &filters,
    };
    let mut sequence = 0u64;
    let mut job_states = HashMap::new();
    for event in replay {
        if cancelled.load(Ordering::Relaxed) {
            return Ok(());
        }
        if !repeated_job_state(&event, &job_states) {
            let next = sequence.saturating_add(1);
            if let Some(value) = hub_event_value(
                &event,
                &filter,
                Stamp {
                    replayed: true,
                    sequence: next,
                    id: Some(&id),
                },
            ) {
                write_notification(&writer, "notifications/events/event", value).await?;
                sequence = next;
                remember_job_state(&event, &mut job_states);
            }
        }
        if !event.snapshot
            && cursor
                .as_deref()
                .and_then(cursor_sequence)
                .is_none_or(|current| event.sequence > current)
        {
            cursor = Some(hub_cursor(event.sequence));
        }
    }
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    loop {
        if cancelled.load(Ordering::Relaxed) {
            return Ok(());
        }
        tokio::select! {
            biased;
            item = receiver.recv() => {
                if cancelled.load(Ordering::Relaxed) {
                    return Ok(());
                }
                match item {
                    Ok(event) => {
                        // The receiver resumes at the oldest value tokio still retains after a
                        // lagged catch-up, so skip anything the cursor already crossed.
                        let fresh = cursor
                            .as_deref()
                            .and_then(cursor_sequence)
                            .is_none_or(|current| event.sequence > current);
                        if fresh {
                            if !repeated_job_state(&event, &job_states) {
                                let next = sequence.saturating_add(1);
                                if let Some(value) =
                                    hub_event_value(
                                        &event,
                                        &filter,
                                        Stamp {
                                            replayed: false,
                                            sequence: next,
                                            id: Some(&id),
                                        },
                                    )
                                {
                                    write_notification(&writer, "notifications/events/event", value)
                                        .await?;
                                    sequence = next;
                                    remember_job_state(&event, &mut job_states);
                                }
                            }
                            // Advance even when a repeated state or a filter suppressed delivery;
                            // the cursor is durable feed position, not a delivered-event count.
                            cursor = Some(hub_cursor(event.sequence));
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let after = cursor
                            .as_deref()
                            .and_then(|cursor| cursor.strip_prefix("hub:"))
                            .and_then(|value| value.parse::<u64>().ok())
                            .unwrap_or(0);
                        let (missed, lost) = feed.after(after);
                        if let (Some(from), Some(to)) = truncation_bounds(&lost) {
                            truncated = merge_truncation(truncated, from, to);
                        }
                        write_active(
                            &writer,
                            &id,
                            &ActiveView {
                                cursor: &cursor,
                                run: None,
                                truncated: truncated.clone(),
                                job: None,
                            },
                        )
                        .await?;
                        for event in missed {
                            if !repeated_job_state(&event, &job_states) {
                                let next = sequence.saturating_add(1);
                                if let Some(value) =
                                    hub_event_value(
                                        &event,
                                        &filter,
                                        Stamp {
                                            replayed: false,
                                            sequence: next,
                                            id: Some(&id),
                                        },
                                    )
                                {
                                    write_notification(&writer, "notifications/events/event", value)
                                        .await?;
                                    sequence = next;
                                    remember_job_state(&event, &mut job_states);
                                }
                            }
                            cursor = Some(hub_cursor(event.sequence));
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
            _ = heartbeat.tick() => {
                write_heartbeat(&writer, &id, &cursor).await?;
            }
        }
    }
}

struct HubFilter<'a> {
    selector: &'a Selector,
    target: &'a Option<String>,
    kinds: &'a Option<Vec<String>>,
    filters: &'a Filters,
}

/// One delivery's position: replay state, monotone sequence, and the enclosing request ID.
struct Stamp<'a> {
    replayed: bool,
    sequence: u64,
    id: Option<&'a Value>,
}

fn hub_event_value(event: &HubEvent, filter: &HubFilter<'_>, stamp: Stamp<'_>) -> Option<Value> {
    if let Some(kinds) = filter.kinds {
        if !kinds.iter().any(|kind| kind == event.name) {
            return None;
        }
    }
    match filter.selector {
        Selector::Hub => {}
        Selector::Run(run_id) => {
            if event.run_id.as_deref() != Some(run_id.as_str()) {
                return None;
            }
        }
        Selector::Job(job_id) => {
            if event.job_id.as_deref() != Some(job_id.as_str()) {
                return None;
            }
        }
    }
    if let Some(target) = filter.target {
        if event.target.as_deref() != Some(target.as_str()) {
            return None;
        }
    }
    let mut data = if event.name == HUB_RUN {
        event_data(&event.data, filter.filters.verbose)
    } else {
        event.data.clone()
    };
    if !filter.filters.accepts(&data, event.terminal) {
        return None;
    }
    let summary = if event.name == HUB_RUN {
        summary(&data)
    } else {
        json!({
            "phase": event.phase,
            "node": Value::Null,
            "terminal": event.terminal,
            "reason": data.get("reason").cloned().unwrap_or(Value::Null),
        })
    };
    if let Some(map) = data.as_object_mut() {
        // hub.run carries a durable v2: watch cursor in its recorded data. Keep that
        // provenance while data.cursor remains the hub-wide resume cursor.
        if event.name == HUB_RUN {
            if let Some(run_cursor) = map.get("cursor").cloned() {
                map.insert("runCursor".to_owned(), run_cursor);
            }
        }
        map.insert("cursor".to_owned(), json!(hub_cursor(event.sequence)));
        map.insert("summary".to_owned(), summary);
        map.insert("replayed".to_owned(), json!(stamp.replayed));
        map.insert("sequence".to_owned(), json!(stamp.sequence));
        if event.snapshot {
            map.insert("snapshot".to_owned(), json!(true));
        }
    }
    let value = json!({
        "eventId": hub_cursor(event.sequence),
        "name": event.name,
        "timestamp": event.timestamp,
        "data": data,
        "_meta": stamp.id.map(meta).unwrap_or_else(|| json!({})),
    });
    Some(value)
}

fn truncation_bounds(value: &Value) -> (Option<u64>, Option<u64>) {
    let parse = |key: &str| {
        value
            .get(key)
            .and_then(Value::as_str)
            .and_then(|cursor| cursor.strip_prefix("hub:"))
            .and_then(|value| value.parse::<u64>().ok())
    };
    (parse("from"), parse("to"))
}

pub(super) fn rpc_error(error: EventError) -> (i64, &'static str, Value) {
    match error {
        EventError::Invalid(failure) => (-32602, "Invalid params", failure_data(failure)),
        EventError::NotFound => (-32600, "Invalid Request", json!({"kind": "event"})),
        EventError::Failed(failure) if failure.code == "run.not_found" => (
            -32023,
            "NotFound",
            json!({"kind": "run", "code": failure.code, "message": failure.message}),
        ),
        EventError::Failed(failure) => (-32603, "Internal error", failure_data(failure)),
    }
}

fn failure_data(failure: McpFailure) -> Value {
    json!({"code": failure.code, "message": failure.message})
}

fn classify(failure: McpFailure) -> EventError {
    if matches!(
        failure.code.as_str(),
        "request.invalid" | "template.unknown" | "template.unsupported_delivery"
    ) {
        EventError::Invalid(failure)
    } else {
        EventError::Failed(failure)
    }
}

fn parse_subscription(params: &Value, capped: bool) -> Result<PollRequest, EventError> {
    let empty = Map::new();
    let map = match params {
        Value::Null => &empty,
        Value::Object(map) => map,
        _ => {
            return Err(EventError::Invalid(McpFailure::invalid(
                "params must be an object",
            )));
        }
    };
    let allowed: &[&str] = if capped {
        &["name", "arguments", "cursor", "maxAgeMs", "maxEvents"]
    } else {
        &["name", "arguments", "cursor", "maxAgeMs"]
    };
    reject_unknown(map, allowed).map_err(EventError::Invalid)?;
    let name = optional_string(map, "name").map_err(EventError::Invalid)?;
    let name_kinds = match name.as_deref() {
        None => None,
        Some(EVENT_NAME) => None,
        Some(name) if HUB_EVENTS.contains(&name) => Some(vec![name.to_owned()]),
        Some("hub") | Some("hub.event") => None,
        Some(_) => return Err(EventError::NotFound),
    };
    // Watch records have no occurrence time, so maxAgeMs is accepted and not applied.
    if map.contains_key("maxAgeMs") && map.get("maxAgeMs").and_then(Value::as_u64).is_none() {
        return Err(EventError::Invalid(McpFailure::invalid(
            "maxAgeMs must be a non-negative integer",
        )));
    }
    let max_events = if capped {
        max_events(map)?
    } else {
        DEFAULT_MAX_EVENTS
    };
    let cursor = match map.get("cursor") {
        None | Some(Value::Null) => None,
        Some(Value::String(cursor)) => Some(cursor.clone()),
        Some(_) => {
            return Err(EventError::Invalid(McpFailure::invalid(
                "cursor must be a string or null",
            )));
        }
    };
    let arguments = match map.get("arguments") {
        None | Some(Value::Null) => json!({}),
        Some(value @ Value::Object(_)) => value.clone(),
        Some(_) => {
            return Err(EventError::Invalid(McpFailure::invalid(
                "arguments must be an object",
            )));
        }
    };
    let arguments = arguments
        .as_object()
        .expect("arguments was validated as an object");
    let run_id = optional_string(arguments, "runId").map_err(EventError::Invalid)?;
    let job_id = optional_string(arguments, "jobId").map_err(EventError::Invalid)?;
    let selector = match (run_id, job_id) {
        (Some(run_id), None) => Selector::Run(run_id),
        (None, Some(job_id)) => Selector::Job(job_id),
        (None, None) => Selector::Hub,
        _ => {
            return Err(EventError::Invalid(McpFailure::invalid(
                "use at most one of runId or jobId; omit both to subscribe to the whole hub",
            )));
        }
    };
    let hub = !capped
        || name.is_none()
        || selector.is_hub()
        || name_kinds.is_some()
        || matches!(name.as_deref(), Some("hub") | Some("hub.event"));
    let target = optional_string(arguments, "target").map_err(EventError::Invalid)?;
    let mut filters = parse_filters(arguments)?;
    if filters.kinds.is_none() {
        filters.kinds = name_kinds.or_else(|| {
            (hub && name.as_deref() == Some(EVENT_NAME)).then(|| vec![HUB_RUN.to_owned()])
        });
    }
    Ok(PollRequest {
        selector,
        target,
        cursor,
        max_events,
        filters,
        hub,
    })
}

fn parse_filters(arguments: &Map<String, Value>) -> Result<Filters, EventError> {
    reject_unknown(
        arguments,
        &[
            "runId",
            "jobId",
            "target",
            "kinds",
            "phases",
            "states",
            "nodes",
            "labels",
            "terminalOnly",
            "verbose",
        ],
    )
    .map_err(EventError::Invalid)?;
    let phases = match arguments.get("phases") {
        None | Some(Value::Null) => None,
        Some(Value::Array(values)) => {
            let mut phases = Vec::with_capacity(values.len());
            for value in values {
                let Some(phase) = value.as_str() else {
                    return Err(EventError::Invalid(McpFailure::invalid(
                        "phases must be an array of strings",
                    )));
                };
                if !PHASES.contains(&phase) && !POOL_PHASES.contains(&phase) {
                    return Err(EventError::Invalid(McpFailure::invalid(format!(
                        "unknown phase {phase}"
                    ))));
                }
                phases.push(phase.to_owned());
            }
            Some(phases)
        }
        Some(_) => {
            return Err(EventError::Invalid(McpFailure::invalid(
                "phases must be an array of strings",
            )));
        }
    };
    let states = match arguments.get("states") {
        None | Some(Value::Null) => None,
        Some(Value::Array(values)) => {
            let mut states = Vec::with_capacity(values.len());
            for value in values {
                let Some(state) = value.as_str() else {
                    return Err(EventError::Invalid(McpFailure::invalid(
                        "states must be an array of strings",
                    )));
                };
                if !JOB_STATES.contains(&state) {
                    return Err(EventError::Invalid(McpFailure::invalid(format!(
                        "unknown job state {state}"
                    ))));
                }
                states.push(state.to_owned());
            }
            Some(states)
        }
        Some(_) => {
            return Err(EventError::Invalid(McpFailure::invalid(
                "states must be an array of strings",
            )));
        }
    };
    let nodes = match arguments.get("nodes") {
        None | Some(Value::Null) => None,
        Some(Value::Array(values)) => {
            let mut nodes = Vec::with_capacity(values.len());
            for value in values {
                let Some(node) = value.as_str() else {
                    return Err(EventError::Invalid(McpFailure::invalid(
                        "nodes must be an array of strings",
                    )));
                };
                nodes.push(node.to_owned());
            }
            Some(nodes)
        }
        Some(_) => {
            return Err(EventError::Invalid(McpFailure::invalid(
                "nodes must be an array of strings",
            )));
        }
    };
    let labels = match arguments.get("labels") {
        None | Some(Value::Null) => None,
        Some(Value::Object(values)) => {
            let mut labels = Vec::with_capacity(values.len());
            for (key, value) in values {
                let Some(value) = value.as_str() else {
                    return Err(EventError::Invalid(McpFailure::invalid(
                        "labels must be an object of string values",
                    )));
                };
                labels.push((key.clone(), value.to_owned()));
            }
            labels.sort();
            Some(labels)
        }
        Some(_) => {
            return Err(EventError::Invalid(McpFailure::invalid(
                "labels must be an object of string values",
            )));
        }
    };
    let kinds = match arguments.get("kinds") {
        None | Some(Value::Null) => None,
        Some(Value::Array(values)) => {
            let mut kinds = Vec::with_capacity(values.len());
            for value in values {
                let Some(kind) = value.as_str() else {
                    return Err(EventError::Invalid(McpFailure::invalid(
                        "kinds must be an array of hub event names",
                    )));
                };
                if !HUB_EVENTS.contains(&kind) {
                    return Err(EventError::Invalid(McpFailure::invalid(format!(
                        "unknown hub event kind {kind}"
                    ))));
                }
                kinds.push(kind.to_owned());
            }
            Some(kinds)
        }
        Some(_) => {
            return Err(EventError::Invalid(McpFailure::invalid(
                "kinds must be an array of hub event names",
            )));
        }
    };
    Ok(Filters {
        phases,
        states,
        nodes,
        kinds,
        labels,
        terminal_only: optional_bool(arguments, "terminalOnly")?,
        verbose: optional_bool(arguments, "verbose")?,
    })
}

fn optional_bool(arguments: &Map<String, Value>, key: &str) -> Result<bool, EventError> {
    match arguments.get(key) {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(value)) => Ok(*value),
        Some(_) => Err(EventError::Invalid(McpFailure::invalid(format!(
            "{key} must be a boolean"
        )))),
    }
}

fn max_events(map: &Map<String, Value>) -> Result<usize, EventError> {
    let Some(value) = map.get("maxEvents") else {
        return Ok(DEFAULT_MAX_EVENTS);
    };
    let Some(count) = value
        .as_u64()
        .filter(|count| (1..=MAX_EVENTS_LIMIT).contains(count))
    else {
        return Err(EventError::Invalid(McpFailure::invalid(
            "maxEvents must be an integer from 1 to 1000",
        )));
    };
    Ok(usize::try_from(count).unwrap_or(DEFAULT_MAX_EVENTS))
}

async fn collect(
    host: &impl McpHost,
    request: &PollRequest,
    run_id: &str,
    target: Option<&str>,
) -> Result<Batch, EventError> {
    let command = watch_command(run_id, target, request.cursor.as_deref())?;
    let mut stream = host.open_watch(&command).await.map_err(classify)?;
    if let Some(cursor) = request.cursor.as_deref() {
        return collect_after(&mut stream, cursor, request).await;
    }
    let drained = drain(&mut stream).await?;
    Ok(Batch {
        events: Vec::new(),
        cursor: last_cursor(&drained)?,
        truncated: json!(false),
        has_more: false,
    })
}

async fn drain(stream: &mut WatchStream) -> Result<Vec<Value>, EventError> {
    let mut events = Vec::new();
    loop {
        if events.len() >= DRAIN_LIMIT {
            break;
        }
        let wait = if events.is_empty() {
            FIRST_WAIT
        } else {
            IDLE_WAIT
        };
        let Some(event) = next_event(stream, wait).await? else {
            break;
        };
        events.push(event);
    }
    Ok(events)
}

async fn collect_after(
    stream: &mut WatchStream,
    requested: &str,
    request: &PollRequest,
) -> Result<Batch, EventError> {
    let mut events = Vec::new();
    let mut cursor = requested.to_owned();
    let mut waiting = true;
    loop {
        if events.len() == request.max_events {
            let extra = next_event(stream, IDLE_WAIT).await?;
            // A bounded poll advances the cursor so the caller can continue from where
            // this batch stopped. The undelivered tail is hasMore, not permanent loss.
            return Ok(Batch {
                events,
                cursor: Some(cursor),
                truncated: json!(false),
                has_more: extra.is_some(),
            });
        }
        let wait = if waiting { FIRST_WAIT } else { IDLE_WAIT };
        let Some(event) = next_event(stream, wait).await? else {
            break;
        };
        waiting = false;
        let event_cursor = watch_cursor(&event).map_err(classify)?;
        if event_cursor == requested {
            continue;
        }
        cursor = event_cursor;
        if let Some(occurrence) = occurrence(&event, &request.filters, false).map_err(classify)? {
            events.push(occurrence);
        }
    }
    Ok(Batch {
        events,
        cursor: Some(cursor),
        truncated: json!(false),
        has_more: false,
    })
}

async fn next_event(stream: &mut WatchStream, wait: Duration) -> Result<Option<Value>, EventError> {
    match tokio::time::timeout(wait, stream.recv()).await {
        Ok(Some(Ok(value))) => Ok(Some(value)),
        Ok(Some(Err(failure))) => Err(classify(failure)),
        Ok(None) | Err(_) => Ok(None),
    }
}

fn last_cursor(events: &[Value]) -> Result<Option<String>, EventError> {
    events
        .last()
        .map(|event| watch_cursor(event).map_err(classify))
        .transpose()
}

async fn job_status(host: &impl McpHost, job_id: &str) -> Result<JobSnapshot, EventError> {
    let output = host
        .execute(NativeV2CliCommand::Hub(HubRequest {
            context: None,
            command: HubCommand::Status {
                job: job_id.to_owned(),
            },
        }))
        .await
        .map_err(classify)?;
    let document: Value = serde_json::from_str(output.trim()).map_err(invalid_hub_response)?;
    let run = document.get("run").cloned().filter(|run| !run.is_null());
    let view = match document.get("job") {
        Some(view @ Value::Object(_)) => view.clone(),
        None if document.get("jobId").is_some() => document,
        _ => {
            return Err(EventError::Failed(McpFailure {
                code: "hub.invalid_response".to_owned(),
                message: "hub job status did not contain a job".to_owned(),
            }));
        }
    };
    let run_id = view
        .get("runId")
        .and_then(Value::as_str)
        .filter(|run_id| !run_id.is_empty())
        .map(str::to_owned);
    Ok(JobSnapshot {
        target: view
            .get("target")
            .and_then(Value::as_str)
            .filter(|target| *target != LOCAL_HUB_MEMBER)
            .map(str::to_owned),
        view,
        run,
        run_id,
    })
}

fn invalid_hub_response(error: serde_json::Error) -> EventError {
    EventError::Failed(McpFailure {
        code: "hub.invalid_response".to_owned(),
        message: format!("hub job status was not JSON: {error}"),
    })
}

async fn hub_poll<H>(
    host: &Rc<H>,
    feed: &Rc<HubFeed>,
    request: &PollRequest,
    sequence: &Cell<u64>,
) -> Result<Value, EventError>
where
    H: McpHost + 'static,
{
    feed.ensure_started(Rc::clone(host));
    tokio::task::yield_now().await;
    let after = match request.cursor.as_deref() {
        Some(cursor) => Some(parse_hub_cursor(cursor)?),
        None => None,
    };
    let (after, stale) = clamp_resume(feed, after);
    let start = after.unwrap_or_else(|| feed.head());
    let (receiver, mut replay, truncated, _) = feed.snapshot(after);
    drop(receiver);
    let mut truncated = merge_truncated(truncated, stale);
    let filter = HubFilter {
        selector: &request.selector,
        target: &request.target,
        kinds: &request.filters.kinds,
        filters: &request.filters,
    };
    let mut job_states = HashMap::new();
    let snapshots = if after.is_none() {
        snapshot_events(host.as_ref(), feed, request).await
    } else {
        None
    };
    if after.is_none() {
        // Bootstrap may publish an already-terminal job's durable occurrence while it awaits
        // host reads. Replay everything published since the initial head so that publication
        // advances the cursor without skipping unrelated live events.
        let (live, lost) = feed.after(start);
        replay.retain(|event| event.sequence <= start);
        replay.extend(live);
        truncated = merge_truncated(truncated, lost);
    }
    let mut selected = Vec::new();
    if let Some(snapshots) = snapshots.as_ref() {
        for event in snapshots {
            if repeated_job_state(event, &job_states) {
                continue;
            }
            if let Some(value) = hub_event_value(
                event,
                &filter,
                Stamp {
                    replayed: false,
                    sequence: 0,
                    id: None,
                },
            ) {
                selected.push((event.sequence, value));
                remember_job_state(event, &mut job_states);
            }
        }
    }
    let bootstrap_len = selected.len();
    for event in &replay {
        if repeated_job_state(event, &job_states) {
            continue;
        }
        if let Some(value) = hub_event_value(
            event,
            &filter,
            Stamp {
                replayed: true,
                sequence: 0,
                id: None,
            },
        ) {
            selected.push((event.sequence, value));
            remember_job_state(event, &mut job_states);
        }
    }
    let mut has_more = false;
    // A fresh poll starts at the head after any bootstrap terminal publication, so the
    // caller never replays an occurrence it already received as a snapshot.
    let mut cursor = after.unwrap_or_else(|| feed.head());
    if selected.len() > bootstrap_len.saturating_add(request.max_events) {
        selected.truncate(bootstrap_len.saturating_add(request.max_events));
        if let Some((selected_sequence, _)) = selected.last() {
            cursor = *selected_sequence;
        }
        // A maxEvents clip is resumable from the returned cursor, so it is hasMore
        // rather than truncation. Only replay or backpressure can permanently lose events.
        has_more = true;
    } else if snapshots.is_none() {
        // Filters can hide every replay record while the cursor still has to advance past it.
        if let Some(last) = replay.last()
            && last.sequence > cursor
        {
            cursor = last.sequence;
        }
        if let Some((selected_sequence, _)) = selected.last()
            && *selected_sequence > cursor
        {
            cursor = *selected_sequence;
        }
    }

    // Assign the per-caller sequence only after clipping, so a delivered batch is contiguous
    // and starts at 1 even when filters or maxEvents skipped occurrences.
    for (_, value) in &mut selected {
        let next = sequence.get().saturating_add(1);
        sequence.set(next);
        if let Some(map) = value.get_mut("data").and_then(Value::as_object_mut) {
            map.insert("sequence".to_owned(), json!(next));
        }
    }
    let events = selected
        .into_iter()
        .map(|(_, value)| value)
        .collect::<Vec<_>>();
    Ok(json!({
        "resultType": "complete",
        "events": events,
        "cursor": hub_cursor(cursor),
        "truncated": truncated,
        "hasMore": has_more,
        "nextPollMs": NEXT_POLL_MS,
    }))
}

fn parse_hub_cursor(cursor: &str) -> Result<u64, EventError> {
    let Some(rest) = cursor.strip_prefix("hub:") else {
        return Err(EventError::Invalid(McpFailure::invalid(
            "hub events use a hub:<sequence> cursor",
        )));
    };
    if rest.is_empty() || !rest.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(EventError::Invalid(McpFailure::invalid(
            "hub cursor must be hub:<sequence>",
        )));
    }
    rest.parse().map_err(|_| {
        EventError::Invalid(McpFailure::invalid("hub cursor sequence is out of range"))
    })
}

async fn hub_poller<H>(host: Rc<H>, feed: Rc<HubFeed>)
where
    H: McpHost + 'static,
{
    let mut jobs: HashMap<String, String> = HashMap::new();
    let mut pool_sequence: u64 = 0;
    let mut watched: HashSet<String> = HashSet::new();
    let mut poll = tokio::time::interval(HUB_POLL_INTERVAL);
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut warning = tokio::time::interval(HUB_WARNING_INTERVAL);
    warning.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    warning.tick().await;
    loop {
        tokio::select! {
            _ = poll.tick() => {
                poll_jobs(host.as_ref(), &feed, &mut jobs, &mut watched).await;
                poll_pools(host.as_ref(), &feed, &mut pool_sequence).await;
            }
            _ = warning.tick() => {
                publish_warnings(host.as_ref(), &feed).await;
            }
        }
    }
}

async fn hub_job_views<H>(host: &H) -> Result<Vec<Value>, EventError>
where
    H: McpHost,
{
    let output = host
        .execute(NativeV2CliCommand::Hub(HubRequest {
            context: None,
            command: HubCommand::Ps(super::super::hub::HubPs {
                state: "all".to_owned(),
                target: None,
                labels: Vec::new(),
                limit: None,
            }),
        }))
        .await
        .map_err(classify)?;
    let document: Value = serde_json::from_str(output.trim()).map_err(invalid_hub_response)?;
    Ok(document
        .get("jobs")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default())
}

/// Remember the latest job state one subscriber has actually delivered. A bootstrap and the
/// poller's concurrent first look can report the same or an older state; only a real transition
/// should reach the client. Filtered records are not remembered, so a later transition that does
/// match the requested filter is not mistaken for a repeat.
fn repeated_job_state(event: &HubEvent, states: &HashMap<String, String>) -> bool {
    if event.name != HUB_JOB {
        return false;
    }
    let Some(job_id) = event.data.get("jobId").and_then(Value::as_str) else {
        return false;
    };
    let Some(state) = event.data.get("state").and_then(Value::as_str) else {
        return false;
    };
    let Some(seen) = states.get(job_id) else {
        return false;
    };
    seen == state || job_state_rank(seen) > job_state_rank(state)
}

fn remember_job_state(event: &HubEvent, states: &mut HashMap<String, String>) {
    if event.name != HUB_JOB {
        return;
    }
    let Some(job_id) = event.data.get("jobId").and_then(Value::as_str) else {
        return;
    };
    let Some(state) = event.data.get("state").and_then(Value::as_str) else {
        return;
    };
    states.insert(job_id.to_owned(), state.to_owned());
}

fn job_state_rank(state: &str) -> u8 {
    match state {
        "submitted" => 0,
        // `scheduled` is only a future-dated `queued`; when the job becomes due it is still a
        // real transition and must not be suppressed as an older state.
        "queued" | "scheduled" => 1,
        "placing" => 2,
        "running" => 3,
        "succeeded" | "failed" | "dequeued" | "cancelled" => 4,
        _ => 3,
    }
}

/// The bootstrap state of every job a client can already observe. It is synthesized per
/// subscription, so it neither enters the resident log nor advances the durable hub cursor.
async fn snapshot_events<H>(
    host: &H,
    feed: &Rc<HubFeed>,
    request: &PollRequest,
) -> Option<Vec<Arc<HubEvent>>>
where
    H: McpHost + 'static,
{
    let mut views = hub_job_views(host).await.ok()?;
    views.sort_by(|left, right| {
        let left_at = left
            .get("submittedAt")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let right_at = right
            .get("submittedAt")
            .and_then(Value::as_str)
            .unwrap_or_default();
        left_at
            .cmp(right_at)
            .then_with(|| left_job_id(left).cmp(&left_job_id(right)))
    });
    let filter = HubFilter {
        selector: &request.selector,
        target: &request.target,
        kinds: &request.filters.kinds,
        filters: &request.filters,
    };
    let mut events = Vec::with_capacity(views.len());
    for mut view in views {
        let job_id = left_job_id(&view);
        if job_id.is_empty() {
            continue;
        }
        if let Some(labels) = view.get("labels") {
            feed.job_labels
                .borrow_mut()
                .insert(job_id.clone(), labels.clone());
        }
        let state = hub_job_state(&view);
        let terminal = matches!(
            state.as_str(),
            "succeeded" | "failed" | "dequeued" | "cancelled"
        );
        let mut run = feed.run_facts.borrow().get(&job_id).cloned();
        let needs_facts = terminal
            && !matches!(
                run.as_ref()
                    .and_then(find_delivery_facts)
                    .or_else(|| find_delivery_facts(&view)),
                Some((Some(_), Some(_)))
            );
        if (run.is_none() || needs_facts)
            && view.get("runId").and_then(Value::as_str).is_some()
            && let Ok(snapshot) = job_status(host, &job_id).await
        {
            view = snapshot.view;
            run = snapshot.run;
        }
        if let Some(run) = run.as_ref() {
            feed.run_facts
                .borrow_mut()
                .insert(job_id.clone(), run.clone());
        }
        let event = HubEvent {
            // `hub:0` is before every durable occurrence, so a bootstrap record can neither
            // advance the feed cursor nor be mistaken for replayed history.
            sequence: 0,
            name: HUB_JOB,
            timestamp: now_rfc3339(),
            data: job_snapshot_data(&view, run.as_ref(), terminal),
            run_id: view.get("runId").and_then(Value::as_str).map(str::to_owned),
            job_id: Some(job_id.clone()),
            target: view
                .get("target")
                .and_then(Value::as_str)
                .map(str::to_owned),
            phase: Some(state.clone()),
            terminal,
            snapshot: true,
        };
        if hub_event_value(
            &event,
            &filter,
            Stamp {
                replayed: false,
                sequence: 0,
                id: None,
            },
        )
        .is_some()
        {
            if terminal {
                // A job discovered already terminal still owes the durable feed its one
                // terminal occurrence; the poller will then suppress its own copy. The run
                // record fetched for this snapshot supplies the delivery facts.
                publish_job(
                    host,
                    feed,
                    JobPublication {
                        job_id: &job_id,
                        view: &view,
                        state: &state,
                    },
                )
                .await;
            }
            events.push(Arc::new(event));
        }
    }
    Some(events)
}

fn left_job_id(view: &Value) -> String {
    view.get("jobId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn job_snapshot_data(view: &Value, run: Option<&Value>, terminal: bool) -> Value {
    let status = run
        .and_then(|run| run.get("status"))
        .map(|status| {
            let mut trimmed = Map::new();
            if let Some(phase) = status.get("phase") {
                trimmed.insert("phase".to_owned(), phase.clone());
            }
            if let Some(executions) = status
                .get("activeExecutions")
                .and_then(Value::as_array)
            {
                trimmed.insert(
                    "activeExecutions".to_owned(),
                    Value::Array(
                        executions
                            .iter()
                            .map(|execution| {
                                json!({
                                    "execution": execution.get("execution").cloned().unwrap_or(Value::Null),
                                    "node": execution.get("node").cloned().unwrap_or(Value::Null),
                                })
                            })
                            .collect(),
                    ),
                );
            }
            Value::Object(trimmed)
        })
        .unwrap_or(Value::Null);
    let mut data = json!({
        "jobId": view.get("jobId").cloned().unwrap_or(Value::Null),
        "title": view.get("title").cloned().unwrap_or(Value::Null),
        "state": hub_job_state(view),
        "target": view.get("target").cloned().unwrap_or(Value::Null),
        "project": view.get("project").cloned().unwrap_or(Value::Null),
        "runId": view.get("runId").cloned().unwrap_or(Value::Null),
        "attempts": view.get("attempts").cloned().unwrap_or_else(|| json!(0)),
        "pinnedRevision": view.get("pinnedRevision").cloned().unwrap_or(Value::Null),
        "labels": view.get("labels").cloned().unwrap_or_else(|| json!({})),
        "submittedAt": view.get("submittedAt").cloned().unwrap_or(Value::Null),
        "finishedAt": view.get("finishedAt").cloned().unwrap_or(Value::Null),
        "failureReason": view.get("failureReason").cloned().unwrap_or(Value::Null),
        "errorKind": view.get("errorKind").cloned().unwrap_or(Value::Null),
        "reason": view
            .get("failureReason")
            .or_else(|| view.get("error"))
            .cloned()
            .unwrap_or(Value::Null),
        "at": view
            .get("finishedAt")
            .or_else(|| view.get("dispatchedAt"))
            .or_else(|| view.get("submittedAt"))
            .cloned()
            .unwrap_or_else(|| json!(now_rfc3339())),
        "status": status,
        "terminal": terminal,
    });
    if terminal {
        if let Some((branch, revision)) =
            find_delivery_facts(run.unwrap_or(&Value::Null)).or_else(|| find_delivery_facts(view))
        {
            if let Some(map) = data.as_object_mut() {
                if let Some(branch) = branch {
                    map.insert("headBranch".to_owned(), json!(branch));
                }
                if let Some(revision) = revision {
                    map.insert("headRevision".to_owned(), json!(revision));
                }
            }
        }
    }
    data
}

async fn poll_jobs<H>(
    host: &H,
    feed: &Rc<HubFeed>,
    jobs: &mut HashMap<String, String>,
    watched: &mut HashSet<String>,
) where
    H: McpHost + 'static,
{
    let Ok(views) = hub_job_views(host).await else {
        return;
    };
    for view in views {
        let Some(job_id) = view.get("jobId").and_then(Value::as_str).map(str::to_owned) else {
            continue;
        };
        let state = hub_job_state(&view);
        if let Some(labels) = view.get("labels") {
            feed.job_labels
                .borrow_mut()
                .insert(job_id.clone(), labels.clone());
        }
        let previous = jobs.get(&job_id).cloned();
        if previous.is_none() {
            // The first observation is the job's current state, not a synthetic `submitted`
            // that the next record would immediately supersede. A synthetic intermediate is
            // never truthful for a job that was already running before the feed started.
            publish_job(
                host,
                feed,
                JobPublication {
                    job_id: &job_id,
                    view: &view,
                    state: &state,
                },
            )
            .await;
            jobs.insert(job_id.clone(), state.clone());
        }
        if jobs.get(&job_id) != Some(&state) {
            publish_job(
                host,
                feed,
                JobPublication {
                    job_id: &job_id,
                    view: &view,
                    state: &state,
                },
            )
            .await;
            jobs.insert(job_id.clone(), state.clone());
        }
        let Some(run_id) = view
            .get("runId")
            .and_then(Value::as_str)
            .filter(|run_id| !run_id.is_empty())
            .map(str::to_owned)
        else {
            continue;
        };
        if !watched.insert(run_id.clone()) {
            continue;
        }
        let target = view
            .get("target")
            .and_then(Value::as_str)
            .filter(|target| *target != LOCAL_HUB_MEMBER)
            .map(str::to_owned);
        let command = match watch_command(&run_id, target.as_deref(), None) {
            Ok(command) => command,
            Err(failure) => {
                feed.publish_warning(&job_id, &run_id, json!({"message": rpc_error(failure).1}));
                continue;
            }
        };
        let mut stream = match host.open_watch(&command).await {
            Ok(stream) => stream,
            Err(failure) => {
                feed.publish_warning(&job_id, &run_id, json!({"message": failure.message}));
                continue;
            }
        };
        let feed = Rc::clone(feed);
        let watched_run = run_id.clone();
        tokio::task::spawn_local(async move {
            while let Some(item) = stream.recv().await {
                match item {
                    Ok(event) => feed.publish_run(
                        RunOrigin {
                            run_id: &watched_run,
                            job_id: &job_id,
                            target: target.clone(),
                        },
                        event,
                    ),
                    Err(failure) => {
                        feed.publish_warning(
                            &job_id,
                            &watched_run,
                            json!({"message": failure.message}),
                        );
                        break;
                    }
                }
            }
        });
    }
}

fn hub_job_state(view: &Value) -> String {
    let raw = view
        .get("state")
        .and_then(Value::as_str)
        .unwrap_or("queued");
    match raw {
        "running" if view.get("runId").and_then(Value::as_str).is_none() => "placing".to_owned(),
        "expired" => "failed".to_owned(),
        other => other.to_owned(),
    }
}

struct JobPublication<'a> {
    job_id: &'a str,
    view: &'a Value,
    state: &'a str,
}

async fn publish_job<H>(host: &H, feed: &HubFeed, job: JobPublication<'_>)
where
    H: McpHost,
{
    let JobPublication {
        job_id,
        view,
        state,
    } = job;
    // A job that already produced its terminal occurrence never publishes again, even when a
    // later poll or snapshot still sees the same stable terminal state.
    if feed.terminal_jobs.borrow().contains(job_id) {
        return;
    }
    let terminal = matches!(state, "succeeded" | "failed" | "dequeued" | "cancelled");
    if terminal {
        // Reserve the one terminal before awaiting the run lookup so the poller, a bootstrap,
        // and a resumed subscription cannot publish competing copies.
        feed.terminal_jobs.borrow_mut().insert(job_id.to_owned());
        if !matches!(delivery_facts(feed, job_id, view), Some((Some(_), Some(_))))
            && view.get("runId").and_then(Value::as_str).is_some()
            && let Ok(snapshot) = job_status(host, job_id).await
            && let Some(run) = snapshot.run
        {
            feed.run_facts.borrow_mut().insert(job_id.to_owned(), run);
        }
    }
    let reason = view
        .get("failureReason")
        .or_else(|| view.get("error"))
        .cloned()
        .unwrap_or(Value::Null);
    let at = view
        .get("finishedAt")
        .or_else(|| view.get("dispatchedAt"))
        .or_else(|| view.get("submittedAt"))
        .cloned()
        .unwrap_or_else(|| json!(now_rfc3339()));
    let mut data = json!({
        "jobId": job_id,
        "title": view.get("title").cloned().unwrap_or(Value::Null),
        "state": state,
        "target": view.get("target").cloned().unwrap_or(Value::Null),
        "project": view.get("project").cloned().unwrap_or(Value::Null),
        "runId": view.get("runId").cloned().unwrap_or(Value::Null),
        "attempts": view.get("attempts").cloned().unwrap_or_else(|| json!(0)),
        "pinnedRevision": view.get("pinnedRevision").cloned().unwrap_or(Value::Null),
        "labels": view.get("labels").cloned().unwrap_or_else(|| json!({})),
        "submittedAt": view.get("submittedAt").cloned().unwrap_or(Value::Null),
        "finishedAt": view.get("finishedAt").cloned().unwrap_or(Value::Null),
        "failureReason": view.get("failureReason").cloned().unwrap_or(Value::Null),
        "errorKind": view.get("errorKind").cloned().unwrap_or(Value::Null),
        "reason": reason,
        "at": at,
    });
    if terminal {
        if let Some((branch, revision)) = delivery_facts(feed, job_id, view) {
            if let Some(map) = data.as_object_mut() {
                if let Some(branch) = branch {
                    map.insert("headBranch".to_owned(), json!(branch));
                }
                if let Some(revision) = revision {
                    map.insert("headRevision".to_owned(), json!(revision));
                }
            }
        }
        if let Some(map) = data.as_object_mut() {
            map.insert("terminal".to_owned(), json!(true));
        }
    }
    feed.publish(HubEvent {
        sequence: 0,
        name: HUB_JOB,
        timestamp: now_rfc3339(),
        data,
        run_id: view.get("runId").and_then(Value::as_str).map(str::to_owned),
        job_id: Some(job_id.to_owned()),
        target: view
            .get("target")
            .and_then(Value::as_str)
            .map(str::to_owned),
        phase: Some(state.to_owned()),
        terminal,
        snapshot: false,
    });
}

/// Delivery facts from the job's latest run record, then from its own view. The search is
/// deliberately limited to the two published receipt fields, so unrelated `headRevision`
/// values in a request or review context cannot be mistaken for a landing.
fn delivery_facts(
    feed: &HubFeed,
    job_id: &str,
    view: &Value,
) -> Option<(Option<String>, Option<String>)> {
    let stored = feed.run_facts.borrow().get(job_id).cloned();
    let from_run = stored.as_ref().and_then(find_delivery_facts);
    let from_view = find_delivery_facts(view);
    match (from_run, from_view) {
        (Some((mut branch, mut revision)), Some((view_branch, view_revision))) => {
            if branch.is_none() {
                branch = view_branch;
            }
            if revision.is_none() {
                revision = view_revision;
            }
            Some((branch, revision))
        }
        (Some(facts), None) | (None, Some(facts)) => Some(facts),
        (None, None) => None,
    }
}

fn find_delivery_facts(value: &Value) -> Option<(Option<String>, Option<String>)> {
    let mut branch = None;
    let mut revision = None;
    find_delivery_fields(value, &mut branch, &mut revision);
    (branch.is_some() || revision.is_some()).then_some((branch, revision))
}

fn find_delivery_fields(value: &Value, branch: &mut Option<String>, revision: &mut Option<String>) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                match key.as_str() {
                    "headBranch" => {
                        if branch.is_none() {
                            *branch = value
                                .as_str()
                                .filter(|text| !text.is_empty())
                                .map(str::to_owned);
                        }
                    }
                    "headRevision" => {
                        if revision.is_none() {
                            *revision = value
                                .as_str()
                                .filter(|text| !text.is_empty())
                                .map(str::to_owned);
                        }
                    }
                    _ => find_delivery_fields(value, branch, revision),
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                find_delivery_fields(value, branch, revision);
            }
        }
        _ => {}
    }
}

/// Poll the hub's recorded pool transitions. The daemon performs every transition and keeps
/// the bounded log, so this reader publishes each record once, in sequence order.
async fn poll_pools<H>(host: &H, feed: &HubFeed, sequence: &mut u64)
where
    H: McpHost + 'static,
{
    let output = match host
        .execute(NativeV2CliCommand::Hub(HubRequest {
            context: None,
            command: HubCommand::Pool(HubPoolCommand::List),
        }))
        .await
    {
        Ok(output) => output,
        Err(_) => return,
    };
    let Ok(document) = serde_json::from_str::<Value>(output.trim()) else {
        return;
    };
    let records = document
        .get("events")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let newest = records
        .iter()
        .filter_map(|record| record.get("sequence").and_then(Value::as_u64))
        .max();
    if let Some(newest) = newest {
        if newest < *sequence {
            // A restarted hub numbers its log from the start again.
            *sequence = 0;
        }
    }
    for record in records {
        let Some(record_sequence) = record.get("sequence").and_then(Value::as_u64) else {
            continue;
        };
        if record_sequence <= *sequence {
            continue;
        }
        publish_pool(feed, &record);
        *sequence = record_sequence;
    }
}

fn publish_pool(feed: &HubFeed, record: &Value) {
    let phase = record
        .get("phase")
        .and_then(Value::as_str)
        .unwrap_or("failed")
        .to_owned();
    let at = record
        .get("at")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(now_rfc3339);
    let pool = record.get("pool").and_then(Value::as_str);
    let lease = record.get("leaseId").and_then(Value::as_str);
    let member = record.get("memberId").and_then(Value::as_str);
    let terminal = matches!(phase.as_str(), "deleted" | "failed" | "removed");
    let target = member.or(lease).or(pool).map(str::to_owned);
    let data = json!({
        "kind": record.get("kind").cloned().unwrap_or_else(|| json!("lease")),
        "pool": pool,
        "leaseId": record.get("leaseId").cloned().unwrap_or(Value::Null),
        "serverId": record.get("serverId").cloned().unwrap_or(Value::Null),
        "memberId": record.get("memberId").cloned().unwrap_or(Value::Null),
        "machine": record.get("machine").cloned().unwrap_or(Value::Null),
        "phase": phase,
        "previousPhase": record.get("previousPhase").cloned().unwrap_or(Value::Null),
        "at": at,
        "reason": record.get("reason").cloned().unwrap_or(Value::Null),
    });
    feed.publish(HubEvent {
        sequence: 0,
        name: HUB_POOL,
        timestamp: at,
        data,
        run_id: None,
        job_id: None,
        target,
        phase: Some(phase),
        terminal,
        snapshot: false,
    });
}

async fn publish_warnings<H>(host: &H, feed: &HubFeed)
where
    H: McpHost + 'static,
{
    let Ok(views) = hub_job_views(host).await else {
        return;
    };
    let now = now_ms();
    let mut running = HashSet::new();
    for view in &views {
        if hub_job_state(view) != "running" {
            continue;
        }
        let Some(job_id) = view.get("jobId").and_then(Value::as_str) else {
            continue;
        };
        let Some(run_id) = view.get("runId").and_then(Value::as_str) else {
            continue;
        };
        running.insert(job_id.to_owned());
        let data = feed.warning_data(view, now);
        feed.publish_warning(job_id, run_id, data);
    }
    feed.warnings.borrow_mut().retain_running(&running);
}

struct RunOrigin<'a> {
    run_id: &'a str,
    job_id: &'a str,
    target: Option<String>,
}

impl HubFeed {
    fn publish_run(&self, origin: RunOrigin<'_>, mut event: Value) {
        let phase = event
            .pointer("/status/phase")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let terminal = phase.as_deref() == Some("finished");
        self.warnings
            .borrow_mut()
            .observe(origin.job_id, &event, now_ms());
        if let Some(map) = event.as_object_mut() {
            map.entry("runId").or_insert_with(|| json!(origin.run_id));
        }
        self.run_facts
            .borrow_mut()
            .insert(origin.job_id.to_owned(), event.clone());
        let phase_name = phase.as_deref().unwrap_or("running");
        let mut keys = Vec::new();
        if let Some(executions) = event
            .pointer("/status/activeExecutions")
            .and_then(Value::as_array)
            .filter(|executions| !executions.is_empty())
        {
            for execution in executions {
                let node = execution.get("node").and_then(Value::as_str).unwrap_or("");
                let id = execution
                    .get("execution")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                keys.push(format!("{}|{phase_name}|{node}|{id}", origin.job_id));
            }
        } else {
            keys.push(format!("{}|{phase_name}||", origin.job_id));
        }
        // One run occurrence covers every execution that was active at that moment, so the
        // identity of the occurrence is the whole ordered set. Watching individual tuples would
        // miss a shrinking active set whose remaining tuple was already seen.
        keys.sort();
        let attempt = event
            .pointer("/attempt")
            .or_else(|| event.pointer("/status/attempt"))
            .or_else(|| event.pointer("/status/attempts"))
            .and_then(Value::as_u64)
            .map(|attempt| attempt.to_string())
            .unwrap_or_default();
        let occurrence = format!("{attempt}|{}", keys.join(";"));
        // Compare only with the immediately previous occurrence, not everything the job has
        // ever reported: A,B -> A is a real node finish even though A alone was seen before.
        let fresh = {
            let mut seen = self.run_seen.borrow_mut();
            if seen
                .get(origin.job_id)
                .is_some_and(|previous| previous == &occurrence)
            {
                false
            } else {
                seen.insert(origin.job_id.to_owned(), occurrence);
                true
            }
        };
        // The public status can change metadata, warnings, and delivery facts without a new
        // phase, attempt, execution, or active node. Those changes update `run_facts` and the
        // warning book but must not produce look-alike phase transitions on the feed.
        if !fresh {
            return;
        }
        self.publish(HubEvent {
            sequence: 0,
            name: HUB_RUN,
            timestamp: now_rfc3339(),
            data: event,
            run_id: Some(origin.run_id.to_owned()),
            job_id: Some(origin.job_id.to_owned()),
            target: origin.target,
            phase,
            terminal,
            snapshot: false,
        });
    }

    /// Facts the hub already knows about one running job, keeping the payload additive: every
    /// earlier `jobId`/`runId`/`message`/`at` field keeps its meaning.
    fn warning_data(&self, view: &Value, now: u64) -> Value {
        let job_id = view
            .get("jobId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let run_id = view
            .get("runId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let cursor = view
            .get("lastCursor")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let (previous_cursor, node, execution, execution_since_ms) = {
            let mut book = self.warnings.borrow_mut();
            let entry = book.jobs.entry(job_id.to_owned()).or_default();
            let previous = std::mem::replace(&mut entry.announced_cursor, cursor.clone());
            (
                previous,
                entry.node.clone(),
                entry.execution.clone(),
                entry.execution_since_ms,
            )
        };
        let dispatched_at_ms = view
            .get("dispatchedAt")
            .and_then(Value::as_str)
            .and_then(parse_rfc3339_ms);
        let elapsed_ms = dispatched_at_ms.map(|at| now.saturating_sub(at));
        let unchanged_ms = execution_since_ms.map(|at| now.saturating_sub(at));
        let cursor_delta = match (&previous_cursor, &cursor) {
            (Some(previous), Some(current)) => cursor_sequence(current)
                .zip(cursor_sequence(previous))
                .and_then(|(current, previous)| current.checked_sub(previous)),
            _ => None,
        };

        let mut message = format!(
            "job {job_id} run {run_id} has been running for {}",
            elapsed_ms.map_or_else(|| "an unknown time".to_owned(), human_duration)
        );
        match (&node, &execution) {
            (Some(node), Some(execution)) => {
                message.push_str(&format!(" at node {node}"));
                message.push_str(&format!(" (execution {execution} unchanged for "));
                message.push_str(
                    &unchanged_ms.map_or_else(|| "an unknown time".to_owned(), human_duration),
                );
                message.push(')');
            }
            _ => message.push_str("; no active execution was reported"),
        }
        match (&previous_cursor, cursor_delta) {
            (Some(_), Some(delta)) => message.push_str(&format!(
                "; {delta} cursor event{} since the previous warning",
                if delta == 1 { "" } else { "s" }
            )),
            (Some(_), None) => {
                message.push_str("; the cursor did not advance since the previous warning")
            }
            (None, _) => {
                let at = cursor.as_deref().unwrap_or("unknown");
                message.push_str(&format!("; first warning, cursor {at}"));
            }
        }
        if let Some(reason) = view.get("reaper").and_then(Value::as_str) {
            message.push_str("; reaper withheld action: ");
            message.push_str(reason);
        }

        json!({
            "message": message,
            "at": now_rfc3339(),
            "elapsedMs": elapsed_ms,
            "elapsed": elapsed_ms.map(human_duration),
            "dispatchedAt": view.get("dispatchedAt").cloned().unwrap_or(Value::Null),
            "node": node,
            "execution": execution,
            "executionUnchangedMs": unchanged_ms,
            "executionUnchanged": unchanged_ms.map(human_duration),
            "runCursor": cursor,
            "previousRunCursor": previous_cursor,
            "cursorDelta": cursor_delta,
        })
    }

    fn publish_warning(&self, job_id: &str, run_id: &str, facts: Value) {
        let mut data = Map::new();
        data.insert("jobId".to_owned(), json!(job_id));
        data.insert("runId".to_owned(), json!(run_id));
        // Every warning the hub already emitted carried `at`, including watch failures that
        // supply only a message.
        data.insert("at".to_owned(), json!(now_rfc3339()));
        if let Value::Object(facts) = facts {
            data.extend(facts);
        }
        self.publish(HubEvent {
            sequence: 0,
            name: HUB_WARNING,
            timestamp: now_rfc3339(),
            data: Value::Object(data),
            run_id: Some(run_id.to_owned()),
            job_id: Some(job_id.to_owned()),
            target: None,
            phase: None,
            terminal: false,
            snapshot: false,
        });
    }
}

/// Human-readable duration for warnings: `6h 12m`, `3m`, `45s`.
fn human_duration(ms: u64) -> String {
    let total = ms / 1000;
    let days = total / 86_400;
    let hours = (total % 86_400) / 3_600;
    let minutes = (total % 3_600) / 60;
    let seconds = total % 60;
    if days > 0 {
        format!("{days}d {hours}h")
    } else if hours > 0 {
        format!("{hours}h {minutes}m")
    } else if minutes > 0 {
        format!("{minutes}m")
    } else {
        format!("{seconds}s")
    }
}

fn parse_rfc3339_ms(text: &str) -> Option<u64> {
    let instant = OffsetDateTime::parse(text, &Rfc3339).ok()?;
    u64::try_from(instant.unix_timestamp_nanos() / 1_000_000).ok()
}

fn now_ms() -> u64 {
    u64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000).unwrap_or(0)
}

fn watch_command(
    run_id: &str,
    target: Option<&str>,
    cursor: Option<&str>,
) -> Result<RunWatchCommand, EventError> {
    let mut argv = vec!["watch".to_owned(), run_id.to_owned()];
    if let Some(target) = target {
        argv.push("--target".to_owned());
        argv.push(target.to_owned());
    }
    if let Some(cursor) = cursor {
        argv.push("--after".to_owned());
        argv.push(cursor.to_owned());
    }
    match parse_cli(argv).map_err(EventError::Invalid)? {
        NativeV2CliCommand::Watch(command) => Ok(command),
        _ => Err(EventError::Invalid(McpFailure::invalid(
            "watch did not accept those arguments",
        ))),
    }
}

fn watch_cursor(event: &Value) -> Result<String, McpFailure> {
    event
        .get("cursor")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| McpFailure {
            code: "protocol.invalid".to_owned(),
            message: "watch event is missing a cursor".to_owned(),
        })
}

fn event_data(event: &Value, verbose: bool) -> Value {
    let mut data = event.clone();
    if let Some(map) = data.as_object_mut() {
        for key in ["title", "size", "source", "subscriptionId"] {
            map.remove(key);
        }
    }
    let phase = data
        .pointer("/status/phase")
        .and_then(Value::as_str)
        .map(str::to_owned);
    if phase.as_deref() != Some("finished") {
        if let Some(status) = data.get_mut("status").and_then(Value::as_object_mut) {
            status.remove("metadata");
        }
    } else {
        collapse_terminal(&mut data, verbose);
    }
    data
}

fn collapse_terminal(data: &mut Value, verbose: bool) {
    if verbose {
        return;
    }
    let Some(terminal) = data.pointer_mut("/status/terminalResult") else {
        return;
    };
    let reason = terminal.get("reason").cloned().unwrap_or(Value::Null);
    let Some(context) = terminal.get_mut("context").and_then(Value::as_object_mut) else {
        return;
    };
    let Some(recorded) = context.get("recorded").and_then(Value::as_array) else {
        return;
    };
    let error_count = recorded.len();
    let last_error = recorded
        .last()
        .and_then(|entry| entry.get("message"))
        .cloned()
        .unwrap_or(Value::Null);
    context.insert(
        "recorded".to_owned(),
        json!({
            "reason": reason,
            "lastError": last_error,
            "errorCount": error_count,
        }),
    );
}

fn summary(data: &Value) -> Value {
    let phase = data
        .pointer("/status/phase")
        .cloned()
        .unwrap_or(Value::Null);
    let terminal = phase.as_str() == Some("finished");
    let node = data
        .pointer("/status/activeExecutions")
        .and_then(Value::as_array)
        .and_then(|executions| executions.first())
        .and_then(|execution| execution.get("node"))
        .cloned()
        .unwrap_or(Value::Null);
    let reason = if terminal {
        data.pointer("/status/terminalResult/reason")
            .cloned()
            .unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    json!({
        "phase": phase,
        "node": node,
        "terminal": terminal,
        "reason": reason,
    })
}

fn occurrence(
    event: &Value,
    filters: &Filters,
    replayed: bool,
) -> Result<Option<Value>, McpFailure> {
    let cursor = watch_cursor(event)?;
    let run_id = event.get("runId").and_then(Value::as_str).unwrap_or("run");
    let mut data = event_data(event, filters.verbose);
    let terminal = data.pointer("/status/phase").and_then(Value::as_str) == Some("finished");
    if !filters.accepts(&data, terminal) {
        return Ok(None);
    }
    let summary = summary(&data);
    if let Some(map) = data.as_object_mut() {
        map.insert("summary".to_owned(), summary);
        map.insert("replayed".to_owned(), json!(replayed));
    }
    Ok(Some(json!({
        "eventId": format!("{run_id}:{cursor}"),
        "name": EVENT_NAME,
        "timestamp": now_rfc3339(),
        "data": data,
        "_meta": {},
    })))
}

fn cursor_value(cursor: &Option<String>) -> Value {
    match cursor {
        Some(cursor) => Value::String(cursor.clone()),
        None => Value::Null,
    }
}

fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

fn meta(id: &Value) -> Value {
    let mut object = Map::new();
    object.insert(SUBSCRIPTION_META.to_owned(), id.clone());
    Value::Object(object)
}

fn input_schema(name: &str) -> Value {
    let mut schema = object_schema(
        &[],
        vec![
            (
                "runId",
                string_prop(
                    "Only emit events for this run. Omit it and jobId to receive the whole hub.",
                ),
            ),
            (
                "jobId",
                string_prop("Only emit events for this hub job and its run."),
            ),
            (
                "target",
                string_prop("Only emit events for this named target."),
            ),
            (
                "kinds",
                string_array_prop(
                    "Only emit these event names: hub.job, hub.run, hub.pool, or hub.warning.",
                ),
            ),
            (
                "phases",
                string_array_prop(
                    "Only emit hub.run and hub.pool events whose phase is admitted, running, \
                     stopping, or finished (or a pool lease phase).",
                ),
            ),
            (
                "states",
                string_array_prop(
                    "Only emit hub.job events in one of these job states: submitted, queued, \
                     scheduled, placing, running, succeeded, failed, dequeued, or cancelled.",
                ),
            ),
            (
                "labels",
                json!({
                    "type": "object",
                    "description": "Only emit events whose job carries every one of these labels.",
                    "additionalProperties": {"type": "string"},
                }),
            ),
            (
                "nodes",
                string_array_prop(
                    "Only emit run events with an active execution at one of these graph nodes.",
                ),
            ),
            ("terminalOnly", bool_prop("Only emit terminal events.")),
            (
                "verbose",
                bool_prop("Keep the full recorded failure array on terminal run events."),
            ),
        ],
    );
    schema["description"] = json!(format!(
        "Long-lived {name} subscription. Omit runId and jobId to receive every hub.job, \
         hub.run, hub.pool, and hub.warning event on one stream. The stream stays open after \
         terminal run events and idle periods; stop it with events/unsubscribe, session end, or \
         cancellation. Resume with the hub:N cursor from notifications/events/active. maxAgeMs \
         is accepted but not applied because durable watch records carry no occurrence time."
    ));
    schema
}

fn payload_schema(name: &str) -> Value {
    let run_data = json!({
        "type": "object",
        "description": "Trimmed durable run watch record. data.cursor is the hub cursor; data.runCursor keeps the run's v2: cursor.",
        "additionalProperties": true,
        "required": ["runId", "cursor", "status", "summary", "replayed"],
        "properties": {
            "runId": {"type": "string"},
            "cursor": {"type": "string", "pattern": "^hub:[0-9]+$"},
            "runCursor": {"type": "string", "pattern": "^v2:[0-9]+$"},
            "status": {
                "type": "object",
                "description": "Public run status. status.metadata is present only when phase is finished.",
                "additionalProperties": true,
                "properties": {
                    "phase": {"type": "string", "enum": PHASES},
                    "activeExecutions": {"type": "array"},
                    "terminalResult": {
                        "type": "object",
                        "description": "On failed terminal events context.recorded is {reason,lastError,errorCount} unless verbose is true."
                    },
                    "metadata": {"type": "object"}
                }
            },
            "summary": {"type": "object"},
            "replayed": {"type": "boolean"},
            "sequence": {"type": "integer", "minimum": 1}
        }
    });
    let job_data = json!({
        "type": "object",
        "additionalProperties": true,
        "required": ["jobId", "title", "state", "target", "project", "runId", "attempts", "pinnedRevision", "labels", "submittedAt", "finishedAt", "failureReason", "errorKind", "reason", "at", "cursor", "summary", "replayed", "sequence"],
        "properties": {
            "jobId": {"type": "string"},
            "cursor": {"type": "string", "pattern": "^hub:[0-9]+$"},
            "sequence": {"type": "integer", "minimum": 1},
            "snapshot": {"type": "boolean", "description": "Bootstrap state, not replayed history."},
            "terminal": {"type": "boolean", "description": "The one terminal occurrence for this job."},
            "title": {"type": ["string", "null"]},
            "runId": {"type": ["string", "null"]},
            "attempts": {"type": "integer"},
            "pinnedRevision": {"type": ["string", "null"]},
            "labels": {"type": "object", "additionalProperties": {"type": "string"}},
            "submittedAt": {"type": ["string", "null"]},
            "finishedAt": {"type": ["string", "null"]},
            "failureReason": {"type": ["string", "null"]},
            "errorKind": {"type": ["string", "null"]},
            "headBranch": {"type": ["string", "null"]},
            "headRevision": {"type": ["string", "null"]},
            "status": {"type": ["object", "null"]},
            "state": {
                "type": "string",
                "enum": ["submitted", "queued", "scheduled", "placing", "running", "succeeded", "failed", "dequeued", "cancelled"]
            },
            "target": {"type": ["string", "null"]},
            "project": {"type": ["string", "object", "null"]},
            "reason": {"type": ["string", "null"]},
            "at": {"type": "string"},
            "summary": {"type": "object"},
            "replayed": {"type": "boolean"}
        }
    });
    let pool_data = json!({
        "type": "object",
        "additionalProperties": true,
        "required": ["kind", "pool", "leaseId", "serverId", "memberId", "machine", "phase", "previousPhase", "at", "reason", "cursor", "summary", "replayed"],
        "properties": {
            "kind": {"type": "string", "enum": ["lease", "pool"]},
            "pool": {"type": "string"},
            "cursor": {"type": "string", "pattern": "^hub:[0-9]+$"},
            "leaseId": {"type": ["string", "null"]},
            "serverId": {"type": ["integer", "string", "null"]},
            "memberId": {"type": ["string", "null"]},
            "machine": {
                "type": ["object", "null"],
                "additionalProperties": true,
                "properties": {
                    "cores": {"type": "integer"},
                    "memoryMb": {"type": "integer"},
                    "location": {"type": "string"},
                    "serverType": {"type": "string"}
                }
            },
            "phase": {
                "type": "string",
                "enum": ["requested", "creating", "booting", "preparing", "ready", "in_use", "idle", "draining", "deleted", "failed", "added", "removed", "paused", "resumed", "blocked", "shortage"]
            },
            "previousPhase": {"type": ["string", "null"]},
            "reason": {"type": ["string", "null"]},
            "at": {"type": "string"},
            "summary": {"type": "object"},
            "replayed": {"type": "boolean"},
            "sequence": {"type": "integer", "minimum": 1}
        }
    });
    let warning_data = json!({
        "type": "object",
        "additionalProperties": true,
        "required": ["jobId", "runId", "message", "at", "cursor", "summary", "replayed"],
        "properties": {
            "jobId": {"type": "string"},
            "runId": {"type": "string"},
            "cursor": {"type": "string", "pattern": "^hub:[0-9]+$"},
            "message": {"type": "string"},
            "at": {"type": "string"},
            "summary": {"type": "object"},
            "replayed": {"type": "boolean"},
            "sequence": {"type": "integer", "minimum": 1}
        }
    });
    let data = match name {
        HUB_JOB => job_data,
        HUB_RUN => run_data,
        HUB_POOL => pool_data,
        HUB_WARNING => warning_data,
        _ => json!({"oneOf": [job_data, run_data, pool_data, warning_data]}),
    };
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["eventId", "name", "timestamp", "data", "_meta"],
        "properties": {
            "eventId": {
                "type": "string",
                "pattern": "^hub:[0-9]+$",
                "description": "The durable hub:<sequence> cursor. Bootstrap snapshot events use hub:0."
            },
            "name": {
                "type": "string",
                "description": "Event name carried by this multiplexed subscription.",
                "enum": HUB_EVENTS
            },
            "timestamp": {"type": "string", "format": "date-time"},
            "data": data,
            "_meta": {"type": "object"}
        }
    })
}

struct ActiveView<'a> {
    cursor: &'a Option<String>,
    run: Option<&'a Value>,
    truncated: Value,
    job: Option<&'a Value>,
}

async fn write_active<W>(
    writer: &Mutex<W>,
    id: &Value,
    active: &ActiveView<'_>,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut params = json!({
        "cursor": cursor_value(active.cursor),
        "run": active.run.cloned().unwrap_or(Value::Null),
        "truncated": active.truncated.clone(),
        "_meta": meta(id),
    });
    if let Some(job) = active.job {
        params["job"] = job.clone();
        for key in ["state", "error", "errorKind"] {
            params[key] = job.get(key).cloned().unwrap_or(Value::Null);
        }
        if let (Some(job_id), Some(cursor)) =
            (job.get("jobId").and_then(Value::as_str), active.cursor)
        {
            params["eventId"] = json!(format!("{job_id}:{cursor}"));
        }
    }
    write_notification(writer, "notifications/events/active", params).await
}

async fn write_heartbeat<W>(
    writer: &Mutex<W>,
    id: &Value,
    cursor: &Option<String>,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_notification(
        writer,
        "notifications/events/heartbeat",
        json!({"cursor": cursor_value(cursor), "_meta": meta(id)}),
    )
    .await
}

async fn write_notification<W>(
    writer: &Mutex<W>,
    method: &str,
    params: Value,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    super::serve::write_message(
        writer,
        &json!({"jsonrpc": "2.0", "method": method, "params": params}),
    )
    .await
}

#[cfg(test)]
mod warning_tests {
    use super::*;

    #[test]
    fn hub_warnings_name_the_execution_and_cursor_delta() {
        let feed = HubFeed::new();
        let now = parse_rfc3339_ms("2024-01-01T06:12:00Z").expect("time");
        let observed_at = now - 12 * 60_000;
        feed.warnings.borrow_mut().observe(
            "hj_1",
            &json!({
                "status": {
                    "phase": "running",
                    "activeExecutions": [{"node": "worker", "execution": "exec-7"}],
                },
            }),
            observed_at,
        );
        let mut view = json!({
            "jobId": "hj_1",
            "runId": "run-1",
            "dispatchedAt": "2024-01-01T00:00:00Z",
            "lastCursor": "v2:15",
        });

        let first = feed.warning_data(&view, now);
        assert_eq!(first["elapsed"], json!("6h 12m"));
        assert_eq!(first["node"], json!("worker"));
        assert_eq!(first["execution"], json!("exec-7"));
        assert_eq!(first["executionUnchanged"], json!("12m"));
        assert_eq!(first["cursorDelta"], Value::Null);
        assert!(
            first["message"]
                .as_str()
                .expect("message")
                .contains("first warning")
        );

        view["lastCursor"] = json!("v2:20");
        let second = feed.warning_data(&view, now + 60_000);
        assert_eq!(second["previousRunCursor"], json!("v2:15"));
        assert_eq!(second["cursorDelta"], json!(5));
        assert!(
            second["message"]
                .as_str()
                .expect("message")
                .contains("5 cursor events")
        );
    }

    fn job_event(state: &str, snapshot: bool) -> HubEvent {
        HubEvent {
            sequence: 0,
            name: HUB_JOB,
            timestamp: "2024-01-01T00:00:00Z".to_owned(),
            data: json!({"jobId": "hj_1", "state": state}),
            run_id: None,
            job_id: Some("hj_1".to_owned()),
            target: None,
            phase: None,
            terminal: false,
            snapshot,
        }
    }

    #[test]
    fn a_stale_initial_poll_cannot_repeat_the_bootstrap_state() {
        let mut states = HashMap::new();
        let scheduled = job_event("scheduled", true);
        assert!(!repeated_job_state(&scheduled, &states));
        remember_job_state(&scheduled, &mut states);
        // A stale poll cannot walk a scheduled bootstrap back to submitted.
        assert!(repeated_job_state(&job_event("submitted", false), &states));
        // Becoming due is a real scheduled -> queued transition, not a repeat.
        let queued = job_event("queued", false);
        assert!(!repeated_job_state(&queued, &states));
        remember_job_state(&queued, &mut states);
        assert!(repeated_job_state(&queued, &states));
        let running = job_event("running", false);
        assert!(!repeated_job_state(&running, &states));
        remember_job_state(&running, &mut states);
        let dequeued = job_event("dequeued", false);
        assert!(!repeated_job_state(&dequeued, &states));
        remember_job_state(&dequeued, &mut states);
        assert!(repeated_job_state(&dequeued, &states));
    }

    #[test]
    fn a_filtered_job_state_is_not_remembered() {
        let mut states = HashMap::new();
        // The snapshot was filtered out, so the same state arriving live is still deliverable.
        assert!(!repeated_job_state(&job_event("running", true), &states));
        assert!(!repeated_job_state(&job_event("running", false), &states));
        remember_job_state(&job_event("running", false), &mut states);
        assert!(repeated_job_state(&job_event("running", false), &states));
    }

    #[test]
    fn hub_warning_durations_stay_readable() {
        assert_eq!(human_duration(0), "0s");
        assert_eq!(human_duration(45_000), "45s");
        assert_eq!(human_duration(3 * 60_000), "3m");
        assert_eq!(human_duration((6 * 60 + 12) * 60_000), "6h 12m");
        assert_eq!(human_duration((2 * 24 + 3) * 3_600_000), "2d 3h");
    }
}
