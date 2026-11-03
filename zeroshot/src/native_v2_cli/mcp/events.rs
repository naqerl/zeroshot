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
use super::super::hub::{HubCommand, HubRequest};
use super::super::{NativeV2CliCommand, RunWatchCommand};

pub(super) const EXTENSION_ID: &str = "io.modelcontextprotocol/events";
const EVENT_NAME: &str = "run.event";
const HUB_JOB: &str = "hub.job";
const HUB_RUN: &str = "hub.run";
const HUB_RUN_DETAIL: &str = "hub.run.detail";
const HUB_POOL: &str = "hub.pool";
const HUB_WARNING: &str = "hub.warning";
const HUB_EVENTS: &[&str] = &[HUB_JOB, HUB_RUN, HUB_RUN_DETAIL, HUB_POOL, HUB_WARNING];
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
const PHASES: &[&str] = &["admitted", "running", "stopping", "finished"];
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
    run_id: Option<String>,
    target: Option<String>,
}

#[derive(Clone, Default)]
struct Filters {
    phases: Option<Vec<String>>,
    nodes: Option<Vec<String>>,
    kinds: Option<Vec<String>>,
    terminal_only: bool,
    verbose: bool,
    detail: bool,
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
#[derive(Clone)]
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
    /// A stored per-cursor record that starts a new node or ends the running phase. Delivery
    /// exposes it as the collapsed `hub.run` kind unless the subscriber opted into detail.
    transition: bool,
}

#[derive(Default)]
struct RunDeliveryState {
    seen: bool,
    node: Option<String>,
    node_key: Option<String>,
    phase: Option<String>,
    node_changed_at: Option<String>,
    node_changed_at_ms: u64,
    cursors_since_last_event: u64,
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
    /// Delivery state per run, so collapse is decided once for every subscriber and replay.
    runs: RefCell<HashMap<String, RunDeliveryState>>,
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
            runs: RefCell::new(HashMap::new()),
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
        // Only a transition record is the run's observable state. A non-transition detail
        // record is an intermediate cursor, never the snapshot a late subscriber needs.
        HUB_RUN_DETAIL if event.transition => event.run_id.clone().map(|key| ("run", key)),
        HUB_RUN => event.run_id.clone().map(|key| ("run", key)),
        HUB_POOL => event.target.clone().map(|key| ("pool", key)),
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
    let descriptions = [
        (
            HUB_JOB,
            "A hub job lifecycle event for every submitted, queued, scheduled, placing, running, \
             succeeded, failed, dequeued, or cancelled job.",
        ),
        (
            HUB_RUN,
            "A collapsed run occurrence published when the run changes node or leaves the running \
             phase. It carries the liveness evidence for the cursor advances it collapses.",
        ),
        (
            HUB_RUN_DETAIL,
            "Every durable per-cursor run record, delivered only to subscribers that ask for detail \
             with arguments.detail or the hub.run.detail kind.",
        ),
        (
            HUB_POOL,
            "A pool lease lifecycle event: provisioning, active, draining, deleted, or failed.",
        ),
        (
            HUB_WARNING,
            "An hourly warning for a running job, or a run-watch and stream failure.",
        ),
    ];
    let events = descriptions
        .into_iter()
        .map(|(name, description)| {
            json!({
                "name": name,
                "description": description,
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
) -> Result<Value, EventError>
where
    H: McpHost + 'static,
{
    let request = parse_subscription(params, true)?;
    if request.hub {
        return hub_poll(host, feed, &request).await;
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
    prepare_hub(host, feed, &request)
}

fn prepare_hub<H>(
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
    let (receiver, replay, truncated, cursor) = feed.snapshot(after);
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
    for event in replay {
        if cancelled.load(Ordering::Relaxed) {
            return Ok(());
        }
        if let Some(value) = hub_event_value(&event, &filter, true, Some(&id)) {
            write_notification(&writer, "notifications/events/event", value).await?;
        }
        if cursor
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
                            if let Some(value) = hub_event_value(&event, &filter, false, Some(&id)) {
                                write_notification(&writer, "notifications/events/event", value)
                                    .await?;
                            }
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
                            if let Some(value) = hub_event_value(&event, &filter, false, Some(&id)) {
                                write_notification(&writer, "notifications/events/event", value)
                                    .await?;
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

fn delivered_name(event: &HubEvent, detail: bool) -> Option<&'static str> {
    if event.name != HUB_RUN_DETAIL {
        return Some(event.name);
    }
    if detail {
        // An explicit opt-in keeps every cursor, including the one that changed the node.
        return Some(HUB_RUN_DETAIL);
    }
    // Default subscribers only see the node transitions; the intermediate cursors stay in the
    // durable log for readers that ask for them.
    event.transition.then_some(HUB_RUN)
}

fn hub_event_value(
    event: &HubEvent,
    filter: &HubFilter<'_>,
    replayed: bool,
    id: Option<&Value>,
) -> Option<Value> {
    let detail = filter.filters.detail
        || filter
            .kinds
            .as_ref()
            .is_some_and(|kinds| kinds.iter().any(|kind| kind == HUB_RUN_DETAIL));
    let name = delivered_name(event, detail)?;
    if let Some(kinds) = filter.kinds {
        if !kinds.iter().any(|kind| kind == name) {
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
    let is_run = matches!(name, HUB_RUN | HUB_RUN_DETAIL);
    let mut data = if is_run {
        event_data(&event.data, filter.filters.verbose)
    } else {
        event.data.clone()
    };
    if !filter.filters.accepts(&data, event.terminal) {
        return None;
    }
    let summary = if is_run {
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
        if is_run {
            if let Some(run_cursor) = map.get("cursor").cloned() {
                map.insert("runCursor".to_owned(), run_cursor);
            }
        }
        map.insert("kind".to_owned(), json!(name));
        map.insert("cursor".to_owned(), json!(hub_cursor(event.sequence)));
        map.insert("summary".to_owned(), summary);
        map.insert("replayed".to_owned(), json!(replayed));
    }
    let value = json!({
        "eventId": hub_cursor(event.sequence),
        "name": name,
        "timestamp": event.timestamp,
        "data": data,
        "_meta": id.map(meta).unwrap_or_else(|| json!({})),
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
        EventError::NotFound => (-32023, "NotFound", json!({"kind": "event"})),
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
        Some("hub.event") => None,
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
        || matches!(name.as_deref(), Some("hub.event"));
    let target = optional_string(arguments, "target").map_err(EventError::Invalid)?;
    let mut filters = parse_filters(arguments)?;
    if filters.kinds.is_none() {
        filters.kinds = name_kinds.or_else(|| {
            (hub && name.as_deref() == Some(EVENT_NAME)).then(|| vec![HUB_RUN.to_owned()])
        });
    }
    if filters
        .kinds
        .as_ref()
        .is_some_and(|kinds| kinds.iter().any(|kind| kind == HUB_RUN_DETAIL))
    {
        filters.detail = true;
    }
    if filters.detail {
        if let Some(kinds) = filters.kinds.as_mut() {
            // `detail: true` swaps the collapsed run kind for the raw trail. Without this a
            // `run.event` subscription would opt in and then filter its own detail records out.
            for kind in kinds.iter_mut() {
                if kind == HUB_RUN {
                    *kind = HUB_RUN_DETAIL.to_owned();
                }
            }
            let mut seen = HashSet::new();
            kinds.retain(|kind| seen.insert(kind.clone()));
        }
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
            "nodes",
            "terminalOnly",
            "verbose",
            "detail",
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
                if !PHASES.contains(&phase) {
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
        nodes,
        kinds,
        terminal_only: optional_bool(arguments, "terminalOnly")?,
        verbose: optional_bool(arguments, "verbose")?,
        detail: optional_bool(arguments, "detail")?,
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
    let (receiver, replay, truncated, _) = feed.snapshot(after);
    drop(receiver);
    let head = feed.head();
    let filter = HubFilter {
        selector: &request.selector,
        target: &request.target,
        kinds: &request.filters.kinds,
        filters: &request.filters,
    };
    let mut selected = replay
        .iter()
        .filter_map(|event| {
            hub_event_value(event, &filter, true, None).map(|value| (event.sequence, value))
        })
        .collect::<Vec<_>>();
    let mut has_more = false;
    let mut cursor = after.unwrap_or(head);
    if selected.len() > request.max_events {
        selected.truncate(request.max_events);
        if let Some((sequence, _)) = selected.last() {
            cursor = *sequence;
        }
        // A maxEvents clip is resumable from the returned cursor, so it is hasMore
        // rather than truncation. Only replay or backpressure can permanently lose events.
        has_more = true;
    } else if let Some((sequence, _)) = selected.last() {
        if *sequence > cursor {
            cursor = *sequence;
        }
    } else if !replay.is_empty() && cursor < head {
        cursor = head;
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
    let mut members: HashMap<String, String> = HashMap::new();
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
                poll_members(host.as_ref(), &feed, &mut members).await;
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
        let previous = jobs.get(&job_id).cloned();
        if previous.is_none() {
            publish_job(feed, &job_id, &view, "submitted");
            jobs.insert(job_id.clone(), "submitted".to_owned());
        }
        if jobs.get(&job_id) != Some(&state) {
            publish_job(feed, &job_id, &view, &state);
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

fn publish_job(feed: &HubFeed, job_id: &str, view: &Value, state: &str) {
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
    let data = json!({
        "jobId": job_id,
        "title": view.get("title").cloned().unwrap_or(Value::Null),
        "state": state,
        "target": view.get("target").cloned().unwrap_or(Value::Null),
        "project": view.get("project").cloned().unwrap_or(Value::Null),
        "reason": reason,
        "at": at,
    });
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
        terminal: matches!(state, "succeeded" | "failed" | "dequeued" | "cancelled"),
        transition: false,
    });
}

async fn poll_members<H>(host: &H, feed: &HubFeed, members: &mut HashMap<String, String>)
where
    H: McpHost + 'static,
{
    let output = match host
        .execute(NativeV2CliCommand::Hub(HubRequest {
            context: None,
            command: HubCommand::List {
                tags: Vec::new(),
                probe: false,
            },
        }))
        .await
    {
        Ok(output) => output,
        Err(_) => return,
    };
    let Ok(document) = serde_json::from_str::<Value>(output.trim()) else {
        return;
    };
    let mut current = HashMap::new();
    for member in document
        .get("targets")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(name) = member
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_owned)
        else {
            continue;
        };
        let phase = member_phase(member);
        current.insert(name.clone(), phase.to_owned());
        let first = members.get(&name).is_none();
        if first && phase != "provisioning" {
            publish_member(feed, &name, member, "provisioning");
        }
        if first || members.get(&name) != Some(&phase.to_owned()) {
            publish_member(feed, &name, member, phase);
        }
    }
    for (name, phase) in members.iter() {
        if !current.contains_key(name) && phase != "deleted" {
            publish_member(
                feed,
                name,
                &json!({"name": name, "state": "deleted"}),
                "deleted",
            );
        }
    }
    *members = current;
}

fn member_phase(member: &Value) -> &'static str {
    match member.get("state").and_then(Value::as_str) {
        Some("active") => "active",
        Some("drain") => "draining",
        Some("disabled") => "failed",
        Some("deleted") => "deleted",
        _ => "provisioning",
    }
}

fn publish_member(feed: &HubFeed, name: &str, member: &Value, phase: &str) {
    let reason = member
        .get("probeError")
        .or_else(|| member.get("error"))
        .cloned()
        .unwrap_or(Value::Null);
    let at = member
        .get("probedAt")
        .cloned()
        .unwrap_or_else(|| json!(now_rfc3339()));
    let data = json!({
        "pool": member
            .get("pool")
            .cloned()
            .unwrap_or_else(|| json!(name)),
        "leaseId": member.get("leaseId").cloned().unwrap_or_else(|| json!(name)),
        "serverId": member.get("serverId").cloned().unwrap_or(Value::Null),
        "machine": member.get("machine").cloned().unwrap_or_else(|| json!(name)),
        "phase": phase,
        "reason": reason,
        "at": at,
    });
    feed.publish(HubEvent {
        sequence: 0,
        name: HUB_POOL,
        timestamp: now_rfc3339(),
        data,
        run_id: None,
        job_id: None,
        target: Some(name.to_owned()),
        phase: Some(phase.to_owned()),
        terminal: matches!(phase, "deleted" | "failed"),
        transition: false,
    });
}

async fn publish_warnings<H>(host: &H, feed: &HubFeed)
where
    H: McpHost + 'static,
{
    let Ok(views) = hub_job_views(host).await else {
        return;
    };
    for view in views {
        if hub_job_state(&view) != "running" {
            continue;
        }
        let Some(job_id) = view.get("jobId").and_then(Value::as_str) else {
            continue;
        };
        let Some(run_id) = view.get("runId").and_then(Value::as_str) else {
            continue;
        };
        feed.publish_warning(
            job_id,
            run_id,
            json!({"message": "run has been in progress for an hour"}),
        );
    }
}

struct RunOrigin<'a> {
    run_id: &'a str,
    job_id: &'a str,
    target: Option<String>,
}

fn now_stamp() -> (String, u64) {
    let now = OffsetDateTime::now_utc();
    let millis = u64::try_from(now.unix_timestamp_nanos() / 1_000_000).unwrap_or(0);
    (
        now.format(&Rfc3339)
            .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned()),
        millis,
    )
}

fn terminal_phase(phase: &str) -> bool {
    matches!(phase, "finished" | "succeeded" | "failed" | "cancelled")
}

/// The node the run currently reports, plus a stable identity for the whole active node set.
/// Parallel branches expose one active execution each; `node` is the first one for the payload,
/// while `key` lets a shrinking set (A,B -> A) count as a real node change.
fn active_nodes(event: &Value) -> (Option<String>, Option<String>) {
    let mut nodes = event
        .pointer("/status/activeExecutions")
        .and_then(Value::as_array)
        .map(|executions| {
            executions
                .iter()
                .filter_map(|execution| execution.get("node").and_then(Value::as_str))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let first = nodes.first().map(|node| (*node).to_owned());
    nodes.sort_unstable();
    nodes.dedup();
    let key = (!nodes.is_empty()).then(|| nodes.join(","));
    (first, key)
}

fn terminal_reason(event: &Value) -> Value {
    event
        .pointer("/status/terminalResult/reason")
        .or_else(|| event.get("reason"))
        .cloned()
        .unwrap_or(Value::Null)
}

impl HubFeed {
    /// Store every watch record, but mark only node changes and phase exits from `running` as
    /// collapsed transitions. The raw record (with its cursor) is still appended to the log, so
    /// `hub ps` history and detail subscribers keep the complete trail.
    fn publish_run(&self, origin: RunOrigin<'_>, mut event: Value) {
        let (at, now_ms) = now_stamp();
        let phase = event
            .pointer("/status/phase")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let terminal = phase.as_deref().is_some_and(terminal_phase);
        if let Some(map) = event.as_object_mut() {
            map.entry("runId").or_insert_with(|| json!(origin.run_id));
        }
        let (node, node_key) = active_nodes(&event);
        let run_cursor = event
            .get("cursor")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let reason = terminal_reason(&event);
        let (transition, previous_node, cursors_since_last_event, node_changed_at, elapsed) = {
            let mut runs = self.runs.borrow_mut();
            let entry = runs.entry(origin.job_id.to_owned()).or_default();
            let first = !entry.seen;
            // The node set identifies a transition: a parallel run that shrinks from A,B to A
            // really did finish a node even though its first active node is unchanged.
            let node_changed = !first && entry.node_key != node_key;
            let node_moved = first || node_changed;
            let current_phase = phase.as_deref();
            // Leaving `running` ends the progress window. Later non-running phases (stopping ->
            // finished) still announce their own transition, so the terminal event is never lost
            // when a subscriber first observes the run already stopping.
            let left_running = current_phase.is_some_and(|phase| phase != "running")
                && entry.phase.as_deref() != current_phase;
            let transition = node_moved || left_running;
            let previous_node = entry.node.clone();
            // The current record is itself a cursor observation, so it counts toward the gap
            // between collapsed events. The very first event has no previous cursor to count.
            let cursors_since_last_event = if first {
                0
            } else if transition {
                entry.cursors_since_last_event.saturating_add(1)
            } else {
                entry.cursors_since_last_event
            };
            if transition {
                entry.cursors_since_last_event = 0;
            } else {
                entry.cursors_since_last_event = entry.cursors_since_last_event.saturating_add(1);
            }
            // Only a real node change starts a new quiet window. A phase-only transition keeps the
            // retained node start, so `elapsedMsInNode` measures the time the node stayed the same.
            if node_moved {
                entry.node = node.clone();
                entry.node_key = node_key;
                entry.node_changed_at = Some(at.clone());
                entry.node_changed_at_ms = now_ms;
            }
            entry.phase = phase.clone();
            entry.seen = true;
            (
                transition,
                previous_node,
                cursors_since_last_event,
                entry.node_changed_at.clone(),
                now_ms.saturating_sub(entry.node_changed_at_ms),
            )
        };
        if let Some(map) = event.as_object_mut() {
            if transition {
                // The collapsed event is the whole liveness answer: a reader can tell from it
                // and the clock whether the run moved, how long the node has been unchanged, and
                // whether the run ended. Non-transition records keep the plain watch record.
                map.insert(
                    "node".to_owned(),
                    node.map(Value::String).unwrap_or(Value::Null),
                );
                map.insert(
                    "previousNode".to_owned(),
                    previous_node.map(Value::String).unwrap_or(Value::Null),
                );
                map.insert(
                    "phase".to_owned(),
                    phase.clone().map(Value::String).unwrap_or(Value::Null),
                );
                map.insert(
                    "runCursor".to_owned(),
                    run_cursor.map(Value::String).unwrap_or(Value::Null),
                );
                map.insert("lastCursorAt".to_owned(), json!(at.clone()));
                map.insert(
                    "cursorsSinceLastEvent".to_owned(),
                    json!(cursors_since_last_event),
                );
                map.insert("elapsedMsInNode".to_owned(), json!(elapsed));
                map.insert(
                    "nodeChangedAt".to_owned(),
                    node_changed_at.map(Value::String).unwrap_or(Value::Null),
                );
                map.insert("terminal".to_owned(), json!(terminal));
                map.insert("reason".to_owned(), reason);
            }
        }
        self.publish(HubEvent {
            sequence: 0,
            name: HUB_RUN_DETAIL,
            timestamp: at,
            data: event,
            run_id: Some(origin.run_id.to_owned()),
            job_id: Some(origin.job_id.to_owned()),
            target: origin.target,
            phase,
            terminal,
            transition,
        });
    }

    fn publish_warning(&self, job_id: &str, run_id: &str, facts: Value) {
        let mut data = Map::new();
        data.insert("jobId".to_owned(), json!(job_id));
        data.insert("runId".to_owned(), json!(run_id));
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
            transition: false,
        });
    }
}

#[cfg(test)]
impl HubFeed {
    pub(super) fn test_publish_run(&self, run_id: &str, job_id: &str, event: Value) {
        self.publish_run(
            RunOrigin {
                run_id,
                job_id,
                target: None,
            },
            event,
        );
    }

    pub(super) fn test_publish_warning(&self, job_id: &str, run_id: &str, facts: Value) {
        self.publish_warning(job_id, run_id, facts);
    }

    /// Deliver the stored log through the same subscription parser and filter the real poll and
    /// stream paths use, without an HTTP round trip.
    pub(super) fn test_deliver(&self, name: Option<&str>, arguments: Value) -> Vec<Value> {
        let mut params = json!({"arguments": arguments});
        if let Some(name) = name {
            params["name"] = json!(name);
        }
        let Ok(request) = parse_subscription(&params, false) else {
            panic!("test subscription parses");
        };
        let filter = HubFilter {
            selector: &request.selector,
            target: &request.target,
            kinds: &request.filters.kinds,
            filters: &request.filters,
        };
        self.log
            .borrow()
            .events
            .iter()
            .filter_map(|event| hub_event_value(event, &filter, false, None))
            .collect()
    }

    pub(super) fn test_log_records(&self) -> Vec<(u64, &'static str, bool)> {
        self.log
            .borrow()
            .events
            .iter()
            .map(|event| (event.sequence, event.name, event.transition))
            .collect()
    }
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
    let terminal = phase.as_str().is_some_and(terminal_phase);
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
                    "Only emit these event names: hub.job, hub.run, hub.run.detail, hub.pool, or \
                     hub.warning.",
                ),
            ),
            (
                "phases",
                string_array_prop(
                    "Only emit events whose run phase or job state is one of admitted, running, \
                     stopping, or finished.",
                ),
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
            (
                "detail",
                bool_prop(
                    "Deliver every raw per-cursor run record as hub.run.detail instead of the \
                     collapsed hub.run occurrences.",
                ),
            ),
        ],
    );
    schema["description"] = json!(format!(
        "Long-lived {name} subscription. Omit runId and jobId to receive every hub.job, \
         hub.run, hub.pool, and hub.warning event on one stream. hub.run is collapsed to node and \
         phase transitions; pass arguments.detail to receive every raw record as hub.run.detail. \
         The stream stays open after terminal run events and idle periods; stop it with \
         events/unsubscribe, session end, or cancellation. Resume with the hub:N cursor from \
         notifications/events/active. maxAgeMs is accepted but not applied because durable watch \
         records carry no occurrence time."
    ));
    schema
}

fn payload_schema(name: &str) -> Value {
    let run_data = json!({
        "type": "object",
        "description": "Collapsed run transition with the liveness evidence for hidden cursors. data.cursor is the hub cursor; data.runCursor keeps the current raw v2: cursor.",
        "additionalProperties": true,
        "required": [
            "kind", "runId", "cursor", "runCursor", "node", "previousNode", "phase", "lastCursorAt",
            "cursorsSinceLastEvent", "elapsedMsInNode", "nodeChangedAt", "terminal", "reason",
            "status", "summary", "replayed"
        ],
        "properties": {
            "kind": {"const": HUB_RUN},
            "runId": {"type": "string"},
            "cursor": {"type": "string", "pattern": "^hub:[0-9]+$"},
            "runCursor": {"type": ["string", "null"], "pattern": "^v2:[0-9]+$"},
            "node": {"type": ["string", "null"]},
            "previousNode": {"type": ["string", "null"]},
            "phase": {"type": ["string", "null"]},
            "lastCursorAt": {"type": ["string", "null"], "format": "date-time"},
            "cursorsSinceLastEvent": {"type": "integer", "minimum": 0},
            "elapsedMsInNode": {"type": "integer", "minimum": 0},
            "nodeChangedAt": {"type": ["string", "null"], "format": "date-time"},
            "terminal": {"type": "boolean"},
            "reason": {"type": ["string", "null"]},
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
            "replayed": {"type": "boolean"}
        }
    });
    let run_detail_data = json!({
        "type": "object",
        "description": "One raw per-cursor run record. data.cursor is the hub cursor; data.runCursor keeps the run's v2: cursor. Transition records also carry the collapsed-event evidence.",
        "additionalProperties": true,
        "required": ["kind", "runId", "cursor", "runCursor", "status", "summary", "replayed"],
        "properties": {
            "kind": {"const": HUB_RUN_DETAIL},
            "runId": {"type": "string"},
            "cursor": {"type": "string", "pattern": "^hub:[0-9]+$"},
            "runCursor": {"type": ["string", "null"], "pattern": "^v2:[0-9]+$"},
            "node": {"type": ["string", "null"]},
            "previousNode": {"type": ["string", "null"]},
            "phase": {"type": ["string", "null"]},
            "lastCursorAt": {"type": ["string", "null"], "format": "date-time"},
            "cursorsSinceLastEvent": {"type": "integer", "minimum": 0},
            "elapsedMsInNode": {"type": "integer", "minimum": 0},
            "nodeChangedAt": {"type": ["string", "null"], "format": "date-time"},
            "terminal": {"type": "boolean"},
            "reason": {"type": ["string", "null"]},
            "status": {"type": "object", "additionalProperties": true},
            "summary": {"type": "object"},
            "replayed": {"type": "boolean"}
        }
    });
    let job_data = json!({
        "type": "object",
        "additionalProperties": true,
        "required": ["jobId", "title", "state", "target", "project", "reason", "at", "cursor", "summary", "replayed"],
        "properties": {
            "kind": {"const": HUB_JOB},
            "jobId": {"type": "string"},
            "cursor": {"type": "string", "pattern": "^hub:[0-9]+$"},
            "title": {"type": ["string", "null"]},
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
        "required": ["pool", "leaseId", "serverId", "machine", "phase", "reason", "at", "cursor", "summary", "replayed"],
        "properties": {
            "kind": {"const": HUB_POOL},
            "pool": {"type": "string"},
            "cursor": {"type": "string", "pattern": "^hub:[0-9]+$"},
            "leaseId": {"type": "string"},
            "serverId": {"type": ["integer", "string", "null"]},
            "machine": {"type": ["string", "null"]},
            "phase": {"type": "string", "enum": ["provisioning", "active", "draining", "deleted", "failed"]},
            "reason": {"type": ["string", "null"]},
            "at": {"type": "string"},
            "summary": {"type": "object"},
            "replayed": {"type": "boolean"}
        }
    });
    let warning_data = json!({
        "type": "object",
        "additionalProperties": true,
        "required": ["jobId", "runId", "message", "at", "cursor", "summary", "replayed"],
        "properties": {
            "kind": {"const": HUB_WARNING},
            "jobId": {"type": "string"},
            "runId": {"type": "string"},
            "cursor": {"type": "string", "pattern": "^hub:[0-9]+$"},
            "message": {"type": "string"},
            "at": {"type": "string"},
            "summary": {"type": "object"},
            "replayed": {"type": "boolean"}
        }
    });
    let data = match name {
        HUB_JOB => job_data,
        HUB_RUN => run_data,
        HUB_RUN_DETAIL => run_detail_data,
        HUB_POOL => pool_data,
        HUB_WARNING => warning_data,
        _ => json!({"oneOf": [job_data, run_data, pool_data, warning_data, run_detail_data]}),
    };
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["eventId", "name", "timestamp", "data", "_meta"],
        "properties": {
            "eventId": {
                "type": "string",
                "description": "hub:<sequence>; the same monotonic cursor spans every run, job, and pool event."
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
