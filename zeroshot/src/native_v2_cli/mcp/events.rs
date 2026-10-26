use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde_json::{Map, Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::io::AsyncWrite;
use tokio::sync::Mutex;

use super::args::{
    bool_prop, object_schema, optional_string, reject_unknown, string_array_prop, string_prop,
};
use super::host::{parse_cli, McpFailure, McpHost, WatchStream};
use super::super::hub::{HubCommand, HubRequest};
use super::super::{NativeV2CliCommand, RunWatchCommand};

pub(super) const EXTENSION_ID: &str = "io.modelcontextprotocol/events";
const EVENT_NAME: &str = "run.event";
const WARNING_NAME: &str = "run.warning";
const HOUR_MS: u64 = 60 * 60 * 1000;
const SUBSCRIPTION_META: &str = "io.modelcontextprotocol/subscriptionId";
const FIRST_WAIT: Duration = Duration::from_secs(2);
const IDLE_WAIT: Duration = Duration::from_millis(50);
const DEFAULT_MAX_EVENTS: usize = 50;
const MAX_EVENTS_LIMIT: u64 = 1000;
const NEXT_POLL_MS: u64 = 1000;
const DRAIN_LIMIT: usize = 10_000;
const PHASES: &[&str] = &["admitted", "running", "stopping", "finished"];
/// The hub's built-in local member is the default local backend, not a named target.
const LOCAL_HUB_MEMBER: &str = "local";

pub(super) struct Prepared {
    source: Source,
    subscription: Subscription,
}

struct Subscription {
    filters: Filters,
    name: EventName,
    job_id: Option<String>,
    run_id: Option<String>,
}

struct WatchRequest<'a> {
    request: &'a PollRequest,
    run_id: &'a str,
    target: Option<&'a str>,
    job_id: Option<String>,
    started_at_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EventName {
    Run,
    Warning,
}

impl EventName {
    fn parse(name: &str) -> Option<Self> {
        match name {
            EVENT_NAME => Some(Self::Run),
            WARNING_NAME => Some(Self::Warning),
            _ => None,
        }
    }
}

enum Source {
    Watch {
        stream: WatchStream,
        cursor: Option<String>,
        run: Option<Value>,
        replayed: Vec<Value>,
        started_at_ms: Option<u64>,
    },
    Discovery(Discovery),
}

struct Discovery {
    job_id: String,
    target: Option<String>,
    job: JobSnapshot,
}

pub(super) enum EventError {
    Invalid(McpFailure),
    NotFound,
    Unsupported {
        feature: &'static str,
        value: &'static str,
    },
    Failed(McpFailure),
}

struct PollRequest {
    name: EventName,
    selector: Selector,
    target: Option<String>,
    cursor: Option<String>,
    max_events: usize,
    filters: Filters,
}

#[derive(Clone)]
enum Selector {
    Run(String),
    Job(String),
}

struct JobSnapshot {
    view: Value,
    run_id: Option<String>,
    target: Option<String>,
    terminal: bool,
    error_kind: Value,
    started_at_ms: Option<u64>,
}

#[derive(Clone, Default)]
struct Filters {
    phases: Option<Vec<String>>,
    nodes: Option<Vec<String>>,
    terminal_only: bool,
    verbose: bool,
}

impl Filters {
    fn accepts(&self, data: &Value, terminal: bool) -> bool {
        if self.terminal_only && !terminal {
            return false;
        }
        if let Some(phases) = &self.phases {
            let phase = data.pointer("/status/phase").and_then(Value::as_str);
            if !phase.is_some_and(|phase| phases.iter().any(|allowed| allowed == phase)) {
                return false;
            }
        }
        if let Some(nodes) = &self.nodes {
            // A terminal record carries no active executions, so a node filter would
            // otherwise swallow the one event that tells the caller the run ended.
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
    truncated: bool,
    has_more: bool,
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
    Ok(json!({
        "resultType": "complete",
        "events": [
            {
                "name": EVENT_NAME,
                "description": "A durable run event from the same stream `zeroshot watch` reads. \
                    Subscribe with exactly one of runId or jobId and an optional target. Delivery is \
                    poll or push. Signed webhooks (events/subscribe) are unsupported. maxAgeMs is \
                    accepted at the subscription level but not applied because durable watch records \
                    carry no occurrence time; the event timestamp is delivery time.",
                "delivery": ["poll", "push"],
                "inputSchema": input_schema(),
                "payloadSchema": payload_schema(),
            },
            {
                "name": WARNING_NAME,
                "description": "A liveness warning for a run that is still active. Subscribe with \
                    exactly one of runId or jobId and an optional target. One warning is pushed for \
                    each completed hour of run time while the subscription is active; earlier hours \
                    are never replayed, and warnings stop at the terminal phase.",
                "delivery": ["push"],
                "inputSchema": input_schema(),
                "payloadSchema": payload_schema(),
            }
        ]
    }))
}

pub(super) fn requests_subscription(params: &Value) -> bool {
    params
        .get("arguments")
        .is_some_and(|arguments| !arguments.is_null())
}

pub(super) fn unsupported_webhook() -> EventError {
    EventError::Unsupported {
        feature: "deliveryMode",
        value: "webhook",
    }
}

pub(super) async fn poll(host: &impl McpHost, params: &Value) -> Result<Value, EventError> {
    let request = parse_subscription(params, true)?;
    if request.name == EventName::Warning {
        let job = match request.selector.clone() {
            Selector::Job(job_id) => Some(job_status(host, &job_id).await?.view),
            Selector::Run(_) => None,
        };
        let mut result = json!({
            "resultType": "complete",
            "events": [],
            "cursor": Value::Null,
            "truncated": false,
            "hasMore": false,
            "nextPollMs": NEXT_POLL_MS,
        });
        if let Some(job) = job {
            result["job"] = job;
        }
        return Ok(result);
    }
    let (batch, job) = match request.selector.clone() {
        Selector::Run(run_id) => {
            let batch = collect(host, &request, &run_id, request.target.as_deref()).await?;
            (batch, None)
        }
        Selector::Job(job_id) => {
            let snapshot = job_status(host, &job_id).await?;
            if let Some(run_id) = snapshot.run_id.clone() {
                let target = request.target.clone().or_else(|| snapshot.target.clone());
                let batch = collect(host, &request, &run_id, target.as_deref()).await?;
                (batch, Some(snapshot.view))
            } else {
                (
                    Batch {
                        events: Vec::new(),
                        cursor: None,
                        truncated: false,
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

pub(super) async fn prepare(host: &impl McpHost, params: &Value) -> Result<Prepared, EventError> {
    let request = parse_subscription(params, false)?;
    match request.selector.clone() {
        Selector::Run(run_id) => {
            let started_at_ms = run_started_at_ms(&run_id);
            prepare_watch(
                host,
                WatchRequest {
                    request: &request,
                    run_id: &run_id,
                    target: request.target.as_deref(),
                    job_id: None,
                    started_at_ms,
                },
            )
            .await
        }
        Selector::Job(job_id) => {
            let snapshot = job_status(host, &job_id).await?;
            if let Some(run_id) = snapshot.run_id.clone() {
                let target = request.target.clone().or_else(|| snapshot.target.clone());
                let started_at_ms = snapshot
                    .started_at_ms
                    .or_else(|| run_started_at_ms(&run_id));
                prepare_watch(
                    host,
                    WatchRequest {
                        request: &request,
                        run_id: &run_id,
                        target: target.as_deref(),
                        job_id: Some(job_id.clone()),
                        started_at_ms,
                    },
                )
                .await
            } else {
                let target = request.target.clone().or_else(|| snapshot.target.clone());
                Ok(Prepared {
                    source: Source::Discovery(Discovery {
                        job_id: job_id.clone(),
                        target,
                        job: snapshot,
                    }),
                    subscription: Subscription {
                        filters: request.filters,
                        name: request.name,
                        job_id: Some(job_id),
                        run_id: None,
                    },
                })
            }
        }
    }
}

async fn prepare_watch(
    host: &impl McpHost,
    watch: WatchRequest<'_>,
) -> Result<Prepared, EventError> {
    let WatchRequest {
        request,
        run_id,
        target,
        job_id,
        started_at_ms,
    } = watch;
    let command = watch_command(run_id, target, request.cursor.as_deref())?;
    let mut stream = host.open_watch(&command).await.map_err(classify)?;
    let (cursor, run, replayed) = if request.cursor.is_none() {
        let drained = drain(&mut stream).await?;
        let run = drained.first().and_then(run_constants);
        (None, run, drained)
    } else {
        (request.cursor.clone(), None, Vec::new())
    };
    Ok(Prepared {
        source: Source::Watch {
            stream,
            cursor,
            run,
            replayed,
            started_at_ms,
        },
        subscription: Subscription {
            filters: request.filters.clone(),
            name: request.name,
            job_id,
            run_id: Some(run_id.to_owned()),
        },
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

struct WatchState {
    stream: WatchStream,
    cursor: Option<String>,
    run: Option<Value>,
    replayed: Vec<Value>,
    started_at_ms: Option<u64>,
}

#[cfg(test)]
pub(super) fn warning_test_prepared(
    stream: WatchStream,
    run_id: &str,
    job_id: Option<&str>,
    elapsed_ms: u64,
) -> Prepared {
    let now = now_ms();
    Prepared {
        source: Source::Watch {
            stream,
            cursor: None,
            run: None,
            replayed: Vec::new(),
            started_at_ms: Some(now.saturating_sub(elapsed_ms)),
        },
        subscription: Subscription {
            filters: Filters::default(),
            name: EventName::Warning,
            job_id: job_id.map(str::to_owned),
            run_id: Some(run_id.to_owned()),
        },
    }
}

struct WarningState {
    job_id: Option<String>,
    run_id: String,
    started_at_ms: u64,
    elapsed_at_start_ms: u64,
    next_hour: u64,
    since: tokio::time::Instant,
}

impl WarningState {
    fn new(job_id: Option<String>, run_id: String, now_ms: u64, started_at_ms: u64) -> Self {
        let elapsed = now_ms.saturating_sub(started_at_ms);
        Self {
            job_id,
            run_id,
            started_at_ms,
            elapsed_at_start_ms: elapsed,
            next_hour: elapsed / HOUR_MS + 1,
            since: tokio::time::Instant::now(),
        }
    }

    fn elapsed_ms(&self) -> u64 {
        let since = u64::try_from(self.since.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.elapsed_at_start_ms.saturating_add(since)
    }

    fn wait(&self) -> Duration {
        let elapsed = self.elapsed_ms();
        let boundary = self.next_hour.saturating_mul(HOUR_MS);
        Duration::from_millis(boundary.saturating_sub(elapsed).max(1))
    }

    fn due(&mut self) -> Vec<u64> {
        let completed = self.elapsed_ms() / HOUR_MS;
        let mut due = Vec::new();
        while self.next_hour <= completed {
            due.push(self.next_hour);
            self.next_hour = self.next_hour.saturating_add(1);
        }
        due
    }
}

pub(super) async fn drive<W, H>(
    prepared: Prepared,
    output: StreamOutput<W>,
    host: Rc<H>,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin + Send,
    H: McpHost + 'static,
{
    let Prepared {
        source,
        subscription,
    } = prepared;
    match source {
        Source::Watch {
            stream,
            cursor,
            run,
            replayed,
            started_at_ms,
        } => {
            let state = WatchState {
                stream,
                cursor,
                run,
                replayed,
                started_at_ms,
            };
            drive_watch(state, subscription, output).await
        }
        Source::Discovery(discovery) => {
            drive_discovery(host, discovery, subscription, output).await
        }
    }
}

async fn drive_watch<W>(
    state: WatchState,
    subscription: Subscription,
    output: StreamOutput<W>,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    let Subscription {
        filters,
        name,
        job_id,
        run_id,
    } = subscription;
    let WatchState {
        mut stream,
        mut cursor,
        mut run,
        replayed,
        started_at_ms,
    } = state;
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
            run: run.as_ref(),
            truncated: false,
            job: None,
        },
    )
    .await?;
    let warning = name == EventName::Warning;
    let mut last_phase = None;
    let mut delivery = Delivery {
        cursor: &mut cursor,
        run: &mut run,
        filters: &filters,
        replayed: true,
    };
    for event in replayed {
        if cancelled.load(Ordering::Relaxed) {
            return Ok(());
        }
        if warning {
            if let Ok(event_cursor) = watch_cursor(&event) {
                *delivery.cursor = Some(event_cursor);
            }
            if delivery.run.is_none() {
                *delivery.run = run_constants(&event);
            }
            last_phase = phase(&event).map(str::to_owned);
        } else {
            emit_event(&writer, &id, &mut delivery, &event).await?;
        }
    }
    delivery.replayed = false;
    if warning && last_phase.as_deref() == Some("finished") {
        write_result(&writer, &id).await?;
        return Ok(());
    }
    let mut warnings = warning.then(|| {
        WarningState::new(
            job_id,
            run_id.unwrap_or_else(|| "run".to_owned()),
            now_ms(),
            started_at_ms.unwrap_or_else(now_ms),
        )
    });
    let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    loop {
        if cancelled.load(Ordering::Relaxed) {
            return Ok(());
        }
        tokio::select! {
            biased;
            item = stream.recv() => {
                if cancelled.load(Ordering::Relaxed) {
                    return Ok(());
                }
                match item {
                    None => {
                        write_result(&writer, &id).await?;
                        return Ok(());
                    }
                    Some(Err(failure)) => {
                        write_failure(&writer, &id, &failure).await?;
                        write_result(&writer, &id).await?;
                        return Ok(());
                    }
                    Some(Ok(event)) => {
                        if warning {
                            if let Ok(event_cursor) = watch_cursor(&event) {
                                *delivery.cursor = Some(event_cursor);
                            }
                            if delivery.run.is_none() {
                                *delivery.run = run_constants(&event);
                            }
                            last_phase = phase(&event).map(str::to_owned);
                            if last_phase.as_deref() == Some("finished") {
                                write_result(&writer, &id).await?;
                                return Ok(());
                            }
                        } else {
                            emit_event(&writer, &id, &mut delivery, &event).await?;
                        }
                    }
                }
            }
            _ = heartbeat.tick() => {
                write_heartbeat(&writer, &id, delivery.cursor).await?;
            }
            _ = warning_due(&mut warnings) => {
                if cancelled.load(Ordering::Relaxed) {
                    return Ok(());
                }
                let phase = last_phase.clone().unwrap_or_else(|| "running".to_owned());
                if phase == "finished" {
                    continue;
                }
                if let Some(warnings) = warnings.as_mut() {
                    for hours in warnings.due() {
                        write_warning(
                            &writer,
                            &id,
                            Warning {
                                job_id: warnings.job_id.as_deref(),
                                run_id: &warnings.run_id,
                                started_at_ms: warnings.started_at_ms,
                                hours,
                                phase: &phase,
                            },
                        )
                        .await?;
                    }
                }
            }
        }
    }
}

async fn warning_due(warnings: &mut Option<WarningState>) {
    match warnings {
        Some(warnings) => {
            let wait = warnings.wait();
            tokio::time::sleep(wait).await;
        }
        None => std::future::pending().await,
    }
}

async fn drive_discovery<W, H>(
    host: Rc<H>,
    discovery: Discovery,
    subscription: Subscription,
    output: StreamOutput<W>,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin + Send,
    H: McpHost + 'static,
{
    let Subscription {
        filters,
        name,
        job_id: subscription_job_id,
        run_id: _,
    } = subscription;
    let StreamOutput {
        id,
        writer,
        cancelled,
    } = output;
    let Discovery {
        job_id,
        mut target,
        mut job,
    } = discovery;
    let mut sequence = 0_u64;
    let mut cursor = Some(next_discovery_cursor(&mut sequence));
    if cancelled.load(Ordering::Relaxed) {
        return Ok(());
    }
    write_active(
        &writer,
        &id,
        &ActiveView {
            cursor: &cursor,
            run: None,
            truncated: false,
            job: Some(&job.view),
        },
    )
    .await?;
    if job.terminal {
        let terminal_cursor = next_discovery_cursor(&mut sequence);
        if name == EventName::Run {
            write_discovery_terminal(
                &writer,
                &id,
                DiscoveryTerminal {
                    job_id: &job_id,
                    job: &job,
                    cursor: &terminal_cursor,
                },
            )
            .await?;
        }
        write_result(&writer, &id).await?;
        return Ok(());
    }
    let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let mut poll = tokio::time::interval(Duration::from_millis(NEXT_POLL_MS));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    poll.tick().await;
    loop {
        if cancelled.load(Ordering::Relaxed) {
            return Ok(());
        }
        tokio::select! {
            biased;
            _ = heartbeat.tick() => {
                write_heartbeat(&writer, &id, &cursor).await?;
            }
            _ = poll.tick() => {
                if cancelled.load(Ordering::Relaxed) {
                    return Ok(());
                }
                let snapshot = match job_status(host.as_ref(), &job_id).await {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        return finish_stream_error(&writer, &id, error).await;
                    }
                };
                if target.is_none() {
                    target = snapshot.target.clone();
                }
                if let Some(run_id) = snapshot.run_id.clone() {
                    let command = match watch_command(&run_id, target.as_deref(), None) {
                        Ok(command) => command,
                        Err(error) => {
                            return finish_stream_error(&writer, &id, error).await;
                        }
                    };
                    let stream = match host.open_watch(&command).await {
                        Ok(stream) => stream,
                        Err(failure) => {
                            write_failure(&writer, &id, &failure).await?;
                            write_result(&writer, &id).await?;
                            return Ok(());
                        }
                    };
                    let state = WatchState {
                        stream,
                        cursor: None,
                        run: None,
                        replayed: Vec::new(),
                        started_at_ms: snapshot
                            .started_at_ms
                            .or_else(|| run_started_at_ms(&run_id)),
                    };
                    return drive_watch(
                        state,
                        Subscription {
                            filters,
                            name,
                            job_id: subscription_job_id,
                            run_id: Some(run_id),
                        },
                        StreamOutput::new(id, writer, cancelled),
                    )
                    .await;
                }
                if snapshot.terminal {
                    let terminal_cursor = next_discovery_cursor(&mut sequence);
                    if name == EventName::Run {
                        write_discovery_terminal(
                            &writer,
                            &id,
                            DiscoveryTerminal {
                                job_id: &job_id,
                                job: &snapshot,
                                cursor: &terminal_cursor,
                            },
                        )
                        .await?;
                    }
                    write_result(&writer, &id).await?;
                    return Ok(());
                }
                if snapshot.view != job.view {
                    job = snapshot;
                    cursor = Some(next_discovery_cursor(&mut sequence));
                    write_active(
                        &writer,
                        &id,
                        &ActiveView {
                            cursor: &cursor,
                            run: None,
                            truncated: false,
                            job: Some(&job.view),
                        },
                    )
                    .await?;
                }
            }
        }
    }
}

async fn finish_stream_error<W>(
    writer: &Mutex<W>,
    id: &Value,
    error: EventError,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    match error {
        EventError::Invalid(failure) | EventError::Failed(failure) => {
            write_failure(writer, id, &failure).await?;
        }
        other => {
            let (_, _, data) = rpc_error(other);
            let failure = McpFailure {
                code: data
                    .get("code")
                    .and_then(Value::as_str)
                    .unwrap_or("event.error")
                    .to_owned(),
                message: data
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("event stream failed")
                    .to_owned(),
            };
            write_failure(writer, id, &failure).await?;
        }
    }
    write_result(writer, id).await
}

fn next_discovery_cursor(sequence: &mut u64) -> String {
    *sequence = sequence.saturating_add(1);
    format!("v2:{sequence}")
}

struct DiscoveryTerminal<'a> {
    job_id: &'a str,
    job: &'a JobSnapshot,
    cursor: &'a str,
}

async fn write_discovery_terminal<W>(
    writer: &Mutex<W>,
    id: &Value,
    terminal: DiscoveryTerminal<'_>,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let DiscoveryTerminal {
        job_id,
        job,
        cursor,
    } = terminal;
    let mut data = job.view.clone();
    let summary = json!({
        "phase": Value::Null,
        "node": Value::Null,
        "terminal": true,
        "reason": job.error_kind,
    });
    insert_envelope_data(&mut data, cursor, &summary);
    write_notification(
        writer,
        "notifications/events/event",
        json!({
            "eventId": format!("{job_id}:{cursor}"),
            "name": EVENT_NAME,
            "timestamp": now_rfc3339(),
            "data": data,
            "_meta": meta(id),
        }),
    )
    .await
}

struct Warning<'a> {
    job_id: Option<&'a str>,
    run_id: &'a str,
    started_at_ms: u64,
    hours: u64,
    phase: &'a str,
}

async fn write_warning<W>(
    writer: &Mutex<W>,
    id: &Value,
    warning: Warning<'_>,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let Warning {
        job_id,
        run_id,
        started_at_ms,
        hours,
        phase,
    } = warning;
    let unit = if hours == 1 { "hour" } else { "hours" };
    let mut data = json!({
        "runId": run_id,
        "hours": hours,
        "startedAt": rfc3339_ms(started_at_ms),
        "phase": phase,
        "message": format!("run has been active for {hours} {unit}"),
        "summary": {
            "phase": phase,
            "node": Value::Null,
            "terminal": false,
            "reason": Value::Null,
        },
        "replayed": false,
    });
    if let Some(job_id) = job_id {
        data["jobId"] = json!(job_id);
    }
    write_notification(
        writer,
        "notifications/events/event",
        json!({
            "eventId": format!("{run_id}:warning:{hours}"),
            "name": WARNING_NAME,
            "timestamp": now_rfc3339(),
            "data": data,
            "_meta": meta(id),
        }),
    )
    .await
}

pub(super) fn rpc_error(error: EventError) -> (i64, &'static str, Value) {
    match error {
        EventError::Invalid(failure) => (-32602, "Invalid params", failure_data(failure)),
        EventError::NotFound => (-32023, "NotFound", json!({"kind": "event"})),
        EventError::Unsupported { feature, value } => (
            -32026,
            "Unsupported",
            json!({"feature": feature, "value": value}),
        ),
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
    let Some(map) = params.as_object() else {
        return Err(EventError::Invalid(McpFailure::invalid(
            "params must be an object",
        )));
    };
    let allowed: &[&str] = if capped {
        &["name", "arguments", "cursor", "maxAgeMs", "maxEvents"]
    } else {
        &["name", "arguments", "cursor", "maxAgeMs"]
    };
    reject_unknown(map, allowed).map_err(EventError::Invalid)?;
    let name = optional_string(map, "name").map_err(EventError::Invalid)?;
    let name = match name.as_deref() {
        None => EventName::Run,
        Some(name) => EventName::parse(name).ok_or(EventError::NotFound)?,
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
        _ => {
            return Err(EventError::Invalid(McpFailure::invalid(
                "exactly one of runId or jobId is required",
            )));
        }
    };
    let target = optional_string(arguments, "target").map_err(EventError::Invalid)?;
    let filters = parse_filters(arguments)?;
    Ok(PollRequest {
        name,
        selector,
        target,
        cursor,
        max_events,
        filters,
    })
}

fn parse_filters(arguments: &Map<String, Value>) -> Result<Filters, EventError> {
    reject_unknown(
        arguments,
        &[
            "runId",
            "jobId",
            "target",
            "phases",
            "nodes",
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
    Ok(Filters {
        phases,
        nodes,
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
        truncated: false,
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
    let mut truncated = false;
    let mut waiting = true;
    loop {
        if events.len() == request.max_events {
            let extra = next_event(stream, IDLE_WAIT).await?;
            return Ok(Batch {
                events,
                cursor: Some(cursor),
                truncated,
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
        if skipped(&cursor, &event_cursor) {
            truncated = true;
        }
        cursor = event_cursor;
        if let Some(occurrence) = occurrence(&event, &request.filters, false).map_err(classify)? {
            events.push(occurrence);
        }
    }
    Ok(Batch {
        events,
        cursor: Some(cursor),
        truncated,
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
        terminal: run_id.is_none() && job_is_terminal(&view),
        error_kind: view.get("errorKind").cloned().unwrap_or(Value::Null),
        started_at_ms: view
            .get("dispatchedAt")
            .and_then(Value::as_str)
            .and_then(parse_rfc3339_ms),
        view,
        run_id,
    })
}

fn parse_rfc3339_ms(text: &str) -> Option<u64> {
    let time = OffsetDateTime::parse(text, &Rfc3339).ok()?;
    let nanos = time.unix_timestamp_nanos();
    let millis = nanos.checked_div(1_000_000)?;
    u64::try_from(millis).ok()
}

fn invalid_hub_response(error: serde_json::Error) -> EventError {
    EventError::Failed(McpFailure {
        code: "hub.invalid_response".to_owned(),
        message: format!("hub job status was not JSON: {error}"),
    })
}

fn job_is_terminal(view: &Value) -> bool {
    view.get("state")
        .and_then(Value::as_str)
        .is_some_and(|state| matches!(state, "succeeded" | "failed" | "dequeued"))
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

fn run_constants(event: &Value) -> Option<Value> {
    let run_id = event.get("runId")?.as_str()?;
    let mut run = Map::new();
    run.insert("runId".to_owned(), json!(run_id));
    for key in ["title", "size", "source"] {
        run.insert(
            key.to_owned(),
            event.get(key).cloned().unwrap_or(Value::Null),
        );
    }
    Some(Value::Object(run))
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
    insert_envelope_data(&mut data, &cursor, &summary);
    data["replayed"] = json!(replayed);
    Ok(Some(json!({
        "eventId": format!("{run_id}:{cursor}"),
        "name": EVENT_NAME,
        "timestamp": now_rfc3339(),
        "data": data,
        "_meta": json!({}),
    })))
}

fn insert_envelope_data(data: &mut Value, cursor: &str, summary: &Value) {
    if let Some(map) = data.as_object_mut() {
        map.insert("cursor".to_owned(), json!(cursor));
        map.insert("summary".to_owned(), summary.clone());
        map.entry("replayed".to_owned()).or_insert(json!(false));
    }
}

fn phase(event: &Value) -> Option<&str> {
    event.pointer("/status/phase").and_then(Value::as_str)
}

fn run_started_at_ms(run_id: &str) -> Option<u64> {
    let timestamp = uuid::Uuid::parse_str(run_id).ok()?.get_timestamp()?;
    let (seconds, nanos) = timestamp.to_unix();
    seconds
        .checked_mul(1000)?
        .checked_add(u64::from(nanos / 1_000_000))
}

fn rfc3339_ms(ms: u64) -> String {
    let nanos = i128::from(ms).checked_mul(1_000_000);
    nanos
        .and_then(|nanos| time::OffsetDateTime::from_unix_timestamp_nanos(nanos).ok())
        .and_then(|time| {
            time.format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .unwrap_or_default()
}

fn skipped(previous: &str, next: &str) -> bool {
    match (cursor_sequence(previous), cursor_sequence(next)) {
        (Some(earlier), Some(later)) => later > earlier.saturating_add(1),
        _ => false,
    }
}

fn cursor_sequence(cursor: &str) -> Option<u64> {
    let rest = cursor.strip_prefix("v2:")?;
    if rest.is_empty() || !rest.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    rest.parse().ok()
}

fn cursor_value(cursor: &Option<String>) -> Value {
    match cursor {
        Some(cursor) => Value::String(cursor.clone()),
        None => Value::Null,
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or_default()
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

fn input_schema() -> Value {
    let mut schema = object_schema(
        &[],
        vec![
            ("runId", string_prop("Public run ID.")),
            (
                "jobId",
                string_prop(
                    "Hub job ID. A job that has a run behaves like that runId; a queued, scheduled, or placing job is followed until it has a run or reaches a terminal state.",
                ),
            ),
            (
                "target",
                string_prop("Named target. Omit for the local controller."),
            ),
            (
                "phases",
                string_array_prop(
                    "Only emit events whose status phase is one of admitted, running, stopping, \
                     or finished.",
                ),
            ),
            (
                "nodes",
                string_array_prop(
                    "Only emit events with an active execution at one of these graph nodes.",
                ),
            ),
            (
                "terminalOnly",
                bool_prop("Only emit terminal (finished) events."),
            ),
            (
                "verbose",
                bool_prop("Keep the full recorded failure array on terminal events."),
            ),
        ],
    );
    schema["oneOf"] = json!([{"required": ["runId"]}, {"required": ["jobId"]}]);
    schema["description"] = json!(
        "Subscription arguments. Provide exactly one of runId or jobId. A jobId may be combined \
         with target. maxAgeMs is accepted but not applied because durable watch records carry \
         no occurrence time; the event timestamp is delivery time."
    );
    schema
}

fn payload_schema() -> Value {
    let summary_schema = json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["phase", "node", "terminal", "reason"],
        "properties": {
            "phase": {"type": ["string", "null"]},
            "node": {"type": ["string", "null"]},
            "terminal": {"type": "boolean"},
            "reason": {"type": ["string", "null"]}
        }
    });
    let run_data = json!({
        "type": "object",
        "description": "Trimmed watch record. Run constants are sent once on notifications/events/active.",
        "additionalProperties": false,
        "required": ["runId", "cursor", "status", "summary", "replayed"],
        "properties": {
            "runId": {"type": "string"},
            "cursor": {"type": "string"},
            "summary": summary_schema,
            "replayed": {"type": "boolean"},
            "status": {
                "type": "object",
                "description": "Public run status. status.metadata is present only when phase is finished.",
                "additionalProperties": true,
                "properties": {
                    "phase": {
                        "type": "string",
                        "enum": PHASES
                    },
                    "activeExecutions": {"type": "array"},
                    "terminalResult": {
                        "type": "object",
                        "description": "On failed terminal events context.recorded is {reason,lastError,errorCount} unless verbose is true."
                    },
                    "metadata": {"type": "object"}
                }
            }
        }
    });
    let job_data = json!({
        "type": "object",
        "description": "Hub job state while a queued, scheduled, or placing job is followed before it has a run, and on its terminal event if it never runs.",
        "additionalProperties": true,
        "required": ["jobId", "state", "summary", "replayed"],
        "properties": {
            "jobId": {"type": "string"},
            "state": {
                "type": "string",
                "enum": ["queued", "scheduled", "running", "succeeded", "failed", "dequeued"]
            },
            "runId": {"type": ["string", "null"]},
            "target": {"type": ["string", "null"]},
            "error": {"type": ["string", "null"]},
            "errorKind": {"type": ["string", "null"]},
            "summary": summary_schema.clone(),
            "replayed": {"type": "boolean"}
        }
    });
    let warning_data = json!({
        "type": "object",
        "description": "One completed hour of run time for a still-active run.",
        "additionalProperties": false,
        "required": ["runId", "hours", "startedAt", "phase", "message", "summary", "replayed"],
        "properties": {
            "runId": {"type": "string"},
            "jobId": {"type": "string"},
            "hours": {"type": "integer", "minimum": 1},
            "startedAt": {"type": "string", "format": "date-time"},
            "phase": {"type": "string", "enum": PHASES},
            "message": {"type": "string"},
            "summary": summary_schema,
            "replayed": {"type": "boolean"}
        }
    });
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["eventId", "name", "timestamp", "data", "_meta"],
        "properties": {
            "eventId": {
                "type": "string",
                "description": "Run events use <runId>:<cursor>, discovery events <jobId>:<cursor>, and warnings <runId>:warning:<hours>."
            },
            "name": {"type": "string", "enum": [EVENT_NAME, WARNING_NAME]},
            "timestamp": {"type": "string", "format": "date-time"},
            "data": {"oneOf": [run_data, job_data, warning_data]},
            "_meta": {"type": "object"}
        }
    })
}

struct Delivery<'a> {
    cursor: &'a mut Option<String>,
    run: &'a mut Option<Value>,
    filters: &'a Filters,
    replayed: bool,
}

struct ActiveView<'a> {
    cursor: &'a Option<String>,
    run: Option<&'a Value>,
    truncated: bool,
    job: Option<&'a Value>,
}

async fn emit_event<W>(
    writer: &Mutex<W>,
    id: &Value,
    delivery: &mut Delivery<'_>,
    event: &Value,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let event_cursor = match watch_cursor(event) {
        Ok(cursor) => cursor,
        Err(failure) => return write_failure(writer, id, &failure).await,
    };
    let discovered = if delivery.run.is_none() {
        *delivery.run = run_constants(event);
        delivery.run.is_some()
    } else {
        false
    };
    let gap = delivery
        .cursor
        .as_deref()
        .is_some_and(|previous| skipped(previous, &event_cursor));
    let occurrence = match occurrence(event, delivery.filters, delivery.replayed) {
        Ok(occurrence) => occurrence,
        Err(failure) => return write_failure(writer, id, &failure).await,
    };
    *delivery.cursor = Some(event_cursor);
    let active = ActiveView {
        cursor: &*delivery.cursor,
        run: delivery.run.as_ref(),
        truncated: gap,
        job: None,
    };
    let Some(mut params) = occurrence else {
        return write_active(writer, id, &active).await;
    };
    if gap || discovered {
        write_active(writer, id, &active).await?;
    }
    params["_meta"] = meta(id);
    write_notification(writer, "notifications/events/event", params).await
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
        "truncated": active.truncated,
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

async fn write_failure<W>(
    writer: &Mutex<W>,
    id: &Value,
    failure: &McpFailure,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_notification(
        writer,
        "notifications/events/error",
        json!({
            "error": {
                "code": -32603,
                "message": failure.message,
                "data": {"code": failure.code}
            },
            "_meta": meta(id),
        }),
    )
    .await
}

async fn write_result<W>(writer: &Mutex<W>, id: &Value) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    super::serve::write_message(
        writer,
        &json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {"resultType": "complete", "_meta": meta(id)}
        }),
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
