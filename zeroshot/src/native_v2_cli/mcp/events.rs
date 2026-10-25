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
use super::super::{NativeV2CliCommand, RunWatchCommand};

pub(super) const EXTENSION_ID: &str = "io.modelcontextprotocol/events";
const EVENT_NAME: &str = "run.event";
const SUBSCRIPTION_META: &str = "io.modelcontextprotocol/subscriptionId";
const FIRST_WAIT: Duration = Duration::from_secs(2);
const IDLE_WAIT: Duration = Duration::from_millis(50);
const DEFAULT_MAX_EVENTS: usize = 50;
const MAX_EVENTS_LIMIT: u64 = 1000;
const NEXT_POLL_MS: u64 = 1000;
const DRAIN_LIMIT: usize = 10_000;
const PHASES: &[&str] = &["admitted", "running", "stopping", "finished"];

pub(super) struct Prepared {
    stream: WatchStream,
    cursor: Option<String>,
    run: Option<Value>,
    replayed: Vec<Value>,
    filters: Filters,
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
    arguments: Value,
    cursor: Option<String>,
    max_events: usize,
    filters: Filters,
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
        "events": [{
            "name": EVENT_NAME,
            "description": "A durable run event from the same stream `zeroshot watch` reads. \
                Subscribe with runId and an optional target. Delivery is poll or push. \
                Signed webhooks (events/subscribe) are unsupported. maxAgeMs is accepted at \
                the subscription level but not applied because durable watch records carry no \
                occurrence time; the event timestamp is delivery time.",
            "delivery": ["poll", "push"],
            "inputSchema": input_schema(),
            "payloadSchema": payload_schema(),
        }]
    }))
}

pub(super) fn unsupported_webhook() -> EventError {
    EventError::Unsupported {
        feature: "deliveryMode",
        value: "webhook",
    }
}

pub(super) async fn poll(host: &impl McpHost, params: &Value) -> Result<Value, EventError> {
    let request = parse_subscription(params, true)?;
    let batch = collect(host, &request).await?;
    Ok(json!({
        "resultType": "complete",
        "events": batch.events,
        "cursor": cursor_value(&batch.cursor),
        "truncated": batch.truncated,
        "hasMore": batch.has_more,
        "nextPollMs": NEXT_POLL_MS,
    }))
}

pub(super) async fn prepare(host: &impl McpHost, params: &Value) -> Result<Prepared, EventError> {
    let request = parse_subscription(params, false)?;
    let command = watch_command(&request.arguments, request.cursor.as_deref())?;
    let mut stream = host.open_watch(&command).await.map_err(classify)?;
    let (cursor, run, replayed) = if request.cursor.is_none() {
        let drained = drain(&mut stream).await?;
        let run = drained.first().and_then(run_constants);
        (None, run, drained)
    } else {
        (request.cursor, None, Vec::new())
    };
    Ok(Prepared {
        stream,
        cursor,
        run,
        replayed,
        filters: request.filters,
    })
}

pub(super) async fn drive<W>(
    prepared: Prepared,
    id: Value,
    writer: Arc<Mutex<W>>,
    cancelled: Arc<AtomicBool>,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    let Prepared {
        mut stream,
        mut cursor,
        mut run,
        replayed,
        filters,
    } = prepared;
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
        },
    )
    .await?;
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
        emit_event(&writer, &id, &mut delivery, &event).await?;
    }
    delivery.replayed = false;
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
                        emit_event(&writer, &id, &mut delivery, &event).await?;
                    }
                }
            }
            _ = heartbeat.tick() => {
                write_heartbeat(&writer, &id, delivery.cursor).await?;
            }
        }
    }
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
    let Some(name) = name else {
        return Err(EventError::Invalid(McpFailure::invalid("name is required")));
    };
    if name != EVENT_NAME {
        return Err(EventError::NotFound);
    }
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
    let filters = parse_filters(
        arguments
            .as_object()
            .expect("arguments was validated as an object"),
    )?;
    Ok(PollRequest {
        arguments,
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

async fn collect(host: &impl McpHost, request: &PollRequest) -> Result<Batch, EventError> {
    let command = watch_command(&request.arguments, request.cursor.as_deref())?;
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
        if let Some(occurrence) =
            occurrence(&event, false, &request.filters, false).map_err(classify)?
        {
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

fn watch_command(arguments: &Value, cursor: Option<&str>) -> Result<RunWatchCommand, EventError> {
    let Some(map) = arguments.as_object() else {
        return Err(EventError::Invalid(McpFailure::invalid(
            "arguments must be an object",
        )));
    };
    reject_unknown(
        map,
        &[
            "runId",
            "target",
            "phases",
            "nodes",
            "terminalOnly",
            "verbose",
        ],
    )
    .map_err(EventError::Invalid)?;
    let mut argv = vec!["watch".to_owned()];
    if let Some(run_id) = optional_string(map, "runId").map_err(EventError::Invalid)? {
        argv.push(run_id);
    }
    if let Some(target) = optional_string(map, "target").map_err(EventError::Invalid)? {
        argv.push("--target".to_owned());
        argv.push(target);
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
    include_cursor: bool,
    filters: &Filters,
    replayed: bool,
) -> Result<Option<Value>, McpFailure> {
    let cursor = watch_cursor(event)?;
    let run_id = event.get("runId").and_then(Value::as_str).unwrap_or("run");
    let data = event_data(event, filters.verbose);
    let terminal = data.pointer("/status/phase").and_then(Value::as_str) == Some("finished");
    if !filters.accepts(&data, terminal) {
        return Ok(None);
    }
    let mut value = json!({
        "eventId": format!("{run_id}:{cursor}"),
        "name": EVENT_NAME,
        "timestamp": now_rfc3339(),
        "data": data,
        "summary": summary(&data),
        "replayed": replayed,
    });
    if include_cursor {
        value["cursor"] = json!(cursor);
    }
    Ok(Some(value))
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
        &["runId"],
        vec![
            ("runId", string_prop("Public run ID.")),
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
    schema["description"] = json!(
        "Subscription arguments. maxAgeMs is accepted but not applied because durable watch \
         records carry no occurrence time; the event timestamp is delivery time."
    );
    schema
}

fn payload_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["eventId", "name", "timestamp", "data", "summary", "replayed", "_meta"],
        "properties": {
            "eventId": {"type": "string"},
            "name": {"type": "string", "const": EVENT_NAME},
            "timestamp": {"type": "string", "format": "date-time"},
            "cursor": {
                "type": "string",
                "description": "Stream notification cursor; poll results return the next cursor separately."
            },
            "replayed": {"type": "boolean"},
            "data": {
                "type": "object",
                "description": "Trimmed watch record. Run constants are sent once on notifications/events/active.",
                "additionalProperties": false,
                "required": ["runId", "cursor", "status"],
                "properties": {
                    "runId": {"type": "string"},
                    "cursor": {"type": "string"},
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
            },
            "summary": {
                "type": "object",
                "additionalProperties": false,
                "required": ["phase", "node", "terminal", "reason"],
                "properties": {
                    "phase": {"type": ["string", "null"]},
                    "node": {"type": ["string", "null"]},
                    "terminal": {"type": "boolean"},
                    "reason": {"type": ["string", "null"]}
                }
            },
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
    let occurrence = match occurrence(event, true, delivery.filters, delivery.replayed) {
        Ok(occurrence) => occurrence,
        Err(failure) => return write_failure(writer, id, &failure).await,
    };
    *delivery.cursor = Some(event_cursor);
    let active = ActiveView {
        cursor: &*delivery.cursor,
        run: delivery.run.as_ref(),
        truncated: gap,
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
    write_notification(
        writer,
        "notifications/events/active",
        json!({
            "cursor": cursor_value(active.cursor),
            "run": active.run.cloned().unwrap_or(Value::Null),
            "truncated": active.truncated,
            "_meta": meta(id),
        }),
    )
    .await
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
