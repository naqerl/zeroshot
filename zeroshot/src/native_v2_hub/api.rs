//! The hub API the daemon serves: JSON over HTTP with bearer-token auth.
//!
//! Errors answer with the `zeroshot.error/v1` diagnostic as the body and the CLI exit code in
//! `x-zeroshot-exit-code`, so a remote client fails exactly like a local one. Observation and
//! stop commands stream NDJSON frames (`{"stdout": LINE}`, `{"stderr": LINE}`, then
//! `{"exit": CODE}`) from a `zeroshot` child process run on the hub host.

use std::process::Stdio;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{MatchedPath, Path, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;

use super::driver::hub_executable;
use super::error::HubError;
use super::eval::{AddCase, AddSuite, CaseQuery, CreateRun, ReportRequest, RunQuery};
use super::service::HubService;
use super::wire::{
    AddMember, AddPool, CreateSchedule, EnqueueJob, JobQuery, MemberQuery, StatsRequest,
    StreamFrame, StreamRequest, SubmitJob, UpdateMember,
};

/// Response header carrying the CLI exit code of an error.
pub(crate) const EXIT_CODE_HEADER: &str = "x-zeroshot-exit-code";
/// Largest accepted request body: a job with its uploaded files.
const BODY_LIMIT: usize = 16 * 1024 * 1024;

pub(crate) struct ApiState {
    pub service: Arc<HubService>,
    pub token: String,
    /// The daemon's latest heartbeat document.
    pub heartbeat: Arc<Mutex<Value>>,
    /// Program observation streams run; `None` is this executable (or
    /// `ZEROSHOT_HUB_EXECUTABLE`), like the hub's target driver.
    pub executable: Option<std::path::PathBuf>,
}

pub(crate) fn router(state: Arc<ApiState>) -> Router {
    let protected = Router::new()
        .route("/v1/hub/daemon", get(daemon))
        .route("/v1/hub/members", post(add_member))
        .route("/v1/hub/members/query", post(list_members))
        .route(
            "/v1/hub/members/{name}",
            patch(update_member).delete(remove_member),
        )
        .route("/v1/hub/pools", get(list_pools).post(add_pool))
        .route("/v1/hub/pools/types", get(pool_types))
        .route("/v1/hub/pools/{name}", delete(remove_pool))
        .route("/v1/hub/pools/{name}/resume", post(resume_pool))
        .route("/v1/hub/jobs", post(submit))
        .route("/v1/hub/jobs/query", post(ps))
        .route("/v1/hub/jobs/{id}", get(status).delete(dequeue))
        .route("/v1/hub/jobs/{id}/stream", post(stream))
        .route("/v1/hub/queue", post(enqueue))
        .route("/v1/hub/schedules", post(create_schedule))
        .route("/v1/hub/dispatch", post(dispatch))
        .route("/v1/hub/stats", post(stats))
        .route("/v1/hub/eval/suites", post(eval_suite_add).get(eval_suites))
        .route("/v1/hub/eval/cases", post(eval_case_add))
        .route("/v1/hub/eval/cases/query", post(eval_cases))
        .route("/v1/hub/eval/cases/{id}", get(eval_case))
        .route("/v1/hub/eval/runs", post(eval_run_create))
        .route("/v1/hub/eval/runs/query", post(eval_runs))
        .route("/v1/hub/eval/runs/{id}", get(eval_run))
        .route("/v1/hub/eval/runs/{id}/report", post(eval_report))
        .route("/metrics", get(metrics))
        .route_layer(middleware::from_fn_with_state(state.clone(), authorize));
    Router::new()
        .route("/v1/hub/health", get(health))
        .merge(protected)
        .fallback(delete(not_found).get(not_found).post(not_found))
        .layer(middleware::from_fn_with_state(state.clone(), observe))
        .layer(axum::extract::DefaultBodyLimit::max(BODY_LIMIT))
        .with_state(state)
}

/// The unauthenticated loopback listener of `--metrics-listen`: only `GET /metrics`.
pub(crate) fn metrics_router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/metrics", get(metrics))
        .fallback(get(not_found))
        .layer(middleware::from_fn_with_state(state.clone(), observe))
        .with_state(state)
}

/// Count every request by route template (never the concrete path), method, and status.
async fn observe(State(state): State<Arc<ApiState>>, request: Request, next: Next) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| "unmatched".to_owned(), |path| path.as_str().to_owned());
    let method = match request.method().as_str() {
        method @ ("GET" | "POST" | "PATCH" | "DELETE" | "PUT" | "HEAD") => method,
        _ => "other",
    }
    .to_owned();
    let started = std::time::Instant::now();
    let response = next.run(request).await;
    state.service.metrics().record_request(
        (&route, &method),
        response.status().as_u16(),
        started.elapsed().as_secs_f64(),
    );
    response
}

async fn metrics(State(state): State<Arc<ApiState>>) -> Response {
    let text = state.service.render_metrics().await;
    ([(header::CONTENT_TYPE, super::metrics::CONTENT_TYPE)], text).into_response()
}

/// Constant-time comparison, so response timing does not reveal how much of a token matched.
pub(crate) fn token_matches(expected: &str, presented: &str) -> bool {
    let (expected, presented) = (expected.as_bytes(), presented.as_bytes());
    if expected.len() != presented.len() {
        return false;
    }
    expected
        .iter()
        .zip(presented)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(str::trim)
}

async fn authorize(State(state): State<Arc<ApiState>>, request: Request, next: Next) -> Response {
    match bearer(request.headers()) {
        Some(token) if token_matches(&state.token, token) => next.run(request).await,
        presented => {
            let message = if presented.is_some() {
                "the bearer token does not match this hub's token"
            } else {
                "a bearer token is required"
            };
            let diagnostic = json!({
                "schema": "zeroshot.error/v1",
                "kind": "target",
                "code": "hub.unauthorized",
                "message": message,
                "path": null,
                "node": null,
                "details": {},
            });
            json_response(StatusCode::UNAUTHORIZED, &diagnostic, Some(1))
        }
    }
}

fn json_response(status: StatusCode, value: &Value, exit_code: Option<i32>) -> Response {
    let mut response = (status, axum::Json(value.clone())).into_response();
    if let Some(code) = exit_code {
        response
            .headers_mut()
            .insert(EXIT_CODE_HEADER, HeaderValue::from(code));
    }
    response
}

fn reply(result: Result<Value, HubError>) -> Response {
    match result {
        Ok(value) => json_response(StatusCode::OK, &value, None),
        Err(error) => error_response(&error),
    }
}

fn error_response(error: &HubError) -> Response {
    let status =
        StatusCode::from_u16(error.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    json_response(status, &error.diagnostic(), Some(error.exit_code()))
}

fn body<T: DeserializeOwned>(bytes: &Bytes) -> Result<T, HubError> {
    serde_json::from_slice(bytes)
        .map_err(|error| HubError::Usage(format!("invalid hub API request: {error}")))
}

async fn not_found() -> Response {
    error_response(&HubError::Usage("unknown hub API route".to_owned()))
}

async fn health() -> Response {
    json_response(
        StatusCode::OK,
        &json!({ "schema": "zeroshot.hub.health/v1", "ok": true }),
        None,
    )
}

async fn daemon(State(state): State<Arc<ApiState>>) -> Response {
    let heartbeat = state
        .heartbeat
        .lock()
        .map(|heartbeat| heartbeat.clone())
        .unwrap_or(Value::Null);
    json_response(StatusCode::OK, &heartbeat, None)
}

async fn add_member(State(state): State<Arc<ApiState>>, bytes: Bytes) -> Response {
    let result = async { state.service.add(body::<AddMember>(&bytes)?).await }.await;
    reply(result)
}

async fn list_members(State(state): State<Arc<ApiState>>, bytes: Bytes) -> Response {
    let result = async { state.service.list(body::<MemberQuery>(&bytes)?).await }.await;
    reply(result)
}

async fn update_member(
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
    bytes: Bytes,
) -> Response {
    let result = async {
        state
            .service
            .update(&name, body::<UpdateMember>(&bytes)?)
            .await
    }
    .await;
    reply(result)
}

async fn remove_member(State(state): State<Arc<ApiState>>, Path(name): Path<String>) -> Response {
    reply(state.service.remove(&name).await)
}

async fn add_pool(State(state): State<Arc<ApiState>>, bytes: Bytes) -> Response {
    let result = async { state.service.add_pool(body::<AddPool>(&bytes)?).await }.await;
    reply(result)
}

async fn list_pools(State(state): State<Arc<ApiState>>) -> Response {
    reply(Ok(state.service.list_pools().await))
}

async fn remove_pool(State(state): State<Arc<ApiState>>, Path(name): Path<String>) -> Response {
    reply(state.service.remove_pool(&name).await)
}

async fn pool_types(State(state): State<Arc<ApiState>>) -> Response {
    reply(state.service.pool_types().await)
}

async fn resume_pool(State(state): State<Arc<ApiState>>, Path(name): Path<String>) -> Response {
    reply(state.service.resume_pool(&name).await)
}

async fn submit(State(state): State<Arc<ApiState>>, bytes: Bytes) -> Response {
    let result = async {
        let request = body::<SubmitJob>(&bytes)?;
        state
            .service
            .submit(request.job, request.queue, request.files)
            .await
    }
    .await;
    reply(result)
}

async fn enqueue(State(state): State<Arc<ApiState>>, bytes: Bytes) -> Response {
    let result = async {
        let request = body::<EnqueueJob>(&bytes)?;
        state.service.enqueue(request.job, request.files).await
    }
    .await;
    reply(result)
}

async fn create_schedule(State(state): State<Arc<ApiState>>, bytes: Bytes) -> Response {
    let result = async {
        let request = body::<CreateSchedule>(&bytes)?;
        state
            .service
            .create_schedule(request.schedule, request.files)
            .await
    }
    .await;
    reply(result)
}

async fn ps(State(state): State<Arc<ApiState>>, bytes: Bytes) -> Response {
    let result = async { state.service.ps(body::<JobQuery>(&bytes)?).await }.await;
    reply(result)
}

async fn status(State(state): State<Arc<ApiState>>, Path(id): Path<String>) -> Response {
    reply(state.service.status(&id).await)
}

async fn dequeue(State(state): State<Arc<ApiState>>, Path(id): Path<String>) -> Response {
    reply(state.service.dequeue(&id).await)
}

async fn dispatch(State(state): State<Arc<ApiState>>) -> Response {
    reply(state.service.pass().await.map(|(_, view)| view))
}

async fn stats(State(state): State<Arc<ApiState>>, bytes: Bytes) -> Response {
    let result = async { state.service.stats(body::<StatsRequest>(&bytes)?).await }.await;
    reply(result)
}

async fn eval_suite_add(State(state): State<Arc<ApiState>>, bytes: Bytes) -> Response {
    let result = async {
        state
            .service
            .eval_suite_add(body::<AddSuite>(&bytes)?)
            .await
    }
    .await;
    reply(result)
}

async fn eval_suites(State(state): State<Arc<ApiState>>) -> Response {
    reply(state.service.eval_suites().await)
}

async fn eval_case_add(State(state): State<Arc<ApiState>>, bytes: Bytes) -> Response {
    let result = async { state.service.eval_case_add(body::<AddCase>(&bytes)?).await }.await;
    reply(result)
}

async fn eval_cases(State(state): State<Arc<ApiState>>, bytes: Bytes) -> Response {
    let result = async { state.service.eval_cases(body::<CaseQuery>(&bytes)?).await }.await;
    reply(result)
}

async fn eval_case(State(state): State<Arc<ApiState>>, Path(id): Path<String>) -> Response {
    reply(state.service.eval_case(&id).await)
}

async fn eval_run_create(State(state): State<Arc<ApiState>>, bytes: Bytes) -> Response {
    let result = async {
        state
            .service
            .eval_run_create(body::<CreateRun>(&bytes)?)
            .await
    }
    .await;
    reply(result)
}

async fn eval_runs(State(state): State<Arc<ApiState>>, bytes: Bytes) -> Response {
    let result = async { state.service.eval_runs(body::<RunQuery>(&bytes)?).await }.await;
    reply(result)
}

async fn eval_run(State(state): State<Arc<ApiState>>, Path(id): Path<String>) -> Response {
    reply(state.service.eval_run(&id).await)
}

async fn eval_report(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    bytes: Bytes,
) -> Response {
    let result = async {
        state
            .service
            .eval_report(&id, body::<ReportRequest>(&bytes)?)
            .await
    }
    .await;
    reply(result)
}

async fn stream(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<String>,
    bytes: Bytes,
) -> Response {
    let arguments = match body::<StreamRequest>(&bytes) {
        Ok(request) => state.service.observation(&id, request).await,
        Err(error) => Err(error),
    };
    let arguments = match arguments {
        Ok(arguments) => arguments,
        Err(error) => return error_response(&error),
    };
    let executable = match state.executable.clone().map_or_else(hub_executable, Ok) {
        Ok(executable) => executable,
        Err(error) => return error_response(&error),
    };
    let (sender, receiver) = mpsc::channel::<Bytes>(64);
    tokio::spawn(proxy(executable, arguments, sender));
    let frames = futures_util::stream::unfold(receiver, |mut receiver| async move {
        receiver
            .recv()
            .await
            .map(|frame| (Ok::<_, std::io::Error>(frame), receiver))
    });
    Response::builder()
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .body(Body::from_stream(frames))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

fn frame(frame: &StreamFrame) -> Bytes {
    let mut line = serde_json::to_vec(frame).unwrap_or_default();
    line.push(b'\n');
    Bytes::from(line)
}

/// Run one `zeroshot` command and forward its output lines until it exits or the client goes
/// away; a dropped client kills the child, which only ends the observation, never the run.
pub(crate) async fn proxy(
    executable: std::path::PathBuf,
    arguments: Vec<String>,
    sender: mpsc::Sender<Bytes>,
) {
    let spawned = Command::new(executable)
        .args(&arguments)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn();
    let mut child = match spawned {
        Ok(child) => child,
        Err(error) => {
            let _ = sender
                .send(frame(&StreamFrame::Stderr {
                    stderr: format!("zeroshot could not start on the hub host: {error}"),
                }))
                .await;
            let _ = sender.send(frame(&StreamFrame::Exit { exit: 1 })).await;
            return;
        }
    };
    let (Some(stdout), Some(stderr)) = (child.stdout.take(), child.stderr.take()) else {
        return;
    };
    let mut stdout = BufReader::new(stdout).lines();
    let mut stderr = BufReader::new(stderr).lines();
    let (mut stdout_open, mut stderr_open) = (true, true);
    while stdout_open || stderr_open {
        let next = tokio::select! {
            line = stdout.next_line(), if stdout_open => match line {
                Ok(Some(line)) => Some(StreamFrame::Stdout { stdout: line }),
                _ => { stdout_open = false; None }
            },
            line = stderr.next_line(), if stderr_open => match line {
                Ok(Some(line)) => Some(StreamFrame::Stderr { stderr: line }),
                _ => { stderr_open = false; None }
            },
            () = sender.closed() => return,
        };
        if let Some(next) = next {
            if sender.send(frame(&next)).await.is_err() {
                return;
            }
        }
    }
    let exit = tokio::select! {
        status = child.wait() => status.ok().and_then(|status| status.code()).unwrap_or(1),
        () = sender.closed() => return,
    };
    let _ = sender.send(frame(&StreamFrame::Exit { exit })).await;
}
