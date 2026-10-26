//! The CLI side of the hub API: every hub command except contexts, install, and `daemon run`
//! is a request to the selected hub context.

use std::io::Write;
use std::time::Duration;

use futures_util::StreamExt;
use reqwest::{Method, StatusCode};
use serde::Serialize;
use serde_json::{Value, json};
use zeroshot_engine::native_v2_cli::hub::{
    HubCommand, HubEnqueue, HubPoolAdd, HubPoolCommand, HubPs, HubStats, HubSubmit,
};

use super::api::EXIT_CODE_HEADER;
use super::clock::{now_ms, parse_duration_ms, parse_future_ms, parse_past_ms};
use super::context::Resolved;
use super::data::{DataFs, DataSettings};
use super::cron;
use super::error::HubError;
use super::jobs;
use super::model::{Job, PlacementSpec, Schedule, ScheduleState};
use super::pool::Provider;
use super::service::FILE_FLAGS;
use super::wire::{
    AddMember, AddPool, CreateSchedule, SetTagProfile, EnqueueJob, JobFile, JobQuery, MemberQuery,
    StatsRequest, StreamFrame, StreamRequest, SubmitJob, UpdateMember,
};
use super::write_json;

const WAIT_POLL: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Bound for one request; placement may discover and probe several targets.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);
/// Largest file uploaded with a job.
const FILE_LIMIT: u64 = 8 * 1024 * 1024;

pub(crate) struct HubClient {
    context: String,
    url: String,
    token: String,
    http: reqwest::Client,
}

impl HubClient {
    pub(crate) fn new(resolved: &Resolved) -> Result<Self, HubError> {
        let token = resolved.token.read(&resolved.name, &resolved.url)?;
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("zeroshot-hub/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|error| HubError::storage("HTTP client could not be initialized", error))?;
        Ok(Self {
            context: resolved.name.clone(),
            url: resolved.url.clone(),
            token,
            http,
        })
    }

    fn endpoint(&self, segments: &[&str]) -> Result<url::Url, HubError> {
        let mut url = url::Url::parse(&self.url)
            .map_err(|error| HubError::Usage(format!("invalid hub URL {}: {error}", self.url)))?;
        url.path_segments_mut()
            .map_err(|()| HubError::Usage(format!("invalid hub URL {}", self.url)))?
            .pop_if_empty()
            .extend(segments);
        Ok(url)
    }

    fn unreachable(&self, error: &reqwest::Error) -> HubError {
        let cause = if error.is_timeout() {
            "the request timed out".to_owned()
        } else if error.is_connect() {
            let mut cause = error.to_string();
            let mut source = std::error::Error::source(error);
            while let Some(inner) = source {
                cause = inner.to_string();
                source = inner.source();
            }
            cause
        } else {
            error.to_string()
        };
        HubError::Unreachable {
            context: self.context.clone(),
            url: self.url.clone(),
            cause,
        }
    }

    async fn send(
        &self,
        method: Method,
        segments: &[&str],
        body: Option<&(impl Serialize + Sync)>,
    ) -> Result<reqwest::Response, HubError> {
        let mut request = self
            .http
            .request(method, self.endpoint(segments)?)
            .bearer_auth(&self.token);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request
            .send()
            .await
            .map_err(|error| self.unreachable(&error))?;
        if response.status().is_success() {
            return Ok(response);
        }
        Err(self.failure(response).await)
    }

    async fn failure(&self, response: reqwest::Response) -> HubError {
        let status = response.status();
        let exit_code = response
            .headers()
            .get(EXIT_CODE_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok())
            .unwrap_or(1);
        let diagnostic = response.json::<Value>().await.ok();
        let message = diagnostic
            .as_ref()
            .and_then(|value| value.get("message"))
            .and_then(Value::as_str)
            .map_or_else(
                || format!("hub API answered HTTP {}", status.as_u16()),
                str::to_owned,
            );
        if status == StatusCode::UNAUTHORIZED {
            return HubError::Unauthorized {
                context: self.context.clone(),
                url: self.url.clone(),
                message,
            };
        }
        match diagnostic.filter(|value| value.get("schema") == Some(&json!("zeroshot.error/v1"))) {
            Some(diagnostic) => HubError::Remote {
                message,
                diagnostic,
                exit_code,
            },
            None => HubError::Unreachable {
                context: self.context.clone(),
                url: self.url.clone(),
                cause: format!("{message}; is this a zeroshot hub?"),
            },
        }
    }

    async fn call(
        &self,
        method: Method,
        segments: &[&str],
        body: Option<&(impl Serialize + Sync)>,
    ) -> Result<Value, HubError> {
        let request = async {
            let response = self.send(method, segments, body).await?;
            response
                .json::<Value>()
                .await
                .map_err(|error| HubError::Unreachable {
                    context: self.context.clone(),
                    url: self.url.clone(),
                    cause: format!("the hub answered with invalid JSON: {error}"),
                })
        };
        tokio::time::timeout(REQUEST_TIMEOUT, request)
            .await
            .unwrap_or_else(|_| {
                Err(HubError::Unreachable {
                    context: self.context.clone(),
                    url: self.url.clone(),
                    cause: "the request timed out".to_owned(),
                })
            })
    }

    pub(crate) async fn get(&self, segments: &[&str]) -> Result<Value, HubError> {
        self.call(Method::GET, segments, None::<&Value>).await
    }

    pub(crate) async fn post(
        &self,
        segments: &[&str],
        body: &(impl Serialize + Sync),
    ) -> Result<Value, HubError> {
        self.call(Method::POST, segments, Some(body)).await
    }

    /// Proxy an observation command: replay its stdout and stderr lines, return its exit code.
    /// Ctrl-C stops observing; the run continues.
    async fn stream(&self, job: &str, request: &StreamRequest) -> Result<i32, HubError> {
        let response = self
            .send(
                Method::POST,
                &["v1", "hub", "jobs", job, "stream"],
                Some(request),
            )
            .await?;
        let mut body = response.bytes_stream();
        let mut buffer = Vec::new();
        let interrupted = tokio::signal::ctrl_c();
        tokio::pin!(interrupted);
        loop {
            let chunk = tokio::select! {
                chunk = body.next() => chunk,
                _ = &mut interrupted => return Ok(130),
            };
            let Some(chunk) = chunk else {
                return Err(HubError::Unreachable {
                    context: self.context.clone(),
                    url: self.url.clone(),
                    cause: "the stream ended before the command finished".to_owned(),
                });
            };
            let chunk = chunk.map_err(|error| self.unreachable(&error))?;
            buffer.extend_from_slice(&chunk);
            while let Some(end) = buffer.iter().position(|byte| *byte == b'\n') {
                let line = buffer.drain(..=end).collect::<Vec<_>>();
                match serde_json::from_slice::<StreamFrame>(&line) {
                    Ok(StreamFrame::Stdout { stdout }) => {
                        let mut out = std::io::stdout().lock();
                        let _ = writeln!(out, "{stdout}");
                        let _ = out.flush();
                    }
                    Ok(StreamFrame::Stderr { stderr }) => eprintln!("{stderr}"),
                    Ok(StreamFrame::Exit { exit }) => return Ok(exit),
                    Err(_) => {}
                }
            }
        }
    }
}

/// Execute one hub command against the selected hub.
pub(crate) async fn execute(
    client: &HubClient,
    command: HubCommand,
    output: &mut impl Write,
) -> Result<i32, HubError> {
    match command {
        HubCommand::Submit(request) => submit(client, *request, output).await,
        HubCommand::Eval(command) => super::eval::client::execute(client, command, output).await,
        HubCommand::Watch { .. }
        | HubCommand::Logs(_)
        | HubCommand::Attach { .. }
        | HubCommand::ForceStop { .. } => stream_command(client, command).await,
        HubCommand::Daemon(_)
        | HubCommand::Install(_)
        | HubCommand::Uninstall(_)
        | HubCommand::Context(_) => Err(local_only()),
        command => {
            let document = request_document(client, command).await?;
            write_json(output, &document)?;
            Ok(0)
        }
    }
}

fn local_only() -> HubError {
    HubError::Usage("this hub command runs on this machine, not through a hub context".to_owned())
}

/// Follow one placed job's event, log, attach, or force-stop stream.
async fn stream_command(client: &HubClient, command: HubCommand) -> Result<i32, HubError> {
    let (job, request) = match command {
        HubCommand::Watch { job, after } => (job, StreamRequest::Watch { after }),
        HubCommand::Logs(logs) => (
            logs.job,
            StreamRequest::Logs {
                after: logs.after,
                execution: logs.execution,
            },
        ),
        HubCommand::Attach { job, execution } => (job, StreamRequest::Attach { execution }),
        HubCommand::ForceStop { job } => (job, StreamRequest::ForceStop),
        _ => return Err(local_only()),
    };
    client.stream(&job, &request).await
}

/// Send one request/response hub command and return its JSON document.
async fn request_document(client: &HubClient, command: HubCommand) -> Result<Value, HubError> {
    match command {
        HubCommand::DaemonStatus => {
            let heartbeat = client.get(&["v1", "hub", "daemon"]).await?;
            Ok(json!({
                "schema": "zeroshot.hub.daemon-status/v1",
                "context": client.context,
                "url": client.url,
                "reachable": true,
                "daemon": heartbeat,
            }))
        }
        HubCommand::Add(_)
        | HubCommand::Update(_)
        | HubCommand::Remove { .. }
        | HubCommand::List { .. } => member_document(client, command).await,
        HubCommand::Enqueue(request) => enqueue(client, *request).await,
        HubCommand::Ps(HubPs {
            state,
            target,
            labels,
            limit,
        }) => {
            let query = JobQuery {
                state,
                target,
                labels,
                limit,
            };
            client.post(&["v1", "hub", "jobs", "query"], &query).await
        }
        HubCommand::Status { job } => client.get(&["v1", "hub", "jobs", &job]).await,
        HubCommand::Dequeue { job } => {
            client
                .call(Method::DELETE, &["v1", "hub", "jobs", &job], None::<&Value>)
                .await
        }
        HubCommand::Dispatch => client.post(&["v1", "hub", "dispatch"], &json!({})).await,
        HubCommand::Stats(request) => client.post(&["v1", "hub", "stats"], &stats(request)?).await,
        HubCommand::Pool(command) => pool_document(client, command).await,
        _ => Err(local_only()),
    }
}

/// Add, update, remove, or list hub members.
async fn member_document(client: &HubClient, command: HubCommand) -> Result<Value, HubError> {
    match command {
        HubCommand::Add(request) => {
            let body = AddMember {
                name: request.name,
                tags: request.tags,
                slots: request.slots,
                priority: request.priority,
            };
            client.post(&["v1", "hub", "members"], &body).await
        }
        HubCommand::Update(request) => {
            let body = UpdateMember {
                add_tags: request.add_tags,
                remove_tags: request.remove_tags,
                slots: request.slots,
                priority: request.priority,
                state: request.state.map(Into::into),
            };
            client
                .call(
                    Method::PATCH,
                    &["v1", "hub", "members", &request.name],
                    Some(&body),
                )
                .await
        }
        HubCommand::Remove { name } => {
            client
                .call(
                    Method::DELETE,
                    &["v1", "hub", "members", &name],
                    None::<&Value>,
                )
                .await
        }
        HubCommand::List { tags, probe } => {
            client
                .post(
                    &["v1", "hub", "members", "query"],
                    &MemberQuery { tags, probe },
                )
                .await
        }
        _ => Err(local_only()),
    }
}

/// Read the files a job's run arguments name, relative to the job's invoking folder, so a
/// hub on another machine can run it.
pub(crate) fn job_files(job: &Job) -> Result<Vec<JobFile>, HubError> {
    let mut files: Vec<JobFile> = Vec::new();
    for pair in job.run_args.windows(2) {
        let (flag, argument) = (&pair[0], &pair[1]);
        if !FILE_FLAGS.contains(&flag.as_str())
            || files.iter().any(|file| &file.argument == argument)
        {
            continue;
        }
        let path = job.cwd.join(argument);
        let size = std::fs::metadata(&path)
            .map_err(|error| {
                HubError::Usage(format!(
                    "{flag} {} could not be read: {error}",
                    path.display()
                ))
            })?
            .len();
        if size > FILE_LIMIT {
            return Err(HubError::Usage(format!(
                "{flag} {} is larger than {} MiB",
                path.display(),
                FILE_LIMIT / 1024 / 1024
            )));
        }
        let content = std::fs::read_to_string(&path).map_err(|error| {
            HubError::Usage(format!(
                "{flag} {} could not be read: {error}",
                path.display()
            ))
        })?;
        files.push(JobFile {
            argument: argument.clone(),
            content,
        });
    }
    Ok(files)
}

async fn submit(
    client: &HubClient,
    request: HubSubmit,
    output: &mut impl Write,
) -> Result<i32, HubError> {
    let started = now_ms();
    let job = jobs::new_job(&request.job, started)?;
    let files = job_files(&job)?;
    let deadline = match &request.wait {
        None => None,
        Some(None) => Some(u64::MAX),
        Some(Some(duration)) => Some(started.saturating_add(parse_duration_ms(duration)?)),
    };
    let body = SubmitJob {
        job,
        queue: request.queue,
        files,
    };
    let document = loop {
        match client.post(&["v1", "hub", "jobs"], &body).await {
            Ok(document) => break document,
            Err(HubError::Remote { diagnostic, .. })
                if deadline.is_some()
                    && diagnostic.get("code") == Some(&json!("hub.no_capacity")) =>
            {
                let deadline = deadline.unwrap_or(u64::MAX);
                if now_ms() >= deadline {
                    return Err(HubError::WaitTimeout(super::placement::describe(
                        &body.job.placement,
                    )));
                }
                let remaining = Duration::from_millis(deadline.saturating_sub(now_ms()));
                tokio::time::sleep(remaining.min(WAIT_POLL)).await;
            }
            Err(error) => return Err(error),
        }
    };
    // A job the hub parked for a pool is already accepted: waiting reads it back instead of
    // submitting it again.
    let document = match deadline {
        Some(deadline) if !placed(&document) => {
            wait_for_placement(client, &document, deadline, &body.job.placement).await?
        }
        _ => document,
    };
    write_json(output, &document)?;
    let running = document
        .pointer("/job/runId")
        .and_then(Value::as_str)
        .is_some();
    if request.detach || !running {
        return Ok(0);
    }
    let job_id = document
        .pointer("/job/jobId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    client
        .stream(&job_id, &StreamRequest::Watch { after: None })
        .await
}

/// Whether the hub has placed the job on a member.
fn placed(document: &Value) -> bool {
    document
        .pointer("/job/runId")
        .and_then(Value::as_str)
        .is_some()
        || document.pointer("/job/state").and_then(Value::as_str) == Some("running")
}

/// `--wait` for a job the hub parked: read it back until it is placed, or fail once the
/// deadline passes or the job ends without ever being placed.
async fn wait_for_placement(
    client: &HubClient,
    document: &Value,
    deadline: u64,
    placement: &PlacementSpec,
) -> Result<Value, HubError> {
    let job_id = document
        .pointer("/job/jobId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    loop {
        if now_ms() >= deadline {
            return Err(HubError::WaitTimeout(super::placement::describe(placement)));
        }
        let remaining = Duration::from_millis(deadline.saturating_sub(now_ms()));
        tokio::time::sleep(remaining.min(WAIT_POLL)).await;
        let observed = client.get(&["v1", "hub", "jobs", &job_id]).await?;
        if placed(&observed) {
            return Ok(observed);
        }
        let ended = observed
            .pointer("/job/state")
            .and_then(Value::as_str)
            .is_some_and(|state| matches!(state, "succeeded" | "failed" | "dequeued" | "expired"));
        if ended {
            return Err(HubError::WaitTimeout(super::placement::describe(placement)));
        }
    }
}

async fn enqueue(client: &HubClient, request: HubEnqueue) -> Result<Value, HubError> {
    if request.cron.is_some() {
        let (schedule, files) = schedule(request)?;
        return client
            .post(
                &["v1", "hub", "schedules"],
                &CreateSchedule { schedule, files },
            )
            .await;
    }
    let now = now_ms();
    let mut job = jobs::new_job(&request.job, now)?;
    job.priority = request.priority;
    job.not_before_ms = request
        .at
        .as_deref()
        .map(|at| parse_future_ms(at, now))
        .transpose()?;
    let files = job_files(&job)?;
    client
        .post(&["v1", "hub", "queue"], &EnqueueJob { job, files })
        .await
}

/// Build a recurring schedule; the cron zone defaults to this machine's zone.
pub(crate) fn schedule(request: HubEnqueue) -> Result<(Schedule, Vec<JobFile>), HubError> {
    let now = now_ms();
    let expression = request.cron.unwrap_or_default();
    let parsed = cron::parse(&expression)?;
    let (timezone, zone) = cron::timezone(request.cron_timezone.as_deref())?;
    let next_fire_ms = cron::next_fire(&parsed, &zone, now)?;
    let mut template = jobs::new_job(&request.job, now)?;
    template.priority = request.priority;
    let repin_from = (request.job.repository.is_none() && request.job.revision.is_none())
        .then(|| jobs::working_folder(&request.job))
        .transpose()?;
    let schedule_id = format!("hs_{}", uuid::Uuid::now_v7().simple());
    template.job_id.clone_from(&schedule_id);
    let files = job_files(&template)?;
    Ok((
        Schedule {
            schedule_id,
            state: ScheduleState::Active,
            cron: expression,
            timezone,
            submission_key: template.submission_key.take(),
            template,
            repin_from,
            created_at_ms: now,
            next_fire_ms: Some(next_fire_ms),
            last_fired_ms: None,
            last_job_id: None,
            fired: 0,
            skipped: 0,
            removed_at_ms: None,
            error: None,
        },
        files,
    ))
}

fn stats(request: HubStats) -> Result<StatsRequest, HubError> {
    let now = now_ms();
    for dimension in &request.by {
        super::stats::Dimension::parse(dimension)?;
    }
    let prices = request
        .prices
        .as_ref()
        .map(|path| {
            let bytes = std::fs::read(path).map_err(|error| {
                HubError::Usage(format!(
                    "--prices {} could not be read: {error}",
                    path.display()
                ))
            })?;
            serde_json::from_slice::<Value>(&bytes).map_err(|error| {
                HubError::Usage(format!(
                    "--prices {} is not a price table: {error}",
                    path.display()
                ))
            })
        })
        .transpose()?;
    Ok(StatsRequest {
        by: request.by,
        since_ms: request
            .since
            .as_deref()
            .map(|since| parse_past_ms(since, now))
            .transpose()?,
        until_ms: request
            .until
            .as_deref()
            .map(|until| parse_past_ms(until, now))
            .transpose()?,
        prices,
    })
}

/// Add, list, or remove on-demand server pools.
async fn pool_document(client: &HubClient, command: HubPoolCommand) -> Result<Value, HubError> {
    match command {
        HubPoolCommand::Add(request) => {
            let body = add_pool(*request)?;
            client.post(&["v1", "hub", "pools"], &body).await
        }
        HubPoolCommand::List => client.get(&["v1", "hub", "pools"]).await,
        HubPoolCommand::Types => client.get(&["v1", "hub", "pools", "types"]).await,
        HubPoolCommand::Resume { name } => {
            client
                .post(&["v1", "hub", "pools", &name, "resume"], &json!({}))
                .await
        }
        HubPoolCommand::TagSet(request) => {
            let request = *request;
            let body = SetTagProfile {
                server_types: request.server_types,
                locations: request.locations,
                min_cores: request.min_cores,
                min_memory_gb: request.min_memory_gb,
                slots: request.slots,
                priority: request.priority,
            };
            client
                .call(
                    Method::PUT,
                    &["v1", "hub", "pools", &request.pool, "tags", &request.tag],
                    Some(&body),
                )
                .await
        }
        HubPoolCommand::TagList { name } => {
            client.get(&["v1", "hub", "pools", &name, "tags"]).await
        }
        HubPoolCommand::TagRemove { name, tag } => {
            client
                .call(
                    Method::DELETE,
                    &["v1", "hub", "pools", &name, "tags", &tag],
                    None::<&Value>,
                )
                .await
        }
        HubPoolCommand::Remove { name } => {
            client
                .call(
                    Method::DELETE,
                    &["v1", "hub", "pools", &name],
                    None::<&Value>,
                )
                .await
        }
    }
}

/// The `POST /v1/hub/pools` body: durations parsed, the key path made absolute. The key is a
/// path on the hub host; only its public half ever leaves it.
pub(crate) fn add_pool(request: HubPoolAdd) -> Result<AddPool, HubError> {
    let provider = match request.provider.as_str() {
        "hetzner" => Provider::Hetzner,
        other => {
            return Err(HubError::Usage(format!(
                "unknown pool provider {other:?}; supported: hetzner"
            )));
        }
    };
    Ok(AddPool {
        name: request.name,
        provider,
        server_types: request.server_types,
        locations: request.locations,
        multi_arch: request.multi_arch,
        wait_max_ms: request
            .wait_max
            .as_deref()
            .map(parse_duration_ms)
            .transpose()?,
        image: request.image,
        target_image: request.target_image,
        max: request.max,
        slots: request.slots,
        idle_ms: parse_duration_ms(&request.idle)?,
        tags: request.tags,
        ssh_key: super::paths::absolute(&request.ssh_key)?,
        ssh_from: request.ssh_from,
        data: DataSettings {
            fs: DataFs::parse(&request.data_fs)?,
            size_gb: request.data_size_gb,
            volume_gb: request.data_volume_gb,
        },
    })
}
