use std::fs;
use std::io::Write;

use clap::CommandFactory;
use serde_json::{Map, Value, json};

use super::args::{
    bool_prop, inline_json_prop, inline_object_prop, integer_prop, object_schema, optional_string,
    reject_unknown, string_array_prop, string_prop,
};
use super::host::{parse_cli, McpFailure, McpHost};
use super::super::hub::HubCommand;
use super::super::{Cli, NativeV2CliCommand};

type ToolArgvResult = Result<Vec<String>, McpFailure>;
type ToolArgv = fn(&Map<String, Value>) -> ToolArgvResult;

struct Tool {
    name: &'static str,
    cli: &'static [&'static str],
    allowed: &'static [&'static str],
    schema: fn() -> Value,
    argv: ToolArgv,
}

const INLINE_KEYS: &[&str] = &[
    "input",
    "graph",
    "runtimeConfig",
    "uniformRuntimeConfig",
    "environment",
];

const RUN_KEYS: &[&str] = &[
    "title",
    "input",
    "taskFrom",
    "graph",
    "template",
    "runtimeConfig",
    "uniformRuntimeConfig",
    "environment",
    "noEnvironment",
    "profile",
    "target",
    "cwd",
    "sourceConnection",
    "repository",
    "branch",
    "revision",
    "submissionKey",
    "detach",
    "validateOnly",
    "delivery",
    "push",
    "pr",
    "ship",
    "noPrFeedback",
];

const SELECTOR_KEYS: &[&str] = &["runId", "target"];

const TOOLS: &[Tool] = &[
    Tool {
        name: "run",
        cli: &["run"],
        allowed: RUN_KEYS,
        schema: run_schema,
        argv: run_argv,
    },
    Tool {
        name: "status",
        cli: &["status"],
        allowed: SELECTOR_KEYS,
        schema: status_schema,
        argv: status_argv,
    },
    Tool {
        name: "logs",
        cli: &["logs"],
        allowed: &["runId", "target", "after", "execution"],
        schema: logs_schema,
        argv: logs_argv,
    },
    Tool {
        name: "watch",
        cli: &["watch"],
        allowed: &["runId", "target", "after"],
        schema: watch_schema,
        argv: watch_argv,
    },
    Tool {
        name: "list",
        cli: &["list"],
        allowed: &["target"],
        schema: list_schema,
        argv: list_argv,
    },
    Tool {
        name: "resume",
        cli: &["resume"],
        allowed: &["runId", "target", "fromCheckpoint"],
        schema: resume_schema,
        argv: resume_argv,
    },
    Tool {
        name: "force-stop",
        cli: &["force-stop"],
        allowed: SELECTOR_KEYS,
        schema: force_stop_schema,
        argv: force_stop_argv,
    },
    Tool {
        name: "report",
        cli: &["report"],
        allowed: SELECTOR_KEYS,
        schema: report_schema,
        argv: report_argv,
    },
    Tool {
        name: "hub_list",
        cli: &["hub", "list"],
        allowed: &["hub", "tag", "probe"],
        schema: hub_list_schema,
        argv: hub_list_argv,
    },
    Tool {
        name: "hub_ps",
        cli: &["hub", "ps"],
        allowed: &["hub", "state", "target", "label", "limit"],
        schema: hub_ps_schema,
        argv: hub_ps_argv,
    },
    Tool {
        name: "hub_status",
        cli: &["hub", "status"],
        allowed: &["hub", "job"],
        schema: hub_status_schema,
        argv: hub_status_argv,
    },
    Tool {
        name: "hub_submit",
        cli: &["hub", "submit"],
        allowed: HUB_SUBMIT_KEYS,
        schema: hub_submit_schema,
        argv: hub_submit_argv,
    },
    Tool {
        name: "hub_enqueue",
        cli: &["hub", "enqueue"],
        allowed: HUB_ENQUEUE_KEYS,
        schema: hub_enqueue_schema,
        argv: hub_enqueue_argv,
    },
    Tool {
        name: "hub_stats",
        cli: &["hub", "stats"],
        allowed: &["hub", "by", "since", "until", "prices"],
        schema: hub_stats_schema,
        argv: hub_stats_argv,
    },
    Tool {
        name: "hub_logs",
        cli: &["hub", "logs"],
        allowed: &["hub", "job", "after", "execution"],
        schema: hub_logs_schema,
        argv: hub_logs_argv,
    },
    Tool {
        name: "hub_daemon_status",
        cli: &["hub", "daemon", "status"],
        allowed: &["hub"],
        schema: hub_daemon_status_schema,
        argv: hub_daemon_status_argv,
    },
    Tool {
        name: "hub_dequeue",
        cli: &["hub", "dequeue"],
        allowed: &["job", "hub"],
        schema: hub_dequeue_schema,
        argv: hub_dequeue_argv,
    },
    Tool {
        name: "hub_add",
        cli: &["hub", "add"],
        allowed: &["name", "tag", "slots", "priority", "hub"],
        schema: hub_add_schema,
        argv: hub_add_argv,
    },
    Tool {
        name: "hub_update",
        cli: &["hub", "update"],
        allowed: &[
            "name",
            "addTag",
            "removeTag",
            "slots",
            "priority",
            "state",
            "hub",
        ],
        schema: hub_update_schema,
        argv: hub_update_argv,
    },
    Tool {
        name: "hub_remove",
        cli: &["hub", "remove"],
        allowed: &["name", "hub"],
        schema: hub_remove_schema,
        argv: hub_remove_argv,
    },
    Tool {
        name: "hub_dispatch",
        cli: &["hub", "dispatch"],
        allowed: &["hub"],
        schema: hub_dispatch_schema,
        argv: hub_dispatch_argv,
    },
    Tool {
        name: "hub_force_stop",
        cli: &["hub", "force-stop"],
        allowed: &["job", "hub"],
        schema: hub_force_stop_schema,
        argv: hub_force_stop_argv,
    },
    Tool {
        name: "hub_watch",
        cli: &["hub", "watch"],
        allowed: &["job", "after", "hub"],
        schema: hub_watch_schema,
        argv: hub_watch_argv,
    },
];

const HUB_SUBMIT_KEYS: &[&str] = &[
    "hub",
    "tag",
    "strategy",
    "label",
    "title",
    "input",
    "taskFrom",
    "graph",
    "template",
    "runtimeConfig",
    "uniformRuntimeConfig",
    "environment",
    "noEnvironment",
    "profile",
    "target",
    "cwd",
    "sourceConnection",
    "repository",
    "branch",
    "revision",
    "submissionKey",
    "detach",
    "delivery",
    "push",
    "pr",
    "ship",
    "noPrFeedback",
];

const HUB_ENQUEUE_KEYS: &[&str] = &[
    "hub",
    "tag",
    "strategy",
    "label",
    "at",
    "cron",
    "cronTz",
    "priority",
    "title",
    "input",
    "taskFrom",
    "graph",
    "template",
    "runtimeConfig",
    "uniformRuntimeConfig",
    "environment",
    "noEnvironment",
    "profile",
    "target",
    "cwd",
    "sourceConnection",
    "repository",
    "branch",
    "revision",
    "submissionKey",
    "delivery",
    "push",
    "pr",
    "ship",
    "noPrFeedback",
];

pub(super) fn list(params: &Value) -> Result<Value, McpFailure> {
    let cursor = match params {
        Value::Null => None,
        Value::Object(map) => {
            reject_unknown(map, &["cursor"])?;
            optional_string(map, "cursor")?
        }
        _ => return Err(McpFailure::invalid("params must be an object")),
    };
    if cursor.is_some() {
        return Ok(json!({"tools": []}));
    }
    let tools = TOOLS
        .iter()
        .map(|tool| {
            json!({
                "name": tool.name,
                "description": command_description(tool.cli),
                "inputSchema": (tool.schema)(),
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({"tools": tools}))
}

pub(super) async fn call(host: &impl McpHost, params: &Value) -> Result<Value, McpFailure> {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| McpFailure::invalid("tool name is required"))?;
    let Some(tool) = TOOLS.iter().find(|tool| tool.name == name) else {
        return Err(McpFailure::invalid(format!("unknown tool {name}")));
    };
    let mut arguments = arguments_object(params)?;
    reject_unknown(&arguments, tool.allowed)?;
    let materialized = materialize_inline(&mut arguments)?;
    let command = match command_for(name, (tool.argv)(&arguments)?) {
        Ok(command) => command,
        Err(failure) => return Ok(tool_error(&failure)),
    };
    let result = host.execute(command).await;
    drop(materialized);
    match result {
        Ok(stdout) => Ok(tool_success(stdout)),
        Err(failure) => Ok(tool_error(&failure)),
    }
}

pub(super) fn materialize_inline(
    arguments: &mut Map<String, Value>,
) -> Result<Option<tempfile::TempDir>, McpFailure> {
    let mut directory = None;
    for key in INLINE_KEYS {
        let Some(value) = arguments.get_mut(*key) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        if directory.is_none() {
            directory = Some(
                tempfile::Builder::new()
                    .prefix("zeroshot-mcp-")
                    .tempdir()
                    .map_err(|error| {
                        McpFailure::invalid(format!(
                            "could not create temporary directory: {error}"
                        ))
                    })?,
            );
        }
        let path = directory
            .as_ref()
            .expect("temporary directory exists")
            .path()
            .join(format!("{key}.json"));
        let bytes = serde_json::to_vec(&*value)
            .map_err(|error| McpFailure::invalid(format!("could not encode {key}: {error}")))?;
        write_private_inline_file(&path, &bytes).map_err(|error| {
            McpFailure::invalid(format!(
                "could not write inline {key} to a temporary file: {error}"
            ))
        })?;
        *value = Value::String(path.to_string_lossy().into_owned());
    }
    Ok(directory)
}

fn write_private_inline_file(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.flush()
}

fn arguments_object(params: &Value) -> Result<Map<String, Value>, McpFailure> {
    match params.get("arguments") {
        None | Some(Value::Null) => Ok(Map::new()),
        Some(Value::Object(map)) => Ok(map.clone()),
        Some(_) => Err(McpFailure::invalid("arguments must be an object")),
    }
}

fn command_for(name: &str, argv: Vec<String>) -> Result<NativeV2CliCommand, McpFailure> {
    let command = parse_cli(argv)?;
    if tool_matches(name, &command) {
        Ok(command)
    } else {
        Err(McpFailure::invalid(format!(
            "{name} did not parse as that command"
        )))
    }
}

fn tool_matches(name: &str, command: &NativeV2CliCommand) -> bool {
    match (name, command) {
        ("run", NativeV2CliCommand::Run(_))
        | ("status", NativeV2CliCommand::Status(_))
        | ("logs", NativeV2CliCommand::Logs(_))
        | ("watch", NativeV2CliCommand::Watch(_))
        | ("list", NativeV2CliCommand::List { .. })
        | ("resume", NativeV2CliCommand::Resume(_))
        | ("force-stop", NativeV2CliCommand::ForceStop(_))
        | ("report", NativeV2CliCommand::Report(_)) => true,
        ("hub_list", NativeV2CliCommand::Hub(request)) => {
            matches!(request.command, HubCommand::List { .. })
        }
        ("hub_ps", NativeV2CliCommand::Hub(request)) => {
            matches!(request.command, HubCommand::Ps(_))
        }
        ("hub_status", NativeV2CliCommand::Hub(request)) => {
            matches!(request.command, HubCommand::Status { .. })
        }
        ("hub_submit", NativeV2CliCommand::Hub(request)) => {
            matches!(request.command, HubCommand::Submit(_))
        }
        ("hub_enqueue", NativeV2CliCommand::Hub(request)) => {
            matches!(request.command, HubCommand::Enqueue(_))
        }
        ("hub_stats", NativeV2CliCommand::Hub(request)) => {
            matches!(request.command, HubCommand::Stats(_))
        }
        ("hub_logs", NativeV2CliCommand::Hub(request)) => {
            matches!(request.command, HubCommand::Logs(_))
        }
        ("hub_daemon_status", NativeV2CliCommand::Hub(request)) => {
            matches!(request.command, HubCommand::DaemonStatus)
        }
        ("hub_dequeue", NativeV2CliCommand::Hub(request)) => {
            matches!(request.command, HubCommand::Dequeue { .. })
        }
        ("hub_add", NativeV2CliCommand::Hub(request)) => {
            matches!(request.command, HubCommand::Add(_))
        }
        ("hub_update", NativeV2CliCommand::Hub(request)) => {
            matches!(request.command, HubCommand::Update(_))
        }
        ("hub_remove", NativeV2CliCommand::Hub(request)) => {
            matches!(request.command, HubCommand::Remove { .. })
        }
        ("hub_dispatch", NativeV2CliCommand::Hub(request)) => {
            matches!(request.command, HubCommand::Dispatch)
        }
        ("hub_force_stop", NativeV2CliCommand::Hub(request)) => {
            matches!(request.command, HubCommand::ForceStop { .. })
        }
        ("hub_watch", NativeV2CliCommand::Hub(request)) => {
            matches!(request.command, HubCommand::Watch { .. })
        }
        _ => false,
    }
}

fn tool_success(stdout: String) -> Value {
    let mut result = json!({
        "content": [{"type": "text", "text": stdout}],
        "isError": false,
    });
    if let Some(structured) = structured_output(&stdout) {
        result["structuredContent"] = structured;
    }
    result
}

fn tool_error(failure: &McpFailure) -> Value {
    json!({
        "content": [{"type": "text", "text": failure.message}],
        "structuredContent": {"code": failure.code, "message": failure.message},
        "isError": true,
    })
}

fn structured_output(stdout: &str) -> Option<Value> {
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        return Some(value);
    }
    let mut items = Vec::new();
    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        items.push(serde_json::from_str(line).ok()?);
    }
    if items.is_empty() {
        None
    } else {
        Some(Value::Array(items))
    }
}

fn command_description(path: &[&str]) -> String {
    let mut command = Cli::command();
    for name in path {
        let Some(child) = command.find_subcommand(name).cloned() else {
            return String::new();
        };
        command = child;
    }
    let about = styled(command.get_about());
    let long = styled(command.get_long_about());
    match (about.is_empty(), long.is_empty()) {
        (true, _) => long,
        (_, true) => about,
        (false, false) => format!("{about}\n\n{long}"),
    }
}

fn styled(text: Option<&clap::builder::StyledStr>) -> String {
    text.map(ToString::to_string)
        .unwrap_or_default()
        .trim()
        .to_owned()
}

struct RunFlagSet {
    detach: bool,
    validate_only: bool,
}

fn push_run_flags(
    argv: &mut Vec<String>,
    map: &Map<String, Value>,
    flags: RunFlagSet,
) -> Result<(), McpFailure> {
    push_opt(argv, map, "title", "--title")?;
    push_opt(argv, map, "input", "--input")?;
    push_opt(argv, map, "taskFrom", "--task-from")?;
    push_opt(argv, map, "graph", "--graph")?;
    push_opt(argv, map, "template", "--template")?;
    push_opt(argv, map, "runtimeConfig", "--runtime-config")?;
    push_opt(
        argv,
        map,
        "uniformRuntimeConfig",
        "--uniform-runtime-config",
    )?;
    push_opt(argv, map, "environment", "--environment")?;
    push_flag(argv, map, "noEnvironment", "--no-environment")?;
    push_opt(argv, map, "profile", "--profile")?;
    push_opt(argv, map, "target", "--target")?;
    push_opt(argv, map, "cwd", "--cwd")?;
    push_opt(argv, map, "sourceConnection", "--source-connection")?;
    push_opt(argv, map, "repository", "--repository")?;
    push_opt(argv, map, "branch", "--branch")?;
    push_opt(argv, map, "revision", "--revision")?;
    push_opt(argv, map, "submissionKey", "--submission-key")?;
    if flags.detach {
        push_flag(argv, map, "detach", "--detach")?;
    }
    if flags.validate_only {
        push_flag(argv, map, "validateOnly", "--validate-only")?;
    }
    push_opt(argv, map, "delivery", "--delivery")?;
    push_flag(argv, map, "push", "--push")?;
    push_flag(argv, map, "pr", "--pr")?;
    push_flag(argv, map, "ship", "--ship")?;
    push_flag(argv, map, "noPrFeedback", "--no-pr-feedback")?;
    Ok(())
}

fn push_opt(
    argv: &mut Vec<String>,
    map: &Map<String, Value>,
    key: &str,
    flag: &str,
) -> Result<(), McpFailure> {
    if let Some(value) = optional_string(map, key)? {
        argv.push(flag.to_owned());
        argv.push(value);
    }
    Ok(())
}

fn push_flag(
    argv: &mut Vec<String>,
    map: &Map<String, Value>,
    key: &str,
    flag: &str,
) -> Result<(), McpFailure> {
    match map.get(key) {
        None | Some(Value::Null) | Some(Value::Bool(false)) => Ok(()),
        Some(Value::Bool(true)) => {
            argv.push(flag.to_owned());
            Ok(())
        }
        Some(_) => Err(McpFailure::invalid(format!("{key} must be a boolean"))),
    }
}

fn push_repeat(
    argv: &mut Vec<String>,
    map: &Map<String, Value>,
    key: &str,
    flag: &str,
) -> Result<(), McpFailure> {
    let Some(value) = map.get(key) else {
        return Ok(());
    };
    let Value::Array(items) = value else {
        return Err(McpFailure::invalid(format!(
            "{key} must be an array of strings"
        )));
    };
    for item in items {
        let Value::String(item) = item else {
            return Err(McpFailure::invalid(format!(
                "{key} must be an array of strings"
            )));
        };
        argv.push(flag.to_owned());
        argv.push(item.clone());
    }
    Ok(())
}

fn push_i64(
    argv: &mut Vec<String>,
    map: &Map<String, Value>,
    key: &str,
    flag: &str,
) -> Result<(), McpFailure> {
    let Some(value) = map.get(key) else {
        return Ok(());
    };
    let Some(number) = value.as_i64() else {
        return Err(McpFailure::invalid(format!("{key} must be an integer")));
    };
    argv.push(flag.to_owned());
    argv.push(number.to_string());
    Ok(())
}

fn push_u64(
    argv: &mut Vec<String>,
    map: &Map<String, Value>,
    key: &str,
    flag: &str,
) -> Result<(), McpFailure> {
    let Some(value) = map.get(key) else {
        return Ok(());
    };
    let Some(number) = value.as_u64() else {
        return Err(McpFailure::invalid(format!(
            "{key} must be a non-negative integer"
        )));
    };
    argv.push(flag.to_owned());
    argv.push(number.to_string());
    Ok(())
}

fn push_selector(argv: &mut Vec<String>, map: &Map<String, Value>) -> Result<(), McpFailure> {
    if let Some(run_id) = optional_string(map, "runId")? {
        argv.push(run_id);
    }
    push_opt(argv, map, "target", "--target")
}

fn hub_prefix(map: &Map<String, Value>, subcommand: &[&str]) -> Result<Vec<String>, McpFailure> {
    let mut argv = vec!["hub".to_owned()];
    if let Some(hub) = optional_string(map, "hub")? {
        argv.push("--hub".to_owned());
        argv.push(hub);
    }
    argv.extend(subcommand.iter().map(|part| (*part).to_owned()));
    Ok(argv)
}

fn push_probe(argv: &mut Vec<String>, map: &Map<String, Value>) -> Result<(), McpFailure> {
    match map.get("probe") {
        None | Some(Value::Null) | Some(Value::Bool(true)) => Ok(()),
        Some(Value::Bool(false)) => {
            argv.push("--no-probe".to_owned());
            Ok(())
        }
        Some(_) => Err(McpFailure::invalid("probe must be a boolean")),
    }
}

fn push_placement(argv: &mut Vec<String>, map: &Map<String, Value>) -> Result<(), McpFailure> {
    push_repeat(argv, map, "tag", "--tag")?;
    push_opt(argv, map, "strategy", "--strategy")?;
    push_repeat(argv, map, "label", "--label")
}

fn run_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = vec!["run".to_owned()];
    push_run_flags(
        &mut argv,
        map,
        RunFlagSet {
            detach: true,
            validate_only: true,
        },
    )?;
    Ok(argv)
}

fn status_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = vec!["status".to_owned()];
    push_selector(&mut argv, map)?;
    Ok(argv)
}

fn logs_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = vec!["logs".to_owned()];
    push_selector(&mut argv, map)?;
    push_opt(&mut argv, map, "after", "--after")?;
    push_opt(&mut argv, map, "execution", "--execution")?;
    Ok(argv)
}

fn watch_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = vec!["watch".to_owned()];
    push_selector(&mut argv, map)?;
    push_opt(&mut argv, map, "after", "--after")?;
    Ok(argv)
}

fn list_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = vec!["list".to_owned()];
    push_opt(&mut argv, map, "target", "--target")?;
    Ok(argv)
}

fn resume_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = vec!["resume".to_owned()];
    push_selector(&mut argv, map)?;
    push_opt(&mut argv, map, "fromCheckpoint", "--from-checkpoint")?;
    Ok(argv)
}

fn force_stop_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = vec!["force-stop".to_owned()];
    push_selector(&mut argv, map)?;
    Ok(argv)
}

fn report_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = vec!["report".to_owned()];
    push_selector(&mut argv, map)?;
    Ok(argv)
}

fn hub_list_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = hub_prefix(map, &["list"])?;
    push_repeat(&mut argv, map, "tag", "--tag")?;
    push_probe(&mut argv, map)?;
    Ok(argv)
}

fn hub_ps_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = hub_prefix(map, &["ps"])?;
    push_opt(&mut argv, map, "state", "--state")?;
    push_opt(&mut argv, map, "target", "--target")?;
    push_repeat(&mut argv, map, "label", "--label")?;
    push_u64(&mut argv, map, "limit", "--limit")?;
    Ok(argv)
}

fn hub_status_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = hub_prefix(map, &["status"])?;
    if let Some(job) = optional_string(map, "job")? {
        argv.push(job);
    }
    Ok(argv)
}

fn hub_submit_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = hub_prefix(map, &["submit"])?;
    push_placement(&mut argv, map)?;
    push_run_flags(
        &mut argv,
        map,
        RunFlagSet {
            detach: true,
            validate_only: false,
        },
    )?;
    Ok(argv)
}

fn hub_enqueue_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = hub_prefix(map, &["enqueue"])?;
    push_placement(&mut argv, map)?;
    push_opt(&mut argv, map, "at", "--at")?;
    push_opt(&mut argv, map, "cron", "--cron")?;
    push_opt(&mut argv, map, "cronTz", "--cron-tz")?;
    push_i64(&mut argv, map, "priority", "--priority")?;
    push_run_flags(
        &mut argv,
        map,
        RunFlagSet {
            detach: false,
            validate_only: false,
        },
    )?;
    Ok(argv)
}

fn hub_stats_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = hub_prefix(map, &["stats"])?;
    push_repeat(&mut argv, map, "by", "--by")?;
    push_opt(&mut argv, map, "since", "--since")?;
    push_opt(&mut argv, map, "until", "--until")?;
    push_opt(&mut argv, map, "prices", "--prices")?;
    Ok(argv)
}

fn hub_logs_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = hub_prefix(map, &["logs"])?;
    if let Some(job) = optional_string(map, "job")? {
        argv.push(job);
    }
    push_opt(&mut argv, map, "after", "--after")?;
    push_opt(&mut argv, map, "execution", "--execution")?;
    Ok(argv)
}

fn hub_daemon_status_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    hub_prefix(map, &["daemon", "status"])
}

fn hub_dequeue_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = hub_prefix(map, &["dequeue"])?;
    if let Some(job) = optional_string(map, "job")? {
        argv.push(job);
    }
    Ok(argv)
}

fn hub_add_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = hub_prefix(map, &["add"])?;
    if let Some(name) = optional_string(map, "name")? {
        argv.push(name);
    }
    push_repeat(&mut argv, map, "tag", "--tag")?;
    push_u64(&mut argv, map, "slots", "--slots")?;
    push_i64(&mut argv, map, "priority", "--priority")?;
    Ok(argv)
}

fn hub_update_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = hub_prefix(map, &["update"])?;
    if let Some(name) = optional_string(map, "name")? {
        argv.push(name);
    }
    push_repeat(&mut argv, map, "addTag", "--add-tag")?;
    push_repeat(&mut argv, map, "removeTag", "--remove-tag")?;
    push_u64(&mut argv, map, "slots", "--slots")?;
    push_i64(&mut argv, map, "priority", "--priority")?;
    push_opt(&mut argv, map, "state", "--state")?;
    Ok(argv)
}

fn hub_remove_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = hub_prefix(map, &["remove"])?;
    if let Some(name) = optional_string(map, "name")? {
        argv.push(name);
    }
    Ok(argv)
}

fn hub_dispatch_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    hub_prefix(map, &["dispatch"])
}

fn hub_force_stop_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = hub_prefix(map, &["force-stop"])?;
    if let Some(job) = optional_string(map, "job")? {
        argv.push(job);
    }
    Ok(argv)
}

fn hub_watch_argv(map: &Map<String, Value>) -> Result<Vec<String>, McpFailure> {
    let mut argv = hub_prefix(map, &["watch"])?;
    if let Some(job) = optional_string(map, "job")? {
        argv.push(job);
    }
    push_opt(&mut argv, map, "after", "--after")?;
    Ok(argv)
}

#[cfg(test)]
pub(super) fn argv_for_test(
    name: &str,
    arguments: &Map<String, Value>,
) -> Result<Vec<String>, McpFailure> {
    let tool = TOOLS
        .iter()
        .find(|tool| tool.name == name)
        .ok_or_else(|| McpFailure::invalid(format!("unknown tool {name}")))?;
    reject_unknown(arguments, tool.allowed)?;
    let mut arguments = arguments.clone();
    let materialized = materialize_inline(&mut arguments)?;
    let argv = (tool.argv)(&arguments)?;
    drop(materialized);
    Ok(argv)
}

fn selector_properties() -> Vec<(&'static str, Value)> {
    vec![
        ("runId", string_prop("Public run ID.")),
        (
            "target",
            string_prop("Named target. Omit for the local controller."),
        ),
    ]
}

fn run_properties() -> Vec<(&'static str, Value)> {
    vec![
        (
            "title",
            string_prop("Human-readable title recorded with the run."),
        ),
        (
            "input",
            inline_json_prop("Inline JSON value for the graph's initial input."),
        ),
        (
            "taskFrom",
            string_prop(
                "Resolve a GitHub issue or pull-request URL into the run input. \
                Conflicts with input, repository, and sourceConnection.",
            ),
        ),
        (
            "graph",
            inline_object_prop("Inline graph specification JSON object."),
        ),
        (
            "template",
            string_prop("Built-in template: single-worker, software-change, or auto-research."),
        ),
        (
            "runtimeConfig",
            inline_object_prop("Inline exact runtime plan JSON object."),
        ),
        (
            "uniformRuntimeConfig",
            inline_object_prop(
                "Inline agent runtime JSON object expanded across every executable node.",
            ),
        ),
        (
            "environment",
            inline_object_prop("Inline environment definition JSON object."),
        ),
        (
            "noEnvironment",
            bool_prop("Send an empty environment and bypass the target default."),
        ),
        (
            "profile",
            string_prop("Profile name: NAME, local:NAME, user:NAME, or org:NAME."),
        ),
        ("target", string_prop("Named target. Omit to run locally.")),
        (
            "cwd",
            string_prop("Working folder. File arguments stay relative to the invoking folder."),
        ),
        (
            "sourceConnection",
            string_prop("Named connection for Git checkout and push."),
        ),
        (
            "repository",
            string_prop("GitHub owner/name or a credential-free Git URL. Requires target."),
        ),
        ("branch", string_prop("Source branch. Requires target.")),
        (
            "revision",
            string_prop("Exact source commit SHA. Requires target."),
        ),
        (
            "submissionKey",
            string_prop("Stable idempotency key for retrying submission."),
        ),
        (
            "detach",
            bool_prop("Return after submission instead of following run events."),
        ),
        (
            "validateOnly",
            bool_prop("Validate and materialize the run without submitting it."),
        ),
        ("delivery", string_prop("Template-owned delivery mode.")),
        (
            "push",
            bool_prop("Commit and push the managed branch without a pull request."),
        ),
        (
            "pr",
            bool_prop("Open a pull request for the software-change template."),
        ),
        (
            "ship",
            bool_prop("Merge delivery for the software-change template."),
        ),
        (
            "noPrFeedback",
            bool_prop("Do not route visible pull-request feedback through repair."),
        ),
    ]
}

fn placement_properties() -> Vec<(&'static str, Value)> {
    vec![
        (
            "hub",
            string_prop("Hub context. Omit for ZEROSHOT_HUB, then the current context."),
        ),
        (
            "tag",
            string_array_prop("Member tags that must all match. Repeatable."),
        ),
        (
            "strategy",
            string_prop("Choice among free members: first-free or least-loaded."),
        ),
        ("label", string_array_prop("Job labels KEY=VALUE.")),
    ]
}

fn run_schema() -> Value {
    object_schema(&["title"], run_properties())
}

fn status_schema() -> Value {
    object_schema(&["runId"], selector_properties())
}

fn logs_schema() -> Value {
    let mut properties = selector_properties();
    properties.push((
        "after",
        string_prop("Resume strictly after this durable cursor."),
    ));
    properties.push((
        "execution",
        string_prop("Return records only for this execution selector."),
    ));
    object_schema(&["runId"], properties)
}

fn watch_schema() -> Value {
    let mut properties = selector_properties();
    properties.push((
        "after",
        string_prop("Resume strictly after this durable cursor."),
    ));
    object_schema(&["runId"], properties)
}

fn list_schema() -> Value {
    object_schema(
        &[],
        vec![(
            "target",
            string_prop("Named target. Omit for the local controller."),
        )],
    )
}

fn resume_schema() -> Value {
    let mut properties = selector_properties();
    properties.push((
        "fromCheckpoint",
        string_prop("Checkpoint ID to restore before continuing."),
    ));
    object_schema(&["runId"], properties)
}

fn force_stop_schema() -> Value {
    object_schema(&["runId"], selector_properties())
}

fn report_schema() -> Value {
    object_schema(&["runId"], selector_properties())
}

fn hub_list_schema() -> Value {
    object_schema(
        &[],
        vec![
            (
                "hub",
                string_prop("Hub context. Omit for ZEROSHOT_HUB, then the current context."),
            ),
            (
                "tag",
                string_array_prop("Only members carrying every listed tag."),
            ),
            (
                "probe",
                bool_prop("Contact targets for health. false reports stored members only."),
            ),
        ],
    )
}

fn hub_ps_schema() -> Value {
    object_schema(
        &[],
        vec![
            (
                "hub",
                string_prop("Hub context. Omit for ZEROSHOT_HUB, then the current context."),
            ),
            (
                "state",
                string_prop("Job state filter. Defaults to active."),
            ),
            ("target", string_prop("Only jobs placed on this member.")),
            (
                "label",
                string_array_prop("Only jobs carrying every listed KEY=VALUE label."),
            ),
            (
                "limit",
                integer_prop("Maximum number of newest matching jobs."),
            ),
        ],
    )
}

fn hub_job_properties(extra: Vec<(&'static str, Value)>) -> Vec<(&'static str, Value)> {
    let mut properties = vec![
        (
            "hub",
            string_prop("Hub context. Omit for ZEROSHOT_HUB, then the current context."),
        ),
        ("job", string_prop("Hub job ID.")),
    ];
    properties.extend(extra);
    properties
}

fn hub_status_schema() -> Value {
    object_schema(&["job"], hub_job_properties(Vec::new()))
}

fn hub_logs_schema() -> Value {
    object_schema(
        &["job"],
        hub_job_properties(vec![
            (
                "after",
                string_prop("Resume strictly after this durable cursor."),
            ),
            (
                "execution",
                string_prop("Return records only for this execution selector."),
            ),
        ]),
    )
}

fn hub_submit_schema() -> Value {
    let mut properties = placement_properties();
    properties.extend(
        run_properties()
            .into_iter()
            .filter(|(name, _)| !matches!(*name, "validateOnly")),
    );
    object_schema(&["title"], properties)
}

fn hub_enqueue_schema() -> Value {
    let mut properties = placement_properties();
    properties.extend([
        (
            "at",
            string_prop("Not before this RFC 3339 time or relative +DURATION."),
        ),
        (
            "cron",
            string_prop("Five-field cron expression. Each fire queues one job."),
        ),
        ("cronTz", string_prop("IANA time zone for cron.")),
        (
            "priority",
            integer_prop("Higher priority jobs are placed first."),
        ),
    ]);
    properties.extend(
        run_properties()
            .into_iter()
            .filter(|(name, _)| !matches!(*name, "validateOnly" | "detach")),
    );
    object_schema(&["title"], properties)
}

fn hub_stats_schema() -> Value {
    object_schema(
        &[],
        vec![
            (
                "hub",
                string_prop("Hub context. Omit for ZEROSHOT_HUB, then the current context."),
            ),
            (
                "by",
                string_array_prop("Group by target, model, project, day, or label:KEY."),
            ),
            (
                "since",
                string_prop("Include jobs submitted at or after this time."),
            ),
            (
                "until",
                string_prop("Include jobs submitted before this time."),
            ),
            ("prices", string_prop("JSON price table file.")),
        ],
    )
}

fn hub_daemon_status_schema() -> Value {
    object_schema(
        &[],
        vec![(
            "hub",
            string_prop("Hub context. Omit for ZEROSHOT_HUB, then the current context."),
        )],
    )
}

fn hub_dequeue_schema() -> Value {
    object_schema(&["job"], hub_job_properties(Vec::new()))
}

fn hub_add_schema() -> Value {
    object_schema(
        &["name"],
        vec![
            (
                "hub",
                string_prop("Hub context. Omit for ZEROSHOT_HUB, then the current context."),
            ),
            (
                "name",
                string_prop("Registered target name, or `local` for the local controller."),
            ),
            (
                "tag",
                string_array_prop("Placement tag, such as gpu or project=citydiff. Repeatable."),
            ),
            (
                "slots",
                integer_prop("Concurrent runs the hub places on this member."),
            ),
            (
                "priority",
                integer_prop("Higher priority members are preferred by first-free placement."),
            ),
        ],
    )
}

fn hub_update_schema() -> Value {
    object_schema(
        &["name"],
        vec![
            (
                "hub",
                string_prop("Hub context. Omit for ZEROSHOT_HUB, then the current context."),
            ),
            ("name", string_prop("Member name.")),
            ("addTag", string_array_prop("Add this tag. Repeatable.")),
            (
                "removeTag",
                string_array_prop("Remove this tag. Repeatable."),
            ),
            (
                "slots",
                integer_prop("Concurrent runs the hub places on this member."),
            ),
            ("priority", integer_prop("Placement priority.")),
            ("state", string_prop("Member placement state.")),
        ],
    )
}

fn hub_remove_schema() -> Value {
    object_schema(
        &["name"],
        vec![
            (
                "hub",
                string_prop("Hub context. Omit for ZEROSHOT_HUB, then the current context."),
            ),
            ("name", string_prop("Member name.")),
        ],
    )
}

fn hub_dispatch_schema() -> Value {
    object_schema(
        &[],
        vec![(
            "hub",
            string_prop("Hub context. Omit for ZEROSHOT_HUB, then the current context."),
        )],
    )
}

fn hub_force_stop_schema() -> Value {
    object_schema(&["job"], hub_job_properties(Vec::new()))
}

fn hub_watch_schema() -> Value {
    object_schema(
        &["job"],
        hub_job_properties(vec![(
            "after",
            string_prop("Resume strictly after this durable cursor."),
        )]),
    )
}
