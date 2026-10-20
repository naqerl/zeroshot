use std::ffi::OsString;

use crate::native_v2_cli::hub::{
    HubApiOptions, HubCommand, HubContextCommand, HubDaemonRun, HubInstall, HubManager,
    HubMemberState, HubRequest, HubStrategy, HubTokenSource, HubUninstall,
};
use crate::native_v2_cli::{NativeV2CliCommand, NativeV2CliError, parse_native_v2_args};

fn request(values: &[&str]) -> Result<HubRequest, NativeV2CliError> {
    match parse_native_v2_args(values.iter().map(OsString::from))? {
        NativeV2CliCommand::Hub(request) => Ok(request),
        other => panic!("expected a hub command, got {other:?}"),
    }
}

fn parse(values: &[&str]) -> Result<HubCommand, NativeV2CliError> {
    request(values).map(|request| request.command)
}

#[test]
fn submit_keeps_run_arguments_and_separates_placement() {
    let command = parse(&[
        "hub",
        "submit",
        "--target",
        "box-1",
        "--tag",
        "gpu",
        "--tag",
        "eu",
        "--tag",
        "gpu",
        "--strategy",
        "least-loaded",
        "--label",
        "task=parser",
        "--wait",
        "10m",
        "--title",
        "Fix parser",
        "--template",
        "single-worker",
        "--input",
        "in.json",
        "--uniform-runtime-config",
        "runtime.json",
        "--branch",
        "main",
        "--submission-key",
        "k1",
        "--push",
        "-d",
    ])
    .expect("hub submit parses");
    let HubCommand::Submit(submit) = command else {
        panic!("expected submit");
    };
    assert!(submit.detach);
    assert!(!submit.queue);
    assert_eq!(submit.wait, Some(Some("10m".to_owned())));
    let job = &submit.job;
    assert_eq!(job.target.as_deref(), Some("box-1"));
    assert_eq!(job.tags, ["eu", "gpu"]);
    assert_eq!(job.strategy, HubStrategy::LeastLoaded);
    assert_eq!(job.labels, [("task".to_owned(), "parser".to_owned())]);
    assert_eq!(job.submission_key.as_deref(), Some("k1"));
    assert_eq!(
        job.runtime_files,
        [std::path::PathBuf::from("runtime.json")]
    );
    assert_eq!(
        job.run_args,
        [
            "--title",
            "Fix parser",
            "--input",
            "in.json",
            "--uniform-runtime-config",
            "runtime.json",
            "--template",
            "single-worker",
            "--branch",
            "main",
            "--push",
        ]
    );
}

#[test]
fn bare_wait_means_wait_without_deadline() {
    let command = parse(&[
        "hub",
        "submit",
        "--wait",
        "--title",
        "T",
        "--profile",
        "p",
        "--input",
        "in.json",
    ])
    .expect("hub submit parses");
    let HubCommand::Submit(submit) = command else {
        panic!("expected submit");
    };
    assert_eq!(submit.wait, Some(None));
    assert!(!submit.detach);
}

#[test]
fn wait_and_queue_conflict() {
    let error = parse(&[
        "hub", "submit", "--wait", "1m", "--queue", "--title", "T", "--input", "in.json",
    ])
    .expect_err("conflicting flags are rejected");
    assert!(matches!(error, NativeV2CliError::Usage(_)));
}

#[test]
fn enqueue_parses_schedule_and_rejects_detach() {
    let command = parse(&[
        "hub",
        "enqueue",
        "--at",
        "+2h",
        "--priority",
        "-3",
        "--expires",
        "1d",
        "--title",
        "T",
        "--input",
        "in.json",
    ])
    .expect("hub enqueue parses");
    let HubCommand::Enqueue(enqueue) = command else {
        panic!("expected enqueue");
    };
    assert_eq!(enqueue.at.as_deref(), Some("+2h"));
    assert_eq!(enqueue.priority, -3);
    assert_eq!(enqueue.expires.as_deref(), Some("1d"));
    assert!(
        parse(&["hub", "enqueue", "-d", "--title", "T", "--input", "in.json"]).is_err(),
        "enqueue always detaches"
    );
}

#[test]
fn hub_jobs_reject_validate_only() {
    assert!(
        parse(&[
            "hub",
            "submit",
            "--validate-only",
            "--title",
            "T",
            "--input",
            "in.json"
        ])
        .is_err()
    );
}

#[test]
fn member_commands_and_aliases() {
    assert_eq!(
        parse(&["hub", "ls", "--tag", "gpu", "--no-probe"]).expect("ls alias"),
        HubCommand::List {
            tags: vec!["gpu".to_owned()],
            probe: false
        }
    );
    assert_eq!(
        parse(&["hub", "rm", "box-1"]).expect("rm alias"),
        HubCommand::Remove {
            name: "box-1".to_owned()
        }
    );
    let HubCommand::Add(add) =
        parse(&["hub", "add", "local", "--slots", "3", "--priority", "-1"]).expect("add")
    else {
        panic!("expected add");
    };
    assert_eq!((add.slots, add.priority), (3, -1));
    let HubCommand::Update(update) = parse(&[
        "hub",
        "update",
        "box-1",
        "--state",
        "drain",
        "--add-tag",
        "eu",
    ])
    .expect("update") else {
        panic!("expected update");
    };
    assert_eq!(update.state, Some(HubMemberState::Drain));
    assert_eq!(update.add_tags, ["eu"]);
    assert!(parse(&["hub", "add", "box-1", "--slots", "0"]).is_err());
}

#[test]
fn tags_and_labels_are_validated() {
    assert!(parse(&["hub", "add", "box-1", "--tag", "has space"]).is_err());
    assert!(parse(&["hub", "add", "box-1", "--tag", "a,b"]).is_err());
    assert!(parse(&["hub", "ps", "--label", "novalue"]).is_err());
    let HubCommand::Ps(ps) = parse(&["hub", "ps", "--label", "task=a=b"]).expect("ps") else {
        panic!("expected ps");
    };
    assert_eq!(ps.labels, [("task".to_owned(), "a=b".to_owned())]);
    assert_eq!(ps.state, "active");
}

#[test]
fn observation_and_dispatch_commands_parse() {
    assert_eq!(
        parse(&["hub", "logs", "hj_1", "--after", "c", "--execution", "e"]).expect("logs"),
        HubCommand::Logs(crate::native_v2_cli::hub::HubLogs {
            job: "hj_1".to_owned(),
            after: Some("c".to_owned()),
            execution: Some("e".to_owned()),
        })
    );
    assert_eq!(
        parse(&["hub", "dispatch"]).expect("dispatch"),
        HubCommand::Dispatch
    );
    assert!(
        parse(&["hub", "dispatch", "--loop"]).is_err(),
        "the daemon replaces --loop"
    );
    let HubCommand::Stats(stats) = parse(&[
        "hub",
        "stats",
        "--by",
        "target",
        "--by",
        "label:task",
        "--since",
        "7d",
    ])
    .expect("stats") else {
        panic!("expected stats");
    };
    assert_eq!(stats.by, ["target", "label:task"]);
}

#[test]
fn enqueue_cron_parses_with_zone_and_conflicts_with_at() {
    let base = ["hub", "enqueue", "--title", "T", "--input", "in.json"];
    let with = |extra: &[&'static str]| {
        let mut values = base.to_vec();
        values.extend_from_slice(extra);
        parse(&values)
    };
    let HubCommand::Enqueue(enqueue) =
        with(&["--cron", "0 3 * * 1-5", "--cron-tz", "Europe/Belgrade"]).expect("cron parses")
    else {
        panic!("expected enqueue");
    };
    assert_eq!(enqueue.cron.as_deref(), Some("0 3 * * 1-5"));
    assert_eq!(enqueue.cron_timezone.as_deref(), Some("Europe/Belgrade"));
    assert_eq!(enqueue.at, None);
    assert!(with(&["--cron", "@daily", "--at", "+1h"]).is_err());
    assert!(
        with(&["--cron-tz", "UTC"]).is_err(),
        "--cron-tz requires --cron"
    );
}

#[test]
fn daemon_and_service_commands_parse() {
    assert_eq!(
        parse(&["hub", "daemon", "run", "--interval", "3s"]).expect("daemon run"),
        HubCommand::Daemon(HubDaemonRun {
            interval: "3s".to_owned(),
            api: HubApiOptions::default(),
        })
    );
    assert_eq!(
        parse(&[
            "hub",
            "daemon",
            "run",
            "--listen",
            "0.0.0.0:7383",
            "--token-file",
            "/srv/token",
            "--metrics-listen",
            "127.0.0.1:9383",
            "--metrics-label",
            "task",
        ])
        .expect("daemon run with API options"),
        HubCommand::Daemon(HubDaemonRun {
            interval: "10s".to_owned(),
            api: HubApiOptions {
                listen: Some("0.0.0.0:7383".to_owned()),
                token_file: Some("/srv/token".into()),
                metrics_listen: Some("127.0.0.1:9383".to_owned()),
                metrics_labels: vec!["task".to_owned()],
            },
        })
    );
    assert_eq!(
        parse(&["hub", "daemon", "status"]).expect("daemon status"),
        HubCommand::DaemonStatus
    );
    assert_eq!(
        parse(&["hub", "install", "--systemd", "--system", "--dry-run"]).expect("systemd"),
        HubCommand::Install(HubInstall {
            manager: HubManager::Systemd,
            system: true,
            image: None,
            volumes: Vec::new(),
            interval: "10s".to_owned(),
            api: HubApiOptions::default(),
            dry_run: true,
        })
    );
    let HubCommand::Install(docker) = parse(&[
        "hub",
        "install",
        "--docker",
        "--image",
        "zs:dev",
        "--volume",
        "/src",
        "--interval",
        "5s",
    ])
    .expect("docker") else {
        panic!("expected install");
    };
    assert_eq!(docker.manager, HubManager::Docker);
    assert_eq!(docker.image.as_deref(), Some("zs:dev"));
    assert_eq!(docker.volumes, [std::path::PathBuf::from("/src")]);
    assert_eq!(
        parse(&["hub", "uninstall", "--docker"]).expect("uninstall"),
        HubCommand::Uninstall(HubUninstall {
            manager: HubManager::Docker,
            system: false,
            dry_run: false,
        })
    );
    for invalid in [
        &["hub", "install"][..],
        &["hub", "install", "--systemd", "--docker"],
        &["hub", "install", "--docker", "--system"],
        &["hub", "install", "--systemd", "--image", "x"],
        &["hub", "uninstall"],
    ] {
        assert!(parse(invalid).is_err(), "{invalid:?} must be rejected");
    }
}

#[test]
fn hub_flag_selects_a_context_for_any_hub_command() {
    let selected = request(&["hub", "ps", "--hub", "lab"]).expect("flag after");
    assert_eq!(selected.context.as_deref(), Some("lab"));
    assert!(matches!(selected.command, HubCommand::Ps(_)));
    let selected = request(&["hub", "--hub", "lab", "dequeue", "hj_1"]).expect("flag before");
    assert_eq!(selected.context.as_deref(), Some("lab"));
    assert_eq!(request(&["hub", "ps"]).expect("no flag").context, None);
}

#[test]
fn context_commands_parse_and_require_one_token_source() {
    assert_eq!(
        parse(&[
            "hub",
            "context",
            "add",
            "lab",
            "http://lab:7383",
            "--token-file",
            "lab.token",
        ])
        .expect("add"),
        HubCommand::Context(HubContextCommand::Add {
            name: "lab".to_owned(),
            url: "http://lab:7383".to_owned(),
            token: HubTokenSource::File("lab.token".into()),
        })
    );
    assert_eq!(
        parse(&[
            "hub",
            "context",
            "add",
            "ci",
            "https://h",
            "--token-env",
            "T"
        ])
        .expect("env"),
        HubCommand::Context(HubContextCommand::Add {
            name: "ci".to_owned(),
            url: "https://h".to_owned(),
            token: HubTokenSource::Env("T".to_owned()),
        })
    );
    assert_eq!(
        parse(&["hub", "context", "ls"]).expect("ls"),
        HubCommand::Context(HubContextCommand::List)
    );
    assert_eq!(
        parse(&["hub", "context", "use", "lab"]).expect("use"),
        HubCommand::Context(HubContextCommand::Use {
            name: "lab".to_owned()
        })
    );
    for invalid in [
        &["hub", "context", "add", "lab", "http://lab:7383"][..],
        &[
            "hub",
            "context",
            "add",
            "lab",
            "http://x",
            "--token-file",
            "f",
            "--token-env",
            "T",
        ],
        &[
            "hub",
            "context",
            "add",
            "bad name",
            "http://x",
            "--token-env",
            "T",
        ],
        &["hub", "context", "rm"],
    ] {
        assert!(parse(invalid).is_err(), "{invalid:?} must be rejected");
    }
}
