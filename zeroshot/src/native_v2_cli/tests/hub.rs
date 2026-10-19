use std::ffi::OsString;

use crate::native_v2_cli::hub::{HubCommand, HubMemberState, HubStrategy};
use crate::native_v2_cli::{NativeV2CliCommand, NativeV2CliError, parse_native_v2_args};

fn parse(values: &[&str]) -> Result<HubCommand, NativeV2CliError> {
    match parse_native_v2_args(values.iter().map(OsString::from))? {
        NativeV2CliCommand::Hub(command) => Ok(command),
        other => panic!("expected a hub command, got {other:?}"),
    }
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
        parse(&["hub", "dispatch", "--loop", "--interval", "30s"]).expect("dispatch"),
        HubCommand::Dispatch {
            repeat: true,
            interval: "30s".to_owned()
        }
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
