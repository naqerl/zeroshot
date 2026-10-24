//! Two processes on loopback: a hub daemon owning one config folder, and a client with its own
//! config folder that reaches it only through a hub context over HTTP.

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};

use serde_json::Value;

const ZEROSHOT: &str = env!("CARGO_BIN_EXE_zeroshot");

fn zeroshot(config: &Path, arguments: &[&str]) -> Command {
    let mut command = Command::new(ZEROSHOT);
    command
        .args(arguments)
        .env("ZEROSHOT_CONFIG_DIR", config)
        .env("ZEROSHOT_ERROR_FORMAT", "json")
        .env_remove("ZEROSHOT_HUB_DIR")
        .env_remove("ZEROSHOT_HUB")
        .stdin(Stdio::null());
    command
}

fn run(config: &Path, folder: &Path, arguments: &[&str]) -> Output {
    zeroshot(config, arguments)
        .current_dir(folder)
        .output()
        .expect("zeroshot runs")
}

fn json(output: &Output) -> Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let line = stdout
        .lines()
        .chain(stderr.lines())
        .rev()
        .find(|line| line.starts_with('{'))
        .unwrap_or_else(|| panic!("no JSON in stdout {stdout:?} stderr {stderr:?}"));
    serde_json::from_str(line).expect("JSON line")
}

struct Daemon(Child);

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn a_second_machine_drives_the_hub_over_http() {
    let root = tempfile::tempdir().expect("tempdir");
    let hub_config = root.path().join("hub-machine");
    let client_config = root.path().join("laptop");
    let work = root.path().join("work");
    for folder in [&hub_config, &client_config, &work] {
        std::fs::create_dir_all(folder).expect("folder");
    }

    let mut child = zeroshot(
        &hub_config,
        &[
            "hub",
            "daemon",
            "run",
            "--listen",
            "127.0.0.1:0",
            "--interval",
            "1s",
        ],
    )
    .current_dir(root.path())
    .stdout(Stdio::piped())
    .stderr(Stdio::inherit())
    .spawn()
    .expect("daemon starts");
    let mut started = String::new();
    BufReader::new(child.stdout.take().expect("stdout"))
        .read_line(&mut started)
        .expect("started event");
    let daemon = Daemon(child);
    let started: Value = serde_json::from_str(&started).expect("started JSON");
    assert_eq!(started["event"], "started");
    let url = started["url"].as_str().expect("url").to_owned();
    let token_file = started["tokenFile"]
        .as_str()
        .expect("token file")
        .to_owned();
    assert!(
        !started
            .to_string()
            .contains(std::fs::read_to_string(&token_file).expect("token").trim())
    );

    // The laptop has no daemon: its default `local` context is unreachable.
    let unreachable = run(&client_config, &work, &["hub", "ps"]);
    assert_eq!(unreachable.status.code(), Some(5));
    assert_eq!(json(&unreachable)["code"], "hub.unreachable");

    // A second context with the wrong token is rejected by the hub.
    let wrong = root.path().join("wrong.token");
    std::fs::write(&wrong, "not-the-token\n").expect("wrong token");
    let wrong = wrong.to_string_lossy().into_owned();
    let added = run(
        &client_config,
        &work,
        &[
            "hub",
            "context",
            "add",
            "nope",
            &url,
            "--token-file",
            &wrong,
        ],
    );
    assert!(added.status.success(), "{added:?}");
    let rejected = run(&client_config, &work, &["hub", "--hub", "nope", "ps"]);
    assert_eq!(rejected.status.code(), Some(1));
    assert_eq!(json(&rejected)["code"], "hub.unauthorized");

    // With a copy of the hub's token, the laptop drives the hub.
    let copied = root.path().join("hub.token");
    std::fs::copy(&token_file, &copied).expect("copy token");
    let copied = copied.to_string_lossy().into_owned();
    for arguments in [
        &[
            "hub",
            "context",
            "add",
            "lab",
            &url,
            "--token-file",
            &copied,
        ][..],
        &["hub", "context", "use", "lab"],
        &["hub", "add", "local", "--slots", "2"],
    ] {
        let output = run(&client_config, &work, arguments);
        assert!(output.status.success(), "{arguments:?}: {output:?}");
    }
    std::fs::write(work.join("in.json"), "{\"task\":\"remote\"}").expect("input");
    let queued = run(
        &client_config,
        &work,
        &[
            "hub",
            "enqueue",
            "--at",
            "+1h",
            "--title",
            "Later",
            "--template",
            "single-worker",
            "--input",
            "in.json",
            "--label",
            "from=laptop",
        ],
    );
    assert!(queued.status.success(), "{queued:?}");
    let job = json(&queued)["job"]["jobId"]
        .as_str()
        .expect("job id")
        .to_owned();

    let listed = run(
        &client_config,
        &work,
        &["hub", "ps", "--label", "from=laptop"],
    );
    assert_eq!(json(&listed)["jobs"][0]["jobId"], job.as_str());
    let members = run(&client_config, &work, &["hub", "ls", "--no-probe"]);
    assert_eq!(json(&members)["targets"][0]["slots"], 2);
    let status = run(&client_config, &work, &["hub", "daemon", "status"]);
    assert_eq!(json(&status)["context"], "lab");

    // The hub machine sees the same job through its own `local` context.
    let local = run(&hub_config, root.path(), &["hub", "status", &job]);
    assert_eq!(json(&local)["job"]["state"], "scheduled");

    let logs = run(&client_config, &work, &["hub", "logs", &job]);
    assert_eq!(json(&logs)["code"], "hub.invalid_state");
    let dequeued = run(&client_config, &work, &["hub", "dequeue", &job]);
    assert_eq!(json(&dequeued)["job"]["state"], "dequeued");

    // A second daemon cannot take the hub folder.
    let second = run(
        &hub_config,
        root.path(),
        &["hub", "daemon", "run", "--listen", "127.0.0.1:0"],
    );
    assert_eq!(json(&second)["code"], "hub.daemon_running");

    drop(daemon);
    let gone = run(&client_config, &work, &["hub", "ps"]);
    assert_eq!(gone.status.code(), Some(5));
    let gone = json(&gone);
    assert_eq!(gone["code"], "hub.unreachable");
    assert_eq!(gone["details"]["context"], "lab");
    assert_eq!(gone["details"]["url"], url.as_str());
}
