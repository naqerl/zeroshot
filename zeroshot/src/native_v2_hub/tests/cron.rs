use super::super::clock::{parse_future_ms, rfc3339};
use super::super::cron::{fire_due, local_time, next_fire, parse, timezone};
use super::super::model::{HubState, JobState, Schedule, ScheduleState};
use super::job;

fn at(text: &str) -> u64 {
    parse_future_ms(text, 0).expect("rfc3339")
}

fn next(expression: &str, zone: &str, after: &str) -> String {
    let cron = parse(expression).expect("cron");
    let (_, zone) = timezone(Some(zone)).expect("zone");
    rfc3339(next_fire(&cron, &zone, at(after)).expect("next fire"))
}

fn schedule(id: &str, next_fire_ms: u64) -> Schedule {
    let mut template = job(id, 0);
    template.priority = 3;
    Schedule {
        schedule_id: id.to_owned(),
        state: ScheduleState::Active,
        cron: "*/15 * * * *".to_owned(),
        timezone: "UTC".to_owned(),
        template,
        repin_from: None,
        submission_key: None,
        created_at_ms: 0,
        next_fire_ms: Some(next_fire_ms),
        last_fired_ms: None,
        last_job_id: None,
        fired: 0,
        skipped: 0,
        removed_at_ms: None,
        error: None,
    }
}

#[test]
fn cron_accepts_five_fields_and_nicknames_only() {
    for valid in [
        "0 3 * * *",
        "*/15 9-17 * * mon-fri",
        "0 0 1 jan *",
        "@daily",
    ] {
        assert!(parse(valid).is_ok(), "{valid:?} must parse");
    }
    for invalid in [
        "",
        "* * * *",
        "0 0 3 * * *",
        "0 0 3 * * * 2027",
        "61 * * * *",
        "@sometimes",
    ] {
        assert!(parse(invalid).is_err(), "{invalid:?} must be rejected");
    }
    assert!(timezone(Some("Mars/Olympus")).is_err());
    assert_eq!(timezone(Some("UTC")).expect("utc").0, "UTC");
    assert!(!timezone(None).expect("system zone").0.is_empty());
}

#[test]
fn next_fire_follows_the_schedule_zone_across_daylight_saving() {
    assert_eq!(
        next("0 3 * * *", "UTC", "2026-10-20T03:00:00Z"),
        "2026-10-21T03:00:00Z"
    );
    // Europe/Belgrade is UTC+2 until 2026-10-25 03:00 local, then UTC+1.
    assert_eq!(
        next("0 9 * * *", "Europe/Belgrade", "2026-10-24T12:00:00Z"),
        "2026-10-25T08:00:00Z"
    );
    assert_eq!(
        next("0 9 * * *", "Europe/Belgrade", "2026-10-23T12:00:00Z"),
        "2026-10-24T07:00:00Z"
    );
    // Day-of-month and day-of-week restrictions match either (POSIX).
    assert_eq!(
        next("0 0 1 * mon", "UTC", "2026-10-20T00:00:00Z"),
        "2026-10-26T00:00:00Z"
    );
    assert_eq!(
        local_time("Europe/Belgrade", at("2026-10-25T08:00:00Z")).as_deref(),
        Some("2026-10-25T09:00:00+01:00[Europe/Belgrade]")
    );
}

#[test]
fn fire_due_queues_one_job_per_due_schedule_and_advances() {
    let now = at("2026-10-20T10:07:00Z");
    let mut removed = schedule("hs_removed", 0);
    removed.state = ScheduleState::Removed;
    let mut state = HubState {
        schedules: vec![
            schedule("hs_due", now - 3_600_000),
            schedule("hs_later", at("2026-10-20T10:20:00Z")),
            removed,
        ],
        ..HubState::default()
    };
    let fired = fire_due(&mut state, now);
    assert_eq!(
        fired.len(),
        1,
        "missed fires collapse into one catch-up fire"
    );
    assert_eq!(state.jobs.len(), 1);
    let queued = &state.jobs[0];
    assert_eq!(queued.state, JobState::Queued);
    assert_eq!(queued.schedule_id.as_deref(), Some("hs_due"));
    assert!(queued.job_id.starts_with("hj_"));
    assert_eq!(queued.priority, 3);
    assert_eq!(queued.submitted_at_ms, now);
    let due = &state.schedules[0];
    assert_eq!(due.fired, 1);
    assert_eq!(due.last_job_id.as_deref(), Some(queued.job_id.as_str()));
    assert_eq!(
        rfc3339(due.next_fire_ms.expect("next")),
        "2026-10-20T10:15:00Z"
    );
    assert_eq!(state.schedules[1].fired, 0);

    let later = at("2026-10-20T10:15:00Z");
    let fired = fire_due(&mut state, later);
    assert_eq!(fired[0]["skipped"], "previous job is still queued");
    assert_eq!(state.jobs.len(), 1);
    assert_eq!(state.schedules[0].skipped, 1);

    state.jobs[0].state = JobState::Running;
    let fired = fire_due(&mut state, at("2026-10-20T10:30:00Z"));
    assert_eq!(fired.len(), 2, "hs_due and hs_later are both due");
    assert_eq!(state.jobs.len(), 3);
    assert_eq!(state.schedules[2].fired, 0, "removed schedules never fire");
}

fn git(folder: &std::path::Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args([
            "-c",
            "user.name=hub",
            "-c",
            "user.email=hub@example.invalid",
        ])
        .args(["-c", "commit.gpgsign=false"])
        .args(args)
        .current_dir(folder)
        .output()
        .expect("git");
    assert!(output.status.success(), "git {args:?}: {output:?}");
    String::from_utf8(output.stdout)
        .expect("utf8")
        .trim()
        .to_owned()
}

#[test]
fn fires_repin_to_the_working_folder_head_when_it_exists_here() {
    let folder = tempfile::tempdir().expect("folder");
    git(folder.path(), &["init", "-q"]);
    git(
        folder.path(),
        &["commit", "-q", "--allow-empty", "-m", "first"],
    );
    let first = git(folder.path(), &["rev-parse", "HEAD"]);

    let now = at("2026-10-20T10:07:00Z");
    let mut here = schedule("hs_here", now);
    here.template.pinned_revision = Some("0".repeat(40));
    here.repin_from = Some(folder.path().to_path_buf());
    let mut elsewhere = schedule("hs_elsewhere", now);
    elsewhere.template.pinned_revision = Some("1".repeat(40));
    elsewhere.repin_from = Some(folder.path().join("absent"));
    let mut state = HubState {
        schedules: vec![here, elsewhere],
        ..HubState::default()
    };
    assert_eq!(fire_due(&mut state, now).len(), 2);
    assert_eq!(
        state.jobs[0].pinned_revision.as_deref(),
        Some(first.as_str())
    );
    assert!(
        state.jobs[0].requirements.remote_unsupported.is_some(),
        "a commit on no remote branch cannot run remotely"
    );
    assert_eq!(
        state.jobs[1].pinned_revision.as_deref(),
        Some("1".repeat(40).as_str()),
        "a folder this host lacks keeps the accepted pin"
    );

    git(
        folder.path(),
        &["commit", "-q", "--allow-empty", "-m", "second"],
    );
    let second = git(folder.path(), &["rev-parse", "HEAD"]);
    for job in &mut state.jobs {
        job.state = JobState::Succeeded;
    }
    fire_due(&mut state, at("2026-10-20T10:15:00Z"));
    assert_eq!(
        state.jobs[2].pinned_revision.as_deref(),
        Some(second.as_str())
    );
}

#[test]
fn schedule_views_name_times_in_utc_and_the_schedule_zone() {
    let mut fired = schedule("hs_view", at("2026-10-25T08:00:00Z"));
    fired.timezone = "Europe/Belgrade".to_owned();
    fired.last_fired_ms = Some(at("2026-10-25T07:45:00Z"));
    fired.last_job_id = Some("hj_last".to_owned());
    fired.fired = 4;
    let view = fired.view();
    assert_eq!(view["scheduleId"], "hs_view");
    assert_eq!(view["cron"], "*/15 * * * *");
    assert_eq!(view["nextFireAt"], "2026-10-25T08:00:00Z");
    assert_eq!(
        view["nextFireLocal"],
        "2026-10-25T09:00:00+01:00[Europe/Belgrade]"
    );
    assert_eq!(view["lastFiredAt"], "2026-10-25T07:45:00Z");
    assert_eq!(view["lastJobId"], "hj_last");
    assert_eq!(view["fired"], 4);
    assert!(view["removedAt"].is_null());
}
