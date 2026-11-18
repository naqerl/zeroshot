use openengine_cluster_protocol::RunId;

use super::{
    Capacity, CapacitySourceKind, RESUME_FILE, ResumeAction, ResumeJournal, SampleResumeState,
    SampleStatus,
};
use crate::native_v2_eval::runner::{SampleArtifact, SampleOutcome, SampleRun, SampleTimings};

fn sample_run(run_id: &str) -> SampleRun {
    SampleRun {
        sample_id: "sample".to_owned(),
        run_id: RunId::new(run_id),
        target: None,
        submitted_at_ms: 1,
        concurrency: 2,
    }
}

fn outcome(status: SampleStatus) -> SampleOutcome {
    SampleOutcome {
        run_id: RunId::new("run-resume"),
        status,
        timings: SampleTimings {
            submitted_at_ms: 1,
            started_at_ms: Some(2),
            finished_at_ms: 3,
            queue_wait_ms: 1,
            wall_clock_ms: 2,
            concurrency: 2,
        },
        token_usage: None,
        delivered_revision: None,
        delivered_branch: None,
        delivery: None,
        failure_reason: None,
        output: None,
    }
}

#[test]
fn journal_skips_completed_resumes_interrupted_and_restarts_failed_samples() {
    let mut journal = ResumeJournal::default();
    assert_eq!(journal.action("missing"), ResumeAction::Start);

    journal.record_outcome("done", outcome(SampleStatus::Succeeded));
    assert_eq!(journal.action("done"), ResumeAction::Skip);
    assert_eq!(journal.samples["done"].state, SampleResumeState::Completed);
    assert!(journal.incomplete().is_empty());

    journal.claim("interrupted", sample_run("run-2"));
    assert_eq!(
        journal.action("interrupted"),
        ResumeAction::Resume(sample_run("run-2"))
    );
    assert_eq!(journal.incomplete(), vec!["interrupted"]);

    journal.record_outcome("failed", outcome(SampleStatus::Failed));
    assert_eq!(journal.action("failed"), ResumeAction::Start);
    assert_eq!(
        journal.resumable_run("failed"),
        Some(&RunId::new("run-resume"))
    );
}

#[test]
fn journal_round_trips_and_warns_when_attempt_concurrency_changes() {
    let directory = tempfile::tempdir().unwrap();
    let mut journal = ResumeJournal::default();
    assert!(
        journal
            .observe_capacity(Capacity::new(2, CapacitySourceKind::Concurrency))
            .is_none()
    );
    journal.claim("sample", sample_run("run-1"));
    journal.record_outcome("sample", outcome(SampleStatus::Succeeded));
    journal.save(directory.path()).unwrap();
    assert!(directory.path().join(RESUME_FILE).is_file());

    let mut loaded = ResumeJournal::load(directory.path()).unwrap();
    assert_eq!(loaded, journal);
    let warning = loaded
        .observe_capacity(Capacity::new(3, CapacitySourceKind::Platform))
        .unwrap();
    assert_eq!(warning.previous, 2);
    assert_eq!(warning.current, 3);
    assert_eq!(
        loaded.samples["sample"].run_id.as_ref().map(RunId::as_str),
        Some("run-resume")
    );
}

#[test]
fn capture_incompleteness_stays_resumable_until_patch_and_judge_exist() {
    let mut journal = ResumeJournal::default();
    let mut artifact = SampleArtifact::new("capture", outcome(SampleStatus::Succeeded));
    artifact.capture_error = Some("fetch failed".to_owned());
    journal.record_artifact(&artifact);
    assert_eq!(journal.action("capture"), ResumeAction::Capture);
    assert_eq!(
        journal.samples["capture"].state,
        SampleResumeState::CapturePending
    );
    assert_eq!(journal.incomplete(), vec!["capture"]);

    artifact.capture_error = None;
    journal.record_artifact(&artifact);
    assert_eq!(journal.action("capture"), ResumeAction::Skip);
    assert_eq!(
        journal.samples["capture"].state,
        SampleResumeState::Completed
    );
}
