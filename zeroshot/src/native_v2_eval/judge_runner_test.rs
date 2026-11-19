use serde_json::json;

use crate::native_v2_eval::{EvalError, captured_sample_from_directory};

#[test]
fn malformed_sample_artifact_is_not_treated_as_a_legacy_nested_document() {
    let temporary = tempfile::tempdir().expect("temporary workspace");
    let document = json!({
        "sampleId": "baseline-0000",
        "runId": "run-malformed",
        "status": "succeeded",
        "timings": {
            "submittedAtMs": 1,
            "startedAtMs": 2,
            "finishedAtMs": 3,
            "queueWaitMs": 1,
            "wallClockMs": 2,
            "concurrency": 1
        },
        "captured": { "patch": 7 }
    });
    std::fs::write(
        temporary.path().join("sample.json"),
        serde_json::to_vec(&document).unwrap(),
    )
    .expect("malformed sample.json");
    std::fs::write(temporary.path().join("patch.diff"), "patch\n").expect("patch");
    let error = captured_sample_from_directory(temporary.path(), "baseline", 0)
        .expect_err("strict sample parse errors propagate");
    assert!(matches!(error, EvalError::Data { .. }));
    assert!(error.to_string().contains("sample.json"));
}
