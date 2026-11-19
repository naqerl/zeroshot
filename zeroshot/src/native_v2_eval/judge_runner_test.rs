use serde_json::json;

use crate::native_v2_eval::{
    CapturedSampleInput, EvalError, EvalSample, JudgeOptions, SampleStatus, SampleTiming,
    TokenUsage, captured_sample_from_directory, judge_captured_samples,
};

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

struct SkippingJudge;

#[async_trait::async_trait]
impl crate::native_v2_eval::Judge for SkippingJudge {
    async fn judge(
        &self,
        request: crate::native_v2_eval::JudgeRequest,
    ) -> Result<crate::native_v2_eval::JudgeResult, EvalError> {
        let judgements = request
            .criteria
            .iter()
            .map(|criterion| {
                (
                    criterion.id.clone(),
                    crate::native_v2_eval::Judgement::new(
                        true,
                        "The captured patch satisfies this trait.",
                    )
                    .expect("judgement"),
                )
            })
            .collect();
        Ok(crate::native_v2_eval::JudgeResult { judgements })
    }
}

#[tokio::test]
async fn judge_captured_samples_reports_skipped_captures() {
    let succeeded = CapturedSampleInput {
        sample: EvalSample {
            id: "baseline-0000".to_owned(),
            run_id: "run".to_owned(),
            arm_id: "baseline".to_owned(),
            run_index: 0,
            status: SampleStatus::Succeeded,
            timing: SampleTiming::default(),
            tokens: TokenUsage::default(),
            cost_usd: None,
            delivered_revision: None,
            failure_reason: None,
            patch: None,
            criteria: Default::default(),
            reasoning: Default::default(),
            judge_template: None,
            judge_checkout: None,
            judge_executor: None,
        },
        patch: Some("diff --git a/file b/file\n".to_owned()),
        sample_json: serde_json::Value::Null,
        checkout: None,
    };
    let skipped = CapturedSampleInput {
        sample: EvalSample {
            id: "baseline-0001".to_owned(),
            status: SampleStatus::Failed,
            run_index: 1,
            ..succeeded.sample.clone()
        },
        patch: None,
        sample_json: serde_json::Value::Null,
        checkout: None,
    };
    let criteria = [crate::native_v2_eval::EvalCriterion {
        id: "correctness".to_owned(),
        description: "correct".to_owned(),
        weight: 1.0,
    }];
    let result = judge_captured_samples(
        &SkippingJudge,
        &criteria,
        vec![succeeded, skipped],
        JudgeOptions::default(),
    )
    .await
    .expect("captures are judged");
    assert_eq!(result.judged.len(), 1);
    assert_eq!(result.skipped, vec!["baseline-0001".to_owned()]);
}
