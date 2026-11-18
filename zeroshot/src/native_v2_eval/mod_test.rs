use super::{EvalScore, EvalScoreError, write_json_atomic};

struct ConstantScore;

impl EvalScore for ConstantScore {
    fn scorer(&self) -> &str {
        "constant"
    }

    fn score(&self, sample: &serde_json::Value) -> Result<Option<f64>, EvalScoreError> {
        Ok(sample
            .get("value")
            .and_then(serde_json::Value::as_f64)
            .map(|value| value / 2.0))
    }
}

#[test]
fn atomic_json_write_replaces_the_previous_document() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("nested").join("document.json");
    write_json_atomic(&path, &serde_json::json!({"value": 1})).unwrap();
    write_json_atomic(&path, &serde_json::json!({"value": 2})).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(value, serde_json::json!({"value": 2}));
}

#[test]
fn scorers_receive_stored_json_so_they_can_be_added_after_an_eval() {
    let score = ConstantScore;
    assert_eq!(score.scorer(), "constant");
    assert_eq!(
        score.score(&serde_json::json!({"value": 7})).unwrap(),
        Some(3.5)
    );
    assert_eq!(score.score(&serde_json::json!({"other": 7})).unwrap(), None);
}
