//! Resolution and execution seam for the `eval-judge` JSON graph template.
//!
//! The built-in template lives beside the other built-in graph sources. A caller may pass a path
//! to its own graph JSON instead, so the judge is not tied to the shipped node layout.

use async_trait::async_trait;
use openengine_cluster_protocol::{GraphNode, GraphSpec, PayloadType};
use serde_json::{Value, json};

use super::{
    EvalError, Judge, JudgeParseError, JudgeRequest, JudgeResult, JUDGE_TEMPLATE_NAME,
    parse_judgement_value,
};

/// Built-in JSON graph used when no custom judge template is selected. It is the same source as
/// the `eval-judge` built-in template (`zeroshot template show eval-judge`).
pub const JUDGE_TEMPLATE_JSON: &str = crate::native_v2_cli::EVAL_JUDGE_GRAPH_JSON;

/// A parsed judge graph and the stable identity recorded in reports.
#[derive(Clone, Debug, PartialEq)]
pub struct JudgeTemplate {
    pub name: String,
    pub graph: GraphSpec,
    pub source: String,
}

/// Resolve the built-in `eval-judge` template or a caller-owned graph JSON file.
pub fn resolve_judge_template(reference: &str) -> Result<JudgeTemplate, EvalError> {
    if reference.trim().is_empty() {
        return Err(EvalError::Command(
            "judge template reference must not be empty".to_owned(),
        ));
    }
    let (name, source) = if reference == JUDGE_TEMPLATE_NAME {
        (
            JUDGE_TEMPLATE_NAME.to_owned(),
            JUDGE_TEMPLATE_JSON.to_owned(),
        )
    } else {
        let path = std::path::Path::new(reference);
        if !path.is_file() {
            return Err(EvalError::Command(format!(
                "judge template {reference:?} is neither {JUDGE_TEMPLATE_NAME:?} nor a readable graph JSON file"
            )));
        }
        let source = std::fs::read_to_string(path).map_err(|error| EvalError::io(path, error))?;
        (reference.to_owned(), source)
    };
    let graph: GraphSpec = serde_json::from_str(&source).map_err(|error| {
        EvalError::Command(format!(
            "judge template {name:?} is not a valid graph: {error}"
        ))
    })?;
    Ok(JudgeTemplate {
        name,
        graph,
        source,
    })
}

/// The full judge input for one captured sample: the task, the acceptance traits as a JSON
/// string of `[{id, description}]`, and the captured patch. `verdicts` is the empty placeholder
/// the built-in graph's state needs; `sample` and `checkout` are offered to custom graphs that
/// declare them.
#[must_use]
pub fn judge_input(request: &JudgeRequest) -> Value {
    let sample = request
        .sample_json
        .clone()
        .unwrap_or_else(|| serde_json::to_value(&request.sample).unwrap_or(Value::Null));
    let traits = request
        .criteria
        .iter()
        .map(|criterion| json!({"id": criterion.id, "description": criterion.description}))
        .collect::<Vec<_>>();
    json!({
        "task": request.task,
        "traits": Value::Array(traits).to_string(),
        "patch": request.patch,
        "verdicts": [],
        "sample": sample.to_string(),
        "checkout": request.checkout.clone().unwrap_or_default(),
    })
}

/// The input for a selected template, restricted to the fields its `initialInput` declares.
/// Custom graphs therefore receive exactly the documented contract they author.
pub fn judge_input_for_template(
    template: &JudgeTemplate,
    input: Value,
) -> Result<Value, EvalError> {
    let PayloadType::Record { fields } = &template.graph.initial_input else {
        return Ok(input);
    };
    let Value::Object(mut object) = input else {
        return Err(EvalError::Judge(
            "judge input construction did not produce an object".to_owned(),
        ));
    };
    object.retain(|key, _| fields.keys().any(|field| field.as_str() == key));
    if let Some(missing) = fields
        .iter()
        .find(|(name, field)| field.required && !object.contains_key(name.as_str()))
    {
        return Err(EvalError::Judge(format!(
            "judge template requires undeclared input field {:?}",
            missing.0.as_str()
        )));
    }
    Ok(Value::Object(object))
}

/// Synthetic judge for dry runs. It calls no model: it answers in the judge graph's output shape
/// from the evidence the fake runner already recorded on the sample, so `zeroshot eval` stays
/// runnable without credentials.
#[derive(Clone, Copy, Debug, Default)]
pub struct FixtureJudgeGraphExecutor;

#[async_trait]
impl JudgeGraphExecutor for FixtureJudgeGraphExecutor {
    async fn execute(&self, template: &JudgeTemplate, input: Value) -> Result<Value, EvalError> {
        if !has_step(&template.graph.root) {
            return Err(EvalError::Judge(
                "judge graph template has no executable step".to_owned(),
            ));
        }
        let field = |name: &str| {
            input
                .get(name)
                .and_then(Value::as_str)
                .ok_or_else(|| EvalError::Judge(format!("judge input has no {name}")))
        };
        let traits: Vec<Value> = serde_json::from_str(field("traits")?).map_err(|error| {
            EvalError::Judge(format!("judge input traits are not JSON: {error}"))
        })?;
        let sample: Value = serde_json::from_str(field("sample")?).map_err(|error| {
            EvalError::Judge(format!("judge input sample is not JSON: {error}"))
        })?;
        let sample_id = sample
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        if let Some(existing) = sample.get("judgeExecutor").and_then(Value::as_str) {
            if existing != "synthetic" {
                return Err(EvalError::Judge(format!(
                    "synthetic judge refuses to relabel sample {sample_id} scores produced by judge executor {existing:?}; use judgeRunner = \"local\" or a fresh eval"
                )));
            }
        }
        let mut verdicts = Vec::with_capacity(traits.len());
        for item in &traits {
            let id = item
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| EvalError::Judge("judge input trait has no id".to_owned()))?;
            let result = sample
                .get("criteria")
                .and_then(|criteria| criteria.get(id))
                .ok_or_else(|| {
                    EvalError::Judge(format!(
                        "synthetic judge has no recorded evidence for sample {sample_id} trait {id}; use judgeRunner = \"local\" to run the judge graph"
                    ))
                })?;
            let pass = result
                .get("passed")
                .and_then(Value::as_bool)
                .ok_or_else(|| {
                    EvalError::Judge(format!(
                        "synthetic judge evidence for trait {id} has no passed flag"
                    ))
                })?;
            let reason = result.get("reasoning").and_then(Value::as_str).map_or_else(
                || {
                    format!(
                        "Synthetic sample {sample_id} recorded {id} as {}.",
                        if pass { "passed" } else { "failed" }
                    )
                },
                str::to_owned,
            );
            verdicts.push(json!({"id": id, "pass": pass, "reason": reason}));
        }
        Ok(json!({ "traits": verdicts }))
    }
}
fn has_step(node: &GraphNode) -> bool {
    match node {
        GraphNode::Step(_) => true,
        GraphNode::Seq(node) => node.children.as_slice().iter().any(has_step),
        GraphNode::Choice(node) => {
            node.branches
                .as_slice()
                .iter()
                .any(|branch| has_step(&branch.node))
                || node.otherwise.as_deref().is_some_and(has_step)
        }
        GraphNode::Par(node) => node.branches.as_slice().iter().any(has_step),
        GraphNode::Loop(node) => has_step(&node.body),
        GraphNode::Map(node) => has_step(&node.body),
        GraphNode::Verifier(_) | GraphNode::Succeed(_) | GraphNode::Fail(_) => false,
    }
}

/// Executes one admitted judge graph with a JSON input and returns its terminal output.
#[async_trait]
pub trait JudgeGraphExecutor: Send + Sync {
    /// Run the judge graph once for one sample. `input` is the full [`judge_input`]; executors
    /// that submit a real run restrict it to the template's declared fields first.
    async fn execute(&self, template: &JudgeTemplate, input: Value) -> Result<Value, EvalError>;
}

#[async_trait]
impl<T: JudgeGraphExecutor + ?Sized> JudgeGraphExecutor for Box<T> {
    async fn execute(&self, template: &JudgeTemplate, input: Value) -> Result<Value, EvalError> {
        (**self).execute(template, input).await
    }
}

/// A judge that runs the selected graph once per sample with every trait, then parses its
/// `{traits: [{id, pass, reason}]}` output strictly.
pub struct TemplateJudge<E> {
    executor: E,
    template: JudgeTemplate,
}

impl<E> TemplateJudge<E> {
    #[must_use]
    pub fn new(executor: E, template: JudgeTemplate) -> Self {
        Self { executor, template }
    }

    #[must_use]
    pub fn template(&self) -> &JudgeTemplate {
        &self.template
    }
}

#[async_trait]
impl<E: JudgeGraphExecutor> Judge for TemplateJudge<E> {
    async fn judge(&self, request: JudgeRequest) -> Result<JudgeResult, EvalError> {
        let output = self
            .executor
            .execute(&self.template, judge_input(&request))
            .await?;
        parse_judgement_value(&output, &request.criteria).map_err(EvalError::from)
    }
}

impl From<JudgeParseError> for EvalError {
    fn from(error: JudgeParseError) -> Self {
        Self::Judge(error.to_string())
    }
}
