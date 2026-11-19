//! Resolution and execution seam for the `eval-judge` JSON graph template.
//!
//! The built-in template lives beside the other built-in graph sources. A caller may pass a path
//! to its own graph JSON instead, so the judge is not tied to the shipped node layout.

use async_trait::async_trait;
use openengine_cluster_protocol::{GraphNode, GraphSpec, PayloadType};
use serde_json::{Value, json};

use super::{
    EvalError, Judge, JudgeRequest, JudgeResult, JUDGE_TEMPLATE_NAME, JudgeParseError,
    parse_judgement,
};

/// Built-in JSON graph used when no custom judge template is selected.
pub const JUDGE_TEMPLATE_JSON: &str = include_str!("../native_v2_templates/eval_judge.json");

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

/// The graph input for one criterion of one captured sample.
#[must_use]
pub fn judge_input(request: &JudgeRequest, criterion: &super::EvalCriterion) -> Value {
    let sample = request
        .sample_json
        .clone()
        .unwrap_or_else(|| serde_json::to_value(&request.sample).unwrap_or(Value::Null));
    json!({
        "criterion": criterion.id,
        "criterionDescription": criterion.description,
        "patch": request.patch,
        "sample": serde_json::to_string(&sample).unwrap_or_else(|_| "null".to_owned()),
        "model": request.model,
        "checkout": request.checkout.clone().unwrap_or_default(),
        // Required number field so the root group can materialize state before the judge step.
        "score": 0.0,
    })
}

/// The input for a selected template, restricted to the fields its `initialInput` declares.
/// Custom graphs therefore receive exactly the documented contract they author; the built-in
/// template additionally declares its internal `score` placeholder.
pub fn judge_input_for_template(
    template: &JudgeTemplate,
    request: &JudgeRequest,
    criterion: &super::EvalCriterion,
) -> Result<Value, EvalError> {
    let full = judge_input(request, criterion);
    let PayloadType::Record { fields } = &template.graph.initial_input else {
        return Ok(full);
    };
    let Value::Object(mut object) = full else {
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

/// Executor for the synthetic CLI runner. It reads the sample evidence already recorded by the
/// fake runner, but still runs the graph contract: the selected template must be a single judge
/// step, and its input/output shape is the judge response shape. Provider-backed embeddings use a
/// real executor instead; this keeps `zeroshot eval` runnable without credentials.
#[derive(Clone, Copy, Debug, Default)]
pub struct FixtureJudgeGraphExecutor;

#[async_trait]
impl JudgeGraphExecutor for FixtureJudgeGraphExecutor {
    async fn execute(
        &self,
        template: &JudgeTemplate,
        model: &str,
        input: Value,
    ) -> Result<Value, EvalError> {
        if !has_step(&template.graph.root) {
            return Err(EvalError::Judge(
                "judge graph template has no executable step".to_owned(),
            ));
        }
        let criterion = input
            .get("criterion")
            .and_then(Value::as_str)
            .ok_or_else(|| EvalError::Judge("judge input has no criterion".to_owned()))?;
        let sample_source = input
            .get("sample")
            .and_then(Value::as_str)
            .ok_or_else(|| EvalError::Judge("judge input has no sample".to_owned()))?;
        let sample: Value = serde_json::from_str(sample_source).map_err(|error| {
            EvalError::Judge(format!("judge input sample is not JSON: {error}"))
        })?;
        if let Some(existing) = sample.get("judgeModel").and_then(Value::as_str) {
            if existing != model {
                return Err(EvalError::Judge(format!(
                    "synthetic judge evidence was produced by model {existing:?}; refusing to relabel it as {model:?} without a real judgeRuntimeConfig"
                )));
            }
        }
        let result = sample
            .get("criteria")
            .and_then(|criteria| criteria.get(criterion))
            .ok_or_else(|| {
                EvalError::Judge(format!(
                    "synthetic judge has no recorded evidence for sample {} criterion {criterion}; configure judgeRuntimeConfig to run a real judge graph",
                    sample
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                ))
            })?;
        let score = result
            .get("score")
            .and_then(Value::as_f64)
            .or_else(|| {
                result
                    .get("passed")
                    .and_then(Value::as_bool)
                    .map(|passed| if passed { 5.0 } else { 1.0 })
            })
            .ok_or_else(|| {
                EvalError::Judge(format!(
                    "synthetic judge evidence for criterion {criterion} has no score"
                ))
            })?;
        let reasoning = result
            .get("reasoning")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| {
                format!(
                    "Synthetic sample {} has no captured evidence for {criterion}.",
                    sample
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                )
            });
        let errors = result.get("errors").cloned().unwrap_or_else(|| json!([]));
        Ok(json!({
            "criterion": criterion,
            "score": score,
            "reasoning": reasoning,
            "errors": errors,
        }))
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
    /// Run one graph node for the effective per-sample judge model. Provider-backed executors use
    /// `model` to build the runtime binding; synthetic executors may record or ignore it.
    async fn execute(
        &self,
        template: &JudgeTemplate,
        model: &str,
        input: Value,
    ) -> Result<Value, EvalError>;
}

#[async_trait]
impl<T: JudgeGraphExecutor + ?Sized> JudgeGraphExecutor for Box<T> {
    async fn execute(
        &self,
        template: &JudgeTemplate,
        model: &str,
        input: Value,
    ) -> Result<Value, EvalError> {
        (**self).execute(template, model, input).await
    }
}

/// A judge that runs the selected graph once per criterion. The graph is a real native-v2 node;
/// this adapter only maps its terminal output onto the strict judgement contract.
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
        let mut judgements = std::collections::BTreeMap::new();
        for criterion in &request.criteria {
            let input = judge_input_for_template(&self.template, &request, criterion)?;
            let output = self
                .executor
                .execute(&self.template, &request.model, input)
                .await?;
            let response = normalize_template_output(output, criterion)?;
            let parsed = parse_judgement(&response, std::slice::from_ref(criterion))
                .map_err(|error| EvalError::Judge(error.to_string()))?;
            judgements.extend(parsed.judgements);
        }
        Ok(JudgeResult { judgements })
    }
}

fn normalize_template_output(
    output: Value,
    criterion: &super::EvalCriterion,
) -> Result<String, EvalError> {
    let value = if output.get("judgements").is_some() || output.get("criteria").is_some() {
        output
    } else {
        let mut entry = output.as_object().cloned().ok_or_else(|| {
            EvalError::Judge("judge graph returned a non-object output".to_owned())
        })?;
        if !entry.contains_key("criterion") && !entry.contains_key("id") {
            entry.insert("criterion".to_owned(), Value::String(criterion.id.clone()));
        }
        json!({ "judgements": [entry] })
    };
    serde_json::to_string(&value).map_err(EvalError::from)
}

impl From<JudgeParseError> for EvalError {
    fn from(error: JudgeParseError) -> Self {
        Self::Judge(error.to_string())
    }
}
