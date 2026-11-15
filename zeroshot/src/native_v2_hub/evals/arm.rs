//! A stored evals-v2 arm: a graph reference plus the overrides that distinguish it.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{arm_id, canonical, sha256_hex};

/// `POST /v1/hub/evals/arms`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct CreateArm {
    pub name: String,
    pub graph_ref: String,
    #[serde(default)]
    pub harness_patch: Option<Value>,
    pub base_revision: String,
    #[serde(default)]
    pub model_ids: Vec<String>,
    #[serde(default)]
    pub node_budgets: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct Arm {
    pub arm_id: String,
    pub arm_hash: String,
    pub name: String,
    pub graph_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness_patch: Option<Value>,
    pub base_revision: String,
    /// Models for the arm. When `harnessPatch` is a uniform runtime or a full runtime plan,
    /// these replace the runtime's model(s); otherwise they remain trial input and job
    /// attribution until a later slice materializes them.
    #[serde(default)]
    pub model_ids: Vec<String>,
    /// Per-node graph deadlines. Applied to the authored graph when `graphRef` names graph JSON;
    /// otherwise carried in the trial input.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_budgets: Option<Value>,
    pub created_at_ms: u64,
}

impl CreateArm {
    /// The arm identity deliberately excludes its display name.
    pub(crate) fn hash(&self) -> String {
        let identity = serde_json::json!({
            "graphRef": self.graph_ref,
            "harnessPatch": self.harness_patch,
            "baseRevision": self.base_revision,
            "modelIds": self.model_ids,
            "nodeBudgets": self.node_budgets,
        });
        sha256_hex(canonical(&identity).as_bytes())
    }
}

impl Arm {
    pub(crate) fn from_request(request: &CreateArm, created_at_ms: u64) -> Self {
        let arm_hash = request.hash();
        Self {
            arm_id: arm_id(&arm_hash),
            arm_hash,
            name: request.name.clone(),
            graph_ref: request.graph_ref.clone(),
            harness_patch: request.harness_patch.clone(),
            base_revision: request.base_revision.clone(),
            model_ids: request.model_ids.clone(),
            node_budgets: request.node_budgets.clone(),
            created_at_ms,
        }
    }

    pub(crate) fn view(&self) -> Value {
        serde_json::json!({
            "armId": self.arm_id,
            "armHash": self.arm_hash,
            "name": self.name,
            "graphRef": self.graph_ref,
            "harnessPatch": self.harness_patch,
            "baseRevision": self.base_revision,
            "modelIds": self.model_ids,
            "nodeBudgets": self.node_budgets,
            "createdAtMs": self.created_at_ms,
        })
    }
}
