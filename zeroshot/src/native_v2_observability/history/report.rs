//! Read-only run report derived from the additive history definition.
//!
//! The report is a projection only: it never starts or resumes a controller, and it uses the
//! same bounded lifecycle rows and counters exposed by the run-history definition.
use openengine_cluster_protocol::{ResolvedSource, RunId, RunTitle, TokenUsage};
use serde::Serialize;

use crate::v2_run_ledger::RunPhase;

use super::{RunDefinition, RunNodeDetail};

/// Stable schema identifier for `zeroshot report` JSON.
pub const RUN_REPORT_SCHEMA: &str = "zeroshot.run.report/v1";

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunReport {
    pub schema: &'static str,
    pub run: RunReportHeader,
    pub nodes: Vec<RunNodeDetail>,
    pub totals: RunReportTotals,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunReportHeader {
    pub run_id: RunId,
    pub title: RunTitle,
    pub phase: RunPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<RunReportSource>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunReportSource {
    pub repository: String,
    pub branch: String,
    pub revision: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunReportTotals {
    /// First to last retained node timestamp, or null when the run has no timestamps.
    pub wall_clock_ms: Option<u64>,
    /// Sum of per-node durations. Missing timestamps contribute nothing.
    pub node_duration_ms: u64,
    /// Wall clock minus node durations. Null when wall clock is unavailable.
    pub unattributed_ms: Option<i64>,
    /// Run-wide provider usage already reported by status/history.
    pub token_usage: Option<TokenUsage>,
    pub compactions: u64,
    pub format_rejections: u64,
}

/// Builds the report from one already-decoded history definition and optional usage.
#[must_use]
pub fn build_run_report(definition: &RunDefinition, token_usage: Option<TokenUsage>) -> RunReport {
    let mut first = None::<u64>;
    let mut last = None::<u64>;
    let mut node_duration_ms = 0_u64;
    for node in &definition.nodes {
        for timestamp in [node.started_at, node.completed_at].into_iter().flatten() {
            first = Some(first.map_or(timestamp.get(), |value| value.min(timestamp.get())));
            last = Some(last.map_or(timestamp.get(), |value| value.max(timestamp.get())));
        }
        node_duration_ms = node_duration_ms.saturating_add(node.duration_ms.unwrap_or(0));
    }
    let wall_clock_ms = first
        .zip(last)
        .map(|(first, last)| last.saturating_sub(first));
    let unattributed_ms = wall_clock_ms.and_then(|wall| {
        let wall = i64::try_from(wall).ok()?;
        let nodes = i64::try_from(node_duration_ms).ok()?;
        wall.checked_sub(nodes)
    });
    RunReport {
        schema: RUN_REPORT_SCHEMA,
        run: RunReportHeader {
            run_id: definition.run_id.clone(),
            title: definition.title.clone(),
            phase: definition.phase.clone(),
            source: definition.source.as_ref().map(report_source),
        },
        nodes: definition.nodes.clone(),
        totals: RunReportTotals {
            wall_clock_ms,
            node_duration_ms,
            unattributed_ms,
            token_usage,
            compactions: definition.compactions,
            format_rejections: definition.format_rejections,
        },
    }
}

fn report_source(source: &ResolvedSource) -> RunReportSource {
    RunReportSource {
        repository: source.repository.as_str().to_owned(),
        branch: source.branch.as_str().to_owned(),
        revision: source.revision.as_str().to_owned(),
    }
}
