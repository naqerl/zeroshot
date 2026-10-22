use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::{Map, Value, json};

use super::clock::{rfc3339, utc_day};
use super::error::HubError;
use super::model::{Job, JobState};
use super::usage::{UsageTotals, cost};

/// USD per million tokens for one model. Missing fields price those tokens at zero.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct ModelPrice {
    #[serde(default)]
    pub input: f64,
    #[serde(default)]
    pub output: f64,
    #[serde(default)]
    pub cache_read: f64,
    #[serde(default)]
    pub cache_creation: f64,
}

pub(crate) type Prices = BTreeMap<String, ModelPrice>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Dimension {
    Target,
    Model,
    Project,
    Day,
    Label(String),
}

impl Dimension {
    pub(crate) fn parse(text: &str) -> Result<Self, HubError> {
        match text {
            "target" => Ok(Self::Target),
            "model" => Ok(Self::Model),
            "project" => Ok(Self::Project),
            "day" => Ok(Self::Day),
            _ => match text.strip_prefix("label:") {
                Some(key) if !key.is_empty() => Ok(Self::Label(key.to_owned())),
                _ => Err(HubError::Usage(format!(
                    "invalid --by {text:?}: use target, model, project, day, or label:KEY"
                ))),
            },
        }
    }

    fn name(&self) -> String {
        match self {
            Self::Target => "target".to_owned(),
            Self::Model => "model".to_owned(),
            Self::Project => "project".to_owned(),
            Self::Day => "day".to_owned(),
            Self::Label(key) => format!("label:{key}"),
        }
    }

    fn value(&self, job: &Job) -> Value {
        match self {
            Self::Target => json!(job.target),
            Self::Model => json!(job.model_key()),
            Self::Project => json!(job.project),
            Self::Day => json!(utc_day(job.submitted_at_ms)),
            Self::Label(key) => json!(job.labels.get(key)),
        }
    }
}

/// Jobs submitted in `[since, until)`, grouped by dimensions.
pub(crate) struct StatsQuery<'a> {
    pub by: &'a [Dimension],
    pub since_ms: Option<u64>,
    pub until_ms: Option<u64>,
    pub prices: Option<&'a Prices>,
}

#[derive(Default)]
struct Row {
    jobs: u64,
    queued: u64,
    running: u64,
    succeeded: u64,
    failed: u64,
    other: u64,
    usage: UsageTotals,
    cost: f64,
    unpriced: u64,
    queue_wait: Vec<u64>,
    duration: Vec<u64>,
}

impl Row {
    fn add(&mut self, job: &Job, prices: Option<&Prices>) {
        self.jobs += 1;
        match job.state {
            JobState::Queued => self.queued += 1,
            JobState::Running => self.running += 1,
            JobState::Succeeded => self.succeeded += 1,
            JobState::Failed => self.failed += 1,
            JobState::Dequeued | JobState::Expired => self.other += 1,
        }
        if let Some(dispatched) = job.dispatched_at_ms {
            let ready = job.not_before_ms.unwrap_or(0).max(job.submitted_at_ms);
            self.queue_wait.push(dispatched.saturating_sub(ready));
            if let Some(finished) = job.finished_at_ms {
                self.duration.push(finished.saturating_sub(dispatched));
            }
        }
        // Only complete usage is totalled and priced; the rest is counted in `usage`.
        let Some(usage) = self.usage.add(job.token_usage.as_ref()) else {
            return;
        };
        if let Some(prices) = prices {
            match prices
                .get(&job.model_key())
                .and_then(|price| cost(usage, price))
            {
                Some(cost) => self.cost += cost,
                None => self.unpriced += 1,
            }
        }
    }

    fn view(&self, key: Value, priced: bool) -> Value {
        let mut row = json!({
            "key": key,
            "jobs": self.jobs,
            "queued": self.queued,
            "running": self.running,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "dequeuedOrExpired": self.other,
            "queueWaitMs": percentiles(&self.queue_wait),
            "runDurationMs": percentiles(&self.duration),
        });
        if let (Value::Object(row), Value::Object(usage)) = (&mut row, self.usage.view()) {
            row.extend(usage);
        }
        if priced {
            row["estimatedCostUsd"] = json!((self.cost * 1_000_000.0).round() / 1_000_000.0);
            row["unpricedJobs"] = json!(self.unpriced);
        }
        row
    }
}

/// Nearest-rank p50 and p95; null when no samples exist.
pub(crate) fn percentiles(samples: &[u64]) -> Value {
    if samples.is_empty() {
        return json!({ "p50": null, "p95": null, "samples": 0 });
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = |percent: usize| {
        let index = (percent * sorted.len()).div_ceil(100).saturating_sub(1);
        sorted[index.min(sorted.len() - 1)]
    };
    json!({ "p50": rank(50), "p95": rank(95), "samples": sorted.len() })
}

pub(crate) fn aggregate(jobs: &[Job], query: &StatsQuery<'_>) -> Value {
    let mut rows: BTreeMap<String, (Value, Row)> = BTreeMap::new();
    let mut totals = Row::default();
    for job in jobs.iter().filter(|job| {
        query
            .since_ms
            .is_none_or(|since| job.submitted_at_ms >= since)
            && query
                .until_ms
                .is_none_or(|until| job.submitted_at_ms < until)
    }) {
        let key = query
            .by
            .iter()
            .map(|dimension| (dimension.name(), dimension.value(job)))
            .collect::<Map<_, _>>();
        let key = Value::Object(key);
        let entry = rows
            .entry(key.to_string())
            .or_insert_with(|| (key, Row::default()));
        entry.1.add(job, query.prices);
        totals.add(job, query.prices);
    }
    let priced = query.prices.is_some();
    json!({
        "schema": "zeroshot.hub.stats/v1",
        "window": {
            "since": query.since_ms.map(rfc3339),
            "until": query.until_ms.map(rfc3339),
        },
        "groupBy": query.by.iter().map(Dimension::name).collect::<Vec<_>>(),
        "rows": rows.into_values().map(|(key, row)| row.view(key, priced)).collect::<Vec<_>>(),
        "totals": totals.view(json!({}), priced),
    })
}
