//! Provider-reported token usage of finished jobs: reading it, checked totals, and cost.
//!
//! This module owns usage completeness for every hub surface (`hub stats`, eval reports, and
//! `/metrics`). Only reported usage marked `complete` is totalled or priced; incomplete usage is
//! counted separately, unreadable usage is kept as explicitly invalid, and a total that would
//! overflow is withheld rather than published as a partial number.

use openengine_cluster_protocol::TokenCount;
pub(crate) use openengine_cluster_protocol::TokenUsage;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::stats::ModelPrice;

/// Usage recorded on a finished job.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub(crate) enum JobUsage {
    /// The run's `metadata.tokenUsage`, exactly as the target reported it.
    Reported(TokenUsage),
    /// The run reported usage the hub could not read; it is never totalled or priced.
    Invalid { invalid: String },
}

impl JobUsage {
    /// The usage in a run status projection; `None` when the run reported none.
    pub(crate) fn from_status(status: &Value) -> Option<Self> {
        let usage = status.pointer("/status/metadata/tokenUsage")?;
        if usage.is_null() {
            return None;
        }
        Some(match serde_json::from_value::<TokenUsage>(usage.clone()) {
            Ok(usage) => Self::Reported(usage),
            Err(error) => Self::Invalid {
                invalid: format!("unreadable tokenUsage: {error}"),
            },
        })
    }

    /// Usage that may be totalled and priced: reported and complete.
    pub(crate) const fn complete(&self) -> Option<&TokenUsage> {
        match self {
            Self::Reported(usage) if usage.complete => Some(usage),
            _ => None,
        }
    }
}

/// Token counts of one usage or of a checked total. A cache counter is present only when every
/// counted usage reported it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Tokens {
    pub input: u64,
    pub output: u64,
    pub cache_read: Option<u64>,
    pub cache_creation: Option<u64>,
}

impl Tokens {
    pub(crate) fn of(usage: &TokenUsage) -> Self {
        Self {
            input: usage.input_tokens.get(),
            output: usage.output_tokens.get(),
            cache_read: usage.cache_read_input_tokens.map(TokenCount::get),
            cache_creation: usage.cache_creation_input_tokens.map(TokenCount::get),
        }
    }

    /// Checked sum; `None` when any counter would overflow.
    pub(crate) fn checked_add(self, other: Self) -> Option<Self> {
        let optional = |left: Option<u64>, right: Option<u64>| match (left, right) {
            (Some(left), Some(right)) => left.checked_add(right).map(Some),
            _ => Some(None),
        };
        Some(Self {
            input: self.input.checked_add(other.input)?,
            output: self.output.checked_add(other.output)?,
            cache_read: optional(self.cache_read, other.cache_read)?,
            cache_creation: optional(self.cache_creation, other.cache_creation)?,
        })
    }

    /// Input plus output tokens, `None` on overflow.
    pub(crate) fn input_and_output(self) -> Option<u64> {
        self.input.checked_add(self.output)
    }

    /// The four counters in [`TOKEN_KINDS`] order, absent cache counters as zero.
    pub(crate) fn kinds(self) -> [u64; 4] {
        [
            self.input,
            self.output,
            self.cache_read.unwrap_or(0),
            self.cache_creation.unwrap_or(0),
        ]
    }
}

/// Metric kinds of [`Tokens::kinds`].
pub(crate) const TOKEN_KINDS: [&str; 4] = ["input", "output", "cache_read", "cache_creation"];

/// Estimated USD cost of complete usage; cache counters a provider did not report cost nothing.
#[allow(clippy::cast_precision_loss)]
pub(crate) fn cost(usage: &TokenUsage, price: &ModelPrice) -> Option<f64> {
    if !usage.complete {
        return None;
    }
    let per_million = |tokens: u64, price: f64| tokens as f64 / 1_000_000.0 * price;
    let tokens = Tokens::of(usage);
    Some(
        per_million(tokens.input, price.input)
            + per_million(tokens.output, price.output)
            + per_million(tokens.cache_read.unwrap_or(0), price.cache_read)
            + per_million(tokens.cache_creation.unwrap_or(0), price.cache_creation),
    )
}

/// Checked usage totals over many jobs.
#[derive(Clone, Debug, Default)]
pub(crate) struct UsageTotals {
    tokens: Option<Tokens>,
    overflowed: bool,
    with_usage: u64,
    incomplete: u64,
    invalid: u64,
}

impl UsageTotals {
    /// Count one job's usage. Returns the complete usage it totalled, if any.
    pub(crate) fn add<'a>(&mut self, usage: Option<&'a JobUsage>) -> Option<&'a TokenUsage> {
        let usage = usage?;
        self.with_usage += 1;
        let Some(complete) = usage.complete() else {
            match usage {
                JobUsage::Reported(_) => self.incomplete += 1,
                JobUsage::Invalid { .. } => self.invalid += 1,
            }
            return None;
        };
        let tokens = Tokens::of(complete);
        self.tokens = match self.tokens {
            None => Some(tokens),
            Some(total) => total.checked_add(tokens),
        };
        self.overflowed |= self.tokens.is_none();
        Some(complete)
    }

    /// Totals of complete usage; `None` when nothing was counted or a total overflowed.
    pub(crate) const fn tokens(&self) -> Option<Tokens> {
        if self.overflowed { None } else { self.tokens }
    }

    /// The `hub stats` usage fields.
    pub(crate) fn view(&self) -> Value {
        let tokens = self.tokens();
        let zero = (!self.overflowed).then_some(0);
        let total = |count: fn(Tokens) -> Option<u64>| tokens.map_or(zero, count);
        json!({
            "inputTokens": total(|tokens| Some(tokens.input)),
            "outputTokens": total(|tokens| Some(tokens.output)),
            "cacheReadInputTokens": tokens.and_then(|tokens| tokens.cache_read),
            "cacheCreationInputTokens": tokens.and_then(|tokens| tokens.cache_creation),
            "usageOverflowed": self.overflowed,
            "jobsWithUsage": self.with_usage,
            "jobsWithIncompleteUsage": self.incomplete,
            "jobsWithInvalidUsage": self.invalid,
        })
    }
}

#[cfg(test)]
pub(crate) fn reported(input: u64, output: u64, cache_read: Option<u64>) -> JobUsage {
    let count = |value| TokenCount::new(value).expect("token count");
    JobUsage::Reported(TokenUsage {
        input_tokens: count(input),
        output_tokens: count(output),
        cache_read_input_tokens: cache_read.map(count),
        cache_creation_input_tokens: None,
        complete: true,
    })
}
