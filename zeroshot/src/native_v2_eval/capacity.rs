//! Eval capacity selection and bounded scheduling.
//!
//! The platform protocol has no prescribed capacity field. A platform adapter may expose one
//! through [`CapacitySource`]; otherwise `--concurrency` is used, and the final fallback is one.
//! The selected value and its provenance are recorded with the eval.

use std::future::Future;

use futures_util::stream::{FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};

const MAX_CONCURRENCY: usize = 4_096;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CapacityError {
    #[error("concurrency must be between 1 and {MAX_CONCURRENCY}")]
    OutOfRange,
}

/// A platform-owned capacity advertisement.
///
/// Implementations return `None` when the target does not advertise a usable capacity. Returned
/// values outside the supported range are ignored rather than silently clamped, so an invalid
/// platform response falls back to the caller's explicit concurrency.
pub trait CapacitySource: Send + Sync {
    fn capacity(&self) -> Option<usize>;
}

/// A fixed capacity source, used by adapters that already decoded a platform response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StaticCapacity(pub usize);

impl CapacitySource for StaticCapacity {
    fn capacity(&self) -> Option<usize> {
        (self.0 > 0 && self.0 <= MAX_CONCURRENCY).then_some(self.0)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CapacitySourceKind {
    /// The selected target advertised a capacity value.
    Platform,
    /// The caller supplied `--concurrency`.
    Concurrency,
    /// Neither was available; the documented fallback is one.
    Fallback,
}

impl CapacitySourceKind {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Platform => "platform",
            Self::Concurrency => "concurrency",
            Self::Fallback => "fallback",
        }
    }
}

/// The effective eval concurrency and the provenance recorded as `capacitySource`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Capacity {
    pub concurrency: usize,
    pub capacity_source: CapacitySourceKind,
}

impl Capacity {
    #[must_use]
    pub const fn new(concurrency: usize, capacity_source: CapacitySourceKind) -> Self {
        Self {
            concurrency,
            capacity_source,
        }
    }
}

impl Default for Capacity {
    fn default() -> Self {
        Self::new(1, CapacitySourceKind::Fallback)
    }
}

/// Select `platform`, then `--concurrency`, then one.
pub fn resolve_capacity(
    platform: Option<&dyn CapacitySource>,
    concurrency: Option<usize>,
) -> Result<Capacity, CapacityError> {
    if let Some(value) = platform.and_then(CapacitySource::capacity) {
        return Ok(Capacity::new(value, CapacitySourceKind::Platform));
    }
    match concurrency {
        Some(value) if (1..=MAX_CONCURRENCY).contains(&value) => {
            Ok(Capacity::new(value, CapacitySourceKind::Concurrency))
        }
        Some(_) => Err(CapacityError::OutOfRange),
        None => Ok(Capacity::default()),
    }
}

/// Run at most `concurrency` futures at a time and return their results in input order.
///
/// The scheduler owns futures without spawning them, so it inherits the caller's cancellation and
/// runtime. It is the single bounded-concurrency primitive for sample execution.
pub async fn run_bounded<T, O, F, Fut>(
    items: Vec<T>,
    concurrency: usize,
    mut start: F,
) -> Result<Vec<(usize, O)>, CapacityError>
where
    F: FnMut(usize, T) -> Fut,
    Fut: Future<Output = O>,
{
    if !(1..=MAX_CONCURRENCY).contains(&concurrency) {
        return Err(CapacityError::OutOfRange);
    }
    let mut pending = items.into_iter().enumerate();
    let mut in_flight = FuturesUnordered::new();
    let mut completed = Vec::new();
    loop {
        while in_flight.len() < concurrency {
            let Some((index, item)) = pending.next() else {
                break;
            };
            let future = start(index, item);
            in_flight.push(async move { (index, future.await) });
        }
        if in_flight.is_empty() {
            break;
        }
        if let Some(result) = in_flight.next().await {
            completed.push(result);
        }
    }
    completed.sort_by_key(|(index, _)| *index);
    Ok(completed)
}

#[cfg(test)]
#[path = "capacity_test.rs"]
mod tests;
