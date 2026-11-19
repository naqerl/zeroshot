use std::collections::BTreeMap;
use std::sync::Arc;

use super::{
    CostTimeComparison, EvalData, EvalError, QualityCostFit, TraitMatrix,
    manifest::validate_scorer_name,
};

/// Stored data plus derived matrix facts handed to a scorer.
pub struct ScorerInput<'a> {
    pub data: &'a EvalData,
    pub matrix: &'a TraitMatrix,
    pub cost_time: &'a CostTimeComparison,
    pub quality: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScorerOutput {
    pub name: String,
    pub score: f64,
    pub quality: f64,
    pub fit: Option<QualityCostFit>,
    pub notes: Vec<String>,
}

/// A named scorer that can be recomputed later from stored data alone.
pub trait Scorer: Send + Sync {
    fn name(&self) -> &str;
    fn score(&self, input: &ScorerInput<'_>) -> Result<ScorerOutput, EvalError>;
}

/// Identity scorer for evals that only want the trait matrix.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoneScorer;

impl Scorer for NoneScorer {
    fn name(&self) -> &str {
        "none"
    }

    fn score(&self, input: &ScorerInput<'_>) -> Result<ScorerOutput, EvalError> {
        Ok(ScorerOutput {
            name: self.name().to_owned(),
            score: input.quality,
            quality: input.quality,
            fit: None,
            notes: vec!["identity scorer: aggregate score equals Q".to_owned()],
        })
    }
}

/// Quality per relative cost, with a least-squares fit over arm cost ratios.
#[derive(Clone, Copy, Debug, Default)]
pub struct QualityVsCostScorer;

impl Scorer for QualityVsCostScorer {
    fn name(&self) -> &str {
        "quality-vs-cost"
    }

    fn score(&self, input: &ScorerInput<'_>) -> Result<ScorerOutput, EvalError> {
        let fit = fit_quality_cost(input.cost_time);
        let cost_ratios = input
            .cost_time
            .arms
            .iter()
            .filter_map(|arm| arm.cost_ratio)
            .collect::<Vec<_>>();
        let mean_cost_ratio = if cost_ratios.is_empty() {
            1.0
        } else {
            cost_ratios.iter().sum::<f64>() / cost_ratios.len() as f64
        };
        let denominator = if mean_cost_ratio > 0.0 {
            mean_cost_ratio
        } else {
            1.0
        };
        Ok(ScorerOutput {
            name: self.name().to_owned(),
            score: input.quality / denominator,
            quality: input.quality,
            fit,
            notes: vec![
                "aggregate score is Q divided by mean(C / Cbase) over arms".to_owned(),
                "fit is Q = a + b * (C / Cbase) by ordinary least squares".to_owned(),
            ],
        })
    }
}

/// The baseline/candidate ratio `S_candidate / S_baseline` with `S = Q / (C^a * T^b)` from means.
#[derive(Clone, Copy, Debug, Default)]
pub struct BaselineRatioScorer;

impl Scorer for BaselineRatioScorer {
    fn name(&self) -> &str {
        "baseline-ratio"
    }

    fn score(&self, input: &ScorerInput<'_>) -> Result<ScorerOutput, EvalError> {
        let comparison = super::baseline::compare(input.data);
        let ratio = comparison.as_ref().and_then(|comparison| comparison.ratio);
        let mut notes = vec![
            "aggregate score is S_candidate / S_baseline with S = Q / (C^a * T^b) from the means"
                .to_owned(),
        ];
        match (&comparison, ratio) {
            (None, _) => notes.push(
                "no baseline/candidate pair: the manifest needs two arms; aggregate score is 0"
                    .to_owned(),
            ),
            (Some(_), None) => {
                notes.push("the ratio is undefined; aggregate score is 0".to_owned())
            }
            _ => {}
        }
        Ok(ScorerOutput {
            name: self.name().to_owned(),
            score: ratio.unwrap_or(0.0),
            quality: input.quality,
            fit: None,
            notes,
        })
    }
}

/// Registry of named scorers. Later slices can register additional implementations.
#[derive(Default)]
pub struct ScorerRegistry {
    scorers: BTreeMap<String, Arc<dyn Scorer>>,
}

impl ScorerRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn builtin() -> Self {
        let mut registry = Self::new();
        let _ = registry.register(Arc::new(QualityVsCostScorer));
        let _ = registry.register(Arc::new(NoneScorer));
        let _ = registry.register(Arc::new(BaselineRatioScorer));
        registry
    }

    pub fn register(&mut self, scorer: Arc<dyn Scorer>) -> Result<(), EvalError> {
        let name = scorer.name().to_owned();
        validate_scorer_name(&name)?;
        if self.scorers.contains_key(&name) {
            return Err(EvalError::DuplicateScorer(name));
        }
        self.scorers.insert(name, scorer);
        Ok(())
    }

    pub fn require(&self, name: &str) -> Result<&Arc<dyn Scorer>, EvalError> {
        self.scorers
            .get(name)
            .ok_or_else(|| EvalError::UnknownScorer(name.to_owned()))
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.scorers.keys().map(String::as_str)
    }
}

fn fit_quality_cost(cost_time: &CostTimeComparison) -> Option<QualityCostFit> {
    let points = cost_time
        .arms
        .iter()
        .filter_map(|arm| arm.cost_ratio.map(|ratio| (ratio, arm.quality)))
        .collect::<Vec<_>>();
    if points.len() < 2 {
        return None;
    }
    let count = points.len() as f64;
    let mean_x = points.iter().map(|(x, _)| x).sum::<f64>() / count;
    let mean_y = points.iter().map(|(_, y)| y).sum::<f64>() / count;
    let mut denominator = 0.0;
    let mut numerator = 0.0;
    for (x, y) in &points {
        denominator += (x - mean_x) * (x - mean_x);
        numerator += (x - mean_x) * (y - mean_y);
    }
    let (a, b) = if denominator.abs() <= f64::EPSILON {
        (mean_y, 0.0)
    } else {
        let b = numerator / denominator;
        (mean_y - b * mean_x, b)
    };
    if !a.is_finite() || !b.is_finite() {
        return None;
    }
    Some(QualityCostFit { a, b })
}
