---
description: "Run graph evals with judged criteria, compare two arms as X and Y, and read per-trait reasoning in the stored report."
---

# Evaluate graphs and compare arms

An eval runs one or more graph arms over the same task, records every attempt, and produces a
report from the stored samples. The judge turns a captured patch into a score and short reasoning
for every manifest criterion, so a report explains *why* one arm scored above another.

The synthetic fixture is the smallest complete example:

```bash
zeroshot eval run zeroshot/fixtures/eval/synthetic.toml --directory . --id synthetic-fixture
zeroshot eval report synthetic-fixture
zeroshot eval score synthetic-fixture --scorer quality-vs-cost
```

Every run keeps `manifest.toml`, `eval.json`, `samples/<arm>-<run>.json`, `patches/<arm>-<run>.patch`,
`report.json`, and `report.md` under `.zeroshot/eval/<id>/`.

## Judgement contract

A judge inspects one captured `patch.diff`, the delivered checkout, and `sample.json` metadata when
the platform capture provides them, then returns one judgement per criterion. Scores use an
inclusive **1..=5** scale; **3 is the pass threshold**. Reasoning is short prose that refers to the
patch or surrounding code. Errors are concrete defects, not parser or provider noise.

```json
{
  "judgements": [
    {
      "criterion": "correctness",
      "score": 4,
      "reasoning": "The patch adds the missing guard and a regression test in the parser module.",
      "errors": []
    }
  ]
}
```

The parser is strict and names the offending field. A missing `score`, a score outside `1..=5`,
empty `reasoning`, an empty error string, a duplicate criterion, or an unknown criterion is rejected
with an error such as:

```text
judge response field `judgements[0].score` is invalid: must be between 1 and 5 inclusive
```

The `criteria` array name and the `id` judgement key are accepted as aliases for `judgements` and
`criterion`. Every manifest criterion must appear exactly once.

## Judge model configuration

The model is an opaque provider-owned identifier. Resolution order is:

1. the run-level override (`--judge-model`, or `JudgeOptions::with_model` from an embedding runner);
2. the arm's `judgeModel`;
3. the manifest's `options.judgeModel`.

The judge template is a run-level override (CLI flag or `JudgeOptions`) over the manifest's
`options.judgeTemplate`; arms configure models, not templates. `eval-judge` names the built-in JSON
graph; any other value is a path to a caller-owned graph JSON with the same input/output shape. A
template path passed to `eval run` resolves against the manifest directory and the resolved path is
stored with the eval. An `eval judge --judge-template` path resolves against the invoking folder.

```toml
schema = "zeroshot.eval/v1"
name = "judged-synthetic"
runsPerArm = 3

[options]
scorer = "quality-vs-cost"
runner = "fake"
judgeModel = "provider/default-judge"
judgeTemplate = "eval-judge"
# Required for judging: an authored, secret-free native-v2 runtime plan.
judgeRuntimeConfig = "judge-runtime.json"

[[arms]]
id = "baseline"
template = "single-worker"
prompt = "Implement the synthetic task."
input = { task = "Implement the synthetic task." }
judgeModel = "provider/baseline-judge"

[[arms]]
id = "candidate"
template = "single-worker"
prompt = "Implement the synthetic task with the candidate graph."
input = { task = "Implement the synthetic task with the candidate graph." }

[[criteria]]
id = "correctness"
description = "The change satisfies the requested behavior."
weight = 2.0

[[criteria]]
id = "efficiency"
description = "The change avoids unnecessary work."
weight = 1.0
```

The stored report records the exact model and executor (`graph` for a real judge) on each judged
sample and in the matrix's `Judge` column, so a judged report is reproducible. A succeeded sample
with no captured patch is an error rather than a silent skip. Failed or skipped samples have no
patch and are skipped by the judge rather than judged from missing evidence.

`zeroshot eval run` accepts the same `--judge-model` and `--judge-template` overrides and judges the
new run when any effective judge configuration exists, including the manifest and per-arm defaults.
It still requires `options.judgeRuntimeConfig`; otherwise the run reports the explicit judge error.
`zeroshot eval judge <id>` judges an existing store from its captured artifacts.

To execute the judge graph with a real model, set `options.judgeRuntimeConfig` to an authored
secret-free native-v2 runtime plan. Relative paths resolve against the manifest file (the stored
eval's `manifest.toml` when judging an existing eval) and the resolved absolute path is stored with
the eval. The CLI then submits the selected template through the native-v2 local runner and applies
the effective per-sample judge model to every agent binding before submission.

A judge run without `judgeRuntimeConfig` is an explicit `judgeError` that names the missing
setting. The CLI never falls back to the synthetic `FixtureJudgeGraphExecutor`, so
`eval judge --judge-model` cannot relabel another model's recorded scores as its own. The fixture
executor remains available only behind the explicit unit-test entrypoint. Embedding runners can
also pass a provider-backed `native_v2_eval::Judge` (or a `TemplateJudge` with their own
`JudgeGraphExecutor`) directly to `judge_eval`.

## Judging platform capture directories

Slice-2 captures live in per-sample directories rather than the slice-1 store. Point `eval
judge-captures` at a capture root laid out as `<captures>/<arm-id>/<sample>/`, where each sample
directory contains `sample.json` and `patch.diff`:

```toml
# eval-with-judge.toml: the fixture manifest plus a real judge
[options]
judgeModel = "provider/default-judge"
judgeTemplate = "eval-judge"
judgeRuntimeConfig = "judge-runtime.json" # required for a real judge
```

```bash
zeroshot eval judge-captures ./eval-with-judge.toml \
  --captures /path/to/captures --directory . --id captured-judged
```

The command reads the manifest's criteria and arms, imports every capture directory, reads the
flattened `sample.json`, the captured patch path, and any recorded `judge` checkout, and writes the
judged samples and report to the eval store. The manifest must declare `options.judgeRuntimeConfig`
for the judge to run; without it the command fails with the same explicit `judgeError` as
`eval judge`. The provider-backed judge also needs the captured `judge` checkout, so a capture
without one is reported as a judge error rather than scored from the patch alone. Run indices come
from the numeric suffix of each sample id, so a missing directory cannot shift the X/Y pairing; ids
without a numeric suffix and duplicate ids or run indices are rejected. Failed and skipped captures
without a patch are retained but not judged and are reported as skipped, and captured
patch/checkout paths are required to stay inside the capture directory. It never re-runs an arm.

## Custom judge graphs

`--judge-template eval-judge` (or `judgeTemplate = "eval-judge"`) uses
`zeroshot/src/native_v2_templates/eval_judge.json`. The template is a real native-v2 graph: its
judge step receives `criterion`, `criterionDescription`, `patch`, `sample`, the effective `model`,
and the delivered `checkout` path. It returns `criterion`, `score`, `reasoning`, and `errors`
through an explicit succeed terminal. The built-in template additionally reserves an internal
required numeric `score` placeholder (`0.0`) in its initial input so the root group can materialize
number state before the worker writes the real score. Custom graphs receive only the fields their
own `initialInput` declares from this standard set, so they do not need to accept the placeholder;
declare `score` only when using the same materialization pattern. Point the same setting at your own
JSON graph to change the instructions, worker, or surrounding nodes. `TemplateJudge` runs the
selected graph through a `JudgeGraphExecutor`; the CLI supplies the provider-backed executor, and
an embedding runner can supply its own. A runtime plan that declares connection requirements
currently fails with a clear error because eval judge does not resolve connection values; use an
ambient native-login lane or supply the connections through an embedding runner.

The provider-backed judge runs with the sample's delivered checkout as its working directory and
also receives that path as the `checkout` input, so it can inspect surrounding code, call sites,
and tests, not only the patch. The template instructions say to do so. If a checkout cannot be
produced for a succeeded sample, the judge fails with a `judgeError` instead of inventing a verdict.
Only capture metadata and run artifacts are written to a separate private scratch directory; the
live eval workspace is never used as the judge's working directory.

The runner reads capture artifacts; it never re-runs an eval arm to judge it. Slice-1 samples use
`patches/<sample>.patch`; platform captures use `<sample>/sample.json` plus `<sample>/patch.diff`.
Both slice-2 flattened capture documents and older nested `outcome` documents are accepted.

## Reading the X/Y comparison

The report's `comparison` object compares the first two manifest arms. The first arm is X and the
second is Y. For every criterion the report includes:

- each arm's mean judged score;
- the paired difference (Y minus X) over run indices where both arms have a score;
- up to three outlier pairs with the largest absolute difference, including both judges' reasoning
  and model identifiers.

A positive difference favors Y. The section is omitted unless at least one criterion has a scored
pair. The Markdown report renders the same data:

```text
## X vs Y

X is `baseline`, Y is `candidate`. Difference is Y - X on the judge's 1..=5 scale; positive values favor Y.

| Trait | X mean | Y mean | Paired difference | Pairs |
| --- | ---: | ---: | ---: | ---: |
| correctness | 5.00 | 5.00 | 0.00 | 2 |
| efficiency | 1.00 | 5.00 | 4.00 | 2 |

### Outlier reasoning

- correctness / run 0 / `baseline-0000` -> `candidate-0000`: X 5.00, Y 5.00, difference +0.00
  - X (`provider/default-judge`): The patch keeps the parser guard and the checkout's call sites still pass the existing tests.
  - Y (`provider/default-judge`): The patch keeps the parser guard and the checkout's call sites still pass the existing tests.
- efficiency / run 0 / `baseline-0000` -> `candidate-0000`: X 1.00, Y 5.00, difference +4.00
  - X (`provider/default-judge`): The patch adds an unnecessary second pass over the captured input.
  - Y (`provider/default-judge`): The patch reaches the same result in one pass and the checkout shows no duplicate work.
```

This excerpt is a provider-backed judged report. A plain synthetic fixture run instead records
sample evidence from the fixture runner; its scores are synthetic sample evidence, not a model
judgement.

The `Trait rates` table also lists each criterion's mean judge score and the number of scored
samples, so aggregate per-trait means remain visible outside the X/Y section. Use the outlier
reasoning to check that the score difference comes from the patch, not from a
different task, a missing capture, or a judge that answered outside the contract. Up to three
pairs per criterion are retained even when their scores are equal, so reasoning stays visible.
Actionable defects are listed in the `Judge errors` section beside the reasoning.

## Worked example on the synthetic fixture

The checked-in `synthetic.toml` runs two arms with three runs each. Both arms pass correctness;
the baseline arm fails efficiency while the candidate arm passes it. The synthetic runner records 5
for a pass and 1 for a fail, so the plain fixture run's X/Y table reports a 0 difference for
correctness and +4 for efficiency:

```bash
zeroshot eval run zeroshot/fixtures/eval/synthetic.toml --directory . --id synthetic-fixture
```

Those values are synthetic sample evidence from the fixture runner, not a model judgement, and they
are not labelled with a judge model. The fixture has no delivered `judge` checkout, so it cannot be
provider-judged in place. To produce the judged X/Y excerpt above, run a backend arm whose capture
includes a `judge` checkout, or import those captures with `eval judge-captures` and a manifest that
carries `judgeModel`, `judgeTemplate`, and the required `judgeRuntimeConfig`. A provider-backed
judge fails with an explicit `judgeError` when the runtime or checkout is unavailable instead of
relabelling the synthetic evidence. The JSON report and Markdown are rebuilt from the stored samples
and report, so `zeroshot eval show`, `report`, and `score` never depend on the process that ran the
eval.
