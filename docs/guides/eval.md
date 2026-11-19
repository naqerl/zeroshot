---
description: "Run graph evals with pass/fail trait judgements, compare two arms as X and Y, and read per-trait reasons in the stored report."
---

# Evaluate graphs and compare arms

An eval runs one or more graph arms over the same task, records every attempt, and produces a
report from the stored samples. The judge reads a captured patch and returns a pass/fail verdict
with a short reason for every manifest criterion (trait), so a report explains *why* one arm
scored above another.

The synthetic fixture is the smallest complete example. It needs no provider:

```bash
zeroshot eval run zeroshot/fixtures/eval/synthetic.toml --directory . --id synthetic-fixture
zeroshot eval report synthetic-fixture
zeroshot eval judge synthetic-fixture
zeroshot eval score synthetic-fixture --scorer quality-vs-cost
```

Every run keeps `manifest.toml`, `eval.json`, `samples/<arm>-<run>.json`, `patches/<arm>-<run>.patch`,
`report.json`, and `report.md` under `.zeroshot/eval/<id>/`.

## The `eval-judge` graph

Judging is a ready-to-use graph template, shipped like `software-change`:

```bash
zeroshot template list                 # ... eval-judge
zeroshot template show eval-judge      # the graph JSON
```

The eval runner invokes it once per sample with this input:

| Field | Value |
| --- | --- |
| `task` | The arm's prompt. |
| `traits` | A JSON array, as a string, of `{"id", "description"}` from `[[criteria]]`. |
| `patch` | The captured diff. |
| `verdicts` | `[]`, an empty placeholder that the graph fills. |

The graph's terminal output is one verdict per trait:

```json
{
  "traits": [
    {
      "id": "correctness",
      "pass": true,
      "reason": "The patch adds the missing guard and a regression test in the parser module."
    }
  ]
}
```

The parser is strict and names the offending field. A missing `traits` array, a missing or
non-boolean `pass`, an empty `reason`, or a missing, duplicate, or unknown trait id is rejected
with an error such as:

```text
judge response field `traits[0].pass` is invalid: must be a boolean
```

Each verdict is stored on the sample (`passed`, `reasoning`, and `score` = 1 for pass, 0 for
fail). Quality is `Q = passed / total`, weighted by `[[criteria]].weight` (equal weights by
default).

Check that the template materializes without running it:

```bash
echo '{"task":"t","traits":"[]","patch":"","verdicts":[]}' > /tmp/judge-input.json
zeroshot run --title judge --template eval-judge --uniform-runtime-config runtime.json \
  --input /tmp/judge-input.json --validate-only
```

## Judge configuration

No judge provider or model is built in. The judge runs through the normal harness with whatever
the runtime configuration selects:

```toml
[options]
judgeTemplate = "eval-judge"               # or a path to a custom graph JSON
judgeRunner = "local"                      # default: "synthetic" for runner = "fake", else "local"
judgeUniformRuntimeConfig = "runtime.json" # zeroshot run --uniform-runtime-config
# judgeRuntimeConfig = "judge-runtime.json" # or an exact plan: zeroshot run --runtime-config
# judgeRunArgs = ["--profile", "judge"]     # or a profile, plus any other `zeroshot run` flags
```

- **`local`.** Each judgement is a real `zeroshot run --template eval-judge` (or `--graph <path>`)
  with the runtime flags above, using the sample's delivered checkout as its working directory so
  it can inspect surrounding code, call sites, and tests. Run artifacts and the judge input stay
  in private scratch outside the live workspace. It goes through the same profile and admission
  checks as the CLI, so the judge needs a runtime config or a profile. Selecting `local` without
  `judgeRuntimeConfig`, `judgeUniformRuntimeConfig`, or `judgeRunArgs` fails with an explicit judge
  error naming the missing `judgeRuntimeConfig`; it never falls back to the synthetic judge.
- **`synthetic`.** A dry-run judge that calls no model. It answers in the same output shape from
  the verdicts the fake runner recorded, and is labeled `synthetic` in the report's `Judge`
  column. A sample without recorded verdicts fails with a message that points to
  `judgeRunner = "local"`. It also refuses to re-label verdicts produced by a different judge
  executor, so re-judging a graph-judged sample as `synthetic` fails instead of mutating it.
- **Overrides.** `--judge-runner` and `--judge-template` on `eval run`, `eval judge`, and
  `eval judge-captures` override the manifest. Relative paths in a manifest resolve against the
  manifest directory, and the resolved path is stored with the eval. An `eval judge
  --judge-template` path resolves against the invoking folder.

`zeroshot eval run` judges the new run when any judge setting is present. `zeroshot eval judge <id>`
judges an existing store from its captured artifacts. A succeeded sample with no captured patch
is an error rather than a silent skip. Failed and skipped samples have no patch, so the judge
skips them rather than judging missing evidence. Embedding runners can pass their own
`native_v2_eval::Judge`, or a `TemplateJudge` with their own `JudgeGraphExecutor`, to
`judge_eval`.

## Judging platform capture directories

Slice-2 captures live in per-sample directories rather than the slice-1 store. Point `eval
judge-captures` at a capture root laid out as `<captures>/<arm-id>/<sample>/`, where each sample
directory contains `sample.json` and `patch.diff`:

```toml
# eval-with-judge.toml: the synthetic fixture plus a local judge runtime
[options]
judgeTemplate = "eval-judge"
judgeRunner = "local"
judgeRuntimeConfig = "judge-runtime.json"
```

```bash
zeroshot eval judge-captures ./eval-with-judge.toml \
  --captures /path/to/captures --directory . --id captured-judged --judge-runner local
```

The command reads the manifest's criteria and arms, imports every capture directory, reads the
flattened `sample.json`, the captured patch path, and any recorded `judge` checkout, and writes the
judged samples and report to the eval store. The manifest must supply a judge runtime (or a profile
through `judgeRunArgs`) for the local runner; the `synthetic` runner cannot import provider-backed
captures. A capture without a delivered checkout fails with an explicit judge error rather than
being scored from the patch alone. Run indices come from the numeric suffix of each sample id, so a
missing directory cannot shift the X/Y pairing; ids without a numeric suffix and duplicate ids or
run indices are rejected. Failed and skipped captures without a patch are retained but not judged
and are reported as skipped, and captured patch/checkout paths are required to stay inside the
capture directory. It never re-runs an arm.

## Custom judge graphs

Point `judgeTemplate` (or `--judge-template`) at your own graph JSON to change the instructions,
the worker, or the surrounding nodes. It must accept the input above and produce the same
`traits` output. It may also declare `sample` (the sample JSON as a string) and `checkout` (the
delivered checkout path). Each graph receives only the fields its own `initialInput` declares.
Provider-backed graph runs use the delivered checkout as their working directory, so they can
inspect surrounding code, call sites, and tests; run artifacts stay in private scratch. A succeeded
sample without a captured checkout is a judge error, not a fabricated verdict.

The runner reads capture artifacts; it never re-runs an eval arm to judge it. Slice-1 samples use
`patches/<sample>.patch`; platform captures use `<sample>/sample.json` plus `<sample>/patch.diff`.
Both slice-2 flattened capture documents and older nested `outcome` documents are accepted.

## Reading the X/Y comparison

The report's `comparison` object compares the first two manifest arms. The first arm is X and the
second is Y. For every criterion the report includes:

- each arm's pass rate (the mean of 1 for pass and 0 for fail);
- the paired difference (Y minus X) over run indices where both arms were judged;
- up to three outlier pairs with the largest absolute difference, with both judges' reasons.

A positive difference favors Y. The section is omitted unless at least one criterion has a judged
pair. The Markdown report renders the same data:

```text
## X vs Y

X is `baseline`, Y is `candidate`. Means are pass rates (1 = pass, 0 = fail); the difference is Y - X, and positive values favor Y.

| Trait | X mean | Y mean | Paired difference | Pairs |
| --- | ---: | ---: | ---: | ---: |
| correctness | 1.00 | 1.00 | 0.00 | 2 |
| efficiency | 0.00 | 1.00 | 1.00 | 2 |

### Outlier reasoning

- efficiency / run 0 / `baseline-0000` -> `candidate-0000`: X 0.00, Y 1.00, difference +1.00
  - X: fake judgement: efficiency failed for arm baseline run 0
  - Y: fake judgement: efficiency passed for arm candidate run 0
```

The `Trait rates` table also lists each criterion's pass rate over judged samples, so per-trait
results stay visible outside the X/Y section. Use the reasons to check that a difference comes from
the patch, not from a different task, a missing capture, or a judge that answered outside the
contract. Up to three pairs per criterion are kept even when they are equal, so reasons stay
visible. The `Judge errors` section lists defects that older imported captures recorded.

## Baseline vs candidate score

When an eval compares a baseline arm with a candidate arm, every report gets a
"Baseline vs candidate" section. Each run is reduced to wall time `T` (minutes), cost `C` (USD),
and quality `Q` (weighted passed traits / judged traits), and scored as:

```text
S = Q / (C^a * T^b)        a, b in [0, 1], default 0.5
meanRatio   = S_candidate / S_baseline   (each S from the arm's mean T, C, Q)
medianRatio = S_candidate / S_baseline   (each S from the arm's median T, C, Q)
```

```toml
[options]
scorer = "baseline-ratio"        # aggregate score = meanRatio
deadlineMinutes = 90             # local runner: fail a sample that runs longer

[options.score]
a = 0.5
b = 0.5
baseline = "baseline"            # defaults: the first two arms
candidate = "candidate"

# USD per 1M tokens, set by you: there is no default. [arms.pricing] overrides single fields
# per arm, and --price-input/--price-output/--price-cache-read/--price-cache-write override both.
[options.pricing]
input = 3.0       # uncached input (read) tokens
output = 15.0     # output (write) tokens
cacheRead = 0.3   # cache-read input tokens
cacheWrite = 3.75 # cache-write (creation) input tokens
```

`C` comes from one of these sources, in order:

1. a recorded `costUsd` in an imported capture's `sample.json`;
2. `(input × input + output × output + cacheRead × cacheRead + cacheWrite × cacheWrite) / 1M`,
   each token count times its own price;
3. millions of tokens, used when a measured sample has neither. A note names the arm and every
   missing price, for example `arm baseline used tokens without a price (output)`.

A price is needed only for a token kind with nonzero tokens, and prices are never guessed. Cache
tokens without a read/write split are priced only when `cacheRead` equals `cacheWrite`.

Rules for failed, skipped, and unjudged runs:

- A failed run counts with `Q = 0` and its measured `T` and `C`.
- A run that failed before starting is excluded, and so is a skipped one.
- A succeeded but unjudged run is excluded from mean `Q` only.

```bash
zeroshot eval score <id> --scorer baseline-ratio --cost-weight 0.5 --time-weight 0.5 \
  --price-input 3 --price-output 15 --price-cache-read 0.3 --price-cache-write 3.75
zeroshot eval export <id> --output out/    # score.json, table.csv (slide table), runs.csv (per sample)
```

The export files are:

- `table.csv`: one row per run pair, then `mean` and `median` rows, then `ratio_mean` and
  `ratio_median` rows (the ratio is in the `s` column).
- `runs.csv`: one row per sample, intended for Pareto plots.
- `score.json`: the `zeroshot.eval-score/v1` document, with `meanRatio` and `medianRatio`.

`zeroshot/fixtures/eval/talk/` has two complete evals, a candidate-graph generator, and
`pareto.py` for the cost vs time and cost vs quality plots.

`options.runner = "local"` (or `--runner local`) runs every sample as a real `zeroshot run` in
the `--cwd` checkout:

- Template arms use `delivery` (default `push`). Graph arms use their own delivery nodes.
- Arms may set `runtimeConfig` or `uniformRuntimeConfig`, plus extra `runArgs`.
- The delivered head revision is fetched from `options.captureRemote` (default `origin`) and
  diffed against the starting `HEAD` into the judged patch.
- `--runner fake` dry-runs the same manifest.

## Worked example on the synthetic fixture

The checked-in `synthetic.toml` runs two arms with three runs each. Both arms pass correctness;
the baseline arm fails efficiency while the candidate arm passes it. The synthetic judge records 1
for a pass and 0 for a fail, so the X/Y section reports a 0 difference for correctness and +1 for
efficiency:

```bash
zeroshot eval run zeroshot/fixtures/eval/synthetic.toml --directory . --id judged-synthetic
```

The fixture's `judgeTemplate` is enough, so the plain run is judged by the synthetic judge.
`.zeroshot/eval/judged-synthetic/report.md` then contains the trait matrix with a `Judge` column, the
X/Y means above, and the outlier reasoning that references each captured patch. The JSON report and
Markdown are rebuilt from the stored samples and report, so `zeroshot eval show`, `report`, and
`score` never depend on the process that ran the eval.
