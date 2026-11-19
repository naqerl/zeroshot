# Talk evals: baseline vs candidate

Two evals for "SDLC Pipelines, or Stop Babysitting Your Agents". Each one compares a baseline
graph with a candidate graph on one task and reports, per run, wall time `T` (minutes), cost `C`
(USD), and quality `Q` (passed traits / judged traits, 0..1).

| Term | Meaning |
| --- | --- |
| Task | The input prompt (`arms.prompt`, sent as `{"task": ...}`). |
| Baseline | `T` and `C` of implementing the task before the change (`software-change`). |
| Candidate | `T` and `C` after the change (`graphs/<candidate>.json`). |
| Acceptance | The `[[criteria]]` traits, judged pass/fail by the `eval-judge` graph. |

Flow: select a task, write the traits, run the baseline N times, run the candidate N times, score.

```text
S = Q / (C^a * T^b)      a, b in [0, 1], default 0.5
meanRatio   = S_candidate / S_baseline   (each S from the arm's mean T, C, Q)
medianRatio = S_candidate / S_baseline   (each S from the arm's median T, C, Q)
```

A ratio above 1 means the candidate is better. `score.json`, `table.csv`, and `report.md` carry
both ratios.

## Files

- `eval1-ci-before-review.toml`: Eval 1, "Run CI before any review turn". Expected: lower cost.
- `eval2-delete-comments.toml`: Eval 2, "Delete all the comments" (agents make excuses in
  comments instead of doing the refactor). Expected: higher quality at a similar time and cost.
- `graphs/*.json`: candidate graphs. Each is `zeroshot template show software-change --push`
  with one extra worker instruction. Regenerate them with `./make-graphs.sh [zeroshot-binary]`
  (needs `jq`).
- `runtime.json`: the uniform agent runtime that every arm and the judge use. Set the harness,
  provider, and model here.
- `pareto.py`: plots cost vs time and cost vs quality from an export.

## Dry run (no providers, no cost)

The manifests use `runner = "fake"`, a synthetic runner with deterministic jitter, so the whole
pipeline can be checked offline:

```bash
zeroshot eval --directory /tmp/evals run zeroshot/fixtures/eval/talk/eval1-ci-before-review.toml --id eval1-dry
zeroshot eval --directory /tmp/evals export eval1-dry --output /tmp/evals/eval1
python3 zeroshot/fixtures/eval/talk/pareto.py /tmp/evals/eval1   # needs matplotlib
```

Dry-run numbers come from `[arms.knobs]` and are only placeholders. The dry run uses a synthetic
judge that calls no model.

## Real runs

```bash
zeroshot eval --directory /path/to/store \
  run zeroshot/fixtures/eval/talk/eval1-ci-before-review.toml \
  --runner local --cwd /path/to/checkout --id eval1
```

- **What `local` does.** Every sample is one `zeroshot run` in the `--cwd` checkout, run
  sequentially.
  - It uses `--template software-change --delivery push` or `--graph`, plus the arm's
    `uniformRuntimeConfig` and `runArgs`. It goes through the same profile, review-input and
    admission checks as the CLI.
- **Delivery and capture.** Each run pushes its own run branch. The eval then fetches the
  delivered `headRevision` from `options.captureRemote` (default: the checkout's `origin`) and
  stores `base..head` as the sample's patch for the judge. Set `GH_TOKEN` for a private HTTPS
  remote.
- **Store location.** Keep `--directory`, the store, outside the checkout so that `.zeroshot/eval`
  never dirties the task's workspace.
- **Deadline.** `options.deadlineMinutes` stops a sample that runs too long. The sample is
  recorded as failed.
- **Judge.** After the arms finish, each sample's patch goes to the `eval-judge` graph, which is
  submitted as a normal `zeroshot run` (see "Judge" below).
- **Cost.** Expect N × 2 full `software-change` runs plus one judge run per sample. Start with
  `runsPerArm = 1` (the manifests use 5).

Platform runs are not driven by `eval run`. Produce capture directories elsewhere
(`sample.json` with `costUsd` plus `patch.diff`) and import them with `zeroshot eval judge-captures`.

## Judge

`eval-judge` is a built-in graph template like `software-change`:

```bash
zeroshot template show eval-judge
```

- **Input.** `task` (the arm prompt), `traits` (a JSON array of `{id, description}` from
  `[[criteria]]`), `patch` (the captured diff), and `verdicts: []`, an empty placeholder that the
  graph fills.
- **Output.** `{"traits": [{"id": "...", "pass": true, "reason": "..."}]}`, with exactly one entry
  per trait. The parser is strict: a missing, duplicate, or unknown trait id, a non-boolean
  `pass`, or an empty `reason` fails the judgement and names the field.
- **Score.** `Q = passed / total` (weighted by `[[criteria]].weight`; equal weights by default).
- **Model.** The judge runs through the normal harness with whatever `judgeUniformRuntimeConfig`
  (here `runtime.json`), `judgeRuntimeConfig`, or a profile in `judgeRunArgs` configures. No judge
  provider or model is built in.
- **Runner.** `judgeRunner` defaults to `synthetic` for `runner = "fake"` and `local` otherwise.
  `--judge-runner` overrides it. `synthetic` answers from the fake runner's recorded verdicts and
  never calls a model.

Check that the template materializes, without running it:

```bash
echo '{"task":"t","traits":"[]","patch":"","verdicts":[]}' > /tmp/judge-input.json
zeroshot run --title judge --template eval-judge \
  --uniform-runtime-config zeroshot/fixtures/eval/talk/runtime.json \
  --input /tmp/judge-input.json --validate-only
```

A custom judge is any graph JSON with the same input and output, set with
`judgeTemplate = "path/to/judge.json"`. It may also declare `sample` (the sample JSON) and
`checkout` inputs.

## Scoring and export

```bash
zeroshot eval --directory /path/to/store score eval1 --scorer baseline-ratio --cost-weight 0.5 --time-weight 0.5
# Price overrides, USD per 1M tokens; each wins over the manifest for that token kind:
zeroshot eval --directory /path/to/store score eval1 --scorer baseline-ratio \
  --price-input 3 --price-output 15 --price-cache-read 0.3 --price-cache-write 3.75
zeroshot eval --directory /path/to/store export eval1 --output out/   # score.json, table.csv, runs.csv
```

- **`table.csv`.** The slide table: one row per run pair (`t_base_min,c_base,q_base,s_base,
  t_min,c,q,s`), then `mean` and `median` rows, then `ratio_mean` and `ratio_median` rows (the
  ratio is in the `s` column).
- **`runs.csv`.** One row per sample, for Pareto plots: status, time, cost, its unit and source,
  quality, S, traits, and tokens.
- **`score.json`.** Schema `zeroshot.eval-score/v1`. It holds the means, medians, both S values,
  `meanRatio`, `medianRatio`, and notes.
- **Report.** `report.md` and `report.json` contain the same section, "Baseline vs candidate".

Rules:

- **Cost `C`.** The source is chosen in this order:
  1. a recorded `costUsd` (imported captures);
  2. otherwise `C = (input × input price + output × output price + cacheRead × cacheRead price
     + cacheWrite × cacheWrite price) / 1M`, with prices in USD per 1M tokens from
     `--price-*`, then `[arms.pricing]`, then `[options.pricing]`, per field;
  3. otherwise the whole eval falls back to millions of tokens (`cost_unit = mtok`). A note names
     each arm and each missing price, for example `arm baseline used tokens without a price
     (output)`.

  There is no default price: a token kind with nonzero tokens needs its price. Cache tokens
  without a read/write split are priced only when `cacheRead` equals `cacheWrite`. Judge cost is
  not part of `C`. The values in the talk manifests are placeholders marked "set by user";
  replace them with the prices of the model in `runtime.json`.
- **Time `T`.** Wall time from submit to the terminal state, including queue wait and local
  preparation.
- **Quality `Q`.** The weighted share of passed traits (`pass: true`) among the judged traits.
- **Failed and skipped runs.**
  - A failed run counts with `Q = 0` and its measured `T` and `C`.
  - A run that failed before starting (zero wall time, e.g. a missing executable) and a skipped
    run are excluded from the means.
  - A succeeded but unjudged run is excluded from mean `Q` only.
  - Each of these cases adds a note.
- **Arm selection.** `[options.score]` picks the arms with `baseline` / `candidate`. The default
  is the first two arms.

## Manifest format

```toml
schema = "zeroshot.eval/v1"
name = "eval1-ci-before-review"
runsPerArm = 5

[options]
runner = "fake"                # or "local"; `--runner` overrides it
scorer = "baseline-ratio"
judgeTemplate = "eval-judge"
judgeUniformRuntimeConfig = "runtime.json"  # the judge's harness, provider, and model
# judgeRunner = "local"                     # default: "synthetic" for runner = "fake", else "local"
# judgeRunArgs = ["--profile", "judge"]     # or a profile instead of the runtime config
deadlineMinutes = 90
# captureRemote = "https://github.com/owner/repo.git"

[options.score]
a = 0.5
b = 0.5
baseline = "baseline"
candidate = "ci-before-review"

[options.pricing]   # USD per 1M tokens. Placeholders: set all four to your model's prices.
input = 3.0         # set by user: uncached input (read) tokens
output = 15.0       # set by user: output (write) tokens
cacheRead = 0.3     # set by user: cache-read input tokens
cacheWrite = 3.75   # set by user: cache-write (creation) input tokens

[[arms]]
id = "baseline"
template = "software-change"
delivery = "push"
uniformRuntimeConfig = "runtime.json"
prompt = "..."

[[arms]]
id = "ci-before-review"
graph = "graphs/ci-before-review.json"
uniformRuntimeConfig = "runtime.json"
prompt = "..."
# [arms.pricing] overrides single fields for this arm, e.g. output = 10.0
# runArgs = ["--no-environment"]   # extra `zeroshot run` flags (not --target/--detach/--validate-only)

[[criteria]]
id = "ci-green"
description = "Formatter, linter, and the full test suite pass on the delivered change."
```

Paths in a manifest are resolved against the manifest's directory.
