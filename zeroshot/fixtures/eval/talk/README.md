# Talk evals: baseline vs candidate

Two evals for "SDLC Pipelines, or Stop Babysitting Your Agents". Each one compares a baseline
graph with a candidate graph on one task and reports, per run, wall time `T` (minutes), cost `C`
(USD), and quality `Q` (passed traits / judged traits, 0..1).

| Term | Meaning |
| --- | --- |
| Task | The input prompt (`arms.prompt`, sent as `{"task": ...}`). |
| Baseline | `T` and `C` of implementing the task before the change (`software-change`). |
| Candidate | `T` and `C` after the change (`graphs/<candidate>.json`). |
| Acceptance | The `[[criteria]]` traits, judged by an LLM (`eval-judge`). |

Flow: select a task, write the traits, run the baseline N times, run the candidate N times, score.

```text
S = Q / (C^a * T^b)      a, b in [0, 1], default 0.5
ratio = S_candidate / S_baseline   (each S from the arm's mean T, C, Q)
```

`ratio > 1` means the candidate is better. The report also prints the median ratio.

## Files

- `eval1-ci-before-review.toml`: Eval 1, "Run CI before any review turn". Expected: lower cost.
- `eval2-delete-comments.toml`: Eval 2, "Delete all the comments" (agents make excuses in
  comments instead of doing the refactor). Expected: higher quality at a similar time and cost.
- `graphs/*.json`: candidate graphs. Each is `zeroshot template show software-change --push`
  with one extra worker instruction. Regenerate them with `./make-graphs.sh [zeroshot-binary]`
  (needs `jq`).
- `runtime.json`: the uniform agent runtime that every arm uses. Edit the model here.
- `pareto.py`: plots cost vs time and cost vs quality from an export.

## Dry run (no providers, no cost)

The manifests use `runner = "fake"`, a synthetic runner with deterministic jitter, so the whole
pipeline can be checked offline:

```bash
zeroshot eval --directory /tmp/evals run zeroshot/fixtures/eval/talk/eval1-ci-before-review.toml --id eval1-dry
zeroshot eval --directory /tmp/evals export eval1-dry --output /tmp/evals/eval1
python3 zeroshot/fixtures/eval/talk/pareto.py /tmp/evals/eval1   # needs matplotlib
```

Dry-run numbers come from `[arms.knobs]` and are only placeholders.

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
- **Cost.** Expect N × 2 full `software-change` runs plus the judge runs. Start with
  `runsPerArm = 1`.

Platform runs are not driven by `eval run`. Produce capture directories elsewhere
(`sample.json` with `costUsd` plus `patch.diff`) and import them with `zeroshot eval judge-captures`.

## Scoring and export

```bash
zeroshot eval --directory /path/to/store score eval1 --scorer baseline-ratio --cost-weight 0.5 --time-weight 0.5
zeroshot eval --directory /path/to/store export eval1 --output out/   # score.json, table.csv, runs.csv
```

- **`table.csv`.** The slide table: one row per run pair (`t_base_min,c_base,q_base,s_base,
  t_min,c,q,s`), then a `mean` row and a `ratio` row.
- **`runs.csv`.** One row per sample, for Pareto plots: status, time, cost, its unit and source,
  quality, S, traits, and tokens.
- **`score.json`.** Schema `zeroshot.eval-score/v1`. It holds the means, medians, both S values,
  the ratio, the median ratio, and notes.
- **Report.** `report.md` and `report.json` contain the same section, "Baseline vs candidate".

Rules:

- **Cost `C`.** The source is chosen in this order:
  1. a recorded `costUsd` (imported captures);
  2. otherwise tokens × `[options.pricing]` (or `[arms.pricing]`), in USD per 1M tokens:
     `input`, `output`, `cacheRead`, `cacheWrite`. Cache tokens without a read/write split are
     priced at `cacheRead`;
  3. otherwise, if any measured sample has neither, the whole eval falls back to millions of
     tokens (`cost_unit = mtok`) with a note.

  Judge cost is not part of `C`. Providers do not report USD, so keep the pricing up to date
  with the model in `runtime.json`.
- **Time `T`.** Wall time from submit to the terminal state, including queue wait and local
  preparation.
- **Quality `Q`.** The weighted share of passed traits (judge score ≥ 3) among the judged traits.
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
judgeModel = "synthetic-judge"  # replace with a real judge model for real runs
# judgeRuntimeConfig = "judge-runtime.json"
deadlineMinutes = 90
# captureRemote = "https://github.com/owner/repo.git"

[options.score]
a = 0.5
b = 0.5
baseline = "baseline"
candidate = "ci-before-review"

[options.pricing]              # USD per 1M tokens
input = 3.0
output = 15.0
cacheRead = 0.3
cacheWrite = 3.75

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
# runArgs = ["--no-environment"]   # extra `zeroshot run` flags (not --target/--detach/--validate-only)

[[criteria]]
id = "ci-green"
description = "Formatter, linter, and the full test suite pass on the delivered change."
```

Paths in a manifest are resolved against the manifest's directory.
