# Synthetic eval fixture

`synthetic.toml` is a strict TOML eval manifest. It runs two arms through the built-in
`FakeRunner`, needs no network, and writes the full store plus report:

```bash
zeroshot eval run zeroshot/fixtures/eval/synthetic.toml --directory . --id synthetic-fixture
```

The store is `.zeroshot/eval/<id>/`:

- `manifest.toml` — byte-for-byte manifest copy
- `eval.json` — store schema, id, and creation timestamp
- `samples/<arm>-<run>.json` — one atomically written record per attempted sample
- `patches/<arm>-<run>.patch` — the sample's patch artifact
- `report.json` — report data, written before Markdown
- `report.md` — rendered only from `report.json`

## Manifest shape

```toml
schema = "zeroshot.eval/v1"
name = "synthetic"
runsPerArm = 3

[options]
scorer = "quality-vs-cost" # or "none"
runner = "fake"
seed = 7

[options.fake]               # synthetic runner defaults; arm knobs may override them
queueWaitMs = 5
wallClockMs = 100
inputTokens = 1000
outputTokens = 200
cacheTokens = 0
passRate = 1.0
failEvery = 2                # per-arm ordinal; omit for no forced failures
# skipEvery = 5

[[arms]]
id = "baseline"
template = "single-worker"   # exactly one of template or graph
prompt = "Implement the synthetic task."
input = { task = "Implement the synthetic task." }

[arms.knobs]                 # optional per-arm synthetic settings
wallClockMs = 90
inputTokens = 900
outputTokens = 180

[arms.knobs.criteria]        # optional forced criterion outcomes
efficiency = false

[[criteria]]
id = "correctness"
description = "The change satisfies the requested behavior."
weight = 2.0
```

`quality-vs-cost` reports `Q`, each arm's `T/Tbase` and `C/Cbase`, and the ordinary
least-squares fit `Q = a + b * (C / Cbase)`. `none` keeps the aggregate score equal to `Q`.
Recompute either scorer after the fact with:

```bash
zeroshot eval score <id> --scorer quality-vs-cost
```

Every sample is persisted immediately after it settles. A later process can therefore run
`zeroshot eval show <id>`, `report`, or `score` using only the files above; no run state is
carried in memory. `report` additionally rebuilds `report.json` and `report.md` if the stored
report is absent.


## Judging

A run first records samples and patches. The judge reads those captured artifacts; it never
re-runs an arm. Configure the model per run, per arm, or in `[options]`:

```toml
[options]
judgeModel = "provider/default-judge"
judgeTemplate = "eval-judge"
judgeRuntimeConfig = "judge-runtime.json" # required for a real judge
```

The `eval-judge` graph lives at `zeroshot/src/native_v2_templates/eval_judge.json`. A custom graph
is any JSON path with the same input/output shape. Judgements use the inclusive 1..=5 scale, with
3 as the pass threshold, and must provide short reasoning plus an `errors` array. A plain
`zeroshot eval run` records synthetic sample evidence; judging it requires an authored judge
runtime. Without `judgeRuntimeConfig`, judging fails explicitly instead of relabeling synthetic
scores as a model's judgement. See `docs/guides/eval.md` for the contract and the X/Y report section.
