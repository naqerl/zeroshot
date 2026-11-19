#!/usr/bin/env python3
"""Draw baseline vs candidate Pareto plots from `zeroshot eval export` output.

Usage:
    python3 pareto.py .zeroshot/eval/<id> [--out DIR]

Reads `score.json` (for the arm names, weights, and the mean and median ratios) and `runs.csv` (one row per sample)
and writes `pareto-cost-time.png` and `pareto-cost-quality.png`. Each dot is one run, diamonds are
the per-arm means, and the dashed step line is the Pareto front over all measured runs. Requires
matplotlib (`pip install matplotlib`).
"""

import argparse
import csv
import json
import pathlib
import sys


def load(directory):
    score = json.loads((directory / "score.json").read_text())
    with (directory / "runs.csv").open(newline="") as handle:
        rows = [row for row in csv.DictReader(handle) if row.get("measured", "true") == "true"]
    return score, rows


def front(points, maximize_y):
    """Pareto-optimal points: minimize x; minimize y, or maximize it when maximize_y."""
    ordered = sorted(points, key=lambda point: (point[0], -point[1] if maximize_y else point[1]))
    result = []
    for point in ordered:
        if not result or (point[1] > result[-1][1] if maximize_y else point[1] < result[-1][1]):
            result.append(point)
    return result


def plot(score, rows, y_key, y_label, maximize_y, path):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    unit = "USD" if score["costUnit"] == "usd" else "Mtok"
    colors = {"baseline": "#8a8f98", "candidate": "#14b8a6"}
    figure, axes = plt.subplots(figsize=(8, 4.8), dpi=200)
    points = []
    for role in ("baseline", "candidate"):
        side = [row for row in rows if row["role"] == role and row[y_key] != ""]
        xs = [float(row["cost"]) for row in side]
        ys = [float(row[y_key]) for row in side]
        points.extend(zip(xs, ys))
        arm = score[f"{role}Arm"]
        axes.scatter(xs, ys, color=colors[role], label=f"{role}: {arm}", alpha=0.85)
        for row, x, y in zip(side, xs, ys):
            axes.annotate(f"{role[0].upper()}{row['run']}", (x, y), fontsize=7,
                          xytext=(4, 3), textcoords="offset points")
        mean = score[role]
        mean_y = mean.get("timeMinutes" if y_key == "time_min" else "quality")
        if mean.get("cost") is not None and mean_y is not None:
            axes.scatter([mean["cost"]], [mean_y], marker="D", s=90, color=colors[role],
                         edgecolors="black", linewidths=0.8)
    pareto = front(points, maximize_y)
    if len(pareto) > 1:
        axes.step([p[0] for p in pareto], [p[1] for p in pareto], where="post",
                  linestyle="--", color="black", linewidth=0.8, label="Pareto front")
    mean_ratio = score.get("meanRatio")
    median_ratio = score.get("medianRatio")
    title = f"{score['evalName']}: cost vs {y_label.split(' ')[0].lower()}"
    if mean_ratio is not None:
        title += f"   S_cand / S_base = {mean_ratio:.2f} mean"
        if median_ratio is not None:
            title += f", {median_ratio:.2f} median"
        title += f" (a={score['a']}, b={score['b']})"
    axes.set_title(title, fontsize=10)
    axes.set_xlabel(f"Cost per run ({unit})")
    axes.set_ylabel(y_label)
    axes.grid(alpha=0.3)
    axes.legend(fontsize=8)
    figure.tight_layout()
    figure.savefig(path)
    print(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("directory", type=pathlib.Path)
    parser.add_argument("--out", type=pathlib.Path)
    arguments = parser.parse_args()
    out = arguments.out or arguments.directory
    out.mkdir(parents=True, exist_ok=True)
    score, rows = load(arguments.directory)
    if not rows:
        sys.exit("runs.csv has no measured runs")
    plot(score, rows, "time_min", "Time per run (min)", False, out / "pareto-cost-time.png")
    plot(score, rows, "quality", "Quality (passed traits / total)", True, out / "pareto-cost-quality.png")


if __name__ == "__main__":
    main()
