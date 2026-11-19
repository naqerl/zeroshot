#!/usr/bin/env bash
# Render the candidate graphs for the talk evals from the built-in `software-change` template.
# Both candidates are the baseline graph (push delivery) with one extra worker instruction, so the
# only difference between the arms is the change under test.
#
#   ./make-graphs.sh [path/to/zeroshot]
set -euo pipefail

zeroshot_bin="${1:-zeroshot}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$here/graphs"

render() {
  local extra="$1" output="$2"
  "$zeroshot_bin" template show software-change --push |
    jq --arg extra "$extra" '
      (.root.children[0].body.children[] | select(.kind == "step" and .name == "worker") | .instructions)
        |= . + " " + $extra
    ' >"$output"
  echo "wrote $output"
}

render "Before every hand-off to review, run the repository's full CI checks locally (formatter, linter, and the complete test suite as CI runs them) and fix every failure; never hand off a change whose checks fail." \
  "$here/graphs/ci-before-review.json"

render "Delete every code comment you would otherwise add or keep to explain, justify, or defer work (TODO, FIXME, NOTE, workaround explanations). Do not excuse a shortcut in prose: refactor until the code needs no excuse. Keep license headers and public API documentation." \
  "$here/graphs/delete-comments.json"
