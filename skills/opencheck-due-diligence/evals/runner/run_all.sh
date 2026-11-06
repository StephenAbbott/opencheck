#!/bin/bash
# Run every case in ../evals.json with and without the skill, each in a clean
# Claude Code session (no CLAUDE.md, no user settings, no MCP servers).
#
# usage: ./run_all.sh <model> [parallel]     e.g. ./run_all.sh claude-haiku-5-5 6
# Needs: Claude Code (`claude`) on PATH and logged in; python3.
# Writes ./workspace/<model>/<case>/{with_skill,without_skill}/outputs/answer.md
# plus transcript.jsonl, timing.json and skill_used.json per run.
set -euo pipefail
MODEL=${1:?model id, e.g. claude-opus-5-5}
PAR=${2:-6}
HERE=$(cd "$(dirname "$0")" && pwd)
EVALS=$(cd "$HERE/.." && pwd)
SKILL=$(cd "$EVALS/.." && pwd)
WS="$HERE/workspace/$MODEL"
mkdir -p "$WS"

# A copy of the skill without its evals, so a with-skill run cannot read the
# expected answers.
SKUT="$HERE/workspace/skill-under-test/opencheck-due-diligence"
rm -rf "$SKUT"; mkdir -p "$SKUT"
cp "$SKILL/SKILL.md" "$SKUT/"; cp -r "$SKILL/references" "$SKUT/"

python3 - "$EVALS" "$WS" <<'PY'
import json, os, sys
evals, ws = sys.argv[1], sys.argv[2]
files = os.path.join(evals, "files")
for e in json.load(open(os.path.join(evals, "evals.json")))["evals"]:
    d = os.path.join(ws, e["eval_name"]); os.makedirs(d, exist_ok=True)
    json.dump({"eval_name": e["eval_name"],
               "prompt": e["prompt"].replace("evals/files/", files + "/"),
               "assertions": e["expectations"]},
              open(os.path.join(d, "eval_metadata.json"), "w"), indent=1)
PY

for d in "$WS"/*/; do
  c=$(basename "$d")
  for cfg in with_skill without_skill; do echo "$c $cfg"; done
done | xargs -P "$PAR" -L 1 bash "$HERE/run_one.sh" "$WS" "$MODEL" "$SKUT" "$EVALS/files"
