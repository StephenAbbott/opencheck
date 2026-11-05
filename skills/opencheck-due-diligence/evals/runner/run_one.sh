#!/bin/bash
# One case, one configuration, in a clean Claude Code session.
# usage: run_one.sh <workspace> <model> <skill_dir> <fixtures_dir> <case> <with_skill|without_skill>
WS=$1; MODEL=$2; SKUT=$3; FIX=$4; CASE=$5; CFG=$6
HERE=$(cd "$(dirname "$0")" && pwd)
RUN="$WS/$CASE/$CFG"; OUT="$RUN/outputs"; mkdir -p "$OUT"
WD=$(mktemp -d)
if [ "$CFG" = with_skill ]; then
  mkdir -p "$WD/.claude/skills" && cp -r "$SKUT" "$WD/.claude/skills/"
fi
TASK=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['prompt'])" "$WS/$CASE/eval_metadata.json")
PROMPT="The OpenCheck MCP tool output you need for this has already been recorded to the file(s) named below (each file is {tool, arguments, recorded_at, response}); work from those files and do not call OpenCheck or the network.

$TASK

Write your final answer to me (exactly what you would send, in markdown) to $OUT/answer.md, and save any other files you create there too."
SYS="You are connected to the OpenCheck MCP server (https://api.opencheck.world/mcp). Its handshake instructions:
$(cat "$HERE/server_instructions.txt")"
cd "$WD"
START=$(python3 -c 'import time;print(int(time.time()*1000))')
# --setting-sources project: only this empty directory's settings (and the
# installed skill); --strict-mcp-config with no config: no MCP servers.
env -u CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD timeout 600 claude -p --model "$MODEL" \
  --setting-sources project --strict-mcp-config \
  --allowed-tools "Read,Write,Edit,Glob,Grep,Skill,Bash(python3:*)" \
  --disallowed-tools "WebFetch,WebSearch,Agent,Task" \
  --add-dir "$FIX" --add-dir "$OUT" --permission-mode acceptEdits \
  --append-system-prompt "$SYS" --output-format stream-json --verbose "$PROMPT" \
  > "$RUN/transcript.jsonl" 2> "$RUN/stderr.txt"
END=$(python3 -c 'import time;print(int(time.time()*1000))')
python3 - "$RUN" $((END-START)) <<'PY'
import json, sys
run, ms = sys.argv[1], int(sys.argv[2])
used = any('"name":"Skill"' in l.replace(" ", "") or "opencheck-due-diligence/SKILL.md" in l
           for l in open(run + "/transcript.jsonl"))
json.dump({"skill_used": used}, open(run + "/skill_used.json", "w"))
json.dump({"duration_ms": ms, "total_duration_seconds": round(ms / 1000, 1)}, open(run + "/timing.json", "w"))
PY
echo "$CASE $CFG $(test -f "$OUT/answer.md" && echo ok || echo MISSING)"
