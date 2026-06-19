#!/usr/bin/env bash
#
# build_opentender.sh — resilient, resumable wrapper around
# extract_opentender.py for building the (multi-GB) opentender.db artifact.
#
# Why this exists
# ---------------
# A full 35-country build runs for hours/days. extract_opentender.py commits
# to SQLite every batch (WAL mode), so progress is durable — but the job dies
# the moment the Terminal closes or the Mac sleeps, and the script has no
# per-file --resume flag. This wrapper fixes both:
#
#   * Survives a closed Terminal / idle sleep  — runs under `caffeinate -i`
#     + `nohup` in the background, logging to a file you can tail.
#   * Resumes without redoing finished countries — inspects opentender.db,
#     skips country archives whose rows are already loaded (ingestion is
#     idempotent: INSERT OR REPLACE keyed on persistentId).
#
# Usage
# -----
#   backend/scripts/build_opentender.sh
#       # builds from ~/Downloads/data-*-ndjson.zip into ~/Downloads/opentender.db,
#       # skipping any country already present in the DB
#
# Environment overrides (all optional):
#   INPUT_DIR   directory holding the data-<cc>-ndjson.zip archives  (default: ~/Downloads)
#   OUTPUT      output SQLite path                                   (default: ~/Downloads/opentender.db)
#   FROM_YEAR   passed through to --from-year                        (default: script default, 2024)
#   REDO        space/comma list of country codes to force-reprocess even if
#               already loaded — use this for the country that was mid-load when
#               the build was interrupted, e.g.  REDO="uk de"
#   FOREGROUND  set to 1 to run in the foreground (no nohup/background)
#
# Examples
#   REDO=uk backend/scripts/build_opentender.sh           # re-do the interrupted UK file too
#   FROM_YEAR=0 backend/scripts/build_opentender.sh        # include all years
#   FOREGROUND=1 backend/scripts/build_opentender.sh       # watch it run inline
#
set -euo pipefail

# --- resolve paths (this script lives in backend/scripts/) -------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXTRACT="$SCRIPT_DIR/extract_opentender.py"

INPUT_DIR="${INPUT_DIR:-$HOME/Downloads}"
OUTPUT="${OUTPUT:-$HOME/Downloads/opentender.db}"
LOG="${OUTPUT%.db}-build.log"

command -v python3 >/dev/null || { echo "error: python3 not on PATH" >&2; exit 1; }
[ -f "$EXTRACT" ] || { echo "error: $EXTRACT not found" >&2; exit 1; }

# --- which countries are already loaded? -------------------------------------
# DB stores country uppercased, with UK normalised to GB. Read via python3's
# stdlib sqlite3 (always present with python3) rather than the sqlite3 CLI
# (which isn't installed everywhere). A missing DB / table yields an empty list.
loaded=""
if [ -f "$OUTPUT" ]; then
  loaded="$(python3 - "$OUTPUT" <<'PY' || true
import sqlite3, sys
try:
    con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
    rows = con.execute("SELECT DISTINCT country FROM tenders").fetchall()
    print(" ".join(str(r[0]) for r in rows if r[0]))
except Exception:
    pass
PY
)"
fi

# normalise REDO list to upper-case, DB-form (UK->GB), comma-or-space separated
redo_db=" $(printf '%s' "${REDO:-}" | tr ',a-z' ' A-Z' | sed 's/\bUK\b/GB/g') "

# --- build the input list, skipping finished countries -----------------------
shopt -s nullglob
inputs=()
skipped=()
for f in "$INPUT_DIR"/data-*-ndjson.zip; do
  base="$(basename "$f")"          # data-uk-ndjson.zip
  cc="${base#data-}"; cc="${cc%-ndjson.zip}"   # uk
  cc_db="$(printf '%s' "$cc" | tr 'a-z' 'A-Z')"
  [ "$cc_db" = "UK" ] && cc_db="GB"

  # already loaded AND not forced via REDO -> skip
  if [[ " $loaded " == *" $cc_db "* && " $redo_db " != *" $cc_db "* ]]; then
    skipped+=("$cc_db")
    continue
  fi
  inputs+=("$f")
done

if [ "${#inputs[@]}" -eq 0 ]; then
  echo "Nothing to do: no matching data-*-ndjson.zip in $INPUT_DIR that isn't already loaded."
  [ -n "${skipped[*]:-}" ] && echo "Already loaded (skipped): ${skipped[*]}"
  echo "Tip: to force-rebuild a country, e.g. REDO=uk $0"
  exit 0
fi

echo "Output:        $OUTPUT"
echo "Log:           $LOG"
[ -n "${skipped[*]:-}" ] && echo "Skipping (loaded): ${skipped[*]}"
echo "Processing:    ${#inputs[@]} archive(s):"
for f in "${inputs[@]}"; do echo "  - $(basename "$f")"; done
echo

# --- assemble the python command ---------------------------------------------
cmd=(python3 "$EXTRACT" --input "${inputs[@]}" --output "$OUTPUT")
[ -n "${FROM_YEAR:-}" ] && cmd+=(--from-year "$FROM_YEAR")

# caffeinate (macOS) keeps the machine awake while the job runs; harmless to
# omit elsewhere.
runner=()
command -v caffeinate >/dev/null && runner=(caffeinate -i)

if [ "${FOREGROUND:-}" = "1" ]; then
  exec "${runner[@]}" "${cmd[@]}"
fi

# Detach: nohup + background so closing the Terminal won't kill it.
nohup "${runner[@]}" "${cmd[@]}" >"$LOG" 2>&1 &
pid=$!
disown "$pid" 2>/dev/null || true
echo "Started in background (PID $pid). You can close this Terminal."
echo "Watch progress:  tail -f \"$LOG\""
echo "Stop it:         kill $pid"
echo
echo "When it finishes, grab the 'SHA-256:' line from the log for OPENTENDER_DB_SHA256."
