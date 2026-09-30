#!/usr/bin/env python3
"""mypy as a ratchet, not a ban (Phase 266).

``[tool.mypy]`` in ``pyproject.toml`` asked for ``strict = true`` and reported
469 errors, and nothing ran it, so it checked nothing. The Opus 5.5 check put
the choice as a non-strict gate or no config at all; this is the gate.

The config is now the non-strict default (still 135 errors across 53 files when
this was written — every one of them in code that has shipped and is tested).
Fixing them in one commit would be a diff nobody could review, and a gate that
fails on every existing error is a gate that gets disabled. So this does what
``frontend/scripts/lint-design-system.mjs`` does for the design system:
``mypy-baseline.json`` records how many errors each module carries, and

* a module may never carry **more** than its baseline,
* a module not in the baseline may carry **none**, and
* a module that now carries **fewer** fails too, until the baseline is
  updated — so an improvement is locked in by the commit that made it rather
  than left for the next regression to spend.

Counts, not messages: a message moves with every edit above it and would make
the baseline churn on unrelated changes.

Usage (from ``backend/``)::

    uv run python scripts/mypy_ratchet.py            # check (CI)
    uv run python scripts/mypy_ratchet.py --update   # rewrite the baseline;
                                                     # refuses to raise a count
    uv run python scripts/mypy_ratchet.py --update --allow-increase

Exit status: 0 clean, 1 ratchet failure, 2 mypy itself failed to run.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent
BASELINE = BACKEND / "mypy-baseline.json"
TARGET = "opencheck"

# ``opencheck/app.py:58: error: … [assignment]``. Notes and the summary line
# are not errors; a syntax error is reported as ``error:`` too and counts.
_ERROR = re.compile(r"^(?P<path>[^:\s][^:]*\.pyi?):\d+(?::\d+)?: error: ")


def run_mypy() -> tuple[Counter[str], str]:
    proc = subprocess.run(
        [sys.executable, "-m", "mypy", TARGET, "--no-error-summary", "--show-error-codes"],
        cwd=BACKEND,
        capture_output=True,
        text=True,
    )
    # mypy exits 1 when it found errors and 2 when it could not run at all.
    if proc.returncode not in (0, 1):
        sys.stderr.write(proc.stdout + proc.stderr)
        raise SystemExit(2)
    counts: Counter[str] = Counter()
    for line in proc.stdout.splitlines():
        m = _ERROR.match(line)
        if m:
            counts[Path(m["path"]).as_posix()] += 1
    return counts, proc.stdout


def load_baseline() -> dict[str, int]:
    if not BASELINE.exists():
        return {}
    data = json.loads(BASELINE.read_text(encoding="utf-8"))
    return {str(k): int(v) for k, v in data.get("errors", {}).items()}


def write_baseline(counts: Counter[str]) -> None:
    body = {
        "_comment": (
            "mypy errors per module (non-strict config in pyproject.toml). A module "
            "may never carry more than this; see scripts/mypy_ratchet.py."
        ),
        "total": sum(counts.values()),
        "errors": {k: counts[k] for k in sorted(counts) if counts[k]},
    }
    BASELINE.write_text(json.dumps(body, indent=2) + "\n", encoding="utf-8")


def compare(counts: Counter[str], baseline: dict[str, int]) -> tuple[list[str], list[str]]:
    worse: list[str] = []
    better: list[str] = []
    for path in sorted(set(counts) | set(baseline)):
        now, was = counts.get(path, 0), baseline.get(path, 0)
        if now > was:
            worse.append(f"{path}: {was} → {now}" if was else f"{path}: {now} (new — must be 0)")
        elif now < was:
            better.append(f"{path}: {was} → {now}")
    return worse, better


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--update", action="store_true", help="rewrite the baseline")
    parser.add_argument(
        "--allow-increase", action="store_true", help="with --update: accept higher counts"
    )
    args = parser.parse_args(argv)

    counts, output = run_mypy()
    baseline = load_baseline()
    worse, better = compare(counts, baseline)

    if args.update:
        if worse and not args.allow_increase:
            print("Refusing to raise the baseline without --allow-increase:")
            print("\n".join(f"  {w}" for w in worse))
            return 1
        write_baseline(counts)
        print(f"mypy baseline written: {sum(counts.values())} errors in {len(counts)} modules")
        return 0

    if worse:
        print("mypy found errors above the baseline (scripts/mypy_ratchet.py):")
        print("\n".join(f"  {w}" for w in worse))
        print("\nThe errors in those modules:")
        bad = {w.split(":", 1)[0] for w in worse}
        for line in output.splitlines():
            m = _ERROR.match(line)
            if m and Path(m["path"]).as_posix() in bad:
                print(f"  {line}")
        return 1
    if better:
        print("mypy found fewer errors than the baseline — lock the gain in with")
        print("`uv run python scripts/mypy_ratchet.py --update`:")
        print("\n".join(f"  {b}" for b in better))
        return 1
    print(f"mypy ratchet clean: {sum(counts.values())} errors, none above the baseline")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
