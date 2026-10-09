#!/usr/bin/env python
"""Weekly BODS quality sweep — is what production publishes valid, honest BODS? (Phase 316)

Reads the deployed API: ``GET /lookup?refresh=true`` for every LEI in the
findings golden set, then ``GET /deepen`` for every source-health probe subject
``/deepen`` can address. Validates every statement with lib-cove-bods and
checks OpenCheck's own date and provenance rules. Writes ``<out>.json`` and
``<out>.md``. The checks, their classes and the week-over-week diff are
documented in ``opencheck/bods_quality.py`` and ``docs/bods-quality.md``.

Driven by ``.github/workflows/bods-quality.yml`` (Mondays, after the findings
regression). Runnable by hand::

    cd backend
    uv run python scripts/bods_quality.py --out /tmp/bods-quality
    uv run python scripts/bods_quality.py --no-lookups --only-source prh --deepen-pace 0

Pacing: one fresh lookup a minute (``--pace``), as the findings regression —
a fresh run is charged to this client's per-IP lookup budget and draws on the
GLEIF throttle readers share — and a few seconds between deepens.

Exit status: 0 when no finding is a failure, 1 when any is, 2 when the run
could not start. The workflow reports either way and only then fails.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

BACKEND = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND))

import httpx  # noqa: E402

from opencheck import bods_quality as bq  # noqa: E402
from opencheck import findings_regression as fr  # noqa: E402

DEFAULT_BASE = "https://api.opencheck.world"
USER_AGENT = "OpenCheck-bods-quality/1.0 (+https://github.com/StephenAbbott/opencheck)"


def _commit() -> str | None:
    sha = os.environ.get("GITHUB_SHA")
    if sha:
        return sha[:7]
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, cwd=BACKEND, check=False,
        )
        return out.stdout.strip() or None
    except OSError:
        return None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--out", default="bods-quality", help="Output path without extension.")
    ap.add_argument("--previous", help="Last published bods-quality.json, for the diff.")
    ap.add_argument("--base-url", default=os.environ.get("OPENCHECK_API_BASE", DEFAULT_BASE))
    ap.add_argument("--pace", type=float, default=bq.DEFAULT_PACE_S, help="Seconds between lookups.")
    ap.add_argument(
        "--deepen-pace", type=float, default=bq.DEFAULT_DEEPEN_PACE_S,
        help="Seconds between deepens.",
    )
    ap.add_argument("--only", action="append", default=[], help="Look up just this LEI (repeatable).")
    ap.add_argument(
        "--only-source", action="append", default=[], help="Deepen just this source (repeatable)."
    )
    ap.add_argument("--no-lookups", action="store_true", help="Skip the golden-set lookups.")
    ap.add_argument("--no-deepens", action="store_true", help="Skip the per-source deepens.")
    args = ap.parse_args(argv)

    from opencheck.bods.statements import OFFICIAL_REGISTER_SOURCES
    from opencheck.sources.probes import PROBES

    try:
        golden = fr.load_expectations()
    except (OSError, ValueError) as exc:
        print(f"::error::cannot start: {exc}", file=sys.stderr)
        return 2
    lookups = [{"lei": g["lei"], "name": g.get("name") or g["lei"]} for g in golden]
    if args.only:
        wanted = {lei.strip().upper() for lei in args.only}
        lookups = [s for s in lookups if s["lei"] in wanted] or [{"lei": w, "name": w} for w in wanted]
    if args.no_lookups:
        lookups = []
    deepens = [] if args.no_deepens else bq.deepen_subjects(PROBES)
    if args.only_source:
        deepens = [d for d in deepens if d.source_id in set(args.only_source)]
    if not lookups and not deepens:
        print("::error::no subjects to run", file=sys.stderr)
        return 2

    previous: dict[str, Any] | None = None
    if args.previous and Path(args.previous).is_file():
        try:
            previous = json.loads(Path(args.previous).read_text(encoding="utf-8"))
        except ValueError:
            print("::warning::previous report is not JSON; no comparison this run", file=sys.stderr)

    with httpx.Client(
        timeout=240, headers={"User-Agent": USER_AGENT}, follow_redirects=True
    ) as client:
        report = bq.run(
            client=client,
            base_url=args.base_url,
            lookup_subjects=lookups,
            deepen=deepens,
            official_registers=OFFICIAL_REGISTER_SOURCES,
            pace_s=args.pace,
            deepen_pace_s=args.deepen_pace,
            previous=previous,
            commit=_commit(),
        )

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.with_suffix(".json").write_text(
        json.dumps(report, indent=1, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    out.with_suffix(".md").write_text(bq.render_markdown(report), encoding="utf-8")

    t = report["totals"]
    print(
        f"bods quality: {t['subjects']} subjects, {t['statements']} statements, "
        f"{t['fail']} failing, {t['warn']} warnings; by class {t['by_check']}"
    )
    for key, n in t["by_check_source"].items():
        check, sid = key.split("|", 1)
        print(f"  {check:28} {sid:20} {n}")
    return 1 if bq.failed(report) else 0


if __name__ == "__main__":
    raise SystemExit(main())
