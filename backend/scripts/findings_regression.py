#!/usr/bin/env python
"""Weekly findings regression — what the engine finds for the golden set (Phase 277).

Runs every expectation in ``backend/findings_golden/`` against the deployed
API: ``GET /lookup?refresh=true``, then the MCP ``opencheck_lookup`` tool for
the same LEI (it replays that run from the server's cache). Writes
``<out>.json`` and ``<out>.md``. The assertions, failure classes and the
week-over-week diff are documented in ``opencheck/findings_regression.py``
and ``docs/findings-regression.md``.

Driven by ``.github/workflows/findings-regression.yml`` (Mondays 08:30 UTC,
after the source-health sweep). Runnable by hand::

    cd backend
    uv run python scripts/findings_regression.py --out /tmp/findings
    uv run python scripts/findings_regression.py --only 253400JT3MQWNDKMJE44 --no-mcp --pace 0

Pacing: one fresh lookup a minute by default (``--pace``). A fresh run is
charged to this client's per-IP lookup budget (Phase 234) and draws on the
process-wide GLEIF throttle that readers share, so the run is slow on
purpose; twelve subjects take about fifteen minutes.

Exit status: 0 when every subject met its expectations, 1 when any did not,
2 when the run itself could not start (no golden files, unreadable cards).
The workflow reports either way and only then fails the job — findings drift
on live data is a signal to read, never a red check on a pull request.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

BACKEND = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND))

import httpx  # noqa: E402

from opencheck import findings_regression as fr  # noqa: E402

DEFAULT_BASE = "https://api.opencheck.world"
#: The deployed API serves bots a 403 on the streamed route only, but an
#: honest, identifiable agent string is the right thing to send anyway.
USER_AGENT = "OpenCheck-findings-regression/1.0 (+https://github.com/StephenAbbott/opencheck)"


def _mcp_caller(mcp_url: str) -> fr.McpCall:
    async def call(lei: str) -> dict[str, Any]:
        from mcp import ClientSession
        from mcp.client.streamable_http import streamablehttp_client

        async with (
            streamablehttp_client(mcp_url, headers={"User-Agent": USER_AGENT}, timeout=60) as (
                read,
                write,
                _,
            ),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            res = await session.call_tool("opencheck_lookup", {"lei": lei})
            if res.isError:
                text = res.content[0].text if res.content else "error"  # type: ignore[union-attr]
                return {"error": str(text)[:200]}
            data: Any = res.structuredContent
            if data is None and res.content:
                data = json.loads(res.content[0].text)  # type: ignore[union-attr]
            # FastMCP wraps a non-object return as {"result": …}.
            if (
                isinstance(data, dict)
                and set(data) == {"result"}
                and isinstance(data["result"], dict)
            ):
                data = data["result"]
            if not isinstance(data, dict):
                return {"error": "unexpected MCP result shape"}
            if data.get("error"):
                return {"error": str(data.get("error"))[:200]}
            return data

    return call


def _commit() -> str | None:
    sha = os.environ.get("GITHUB_SHA")
    if sha:
        return sha[:7]
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            cwd=BACKEND,
            check=False,
        )
        return out.stdout.strip() or None
    except OSError:
        return None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--out", default="findings-regression", help="Output path without extension.")
    ap.add_argument("--previous", help="Last published findings-regression.json, for the diff.")
    ap.add_argument("--base-url", default=os.environ.get("OPENCHECK_API_BASE", DEFAULT_BASE))
    ap.add_argument("--mcp-url", help="Defaults to <base-url>/mcp.")
    ap.add_argument("--no-mcp", action="store_true", help="Skip the MCP comparison.")
    ap.add_argument(
        "--pace", type=float, default=fr.DEFAULT_PACE_S, help="Seconds between subjects."
    )
    ap.add_argument("--only", action="append", default=[], help="Run just this LEI (repeatable).")
    ap.add_argument("--golden", type=Path, default=fr.GOLDEN_DIR)
    args = ap.parse_args(argv)

    try:
        expectations = fr.load_expectations(args.golden)
        cards = fr.load_example_cards()
    except (OSError, ValueError) as exc:
        print(f"::error::cannot start: {exc}", file=sys.stderr)
        return 2
    if args.only:
        wanted = {lei.strip().upper() for lei in args.only}
        expectations = [e for e in expectations if e["lei"] in wanted]
    if not expectations:
        print("::error::no golden subjects to run", file=sys.stderr)
        return 2

    previous: dict[str, Any] | None = None
    if args.previous and Path(args.previous).is_file():
        try:
            previous = json.loads(Path(args.previous).read_text(encoding="utf-8"))
        except ValueError:
            print("::warning::previous report is not JSON; no comparison this run", file=sys.stderr)

    mcp_call = (
        None if args.no_mcp else _mcp_caller(args.mcp_url or f"{args.base_url.rstrip('/')}/mcp")
    )
    with httpx.Client(
        timeout=240, headers={"User-Agent": USER_AGENT}, follow_redirects=True
    ) as client:
        report = asyncio.run(
            fr.run(
                expectations,
                client=client,
                base_url=args.base_url,
                rules=fr.Rules.from_code(),
                cards=cards,
                mcp_call=mcp_call,
                pace_s=args.pace,
                previous=previous,
                commit=_commit(),
            )
        )

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.with_suffix(".json").write_text(
        json.dumps(report, indent=1, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    out.with_suffix(".md").write_text(fr.render_markdown(report), encoding="utf-8")

    t = report["totals"]
    print(
        f"findings regression: {t['passed']}/{t['subjects']} as expected; by class {t['by_check']}"
    )
    for s in report["subjects"]:
        for f in s["findings"]:
            level = "error" if f["severity"] == fr.FAIL else "warning"
            print(f"::{level}::{s['name']}: {f['check']} — {f['message']}")
    return 1 if fr.failed(report) else 0


if __name__ == "__main__":
    raise SystemExit(main())
