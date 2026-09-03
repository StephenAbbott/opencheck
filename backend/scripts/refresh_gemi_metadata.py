#!/usr/bin/env python3
"""Refresh the committed ΓΕΜΗ codelist snapshot.

The Greek register's company records embed its codelists in *truncated* form:
a company's ``status``, ``legalType``, ``municipality``, ``prefecture``,
``gemiOffice`` and ``activity`` objects carry only ``{id, descr}``. The
``descrEn`` (English label) and ``isActive`` (is this status an operating
one?) fields that the Swagger definitions advertise are returned **only** by
the ``/metadata/*`` endpoints.

So the codelists are not an optimisation — the BODS mapper cannot label a
Greek company's status in English, or decide whether it is active, without
them. Fetching them per lookup would spend most of the eight requests the API
allows per minute, so they are snapshotted here and committed.

Usage (needs ``GEMI_API_KEY`` in the environment or ``.env``)::

    python3 backend/scripts/refresh_gemi_metadata.py
    python3 backend/scripts/refresh_gemi_metadata.py --check   # CI drift gate

Six requests, paced under the published 8/min budget, so a run takes about
fifty seconds. Re-run when GEMI announce codelist changes, or on the weekly
source-health schedule.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx

_REPO = Path(__file__).resolve().parents[2]
_OUT = _REPO / "backend" / "opencheck" / "data" / "gemi_metadata.json"
_BASE = "https://opendata-api.businessportal.gr/api/opendata/v1"

# Endpoint name → key in the snapshot. Order is the request order.
#
# ``activities`` (the ΚΑΔ catalogue) is deliberately EXCLUDED. It is 19,368
# entries and 6.5 MB — fifty times the size of the other six combined — and
# nothing in the BODS mapping depends on it: a company record already embeds
# the Greek ``descr`` of each of its own activities, and OpenCheck does not
# currently emit activity codes into BODS output. Committing 6.5 MB (or ~765 KB
# gzipped) to save an English label we do not render would be a poor trade.
# Revisit if activity codes ever reach a user-facing surface.
_LISTS = (
    "companyStatuses",
    "legalTypes",
    "gemiOffices",
    "prefectures",
    "municipalities",
    "assemblySubjects",
)

# 20 req/min published (raised from 8 on 2026-09-03); pace at ~18/min to leave
# headroom for a retry.
_INTERVAL_SECONDS = 3.4


def _api_key() -> str:
    key = os.environ.get("GEMI_API_KEY", "").strip()
    if key:
        return key
    env = _REPO / ".env"
    if env.exists():
        for line in env.read_text(encoding="utf-8").splitlines():
            if line.startswith("GEMI_API_KEY="):
                return line.split("=", 1)[1].strip().strip("\"'")
    raise SystemExit(
        "GEMI_API_KEY is not set. Put it in the environment or in .env — "
        "request a key at https://opendata.businessportal.gr/register/"
    )


async def _fetch_all(key: str) -> dict[str, list[dict[str, object]]]:
    out: dict[str, list[dict[str, object]]] = {}
    headers = {"api_key": key, "Accept": "application/json"}
    async with httpx.AsyncClient(timeout=30.0, headers=headers) as client:
        for index, name in enumerate(_LISTS):
            if index:
                await asyncio.sleep(_INTERVAL_SECONDS)
            url = f"{_BASE}/metadata/{name}"
            response = await client.get(url)
            if response.status_code == 429:
                raise SystemExit(
                    f"{name}: HTTP 429 — rate limited. Wait a minute and re-run."
                )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                raise SystemExit(f"{name}: expected a JSON array, got {type(payload).__name__}")
            out[name] = payload
            print(f"  {name}: {len(payload)} entries", file=sys.stderr)
    return out


def _normalise(lists: dict[str, list[dict[str, object]]]) -> dict[str, object]:
    """Shape the snapshot for the adapter, keyed by *string* id.

    The codelists return ``id`` as a string while the objects embedded in a
    company record return it as an integer, so every id is stringified here
    and the adapter stringifies its lookup key. Getting this wrong produces a
    silent total miss — every label falls back to the Greek ``descr``.
    """
    tables: dict[str, dict[str, dict[str, object]]] = {}
    for name, rows in lists.items():
        table: dict[str, dict[str, object]] = {}
        for row in rows:
            if not isinstance(row, dict) or "id" not in row:
                continue
            table[str(row["id"])] = row
        tables[name] = table
    return {
        "_generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_source": _BASE + "/metadata/*",
        "_licence": "ODC-BY-1.0",
        "_note": (
            "Codelist ids are strings here; ids embedded in company records are "
            "integers. Stringify before looking up."
        ),
        "tables": tables,
    }


def _strip_generated(blob: dict[str, object]) -> dict[str, object]:
    return {k: v for k, v in blob.items() if k != "_generated"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if the committed snapshot differs (ignoring the timestamp).",
    )
    args = parser.parse_args()

    print("Fetching ΓΕΜΗ codelists (6 requests, paced ~8.6s apart)…", file=sys.stderr)
    fresh = _normalise(asyncio.run(_fetch_all(_api_key())))

    if args.check:
        if not _OUT.exists():
            print(f"MISSING: {_OUT}", file=sys.stderr)
            return 1
        current = json.loads(_OUT.read_text(encoding="utf-8"))
        if _strip_generated(current) != _strip_generated(fresh):
            print(f"DRIFT: {_OUT} is out of date — re-run without --check", file=sys.stderr)
            return 1
        print("Up to date.", file=sys.stderr)
        return 0

    _OUT.parent.mkdir(parents=True, exist_ok=True)
    _OUT.write_text(
        json.dumps(fresh, ensure_ascii=False, indent=1, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    counts = ", ".join(f"{k} {len(v)}" for k, v in fresh["tables"].items())  # type: ignore[union-attr]
    print(f"Wrote {_OUT.relative_to(_REPO)} — {counts}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
