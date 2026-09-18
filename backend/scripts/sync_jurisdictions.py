#!/usr/bin/env python3
"""Regenerate ``opencheck/data/jurisdictions.json`` from Stephen's Notion table.

The Notion database **"Beneficial ownership access status"** (data source
``b6be86df-6d76-422b-93bd-f1ece5a99781``) is the single source of truth for
what each jurisdiction publishes about beneficial owners and who may see it.
Nothing in the repo hand-types those facts; this script pulls the table and
writes the JSON that ``opencheck/knowability.py`` validates at import.

Three ways in, one transform:

    # 1. The Notion API — an *internal* integration (Notion → Settings →
    #    Connections → Develop or manage integrations → New, type "Internal",
    #    capability "Read content" only), then on the database page
    #    … → Connections → add it. Put the token in backend/.env as
    #    NOTION_API_KEY=ntn_… (or the repo-root .env); this script loads it.
    uv run python scripts/sync_jurisdictions.py --notion

    # 2. A CSV exported from Notion (… → Export → Markdown & CSV)
    uv run python scripts/sync_jurisdictions.py --csv ~/Downloads/Beneficial\\ ownership\\ access\\ status.csv

    # 3. A JSON dump of rows in the Notion SQL-query shape (what a Cowork
    #    session gets back from the Notion MCP query tool)
    uv run python scripts/sync_jurisdictions.py --rows-json rows.json

Run everything through ``uv run`` from ``backend/`` — the system ``python3``
has neither pydantic nor pytest, so a bare ``python3`` fails on import.

Add ``--check`` to fail (exit 1) when the committed file differs from what
the source would produce — the same shape as ``generate_okf.py --check``.

Every column is optional except **Country** and **ISO code**; a row Stephen
has not stamped with a *Last verified* date is written as-is and carried as
``review_status: unverified`` by the loader. Column names are the Notion
property names; renaming a property in Notion means updating ``_COLUMNS``.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

DATA_SOURCE_ID = "b6be86df-6d76-422b-93bd-f1ece5a99781"
#: Same two places ``opencheck.config`` looks: ``backend/.env`` and the repo root.
ENV_PATHS: tuple[Path, ...] = (
    Path(__file__).resolve().parents[1] / ".env",
    Path(__file__).resolve().parents[2] / ".env",
)


def _load_env(paths: tuple[Path, ...] = ENV_PATHS) -> None:
    """Read the ``.env`` files so ``NOTION_API_KEY`` set there is seen — the
    app gets that from pydantic-settings, but this script runs outside the
    app. Never overrides a variable already in the environment."""
    for path in paths:
        if not path.exists():
            continue
        try:
            from dotenv import load_dotenv

            load_dotenv(path, override=False)
            continue
        except ImportError:
            pass
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip().removeprefix("export "), v.strip().strip("'\""))
NOTION_VERSION = "2025-09-03"
OUT_PATH = Path(__file__).resolve().parents[1] / "opencheck" / "data" / "jurisdictions.json"

# Notion property name -> (json path, kind)
# kinds: text | url | select | multi | date | checkbox
_COLUMNS: dict[str, tuple[str, str]] = {
    "Country": ("name", "text"),
    "ISO code": ("code", "text"),
    "Group": ("groups", "multi"),
    "National BO register": ("bo_register.name", "text"),
    "Register URL": ("bo_register.url", "url"),
    "BO access status": ("bo_register.access", "select"),
    "Access status since": ("bo_register.access_since", "date"),
    "Next change expected": ("bo_register.next_change_expected", "date"),
    "Threshold wording": ("bo_register.threshold_wording", "text"),
    "BO fields published": ("bo_register.fields_published", "multi"),
    "Reporting basis": ("bo_register.reporting_basis", "select"),
    "Covers": ("bo_register.covers", "multi"),
    "Verification": ("bo_register.verification", "select"),
    "Verification note": ("bo_register.verification_note", "text"),
    "6AMLD LIA": ("bo_register.amld6_lia", "select"),
    "6AMLD details": ("bo_register.amld6_details", "text"),
    "BORIS interconnection": ("bo_register.boris", "select"),
    "AMLR alignment watch": ("bo_register.amlr_alignment_watch", "checkbox"),
    "Pending changes": ("bo_register.pending_changes", "text"),
    "Company register name": ("company_register.name", "text"),
    "Company register URL": ("company_register.url", "url"),
    "Company register publishes": ("company_register.publishes", "multi"),
    "Company register public": ("company_register.public", "select"),
    "FATF assessment body": ("fatf.body", "text"),
    "Next FATF assessment (onsite)": ("fatf.onsite", "date"),
    "FATF plenary discussion": ("fatf.plenary", "date"),
    "OpenCheck source": ("opencheck_source_note", "text"),
    "Sources": ("_sources_text", "text"),
    "Notes": ("notes", "text"),
    "Last verified": ("last_verified", "date"),
}

_URL_RE = re.compile(r"https?://[^\s|,;]+")


# ----------------------------------------------------------------------
# Readers — each yields {property name: raw value}
# ----------------------------------------------------------------------
def _read_rows_json(path: Path) -> list[dict[str, Any]]:
    """Rows in the Notion SQL-query shape: ``date:X:start`` columns, JSON
    arrays for multi-selects, ``__YES__`` for checkboxes."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    rows = raw["results"] if isinstance(raw, dict) else raw
    out = []
    for r in rows:
        flat: dict[str, Any] = {}
        for prop, (_, kind) in _COLUMNS.items():
            if kind == "date":
                flat[prop] = r.get(f"date:{prop}:start")
            else:
                flat[prop] = r.get(prop)
        flat["_page_id"] = r.get("id")
        out.append(flat)
    return out


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8-sig", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _read_notion_api(data_source_id: str) -> list[dict[str, Any]]:
    import urllib.error
    import urllib.request

    _load_env()
    key = os.environ.get("NOTION_API_KEY")
    if not key:
        sys.exit(
            "NOTION_API_KEY is not set (looked in the environment, backend/.env and the "
            "repo-root .env) — create an internal integration, share the database with "
            "it, and put the token in backend/.env"
        )
    url = f"https://api.notion.com/v1/data_sources/{data_source_id}/query"
    rows: list[dict[str, Any]] = []
    cursor: str | None = None
    while True:
        body: dict[str, Any] = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        req = urllib.request.Request(
            url,
            data=json.dumps(body).encode(),
            headers={
                "Authorization": f"Bearer {key}",
                "Notion-Version": NOTION_VERSION,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:  # noqa: S310
                payload = json.load(resp)
        except urllib.error.HTTPError as e:
            hint = {
                401: "the token is wrong or revoked — copy the 'Internal Integration Secret' again",
                403: "the integration lacks the 'Read content' capability",
                404: "the database is not shared with the integration — on the database page, "
                "… → Connections → add it (Notion answers 404, not 403, for an unshared page)",
            }.get(e.code, e.read().decode(errors="replace")[:300])
            sys.exit(f"Notion API {e.code} for data source {data_source_id}: {hint}")
        for page in payload.get("results", []):
            flat = {"_page_id": page.get("id")}
            for prop, value in (page.get("properties") or {}).items():
                flat[prop] = _api_value(value)
            rows.append(flat)
        if not payload.get("has_more"):
            break
        cursor = payload.get("next_cursor")
    return rows


def _api_value(value: dict[str, Any]) -> Any:
    t = value.get("type")
    v = value.get(t)
    if t in ("title", "rich_text"):
        return "".join(part.get("plain_text", "") for part in (v or []))
    if t == "select":
        return (v or {}).get("name")
    if t == "multi_select":
        return [opt.get("name") for opt in (v or [])]
    if t == "date":
        return (v or {}).get("start")
    if t == "checkbox":
        return "__YES__" if v else "__NO__"
    if t == "url":
        return v
    return v


# ----------------------------------------------------------------------
# Transform
# ----------------------------------------------------------------------
def _parse_date(raw: Any) -> str | None:
    if raw in (None, "", []):
        return None
    s = str(raw).strip()
    if not s:
        return None
    s = s.split("→")[0].strip()  # Notion CSV writes ranges as "A → B"
    for fmt in ("%Y-%m-%d", "%B %d, %Y", "%d %B %Y", "%b %d, %Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s[:10] if fmt == "%Y-%m-%d" else s, fmt).date().isoformat()
        except ValueError:
            continue
    raise SystemExit(f"unparseable date {raw!r}")


def _parse_multi(raw: Any) -> list[str]:
    if raw in (None, ""):
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    s = str(raw).strip()
    if s.startswith("["):
        return [str(x).strip() for x in json.loads(s) if str(x).strip()]
    return [p.strip() for p in s.split(",") if p.strip()]


def _parse_text(raw: Any) -> str | None:
    if raw is None:
        return None
    s = str(raw).strip()
    return s or None


def _parse_checkbox(raw: Any) -> bool:
    return str(raw).strip().lower() in ("__yes__", "yes", "true", "1", "checked")


def _set(target: dict[str, Any], dotted: str, value: Any) -> None:
    parts = dotted.split(".")
    for p in parts[:-1]:
        target = target.setdefault(p, {})
    target[parts[-1]] = value


def transform(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        j: dict[str, Any] = {}
        for prop, (path, kind) in _COLUMNS.items():
            raw = r.get(prop)
            if kind == "date":
                val: Any = _parse_date(raw)
            elif kind == "multi":
                val = _parse_multi(raw)
            elif kind == "checkbox":
                val = _parse_checkbox(raw)
            elif kind == "url":
                val = _parse_text(raw)
            else:
                val = _parse_text(raw)
            _set(j, path, val)
        if not j.get("code") or not j.get("name"):
            continue  # a blank row Notion adds at the bottom of a table
        j["code"] = j["code"].strip().upper()
        sources_text = j.pop("_sources_text", None) or ""
        j["sources"] = [{"url": u} for u in _URL_RE.findall(sources_text)]
        # ``access_url`` = where to learn how to apply — the first source URL,
        # the semantics eu_bo_access.json's field carried before Phase 223.
        j["bo_register"]["access_url"] = j["sources"][0]["url"] if j["sources"] else None
        j["notion_page_id"] = r.get("_page_id")
        out.append(j)
    out.sort(key=lambda x: x["code"])
    return out


def render(jurisdictions: list[dict[str, Any]], *, generated_at: str) -> str:
    doc = {
        "_comment": (
            "GENERATED by backend/scripts/sync_jurisdictions.py from the Notion database "
            "'Beneficial ownership access status' — do not hand-edit; edit the table and re-run."
        ),
        "notion_data_source_id": DATA_SOURCE_ID,
        "generated_at": generated_at,
        "jurisdictions": jurisdictions,
    }
    return json.dumps(doc, indent=2, ensure_ascii=False) + "\n"


def _strip_timestamp(text: str) -> str:
    return re.sub(r'"generated_at": "[^"]*"', '"generated_at": ""', text)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--notion", action="store_true", help="pull from the Notion API (NOTION_API_KEY)")
    src.add_argument("--csv", type=Path, help="a CSV exported from Notion")
    src.add_argument("--rows-json", type=Path, help="rows in the Notion SQL-query JSON shape")
    ap.add_argument("--data-source-id", default=DATA_SOURCE_ID)
    ap.add_argument("--out", type=Path, default=OUT_PATH)
    ap.add_argument("--check", action="store_true", help="exit 1 if the committed file is out of date")
    args = ap.parse_args(argv)

    if args.notion:
        rows = _read_notion_api(args.data_source_id)
    elif args.csv:
        rows = _read_csv(args.csv)
    else:
        rows = _read_rows_json(args.rows_json)

    jurisdictions = transform(rows)
    text = render(jurisdictions, generated_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat())

    if args.check:
        current = args.out.read_text(encoding="utf-8") if args.out.exists() else ""
        if _strip_timestamp(current) != _strip_timestamp(text):
            print(f"{args.out} is out of date with the source ({len(jurisdictions)} rows)", file=sys.stderr)
            return 1
        print(f"{args.out} is in sync ({len(jurisdictions)} rows)")
        return 0

    args.out.write_text(text, encoding="utf-8")
    verified = sum(1 for j in jurisdictions if j.get("last_verified"))
    print(f"wrote {args.out}: {len(jurisdictions)} jurisdictions, {verified} verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
