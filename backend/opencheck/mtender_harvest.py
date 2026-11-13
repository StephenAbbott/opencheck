"""OpenCheck's own harvest of MTender's OCDS API, kept between runs.

Why this exists
---------------
The ``mtender_moldova`` index was first built from the Open Contracting
Partnership's Data Registry copy of MTender (publication 34). That copy's
latest award is dated 3 March 2026, while MTender's own API holds awards
made this week. This module reads the API itself and keeps what it read, so
each weekly build only has to fetch what changed.

The API (measured 13 Nov 2026)
------------------------------
* ``GET public.mtender.gov.md/tenders/?offset=<ISO time>`` lists 100
  procedures at a time as ``{"data": [{"ocid", "date"}], "offset": <next>}``,
  ordered by ``date``, the procedure's latest update. A procedure that
  changes moves to a later ``date``, so reading forward from where the last
  run stopped finds everything that changed. When nothing is left the answer
  is ``{}``. An offset older than about a year answers HTTP 200 with an
  empty body, which is read as a failure, never as "nothing left".
* ``GET public.mtender.gov.md/tenders/<ocid>`` returns a record package with
  one record per stage. The awards are not on the procedure's own record
  (``ocid`` = the procedure): they sit on its stage records
  (``<ocid>-EV-…``, ``<ocid>-NP-…``), while the title, the method, the
  category and the buyer sit on the procedure's own record. ``flatten``
  puts them back together into the one release ``build_index`` reads — the
  shape the registry's file already has. An unknown ocid answers ``{}``.
* About 900 procedures change on a weekday. Four requests at a time read
  about five packages a second, so a week's changes take around twenty
  minutes and a year's backfill about eleven hours, spread over runs by the
  time budget.

What is kept
------------
Only procedures with at least one winning award (``statusDetails`` is
``active``), and only the fields the index reads (``reduce_release``): those
awards with their status, date, value,
suppliers and lots; each lot's status; the parties named as a supplier or
the buyer, by name and identifier; the procedure's title, method, category,
status and buyer. Never a party's ``contactPoint`` or address, items,
documents, bids or requirement responses. A procedure re-read with no award
left is removed, so an award that stops being active leaves with the read
that sees it.

The store is ``mtender_harvest.jsonl.gz`` (one release a line, sorted by
ocid) and ``mtender_harvest_state.json`` (the cursor), both kept as assets
on the ``mtender-harvest`` release by
``.github/workflows/refresh-mtender-index.yml``. The cursor moves past a
page only once every package on it was read, so a run that stops — the time
budget, an outage — resumes where it stopped and loses nothing.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import time
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)

API_BASE = "https://public.mtender.gov.md/tenders/"
STORE_NAME = "mtender_harvest.jsonl.gz"
STATE_NAME = "mtender_harvest_state.json"
#: How far back a first run starts. MTender's listing answers nothing for an
#: offset older than about a year (2025-01-01 was empty, 2025-06-01 was not,
#: on 13 Nov 2026), so a first run starts inside that window.
FIRST_RUN_DAYS = 350
WORKERS = 4
ATTEMPTS = 3
TIMEOUT_S = 60.0
_USER_AGENT = "OpenCheck (+https://github.com/StephenAbbott/opencheck)"


class HarvestError(RuntimeError):
    """MTender answered something that is not its API's shape."""


# ---------------------------------------------------------------------------
# One package → one release
# ---------------------------------------------------------------------------


def _party(party: dict[str, Any]) -> dict[str, Any]:
    ident = party.get("identifier") or {}
    return {
        "id": party.get("id"),
        "name": party.get("name"),
        "identifier": {
            k: ident[k] for k in ("scheme", "id", "legalName") if ident.get(k) is not None
        },
    }


def _award(award: dict[str, Any]) -> dict[str, Any]:
    value = award.get("value") or {}
    return {
        "id": award.get("id"),
        "status": award.get("status"),
        "statusDetails": award.get("statusDetails"),
        "date": award.get("date"),
        "value": {k: value[k] for k in ("amount", "currency") if k in value},
        "suppliers": [
            {"id": s.get("id"), "name": s.get("name")} for s in award.get("suppliers") or []
        ],
        "relatedLots": list(award.get("relatedLots") or []),
    }


def reduce_release(release: dict[str, Any]) -> dict[str, Any]:
    """Only the fields ``build_index`` reads (see the module docstring)."""
    tender = release.get("tender") or {}
    procuring = tender.get("procuringEntity") or {}
    # Only the winning awards: a procedure is re-read whole whenever it
    # changes, so an award that stops being active leaves with the next read.
    awards = [_award(a) for a in release.get("awards") or [] if a.get("statusDetails") == "active"]
    named = {s["id"] for a in awards for s in a["suppliers"]}
    if procuring.get("id"):
        named.add(procuring["id"])
    return {
        "ocid": release.get("ocid"),
        "date": release.get("date"),
        "title": release.get("title") or tender.get("title"),
        "tender": {
            "status": tender.get("status"),
            "title": tender.get("title"),
            "procurementMethodDetails": tender.get("procurementMethodDetails"),
            "mainProcurementCategory": tender.get("mainProcurementCategory"),
            "procuringEntity": {"id": procuring.get("id"), "name": procuring.get("name")},
            "lots": [
                {"id": lot.get("id"), "status": lot.get("status")}
                for lot in tender.get("lots") or []
            ],
        },
        "parties": [_party(p) for p in release.get("parties") or [] if p.get("id") in named],
        "awards": awards,
    }


def flatten(package: dict[str, Any], ocid: str) -> dict[str, Any] | None:
    """The procedure's stage records as one release, reduced; None when the
    package holds no record of ``ocid`` (MTender's ``{}``)."""
    records = package.get("records") or []
    own: dict[str, Any] | None = None
    stages: list[dict[str, Any]] = []
    for record in records:
        compiled = record.get("compiledRelease") or {}
        if str(record.get("ocid") or compiled.get("ocid") or "") == ocid:
            own = compiled
        else:
            stages.append(compiled)
    if own is None and not stages:
        return None
    own = own or {}
    tender = dict(own.get("tender") or {})
    lots: list[dict[str, Any]] = list(tender.get("lots") or [])
    parties: dict[str, dict[str, Any]] = {}
    awards: list[dict[str, Any]] = list(own.get("awards") or [])
    for party in own.get("parties") or []:
        if party.get("id"):
            parties[party["id"]] = party
    dates = [str(own.get("date") or "")]
    for stage in stages:
        dates.append(str(stage.get("date") or ""))
        stage_tender = stage.get("tender") or {}
        stage_cancelled = stage_tender.get("status") == "cancelled"
        for lot in stage_tender.get("lots") or []:
            lots.append({**lot, "status": "cancelled"} if stage_cancelled else lot)
        for party in stage.get("parties") or []:
            if party.get("id"):
                parties.setdefault(party["id"], party)
        awards.extend(stage.get("awards") or [])
        for key in (
            "title",
            "procurementMethodDetails",
            "mainProcurementCategory",
            "procuringEntity",
        ):
            if not tender.get(key) and stage_tender.get(key):
                tender[key] = stage_tender[key]
    tender["lots"] = lots
    merged = {
        "ocid": ocid,
        "date": max(dates),
        "title": own.get("title"),
        "tender": tender,
        "parties": list(parties.values()),
        "awards": awards,
    }
    return reduce_release(merged)


# ---------------------------------------------------------------------------
# The store
# ---------------------------------------------------------------------------


@dataclass
class Store:
    releases: dict[str, dict[str, Any]] = field(default_factory=dict)
    cursor: str | None = None
    caught_up_at: str | None = None
    started_from: str | None = None

    @classmethod
    def load(cls, directory: Path | str) -> Store:
        directory = Path(directory)
        store = cls()
        state_path = directory / STATE_NAME
        if state_path.exists():
            state = json.loads(state_path.read_text(encoding="utf-8"))
            store.cursor = state.get("cursor")
            store.caught_up_at = state.get("caught_up_at")
            store.started_from = state.get("started_from")
        data_path = directory / STORE_NAME
        if data_path.exists():
            with gzip.open(data_path, "rt", encoding="utf-8") as fh:
                for line in fh:
                    if line.strip():
                        release = json.loads(line)
                        store.releases[release["ocid"]] = release
        return store

    def save(self, directory: Path | str) -> dict[str, Any]:
        """Write both files beside their targets and rename into place."""
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        data_path = directory / STORE_NAME
        tmp = data_path.with_name(data_path.name + ".tmp")
        with gzip.open(tmp, "wt", encoding="utf-8", compresslevel=9) as fh:
            for ocid in sorted(self.releases):
                fh.write(json.dumps(self.releases[ocid], ensure_ascii=False, sort_keys=True))
                fh.write("\n")
        os.replace(tmp, data_path)
        state = {
            "cursor": self.cursor,
            "caught_up_at": self.caught_up_at,
            "started_from": self.started_from,
            "releases": len(self.releases),
            "saved_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }
        state_path = directory / STATE_NAME
        state_tmp = state_path.with_name(state_path.name + ".tmp")
        state_tmp.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
        os.replace(state_tmp, state_path)
        return state

    def put(self, ocid: str, release: dict[str, Any] | None) -> None:
        if release and release.get("awards"):
            self.releases[ocid] = release
        else:
            self.releases.pop(ocid, None)


def merged_releases(registry: Iterable[dict[str, Any]], store: Store) -> Iterator[dict[str, Any]]:
    """The registry's releases, each replaced by the harvest's reading of the
    same procedure where there is one, then the procedures only the harvest
    holds."""
    for release in registry:
        if str(release.get("ocid") or "") not in store.releases:
            yield release
    yield from store.releases.values()


# ---------------------------------------------------------------------------
# Reading the API
# ---------------------------------------------------------------------------


def _get(client: httpx.Client, url: str, params: dict[str, str] | None = None) -> Any:
    """GET with a short backoff on 429, 5xx and network errors; None for an
    empty body."""
    last: Exception | None = None
    for attempt in range(ATTEMPTS):
        try:
            resp = client.get(url, params=params)
            if resp.status_code == 429 or resp.status_code >= 500:
                raise HarvestError(f"HTTP {resp.status_code}")
            resp.raise_for_status()
            if not resp.content.strip():
                return None
            return resp.json()
        except (httpx.HTTPError, HarvestError, ValueError) as exc:
            last = exc
            if attempt + 1 < ATTEMPTS:
                time.sleep(2 * (attempt + 1))
    raise HarvestError(f"{url}: {type(last).__name__}: {last}")


def first_cursor(now: datetime | None = None) -> str:
    start = (now or datetime.now(UTC)) - timedelta(days=FIRST_RUN_DAYS)
    return start.strftime("%Y-%m-%dT00:00:00Z")


def client() -> httpx.Client:
    return httpx.Client(
        timeout=TIMEOUT_S,
        headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
        follow_redirects=True,
    )


def harvest(
    http: httpx.Client,
    store: Store,
    *,
    deadline: float | None = None,
    workers: int = WORKERS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Read forward from ``store.cursor`` until caught up or past ``deadline``
    (a ``time.monotonic()`` value). Changes ``store`` in place; never moves
    the cursor past a page that was not read whole. Raises ``HarvestError``
    only when the listing itself cannot be read on the first page."""
    if store.cursor is None:
        store.cursor = first_cursor(now)
        store.started_from = store.cursor
    counts = {"pages": 0, "packages": 0, "kept": 0, "dropped": 0}
    outcome = "deadline"
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        while deadline is None or time.monotonic() < deadline:
            try:
                page = _get(http, API_BASE, {"offset": store.cursor})
            except HarvestError as exc:
                if counts["pages"] == 0:
                    raise
                logger.warning("mtender harvest: listing stopped at %s: %s", store.cursor, exc)
                outcome = "listing_failed"
                break
            if page is None:
                if counts["pages"] == 0:
                    raise HarvestError(
                        f"listing answered an empty body at offset {store.cursor} "
                        "(older than MTender keeps?)"
                    )
                outcome = "listing_failed"
                break
            if not isinstance(page, dict):
                raise HarvestError("listing is not a JSON object")
            rows = page.get("data") or []
            if not rows:
                outcome = "caught_up"
                store.caught_up_at = datetime.now(UTC).isoformat(timespec="seconds")
                break
            ocids = list(dict.fromkeys(str(r.get("ocid") or "") for r in rows if r.get("ocid")))
            try:
                packages = list(pool.map(lambda o: _get(http, API_BASE + o), ocids))
            except HarvestError as exc:
                logger.warning("mtender harvest: a package failed at %s: %s", store.cursor, exc)
                outcome = "package_failed"
                break
            for ocid, package in zip(ocids, packages, strict=True):
                release = flatten(package, ocid) if isinstance(package, dict) else None
                had = ocid in store.releases
                store.put(ocid, release)
                counts["packages"] += 1
                if ocid in store.releases:
                    counts["kept"] += 1
                elif had:
                    counts["dropped"] += 1
            counts["pages"] += 1
            next_cursor = page.get("offset")
            if not next_cursor or next_cursor == store.cursor:
                outcome = "caught_up"
                store.caught_up_at = datetime.now(UTC).isoformat(timespec="seconds")
                break
            store.cursor = str(next_cursor)
    return {"outcome": outcome, "cursor": store.cursor, "releases": len(store.releases), **counts}
