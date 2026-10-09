"""Phase 318 — the EITI organisation index records when it was read, to the second.

``meta.generated`` becomes ``source.retrievedAt`` on every EITI match (Phase
314). It was a bare day (``2026-07-07``), which reads as midnight on the
statement's own date: exactly the shape of a register cut published as the
download, so the BODS quality sweep's ``cut_as_retrieval`` check flagged it in
production after Phase 317 (TAQA lookup, 9 Oct 2026). The value was honest —
OpenCheck's harvest, with no cut of EITI's own — but nothing in the output
could show that. The fix is to record the moment.
"""

from __future__ import annotations

import datetime as dt
import gzip
import json
import os
import urllib.error
from pathlib import Path
from typing import Any

import pytest

from opencheck import bods_quality as bq
from opencheck import provenance
from opencheck.bods import map_eiti
from opencheck.bods.statements import OFFICIAL_REGISTER_SOURCES
from opencheck.provenance import Provenance
from opencheck.sources.eiti import _INDEX_PATH
from scripts import build_eiti_index as build

_FULL = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"


def test_stamp_is_utc_to_the_second() -> None:
    moment = dt.datetime(2026, 10, 9, 21, 36, 12, 987654, tzinfo=dt.timezone(dt.timedelta(hours=2)))
    assert build._stamp(moment) == "2026-10-09T19:36:12Z"


def test_an_offline_rebuild_is_dated_by_its_oldest_page(tmp_path: Path) -> None:
    """A rebuild from downloaded pages was harvested when the first page came
    down, not when the rebuild ran."""
    for n, when in ((0, 1_780_000_000), (1, 1_780_000_600)):
        page = tmp_path / f"page_{n}.json"
        page.write_text(json.dumps({"data": []}))
        os.utime(page, (when, when))
    data = build.build_from_dir(tmp_path)
    assert data["meta"]["generated"] == build._stamp(
        dt.datetime.fromtimestamp(1_780_000_000, dt.timezone.utc)
    )


def test_the_crawl_identifies_itself() -> None:
    """EITI's Cloudflare zone answers 403 to urllib's default User-Agent."""
    assert build.HEADERS["User-Agent"].startswith("OpenCheck/")


def test_a_page_is_retried_before_the_crawl_gives_up(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    class _Resp:
        def __enter__(self) -> "_Resp":
            return self

        def __exit__(self, *exc: Any) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps({"data": [{"id": "1"}]}).encode()

    def _urlopen(req: Any, timeout: int) -> _Resp:
        calls.append(req.full_url)
        if len(calls) < 3:
            raise urllib.error.URLError("handshake timed out")
        return _Resp()

    monkeypatch.setattr(build.urllib.request, "urlopen", _urlopen)
    monkeypatch.setattr(build.time, "sleep", lambda s: None)
    assert build._fetch_page(7) == [{"id": "1"}]
    assert len(calls) == 3


def test_the_committed_index_records_a_moment() -> None:
    with gzip.open(_INDEX_PATH, "rt", encoding="utf-8") as f:
        generated = json.load(f)["meta"]["generated"]
    assert generated and __import__("re").match(_FULL, generated), generated


def test_an_eiti_match_no_longer_reads_as_a_cut() -> None:
    """End to end: a full stamp as retrievedAt passes the sweep's check, where
    the bare day it replaces was flagged."""
    bundle = {
        "source_id": "eiti", "country": "MN", "identification": "2016656",
        "entity_name": "Tavantolgoi JSC", "is_stub": False,
    }

    def _sweep(generated: str) -> list[str]:
        prov = Provenance(liveness="snapshot", retrieved_at=provenance.parse_moment(generated))
        with provenance.mapping_provenance(prov):
            statements = list(map_eiti(bundle))
        findings = bq.check_statements(
            statements,
            today=dt.date.today().isoformat(),
            liveness={"eiti": "snapshot"},
            no_cut=frozenset({"eiti"}),
            official_registers=OFFICIAL_REGISTER_SOURCES,
        )
        return [f.check for f in findings]

    # The same July harvest, as a bare day and as the moment it happened. (A
    # past day: a snapshot dated the day the sweep runs is its own failure.)
    assert "cut_as_retrieval" in _sweep("2026-07-07")
    assert _sweep("2026-07-07T09:15:00Z") == []
