"""OpenCheck's own harvest of MTender's API (``opencheck/mtender_harvest.py``).

Packages are shaped like MTender's answers of 13 Nov 2026: the procedure's
own record carries the title, method and buyer, a stage record
(``<ocid>-EV-…``) carries the lots, the parties and the awards. Contact
points and addresses are in the fixtures so the tests can show they are
never kept.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

from opencheck import mtender_harvest as mh
from opencheck import provenance
from opencheck.sources import mtender_moldova
from opencheck.sources.mtender_moldova import build_index

BUYER = "MD-IDNO-1007601006450"
WINNER = "MD-IDNO-1016600004811"
LOSER = "MD-IDNO-1003600012345"


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    monkeypatch.setattr(mh.time, "sleep", lambda _s: None)


def _party(pid: str, name: str) -> dict[str, Any]:
    return {
        "id": pid,
        "name": name,
        "identifier": {"scheme": "MD-IDNO", "id": pid.removeprefix("MD-IDNO-"), "legalName": name},
        "address": {"streetAddress": "str. Exemplu 1"},
        "contactPoint": {
            "name": "Ion Popescu",
            "email": "ion@example.md",
            "telephone": "+37300000000",
        },
        "roles": ["supplier"],
    }


def package(
    ocid: str,
    *,
    awards: list[dict[str, Any]] | None = None,
    stage_status: str = "active",
    method: str = "smallValue",
) -> dict[str, Any]:
    own = {
        "ocid": ocid,
        "date": "2026-11-09T05:00:00Z",
        "tag": ["compiled"],
        "tender": {
            "title": "Produse alimentare",
            "status": "active",
            "procurementMethodDetails": method,
            "mainProcurementCategory": "goods",
            "procuringEntity": {"id": BUYER, "name": "PRIMARIA COMUNEI VATICI"},
            "items": [{"id": "i1", "description": "Cașcaval"}],
        },
        "parties": [_party(BUYER, "PRIMARIA COMUNEI VATICI")],
    }
    stage = {
        "ocid": f"{ocid}-EV-1",
        "date": "2026-11-09T07:34:58Z",
        "tag": ["awardUpdate"],
        "tender": {"status": stage_status, "lots": [{"id": "lot-1", "status": "active"}]},
        "parties": [_party(WINNER, 'S.R.L. "KAUFLAND"'), _party(LOSER, "ALTA S.R.L.")],
        "awards": awards
        if awards is not None
        else [
            {
                "id": "a1",
                "status": "pending",
                "statusDetails": "active",
                "date": "2026-11-09T07:34:58Z",
                "value": {"amount": 37754.07, "currency": "MDL", "amountNet": 45304.88},
                "suppliers": [{"id": WINNER, "name": 'S.R.L. "KAUFLAND"'}],
                "relatedLots": ["lot-1"],
                "items": [{"id": "i1"}],
            },
            {
                "id": "a2",
                "status": "pending",
                "statusDetails": "unsuccessful",
                "suppliers": [{"id": LOSER, "name": "ALTA S.R.L."}],
                "relatedLots": ["lot-1"],
            },
        ],
    }
    # MTender does not keep the procedure's own record first.
    return {
        "records": [
            {
                "ocid": f"{ocid}-PN-1",
                "compiledRelease": {"ocid": f"{ocid}-PN-1", "tag": ["planningUpdate"]},
            },
            {"ocid": ocid, "compiledRelease": own},
            {"ocid": f"{ocid}-EV-1", "compiledRelease": stage},
        ]
    }


# ---------------------------------------------------------------------------
# flatten
# ---------------------------------------------------------------------------


def test_flatten_joins_the_stage_awards_to_the_procedure() -> None:
    release = mh.flatten(package("ocds-b3wdp1-MD-1"), "ocds-b3wdp1-MD-1")
    assert release is not None
    assert release["ocid"] == "ocds-b3wdp1-MD-1"
    assert release["title"] == release["tender"]["title"] == "Produse alimentare"
    assert release["tender"]["procurementMethodDetails"] == "smallValue"
    assert release["tender"]["procuringEntity"] == {"id": BUYER, "name": "PRIMARIA COMUNEI VATICI"}
    assert release["tender"]["lots"] == [{"id": "lot-1", "status": "active"}]
    assert release["date"] == "2026-11-09T07:34:58Z"
    assert [a["id"] for a in release["awards"]] == ["a1"]  # the winner only
    assert release["awards"][0]["value"] == {"amount": 37754.07, "currency": "MDL"}
    assert {p["id"] for p in release["parties"]} == {BUYER, WINNER}


def test_flatten_never_keeps_contacts_addresses_or_items() -> None:
    text = json.dumps(
        mh.flatten(package("ocds-b3wdp1-MD-1"), "ocds-b3wdp1-MD-1"), ensure_ascii=False
    )
    for word in (
        "contactPoint",
        "Ion Popescu",
        "ion@example.md",
        "address",
        "Exemplu",
        "items",
        "Cașcaval",
        "roles",
    ):
        assert word not in text


def test_a_cancelled_stage_cancels_its_lots() -> None:
    release = mh.flatten(package("ocds-b3wdp1-MD-1", stage_status="cancelled"), "ocds-b3wdp1-MD-1")
    assert release is not None
    assert release["tender"]["lots"] == [{"id": "lot-1", "status": "cancelled"}]


def test_an_unknown_procedure_is_none() -> None:
    assert mh.flatten({}, "ocds-b3wdp1-MD-0") is None


def test_the_flattened_release_is_what_the_index_reads(tmp_path: Path) -> None:
    release = mh.flatten(package("ocds-b3wdp1-MD-1"), "ocds-b3wdp1-MD-1")
    meta = build_index([release], tmp_path / "m.sqlite")
    assert meta["awards_kept"] == "1" and meta["suppliers"] == "1"
    assert meta["data_to"] == "2026-11-09"


# ---------------------------------------------------------------------------
# harvest
# ---------------------------------------------------------------------------


class FakeMTender:
    """The listing in pages of the given ocids, then ``{}``."""

    def __init__(
        self,
        pages: list[list[str]],
        packages: dict[str, Any],
        *,
        fail: set[str] | None = None,
        empty_listing: bool = False,
        listing_502s: int = 0,
    ) -> None:
        self.pages = pages
        self.packages = packages
        self.fail = fail or set()
        self.empty_listing = empty_listing
        self.listing_502s = listing_502s
        self.requests: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(str(request.url))
        path = request.url.path.rstrip("/")
        if path == "/tenders":
            if self.listing_502s:
                self.listing_502s -= 1
                return httpx.Response(502)
            if self.empty_listing:
                return httpx.Response(200, content=b"")
            offset = request.url.params["offset"]
            index = 0 if not offset.startswith("page-") else int(offset.removeprefix("page-"))
            if index >= len(self.pages):
                return httpx.Response(200, json={})
            rows = [{"ocid": o, "date": "2026-11-09T07:00:00Z"} for o in self.pages[index]]
            return httpx.Response(200, json={"data": rows, "offset": f"page-{index + 1}"})
        ocid = path.rsplit("/", 1)[-1]
        if ocid in self.fail:
            return httpx.Response(503)
        return httpx.Response(200, json=self.packages.get(ocid, {}))


def _client(fake: FakeMTender) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(fake))


def test_harvest_reads_every_page_then_stops_caught_up() -> None:
    fake = FakeMTender(
        [["ocds-b3wdp1-MD-1", "ocds-b3wdp1-MD-2"], ["ocds-b3wdp1-MD-3"]],
        {
            "ocds-b3wdp1-MD-1": package("ocds-b3wdp1-MD-1"),
            "ocds-b3wdp1-MD-2": package("ocds-b3wdp1-MD-2", awards=[]),  # nothing won yet
            "ocds-b3wdp1-MD-3": package("ocds-b3wdp1-MD-3"),
        },
    )
    store = mh.Store(cursor="page-0")
    result = mh.harvest(_client(fake), store, workers=2)
    assert result["outcome"] == "caught_up"
    assert result["pages"] == 2 and result["packages"] == 3
    assert sorted(store.releases) == ["ocds-b3wdp1-MD-1", "ocds-b3wdp1-MD-3"]
    assert store.cursor == "page-2" and store.caught_up_at


def test_a_procedure_whose_award_went_is_removed() -> None:
    fake = FakeMTender(
        [["ocds-b3wdp1-MD-1"]], {"ocds-b3wdp1-MD-1": package("ocds-b3wdp1-MD-1", awards=[])}
    )
    store = mh.Store(
        cursor="page-0", releases={"ocds-b3wdp1-MD-1": {"ocid": "ocds-b3wdp1-MD-1", "awards": [{}]}}
    )
    result = mh.harvest(_client(fake), store)
    assert store.releases == {} and result["dropped"] == 1


def test_a_failed_package_keeps_the_cursor_before_its_page() -> None:
    fake = FakeMTender(
        [["ocds-b3wdp1-MD-1"], ["ocds-b3wdp1-MD-2", "ocds-b3wdp1-MD-3"]],
        {o: package(o) for o in ("ocds-b3wdp1-MD-1", "ocds-b3wdp1-MD-2", "ocds-b3wdp1-MD-3")},
        fail={"ocds-b3wdp1-MD-3"},
    )
    store = mh.Store(cursor="page-0")
    result = mh.harvest(_client(fake), store)
    assert result["outcome"] == "package_failed"
    assert store.cursor == "page-1"  # the next run reads page 1 again, whole
    assert sorted(store.releases) == ["ocds-b3wdp1-MD-1"]
    # three attempts before giving up
    assert sum(1 for u in fake.requests if u.endswith("MD-3")) == mh.ATTEMPTS


def test_an_empty_listing_body_is_a_failure_not_caught_up() -> None:
    fake = FakeMTender([], {}, empty_listing=True)
    store = mh.Store(cursor="2024-01-01T00:00:00Z")
    with pytest.raises(mh.HarvestError):
        mh.harvest(_client(fake), store)
    assert store.caught_up_at is None


def test_a_spent_budget_reads_nothing() -> None:
    fake = FakeMTender([["ocds-b3wdp1-MD-1"]], {"ocds-b3wdp1-MD-1": package("ocds-b3wdp1-MD-1")})
    store = mh.Store()
    result = mh.harvest(_client(fake), store, deadline=0.0, now=datetime(2026, 11, 13, tzinfo=UTC))
    assert result["outcome"] == "deadline" and fake.requests == []
    assert store.cursor == store.started_from == "2025-11-28T00:00:00Z"


# ---------------------------------------------------------------------------
# The store and the build
# ---------------------------------------------------------------------------


def test_the_store_round_trips(tmp_path: Path) -> None:
    release = mh.flatten(package("ocds-b3wdp1-MD-1"), "ocds-b3wdp1-MD-1")
    store = mh.Store(cursor="2026-11-10T06:32:17.000Z", started_from="2025-11-28T00:00:00Z")
    store.put("ocds-b3wdp1-MD-1", release)
    state = store.save(tmp_path)
    assert state["releases"] == 1
    again = mh.Store.load(tmp_path)
    assert again.releases == store.releases
    assert again.cursor == store.cursor and again.started_from == store.started_from


def test_the_harvest_replaces_the_registrys_reading(tmp_path: Path) -> None:
    harvested = mh.flatten(package("ocds-b3wdp1-MD-1"), "ocds-b3wdp1-MD-1")
    store = mh.Store(releases={"ocds-b3wdp1-MD-1": harvested})
    registry = [
        {"ocid": "ocds-b3wdp1-MD-1", "awards": []},  # the registry's planning-only copy
        {"ocid": "ocds-b3wdp1-MD-9"},
    ]
    merged = list(mh.merged_releases(registry, store))
    assert [r["ocid"] for r in merged] == ["ocds-b3wdp1-MD-9", "ocds-b3wdp1-MD-1"]
    assert merged[1] is harvested


def test_the_cut_is_how_far_the_harvest_has_read() -> None:
    with provenance.recording() as recorder:
        mtender_moldova.declare_snapshot(
            {
                "built_at": "2026-11-13T10:00:00+00:00",
                "harvest_cursor": "2026-11-12T23:59:01.000Z",
                "source_last_modified": "Tue, 21 Oct 2026 06:00:00 GMT",
                "data_to": "2026-11-12",
            }
        )
    resolved = recorder.resolve()
    assert resolved.source_as_of is not None
    assert resolved.source_as_of.date().isoformat() == "2026-11-12"


def test_a_502_spell_on_the_listing_is_waited_out(monkeypatch) -> None:
    # 13 Nov 2026: MTender answered 502 for a minute or more; three tries a few
    # seconds apart ended a backfill run and failed the next two.
    waits: list[float] = []
    monkeypatch.setattr(mh.time, "sleep", waits.append)
    fake = FakeMTender(
        [["ocds-b3wdp1-MD-1"]],
        {"ocds-b3wdp1-MD-1": package("ocds-b3wdp1-MD-1")},
        listing_502s=4,
    )
    store = mh.Store(cursor="page-0")
    result = mh.harvest(_client(fake), store)
    assert result["outcome"] == "caught_up"
    assert "ocds-b3wdp1-MD-1" in store.releases
    assert waits == list(mh.BACKOFF_S[:4])
    assert sum(mh.BACKOFF_S) >= 180
