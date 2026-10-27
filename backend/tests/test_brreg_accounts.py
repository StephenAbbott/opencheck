"""Brreg annual accounts (Regnskapsregisteret) on the brreg card.

Fixtures in ``tests/data/brreg_accounts_live.json`` are **real answers** from
``data.brreg.no/regnskapsregisteret/regnskap`` captured on 26 Oct 2026 (see
its ``_captured`` note): Equinor ASA (company + group accounts, USD, IFRS),
ARNTZEN BAKERI AS (small company, audit opted out), DNB Bank ASA (the BANK
format the open API refuses with HTTP 500) and 123ENKELT AS (registered
1 Sept 2026, no accounts yet: HTTP 404).

Stephen's decisions (26 Oct 2026): part of the brreg bundle; facts about the
latest period only, no figures; link the latest filed PDF; nothing in BODS;
404 = "no annual accounts in the last three filed years", never "overdue";
BANK/LIV/SKADE = "a format the open API does not serve".
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from pytest_httpx import HTTPXMock

from opencheck import degradation
from opencheck.bods.mapper import map_brreg
from opencheck.cache import Cache
from opencheck.config import get_settings
from opencheck.findings import MAX_FINDING_CHARS, brreg_accounts_clauses, finding_brreg
from opencheck.knowability import BANNED_VOCABULARY
from opencheck.routers.hit_builders import _bh_brreg, _LookupCtx
from opencheck.sources.brreg import BrregAdapter
from opencheck.sources.brreg_accounts import (
    ACCOUNTS_BASE,
    read_accounts,
    summarise,
    unsupported_format,
)

_RESPONSES: dict[str, Any] = json.loads(
    (Path(__file__).parent / "data" / "brreg_accounts_live.json").read_text(encoding="utf-8")
)["responses"]

EQUINOR = "923609016"
BAKERY = "929403185"
DNB = "984851006"
NEW_AS = "938386080"


def _body(key: str) -> Any:
    return json.loads(json.dumps(_RESPONSES[key]["body"]))


@pytest.fixture
def live(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# summarise — the latest period's facts, never a figure
# ---------------------------------------------------------------------------


def _no_figures(summary: dict[str, Any]) -> None:
    """No amount from the accounts reaches the summary (decision 2)."""
    for key, value in summary.items():
        assert not (isinstance(value, float)), (key, value)
        if isinstance(value, int) and not isinstance(value, bool):
            assert key == "year", (key, value)
    text = json.dumps(summary)
    for figure_key in ("sum", "aarsresultat", "driftsinntekter", "egenkapital", "eiendeler"):
        assert figure_key not in text, figure_key


def test_equinor_latest_period_with_group_accounts() -> None:
    s = summarise(_body("equinor"), _body("equinor_pdf_years"), EQUINOR)
    assert s["status"] == "filed"
    assert (s["period_start"], s["period_end"], s["year"]) == ("2025-01-01", "2025-12-31", 2025)
    assert s["journal_number"] == "2026635024"
    assert s["group_accounts"] is True and s["group_parent"] is True
    # The parent company reports under simplified IFRS (its group accounts
    # are full IFRS); the summary reads the company record.
    assert s["currency"] == "USD" and s["rules"] == "forenkletAnvendelseIFRS"
    assert s["audit_opted_out"] is False and s["liquidation_accounts"] is False
    assert s["pdf_url"] == f"{ACCOUNTS_BASE}/aarsregnskap/kopi/{EQUINOR}/2025"
    _no_figures(s)


def test_small_company_audit_opt_out_and_no_group() -> None:
    s = summarise(_body("bakery"), _body("bakery_pdf_years"), BAKERY)
    assert s["year"] == 2025
    assert s["group_accounts"] is False
    assert s["audit_opted_out"] is True and s["small_company"] is True
    assert s["rules"] == "regnskapslovenAlminneligRegler"
    _no_figures(s)


def test_no_pdf_link_when_the_register_lists_no_copy_for_the_year() -> None:
    s = summarise(_body("equinor"), ["2023", "2024"], EQUINOR)
    assert s["pdf_url"] is None
    assert summarise(_body("equinor"), None, EQUINOR)["pdf_url"] is None


def test_a_non_calendar_year_is_read_from_its_end_date() -> None:
    record = json.loads(json.dumps(_body("bakery")[-1]))
    record["regnskapsperiode"] = {"fraDato": "2024-07-01", "tilDato": "2025-06-30"}
    s = summarise([record], ["2025"], BAKERY)
    assert s["year"] == 2025
    assert brreg_accounts_clauses(s)[0] == "latest annual accounts filed for the year to 30 June 2025"


def test_unsupported_format_reads_the_500_message() -> None:
    assert unsupported_format(json.dumps(_body("dnb_bank"))) == "BANK"
    assert unsupported_format('{"message": "... ikke er stottet (LIV)"}') is None
    assert unsupported_format("oppstillingsplan som ikke er stottet (SKADE)") == "SKADE"
    assert unsupported_format("Internal Server Error") is None


# ---------------------------------------------------------------------------
# read_accounts — the four answers
# ---------------------------------------------------------------------------


async def test_filed_reads_the_records_and_the_pdf_list_and_caches(
    live, httpx_mock: HTTPXMock, tmp_path
) -> None:
    httpx_mock.add_response(url=f"{ACCOUNTS_BASE}/{EQUINOR}", json=_body("equinor"))
    httpx_mock.add_response(
        url=f"{ACCOUNTS_BASE}/aarsregnskap/kopi/{EQUINOR}/aar", json=_body("equinor_pdf_years")
    )
    cache = Cache()
    first = await read_accounts(EQUINOR, cache, live=True)
    assert first and first["status"] == "filed" and first["pdf_url"]
    again = await read_accounts(EQUINOR, cache, live=True)
    assert again == first
    assert len(httpx_mock.get_requests()) == 2


async def test_a_404_is_no_accounts_and_cached_as_an_absence(live, httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(url=f"{ACCOUNTS_BASE}/{NEW_AS}", status_code=404)
    cache = Cache()
    assert (await read_accounts(NEW_AS, cache, live=True))["status"] == "none"
    assert (await read_accounts(NEW_AS, cache, live=True))["status"] == "none"
    assert len(httpx_mock.get_requests()) == 1


async def test_bank_format_is_an_answer_not_an_outage(live, httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        url=f"{ACCOUNTS_BASE}/{DNB}", status_code=500, json=_body("dnb_bank")
    )
    cache = Cache()
    degradation.begin()
    try:
        s = await read_accounts(DNB, cache, live=True)
    finally:
        recorded = degradation.collect()
    assert s == {
        "status": "unsupported",
        "format": "BANK",
        "format_words": "bank",
        "source_url": f"{ACCOUNTS_BASE}/{DNB}",
    }
    assert recorded == []
    assert (await read_accounts(DNB, cache, live=True))["status"] == "unsupported"
    assert len(httpx_mock.get_requests()) == 1


async def test_any_other_failure_is_a_degradation_and_never_cached(
    live, httpx_mock: HTTPXMock
) -> None:
    httpx_mock.add_response(url=f"{ACCOUNTS_BASE}/{EQUINOR}", status_code=503)
    httpx_mock.add_response(url=f"{ACCOUNTS_BASE}/{EQUINOR}", status_code=503)
    cache = Cache()
    degradation.begin()
    try:
        s = await read_accounts(EQUINOR, cache, live=True)
    finally:
        recorded = degradation.collect()
    assert s["status"] == "unavailable"
    assert [(d.source_id, d.check, d.reason) for d in recorded] == [
        ("brreg", degradation.CHECK_SOURCE_READ, degradation.REASON_UPSTREAM_ERROR)
    ]
    assert (await read_accounts(EQUINOR, cache, live=True))["status"] == "unavailable"
    assert len(httpx_mock.get_requests()) == 2


async def test_a_failed_pdf_list_keeps_the_facts(live, httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(url=f"{ACCOUNTS_BASE}/{EQUINOR}", json=_body("equinor"))
    httpx_mock.add_response(
        url=f"{ACCOUNTS_BASE}/aarsregnskap/kopi/{EQUINOR}/aar", status_code=502
    )
    s = await read_accounts(EQUINOR, Cache(), live=True)
    assert s["status"] == "filed" and s["pdf_url"] is None


async def test_offline_with_nothing_cached_reads_nothing(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    try:
        assert await read_accounts(EQUINOR, Cache(), live=False) is None
    finally:
        get_settings.cache_clear()


# ---------------------------------------------------------------------------
# The adapter, the mapper and the hit
# ---------------------------------------------------------------------------


def _mock_register(httpx_mock: HTTPXMock, orgnr: str) -> None:
    base = "https://data.brreg.no/enhetsregisteret/api"
    httpx_mock.add_response(
        url=f"{base}/enheter/{orgnr}",
        json={"organisasjonsnummer": orgnr, "navn": "EQUINOR ASA",
              "organisasjonsform": {"kode": "ASA"}},
    )
    httpx_mock.add_response(url=f"{base}/enheter/{orgnr}/roller", json={"rollegrupper": []})


async def test_fetch_puts_the_summary_on_the_bundle(live, httpx_mock: HTTPXMock) -> None:
    _mock_register(httpx_mock, EQUINOR)
    httpx_mock.add_response(url=f"{ACCOUNTS_BASE}/{EQUINOR}", json=_body("equinor"))
    httpx_mock.add_response(
        url=f"{ACCOUNTS_BASE}/aarsregnskap/kopi/{EQUINOR}/aar", json=_body("equinor_pdf_years")
    )
    bundle = await BrregAdapter().fetch(EQUINOR, legal_name="EQUINOR ASA")
    assert bundle["accounts"]["status"] == "filed"
    assert bundle["accounts"]["journal_number"] == "2026635024"


async def test_nothing_reaches_bods(live, httpx_mock: HTTPXMock) -> None:
    """Decision 4: the BODS is the same with and without the accounts."""
    _mock_register(httpx_mock, EQUINOR)
    httpx_mock.add_response(url=f"{ACCOUNTS_BASE}/{EQUINOR}", json=_body("equinor"))
    httpx_mock.add_response(
        url=f"{ACCOUNTS_BASE}/aarsregnskap/kopi/{EQUINOR}/aar", json=_body("equinor_pdf_years")
    )
    bundle = await BrregAdapter().fetch(EQUINOR, legal_name="EQUINOR ASA")
    without = {**bundle, "accounts": None}

    def strip_dates(stmts):
        out = []
        for s in stmts:
            s = json.loads(json.dumps(s))
            s.pop("publicationDetails", None)
            s.get("source", {}).pop("retrievedAt", None)
            out.append(s)
        return out

    assert strip_dates(list(map_brreg(bundle))) == strip_dates(list(map_brreg(without)))
    assert "regnskap" not in json.dumps(list(map_brreg(bundle)))


def test_hit_carries_the_summary_and_the_sentence() -> None:
    accounts = summarise(_body("equinor"), _body("equinor_pdf_years"), EQUINOR)
    r = {"orgnr": EQUINOR, "entity": {"navn": "EQUINOR ASA"}, "roles": [], "accounts": accounts}
    hit = _bh_brreg(r, EQUINOR, _LookupCtx(lei="OW6OFBNCKXC4US5C7523"))
    assert hit.raw["accounts"]["pdf_url"].endswith(f"/kopi/{EQUINOR}/2025")
    assert hit.raw["navn"] == "EQUINOR ASA"
    assert hit.finding == "Latest annual accounts filed for 2025, with group accounts."
    assert hit.summary == f"NO-ORGNR {EQUINOR}"
    # The register's entity record is not mutated by the hit.
    assert "accounts" not in r["entity"]


# ---------------------------------------------------------------------------
# The sentence
# ---------------------------------------------------------------------------


def _bundle(accounts: dict[str, Any] | None, **entity: Any) -> dict[str, Any]:
    return {"orgnr": "1", "entity": {"navn": "X", **entity}, "roles": [], "accounts": accounts}


@pytest.mark.parametrize(
    "accounts, entity, expected",
    [
        (summarise(_body("equinor"), [], EQUINOR), {},
         "Latest annual accounts filed for 2025, with group accounts."),
        (summarise(_body("bakery"), [], BAKERY), {},
         "Latest annual accounts filed for 2025; audit opted out."),
        ({"status": "none"}, {}, "No annual accounts in the last three filed years."),
        ({"status": "unsupported", "format": "BANK", "format_words": "bank"}, {},
         "Annual accounts in a format the open API does not serve (bank)."),
        ({"status": "unsupported", "format": "LIV", "format_words": "life insurance"}, {},
         "Annual accounts in a format the open API does not serve (life insurance)."),
        ({"status": "unavailable"}, {}, "Annual accounts not read."),
        ({"status": "none"}, {"konkurs": True},
         "Declared bankrupt (konkurs); no annual accounts in the last three filed years."),
        ({"status": "filed", "period_start": "2024-01-01", "period_end": "2024-12-31",
          "liquidation_accounts": True, "not_audited": True}, {"underAvvikling": True},
         "In liquidation (under avvikling); latest annual accounts filed for 2024; "
         "liquidation accounts; accounts not audited."),
        (None, {"slettedato": "2021-03-04"}, "Deleted from the register on 4 March 2021."),
        (None, {}, None),
    ],
)
def test_finding_brreg(accounts, entity, expected) -> None:
    assert finding_brreg(_bundle(accounts, **entity)) == expected


def test_finding_brreg_stub_says_nothing() -> None:
    assert finding_brreg({"orgnr": "1", "is_stub": True, "accounts": {"status": "none"}}) is None


@pytest.mark.parametrize(
    "accounts",
    [
        summarise(_body("equinor"), [], EQUINOR),
        summarise(_body("bakery"), [], BAKERY),
        {"status": "none"},
        {"status": "unsupported", "format": "SKADE", "format_words": "non-life insurance"},
        {"status": "unavailable"},
    ],
)
def test_every_sentence_fits_and_judges_nothing(accounts) -> None:
    for entity in ({}, {"konkurs": True}, {"underTvangsavviklingEllerTvangsopplosning": True}):
        sentence = finding_brreg(_bundle(accounts, **entity))
        assert sentence and len(sentence) <= MAX_FINDING_CHARS
        lowered = sentence.lower()
        assert "overdue" not in lowered and "late" not in lowered.split()
        for word in BANNED_VOCABULARY:
            assert word not in lowered.split(), (word, sentence)


def test_frontend_rules_words_match_the_backend() -> None:
    """``lib/brregAccounts.ts`` keeps a copy of ``RULES_WORDS``; parse it."""
    import re

    from opencheck.sources.brreg_accounts import RULES_WORDS

    ts = (
        Path(__file__).resolve().parents[2] / "frontend" / "src" / "lib" / "brregAccounts.ts"
    ).read_text(encoding="utf-8")
    block = re.search(r"RULES_WORDS: Record<string, string> = \{(.*?)\};", ts, re.S)
    assert block, "RULES_WORDS not found in brregAccounts.ts"
    parsed = dict(re.findall(r'(\w+):\s*"([^"]*)"', block.group(1)))
    assert parsed == RULES_WORDS
