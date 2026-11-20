"""Connecticut on the History tab: the Filing History read and the emitter.

``tests/data/ct_sots_filings_live.json`` holds **real** Filing History
(``ah3s-bes7``) answers captured from data.ct.gov on 2026-11-20 with the exact
``$select`` the adapter sends, for the businesses in ``ct_sots_live.json``
plus WEB ACQUISITION CO. (``1049572``), a domestic corporation the register
records as Merged.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from opencheck.sources import REGISTRY
from opencheck.sources import ct_sots as ct
from opencheck.sources.ct_sots import CtSotsAdapter
from opencheck.sources.schemas import SourceSchemaError, validate_raw
from opencheck.sources.schemas.ct_sots import CtSotsTimelineBundle
from opencheck.timeline import ChangeType, Tier
from opencheck.timeline.ct_sots import (
    CONSOLIDATED_WORDS,
    MERGED_WORDS,
    annual_report_summary,
    ct_sots_change_events,
    text,
)

_HERE = Path(__file__).parent
_LIVE = json.loads((_HERE / "data" / "ct_sots_live.json").read_text(encoding="utf-8"))
_FILINGS = json.loads((_HERE / "data" / "ct_sots_filings_live.json").read_text(encoding="utf-8"))
_R: dict[str, Any] = _LIVE["responses"]
_IDS: dict[str, str] = _LIVE["account_numbers"]


def _bundle(label: str, **extra: Any) -> dict[str, Any]:
    """The shape ``fetch_timeline_data`` returns."""
    if label == "merged":
        master, business_id, principals, changes = _FILINGS["merged_master"], "1049572", [], []
    else:
        master = _R[f"{label}/master"][0]
        business_id = _IDS[label]
        principals = _R[f"{label}/principals"]
        changes = _R[f"{label}/name_changes"]
    bundle = {
        "source_id": "ct_sots",
        "business_id": business_id,
        "legal_name": "",
        "master": master,
        "principals": principals,
        "name_changes": changes,
        "truncated": False,
        "is_stub": False,
        "filings": _FILINGS["responses"][label],
        "filings_truncated": False,
    }
    bundle.update(extra)
    return bundle


def _filing(category: str, filing_type: str, day: str, *, number: str = "X1", **extra: Any) -> dict:
    row = {
        "account": "001t000000WmSc6AAF",
        "name": number,
        "filing_date": f"{day}T00:00:00.000",
        "type": category,
        "filing_type": filing_type,
    }
    row.update(extra)
    return row


def _notable(events):
    return [e for e in events if e.tier is Tier.IDENTITY_STATUS]


# ----------------------------------------------------------------------
# The emitter, on real filings
# ----------------------------------------------------------------------


class TestEmitter:
    def test_every_event_is_connecticuts_and_keyed_on_the_business_id(self):
        events = ct_sots_change_events(_bundle("eurpac"))
        assert events
        assert {e.source_id for e in events} == {"ct_sots"}
        assert {e.subject_id for e in events} == {"0016116"}

    def test_annual_reports_are_one_grouped_row(self):
        bundle = _bundle("eurpac")
        reports = [f for f in bundle["filings"] if f.get("type") == "Annual Report"]
        assert len(reports) > 10
        events = ct_sots_change_events(bundle)
        grouped = [e for e in events if e.raw_field == "Annual Report"]
        assert len(grouped) == 1
        row = grouped[0]
        assert row.tier is Tier.ADMIN_NOISE and row.change_type is None
        assert row.raw_change_type.startswith(f"{len(reports)} annual reports filed, for ")
        assert row.raw_payload_ref is None
        days = [ct.iso_date(r.get("effective_date")) or ct.iso_date(r.get("filing_date")) for r in reports]
        latest = max(d for d in days if d)
        assert row.event_date == latest
        # Every other filing is a row of its own.
        assert len(events) == len(bundle["filings"]) - len(reports) + 1

    def test_a_dissolution_is_a_status_change(self):
        events = ct_sots_change_events(_bundle("dissolved"))
        [status] = _notable(events)
        assert status.change_type is ChangeType.STATUS_CHANGED
        assert status.value_new == "Dissolved"
        assert status.raw_change_type == "Certificate of Dissolution"
        assert status.raw_field == "Dissolution"

    def test_forfeiture_revocation_and_reinstatement_are_status_changes_and_the_warning_is_not(self):
        events = ct_sots_change_events(_bundle("forfeited"))
        words = [e.value_new for e in _notable(events)]
        assert "Dissolved by the Secretary of the State" in words
        assert "Reinstated" in words
        warnings = [e for e in events if e.raw_change_type == "Notice of Intent to Dissolve/Revoke"]
        assert warnings and all(e.tier is Tier.ADMIN_NOISE for e in warnings)

    def test_a_rename_is_read_from_the_name_change_history_and_its_filing_folded(self):
        events = ct_sots_change_events(_bundle("glenstone"))
        renames = [e for e in events if e.change_type is ChangeType.LEGAL_NAME_CHANGE]
        assert {e.event_date for e in renames} == {"2019-03-06", "2024-09-26"}
        assert all(e.value_new and e.tier is Tier.IDENTITY_STATUS for e in renames)
        # The Amend Name of 6 March 2019 and both Name Change Amendments of
        # 26 Sept 2024 are the renames themselves; the agent change and the
        # interim notice filed that day are not.
        types = [e.raw_change_type for e in events if e.tier is Tier.ADMIN_NOISE]
        assert "Amend Name" not in types
        assert "Certificate of Amendment" not in types
        assert types.count("Agent Change") == 3
        assert "Interim Notice" in types

    def test_a_plain_amendment_on_the_rename_day_is_the_rename(self):
        """Eder-Goodman, LLC's rename was filed as Amendment / Certificate of
        Amendment, effective 1 April 2026 — the day the rename is recorded."""
        events = ct_sots_change_events(_bundle("eder"))
        renames = [e for e in events if e.change_type is ChangeType.LEGAL_NAME_CHANGE]
        assert renames and {e.event_date for e in renames} == {"2026-04-01"}
        assert not [e for e in events if e.raw_change_type == "Certificate of Amendment"]

    def test_an_amendment_on_another_day_stays(self):
        bundle = _bundle(
            "eder",
            filings=[_filing("Amendment", "Amend Name", "2020-01-02")],
        )
        events = ct_sots_change_events(bundle)
        assert [e.raw_change_type for e in events if e.tier is Tier.ADMIN_NOISE] == ["Amend Name"]

    def test_the_merger_that_ended_a_merged_business_is_a_succession(self):
        events = ct_sots_change_events(_bundle("merged"))
        [succession] = [e for e in events if e.change_type is ChangeType.SUCCESSION]
        assert succession.event_date == "2011-10-31"
        assert succession.value_new == MERGED_WORDS
        assert succession.raw_payload_ref == "0004465199"

    def test_a_merger_certificate_on_a_live_business_changes_nothing_about_it(self):
        """EURPAC is Active and filed a Certificate of Merger: it survived."""
        events = ct_sots_change_events(_bundle("eurpac"))
        mergers = [e for e in events if e.raw_change_type == "Certificate of Merger"]
        assert mergers and all(e.tier is Tier.ADMIN_NOISE for e in mergers)
        assert not [e for e in events if e.change_type is ChangeType.SUCCESSION]

    def test_a_consolidated_business_is_worded_as_consolidated(self):
        master = dict(_FILINGS["merged_master"], status="Consolidation")
        bundle = _bundle(
            "merged",
            master=master,
            filings=[
                _filing("Merger", "Certificate of Merger", "2015-01-01", number="A"),
                _filing("Certificate of Consolidation", "Certificate of Consolidation", "2016-01-01", number="B"),
            ],
        )
        events = ct_sots_change_events(bundle)
        [succession] = [e for e in events if e.change_type is ChangeType.SUCCESSION]
        assert succession.raw_payload_ref == "B" and succession.value_new == CONSOLIDATED_WORDS

    def test_a_foreign_business_ends_only_its_connecticut_registration(self):
        bundle = _bundle(
            "ubs",
            filings=[
                _filing("Withdrawal", "Certificate of Withdrawal", "2020-01-02"),
                _filing("Administrative Dissolution", "Certificate of Dissolution/Revocation", "2021-01-02"),
            ],
        )
        words = [e.value_new for e in _notable(ct_sots_change_events(bundle))]
        assert words == [
            "Withdrew its registration in Connecticut",
            "Registration in Connecticut revoked by the Secretary of the State",
        ]

    def test_the_effective_date_wins_over_the_filing_date(self):
        bundle = _bundle(
            "dissolved",
            filings=[
                _filing("Dissolution", "Certificate of Dissolution", "2026-11-01", effective_date="2026-11-05T00:00:00.000")
            ],
        )
        [status] = _notable(ct_sots_change_events(bundle))
        assert status.event_date == "2026-11-05"

    def test_a_blank_filing_type_falls_back_on_the_category(self):
        bundle = _bundle("dissolved", filings=[_filing("Withdrawal", "None", "2020-01-02")])
        [status] = _notable(ct_sots_change_events(bundle))
        assert status.value_new == "Withdrawn"
        assert status.raw_change_type == "Withdrawal"

    def test_conversion_and_domestication(self):
        bundle = _bundle(
            "dissolved",
            filings=[
                _filing("Conversion", "Certificate of Conversion", "2020-01-02"),
                _filing("Domestication", "Certificate of Domestication", "2021-01-02"),
            ],
        )
        kinds = [e.change_type for e in _notable(ct_sots_change_events(bundle))]
        assert kinds == [ChangeType.LEGAL_FORM_CHANGE, ChangeType.JURISDICTION_CHANGE]

    def test_the_broken_dash_and_the_literal_none_are_cleaned(self):
        assert text("Mass Agent Change � Address") == "Mass Agent Change - Address"
        assert text("None") == ""
        events = ct_sots_change_events(_bundle("ubs"))
        fields = {e.raw_field for e in events}
        assert "Mass Agent Change - Address" in fields
        assert not any("�" in (f or "") for f in fields)

    def test_no_record_no_events(self):
        assert ct_sots_change_events(None) == []
        assert ct_sots_change_events({"is_stub": True}) == []
        assert ct_sots_change_events({"master": None}) == []

    def test_a_business_with_no_filings_still_shows_its_renames(self):
        events = ct_sots_change_events(_bundle("glenstone", filings=[], filings_unavailable="timeout"))
        assert events and all(e.change_type is ChangeType.LEGAL_NAME_CHANGE for e in events)


class TestAnnualReportSummary:
    def test_one_report(self):
        assert annual_report_summary([{"report_year": "2024"}]) == "1 annual report filed, for 2024"

    def test_report_years_then_filing_years(self):
        rows = [
            {"report_year": "2003"},
            {"filing_date": "2026-02-01T00:00:00.000"},
            {"report_year": "None", "filing_date": "2010-05-01T00:00:00.000"},
        ]
        assert annual_report_summary(rows) == "3 annual reports filed, for 2003 to 2026"

    def test_no_year_at_all(self):
        assert annual_report_summary([{}, {}]) == "2 annual reports filed"


# ----------------------------------------------------------------------
# The adapter's Filing History read
# ----------------------------------------------------------------------


def _response(payload: Any, status: int = 200) -> MagicMock:
    response = MagicMock()
    response.status_code = status
    response.is_success = 200 <= status < 300
    response.json.return_value = payload
    return response


def _run_timeline(
    label: str, *, filings_status: int = 200, cached_filings: list | None = None
) -> tuple[dict[str, Any] | None, AsyncMock, MagicMock]:
    adapter = CtSotsAdapter()
    settings = MagicMock(allow_live=True, socrata_app_token=None)
    parts = {
        ct.DATASET_MASTER: f"{label}/master",
        ct.DATASET_PRINCIPALS: f"{label}/principals",
        ct.DATASET_NAME_CHANGES: f"{label}/name_changes",
    }
    seen: list[dict] = []

    async def get(url: str, params=None, headers=None):  # noqa: ANN001
        dataset = url.rsplit("/", 1)[-1].removesuffix(".json")
        seen.append({"dataset": dataset, **(params or {})})
        if dataset == ct.DATASET_FILINGS:
            if filings_status != 200:
                return _response({}, filings_status)
            return _response(_FILINGS["responses"][label])
        return _response(_R.get(parts[dataset], []))

    def get_payload(key, max_age_days=None):  # noqa: ANN001
        if key.startswith(ct._FILINGS_CACHE_NS) and cached_filings is not None:
            return (cached_filings, None)
        return None

    getter = AsyncMock(side_effect=get)
    put = MagicMock()
    with patch("opencheck.sources.ct_sots.get_settings", return_value=settings), \
         patch.object(adapter._cache, "get_payload", side_effect=get_payload), \
         patch.object(adapter._cache, "put", put), \
         patch("opencheck.sources.ct_sots.build_client") as client:
        client.return_value.__aenter__.return_value.get = getter
        result = asyncio.run(adapter.fetch_timeline_data(_IDS[label]))
    getter.seen = seen  # type: ignore[attr-defined]
    return result, getter, put


class TestFetchTimelineData:
    def test_the_filings_are_read_on_the_record_id_with_an_allowlisted_select(self):
        result, getter, put = _run_timeline("eurpac")
        assert result is not None
        assert len(result["filings"]) == len(_FILINGS["responses"]["eurpac"])
        [query] = [q for q in getter.seen if q["dataset"] == ct.DATASET_FILINGS]
        assert query["$where"] == "account='001t000000WmSc6AAF'"
        select = query["$select"].split(",")
        assert not [c for c in select if c.startswith(("organizer", "name_of_incorporator"))]
        keys = [call.args[0] for call in put.call_args_list]
        assert "ct_sots/filings-v1/001t000000WmSc6AAF" in keys

    def test_a_failed_filing_read_is_said_and_never_cached(self):
        result, _getter, put = _run_timeline("eurpac", filings_status=503)
        assert result is not None
        assert result["filings"] == [] and result["filings_unavailable"] == "upstream_error"
        assert not [c for c in put.call_args_list if c.args[0].startswith(ct._FILINGS_CACHE_NS)]

    def test_a_refusal_reads_as_rate_limited(self):
        result, _getter, _put = _run_timeline("eurpac", filings_status=429)
        assert result is not None and result["filings_unavailable"] == "rate_limited"

    def test_cached_filings_are_not_asked_for_again(self):
        result, getter, _put = _run_timeline("eurpac", cached_filings=[{"account": "x", "name": "1"}])
        assert result is not None and result["filings"] == [{"account": "x", "name": "1"}]
        assert not [q for q in getter.seen if q["dataset"] == ct.DATASET_FILINGS]

    def test_no_record_no_timeline(self):
        result, getter, _put = _run_timeline("missing")
        assert result is None
        assert not [q for q in getter.seen if q["dataset"] == ct.DATASET_FILINGS]

    def test_the_schema_refuses_an_organizer(self):
        bundle = _bundle("eurpac")
        bundle["filings"] = [dict(bundle["filings"][0], organizer_name="A Person")]
        with pytest.raises(SourceSchemaError):
            validate_raw("ct_sots", CtSotsTimelineBundle, bundle)
        bundle["filings"] = [dict(bundle["filings"][0], name_of_incorporator="A Person")]
        bundle["filings"][0].pop("organizer_name")
        with pytest.raises(SourceSchemaError):
            validate_raw("ct_sots", CtSotsTimelineBundle, bundle)


# ----------------------------------------------------------------------
# /history
# ----------------------------------------------------------------------

_LEI = "5493000CTSOTS0000000"


def _gleif_record(name: str, registered_as: str, jurisdiction: str = "US-CT") -> dict[str, Any]:
    return {
        "data": {
            "attributes": {
                "entity": {
                    "legalName": {"name": name},
                    "registeredAs": registered_as,
                    "registeredAt": {"id": ct.CT_SOTS_RA_CODE},
                    "jurisdiction": jurisdiction,
                }
            }
        }
    }


async def _history_for(monkeypatch, record: dict[str, Any], bundle: dict[str, Any] | None):
    import respx
    from httpx import Response

    from opencheck.config import get_settings
    from opencheck.routers.history import history

    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.delenv("COMPANIES_HOUSE_HISTORY_API_KEY", raising=False)
    monkeypatch.delenv("COMPANIES_HOUSE_API_KEY", raising=False)
    get_settings.cache_clear()
    fetch = AsyncMock(return_value=bundle)
    monkeypatch.setattr(REGISTRY["ct_sots"], "fetch_timeline_data", fetch)
    try:
        with respx.mock:
            respx.get(f"https://api.gleif.org/api/v1/lei-records/{_LEI}").mock(
                return_value=Response(200, json=record)
            )
            respx.get(
                url__regex=rf"https://api\.gleif\.org/api/v1/lei-records/{_LEI}/field-modifications"
            ).mock(return_value=Response(200, json={"data": [], "meta": {"pagination": {"lastPage": 1}}}))
            resp = await history(request=None, response=None, lei=_LEI, include_noise=True)
    finally:
        get_settings.cache_clear()
    return resp, fetch


@pytest.mark.asyncio
async def test_history_reads_connecticut_and_names_its_number(monkeypatch):
    resp, fetch = await _history_for(
        monkeypatch, _gleif_record("Glenstone Builders LLC", "1094606"), _bundle("glenstone")
    )
    assert fetch.await_args.args[0] == "1094606"
    assert fetch.await_args.kwargs["legal_name"] == "Glenstone Builders LLC"
    assert resp.registry_numbers == {"ct_sots": "1094606"}
    assert "ct_sots" in resp.sources
    notable = [e for e in resp.notable if "ct_sots" in e.sources]
    assert {e.change_type for e in notable} == {"LEGAL_NAME_CHANGE"}
    grouped = [e for e in resp.events if e.source_id == "ct_sots" and e.raw_field == "Annual Report"]
    assert len(grouped) == 1


@pytest.mark.asyncio
async def test_history_keys_on_the_ra_code_and_pads_the_number(monkeypatch):
    """UBS AG is Swiss; its Connecticut history is still Connecticut's."""
    resp, fetch = await _history_for(
        monkeypatch, _gleif_record("UBS AG", "597268", "CH"), _bundle("ubs")
    )
    assert fetch.await_args.args[0] == "0597268"
    assert resp.registry_numbers == {"ct_sots": "0597268"}


@pytest.mark.asyncio
async def test_a_filing_read_that_failed_is_said(monkeypatch):
    resp, _ = await _history_for(
        monkeypatch,
        _gleif_record("Glenstone Builders LLC", "1094606"),
        _bundle("glenstone", filings=[], filings_unavailable="timeout"),
    )
    assert resp.registers_unavailable == {"ct_sots": "timeout"}
    assert "ct_sots" in resp.sources  # the renames still show


@pytest.mark.asyncio
async def test_a_wrong_number_contributes_no_history(monkeypatch):
    resp, _ = await _history_for(
        monkeypatch, _gleif_record("38 TOWN LINE ROAD LIMITED PARTNERSHIP", "0637799"), None
    )
    assert resp.registry_numbers == {"ct_sots": "0637799"}
    assert "ct_sots" not in resp.sources
    assert not resp.registers_unavailable
