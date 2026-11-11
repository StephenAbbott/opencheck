"""Sirene history periods on the History tab (the Sirene history ticket, 11 Nov 2026).

The fixture is the four units' ``periodesUniteLegale`` cut from INSEE's
StockUniteLegaleHistorique of 1 Nov 2026 — the same periods API Sirene's
``/siren/{siren}`` returns — for TotalEnergies, Engie, Orange and
Transparency International France. Each case below is a trap measured on the
whole file: the first period, INSEE's processing dates, NAF recodes, blank →
value, flags that fire with no change, and renames of case only.
"""

from __future__ import annotations

import asyncio
import json
import unittest.mock as mock
from pathlib import Path
from typing import Any

import httpx
import pytest

from opencheck.config import get_settings
from opencheck.sources import REGISTRY, sirene
from opencheck.sources.schemas import validate_raw
from opencheck.sources.schemas.sirene import SireneBundle
from opencheck.timeline import service
from opencheck.timeline.assemble import assemble_timeline
from opencheck.timeline.model import ChangeEvent, ChangeType, DateBasis, DateConfidence, RecordType, Tier
from opencheck.timeline.sirene import batch_dated, sirene_change_events

_FIXTURE = Path(__file__).parent / "fixtures" / "sirene" / "history_periods_2026-11-01.json"
UNITS: dict[str, list[dict[str, Any]]] = json.loads(_FIXTURE.read_text("utf-8"))["units"]

TOTAL, ENGIE, ORANGE, TI = "542051180", "542107651", "380129866", "425138393"


def _bundle(siren: str, periods: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    raw = periods if periods is not None else UNITS[siren]
    return {
        "source_id": "sirene",
        "hit_id": siren,
        "siren": siren,
        "unit": {"siren": siren},
        "periods": sirene.history_periods({"periodesUniteLegale": raw}),
        "rna_status": "none",
        "link": sirene.annuaire_url(siren),
        "is_stub": False,
    }


def _of(events: list[ChangeEvent], field: str) -> list[ChangeEvent]:
    return [e for e in events if e.raw_field == field]


# ---------------------------------------------------------------------------
# The adapter keeps the periods
# ---------------------------------------------------------------------------


def test_history_periods_are_oldest_first_and_allowlisted() -> None:
    raw = [dict(p, nomUniteLegale="DUPONT", changementNomUniteLegale=True) for p in UNITS[TOTAL]]
    periods = sirene.history_periods({"periodesUniteLegale": raw})
    assert [p.get("dateDebut") for p in periods] == sorted(p["dateDebut"] for p in UNITS[TOTAL])
    assert all(set(p) <= set(sirene.PERIOD_FIELDS) for p in periods)
    assert not any("nomUniteLegale" in p or k.startswith("changement") for p in periods for k in p)


def test_the_schema_refuses_a_persons_name_in_a_period() -> None:
    bundle = _bundle(TI)
    bundle["periods"][0]["nomUniteLegale"] = "DUPONT"
    with pytest.raises(Exception, match="name fields"):
        SireneBundle.model_validate(bundle)
    del bundle["periods"][0]["nomUniteLegale"]
    validate_raw("sirene", SireneBundle, bundle)


def test_fetch_keeps_the_periods_and_caches_under_v2(tmp_path: Path) -> None:
    import respx

    from opencheck import degradation, provenance
    from opencheck.cache import Cache

    unit = {"siren": TI, "statutDiffusionUniteLegale": "O", "periodesUniteLegale": UNITS[TI]}
    adapter = sirene.SireneAdapter()
    adapter._cache = Cache(root=tmp_path)
    settings = get_settings().model_copy(update={"allow_live": True, "insee_sirene_api_key": "k"})
    with mock.patch("opencheck.sources.sirene.get_settings", return_value=settings), \
            mock.patch.object(sirene.rna_index, "index_available", return_value=False), \
            respx.mock(assert_all_called=False) as router:
        router.get(f"{sirene.API_BASE}/siren/{TI}").mock(
            return_value=httpx.Response(200, json={"uniteLegale": unit})
        )
        router.get(url__startswith=f"{sirene.API_BASE}/siret/").mock(return_value=httpx.Response(404))
        with degradation.recording(), provenance.recording():
            bundle = asyncio.run(adapter.fetch(TI))
    assert len(bundle["periods"]) == 6
    assert bundle["periods"][0]["dateDebut"] == "1999-02-01"
    held = adapter._cache.get_payload(f"sirene/unit-v2/{TI}", max_age_days=1)
    assert held is not None and len(held[0]["periods"]) == 6


# ---------------------------------------------------------------------------
# The emitter
# ---------------------------------------------------------------------------


def test_totalenergies_renames_and_form_change() -> None:
    events = sirene_change_events(_bundle(TOTAL))
    names = [(e.event_date, e.value_old, e.value_new) for e in _of(events, "denominationUniteLegale")]
    assert names == [
        ("2000-01-01", "TOTAL FINA SA", "TOTAL FINA ELF S.A."),
        ("2003-01-01", "TOTAL FINA ELF S.A.", "TOTAL SA"),
        ("2020-05-29", "TOTAL SA", "TOTAL SE"),
        ("2021-05-28", "TOTAL SE", "TOTALENERGIES SE"),
    ]
    form = _of(events, "categorieJuridiqueUniteLegale")
    assert len(form) == 1
    assert form[0].change_type is ChangeType.LEGAL_FORM_CHANGE
    assert form[0].tier is Tier.IDENTITY_STATUS
    assert form[0].event_date == "2020-05-29"
    assert form[0].value_old.endswith("(5599)") and form[0].value_new.endswith("(5800)")
    # Every activity change on TotalEnergies is a NAF version switch or a
    # first coding: none survives.
    assert _of(events, "main activity") == []


def test_processing_dates_are_recorded_and_low_confidence() -> None:
    by_date = {e.event_date: e for e in _of(sirene_change_events(_bundle(TOTAL)), "denominationUniteLegale")}
    assert by_date["2003-01-01"].date_basis is DateBasis.RECORDED
    assert by_date["2003-01-01"].date_confidence is DateConfidence.LOW
    assert by_date["2021-05-28"].date_basis is DateBasis.EFFECTIVE
    assert by_date["2021-05-28"].date_confidence is DateConfidence.HIGH
    assert batch_dated("2004-12-25") and batch_dated("2008-01-01")
    assert not batch_dated("2009-01-01") and not batch_dated("2015-08-04") and not batch_dated(None)


def test_engie_name_form_head_office_and_trading_name() -> None:
    events = sirene_change_events(_bundle(ENGIE))
    assert [(e.event_date, e.value_new) for e in _of(events, "denominationUniteLegale")] == [
        ("2008-07-22", "GDF SUEZ"),
        ("2015-08-04", "ENGIE"),
    ]
    # EPIC → SA in 2004, dated by INSEE's 25 December processing.
    (form,) = _of(events, "categorieJuridiqueUniteLegale")
    assert form.event_date == "2004-12-25" and form.date_basis is DateBasis.RECORDED
    # Gaz de France's first state is blank; blank → A is not a change.
    assert _of(events, "etatAdministratifUniteLegale") == []
    moves = _of(events, "head office")
    assert [m.value_new for m in moves] == [
        f"SIRET {ENGIE}12867", f"SIRET {ENGIE}13030", f"SIRET {ENGIE}14459",
    ]
    assert all(m.tier is Tier.ADMIN_NOISE and m.change_type is None for m in moves)
    (usual,) = _of(events, "trading name")
    assert (usual.event_date, usual.value_old, usual.value_new) == ("2015-04-24", "GDF SUEZ", "ENGIE")


def test_orange_mission_flag_is_read_only_between_real_values() -> None:
    events = sirene_change_events(_bundle(ORANGE))
    assert [(e.event_date, e.value_new) for e in _of(events, "denominationUniteLegale")] == [
        ("2013-07-01", "ORANGE")
    ]
    # INSEE flags société à mission on 18 May 2021 going from blank to O
    # (blank → value: first recorded), so no row.
    assert _of(events, "société à mission") == []


def test_ti_france_first_period_is_never_an_event() -> None:
    events = sirene_change_events(_bundle(TI))
    assert all(e.event_date != "1999-02-01" for e in events)
    assert [e.value_new for e in _of(events, "denominationUniteLegale")] == [
        "TRANSPARENCE INTERNATIONAL FRANCE",
        "TRANSPARENCY INTERNATIONAL FRANCE",
    ]
    # 91.3E → 94.99Z is NAF 1993/Rev1 → Rev2: a recode, never a change.
    assert _of(events, "main activity") == []
    assert all(e.source_id == "sirene" and e.record_type is RecordType.ENTITY for e in events)


def _period(day: str, **values: str) -> dict[str, Any]:
    return {"dateDebut": day, **values}


def test_synthetic_traps() -> None:
    periods = [
        _period("1900-01-01", denominationUniteLegale="SOLAIRE DIRECT", etatAdministratifUniteLegale="A",
                activitePrincipaleUniteLegale="35.11Z", nomenclatureActivitePrincipaleUniteLegale="NAFRev2",
                economieSocialeSolidaireUniteLegale="N"),
        _period("2012-03-01", denominationUniteLegale="SOLAIREDIRECT", etatAdministratifUniteLegale="A",
                activitePrincipaleUniteLegale="71.12B", nomenclatureActivitePrincipaleUniteLegale="NAFRev2",
                economieSocialeSolidaireUniteLegale="O"),
        _period("2019-06-30", denominationUniteLegale="SOLAIREDIRECT", etatAdministratifUniteLegale="C",
                activitePrincipaleUniteLegale="71.12B", nomenclatureActivitePrincipaleUniteLegale="NAFRev2",
                economieSocialeSolidaireUniteLegale="O"),
        _period("2021-01-04", denominationUniteLegale="SOLAIREDIRECT", etatAdministratifUniteLegale="A",
                activitePrincipaleUniteLegale="71.12B", nomenclatureActivitePrincipaleUniteLegale="NAFRev2",
                economieSocialeSolidaireUniteLegale="O"),
    ]
    events = sirene_change_events(_bundle("123456789", periods))
    # A rename of spacing only is not one.
    assert _of(events, "denominationUniteLegale") == []
    states = _of(events, "etatAdministratifUniteLegale")
    assert [(e.event_date, e.value_old, e.value_new) for e in states] == [
        ("2019-06-30", "active", "ceased"),
        ("2021-01-04", "ceased", "active again"),
    ]
    assert all(e.change_type is ChangeType.STATUS_CHANGED and e.is_notable for e in states)
    (activity,) = _of(events, "main activity")
    assert activity.value_new == "71.12B (NAFRev2)" and not activity.is_notable
    (ess,) = _of(events, "social and solidarity economy")
    assert (ess.value_old, ess.value_new) == ("no", "yes")


def test_a_stub_or_note_card_emits_nothing() -> None:
    assert sirene_change_events({"is_stub": True, "periods": UNITS[TI]}) == []
    assert sirene_change_events({"not_found": True}) == []
    assert sirene_change_events({}) == []


# ---------------------------------------------------------------------------
# Clustering with GLEIF
# ---------------------------------------------------------------------------


def _gleif_name(day: str, old: str, new: str) -> ChangeEvent:
    return ChangeEvent(
        source_id="gleif", subject_id="529900S21EQ1BO4ESM68", record_type=RecordType.ENTITY,
        raw_change_type="lei:legalName", value_old=old, value_new=new,
        change_type=ChangeType.LEGAL_NAME_CHANGE, tier=Tier.IDENTITY_STATUS,
        event_date=day, date_basis=DateBasis.RECORDED, date_confidence=DateConfidence.MEDIUM,
    )


def test_two_renames_inside_the_window_stay_two_rows_and_gleif_joins_its_own() -> None:
    sirene_events = sirene_change_events(_bundle(TOTAL))
    timeline = assemble_timeline(
        lei="529900S21EQ1BO4ESM68",
        extra_events=sirene_events + [_gleif_name("2021-06-15", "TOTAL SE", "TotalEnergies SE")],
    )
    renames = [e for e in timeline.notable if e.change_type is ChangeType.LEGAL_NAME_CHANGE]
    recent = {e.date: e for e in renames if e.date and e.date >= "2020"}
    assert set(recent) == {"2020-05-29", "2021-05-28"}
    assert recent["2021-05-28"].sources == ["gleif", "sirene"]
    assert recent["2021-05-28"].date_basis is DateBasis.EFFECTIVE
    assert recent["2020-05-29"].sources == ["sirene"]


# ---------------------------------------------------------------------------
# The History service
# ---------------------------------------------------------------------------


def test_registration_numbers_read_the_siren_from_every_french_authority() -> None:
    for ra, raw in (("RA000189", "542 051 180"), ("RA000192", "542051180"), ("RA001129", "542051180")):
        payload = {"data": {"attributes": {"entity": {
            "registeredAs": raw, "registeredAt": {"id": ra}, "jurisdiction": "FR",
            "legalName": {"name": "TOTALENERGIES SE"},
        }}}}
        numbers = service._registration_numbers(payload)
        assert numbers[8] == TOTAL
        assert numbers[7] == "TOTALENERGIES SE"
    payload = {"data": {"attributes": {"entity": {"registeredAs": "542051180", "registeredAt": {"id": "RA000190"}}}}}
    assert service._registration_numbers(payload)[8] is None


@pytest.fixture()
def live(monkeypatch):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("INSEE_SIRENE_API_KEY", "test-key")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def test_the_history_service_reads_sirene(live, monkeypatch) -> None:
    async def registration(_client, _lei):
        return (None, None, None, None, None, None, None, "TOTALENERGIES SE", TOTAL)

    async def modifications(_client, _lei):
        return [], []

    asked: list[str] = []

    async def fetch(siren: str, *, legal_name: str = "") -> dict[str, Any]:
        asked.append(siren)
        return _bundle(siren)

    monkeypatch.setattr(service, "_gleif_registration", registration)
    monkeypatch.setattr(service, "_gleif_modifications", modifications)
    monkeypatch.setattr(REGISTRY["sirene"], "fetch", fetch)
    timeline = await service.fetch_timeline("529900S21EQ1BO4ESM68")
    assert asked == [TOTAL]
    assert timeline.registry_numbers["sirene"] == TOTAL
    assert any(e.source_id == "sirene" for e in timeline.notable for e in [e.primary])


async def test_without_a_key_sirene_is_not_asked(monkeypatch) -> None:
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.delenv("INSEE_SIRENE_API_KEY", raising=False)
    get_settings.cache_clear()
    get_settings().insee_sirene_api_key = None

    async def registration(_client, _lei):
        return (None, None, None, None, None, None, None, "X", TOTAL)

    async def modifications(_client, _lei):
        return [], []

    async def fetch(*_a, **_k):  # pragma: no cover - must not run
        raise AssertionError("sirene asked without a key")

    monkeypatch.setattr(service, "_gleif_registration", registration)
    monkeypatch.setattr(service, "_gleif_modifications", modifications)
    monkeypatch.setattr(REGISTRY["sirene"], "fetch", fetch)
    try:
        timeline = await service.fetch_timeline("529900S21EQ1BO4ESM68")
    finally:
        get_settings.cache_clear()
    assert timeline.registry_numbers["sirene"] == TOTAL
    assert not any(e.source_id == "sirene" for e in timeline.events)
