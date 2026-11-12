"""BODACC on History, the watchlist and the Sirene card (the BODACC ticket,
11 Nov 2026).

Stephen's decisions A–G are what these pin: a live per-SIREN read (A); no
person ever named (B); every family shown, the small ones in Tier 3 (C); the
watchlist's grading (D); the QuickCheck clause as context only (E); a sale
shown only from the seller's side, the buyer named only when it is a company
(F); personal judgments left out (G). The records below are shaped as the
opendatasoft API returns them (JSON strings inside the record), with the
personal fields a real notice carries kept in so the allowlist is tested
against them.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from opencheck import bodacc as b
from opencheck import watchlist as wl
from opencheck import watchlist_materiality as m
from opencheck.timeline.assemble import assemble_timeline
from opencheck.timeline.bodacc import bodacc_change_events
from opencheck.timeline.model import ChangeType, Tier
from tests.test_watchlist import EASY, _watch, client, env  # noqa: F401 — fixtures

SIREN = "401251566"
BUYER = "552100554"
PERSON_NAME = "Dupont Jean-Marie"


def _person(siren: str | None, *, kind: str = "pm", name: str = "ORPEA") -> dict[str, Any]:
    p: dict[str, Any] = {"typePersonne": kind, "adresseSiegeSocial": {"nomVoie": "12 Rue X", "ville": "Puteaux"}}
    if siren:
        p["numeroImmatriculation"] = {"numeroIdentification": f"{siren[:3]} {siren[3:6]} {siren[6:]}"}
    if kind == "pm":
        p["denomination"] = name
        p["administration"] = f"Président : {PERSON_NAME}"
    else:
        p["nom"], p["prenom"] = "Dupont", "Jean-Marie"
    return p


def _record(nid: str, family: str, day: str, *, people: list[dict] | None = None, **extra: Any) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "id": nid, "familleavis": family, "typeavis": extra.pop("typeavis", "annonce"),
        "publicationavis": nid[0], "dateparution": day,
        "tribunal": "Greffe du Tribunal des Activités Economiques de Nanterre",
        "listepersonnes": json.dumps({"personne": people if people is not None else _person(SIREN)}),
    }
    for key, value in extra.items():
        rec[key] = json.dumps(value) if isinstance(value, (dict, list)) else value
    return rec


OPENED = _record(
    "A202300100001", "collective", "2023-04-05",
    jugement={"nature": "Jugement d'ouverture d'une procédure de sauvegarde accélérée", "date": "2023-03-24",
              "complementJugement": f"Administrateur judiciaire : Me {PERSON_NAME}"},
)
PLAN = _record(
    "A202301482862", "collective", "2023-08-03",
    jugement={"nature": "Jugement arrêtant le plan de sauvegarde accélérée", "date": "2023-07-24",
              "complementJugement": f"Commissaire à l'exécution du plan Me {PERSON_NAME}"},
)
BAN = _record(
    "A202300100009", "collective", "2023-09-01",
    jugement={"nature": "Jugement prononçant une interdiction de gérer", "date": "2023-08-20"},
)
RENAMED = _record(
    "B202400200002", "modification", "2024-03-01", people=[_person(SIREN, name="emeis")],
    modificationsgenerales={"descriptif": "Modification de la dénomination."},
)
SOLD = _record(
    "A202500300003", "vente", "2025-05-02",
    people=[_person(BUYER, name="ROGANDCO")],
    listeprecedentproprietaire={"personne": _person(SIREN)},
    acte={"vente": {"categorieVente": "Achat d'un fonds"}, "descriptif": f"Oppositions chez Me {PERSON_NAME}"},
)
SOLD_TO_PERSON = _record(
    "A202500300004", "vente", "2025-05-03",
    people=[_person(None, kind="pp")],
    listeprecedentproprietaire={"personne": _person(SIREN)},
    acte={"vente": {"categorieVente": "Achat d'un fonds"}},
)
BOUGHT_BY_OTHER = _record(  # SIREN only mentioned: another company's purchase
    "A202500300005", "vente", "2025-05-04", people=[_person(BUYER, name="ROGANDCO")],
    listeprecedentproprietaire={"personne": _person("999999999", name="OTHER")},
)
ACCOUNTS = _record("C202601343812", "dpc", "2026-07-17", depot={"dateCloture": "2025-12-31"})
WRONG = _record(
    "B202600406", "modification", "2026-02-01",
    modificationsgenerales={"descriptif": "Modification du capital."},
)
WITHDRAWN = _record(
    "B202600400007", "modification", "2026-02-10", typeavis="annulation",
    parutionavisprecedent={"nomPublication": "BODACC B", "numeroParution": "20260040", "numeroAnnonce": "0006"},
)
ALL = [OPENED, PLAN, BAN, RENAMED, SOLD, SOLD_TO_PERSON, BOUGHT_BY_OTHER, ACCOUNTS, WRONG, WITHDRAWN]


def _payload(records: list[dict] | None = None) -> dict[str, Any]:
    recs = records if records is not None else ALL
    return b.notices_payload(recs, SIREN, total=len(recs))


# ---- the reader -----------------------------------------------------------


def test_every_kept_notice_carries_only_the_allowlisted_keys_and_no_person() -> None:
    payload = _payload()
    text = json.dumps(payload, ensure_ascii=False)
    assert "Dupont" not in text and "Jean-Marie" not in text
    for n in payload["notices"]:
        assert set(n) <= b.NOTICE_KEYS
        assert "adresse" not in json.dumps(n)


def test_a_personal_judgment_and_a_mere_mention_are_left_out() -> None:
    ids = {n["id"] for n in _payload()["notices"]}
    assert BAN["id"] not in ids  # decision G
    assert BOUGHT_BY_OTHER["id"] not in ids  # decision F


def test_judgments_are_dated_by_the_judgment_and_worded_in_english() -> None:
    by_id = {n["id"]: n for n in _payload()["notices"]}
    opened = by_id[OPENED["id"]]
    assert (opened["kind"], opened["label"], opened["effective"]) == (
        b.KIND_INSOLVENCY_OPENED, "Accelerated safeguard opened", "2023-03-24",
    )
    assert by_id[PLAN["id"]]["label"] == "Accelerated safeguard plan adopted"
    assert by_id[OPENED["id"]]["url"] == b.NOTICE_URL.format(id=OPENED["id"])


def test_a_sale_names_a_company_buyer_and_never_a_person() -> None:
    by_id = {n["id"]: n for n in _payload()["notices"]}
    assert by_id[SOLD["id"]]["role"] == b.ROLE_SELLER
    assert by_id[SOLD["id"]]["counterparty"] == {"siren": BUYER, "name": "ROGANDCO"}
    assert by_id[SOLD_TO_PERSON["id"]]["counterparty"] == {"siren": None, "name": None}


def test_an_annulation_withdraws_the_notice_it_names() -> None:
    by_id = {n["id"]: n for n in _payload()["notices"]}
    assert WRONG["id"] not in by_id
    assert by_id[WITHDRAWN["id"]]["kind"] == b.KIND_WITHDRAWN
    assert by_id[WITHDRAWN["id"]]["corrects"] == WRONG["id"]


def test_a_name_change_carries_the_new_company_name() -> None:
    by_id = {n["id"]: n for n in _payload()["notices"]}
    assert by_id[RENAMED["id"]]["kind"] == b.KIND_NAME_CHANGE
    assert by_id[RENAMED["id"]]["detail"] == "emeis"


@pytest.mark.parametrize(("nature", "kind", "label"), [
    ("Jugement d'ouverture d'une procédure de liquidation judiciaire", b.KIND_INSOLVENCY_OPENED, "Judicial liquidation opened"),
    ("Jugement de conversion en liquidation judiciaire", b.KIND_INSOLVENCY_OPENED, "Converted to judicial liquidation"),
    ("Jugement de clôture pour insuffisance d'actif", b.KIND_INSOLVENCY_CLOSED, "Collective proceedings closed for insufficient assets"),
    ("Jugement arrêtant le plan de cession", b.KIND_INSOLVENCY_PLAN, "Sale plan adopted by the court"),
    ("Dépôt de l'état des créances", b.KIND_INSOLVENCY_OTHER, "Statement of claims filed"),
])
def test_judgment_natures_are_classified(nature: str, kind: str, label: str) -> None:
    assert b.classify_judgment(nature) == (kind, label)


@pytest.mark.parametrize("nature", ["Jugement prononçant la faillite personnelle", "Jugement prononçant l'interdiction de gérer"])
def test_personal_judgments_are_not_classified(nature: str) -> None:
    assert b.classify_judgment(nature) is None


def test_mojibake_is_repaired() -> None:
    assert b._fix_text("RÃ©solution") == "Résolution"


def test_the_date_filter_uses_single_quotes() -> None:
    params = b.query_params(SIREN, 0, since="2026-11-01")
    assert params["where"] == f"registre=\"{SIREN}\" and dateparution >= date'2026-11-01'"


def test_latest_collective_is_the_newest_subject_procedure() -> None:
    latest = b.latest_collective(_payload())
    assert latest is not None and latest["id"] == PLAN["id"]


def test_a_truncated_read_says_so() -> None:
    assert b.notices_payload([OPENED], SIREN, total=5000)["truncated"] is True


async def test_a_failed_read_raises_and_is_not_cached(httpx_mock, tmp_path: Path) -> None:  # noqa: ANN001
    from opencheck.cache import Cache

    httpx_mock.add_response(url=re.compile(r"https://bodacc-datadila\.opendatasoft\.com/.*"), status_code=503, is_reusable=True)
    cache = Cache(tmp_path)
    with pytest.raises(b.BodaccUnavailable) as err:
        await b.notices_for(SIREN, cache=cache)
    assert err.value.reason == "upstream_error"
    assert cache.get_payload(b.CACHE_KEY.format(siren=SIREN), max_age_days=1) is None


async def test_a_read_is_cached_after_the_allowlist(httpx_mock, tmp_path: Path) -> None:  # noqa: ANN001
    from opencheck.cache import Cache

    httpx_mock.add_response(url=re.compile(r"https://bodacc-datadila\.opendatasoft\.com/.*"),
                            json={"total_count": 2, "results": [OPENED, BAN]})
    cache = Cache(tmp_path)
    payload = await b.notices_for(SIREN, cache=cache)
    assert [n["id"] for n in payload["notices"]] == [OPENED["id"]]
    held = cache.get_payload(b.CACHE_KEY.format(siren=SIREN), max_age_days=1)
    assert held is not None and "Dupont" not in json.dumps(held[0])
    assert await b.notices_for(SIREN, cache=cache) == payload  # no second request


# ---- History ---------------------------------------------------------------


def test_history_events_take_their_tiers_and_links() -> None:
    events = {e.raw_payload_ref: e for e in bodacc_change_events(SIREN, _payload())}
    assert events[OPENED["id"]].change_type is ChangeType.STATUS_CHANGED
    assert events[OPENED["id"]].event_date == "2023-03-24"
    assert events[RENAMED["id"]].change_type is ChangeType.LEGAL_NAME_CHANGE
    assert events[RENAMED["id"]].value_new == "emeis"
    sold = events[SOLD["id"]]
    assert sold.change_type is ChangeType.BUSINESS_SOLD
    assert sold.counterparty == f"ROGANDCO (SIREN {BUYER})"
    assert events[SOLD_TO_PERSON["id"]].counterparty == "a buyer that is not a company (not named)"
    assert events[ACCOUNTS["id"]].tier is Tier.ADMIN_NOISE
    assert all(e.record_url == b.NOTICE_URL.format(id=ref) for ref, e in events.items())


def test_two_bodacc_events_never_merge_into_one_row() -> None:
    events = bodacc_change_events(SIREN, _payload([OPENED, PLAN]))
    timeline = assemble_timeline(lei=EASY, extra_events=events)
    rows = [e for e in timeline.notable if "bodacc" in e.sources]
    assert len(rows) == 2
    assert {r.record_url for r in rows} == {b.NOTICE_URL.format(id=OPENED["id"]), b.NOTICE_URL.format(id=PLAN["id"])}


# ---- the watchlist -----------------------------------------------------------


@pytest.mark.parametrize(("kind", "tier"), [
    (b.KIND_INSOLVENCY_OPENED, "escalate"), (b.KIND_CONCILIATION, "escalate"),
    (b.KIND_DEREGISTERED, "escalate"), (b.KIND_DISSOLUTION, "escalate"),
    (b.KIND_NAME_CHANGE, "recheck"), (b.KIND_SEAT_TRANSFER, "recheck"),
    (b.KIND_BUSINESS_SOLD, "recheck"), (b.KIND_MODIFICATION, "recheck"),
    (b.KIND_ACCOUNTS_FILED, None), (b.KIND_ESTABLISHMENT_CREATED, None), (b.KIND_REGISTERED, None),
])
def test_decision_d_grades_each_kind(kind: str, tier: str | None) -> None:
    assert m.bodacc_notice_tier({"kind": kind}) == tier


@pytest.fixture
def france(env: Path, client: TestClient):  # noqa: F811
    token = _watch(client, EASY)["token"]
    store = wl.get_store()
    assert store is not None
    with store._conn() as conn:
        conn.execute(
            "UPDATE watches SET gleif_facts_json = ?, added_at = ? WHERE lei = ?",
            (json.dumps({"registered_at": "RA000189", "registered_as": "401 251 566"}), "2026-11-01T00:00:00Z", EASY),
        )
    yield {"store": store, "token": token}


def test_a_watchs_siren_comes_from_gleif_then_the_baseline() -> None:
    assert wl.watch_fr_siren({"gleif_facts": {"registered_at": "RA000192", "registered_as": "401 251 566"}}) == SIREN
    assert wl.watch_fr_siren({"snapshot": {"identifiers": {"FR-INSEE": SIREN}}}) == SIREN
    assert wl.watch_fr_siren({"gleif_facts": {"registered_at": "RA000585", "registered_as": "00102498"}}) is None


def test_the_first_tick_starts_the_watermark_and_reads_nothing(france: dict) -> None:
    assert wl.bodacc_tick(today="2026-11-10")["started"] == "2026-11-10"
    assert france["store"].get_meta(wl.BODACC_DAY_KEY) == "2026-11-10"


def test_a_material_notice_queues_a_rerun_once(france: dict, httpx_mock) -> None:  # noqa: ANN001
    store = france["store"]
    store.set_meta(wl.BODACC_DAY_KEY, "2026-11-09")
    opened = _record("A202600999001", "collective", "2026-11-10",
                     jugement={"nature": "Jugement d'ouverture de liquidation judiciaire", "date": "2026-11-05"})
    accounts = _record("C202600999002", "dpc", "2026-11-10", depot={"dateCloture": "2025-12-31"})
    old = _record("A202600999003", "collective", "2026-10-20",
                  jugement={"nature": "Jugement d'ouverture de redressement judiciaire", "date": "2026-10-15"})
    httpx_mock.add_response(url=re.compile(r"https://bodacc-datadila\.opendatasoft\.com/.*"),
                            json={"total_count": 3, "results": [opened, accounts, old]}, is_reusable=True)
    out = wl.bodacc_tick(force=True, today="2026-11-11")
    assert out == {"sirens": 1, "notices": 3, "queued": 1, "unavailable": 0}
    (item,) = store.take_pending(10)
    assert item["lei"] == EASY and item["tier"] == wl.TIER_REGISTER
    trigger = item["trigger"]
    assert trigger["register"] == "bodacc" and trigger["code"] == SIREN
    # Accounts are not material; a notice from before the watch is not news.
    assert [n["id"] for n in trigger["changes"]["notices"]] == ["A202600999001"]
    assert wl.bodacc_change_lines(trigger["changes"]) == [
        "Judicial liquidation opened (judgment of 2026-11-05; published in BODACC 2026-11-10).",
    ]
    assert m.register_trigger_tier(trigger) == "escalate"
    assert store.get_meta(wl.BODACC_DAY_KEY) == "2026-11-11"
    # The same notices again: nothing new is queued.
    assert wl.bodacc_tick(force=True, today="2026-11-11")["queued"] == 0


def test_an_unanswered_read_holds_the_watermark(france: dict, httpx_mock) -> None:  # noqa: ANN001
    store = france["store"]
    store.set_meta(wl.BODACC_DAY_KEY, "2026-11-09")
    httpx_mock.add_response(url=re.compile(r"https://bodacc-datadila\.opendatasoft\.com/.*"), status_code=503, is_reusable=True)
    out = wl.bodacc_tick(force=True, today="2026-11-11")
    assert out["unavailable"] == 1 and out["queued"] == 0
    assert store.get_meta(wl.BODACC_DAY_KEY) == "2026-11-09"
    assert wl.state()["fr_last_error"] == "BODACC answered HTTP 503."


def test_the_tier_is_off_without_live_calls(france: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    from opencheck.config import get_settings

    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    get_settings.cache_clear()
    assert wl.bodacc_tick(force=True)["skipped"] == "disabled"


# ---- the Sirene card (decision E) -------------------------------------------


def _sirene_fetch(monkeypatch: pytest.MonkeyPatch, answer: Any, **unit: Any) -> tuple[dict, list]:
    from tests import test_sirene as ts

    async def _notices(siren: Any, **_: Any) -> dict:
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(b, "notices_for", _notices)
    return ts._fetch(ts._ok(ts._unit(**unit)))


def test_the_card_says_the_latest_collective_procedure(monkeypatch: pytest.MonkeyPatch) -> None:
    from opencheck.findings import MAX_FINDING_CHARS, finding_sirene

    bundle, recorded = _sirene_fetch(monkeypatch, _payload(), categorieJuridiqueUniteLegale="5599",
                                     identifiantAssociationUniteLegale=None)
    assert recorded == [] and bundle["bodacc_read"] is True
    assert bundle["bodacc_collective"]["id"] == PLAN["id"]
    text = finding_sirene(bundle)
    assert text and len(text) <= MAX_FINDING_CHARS
    assert "accelerated safeguard plan adopted 24 July 2023 (BODACC)" in text


def test_an_association_is_not_asked(monkeypatch: pytest.MonkeyPatch) -> None:
    bundle, _ = _sirene_fetch(monkeypatch, RuntimeError("must not be called"))
    assert "bodacc_read" not in bundle


def test_bodacc_not_answering_is_a_degradation_not_a_clean_card(monkeypatch: pytest.MonkeyPatch) -> None:
    bundle, recorded = _sirene_fetch(
        monkeypatch, b.BodaccUnavailable("timeout", "BODACC did not answer in time."),
        categorieJuridiqueUniteLegale="5599", identifiantAssociationUniteLegale=None,
    )
    assert bundle["bodacc_read"] is False and "bodacc_collective" not in bundle
    (deg,) = recorded
    assert deg.check == "source_read" and deg.reason == "timeout"
