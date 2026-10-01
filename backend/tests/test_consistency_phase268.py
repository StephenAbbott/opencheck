"""Phase 268 — the record-consistency instrument after its first two
production readings (17 Sept and 1 Oct 2026).

Each test is named for what the counters showed: MEIP's PermID read as a
register number (every identifier clash in the first reading), the bridging
identifier counted as agreement (52 tautological "agreements" on one pair),
ABN against ACN under one jurisdiction key, GLEIF's creation date against
seven registers, a liquidation counted as a disagreement with ACTIVE, and
counters that did not survive the day's deploy.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from opencheck import consistencystats
from opencheck.app import app
from opencheck.bods import liveness
from opencheck.consistency import (
    AGREE,
    DISAGREE,
    IDENTIFIER_CLASH,
    ONE_MISSING,
    STALE,
    assess_consistency,
    one_per_entity_identifiers,
)
from opencheck.config import get_settings

from tests.test_consistency import LEI, _items, _stmt


def _clash(result, relation=None):
    return _items(result, IDENTIFIER_CLASH, relation)


# ---------------------------------------------------------------------
# Identifiers
# ---------------------------------------------------------------------


def test_meip_permid_without_a_scheme_key_is_not_a_register_number() -> None:
    """MEIP identifiers carry ``schemeName`` only. Before Phase 268 the absent
    key read as the empty scheme and a PermID became the entity's
    ``REGISTER:<jur>`` value — 50 of 58 gleif|meip pairs "clashed"."""
    meip = _stmt("meip", lei=LEI)
    meip["recordDetails"]["identifiers"] += [
        {"id": "4295889999", "schemeName": "Refinitiv PermID"},
        {"id": "IQ12345", "schemeName": "S&P Capital IQ"},
    ]
    assert set(one_per_entity_identifiers(meip)) == {"LEI"}
    gl = _stmt("gleif", number=("GB-COH", "04366849"))
    assert _clash(assess_consistency([meip, gl]), DISAGREE) == []


def test_a_present_and_empty_scheme_is_still_gleifs_unlabelled_number() -> None:
    gl = _stmt("gleif", number=("", "04366849"))
    assert one_per_entity_identifiers(gl)["REGISTER:GB"] == "04366849"
    ra = _stmt("gleif", number=("RA000585", "04366849"))
    assert one_per_entity_identifiers(ra)["REGISTER:GB"] == "04366849"


def test_the_bridge_is_not_agreement() -> None:
    """Two statements sharing only the LEI are in one group *because* of the
    LEI; that is not an agreement about the entity. 52 companies_house|gleif
    "agreements" in the first reading were exactly this."""
    assert _clash(assess_consistency([_stmt("gleif"), _stmt("companies_house")])) == []


def test_a_second_matching_identifier_is_agreement() -> None:
    gl = _stmt("gleif", number=("GB-COH", "04366849"))
    ch = _stmt("companies_house", number=("GB-COH", "04366849"))
    (item,) = _clash(assess_consistency([gl, ch]))
    assert item.relation == AGREE and item.values == ("GB-COH:04366849", "GB-COH:04366849")


def test_a_number_under_label_and_jurisdiction_counts_once() -> None:
    """GLEIF before Phase 239 (unlabelled) against a labelled register: one
    identifier, one item — not one under ``REGISTER:GB`` and one under the
    label, which doubled every register|gleif agree count."""
    gl = _stmt("gleif", lei=None, number=("", "04366849"))
    ch = _stmt("companies_house", lei=None, number=("GB-COH", "04366849"))
    result = assess_consistency([gl, ch])
    # The one shared number bridged the pair, so nothing survives …
    assert _clash(result) == []
    # … and with the LEI as the bridge, the register number is one agreement.
    gl2 = _stmt("gleif", number=("", "04366849"))
    ch2 = _stmt("companies_house", number=("GB-COH", "04366849"))
    items = _clash(assess_consistency([gl2, ch2]))
    assert len(items) == 1 and items[0].relation == AGREE


def test_abn_and_acn_are_two_registers_not_a_clash() -> None:
    """ABR carries the ABN and the ACN; GLEIF's ``registeredAs`` for an
    Australian entity is the ACN. Under one ``REGISTER:AU`` key the ABN met
    the ACN — 8 of 16 abr_australia|gleif pairs "clashed" on 1 Oct 2026."""
    abr = _stmt("abr_australia", jurisdiction=("Australia", "AU"))
    abr["recordDetails"]["identifiers"] += [
        {"id": "51824753556", "scheme": "AU-ABN"},
        {"id": "004085616", "scheme": "AU-ACN"},
    ]
    gl = _stmt("gleif", jurisdiction=("Australia", "AU"), number=("AU-ACN", "004085616"))
    items = _clash(assess_consistency([abr, gl]))
    assert [i.relation for i in items] == [AGREE]
    assert items[0].values == ("AU-ACN:004085616", "AU-ACN:004085616")
    # The same register, a different number: still the clash worth finding.
    gl2 = _stmt("gleif", jurisdiction=("Australia", "AU"), number=("AU-ACN", "004085617"))
    (item,) = _clash(assess_consistency([abr, gl2]))
    assert item.relation == DISAGREE


def test_an_unlabelled_number_agrees_with_any_register_of_its_jurisdiction() -> None:
    """A pre-239 GLEIF record (empty scheme) carrying the ACN against ABR's
    two labelled numbers: agreement, because one of them is that number."""
    abr = _stmt("abr_australia", jurisdiction=("Australia", "AU"))
    abr["recordDetails"]["identifiers"] += [
        {"id": "51824753556", "scheme": "AU-ABN"},
        {"id": "004085616", "scheme": "AU-ACN"},
    ]
    gl = _stmt("gleif", jurisdiction=("Australia", "AU"), number=("", "004085616"))
    items = _clash(assess_consistency([abr, gl]))
    assert [i.relation for i in items] == [AGREE]
    gl2 = _stmt("gleif", jurisdiction=("Australia", "AU"), number=("", "999999999"))
    (item,) = _clash(assess_consistency([abr, gl2]))
    assert item.relation == DISAGREE and "REGISTER:AU:999999999" in item.values


def test_romanian_cui_and_j_number_are_never_compared_with_each_other() -> None:
    anaf = _stmt("anaf_romania", jurisdiction=("Romania", "RO"), number=("RO-CUI", "14399840"))
    onrc = _stmt("onrc_romania", jurisdiction=("Romania", "RO"), number=("RO-ONRC", "J40/8302/1997"))
    assert _clash(assess_consistency([anaf, onrc])) == []  # LEI was the bridge; nothing else shared


# ---------------------------------------------------------------------
# Comparison table
# ---------------------------------------------------------------------


def test_gleif_creation_date_is_not_compared_with_a_registers_incorporation() -> None:
    """ARES vs GLEIF: 57 of 65 differed; KRS vs GLEIF: 12 of 12. A different
    concept, so no item — and no one_missing either."""
    gl = _stmt("gleif", founding="1992-05-01", jurisdiction=("Czechia", "CZ"))
    ares = _stmt("ares", founding="1993-01-01", jurisdiction=("Czechia", "CZ"))
    assert _items(assess_consistency([gl, ares]), "founding_date") == []
    # Register against register still runs.
    other = _stmt("openaleph", founding="1993-01-01", jurisdiction=("Czechia", "CZ"))
    assert _items(assess_consistency([ares, other]), "founding_date")[0].relation == AGREE


def test_anaf_fiscal_registration_is_not_onrc_incorporation() -> None:
    anaf = _stmt("anaf_romania", founding="2001-03-01", jurisdiction=("Romania", "RO"))
    onrc = _stmt("onrc_romania", founding="1997-11-20", jurisdiction=("Romania", "RO"))
    assert _items(assess_consistency([anaf, onrc]), "founding_date") == []
    assert _items(assess_consistency([anaf, onrc]), "jurisdiction")[0].relation == AGREE


def test_meip_country_is_not_compared_as_jurisdiction() -> None:
    meip = _stmt("meip", jurisdiction=("Netherlands", "NL"))
    gl = _stmt("gleif")
    assert _items(assess_consistency([meip, gl]), "jurisdiction") == []


def test_a_liquidation_under_way_is_not_a_disagreement_with_active() -> None:
    """A company in liquidation is ACTIVE in GLEIF until it is dissolved."""
    reg = _stmt("bolagsverket", status=liveness.PENDING, jurisdiction=("Sweden", "SE"))
    gl = _stmt("gleif", status=liveness.LIVE, jurisdiction=("Sweden", "SE"))
    (item,) = _items(assess_consistency([reg, gl]), "liveness")
    assert item.relation == ONE_MISSING and item.values == (None, "live")
    dissolved = _stmt("bolagsverket", status=liveness.TERMINAL, jurisdiction=("Sweden", "SE"))
    assert _items(assess_consistency([dissolved, gl]), "liveness")[0].relation == DISAGREE


def test_independent_disagreements_are_logged_with_their_values(caplog) -> None:
    ch = _stmt("companies_house", status=liveness.TERMINAL, since="2019-04-03")
    gl = _stmt("gleif", status=liveness.LIVE)
    with caplog.at_level(logging.INFO, logger="opencheck.consistency"):
        assess_consistency([ch, gl])
    lines = [r.getMessage() for r in caplog.records if "consistency disagree" in r.getMessage()]
    assert len(lines) == 1
    assert "field=liveness" in lines[0] and "'terminal'" in lines[0] and "'live'" in lines[0]
    assert "companies_house" in lines[0] and "gleif" in lines[0]


def test_stale_is_not_logged(caplog) -> None:
    ch = _stmt("companies_house", status=liveness.TERMINAL)
    oc = _stmt("opencorporates", status=liveness.LIVE)
    with caplog.at_level(logging.INFO, logger="opencheck.consistency"):
        result = assess_consistency([ch, oc])
    assert _items(result, "liveness")[0].relation == STALE
    assert not [r for r in caplog.records if "consistency disagree" in r.getMessage()]


# ---------------------------------------------------------------------
# Persisted counters
# ---------------------------------------------------------------------


def _one_lookup() -> None:
    ch = _stmt("companies_house", status=liveness.TERMINAL)
    gl = _stmt("gleif", status=liveness.LIVE)
    consistencystats.record(assess_consistency([ch, gl]))


def test_counters_survive_a_restart(tmp_path: Path) -> None:
    path = tmp_path / "consistencystats.sqlite"
    consistencystats.reset()
    try:
        assert consistencystats.configure(path)
        _one_lookup()
        _one_lookup()
        first = consistencystats.stats()
        assert first["persisted"] is True and first["boots"] == 1 and first["lookups"] == 2
        assert first["pairs"]["liveness|companies_house|gleif"]["disagree"] == 2
        since = first["since"]
        # A deploy: new process, same file.
        consistencystats.reset()
        assert consistencystats.stats()["persisted"] is False
        assert consistencystats.configure(path)
        _one_lookup()
        second = consistencystats.stats()
        assert second["boots"] == 2 and second["lookups"] == 3 and second["since"] == since
        assert second["pairs"]["liveness|companies_house|gleif"]["disagree"] == 3
        assert "Persisted" in second["note"]
    finally:
        consistencystats.reset()


def test_the_file_holds_counts_under_closed_keys_and_nothing_else(tmp_path: Path) -> None:
    path = tmp_path / "consistencystats.sqlite"
    consistencystats.reset()
    try:
        consistencystats.configure(path)
        ch = _stmt("companies_house", status=liveness.TERMINAL, founding="2002-02-05", name="ZQX SECRET LTD")
        gl = _stmt("gleif", status=liveness.LIVE, founding="2002-02-05", name="ZQX SECRET LTD")
        consistencystats.record(assess_consistency([ch, gl]))
    finally:
        consistencystats.reset()
    conn = sqlite3.connect(path)
    try:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert tables == {"meta", "pairs"}
        rows = conn.execute("SELECT field, source_a, source_b, relation, n FROM pairs").fetchall()
        assert ("liveness", "companies_house", "gleif", "disagree", 1) in rows
        blob = " ".join(" ".join(map(str, r)) for r in rows)
        blob += " ".join(f"{k} {v}" for k, v in conn.execute("SELECT key, value FROM meta"))
        assert LEI not in blob and "ZQX" not in blob and "2002" not in blob and "terminal" not in blob
        assert int(conn.execute("PRAGMA user_version").fetchone()[0]) == 1
    finally:
        conn.close()


def test_an_unwritable_path_leaves_the_counters_in_process(tmp_path: Path) -> None:
    consistencystats.reset()
    try:
        blocker = tmp_path / "not-a-directory"
        blocker.write_text("x")
        assert consistencystats.configure(blocker / "consistencystats.sqlite") is False
        _one_lookup()
        out = consistencystats.stats()
        assert out["persisted"] is False and out["lookups"] == 1
        assert "resets on deploy" in out["note"]
    finally:
        consistencystats.reset()


def test_the_app_attaches_the_file_from_the_setting(monkeypatch, tmp_path: Path) -> None:
    path = tmp_path / "consistencystats.sqlite"
    monkeypatch.setenv("OPENCHECK_CONSISTENCYSTATS_DB_FILE", str(path))
    get_settings.cache_clear()
    consistencystats.reset()
    try:
        with TestClient(app) as client:
            body = client.get("/consistencystats").json()
            assert body["persisted"] is True and body["boots"] == 1
        assert path.exists()
    finally:
        get_settings.cache_clear()
        consistencystats.reset()


def test_endpoint_without_the_setting_says_so() -> None:
    consistencystats.reset()
    body = TestClient(app).get("/consistencystats").json()
    assert body["persisted"] is False and body["boots"] == 1
    assert "OPENCHECK_CONSISTENCYSTATS_DB_FILE" in body["note"]
    assert json.dumps(body)  # serialisable
