"""INSEE's Sirene register, with the Répertoire national des associations.

The API answers are shaped on API Sirene 3.11's documented responses for
Transparency International France (SIREN 425138393, RNA W922001074), read
11 Nov 2026 with the key held out of the repository. The RNA index is built
by the real builder from a tiny waldec zip.
"""

from __future__ import annotations

import asyncio
import csv
import io
import os
import sqlite3
import zipfile
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from opencheck import degradation, provenance
from opencheck.bods import map_sirene
from opencheck.cache import Cache
from opencheck.config import get_settings
from opencheck.findings import MAX_FINDING_CHARS, finding_sirene
from opencheck.register_hops import hop_for
from opencheck.routers.lookup import _bh_sirene, _LookupCtx
from opencheck.sources import REGISTRY, rna_index, sirene
from opencheck.sources.schemas.sirene import SireneRna
from scripts import build_rna_index as build

TI_SIREN = "425138393"
TI_NIC = "00025"
TI_RNA = "W922001074"
CUT = "2026-10-01"
API = sirene.API_BASE


# ---------------------------------------------------------------------------
# The RNA index
# ---------------------------------------------------------------------------


def _waldec_row(rna: str, **values: str) -> list[str]:
    row = {name: "" for name in rna_index.WALDEC_HEADER}
    row.update({
        "id": rna, "id_ex": "0922001074", "date_creat": "1995-02-14",
        "date_decla": "2019-06-03", "date_publi": "1995-03-08", "date_disso": "0001-01-01",
        "nature": "D", "groupement": "S", "titre": "TRANSPARENCY INTERNATIONAL FRANCE",
        "objet": "lutter contre la corruption", "position": "A",
        "maj_time": "2019-06-04 10:00:00",
        # Personal data the builder must drop.
        "adrg_declarant": "MME DUPONT", "adrg_libvoie": "12 RUE DU DECLARANT",
        "dir_civilite": "MME", "observation": "note de la préfecture",
        "publiweb": "1",
    })
    row.update(values)
    return [row[name] for name in rna_index.WALDEC_HEADER]


def _zip(path: Path, members: dict[str, list[list[str]]]) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        for name, rows in members.items():
            buf = io.StringIO()
            writer = csv.writer(buf, delimiter=";")
            writer.writerow(rna_index.WALDEC_HEADER)
            writer.writerows(rows)
            archive.writestr(name, "﻿" + buf.getvalue())
    return path


ROWS = {
    "rna_waldec_20261001_dpt_92.csv": [
        _waldec_row(TI_RNA),
        # Dissolved, and with a Windows-1252 apostrophe arriving as U+0092.
        _waldec_row("W751000001", titre="L\u0092AMICALE", position="D",
                    date_disso="2020-05-01", date_creat="1900-01-01"),
        # Not named by any Sirene unit: dropped by the filter.
        _waldec_row("W751000002", titre="NOT IN SIRENE"),
        # A Paris number with a letter in the third place.
        _waldec_row("W9G2003456", titre="PARIS LETTER", rup_mi="1"),
    ],
    "rna_waldec_20261001_dpt_75.csv": [
        ["broken"],
        _waldec_row("not-a-number"),
    ],
}

SIRENE_NAMES = {
    TI_RNA: TI_SIREN, "W751000001": "111111111", "W9G2003456": "222222222",
}


@pytest.fixture()
def small_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(build, "MIN_MEMBERS", 1)
    monkeypatch.setattr(build, "MIN_ROWS", 1)
    monkeypatch.setattr(build, "MIN_KEPT", 1)


def _point_at(path: Path | None) -> None:
    if path is None:
        os.environ.pop("RNA_DB_FILE", None)
    else:
        os.environ["RNA_DB_FILE"] = str(path)
    get_settings.cache_clear()
    rna_index.reset_connection()


@pytest.fixture()
def index(tmp_path: Path, small_limits: None):
    out = tmp_path / "rna.sqlite"
    meta = build.build(
        _zip(tmp_path / "waldec.zip", ROWS), out, cut=CUT,
        source_url="https://example.org/rna_waldec_20261001.zip",
        sirene=SIRENE_NAMES, sirene_source="fixture",
    )
    _point_at(out)
    yield out, meta
    _point_at(None)


@pytest.fixture()
def no_index(tmp_path: Path):
    _point_at(tmp_path / "absent.sqlite")
    yield
    _point_at(None)


class TestNormalise:
    @pytest.mark.parametrize("raw", [" w922001074", "W922001074", "W9G2003456 "])
    def test_rna_numbers(self, raw: str) -> None:
        assert rna_index.normalise_rna(raw) == raw.strip().upper()

    @pytest.mark.parametrize("raw", ["0711003573", "W12345", "", None, "X922001074"])
    def test_not_rna_numbers(self, raw: Any) -> None:
        with pytest.raises(ValueError):
            rna_index.normalise_rna(raw)

    @pytest.mark.parametrize("raw", ["0001-01-01", "1900-01-01", "", "garbage"])
    def test_no_date(self, raw: str) -> None:
        assert rna_index.iso_day(raw) is None

    def test_a_date(self) -> None:
        assert rna_index.iso_day("2007-09-17 00:00:00") == "2007-09-17"


class TestBuilder:
    def test_keeps_only_what_sirene_names(self, index) -> None:
        path, meta = index
        conn = sqlite3.connect(path)
        ids = {r[0] for r in conn.execute("SELECT id FROM association")}
        assert ids == {TI_RNA, "W751000001", "W9G2003456"}
        assert meta["malformed"] == "2"
        assert meta["rows_read"] == "4"
        assert meta["filtered_by_sirene"] == "yes"
        assert meta["schema_version"] == rna_index.INDEX_SCHEMA_VERSION

    def test_personal_columns_never_reach_the_index(self, index) -> None:
        path, _ = index
        conn = sqlite3.connect(path)
        columns = {r[1] for r in conn.execute("PRAGMA table_info(association)")}
        assert columns == set(rna_index.COLUMNS) | set(rna_index.SIRENE_COLUMNS)
        assert not columns & rna_index.DROPPED_COLUMNS
        for dropped in ("adrg_declarant", "dir_civilite", "observation", "publiweb"):
            assert dropped in rna_index.DROPPED_COLUMNS
        dump = "\n".join(conn.iterdump())
        assert "DUPONT" not in dump and "DECLARANT" not in dump and "préfecture" not in dump

    def test_cleans_values(self, index) -> None:
        record = rna_index.association("W751000001")
        assert record is not None
        assert record["titre"] == "L’AMICALE"
        assert "date_creat" not in record  # 1900-01-01 is no date
        assert record["date_disso"] == "2020-05-01"
        assert rna_index.association(TI_RNA)["siren"] == TI_SIREN
        assert "date_disso" not in rna_index.association(TI_RNA)  # 0001-01-01

    def test_a_changed_header_is_refused(self, tmp_path: Path, small_limits: None) -> None:
        bad = tmp_path / "bad.zip"
        with zipfile.ZipFile(bad, "w") as archive:
            archive.writestr("x.csv", "id;titre\nW922001074;X\n")
        with pytest.raises(build.BuildRefused):
            build.build(bad, tmp_path / "o.sqlite", cut=CUT, source_url="", sirene=None)
        assert not (tmp_path / "o.sqlite").exists()

    def test_too_few_rows_is_refused(self, tmp_path: Path, small_limits: None,
                                     monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(build, "MIN_ROWS", 100)
        with pytest.raises(build.BuildRefused):
            build.build(_zip(tmp_path / "w.zip", ROWS), tmp_path / "o.sqlite",
                        cut=CUT, source_url="", sirene=None)

    def test_cut_of(self) -> None:
        assert build.cut_of("rna_waldec_20261001.zip") == "2026-10-01"

    def test_no_index_reads_none(self, no_index) -> None:
        assert not rna_index.index_available()
        assert rna_index.association(TI_RNA) is None


def test_the_schema_refuses_personal_rna_columns() -> None:
    with pytest.raises(Exception):
        SireneRna.model_validate({"id": TI_RNA, "adrg_declarant": "MME DUPONT"})
    with pytest.raises(Exception):
        SireneRna.model_validate({"id": TI_RNA, "dir_civilite": "MME"})


# ---------------------------------------------------------------------------
# The adapter
# ---------------------------------------------------------------------------


def _unit(**overrides: Any) -> dict[str, Any]:
    period = {
        "dateFin": None, "dateDebut": "2019-06-01",
        "etatAdministratifUniteLegale": "A",
        "denominationUniteLegale": "TRANSPARENCY INTERNATIONAL FRANCE",
        "categorieJuridiqueUniteLegale": "9220",
        "nicSiegeUniteLegale": TI_NIC,
        "activitePrincipaleUniteLegale": "94.99Z",
    }
    unit = {
        "siren": TI_SIREN, "statutDiffusionUniteLegale": "O",
        "dateCreationUniteLegale": "1995-02-14", "sigleUniteLegale": "TI FRANCE",
        "identifiantAssociationUniteLegale": TI_RNA,
        "dateDernierTraitementUniteLegale": "2025-03-12T09:00:00",
        "nombrePeriodesUniteLegale": 3,
        "periodesUniteLegale": [period, {**period, "dateFin": "2019-05-31", "dateDebut": "1995-02-14"}],
    }
    for key, value in overrides.items():
        if key in period:
            period[key] = value
        else:
            unit[key] = value
    return {"header": {"statut": 200, "message": "OK"}, "uniteLegale": unit}


ETAB = {
    "header": {"statut": 200, "message": "OK"},
    "etablissement": {
        "siret": TI_SIREN + TI_NIC,
        "adresseEtablissement": {
            "numeroVoieEtablissement": "14", "typeVoieEtablissement": "PASS",
            "libelleVoieEtablissement": "DAGORNO", "codePostalEtablissement": "75020",
            "libelleCommuneEtablissement": "PARIS", "complementAdresseEtablissement": None,
        },
    },
}


def _fetch(routes: dict[str, httpx.Response], *, key: str | None = "test-key",
           live: bool = True, cache: Cache | None = None) -> tuple[dict[str, Any], list]:
    adapter = sirene.SireneAdapter()
    adapter._cache = cache or Cache(root=Path(os.environ["OPENCHECK_DATA_ROOT"]) / "sirene-tests" / os.urandom(4).hex())
    settings = get_settings().model_copy(update={"allow_live": live, "insee_sirene_api_key": key})
    import unittest.mock as mock

    with mock.patch("opencheck.sources.sirene.get_settings", return_value=settings), \
            respx.mock(assert_all_called=False) as router:
        for path, response in routes.items():
            router.get(f"{API}{path}").mock(return_value=response)
        with degradation.recording() as recorded, provenance.recording():
            bundle = asyncio.run(adapter.fetch(TI_SIREN, legal_name="TI FRANCE (GLEIF)"))
        calls = list(router.calls)
    for call in calls:
        assert call.request.headers.get(sirene.API_KEY_HEADER) == (key or "")
        assert (key or "-") not in str(call.request.url)
    return bundle, list(recorded)


def _ok(unit: dict[str, Any] | None = None) -> dict[str, httpx.Response]:
    return {
        f"/siren/{TI_SIREN}": httpx.Response(200, json=unit or _unit()),
        f"/siret/{TI_SIREN}{TI_NIC}": httpx.Response(200, json=ETAB),
    }


class TestFetch:
    def test_an_association_with_its_rna_record(self, index) -> None:
        bundle, recorded = _fetch(_ok())
        assert recorded == []
        assert bundle["unit"]["denominationUniteLegale"] == "TRANSPARENCY INTERNATIONAL FRANCE"
        assert "periodesUniteLegale" not in bundle["unit"]
        assert bundle["head_office"]["siret"] == TI_SIREN + TI_NIC
        assert bundle["rna_status"] == "read"
        assert bundle["rna"]["titre"] == "TRANSPARENCY INTERNATIONAL FRANCE"
        assert bundle["rna_cut"] == CUT

    def test_no_index_is_said(self, no_index) -> None:
        bundle, recorded = _fetch(_ok())
        assert bundle["rna_status"] == "no_index"
        assert bundle["rna"] is None
        assert recorded == []

    def test_a_404_is_a_note_card_and_cached(self, no_index, tmp_path: Path) -> None:
        cache = Cache(root=tmp_path)
        bundle, recorded = _fetch({f"/siren/{TI_SIREN}": httpx.Response(404, json={})}, cache=cache)
        assert bundle["not_found"] and bundle["not_found_kind"] == "not_found"
        assert recorded == []
        again, _ = _fetch({}, cache=cache)
        assert again["not_found"]

    def test_a_403_non_diffusible_is_a_note_card(self, no_index) -> None:
        bundle, recorded = _fetch({f"/siren/{TI_SIREN}": httpx.Response(
            403, json={"header": {"statut": 403, "message": "Unité légale non diffusable"}})})
        assert bundle["not_found_kind"] == "not_diffusible"
        assert recorded == []

    def test_a_partially_public_unit_is_never_shown(self, no_index, tmp_path: Path) -> None:
        cache = Cache(root=tmp_path)
        bundle, _ = _fetch(_ok(_unit(statutDiffusionUniteLegale="P")), cache=cache)
        assert bundle["not_found_kind"] == "not_diffusible"
        assert "unit" not in bundle
        assert cache.get_payload(f"sirene/unit-v2/{TI_SIREN}", max_age_days=1) is None

    @pytest.mark.parametrize("status", [401, 403])
    def test_a_refused_key_is_a_degradation(self, no_index, status: int) -> None:
        bundle, recorded = _fetch({f"/siren/{TI_SIREN}": httpx.Response(
            status, json={"header": {"message": "Invalid key"}})})
        assert bundle["is_stub"]
        assert recorded and recorded[0].reason == degradation.REASON_NOT_CONFIGURED

    def test_a_429_is_a_degradation_and_not_cached(self, no_index, tmp_path: Path) -> None:
        cache = Cache(root=tmp_path)
        bundle, recorded = _fetch({f"/siren/{TI_SIREN}": httpx.Response(429)}, cache=cache)
        assert bundle["is_stub"]
        assert recorded and recorded[0].reason == degradation.REASON_RATE_LIMITED
        assert cache.get_payload(f"sirene/unit-v2/{TI_SIREN}", max_age_days=1) is None

    def test_a_failed_head_office_is_partial_and_not_cached(self, no_index, tmp_path: Path) -> None:
        cache = Cache(root=tmp_path)
        routes = _ok()
        routes[f"/siret/{TI_SIREN}{TI_NIC}"] = httpx.Response(503)
        bundle, recorded = _fetch(routes, cache=cache)
        assert bundle["head_office_read"] is False
        assert recorded[0].check == degradation.CHECK_SOURCE_READ
        assert cache.get_payload(f"sirene/unit-v2/{TI_SIREN}", max_age_days=1) is None

    def test_no_key_is_said(self, no_index) -> None:
        bundle, recorded = _fetch({}, key=None)
        assert bundle["is_stub"]
        assert recorded[0].reason == degradation.REASON_NOT_CONFIGURED

    def test_announced_only_with_a_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("INSEE_SIRENE_API_KEY", raising=False)
        get_settings.cache_clear()
        assert not REGISTRY["sirene"].covers_lei("969500AEH12X8M5XEO53")
        monkeypatch.setenv("INSEE_SIRENE_API_KEY", "k")
        get_settings.cache_clear()
        assert REGISTRY["sirene"].covers_lei("969500AEH12X8M5XEO53")
        monkeypatch.delenv("INSEE_SIRENE_API_KEY")
        get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Mapping, finding and hit
# ---------------------------------------------------------------------------


def _bundle(index_fixture: Any = None, **unit_overrides: Any) -> dict[str, Any]:
    bundle, _ = _fetch(_ok(_unit(**unit_overrides)))
    return bundle


def _map(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    with provenance.mapping_provenance(provenance.Provenance(liveness="live")):
        return list(map_sirene(bundle))


class TestMapper:
    def test_one_entity_with_both_identifiers(self, index) -> None:
        (entity,) = _map(_bundle())
        rd = entity["recordDetails"]
        schemes = {i["scheme"]: i["id"] for i in rd["identifiers"]}
        assert schemes == {"FR-INSEE": TI_SIREN, "FR-RNA": TI_RNA}
        assert rd["foundingDate"] == "1995-02-14"
        assert rd["entityType"]["type"] == "registeredEntity"
        assert "9220" in rd["entityType"]["details"]
        assert rd["addresses"][0]["address"].startswith("14 PASS DAGORNO")
        assert "TI FRANCE" in rd.get("alternateNames", [])

    def test_the_placeholder_date_borrows_the_rna_date(self, index) -> None:
        (entity,) = _map(_bundle(dateCreationUniteLegale="1900-01-01"))
        rd = entity["recordDetails"]
        assert rd["foundingDate"] == "1995-02-14"
        assert any("1900-01-01" in a["description"] for a in entity.get("annotations", []))

    def test_the_placeholder_date_without_the_rna(self, no_index) -> None:
        (entity,) = _map(_bundle(dateCreationUniteLegale="1900-01-01"))
        assert "foundingDate" not in entity["recordDetails"]

    def test_a_public_body_is_a_state_body(self, no_index) -> None:
        (entity,) = _map(_bundle(categorieJuridiqueUniteLegale="7220",
                                 identifiantAssociationUniteLegale=None))
        assert entity["recordDetails"]["entityType"]["type"] == "stateBody"

    def test_note_cards_and_stubs_map_to_nothing(self) -> None:
        assert _map({"not_found": True, "siren": TI_SIREN}) == []
        assert _map({"is_stub": True, "siren": TI_SIREN}) == []


class TestFinding:
    def test_an_association(self, index) -> None:
        text = finding_sirene(_bundle())
        assert text and len(text) <= MAX_FINDING_CHARS
        assert "active" in text and TI_RNA in text and "1995" in text

    def test_a_ceased_unit(self, no_index) -> None:
        text = finding_sirene(_bundle(etatAdministratifUniteLegale="C", dateDebut="2021-01-01"))
        assert "ceased on 1 January 2021" in text

    def test_creation_not_recorded(self, no_index) -> None:
        text = finding_sirene(_bundle(dateCreationUniteLegale="1900-01-01"))
        assert "creation date not recorded" in text

    def test_a_note_card_says_why(self) -> None:
        assert finding_sirene({"not_found": True, "coverage_note": "why"}) == "why"


class TestHit:
    def test_asserts_siren_and_rna(self, index) -> None:
        hit = _bh_sirene(_bundle(), TI_SIREN, _LookupCtx(lei="969500AEH12X8M5XEO53", legal_name="TI"))
        assert hit.identifiers == {"siren": TI_SIREN, "fr_rna": TI_RNA}
        assert "FR-RNA W922001074" in hit.summary

    def test_a_note_card_asserts_nothing(self) -> None:
        hit = _bh_sirene({"not_found": True, "not_found_kind": "not_found", "coverage_note": "x"},
                         TI_SIREN, _LookupCtx(lei="969500AEH12X8M5XEO53", legal_name="TI"))
        assert hit.identifiers == {}


def test_the_fr_insee_hop_falls_back_to_sirene() -> None:
    hop = hop_for("FR-INSEE")
    assert hop is not None and hop.source_id == "inpi"
    assert hop.fallback is not None and hop.fallback.source_id == "sirene"


def test_inpi_404_points_at_sirene() -> None:
    from opencheck.sources import inpi

    assert "Sirene" in inpi.COVERAGE_404 and len(inpi.COVERAGE_404) < 400
