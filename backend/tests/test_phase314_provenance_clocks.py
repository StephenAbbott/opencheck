"""Phase 314 — two clocks on ``Provenance``, and statements built inside scope.

The dates audit (Notion "📆 Audit of dates captured across OpenCheck", §4.2)
found one ``Provenance.retrieved_at`` slot doing two jobs for every bulk
source: the register's cut (right for ``statementDate``) and OpenCheck's
download (right for ``source.retrievedAt``). It now holds only the second;
``source_as_of`` holds the first. Reproducing the audit's open
``eiti_assessment`` observation then found a wider defect: 41 generator
mappers were drained after ``mapping_provenance`` had closed, so every
statement they built on ``/deepen`` and the lookup's deepen pass was dated
today with no ``retrievedAt``. Both are pinned here, in both directions.
"""

from __future__ import annotations

import ast
import csv
import datetime as dt
import os
import sqlite3
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import opencheck.bods as bods_pkg
from opencheck import provenance
from opencheck.app import app
from opencheck.bods.statements import _source_block, _statement_date
from opencheck.config import get_settings
from opencheck.provenance import Provenance
from tests import _entity_subtype_guard as guard

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

_PKG = Path(__file__).resolve().parent.parent / "opencheck"
UTC = timezone.utc
TODAY = dt.datetime.now(UTC).date().isoformat()
CUT = datetime(2026, 5, 31, tzinfo=UTC)
BUILT = datetime(2026, 10, 2, 4, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# The two clocks
# ---------------------------------------------------------------------------


class TestTwoClocks:
    def test_record_snapshot_takes_both_clocks_by_name(self):
        with pytest.raises(TypeError):
            provenance.record_snapshot(BUILT, "positional")  # type: ignore[misc]

    def test_each_clock_resolves_to_its_own_oldest(self):
        with provenance.recording() as rec:
            provenance.record_snapshot(retrieved_at=BUILT, source_as_of=CUT, detail="a")
            provenance.record_snapshot(
                retrieved_at=datetime(2026, 9, 1, tzinfo=UTC),
                source_as_of=datetime(2026, 7, 1, tzinfo=UTC),
            )
        resolved = rec.resolve()
        assert resolved.retrieved_at == datetime(2026, 9, 1, tzinfo=UTC)
        assert resolved.source_as_of == CUT
        assert resolved.to_dict()["source_as_of"] == "2026-05-31T00:00:00Z"
        assert resolved.to_dict()["retrieved_at"] == "2026-09-01T00:00:00Z"

    def test_the_cut_dates_the_statement_and_the_build_is_the_retrieval(self):
        prov = Provenance(liveness="snapshot", retrieved_at=BUILT, source_as_of=CUT)
        with provenance.mapping_provenance(prov):
            assert _statement_date() == "2026-05-31"
            assert _source_block("asp_moldova", None)["retrievedAt"] == "2026-10-02T04:00:00Z"
            # An explicit source date still wins.
            assert _statement_date("2026-04-01") == "2026-04-01"

    def test_without_a_cut_the_build_dates_the_statement(self):
        with provenance.mapping_provenance(
            Provenance(liveness="snapshot", retrieved_at=BUILT)
        ):
            assert _statement_date() == "2026-10-02"

    def test_a_cut_alone_claims_no_retrieval(self):
        with provenance.mapping_provenance(
            Provenance(liveness="snapshot", source_as_of=CUT)
        ):
            assert _statement_date() == "2026-05-31"
            assert "retrievedAt" not in _source_block("bce_belgium", None)

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("2026-08-31", datetime(2026, 8, 31, tzinfo=UTC)),
            ("2026-08-31T07:01:00Z", datetime(2026, 8, 31, 7, 1, tzinfo=UTC)),
            ("", None),
            (None, None),
            ("August 2026", None),
        ],
    )
    def test_parse_moment_never_guesses(self, raw, expected):
        assert provenance.parse_moment(raw) == expected

    def test_index_meta_tolerates_an_index_without_one(self, tmp_path):
        conn = sqlite3.connect(tmp_path / "x.sqlite")
        assert provenance.index_meta(conn) == {}
        conn.execute("CREATE TABLE meta (key TEXT, value TEXT)")
        conn.execute("INSERT INTO meta VALUES ('built_at', '2026-10-02T04:00:00Z')")
        assert provenance.index_meta(conn) == {"built_at": "2026-10-02T04:00:00Z"}


# ---------------------------------------------------------------------------
# Statements are built inside the scope (the /deepen defect)
# ---------------------------------------------------------------------------


def _mapper_call_parents() -> list[tuple[str, int, str]]:
    mappers = {
        n for n in dir(bods_pkg) if n.startswith("map_") and not n.startswith("map_to_")
    }
    found = []
    for path in sorted(_PKG.rglob("*.py")):
        rel = path.relative_to(_PKG).as_posix()
        if rel.startswith("bods/"):
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        parents = {c: n for n in ast.walk(tree) for c in ast.iter_child_nodes(n)}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            f = node.func
            name = (
                f.id if isinstance(f, ast.Name)
                else f.attr if isinstance(f, ast.Attribute) else None
            )
            if name in mappers or name == "mapper":
                found.append((rel, node.lineno, type(parents.get(node)).__name__))
    return found


def test_every_mapper_call_is_consumed_where_it_is_made():
    """A mapper's output is drained by the expression that calls it.

    Most mappers are generators. ``bundle = mapper(raw)`` inside
    ``mapping_provenance`` builds nothing; the statements are built when the
    generator is drained, and if that happens after the ``with`` block every
    one of them is mapped under the stub default. The Phase 313 guard checked
    that each call sat inside a scope; this one checks the call is consumed
    there — wrapped in ``list()``, ``unique_statements()`` or another call.
    """
    calls = _mapper_call_parents()
    assert len(calls) >= 6, "the scan found too few call sites to be meaningful"
    escaping = [c for c in calls if c[2] != "Call"]
    assert not escaping, (
        "mapper output assigned or returned without being consumed in the "
        f"provenance scope: {escaping}"
    )


def test_deepen_dates_a_generator_mapper_by_its_provenance():
    """End to end, the production reproduction: ``/deepen`` for the EITI
    Company Assessment (a generator mapper over the committed index) was dated
    today with no ``retrievedAt`` although the fetch recorded the harvest."""
    body = (
        TestClient(app)
        .get(
            "/deepen",
            params={"source": "eiti_assessment", "hit_id": "21380068P1DRHMJ8KU70"},
        )
        .json()
    )
    assert body["bods"], "Shell plc is in the committed assessment index"
    for statement in body["bods"]:
        assert statement["statementDate"] == "2026-09-04"
        assert statement["source"]["retrievedAt"] == "2026-09-04T18:55:30Z"


class TestTheGuard:
    """The conftest mapper guard catches both date defects (its self-test)."""

    def _snapshot_statement(self, date: str) -> dict:
        return {"statementId": "x", "recordType": "entity", "statementDate": date}

    def test_a_snapshot_dated_today_is_a_violation(self):
        guard.drain()
        with provenance.mapping_provenance(Provenance(liveness="snapshot")):
            guard.check_dates("map_x", self._snapshot_statement(TODAY), "snapshot")
        violations = guard.drain()
        assert violations and "today under snapshot" in violations[0][2]

    def test_a_snapshot_dated_by_its_cut_is_not(self):
        guard.drain()
        with provenance.mapping_provenance(Provenance(liveness="snapshot", source_as_of=CUT)):
            guard.check_dates("map_x", self._snapshot_statement("2026-05-31"), "snapshot")
        assert guard.drain() == []

    def test_a_generator_drained_outside_its_scope_is_a_violation(self):
        def map_fake(raw):
            yield {"statementId": "y", "recordType": "entity", "statementDate": "2026-01-01"}

        wrapped = guard.wrap(map_fake)
        guard.drain()
        with provenance.mapping_provenance(Provenance(liveness="live", retrieved_at=BUILT)):
            gen = wrapped({})
        list(gen)  # drained after the scope closed — the /deepen defect
        violations = guard.drain()
        assert violations and "outside the provenance scope" in violations[0][2]

    def test_a_generator_drained_inside_its_scope_is_not(self):
        def map_fake(raw):
            yield {"statementId": "y", "recordType": "entity", "statementDate": "2026-01-01"}

        wrapped = guard.wrap(map_fake)
        guard.drain()
        with provenance.mapping_provenance(Provenance(liveness="live", retrieved_at=BUILT)):
            list(wrapped({}))
        assert guard.drain() == []


# ---------------------------------------------------------------------------
# The bulk sources that read "declared today" (audit §4.3)
# ---------------------------------------------------------------------------


@pytest.fixture
def _env(monkeypatch):
    def _set(name: str, value: str) -> None:
        monkeypatch.setenv(name, value)
        get_settings.cache_clear()

    yield _set
    get_settings.cache_clear()


def _bce_db(path: Path, *, with_meta: bool) -> Path:
    import extract_bce

    conn = sqlite3.connect(path)
    for stmt in extract_bce._DDL.strip().split(";"):
        if stmt.strip():
            conn.execute(stmt)
    conn.execute(
        "INSERT INTO entities (enterprise_number, status, juridical_form, start_date, "
        "name_nl, name_fr, name_de, address, link) VALUES (?,?,?,?,?,?,?,?,?)",
        ("0403170701", "AC", "SA/NV", "1919-07-25", "ANHEUSER", "", "", "", ""),
    )
    if with_meta:
        # KBO's meta.csv shape: two columns, day-first dates.
        extract_bce._load_meta(
            conn,
            iter([
                {"Variable": "SnapshotDate", "Value": "04-10-2026"},
                {"Variable": "ExtractTimestamp", "Value": "05-10-2026 02:15:00"},
                {"Variable": "ExtractNumber", "Value": "321"},
            ]),
        )
    else:
        conn.execute("DROP TABLE meta")
    conn.commit()
    conn.close()
    return path


class TestBceBelgium:
    async def _fetch(self, db: Path, _env) -> Provenance:
        from opencheck.sources.bce_belgium import BceBelgiumAdapter

        _env("BCE_BELGIUM_DB_FILE", str(db))
        with provenance.recording() as rec:
            bundle = await BceBelgiumAdapter().fetch("0403170701")
        assert bundle["is_stub"] is False
        return rec.resolve()

    async def test_the_kbo_snapshot_date_is_the_cut(self, tmp_path, _env):
        resolved = await self._fetch(_bce_db(tmp_path / "bce.db", with_meta=True), _env)
        assert resolved.liveness == "snapshot"
        assert resolved.source_as_of == datetime(2026, 10, 4, tzinfo=UTC)
        assert resolved.retrieved_at is not None  # meta.built_at — the build
        assert resolved.retrieved_at.date() == dt.datetime.now(UTC).date()

    async def test_a_db_built_before_the_meta_table_uses_its_build_time(self, tmp_path, _env):
        db = _bce_db(tmp_path / "old.db", with_meta=False)
        resolved = await self._fetch(db, _env)
        assert resolved.source_as_of is None
        assert resolved.retrieved_at is not None
        assert resolved.retrieved_at.timestamp() == pytest.approx(db.stat().st_mtime, abs=1)

    def test_meta_is_stored_verbatim(self, tmp_path):
        conn = sqlite3.connect(_bce_db(tmp_path / "bce.db", with_meta=True))
        meta = provenance.index_meta(conn)
        assert meta["SnapshotDate"] == "04-10-2026"
        assert meta["ExtractNumber"] == "321"
        assert provenance.parse_moment(meta["built_at"]) is not None

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("04-10-2026", datetime(2026, 10, 4, tzinfo=UTC)),
            ("2026-10-04", datetime(2026, 10, 4, tzinfo=UTC)),
            ("", None),
            ("not a date", None),
        ],
    )
    def test_kbo_dates_are_day_first(self, raw, expected):
        from opencheck.sources.bce_belgium import _kbo_date

        assert _kbo_date(raw) == expected


class TestEdrUkraine:
    def test_the_export_date_is_read_from_the_xml_member(self, tmp_path):
        import build_edr_ukraine_index as edr

        zpath = tmp_path / "UO.zip"
        with zipfile.ZipFile(zpath, "w") as zf:
            info = zipfile.ZipInfo("UO.xml", date_time=(2026, 9, 8, 6, 30, 0))
            zf.writestr(info, "<DATA/>")
        assert edr.export_date_of(zpath) == "2026-09-08"

    def test_the_adapter_declares_both_clocks(self):
        from opencheck.sources.edr_ukraine import EdrUkraineAdapter

        adapter = EdrUkraineAdapter()
        adapter._meta = {"built_at": "2026-09-14T10:00:00Z", "export_date": "2026-09-08"}
        with provenance.recording() as rec:
            adapter._record_index_provenance()
        resolved = rec.resolve()
        assert resolved.liveness == "snapshot"
        assert resolved.source_as_of == datetime(2026, 9, 8, tzinfo=UTC)
        assert resolved.retrieved_at == datetime(2026, 9, 14, 10, tzinfo=UTC)


class TestCyprusDrcor:
    def test_the_release_date_is_persisted(self, tmp_path):
        import extract_cyprus

        orgs = tmp_path / "organisations.csv"
        with orgs.open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["registration_no", "organisation_name"])
            w.writerow(["HE123456", "EXAMPLE LTD"])
        out = tmp_path / "cyprus.db"
        assert extract_cyprus.main([
            "--organisations-csv", str(orgs), "--output", str(out),
            "--release-date", "2026-09-30",
        ]) == 0
        meta = provenance.index_meta(sqlite3.connect(out))
        assert meta["release_date"] == "2026-09-30"
        assert provenance.parse_moment(meta["built_at"]) is not None

    def test_the_adapter_declares_both_clocks(self):
        from opencheck.sources.cyprus_drcor import CyprusDrcorAdapter

        adapter = CyprusDrcorAdapter()
        adapter._meta = {"built_at": "2026-10-01T09:00:00Z", "release_date": "2026-09-30"}
        with provenance.recording() as rec:
            adapter._record_index_provenance()
        resolved = rec.resolve()
        assert resolved.source_as_of == datetime(2026, 9, 30, tzinfo=UTC)
        assert resolved.retrieved_at == datetime(2026, 10, 1, 9, tzinfo=UTC)

    def test_an_index_without_meta_claims_no_cut(self):
        from opencheck.sources.cyprus_drcor import CyprusDrcorAdapter

        with provenance.recording() as rec:
            CyprusDrcorAdapter()._record_index_provenance()
        (obs,) = rec.observations
        assert obs.liveness == "snapshot"
        assert obs.retrieved_at is None and obs.source_as_of is None


class TestEitiOrganisationIndex:
    async def test_a_match_declares_the_index_harvest(self, monkeypatch):
        from opencheck.sources import eiti

        monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
        get_settings.cache_clear()
        try:
            with provenance.recording() as rec:
                bundle = await eiti.EitiAdapter().fetch_by_registration("GB", "01285743")
        finally:
            get_settings.cache_clear()
        assert bundle is not None
        resolved = rec.resolve()
        assert resolved.liveness == "snapshot"
        assert resolved.retrieved_at is not None
        assert resolved.retrieved_at.date().isoformat() == eiti._index_generated

    def test_the_sweep_expects_a_snapshot(self):
        from opencheck.sources.probes import PROBES

        assert PROBES["eiti"].expect_liveness == frozenset({"snapshot"})


# ---------------------------------------------------------------------------
# Consumers of the clocks
# ---------------------------------------------------------------------------


def test_the_open_ownership_bundle_dates_by_its_publication(monkeypatch):
    from opencheck.routers import lookup

    monkeypatch.setattr(
        lookup.bods_data,
        "load_bundle",
        lambda subdir, key: [
            {"publicationDetails": {"publicationDate": "2026-03-01"}},
            {"publicationDetails": {"publicationDate": "2026-04-15"}},
        ],
    )
    prov = lookup._stored_bundle_provenance("gleif", "X")
    assert prov.liveness == "snapshot"
    assert prov.source_as_of == datetime(2026, 4, 15, tzinfo=UTC)
    assert prov.retrieved_at is None, "OO's publication is not OpenCheck's download"


async def test_the_sweep_ages_a_snapshot_by_its_cut(monkeypatch):
    """A fresh rebuild of an old dump is still an old dump."""
    had = "OPENCHECK_ALLOW_LIVE" in os.environ
    import source_health as sweep

    if not had:
        os.environ.pop("OPENCHECK_ALLOW_LIVE", None)
    from opencheck.sources.probes import SourceProbe

    class _Adapter:
        async def fetch(self, *a, **k):
            provenance.record_snapshot(
                retrieved_at=datetime.now(UTC),
                source_as_of=datetime.now(UTC) - dt.timedelta(days=100),
                detail="test",
            )
            return {"company": {"name": "x"}}

    monkeypatch.setitem(sweep.REGISTRY, "_probe_314", _Adapter())
    probe = SourceProbe(
        tier="index", subject="s", args=("1",),
        expect_liveness=frozenset({"snapshot"}), snapshot_max_age_days=70,
    )
    result = await sweep._run_probe("_probe_314", probe, timeout=5)
    assert result.status == sweep.DEGRADED
    assert "100 days old" in result.reason
    assert result.source_as_of is not None
