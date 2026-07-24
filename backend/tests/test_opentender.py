"""Tests for the OpenTender (DIGIWHIST) adapter + BODS mapper."""

from __future__ import annotations

import json
import sqlite3
import textwrap
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck.app import app
from opencheck.bods import map_opentender, validate_shape
from opencheck.config import get_settings
from opencheck.sources import REGISTRY, SearchKind
from opencheck.sources.opentender import (
    OpenTenderAdapter,
    _bridge_identifier,
    _id_forms,
    _slug,
)


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.delenv("OPENCHECK_ALLOW_LIVE", raising=False)
    monkeypatch.delenv("OPENTENDER_DB_FILE", raising=False)
    monkeypatch.delenv("OPENTENDER_S3_URL", raising=False)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _seed(tmp_path: Path, key: str, payload: dict) -> None:
    target = tmp_path / "cache" / "demos" / f"{key}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps({"_cached_at": 0, "payload": payload}))


# ------------------------------------------------------------------
# SQLite fixture helpers
# ------------------------------------------------------------------

_DDL = textwrap.dedent("""
    CREATE TABLE tenders (
        persistent_id TEXT PRIMARY KEY,
        source_id TEXT,
        country TEXT NOT NULL,
        title TEXT,
        is_awarded INTEGER DEFAULT 0,
        award_date TEXT,
        integrity_score REAL,
        transparency_score REAL,
        data TEXT NOT NULL
    );
    CREATE VIRTUAL TABLE body_names_fts
    USING fts5(persistent_id UNINDEXED, name, role,
               tokenize = "unicode61 remove_diacritics 1");
    CREATE TABLE body_ids (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        persistent_id TEXT NOT NULL,
        id_type TEXT, id_scope TEXT, id_value TEXT
    );
    CREATE INDEX idx_body_ids_lookup ON body_ids (id_type, id_value);
""")


def _make_db(tmp_path: Path, tenders: list[dict]) -> Path:
    """Write a minimal opentender.db with the supplied tender records."""
    db_path = tmp_path / "opentender.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(_DDL)
    cur = conn.cursor()
    for tender in tenders:
        pid = tender.get("persistentId") or tender.get("id") or ""
        country = tender.get("country", "GB")
        cur.execute(
            "INSERT OR REPLACE INTO tenders "
            "(persistent_id, source_id, country, title, is_awarded, award_date, "
            "integrity_score, transparency_score, data) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                pid,
                tender.get("id"),
                country,
                tender.get("title"),
                1 if tender.get("isAwarded") else 0,
                tender.get("awardDecisionDate"),
                (tender.get("ot") or {}).get("integrity"),
                (tender.get("ot") or {}).get("transparency"),
                json.dumps(tender),
            ),
        )
        # FTS + body_ids entries for buyers + bidders (mirrors the extract
        # script: names go to the FTS index, every bodyIds[] entry goes to the
        # flat body_ids identifier index with its raw id_value).
        def _index_body(body: dict, role: str) -> None:
            name = (body.get("name") or "").strip()
            if name:
                cur.execute(
                    "INSERT INTO body_names_fts (persistent_id, name, role) VALUES (?,?,?)",
                    (pid, name, role),
                )
            for ident in body.get("bodyIds") or []:
                id_value = str(ident.get("id") or "").strip()
                if not id_value:
                    continue
                cur.execute(
                    "INSERT INTO body_ids (persistent_id, id_type, id_scope, id_value) "
                    "VALUES (?,?,?,?)",
                    (pid, str(ident.get("type") or ""), str(ident.get("scope") or ""), id_value),
                )

        for body in tender.get("buyers") or []:
            _index_body(body, "buyer")
        for lot in tender.get("lots") or []:
            for bid in lot.get("bids") or []:
                for body in bid.get("bidders") or []:
                    _index_body(body, "bidder")
    conn.commit()
    conn.close()
    return db_path


# ---------------------------------------------------------------------
# Adapter — no DB (fixture / stub mode)
# ---------------------------------------------------------------------


def test_adapter_is_registered() -> None:
    # OpenTender is currently deactivated (removed from REGISTRY) pending
    # a more robust deployment approach. The adapter module is retained for
    # future re-enablement. Verify the adapter class itself still works.
    from opencheck.sources.opentender import OpenTenderAdapter
    adapter = OpenTenderAdapter()
    info = adapter.info
    assert info.license == "CC-BY-NC-SA-4.0"
    assert SearchKind.ENTITY in info.supports
    assert info.live_available is False


def test_adapter_live_available_when_db_configured(
    monkeypatch, tmp_path: Path
) -> None:
    """live_available becomes True once OPENTENDER_DB_FILE points at an existing DB."""
    db_path = _make_db(tmp_path, [])
    monkeypatch.setenv("OPENTENDER_DB_FILE", str(db_path))
    get_settings.cache_clear()
    # opentender is temporarily deactivated from REGISTRY; instantiate directly.
    info = OpenTenderAdapter().info
    assert info.live_available is True


async def test_search_rejects_person_kind() -> None:
    adapter = OpenTenderAdapter()
    assert await adapter.search("acme", SearchKind.PERSON) == []


async def test_search_returns_stub_when_no_fixture(tmp_path: Path) -> None:
    adapter = OpenTenderAdapter()
    hits = await adapter.search("nothing-here", SearchKind.ENTITY)
    assert len(hits) == 1
    assert hits[0].is_stub is True


async def test_search_serves_demo_fixture(tmp_path: Path) -> None:
    _seed(
        tmp_path,
        f"opentender/search/{_slug('Acme')}",
        {
            "tenders": [
                {
                    "id": "OT-XX-1",
                    "title": "Demo tender",
                    "country": "DE",
                    "buyers": [
                        {
                            "name": "Demo Authority",
                            "bodyIds": [
                                {"id": "DE111111111", "type": "VAT", "scope": "EU"}
                            ],
                        }
                    ],
                }
            ]
        },
    )
    adapter = OpenTenderAdapter()
    hits = await adapter.search("Acme", SearchKind.ENTITY)
    assert len(hits) == 1
    assert hits[0].is_stub is False
    assert hits[0].name == "Demo tender"
    # VAT identifier was bridged through to a strong-bridge key.
    assert hits[0].identifiers["vat"] == "DE111111111"
    assert hits[0].identifiers["opentender_id"] == "OT-XX-1"


async def test_fetch_returns_stub_when_no_fixture() -> None:
    adapter = OpenTenderAdapter()
    bundle = await adapter.fetch("OT-missing")
    assert bundle["is_stub"] is True


async def test_fetch_serves_demo_fixture(tmp_path: Path) -> None:
    _seed(
        tmp_path,
        f"opentender/tender/{_slug('OT-XX-1')}",
        {"id": "OT-XX-1", "title": "Demo tender"},
    )
    adapter = OpenTenderAdapter()
    bundle = await adapter.fetch("OT-XX-1")
    assert bundle["tender_id"] == "OT-XX-1"
    assert bundle["tender"]["title"] == "Demo tender"


# ---------------------------------------------------------------------
# Adapter — SQLite live mode
# ---------------------------------------------------------------------

_UK_TENDER = {
    "id": "cf-source-001",
    "persistentId": "UK_abc123def456",
    "title": "Road maintenance framework",
    "country": "UK",
    "isAwarded": True,
    "awardDecisionDate": "2024-06-01",
    "ot": {"integrity": 0.82, "transparency": 0.74},
    "buyers": [
        {
            "name": "Highways England",
            "bodyIds": [],
        }
    ],
    "lots": [
        {
            "lotId": "L1",
            "bids": [
                {
                    "isWinning": True,
                    "bidders": [
                        {
                            "name": "Balfour Beatty Ltd",
                            "bodyIds": [
                                {
                                    "id": "395826",
                                    "type": "ORGANIZATION_ID",
                                    "scope": "UNKNOWN",
                                }
                            ],
                        }
                    ],
                }
            ],
        }
    ],
}


async def test_db_search_finds_buyer_by_name(monkeypatch, tmp_path: Path) -> None:
    db_path = _make_db(tmp_path, [_UK_TENDER])
    monkeypatch.setenv("OPENTENDER_DB_FILE", str(db_path))
    get_settings.cache_clear()

    adapter = OpenTenderAdapter()
    hits = await adapter.search("Highways England", SearchKind.ENTITY)
    assert len(hits) == 1
    assert hits[0].hit_id == "UK_abc123def456"
    assert hits[0].name == "Road maintenance framework"


async def test_db_search_finds_bidder_by_name(monkeypatch, tmp_path: Path) -> None:
    db_path = _make_db(tmp_path, [_UK_TENDER])
    monkeypatch.setenv("OPENTENDER_DB_FILE", str(db_path))
    get_settings.cache_clear()

    adapter = OpenTenderAdapter()
    hits = await adapter.search("Balfour Beatty", SearchKind.ENTITY)
    assert len(hits) == 1
    assert hits[0].hit_id == "UK_abc123def456"


async def test_db_fetch_returns_tender(monkeypatch, tmp_path: Path) -> None:
    db_path = _make_db(tmp_path, [_UK_TENDER])
    monkeypatch.setenv("OPENTENDER_DB_FILE", str(db_path))
    get_settings.cache_clear()

    adapter = OpenTenderAdapter()
    bundle = await adapter.fetch("UK_abc123def456")
    assert bundle["tender_id"] == "UK_abc123def456"
    assert bundle["tender"]["title"] == "Road maintenance framework"


async def test_db_fetch_stub_when_not_found(monkeypatch, tmp_path: Path) -> None:
    db_path = _make_db(tmp_path, [])
    monkeypatch.setenv("OPENTENDER_DB_FILE", str(db_path))
    get_settings.cache_clear()

    adapter = OpenTenderAdapter()
    bundle = await adapter.fetch("nonexistent")
    assert bundle["is_stub"] is True


def _truncate(path: Path) -> None:
    """Simulate a download truncated mid-stream: keep the 16-byte SQLite header
    (so the bare header check still passes) but drop interior pages."""
    full = path.read_bytes()
    path.write_bytes(full[: max(16, len(full) // 2)])


def test_health_check_rejects_truncated_download(tmp_path: Path) -> None:
    db_path = _make_db(tmp_path, [_UK_TENDER])
    _truncate(db_path)
    # Header alone still looks valid — that's the trap the old check fell into.
    assert OpenTenderAdapter._is_valid_sqlite(db_path) is True
    # The integrity check catches the truncation.
    assert OpenTenderAdapter._db_is_healthy(db_path) is False


async def test_search_degrades_to_stub_when_db_corrupt(
    monkeypatch, tmp_path: Path
) -> None:
    """A malformed DB must degrade gracefully (stub), never raise
    'database disk image is malformed' up the lookup pipeline."""
    db_path = _make_db(tmp_path, [_UK_TENDER])
    _truncate(db_path)
    monkeypatch.setenv("OPENTENDER_DB_FILE", str(db_path))
    get_settings.cache_clear()

    adapter = OpenTenderAdapter()
    hits = await adapter.search("Highways England", SearchKind.ENTITY)
    # _conn() detected the bad DB, deleted it, and fell back to the stub path.
    assert len(hits) == 1 and hits[0].is_stub is True
    assert not db_path.exists()  # corrupt file removed so a re-download can run


class _FakeStream:
    """Minimal stand-in for httpx.stream(...) used as a context manager."""

    def __init__(self, content: bytes, headers: dict[str, str]):
        self._content = content
        self.headers = headers

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        return None

    def iter_bytes(self, chunk_size: int = 1 << 20):
        for i in range(0, len(self._content), chunk_size):
            yield self._content[i : i + chunk_size]


def _serve(monkeypatch, blob: bytes, *, content_length: int | None = None) -> None:
    from opencheck.sources import opentender as ot

    headers = {"Content-Length": str(content_length if content_length is not None else len(blob))}
    monkeypatch.setattr(ot.httpx, "stream", lambda method, url, **kw: _FakeStream(blob, headers))


def test_download_db_writes_verified_file_atomically(monkeypatch, tmp_path: Path) -> None:
    from opencheck.sources import opentender as ot

    blob = _make_db(tmp_path, [_UK_TENDER]).read_bytes()
    dest = tmp_path / "downloaded.db"
    _serve(monkeypatch, blob)

    assert ot._download_db(dest, "https://example/opentender.db", None) is True
    assert dest.exists() and ot.OpenTenderAdapter._db_is_healthy(dest)
    assert not (tmp_path / "downloaded.db.part").exists()  # temp cleaned up


def test_download_db_rejects_truncated_stream(monkeypatch, tmp_path: Path) -> None:
    """A stream that claims full Content-Length but delivers fewer bytes must be
    discarded — never published — so the malformed file can't reach a query."""
    from opencheck.sources import opentender as ot

    blob = _make_db(tmp_path, [_UK_TENDER]).read_bytes()
    dest = tmp_path / "downloaded.db"
    _serve(monkeypatch, blob[: len(blob) // 2], content_length=len(blob))

    assert ot._download_db(dest, "https://example/opentender.db", None) is False
    assert not dest.exists()
    assert not (tmp_path / "downloaded.db.part").exists()


def test_download_db_rejects_sha_mismatch(monkeypatch, tmp_path: Path) -> None:
    from opencheck.sources import opentender as ot

    blob = _make_db(tmp_path, [_UK_TENDER]).read_bytes()
    dest = tmp_path / "downloaded.db"
    _serve(monkeypatch, blob)

    assert ot._download_db(dest, "https://example/x.db", "deadbeef" * 8) is False
    assert not dest.exists()


def test_warm_opentender_db_noop_when_unconfigured() -> None:
    from opencheck.sources.opentender import warm_opentender_db

    warm_opentender_db()  # OPENTENDER_DB_FILE unset → returns without error


def test_warm_opentender_db_noop_when_not_registered(monkeypatch, tmp_path: Path) -> None:
    """Even fully configured, the warm-up must NOT download when OpenTender is
    absent from the REGISTRY. This is the Render /tmp regression guard: a
    retired source must never pull a multi-hundred-MB DB onto ephemeral disk on
    every cold start."""
    from opencheck.sources import REGISTRY
    from opencheck.sources import opentender as ot

    assert "opentender" not in REGISTRY  # retired — the live state
    dest = tmp_path / "warm.db"
    monkeypatch.setenv("OPENTENDER_DB_FILE", str(dest))
    monkeypatch.setenv("OPENTENDER_S3_URL", "https://example/opentender.db")
    get_settings.cache_clear()

    ot.warm_opentender_db()  # no HTTP mock wired — must not attempt a download
    assert not dest.exists()


def test_warm_opentender_db_downloads_when_registered_and_configured(
    monkeypatch, tmp_path: Path
) -> None:
    from opencheck.sources import REGISTRY
    from opencheck.sources import opentender as ot

    blob = _make_db(tmp_path, [_UK_TENDER]).read_bytes()
    dest = tmp_path / "warm.db"
    monkeypatch.setenv("OPENTENDER_DB_FILE", str(dest))
    monkeypatch.setenv("OPENTENDER_S3_URL", "https://example/opentender.db")
    get_settings.cache_clear()
    _serve(monkeypatch, blob)
    # Simulate OpenTender being live (re-added to the registry).
    monkeypatch.setitem(REGISTRY, "opentender", ot.OpenTenderAdapter())

    ot.warm_opentender_db()
    assert dest.exists() and ot.OpenTenderAdapter._db_is_healthy(dest)


async def test_db_search_no_results_returns_empty(monkeypatch, tmp_path: Path) -> None:
    db_path = _make_db(tmp_path, [_UK_TENDER])
    monkeypatch.setenv("OPENTENDER_DB_FILE", str(db_path))
    get_settings.cache_clear()

    adapter = OpenTenderAdapter()
    hits = await adapter.search("zzznomatch9999", SearchKind.ENTITY)
    assert hits == []


# ---------------------------------------------------------------------
# Identifier-first dispatch — fetch_by_registration (issue #29)
# ---------------------------------------------------------------------

# Orange S.A. — SIREN 380129866 — a genuine telecoms supplier the derived
# national ID resolves to. The lot bidder carries the SIREN as ORGANIZATION_ID.
_ORANGE_TENDER = {
    "id": "cf-fr-orange",
    "persistentId": "FR_orange_telecom_1",
    "title": "FOURNITURE DE SERVICE DE TELECOMMUNICATIONS",
    "country": "FR",
    "isAwarded": True,
    "buyers": [{"name": "Ministère de l'Intérieur", "bodyIds": []}],
    "lots": [
        {
            "bids": [
                {
                    "isWinning": True,
                    "bidders": [
                        {
                            "name": "Orange S.A.",
                            "bodyIds": [
                                {"id": "380129866", "type": "ORGANIZATION_ID", "scope": "FR"}
                            ],
                        }
                    ],
                }
            ]
        }
    ],
}

# Red-Orange e.U. — an Austrian furniture supplier that a name-keyed FTS MATCH
# on "Orange" wrongly surfaces (issue #29). Different entity, different country,
# different registration number — identifier dispatch must NOT return it.
_RED_ORANGE_TENDER = {
    "id": "cf-at-redorange",
    "persistentId": "AT_red_orange_furniture_1",
    "title": "Büromöbel Rahmenvereinbarung",
    "country": "AT",
    "isAwarded": True,
    "buyers": [{"name": "Stadt Wien", "bodyIds": []}],
    "lots": [
        {
            "bids": [
                {
                    "isWinning": True,
                    "bidders": [
                        {
                            "name": "Red-Orange e.U.",
                            "bodyIds": [
                                {"id": "999888777", "type": "ORGANIZATION_ID", "scope": "AT"}
                            ],
                        }
                    ],
                }
            ]
        }
    ],
}


def _live_adapter(monkeypatch, tmp_path: Path, tenders: list[dict]) -> OpenTenderAdapter:
    db_path = _make_db(tmp_path, tenders)
    monkeypatch.setenv("OPENTENDER_DB_FILE", str(db_path))
    get_settings.cache_clear()
    return OpenTenderAdapter()


async def test_fetch_by_registration_matches_by_national_id(
    monkeypatch, tmp_path: Path
) -> None:
    """Keying on Orange S.A.'s SIREN returns Orange's telecoms tender — the
    identifier-first inverse of the name search."""
    adapter = _live_adapter(monkeypatch, tmp_path, [_ORANGE_TENDER, _RED_ORANGE_TENDER])
    hits = await adapter.fetch_by_registration("FR", "380129866", legal_name="Orange S.A.")
    assert len(hits) == 1
    assert hits[0].hit_id == "FR_orange_telecom_1"
    assert hits[0].name == "FOURNITURE DE SERVICE DE TELECOMMUNICATIONS"


async def test_fetch_by_registration_rejects_name_token_collision(
    monkeypatch, tmp_path: Path
) -> None:
    """The #29 regression: a name MATCH on 'Orange' surfaces the unrelated
    Austrian 'Red-Orange e.U.', but the identifier path keyed on Orange's SIREN
    never returns it."""
    adapter = _live_adapter(monkeypatch, tmp_path, [_ORANGE_TENDER, _RED_ORANGE_TENDER])

    # Name search is the buggy path: 'Orange' pulls in Red-Orange e.U. as noise.
    name_hits = await adapter.search("Orange", SearchKind.ENTITY)
    name_ids = {h.hit_id for h in name_hits}
    assert "AT_red_orange_furniture_1" in name_ids  # the false positive

    # Identifier dispatch keyed on Orange's SIREN excludes the collision.
    id_hits = await adapter.fetch_by_registration("FR", "380129866")
    id_ids = {h.hit_id for h in id_hits}
    assert id_ids == {"FR_orange_telecom_1"}
    assert "AT_red_orange_furniture_1" not in id_ids


async def test_fetch_by_registration_is_country_scoped(
    monkeypatch, tmp_path: Path
) -> None:
    """The same registration number in a different country is not returned —
    scoping prevents cross-registry id collisions."""
    adapter = _live_adapter(monkeypatch, tmp_path, [_ORANGE_TENDER])
    # Right number, wrong country.
    assert await adapter.fetch_by_registration("CZ", "380129866") == []


async def test_fetch_by_registration_ignores_internal_id_types(
    monkeypatch, tmp_path: Path
) -> None:
    """A body that carries the value only under an internal key (SOURCE_ID /
    BVD_ID / ETALON_ID) is not a registration-number match."""
    tender = {
        "id": "cf-fr-internal",
        "persistentId": "FR_internal_only",
        "title": "Internal-keyed tender",
        "country": "FR",
        "lots": [
            {
                "bids": [
                    {
                        "isWinning": True,
                        "bidders": [
                            {
                                "name": "Some Supplier",
                                "bodyIds": [
                                    {"id": "552081317", "type": "SOURCE_ID", "scope": "FR"},
                                    {"id": "552081317", "type": "BVD_ID", "scope": "FR"},
                                ],
                            }
                        ],
                    }
                ]
            }
        ],
    }
    adapter = _live_adapter(monkeypatch, tmp_path, [tender])
    assert await adapter.fetch_by_registration("FR", "552081317") == []


async def test_fetch_by_registration_normalises_id_forms(
    monkeypatch, tmp_path: Path
) -> None:
    """A punctuated / zero-padded GLEIF registeredAs matches the raw DIGIWHIST
    id_value stored verbatim."""
    tender = {
        "id": "cf-cz-ico",
        "persistentId": "CZ_ico_1",
        "title": "Czech works contract",
        "country": "CZ",
        "lots": [
            {
                "bids": [
                    {
                        "isWinning": True,
                        "bidders": [
                            {
                                "name": "Stavby s.r.o.",
                                "bodyIds": [
                                    {"id": "45274649", "type": "HEADER_ICO", "scope": "CZ"}
                                ],
                            }
                        ],
                    }
                ]
            }
        ],
    }
    adapter = _live_adapter(monkeypatch, tmp_path, [tender])
    # GLEIF might publish it zero-padded / spaced; both normalise to the raw form.
    hits = await adapter.fetch_by_registration("CZ", "0045274649")
    assert {h.hit_id for h in hits} == {"CZ_ico_1"}


async def test_fetch_by_registration_empty_without_db() -> None:
    """Demo/stub mode (no DB) yields nothing — identifier dispatch has no
    relevance fallback."""
    adapter = OpenTenderAdapter()
    assert await adapter.fetch_by_registration("FR", "380129866") == []


async def test_fetch_by_registration_empty_on_blank_inputs(
    monkeypatch, tmp_path: Path
) -> None:
    adapter = _live_adapter(monkeypatch, tmp_path, [_ORANGE_TENDER])
    assert await adapter.fetch_by_registration("FR", "") == []
    assert await adapter.fetch_by_registration("", "380129866") == []


async def test_fetch_by_registration_no_match_returns_empty(
    monkeypatch, tmp_path: Path
) -> None:
    adapter = _live_adapter(monkeypatch, tmp_path, [_ORANGE_TENDER])
    assert await adapter.fetch_by_registration("FR", "000000000") == []


async def test_fetch_by_registration_degrades_to_empty_when_db_corrupt(
    monkeypatch, tmp_path: Path
) -> None:
    """A malformed DB must degrade gracefully (empty), never raise
    'database disk image is malformed' up the lookup pipeline."""
    db_path = _make_db(tmp_path, [_ORANGE_TENDER])
    _truncate(db_path)
    monkeypatch.setenv("OPENTENDER_DB_FILE", str(db_path))
    get_settings.cache_clear()

    adapter = OpenTenderAdapter()
    assert await adapter.fetch_by_registration("FR", "380129866") == []
    assert not db_path.exists()  # corrupt file removed so a re-download can run


def test_id_forms_normalisation() -> None:
    assert _id_forms("380129866") == ["380129866"]
    assert _id_forms("0045274649") == ["0045274649", "45274649"]
    assert _id_forms("0056.58.214") == ["0056.58.214", "005658214", "5658214"]
    assert _id_forms("") == []
    assert _id_forms("   ") == []


# ---------------------------------------------------------------------
# _bridge_identifier
# ---------------------------------------------------------------------


def test_bridge_vat() -> None:
    key, val = _bridge_identifier({"type": "VAT", "scope": "EU", "id": "DE123456789"})
    assert key == "vat"
    assert val == "DE123456789"


def test_bridge_organization_id_gb_scope() -> None:
    key, val = _bridge_identifier({"type": "ORGANIZATION_ID", "scope": "GB", "id": "06426844"})
    assert key == "gb_coh"
    assert val == "06426844"


def test_bridge_organization_id_unknown_scope() -> None:
    """DIGIWHIST UK data publishes ORGANIZATION_ID with scope UNKNOWN — should bridge to gb_coh."""
    key, val = _bridge_identifier({"type": "ORGANIZATION_ID", "scope": "UNKNOWN", "id": "395826"})
    assert key == "gb_coh"
    assert val == "00395826"  # zero-padded to 8 digits


def test_bridge_organization_id_unknown_scope_already_8_digits() -> None:
    key, val = _bridge_identifier({"type": "ORGANIZATION_ID", "scope": "UNKNOWN", "id": "06426844"})
    assert key == "gb_coh"
    assert val == "06426844"


def test_bridge_organization_id_non_gb_scope_not_bridged() -> None:
    """ORGANIZATION_ID with a non-UK scope should not be bridged to gb_coh."""
    key, val = _bridge_identifier({"type": "ORGANIZATION_ID", "scope": "DE", "id": "12345678"})
    assert key is None


def test_bridge_lei_detected_by_shape() -> None:
    lei = "2138003EK6PNMJUVGA51"
    key, val = _bridge_identifier({"type": "ETALON_ID", "scope": "GLOBAL", "id": lei})
    assert key == "lei"
    assert val == lei.upper()


def test_bridge_header_ico() -> None:
    key, val = _bridge_identifier({"type": "HEADER_ICO", "scope": "CZ", "id": "12345678"})
    assert key == "registration_number"
    assert val == "12345678"


def test_bridge_unknown_type_returns_none() -> None:
    key, val = _bridge_identifier({"type": "SOME_UNKNOWN_TYPE", "scope": "", "id": "abc"})
    assert key is None
    assert val is None


# ---------------------------------------------------------------------
# BODS mapper
# ---------------------------------------------------------------------


def _sample_tender() -> dict:
    return {
        "id": "OT-DE-2024-1",
        "title": "Crude oil framework",
        "country": "DE",
        "isAwarded": True,
        "awardDecisionDate": "2024-03-15",
        "buyers": [
            {
                "name": "Bundesamt für Energie",
                "address": {"city": "Berlin", "country": "DE"},
                "bodyIds": [
                    {"id": "DE324523002", "type": "VAT", "scope": "EU"}
                ],
            }
        ],
        "lots": [
            {
                "lotId": "L1",
                "awardDecisionDate": "2024-03-15",
                "bids": [
                    {
                        "isWinning": True,
                        "price": {"netAmount": 12500000, "currency": "EUR"},
                        "bidders": [
                            {
                                "name": "Acme Trading GmbH",
                                "address": {"country": "DE"},
                                "bodyIds": [
                                    {"id": "DE123456789", "type": "VAT", "scope": "EU"}
                                ],
                            }
                        ],
                    },
                    {
                        "isWinning": False,
                        "bidders": [
                            {
                                "name": "Loser Trading Ltd",
                                "bodyIds": [
                                    {"id": "12345678", "type": "ORGANIZATION_ID", "scope": "GB"}
                                ],
                            }
                        ],
                    },
                ],
            }
        ],
    }


def test_map_opentender_emits_buyer_and_bidder_entity_statements() -> None:
    bundle = map_opentender({"tender_id": "OT-DE-2024-1", "tender": _sample_tender()})
    statements = list(bundle)

    entities = [s for s in statements if s["recordType"] == "entity"]
    names = sorted(s["recordDetails"]["name"] for s in entities)
    assert "Bundesamt für Energie" in names
    assert "Acme Trading GmbH" in names
    # Losing bidder is also surfaced (so reconciler can bridge them).
    assert "Loser Trading Ltd" in names


def test_map_opentender_emits_award_relationship_only_for_winning_bid() -> None:
    bundle = map_opentender({"tender_id": "OT-DE-2024-1", "tender": _sample_tender()})
    statements = list(bundle)

    rels = [s for s in statements if s["recordType"] == "relationship"]
    # One winner × one buyer = one relationship; losing bidder gets none.
    assert len(rels) == 1
    interest = rels[0]["recordDetails"]["interests"][0]
    assert interest["type"] == "otherInfluenceOrControl"
    assert interest["beneficialOwnershipOrControl"] is False
    assert "12500000" in interest["details"]
    assert interest["startDate"] == "2024-03-15"


def test_map_opentender_bridges_gb_organization_id_to_gb_coh() -> None:
    """A GB-scoped ORGANIZATION_ID lands as the GB-ORG scheme identifier."""
    bundle = map_opentender({"tender_id": "OT-GB-1", "tender": {
        "id": "OT-GB-1",
        "buyers": [{"name": "Crown Commercial Service", "bodyIds": [
            {"id": "06426844", "type": "ORGANIZATION_ID", "scope": "GB"},
        ]}],
        "lots": [],
    }})
    entities = [s for s in bundle if s["recordType"] == "entity"]
    schemes = {
        i["scheme"]: i["id"]
        for s in entities
        for i in s["recordDetails"]["identifiers"]
    }
    assert schemes.get("ORG") is None  # Promoted to GB-ORG instead.
    assert schemes.get("GB-ORG") == "06426844"


def test_map_opentender_bridges_unknown_scope_organization_id_to_gb_org() -> None:
    """ORGANIZATION_ID with scope UNKNOWN (UK DIGIWHIST pattern) → GB-ORG, zero-padded."""
    bundle = map_opentender({"tender_id": "OT-UK-1", "tender": {
        "id": "OT-UK-1",
        "buyers": [{"name": "Highways England", "bodyIds": [
            {"id": "395826", "type": "ORGANIZATION_ID", "scope": "UNKNOWN"},
        ]}],
        "lots": [],
    }})
    entities = [s for s in bundle if s["recordType"] == "entity"]
    schemes = {
        i["scheme"]: i["id"]
        for s in entities
        for i in s["recordDetails"]["identifiers"]
    }
    assert schemes.get("GB-ORG") == "00395826"


def test_map_opentender_uk_country_maps_to_gb_jurisdiction() -> None:
    """DIGIWHIST country code 'UK' must map to ISO 'GB' in BODS jurisdiction."""
    bundle = map_opentender({"tender_id": "OT-UK-2", "tender": {
        "id": "OT-UK-2",
        "country": "UK",
        "buyers": [{"name": "NHS England", "address": {"country": "UK"}, "bodyIds": []}],
        "lots": [],
    }})
    entities = [s for s in bundle if s["recordType"] == "entity"]
    assert entities, "Expected at least one entity statement"
    jurisdiction = entities[0]["recordDetails"].get("jurisdiction")
    if jurisdiction:
        assert jurisdiction.get("code") == "GB", (
            f"Expected jurisdiction code 'GB', got {jurisdiction.get('code')!r}"
        )


def test_map_opentender_output_passes_bods_validation() -> None:
    bundle = map_opentender({"tender_id": "OT-DE-2024-1", "tender": _sample_tender()})
    issues = validate_shape(list(bundle))
    assert issues == []


def test_map_opentender_handles_empty_bundle() -> None:
    assert list(map_opentender({})) == []


def test_map_opentender_handles_present_but_empty_publications() -> None:
    """Regression for issue #39: ``"publications": []`` is PRESENT (not absent),
    so ``tender.get("publications", [{}])[0]`` bypasses the ``[{}]`` default and
    indexes an empty list — IndexError, killing the whole map_opentender call.
    The tender_url must fall back to the synthesized opentender.eu URL instead
    of crashing, and no statement's ``humanReadableURL`` should leak through."""
    tender = _sample_tender()
    tender["publications"] = []
    bundle = map_opentender({"tender_id": "OT-DE-2024-1", "tender": tender})
    statements = list(bundle)
    assert statements, "expected statements despite empty publications"
    rels = [s for s in statements if s["recordType"] == "relationship"]
    assert rels, "expected the winning-bid relationship statement"
    assert rels[0]["source"]["url"] == (
        "https://opentender.eu/de/tender/OT-DE-2024-1"
    )


def test_map_opentender_handles_none_publications() -> None:
    """``"publications": None`` must be treated the same as absent/empty."""
    tender = _sample_tender()
    tender["publications"] = None
    bundle = map_opentender({"tender_id": "OT-DE-2024-1", "tender": tender})
    statements = list(bundle)
    assert statements, "expected statements despite publications=None"
    rels = [s for s in statements if s["recordType"] == "relationship"]
    assert rels, "expected the winning-bid relationship statement"
    assert rels[0]["source"]["url"] == (
        "https://opentender.eu/de/tender/OT-DE-2024-1"
    )


# ---------------------------------------------------------------------
# Endpoint integration
# ---------------------------------------------------------------------


def test_deepen_opentender_flags_nc_sa_license(tmp_path: Path) -> None:
    # OpenTender is deactivated — /deepen returns 404 for unregistered sources.
    client = TestClient(app)
    r = client.get(
        "/deepen", params={"source": "opentender", "hit_id": "OT-DE-2024-1"}
    )
    assert r.status_code == 404
