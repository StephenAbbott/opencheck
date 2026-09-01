"""Phase 88 — SEO entity pages: builder, store, routes, page furniture.

The fixture CSVs mirror the real GLEIF Golden Copy LEI2/RR column names
(LEI_3.1 CDF, verified against a live delta file 2026-08-03), so the builder
is exercised through exactly the code path the monthly refresh uses. LEIs
are deliberately fake but shape-valid; check-digit enforcement is off
suite-wide (see conftest.py), matching the long-standing fixture convention.
"""

from __future__ import annotations

import csv
import sqlite3
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck import entity_pages as ep
from opencheck.app import app
from opencheck.config import get_settings

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from build_entity_pages_db import load_lei2, load_rr, write_meta  # noqa: E402

# ---------------------------------------------------------------------------
# Fixture data
# ---------------------------------------------------------------------------

ACME_LEI = "2138000000000000A001"
PARENT_LEI = "2138000000000000P001"
NORDIC_LEI = "2138000000000000N001"
RETIRED_LEI = "2138000000000000R001"
SUCCESSOR_LEI = "2138000000000000S001"
GREEK_LEI = "2138000000000000G001"

LEI2_HEADER = [
    "LEI",
    "Entity.LegalName",
    "Entity.TransliteratedOtherEntityNames.TransliteratedOtherEntityName.1",
    "Entity.LegalAddress.City",
    "Entity.LegalAddress.Region",
    "Entity.LegalAddress.Country",
    "Entity.LegalJurisdiction",
    "Entity.EntityStatus",
    "Entity.LegalForm.EntityLegalFormCode",
    "Entity.SuccessorEntity.1.SuccessorLEI",
    "Registration.InitialRegistrationDate",
    "Registration.LastUpdateDate",
    "Registration.RegistrationStatus",
]

LEI2_ROWS = [
    # An ordinary UK company — quote in the name (prior form: Lucene bugs).
    [ACME_LEI, 'Acme "Widgets" & Sons Ltd', "", "LONDON", "GB-ENG", "GB", "GB",
     "ACTIVE", "H0PO", "", "2020-01-02T00:00:00Z", "2026-07-01T08:00:00.000Z", "ISSUED"],
    # Its direct+ultimate parent.
    [PARENT_LEI, "Acme Holdings PLC", "", "LONDON", "", "GB", "GB",
     "ACTIVE", "B6ES", "", "2019-05-05T00:00:00Z", "2026-06-15T08:00:00.000Z", "ISSUED"],
    # Danish entity with a slash in the name (A/S) — slug must stay clean.
    [NORDIC_LEI, "Nordic Widgets A/S", "", "KØBENHAVN", "", "DK", "DK",
     "ACTIVE", "9KSX", "", "2021-03-03T00:00:00Z", "2026-05-20T08:00:00.000Z", "LAPSED"],
    # Retired + merged into a successor.
    [RETIRED_LEI, "Olde Widgets GmbH", "", "BERLIN", "", "DE", "DE",
     "INACTIVE", "2HBR", SUCCESSOR_LEI, "2018-02-02T00:00:00Z",
     "2025-11-11T08:00:00.000Z", "MERGED_INTO_SUCCESSOR"],
    [SUCCESSOR_LEI, "Neue Widgets GmbH", "", "BERLIN", "", "DE", "DE",
     "ACTIVE", "2HBR", "", "2025-10-01T00:00:00Z", "2026-04-04T08:00:00.000Z", "ISSUED"],
    # Greek-script name with a GLEIF transliteration → slug from the fallback.
    [GREEK_LEI, "ΕΛΛΗΝΙΚΑ ΓΡΑΝΑΖΙΑ Α.Ε.", "HELLENIC GEARS SA", "ΑΘΗΝΑ", "", "GR", "GR",
     "ACTIVE", "5WWO", "", "2022-06-06T00:00:00Z", "2026-03-03T08:00:00.000Z", "ISSUED"],
]

RR_HEADER = [
    "Relationship.StartNode.NodeID",
    "Relationship.StartNode.NodeIDType",
    "Relationship.EndNode.NodeID",
    "Relationship.EndNode.NodeIDType",
    "Relationship.RelationshipType",
    "Registration.RegistrationStatus",
]

RR_ROWS = [
    [ACME_LEI, "LEI", PARENT_LEI, "LEI", "IS_DIRECTLY_CONSOLIDATED_BY", "PUBLISHED"],
    [ACME_LEI, "LEI", PARENT_LEI, "LEI", "IS_ULTIMATELY_CONSOLIDATED_BY", "PUBLISHED"],
    [NORDIC_LEI, "LEI", PARENT_LEI, "LEI", "IS_DIRECTLY_CONSOLIDATED_BY", "PUBLISHED"],
    # Retracted relationship must NOT apply.
    [RETIRED_LEI, "LEI", PARENT_LEI, "LEI", "IS_DIRECTLY_CONSOLIDATED_BY", "RETIRED"],
]


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> Path:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    return path


@pytest.fixture(scope="module")
def db_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    tmp = tmp_path_factory.mktemp("entity_pages")
    lei2 = _write_csv(tmp / "lei2.csv", LEI2_HEADER, LEI2_ROWS)
    rr = _write_csv(tmp / "rr.csv", RR_HEADER, RR_ROWS)
    out = tmp / "entity_pages.sqlite"
    conn = sqlite3.connect(out)
    conn.executescript(ep.SCHEMA)
    assert load_lei2(conn, lei2) == len(LEI2_ROWS)
    load_rr(conn, rr)
    write_meta(conn, source_publish_date="2026-08-03 08:00:00", record_count="6")
    conn.close()
    return out


@pytest.fixture
def client(db_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("OPENCHECK_ENTITY_PAGES_DB_FILE", str(db_path))
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    yield TestClient(app)
    get_settings.cache_clear()
    ep.reset_store_for_tests()


# ---------------------------------------------------------------------------
# slugify
# ---------------------------------------------------------------------------


def test_slugify_basics() -> None:
    assert ep.slugify_name("Unilever PLC") == "unilever-plc"
    assert ep.slugify_name('Acme "Widgets" & Sons Ltd') == "acme-widgets-sons-ltd"
    assert ep.slugify_name("Nordic Widgets A/S") == "nordic-widgets-a-s"
    assert ep.slugify_name("Café Européen S.à r.l.") == "cafe-europeen-s-a-r-l"


def test_slugify_falls_back_to_transliterated_name() -> None:
    # Greek/CJK names ASCII-fold to nothing, so the slug comes from GLEIF's
    # transliterated name; with no fallback the slug is empty (a slug-less
    # /entity/{LEI} URL is valid and canonical).
    assert (
        ep.slugify_name("ΕΛΛΗΝΙΚΑ ΓΡΑΝΑΖΙΑ Α.Ε.", "HELLENIC GEARS SA")
        == "hellenic-gears-sa"
    )
    assert ep.slugify_name("北京华为技术有限公司", "Huawei Technologies") == "huawei-technologies"
    assert ep.slugify_name("北京华为技术有限公司", None) == ""


def test_slugify_caps_length_at_word_boundary() -> None:
    long = (
        "The Extremely Long Corporate Vehicle For Structured "
        "Alternative Investments Number Twelve"
    )
    slug = ep.slugify_name(long)
    assert len(slug) <= 60
    assert not slug.endswith("-")
    assert slug.startswith("the-extremely-long")


# ---------------------------------------------------------------------------
# Builder + store
# ---------------------------------------------------------------------------


def test_builder_loads_entities_and_relationships(db_path: Path) -> None:
    store = ep.EntityStore(db_path)
    row = store.get(ACME_LEI)
    assert row is not None
    assert row.name == 'Acme "Widgets" & Sons Ltd'
    assert row.slug == "acme-widgets-sons-ltd"
    assert row.direct_parent_lei == PARENT_LEI
    assert row.ultimate_parent_lei == PARENT_LEI
    # Retracted RR row must not have applied.
    retired = store.get(RETIRED_LEI)
    assert retired is not None
    assert retired.direct_parent_lei is None
    assert retired.successor_lei == SUCCESSOR_LEI
    # Children of the parent: the two PUBLISHED direct links.
    children, total = store.children(PARENT_LEI)
    assert total == 2
    assert {c.lei for c in children} == {ACME_LEI, NORDIC_LEI}


def test_builder_delta_upsert_renames_and_reslugs(db_path: Path, tmp_path: Path) -> None:
    import shutil

    db2 = tmp_path / "delta.sqlite"
    shutil.copy(db_path, db2)
    delta = _write_csv(
        tmp_path / "lei2-delta.csv",
        LEI2_HEADER,
        [[ACME_LEI, "Acme Rockets Ltd", "", "LONDON", "", "GB", "GB",
          "ACTIVE", "H0PO", "", "2020-01-02T00:00:00Z",
          "2026-08-01T08:00:00.000Z", "ISSUED"]],
    )
    conn = sqlite3.connect(db2)
    assert load_lei2(conn, delta) == 1
    conn.close()
    store = ep.EntityStore(db2)
    row = store.get(ACME_LEI)
    assert row is not None
    assert row.name == "Acme Rockets Ltd"
    assert row.slug == "acme-rockets-ltd"
    assert store.count() == len(LEI2_ROWS)  # upsert, not append


# ---------------------------------------------------------------------------
# /entity route — the exact page furniture from the SEO ticket
# ---------------------------------------------------------------------------


def test_entity_page_furniture_exact(client: TestClient) -> None:
    r = client.get(f"/entity/{ACME_LEI}-acme-widgets-sons-ltd")
    assert r.status_code == 200
    body = r.text
    # Exact title template: "NAME OF SUBJECT - OpenCheck" (hyphen, not em-dash).
    assert "<title>Acme &quot;Widgets&quot; &amp; Sons Ltd - OpenCheck</title>" in body
    # Exact meta description template.
    assert (
        'content="Open-source customer due diligence risk checks on '
        "Acme &quot;Widgets&quot; &amp; Sons Ltd, powered by the Legal Entity "
        'Identifier, open data and open standards"' in body
    )
    assert (
        f'<link rel="canonical" href="https://opencheck.world/entity/{ACME_LEI}-acme-widgets-sons-ltd">'
        in body
    )
    # JSON-LD Organization with leiCode.
    assert '"@type": "Organization"' in body
    assert f'"leiCode": "{ACME_LEI}"' in body
    # CTA deep-links into the SPA's user-initiated live check — nofollow, so
    # crawlers don't treat 3.4M ?lei= parameter URLs as crawl targets.
    assert f'href="https://opencheck.world/?lei={ACME_LEI}" rel="nofollow"' in body
    # Parent link resolved to the parent's canonical path.
    assert f'href="/entity/{PARENT_LEI}-acme-holdings-plc"' in body
    # Cacheable + ETagged.
    assert r.headers["cache-control"].startswith("public, max-age=86400")
    assert r.headers.get("etag")


def test_entity_page_stale_or_missing_slug_301s_to_canonical(client: TestClient) -> None:
    for token in (ACME_LEI, f"{ACME_LEI}-old-company-name", ACME_LEI.lower()):
        r = client.get(f"/entity/{token}", follow_redirects=False)
        assert r.status_code == 301, token
        assert r.headers["location"] == f"/entity/{ACME_LEI}-acme-widgets-sons-ltd"


def test_entity_page_conditional_get_304(client: TestClient) -> None:
    first = client.get(f"/entity/{ACME_LEI}-acme-widgets-sons-ltd")
    etag = first.headers["etag"]
    again = client.get(
        f"/entity/{ACME_LEI}-acme-widgets-sons-ltd", headers={"If-None-Match": etag}
    )
    assert again.status_code == 304


def test_entity_page_head_supported(client: TestClient) -> None:
    r = client.head(f"/entity/{ACME_LEI}-acme-widgets-sons-ltd")
    assert r.status_code == 200
    assert r.headers.get("etag")


def test_entity_page_merged_entity_links_successor(client: TestClient) -> None:
    r = client.get(f"/entity/{RETIRED_LEI}-olde-widgets-gmbh")
    assert r.status_code == 200
    assert "Merged into successor" in r.text
    assert f'href="/entity/{SUCCESSOR_LEI}-neue-widgets-gmbh"' in r.text


def test_entity_page_transliterated_slug(client: TestClient) -> None:
    r = client.get(f"/entity/{GREEK_LEI}", follow_redirects=False)
    assert r.status_code == 301
    assert r.headers["location"] == f"/entity/{GREEK_LEI}-hellenic-gears-sa"


def test_entity_page_unknown_lei_404_noindex(client: TestClient) -> None:
    r = client.get("/entity/2138000000000000Z999-nope")
    assert r.status_code == 404
    assert 'content="noindex"' in r.text


def test_entity_page_malformed_token_404(client: TestClient) -> None:
    assert client.get("/entity/not-a-lei").status_code == 404


def test_entity_routes_503_with_retry_after_when_db_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPENCHECK_ENTITY_PAGES_DB_FILE", raising=False)
    monkeypatch.delenv("OPENCHECK_ENTITY_PAGES_DB_URL", raising=False)
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    c = TestClient(app)
    for path in (f"/entity/{ACME_LEI}", "/sitemaps/sitemap-index.xml", "/browse"):
        r = c.get(path)
        assert r.status_code == 503, path
        assert r.headers.get("retry-after"), path
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Sitemaps
# ---------------------------------------------------------------------------


def test_sitemap_index_and_shard(client: TestClient) -> None:
    idx = client.get("/sitemaps/sitemap-index.xml")
    assert idx.status_code == 200
    assert idx.headers["content-type"].startswith("application/xml")
    assert "https://opencheck.world/sitemaps/entities-1.xml" in idx.text
    assert "<lastmod>2026-08-03</lastmod>" in idx.text

    shard = client.get("/sitemaps/entities-1.xml")
    assert shard.status_code == 200
    assert (
        f"<loc>https://opencheck.world/entity/{ACME_LEI}-acme-widgets-sons-ltd</loc>"
        in shard.text
    )
    assert "<lastmod>2026-07-01</lastmod>" in shard.text
    assert shard.text.count("<url>") == len(LEI2_ROWS)

    assert client.get("/sitemaps/entities-2.xml").status_code == 404


# ---------------------------------------------------------------------------
# Browse hub
# ---------------------------------------------------------------------------


def test_browse_index_lists_countries(client: TestClient) -> None:
    r = client.get("/browse")
    assert r.status_code == 200
    assert 'href="/browse/GB"' in r.text
    assert "United Kingdom" in r.text
    assert "Denmark" in r.text


def test_browse_country_page_links_entities(client: TestClient) -> None:
    r = client.get("/browse/GB")
    assert r.status_code == 200
    assert f'href="/entity/{ACME_LEI}-acme-widgets-sons-ltd"' in r.text
    assert "<title>Entities registered in United Kingdom - OpenCheck</title>" in r.text
    # Unknown country and out-of-range page 404.
    assert client.get("/browse/XX").status_code == 404
    assert client.get("/browse/GB?page=99").status_code == 404


# ---------------------------------------------------------------------------
# robots.txt — crawlers welcome on pages, kept off the live-lookup API
# ---------------------------------------------------------------------------


def test_backend_robots_txt(client: TestClient) -> None:
    r = client.get("/robots.txt")
    assert r.status_code == 200
    assert "Disallow: /lookup" in r.text
    assert "Disallow: /search" in r.text
    assert "Disallow: /export" in r.text
    assert "Allow: /entity/" in r.text
    assert "Sitemap: https://opencheck.world/sitemaps/sitemap-index.xml" in r.text


# ---------------------------------------------------------------------------
# Phase 89 — GoatCounter on the server-rendered pages
# ---------------------------------------------------------------------------


def test_entity_page_goatcounter_bucket_and_cta_event(client: TestClient) -> None:
    """The snippet records the fixed '/entity' bucket — never the LEI —
    and the CTA fires the entity_page_cta event."""
    r = client.get(f"/entity/{ACME_LEI}-acme-widgets-sons-ltd")
    body = r.text
    assert "window.goatcounter={path:'/entity'}" in body
    assert 'data-goatcounter="https://opencheck.goatcounter.com/count"' in body
    assert "entity_page_cta" in body
    assert "cookies or fingerprinting" in body  # privacy note in the footer


def test_browse_page_goatcounter_bucket(client: TestClient) -> None:
    r = client.get("/browse/GB")
    assert "window.goatcounter={path:'/browse'}" in r.text


def test_goatcounter_disabled_by_empty_endpoint(
    db_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OPENCHECK_ENTITY_PAGES_DB_FILE", str(db_path))
    monkeypatch.setenv("OPENCHECK_GOATCOUNTER_ENDPOINT", "")
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    c = TestClient(app)
    body = c.get(f"/entity/{ACME_LEI}-acme-widgets-sons-ltd").text
    assert "goatcounter" not in body.lower()
    get_settings.cache_clear()
    ep.reset_store_for_tests()


def test_not_found_page_has_no_analytics(client: TestClient) -> None:
    """404s are noindex and shouldn't count as pageviews."""
    body = client.get("/entity/2138000000000000Z999-nope").text
    assert "gc.zgo.at" not in body


# ---------------------------------------------------------------------------
# Phase 91 — IndexNow (SEO Phase D)
# ---------------------------------------------------------------------------


def test_indexnow_key_route(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENCHECK_INDEXNOW_KEY", "abc123def456")
    get_settings.cache_clear()
    r = client.get("/indexnow/abc123def456.txt")
    assert r.status_code == 200
    assert r.text == "abc123def456"
    assert client.get("/indexnow/wrong-key.txt").status_code == 404
    get_settings.cache_clear()


def test_indexnow_key_route_404_when_unconfigured(client: TestClient) -> None:
    assert client.get("/indexnow/anything.txt").status_code == 404


def test_indexnow_urls_match_db_slugs(tmp_path: Path) -> None:
    """The URLs submitted to IndexNow must equal the pages' canonical URLs —
    both sides go through slugify_name, pinned here end to end."""
    from submit_indexnow import batched, payload_for, urls_from_delta

    delta = _write_csv(tmp_path / "delta.csv", LEI2_HEADER, LEI2_ROWS + LEI2_ROWS[:1])
    import csv as _csv

    with open(delta, encoding="utf-8") as fh:
        urls = list(urls_from_delta(_csv.DictReader(fh), "https://opencheck.world"))
    # Deduplicated (the duplicate ACME row collapses) and slug-identical.
    assert len(urls) == len(LEI2_ROWS)
    assert f"https://opencheck.world/entity/{ACME_LEI}-acme-widgets-sons-ltd" in urls
    assert f"https://opencheck.world/entity/{GREEK_LEI}-hellenic-gears-sa" in urls

    batches = list(batched(urls, size=4))
    assert [len(b) for b in batches] == [4, 2]
    body = payload_for(batches[0], "k123")
    assert body["host"] == "opencheck.world"
    assert body["keyLocation"] == "https://opencheck.world/indexnow/k123.txt"
    assert body["urlList"] == batches[0]


def test_indexnow_key_location_is_redacted_for_logs() -> None:
    """The key is a secret shared with Render; it must not reach a log line."""
    from submit_indexnow import redacted_key_location

    shown = redacted_key_location("supersecretkey123")
    assert "supersecretkey123" not in shown
    assert shown.startswith("https://opencheck.world/indexnow/supe")


@pytest.mark.parametrize(
    ("status", "body", "expected"),
    [
        (200, "rightkey", None),
        (404, "Not Found", "returned HTTP 404"),
        (200, "otherkey", "served a different value"),
    ],
)
def test_indexnow_check_key_location(
    monkeypatch: pytest.MonkeyPatch, status: int, body: str, expected: str | None
) -> None:
    """The pre-flight check does the engines' fetch and names what's wrong.

    A mismatch between OPENCHECK_INDEXNOW_KEY and the INDEXNOW_KEY secret is
    invisible at api.indexnow.org — it's just a 403 — so it's caught here.
    """
    import httpx
    import submit_indexnow

    def fake_get(url: str, **kwargs: object) -> httpx.Response:
        assert url == "https://opencheck.world/indexnow/rightkey.txt"
        return httpx.Response(status, text=body)

    monkeypatch.setattr(httpx, "get", fake_get)
    problem = submit_indexnow.check_key_location("rightkey")
    if expected is None:
        assert problem is None
    else:
        assert problem is not None and expected in problem
        assert "rightkey" not in problem  # redacted


def test_indexnow_check_key_location_survives_a_network_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A backend that's down is a skip with a reason, not a traceback."""
    import httpx
    import submit_indexnow

    def boom(url: str, **kwargs: object) -> httpx.Response:
        raise httpx.ConnectError("no route to host")

    monkeypatch.setattr(httpx, "get", boom)
    problem = submit_indexnow.check_key_location("rightkey")
    assert problem is not None and "could not fetch" in problem
