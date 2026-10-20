"""A 404 from OpenCorporates' company endpoint is "no record", not "did not answer".

Found by the weekly source-health check of 20 Oct 2026, reproducing
``/lookup?lei=529900IQDQ6X3VTMWN63`` ("Kaufland" S.R.L., Moldova) in
production: ``errors: {"opencorporates": "HTTPStatusError: HTTP 404 Not
Found …"}`` and a ``source_fetch`` degradation reading "did not answer, so its
records were not consulted". ``_get`` raised on every non-2xx, so the one
answer that means "nothing here" reached the reader as an outage — while
``_get_optional`` beside it already treated 404 as a definitive answer.

Behind it, a second defect: GLEIF's OpenCorporates mapping gives that LEI the
ocid ``md/1.0166E+12`` — IDNO 1016600004811 written in scientific notation
by a spreadsheet — so the 404 was guaranteed. ``repair_ocid`` restores the
number from the subject's own ``registeredAs`` when it rounds to exactly the
mangled value, and declines to ask otherwise.

Pinned here: the 404 is a no-record bundle and is cached as absent with the
TTL; 402/403/429/5xx still raise (could not ask); the pipeline turns the
bundle into no hit and no error, so coverage counts it as "no record"; and
the ocid repair.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from opencheck.cache import Cache
from opencheck.coverage import source_coverage
from opencheck.routers.lookup import _build_result_hit, _LookupCtx
from opencheck.sources.opencorporates import (
    _API_BASE,
    COVERAGE_404,
    OpenCorporatesAdapter,
    repair_ocid,
)

OCID = "md/1016600004811"
COMPANY_URL = f"{_API_BASE}/companies/md/1016600004811"
NETWORK_URL = f"{COMPANY_URL}/network"


@pytest.fixture(autouse=True)
def _live(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCORPORATES_API_KEY", "test-key")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    from opencheck.config import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# The adapter
# ---------------------------------------------------------------------------


@respx.mock
async def test_a_company_404_is_a_no_record_bundle_not_an_exception():
    company = respx.get(COMPANY_URL).mock(return_value=httpx.Response(404))
    network = respx.get(NETWORK_URL).mock(return_value=httpx.Response(200, json={}))

    bundle = await OpenCorporatesAdapter().fetch(OCID)

    assert bundle["not_found"] is True
    assert bundle["is_stub"] is False
    assert bundle["coverage_note"] == COVERAGE_404
    assert company.call_count == 1
    # No company, so nothing to ask the network endpoint about.
    assert network.call_count == 0


@respx.mock
async def test_the_404_is_cached_as_absent_and_asked_once():
    company = respx.get(COMPANY_URL).mock(return_value=httpx.Response(404))
    adapter = OpenCorporatesAdapter()

    for _ in range(2):
        assert (await adapter.fetch(OCID))["not_found"] is True

    assert company.call_count == 1
    cached = Cache().get_payload(next(iter(_company_cache_keys(adapter))))
    assert cached is not None and cached[0] is None


def _company_cache_keys(adapter: OpenCorporatesAdapter):
    from opencheck.sources.opencorporates import _CACHE_NS, _slug

    yield f"{_CACHE_NS}/company/{_slug(OCID)}"


@pytest.mark.parametrize("status", [402, 403, 429, 500, 503])
@respx.mock
async def test_could_not_ask_still_raises_and_is_not_cached(status):
    respx.get(COMPANY_URL).mock(return_value=httpx.Response(status))
    adapter = OpenCorporatesAdapter()

    with pytest.raises(httpx.HTTPStatusError):
        await adapter.fetch(OCID)

    assert Cache().get_payload(next(_company_cache_keys(adapter))) is None


@respx.mock
async def test_a_network_error_still_raises():
    respx.get(COMPANY_URL).mock(side_effect=httpx.ConnectError("down"))
    with pytest.raises(httpx.ConnectError):
        await OpenCorporatesAdapter().fetch(OCID)


async def test_offline_with_a_cached_absence_is_still_no_record(monkeypatch):
    Cache().put_absent(next(_company_cache_keys(OpenCorporatesAdapter())))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    from opencheck.config import get_settings

    get_settings.cache_clear()
    bundle = await OpenCorporatesAdapter().fetch(OCID)
    assert bundle.get("not_found") is True


# ---------------------------------------------------------------------------
# The pipeline: no hit, no error — "answered with no record"
# ---------------------------------------------------------------------------


def test_the_no_record_bundle_builds_no_hit():
    ctx = _LookupCtx(lei="529900IQDQ6X3VTMWN63", legal_name='"Kaufland" S.R.L.')
    ctx.ocid = OCID
    bundle = {
        "source_id": "opencorporates",
        "hit_id": OCID,
        "ocid": OCID,
        "is_stub": False,
        "not_found": True,
        "coverage_note": COVERAGE_404,
    }
    assert _build_result_hit("opencorporates", bundle, ctx) is None


def test_coverage_counts_it_as_answered_with_no_record():
    cov = source_coverage(hits=[], errors={}, sources_applicable=["opencorporates"])
    assert "opencorporates" in cov["no_record_ids"]
    assert "opencorporates" not in cov["failed_ids"]
    # The production shape before the fix: the same 404 sat in ``errors``.
    before = source_coverage(
        hits=[],
        errors={"opencorporates": "HTTPStatusError: HTTP 404 Not Found"},
        sources_applicable=["opencorporates"],
    )
    assert "opencorporates" in before["failed_ids"]


# ---------------------------------------------------------------------------
# The ocid GLEIF hands over
# ---------------------------------------------------------------------------


def test_the_kaufland_ocid_is_repaired_from_registered_as():
    assert repair_ocid("md/1.0166E+12", "1016600004811") == "md/1016600004811"


@pytest.mark.parametrize(
    ("ocid", "registered_as"),
    [
        ("md/1.0166E+12", "2016600004811"),  # rounds to a different value
        ("md/1.0166E+12", None),  # nothing to repair from
        ("md/1.0166E+12", "MD-1016600004811"),  # not all digits
    ],
)
def test_a_scientific_number_that_cannot_be_restored_is_not_asked_for(
    ocid, registered_as
):
    assert repair_ocid(ocid, registered_as) is None


@pytest.mark.parametrize(
    "ocid", ["gb/00102498", "us_de/1234567", "md/1016600004811", "nl/E12345"]
)
def test_an_ordinary_ocid_is_left_alone(ocid):
    assert repair_ocid(ocid, "00102498") == ocid


def test_no_ocid_is_no_ocid():
    assert repair_ocid(None, "1016600004811") is None
    assert repair_ocid("", "1016600004811") is None


def test_the_lookup_reads_the_ocid_through_the_repair():
    import inspect

    from opencheck.routers import lookup

    src = inspect.getsource(lookup._resolve_ctx)
    assert "repair_ocid(" in src
