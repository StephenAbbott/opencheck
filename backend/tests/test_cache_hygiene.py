"""Phase 262 — cache hygiene.

Two defects from the Opus 5.5 project check (C-M2, C-M1):

1. **A transient upstream failure was remembered as "no record".** ARES cached
   any ``HTTPStatusError`` on its VR endpoint — 500, 503 and 429 included — as
   ``None``, and then cached the bundle built without VR data. OpenCorporates'
   ``_get_optional`` cached ``None`` on any exception at all. Neither entry
   expired, so one register outage made a company read as "found, nothing
   filed" until the next deploy wiped the cache directory. Now only a 404 (and,
   for OpenCorporates, a 402/403 tier refusal) is cached as absent, with a TTL;
   anything else is recorded as a degradation and not cached.

2. **INPI beneficial-owner rows reached the raw payload.** The adapter returned
   the RNE ``company`` payload unchanged and BO rows were dropped only in the
   mapper — but ``/deepen`` republishes the raw bundle in the Data drawer,
   which Loi Sapin II / décret 2017-1094 forbids. They are now stripped before
   the payload is cached and again whenever a bundle is built.
"""

from __future__ import annotations

import json
import time
from typing import Any

import httpx
import pytest
import respx

from opencheck import degradation
from opencheck.cache import ABSENT_TTL_DAYS, Cache
from opencheck.sources.ares import AresAdapter
from opencheck.sources.inpi import InpiAdapter, strip_beneficial_owners
from opencheck.sources.opencorporates import OpenCorporatesAdapter

ARES = "https://ares.gov.cz/ekonomicke-subjekty-v-be/rest"
ICO = "27082440"
AGG_URL = f"{ARES}/ekonomicke-subjekty/{ICO}"
VR_URL = f"{ARES}/ekonomicke-subjekty-vr/{ICO}"

AGGREGATE = {
    "ico": ICO,
    "obchodniJmeno": "Alza.cz a.s.",
    "sidlo": {"textovaAdresa": "Jankovcova 1522/53, Praha 7"},
    "pravniForma": "121",
    "datumVzniku": "2003-08-26",
}
VR = {
    "zaznamy": [
        {
            "statutarniOrgany": [
                {
                    "nazevOrganu": "Představenstvo",
                    "clenoveOrganu": [
                        {
                            "fyzickaOsoba": {"jmeno": "JAN", "prijmeni": "NOVAK"},
                            "clenstvi": {"funkce": {"nazev": "předseda"}},
                        }
                    ],
                }
            ]
        }
    ]
}


@pytest.fixture(autouse=True)
def _live(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("OPENCORPORATES_API_KEY", "test-key")
    monkeypatch.setenv("INPI_USERNAME", "user")
    monkeypatch.setenv("INPI_PASSWORD", "pass")
    from opencheck.config import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _age_entry(key: str, days: float) -> None:
    """Backdate a live cache entry's ``_cached_at`` by ``days``."""
    path = Cache()._live() / f"{key}.json"
    wrapper = json.loads(path.read_text())
    wrapper["_cached_at"] = time.time() - days * 86_400
    path.write_text(json.dumps(wrapper))


# ---------------------------------------------------------------------------
# The cache's absence TTL
# ---------------------------------------------------------------------------


def test_a_cached_absence_expires_after_the_ttl():
    cache = Cache()
    cache.put_absent("x/absent")
    assert cache.get_payload("x/absent") == (None, "live")
    _age_entry("x/absent", ABSENT_TTL_DAYS + 0.1)
    assert cache.get_payload("x/absent") is None


def test_the_absence_ttl_applies_even_with_a_longer_max_age():
    cache = Cache()
    cache.put_absent("x/absent")
    _age_entry("x/absent", ABSENT_TTL_DAYS + 0.1)
    assert cache.get_payload("x/absent", max_age_days=365) is None


def test_a_cached_record_is_not_subject_to_the_absence_ttl():
    cache = Cache()
    cache.put("x/present", {"name": "kept"})
    _age_entry("x/present", ABSENT_TTL_DAYS * 10)
    assert cache.get_payload("x/present") == ({"name": "kept"}, "live")


# ---------------------------------------------------------------------------
# ARES — the VR endpoint
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (500, degradation.REASON_UPSTREAM_ERROR),
        (503, degradation.REASON_UPSTREAM_ERROR),
        (429, degradation.REASON_RATE_LIMITED),
    ],
)
@respx.mock
async def test_ares_vr_failure_is_not_cached_and_is_a_degradation(status, reason):
    respx.get(AGG_URL).mock(return_value=httpx.Response(200, json=AGGREGATE))
    vr = respx.get(VR_URL).mock(return_value=httpx.Response(status))

    with degradation.recording() as recorded:
        bundle = await AresAdapter().fetch(ICO)

    assert bundle["is_stub"] is False
    assert bundle["directors"] == []
    assert [(d.source_id, d.reason) for d in recorded] == [("ares", reason)]
    cache = Cache()
    assert cache.get_payload(f"ares/vr/{ICO}") is None
    assert cache.get_payload(f"ares/bundle-v2/{ICO}") is None

    # The register recovers: the next lookup asks again and gets the board.
    vr.mock(return_value=httpx.Response(200, json=VR))
    bundle = await AresAdapter().fetch(ICO)
    assert [d["name"] for d in bundle["directors"]] == ["JAN NOVAK"]
    assert vr.call_count == 2


@respx.mock
async def test_ares_vr_network_error_is_not_cached():
    respx.get(AGG_URL).mock(return_value=httpx.Response(200, json=AGGREGATE))
    respx.get(VR_URL).mock(side_effect=httpx.ConnectTimeout("slow"))

    with degradation.recording() as recorded:
        await AresAdapter().fetch(ICO)

    assert [(d.source_id, d.reason) for d in recorded] == [
        ("ares", degradation.REASON_TIMEOUT)
    ]
    assert Cache().get_payload(f"ares/vr/{ICO}") is None
    assert Cache().get_payload(f"ares/bundle-v2/{ICO}") is None


@respx.mock
async def test_ares_vr_404_is_cached_as_absent_with_a_ttl():
    respx.get(AGG_URL).mock(return_value=httpx.Response(200, json=AGGREGATE))
    vr = respx.get(VR_URL).mock(return_value=httpx.Response(404))

    with degradation.recording() as recorded:
        await AresAdapter().fetch(ICO)
        await AresAdapter().fetch(ICO)

    assert recorded == []  # not in the commercial register is an answer
    assert vr.call_count == 1  # remembered inside the TTL
    assert Cache().get_payload(f"ares/vr/{ICO}") == (None, "live")

    # Past the TTL the register is asked again — and the company has since
    # been entered in the commercial register.
    _age_entry(f"ares/vr/{ICO}", ABSENT_TTL_DAYS + 0.1)
    vr.mock(return_value=httpx.Response(200, json=VR))
    bundle = await AresAdapter().fetch(ICO)
    assert vr.call_count == 2
    assert [d["name"] for d in bundle["directors"]] == ["JAN NOVAK"]


@respx.mock
async def test_ares_aggregate_5xx_is_a_degradation_not_a_quiet_stub():
    respx.get(AGG_URL).mock(return_value=httpx.Response(503))

    with degradation.recording() as recorded:
        bundle = await AresAdapter().fetch(ICO)

    assert bundle["is_stub"] is True
    assert [(d.source_id, d.check) for d in recorded] == [
        ("ares", degradation.CHECK_SOURCE_FETCH)
    ]
    assert Cache().get_payload(f"ares/aggregate/{ICO}") is None


@respx.mock
async def test_ares_aggregate_404_is_not_a_degradation():
    respx.get(AGG_URL).mock(return_value=httpx.Response(404))
    with degradation.recording() as recorded:
        bundle = await AresAdapter().fetch(ICO)
    assert bundle["is_stub"] is True
    assert recorded == []


@respx.mock
async def test_a_pre_phase_262_ares_bundle_is_never_read():
    """Bundles under the old key may have been built during a VR outage."""
    Cache().put(f"ares/bundle/{ICO}", {"poisoned": True, "directors": []})
    respx.get(AGG_URL).mock(return_value=httpx.Response(200, json=AGGREGATE))
    respx.get(VR_URL).mock(return_value=httpx.Response(200, json=VR))
    bundle = await AresAdapter().fetch(ICO)
    assert "poisoned" not in bundle
    assert [d["name"] for d in bundle["directors"]] == ["JAN NOVAK"]


# ---------------------------------------------------------------------------
# OpenCorporates — the optional network endpoint
# ---------------------------------------------------------------------------



def _oc_url() -> str:
    from opencheck.sources import opencorporates

    return f"{opencorporates._API_BASE}/companies/gb/00102498/network"


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (500, degradation.REASON_UPSTREAM_ERROR),
        (502, degradation.REASON_UPSTREAM_ERROR),
        (429, degradation.REASON_RATE_LIMITED),
    ],
)
@respx.mock
async def test_oc_optional_failure_is_not_cached(status, reason):
    route = respx.get(_oc_url()).mock(return_value=httpx.Response(status))
    adapter = OpenCorporatesAdapter()

    with degradation.recording() as recorded:
        got = await adapter._get_optional(
            "/companies/gb/00102498/network", cache_key="oc-test/network"
        )

    assert got is None
    assert [(d.source_id, d.reason) for d in recorded] == [("opencorporates", reason)]
    assert Cache().get_payload("oc-test/network") is None

    route.mock(return_value=httpx.Response(200, json={"results": {"x": 1}}))
    got = await adapter._get_optional(
        "/companies/gb/00102498/network", cache_key="oc-test/network"
    )
    assert got == {"results": {"x": 1}}


@respx.mock
async def test_oc_optional_network_error_is_not_cached():
    respx.get(_oc_url()).mock(side_effect=httpx.ConnectError("down"))
    with degradation.recording() as recorded:
        got = await OpenCorporatesAdapter()._get_optional(
            "/companies/gb/00102498/network", cache_key="oc-test/network"
        )
    assert got is None
    assert len(recorded) == 1
    assert Cache().get_payload("oc-test/network") is None


@pytest.mark.parametrize("status", [402, 403, 404])
@respx.mock
async def test_oc_definitive_absence_is_cached_with_a_ttl(status):
    route = respx.get(_oc_url()).mock(return_value=httpx.Response(status))
    adapter = OpenCorporatesAdapter()
    with degradation.recording() as recorded:
        for _ in range(2):
            got = await adapter._get_optional(
                "/companies/gb/00102498/network", cache_key="oc-test/network"
            )
            assert got is None
    assert recorded == []
    assert route.call_count == 1
    _age_entry("oc-test/network", ABSENT_TTL_DAYS + 0.1)
    await adapter._get_optional(
        "/companies/gb/00102498/network", cache_key="oc-test/network"
    )
    assert route.call_count == 2


# ---------------------------------------------------------------------------
# INPI — beneficial-owner rows never reach the raw payload
# ---------------------------------------------------------------------------

INPI_AUTH = "https://registre-national-entreprises.inpi.fr/api/sso/login"
SIREN = "832434856"
INPI_COMPANY = f"https://registre-national-entreprises.inpi.fr/api/companies/{SIREN}"


def _pouvoir(nom: str, bo: Any) -> dict[str, Any]:
    return {
        "typeDePersonne": "INDIVIDU",
        "beneficiaireEffectif": bo,
        "individu": {"descriptionPersonne": {"nom": nom, "prenoms": ["A"], "roleEntreprise": 30}},
    }


def _rne_payload() -> dict[str, Any]:
    return {
        "siren": SIREN,
        "diffusionINSEE": "O",
        "identite": {"entreprise": {"denomination": "LEGO FRANCE"}},
        "formality": {
            "content": {
                "personneMorale": {
                    "identite": {"entreprise": {"denomination": "LEGO FRANCE"}},
                    "composition": {
                        "pouvoirs": [
                            _pouvoir("DIRECTOR", False),
                            _pouvoir("OWNERONE", True),
                            _pouvoir("OWNERTWO", "true"),
                        ]
                    },
                    "beneficiairesEffectifs": [{"nom": "OWNERTHREE"}],
                },
            },
            "historique": [
                {"composition": {"pouvoirs": [_pouvoir("OWNERFOUR", True)]}}
            ],
        },
    }


def _contains_bo(value: Any) -> bool:
    text = json.dumps(value)
    return (
        "OWNER" in text
        or '"beneficiaireEffectif": true' in text
        or '"beneficiaireEffectif": "true"' in text
        or "beneficiairesEffectifs" in text
    )


def test_strip_removes_every_bo_row_and_keeps_officers():
    stripped = strip_beneficial_owners(_rne_payload())
    assert not _contains_bo(stripped)
    pouvoirs = stripped["formality"]["content"]["personneMorale"]["composition"]["pouvoirs"]
    assert [p["individu"]["descriptionPersonne"]["nom"] for p in pouvoirs] == ["DIRECTOR"]
    assert strip_beneficial_owners(stripped) == stripped  # idempotent


def test_strip_does_not_mutate_its_input():
    payload = _rne_payload()
    strip_beneficial_owners(payload)
    assert _contains_bo(payload)


@respx.mock
async def test_a_live_inpi_fetch_neither_returns_nor_caches_bo_rows():
    respx.post(INPI_AUTH).mock(return_value=httpx.Response(200, json={"token": "t"}))
    respx.get(INPI_COMPANY).mock(return_value=httpx.Response(200, json=_rne_payload()))

    bundle = await InpiAdapter().fetch(SIREN)

    assert bundle["is_stub"] is False
    assert not _contains_bo(bundle)
    on_disk = (Cache()._live() / f"inpi/company/{SIREN}.json").read_text()
    assert "OWNER" not in on_disk


async def test_an_inpi_entry_cached_before_phase_262_is_stripped_on_read():
    Cache().put(f"inpi/company/{SIREN}", _rne_payload())  # as an old cache holds it
    bundle = await InpiAdapter().fetch(SIREN)
    assert not _contains_bo(bundle)


async def test_the_inpi_mapper_still_emits_the_non_bo_officer():
    from opencheck.bods.mapper import map_inpi

    Cache().put(f"inpi/company/{SIREN}", _rne_payload())
    bundle = await InpiAdapter().fetch(SIREN)
    names = [
        n.get("fullName")
        for s in map_inpi(bundle)
        if s.get("recordType") == "person"
        for n in s["recordDetails"].get("names", [])
    ]
    assert names and all("OWNER" not in (n or "") for n in names)
