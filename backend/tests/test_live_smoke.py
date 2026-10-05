"""Opt-in live smoke tests — hit real, open APIs to catch API-shape drift.

These are the lightweight alternative to recorded cassettes: instead of baking
real payloads (with their PII, secrets and licence restrictions) into the repo,
a handful of tests hit the *current* live API and assert it still parses and
maps to valid BODS. Nothing is recorded or committed.

Scope is deliberately limited to **open, key-free, low-sensitivity** sources:

- **GLEIF** — public, CC0, no API key, legal-entity reference data.
- **Wikidata** — public, CC0, no API key.
- **Malta Business Registry** — public, CC BY 4.0, no API key (EU HVD).
- **Brazil CNPJ** (Receita Federal) — public open data, no API key (OpenCNPJ / BrasilAPI).
- **New Zealand Companies Register (NZBN)** — CC BY 4.0, but *key-gated*: this
  one runs only when ``NZBN_API_KEY`` is set, otherwise it skips.

- **New Zealand sandbox (NZBN + Entity Role Search)** — MBIE's sandbox, which
  carries the Companies (Address Information) Amendment Act 2025 address shape
  ahead of its 18 Nov 2026 production release. Key-gated on the *separate*
  sandbox subscriptions (``NZBN_SANDBOX_API_KEY`` /
  ``NZBN_ROLE_SEARCH_SANDBOX_API_KEY``) and skipped without them.

Licence-restricted (OpenSanctions CC-BY-NC, OpenCorporates) and PII-heavy
sources are intentionally excluded. The NZBN smoke test is the one key-gated
exception (the key is free and the data is CC BY 4.0), and it skips cleanly
when no key is configured.

Skipped by default. Run with::

    pytest --run-live -m live            # just the live smoke tier
    OPENCHECK_RUN_LIVE=1 pytest -m live
"""

from __future__ import annotations

import pytest

from opencheck.bods.mapper import (
    map_acra_singapore,
    map_cnpj_brazil,
    map_gleif,
    map_cr_hongkong,
    map_malta_mbr,
    map_nz_companies,
    map_wikidata,
)
from opencheck.bods.validator import validate_shape
from opencheck.config import get_settings
from opencheck.sources import REGISTRY, SearchKind

pytestmark = pytest.mark.live


@pytest.fixture(autouse=True)
def _live_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# --- GLEIF (public, CC0, no key) ---------------------------------------------


def test_gleif_is_key_free_and_live():
    info = REGISTRY["gleif"].info
    assert info.requires_api_key is False
    assert info.live_available is True


async def test_gleif_search_then_fetch_maps_to_valid_bods():
    """Search resolves a real LEI, the Level-1/Level-2 fetch shape still
    validates, and it maps to valid BODS. No LEI is hardcoded (so the test can't
    rot if one company's record lapses) — whatever the live search returns is used."""
    adapter = REGISTRY["gleif"]
    hits = await adapter.search("Apple Inc", SearchKind.ENTITY)
    assert hits, "GLEIF search returned nothing — API shape changed?"
    leis = [h.identifiers.get("lei", "") for h in hits]
    assert any(len(x) == 20 for x in leis), f"no 20-char LEI in hits: {leis[:3]}"

    lei = next(x for x in leis if len(x) == 20)
    bundle = await adapter.fetch(lei)
    assert bundle["lei"] == lei
    assert bundle.get("record"), "GLEIF Level-1 record missing — API shape changed?"

    bods = list(map_gleif(bundle))
    assert bods, "GLEIF bundle produced no BODS statements"
    assert validate_shape(bods) == []
    assert any(s["recordType"] == "entity" for s in bods), "no entity statement"


# --- Wikidata (public, CC0, no key) ------------------------------------------


def test_wikidata_is_key_free_and_live():
    info = REGISTRY["wikidata"].info
    assert info.requires_api_key is False
    assert info.live_available is True


async def test_wikidata_search_then_fetch_maps_to_valid_bods():
    adapter = REGISTRY["wikidata"]
    hits = await adapter.search("Unilever", SearchKind.ENTITY)
    assert hits, "Wikidata search returned nothing — API shape changed?"

    qid = hits[0].identifiers.get("wikidata_qid") or hits[0].hit_id
    assert qid.startswith("Q"), f"unexpected Wikidata id shape: {qid!r}"

    bundle = await adapter.fetch(qid)
    bods = list(map_wikidata(bundle))
    # A sparse entity may yield no BODS, but whatever is produced must be valid.
    assert validate_shape(bods) == []


# --- Malta Business Registry (public, CC BY 4.0, no key) ----------------------


def test_malta_mbr_is_key_free_and_live():
    info = REGISTRY["malta_mbr"].info
    assert info.requires_api_key is False
    assert info.live_available is True


async def test_malta_mbr_live_fetch_maps_to_valid_bods():
    """Fetch a real company from the live MBR Open Data API and confirm the
    response still parses and maps to valid BODS.

    This is also the production access check: if the MBR endpoint starts
    rejecting non-browser clients (WAF / IP block) the fetch returns a stub
    and this test fails loudly. A few stable registration numbers are tried so
    one company being purged can't rot the test (struck-off companies normally
    remain queryable, so this is belt-and-braces)."""
    adapter = REGISTRY["malta_mbr"]

    bundle = None
    for reg in ("C 113927", "C 1", "C 100", "C 1000"):
        b = await adapter.fetch(reg, legal_name="")
        if not b.get("is_stub") and (b.get("company") or {}).get("name"):
            bundle = b
            break

    assert bundle is not None, (
        "Malta MBR live fetch returned no parseable record — the API shape or "
        "access policy (e.g. a WAF blocking non-browser clients) may have changed"
    )

    company = bundle["company"]
    # Fields the adapter/mapper rely on.
    assert company.get("registration_number"), "no registration_number in live record"

    bods = list(map_malta_mbr(bundle))
    assert bods, "Malta MBR bundle produced no BODS statements"
    assert validate_shape(bods) == []
    assert any(s["recordType"] == "entity" for s in bods), "no entity statement"


# --- Singapore ACRA (data.gov.sg, key optional) -------------------------------


def test_acra_singapore_is_key_free_and_live():
    info = REGISTRY["acra_singapore"].info
    assert info.requires_api_key is False
    assert info.live_available is True


async def test_acra_singapore_live_fetch_maps_to_valid_bods():
    """Fetch DBS Bank from data.gov.sg with GLEIF's legal name and confirm both
    rows arrive — the UEN row and the collection-2 detail from the 'D' file —
    and map to valid BODS.

    A stub here, with no degradation, means the collection metadata stopped
    naming the datasets the way the adapter resolves them."""
    adapter = REGISTRY["acra_singapore"]
    bundle = await adapter.fetch("196800306E", legal_name="DBS BANK LTD.")
    assert not bundle.get("is_stub"), (
        "ACRA live fetch returned no record — the collection metadata, the "
        "datastore_search shape or the rate limit may have changed"
    )
    assert bundle["entity"]["uen"] == "196800306E"
    assert bundle["detail"] and bundle["detail"]["entity_name"] == "DBS BANK LTD."

    bods = list(map_acra_singapore(bundle))
    assert len(bods) == 1
    assert validate_shape(bods) == []
    assert bods[0]["recordDetails"]["identifiers"][0]["scheme"] == "SG-ACRA"


# --- Hong Kong Companies Registry (DATA.GOV.HK, no key) -----------------------


def test_cr_hongkong_is_key_free_and_live():
    info = REGISTRY["cr_hongkong"].info
    assert info.requires_api_key is False
    assert info.live_available is True


async def test_cr_hongkong_live_fetch_maps_to_valid_bods():
    """Fetch HSBC from the live Companies Registry API, with GLEIF's legal name,
    and confirm it parses, passes the name check and maps to valid BODS.

    Also the access check: the API sits behind CloudFront, which refuses an
    empty User-Agent with 403 — a tightening of that filter would surface here
    as a stub rather than a record."""
    adapter = REGISTRY["cr_hongkong"]
    bundle = await adapter.fetch(
        "00173611", legal_name="HONGKONG AND SHANGHAI BANKING CORPORATION LIMITED -THE-"
    )
    assert not bundle.get("is_stub"), (
        "Companies Registry live fetch returned no record — the API shape, access "
        "policy or the name check may have changed"
    )
    assert bundle["company"].get("Brn") == "00173611"

    bods = list(map_cr_hongkong(bundle))
    assert len(bods) == 1
    assert validate_shape(bods) == []
    assert bods[0]["recordDetails"]["identifiers"][0]["scheme"] == "HK-BRN"


# --- Brazil CNPJ (public open data, key-free) --------------------------------


def test_cnpj_brazil_is_key_free_and_live():
    info = REGISTRY["cnpj_brazil"].info
    assert info.requires_api_key is False
    assert info.live_available is True


async def test_cnpj_brazil_live_fetch_maps_to_valid_bods():
    """Fetch a real company from the live CNPJ providers (OpenCNPJ primary,
    BrasilAPI fallback) and confirm the response still parses, includes the QSA,
    and maps to valid BODS with ownership/control relationships. Also a
    production access check for both providers.

    Uses Petrobras (a stable, long-lived CNPJ) so the test can't rot easily."""
    adapter = REGISTRY["cnpj_brazil"]
    bundle = await adapter.fetch("33000167000101", legal_name="")

    assert not bundle.get("is_stub"), (
        "CNPJ Brazil live fetch returned a stub — both OpenCNPJ and BrasilAPI "
        "may be unreachable or have changed shape/access"
    )
    company = bundle.get("company") or {}
    assert company.get("name"), "no company name in live record"
    assert bundle.get("partners"), "no QSA partners parsed — provider shape changed?"

    bods = list(map_cnpj_brazil(bundle))
    assert bods, "CNPJ Brazil bundle produced no BODS statements"
    assert validate_shape(bods) == []
    assert any(s["recordType"] == "entity" for s in bods), "no entity statement"
    assert any(s["recordType"] == "relationship" for s in bods), (
        "no ownership/control relationship from the QSA"
    )


# --- New Zealand Companies Register / NZBN (CC BY 4.0, key-gated) -------------


def test_nz_companies_requires_a_key():
    assert REGISTRY["nz_companies"].info.requires_api_key is True


async def test_nz_companies_live_fetch_maps_to_valid_bods():
    """Resolve a real company number → NZBN, fetch the live FullEntity, and
    confirm it still parses and maps to valid BODS. Skipped unless
    ``NZBN_API_KEY`` is set. Also the production access check: if the NZBN API
    changes shape or rejects the key, the fetch returns a stub and this fails.

    Uses Fonterra Co-operative Group (company number 1166320) — a stable,
    long-lived NZ company — so the test can't rot easily."""
    if not get_settings().nzbn_api_key:
        pytest.skip("NZBN_API_KEY not set — skipping NZ live smoke test")

    adapter = REGISTRY["nz_companies"]
    bundle = await adapter.fetch("1166320", legal_name="")

    assert not bundle.get("is_stub"), (
        "NZ live fetch returned a stub — the NZBN API key, access policy or "
        "response shape may have changed"
    )
    assert bundle.get("nzbn"), "company number did not resolve to an NZBN"
    company = bundle.get("company") or {}
    assert company.get("name"), "no company name in live record"

    bods = list(map_nz_companies(bundle))
    assert bods, "NZ bundle produced no BODS statements"
    assert validate_shape(bods) == []
    assert any(s["recordType"] == "entity" for s in bods), "no entity statement"


# --- TED — Tenders Electronic Daily (EU open data, no key) --------------------


def test_ted_eu_is_key_free_and_live():
    info = REGISTRY["ted_eu"].info
    assert info.requires_api_key is False
    assert info.live_available is True


async def test_ted_eu_live_search_confirms_orange_wins():
    """Query the live TED Search API with Orange SA's SIREN (the identifier
    OpenCheck derives from GLEIF ``registeredAs``) and confirm the API shape
    and the XML winner chain still parse. Orange had 5 award notices at
    investigation time (2026-08-03) — assert a floor of 3 so the test can't
    rot on notice archival, and assert at least one notice resolves to a
    confirmed role via the eForms XML.

    This is exactly the API-drift risk the live tier exists for: the Search
    API response keys, the expert-search grammar, and the eForms XML
    structure are all outside OpenCheck's control.

    A role can come from either path. Since September 2026 ted.europa.eu's
    AWS WAF challenges GitHub's runners (HTTP 202, empty body), so on CI the
    notice XML is usually unavailable and the role comes from the Search
    API's ``winner-identifier`` field — this test then guards that field. The
    XML parser is pinned offline in test_ted_eu.py."""
    from opencheck.bods import map_ted_eu

    adapter = REGISTRY["ted_eu"]
    bundle = await adapter.fetch_by_identifiers(
        "969500MCOONR8990S771",  # Orange SA LEI (fill rate zero today — see adapter)
        "380129866",  # SIREN, the key that actually matches
        "FR",
        legal_name="ORANGE",
    )

    assert bundle is not None, "TED bundle unexpectedly None in live mode"
    assert bundle["total_notice_count"] >= 3, (
        "TED identifier search returned fewer notices than the investigation "
        "floor — the organisation-identifier-tenderer field or scope semantics "
        "may have changed"
    )
    assert bundle["notices"], "no notices in bundle despite non-zero count"
    assert any(n["confirmed"] for n in bundle["notices"]), (
        "no notice resolved to a confirmed role by either path — the eForms "
        "notice XML (NoticeResult winner chain) and the Search API's "
        "winner-identifier field may both have changed; roles seen: "
        + repr([(n["role"], n["role_basis"]) for n in bundle["notices"]])
    )
    assert all(
        n["role_basis"] in ("notice_xml", "search_index")
        for n in bundle["notices"]
        if n["confirmed"]
    )

    bods = list(map_ted_eu(bundle))
    assert bods, "TED bundle produced no BODS statements"
    assert validate_shape(bods) == []


# --- New Zealand sandbox — the alternative-address shape (key-gated) ----------
#
# The Companies (Address Information) Amendment Act 2025 reaches production on
# 18 November 2026; MBIE's sandbox has carried the new shape since
# 30 September 2026. These tests exist to answer, against real payloads, the
# two questions MBIE's notice left open — whether an alternative address
# carries its own ``pafId``, and whether the Entity Role Search API is affected
# at all — and then to keep failing loudly if either answer changes.
#
# The sandbox needs its own subscription keys: the production keys return 401
# against /sandbox/. The base URLs are derived from the production constants by
# swapping the gateway segment, so they cannot drift apart from the adapters.

_NZ_SANDBOX_DIRECTOR_NZBN = "9429050923540"   # MBIE test entity: a director
_NZ_SANDBOX_DIRECTOR_NZBN_2 = "9429050923557"  # with an alternative address


def _nzbn_sandbox_base() -> str:
    from opencheck.sources.nz_companies import _API_BASE

    return _API_BASE.replace("/gateway/", "/sandbox/")


def _role_search_sandbox_url() -> str:
    from opencheck.nz_associations import _ROLE_SEARCH_URL

    return _ROLE_SEARCH_URL.replace("/gateway/", "/sandbox/")


async def _sandbox_get(url: str, key: str):
    from opencheck.http import build_client

    async with build_client() as client:
        resp = await client.get(url, headers={"Ocp-Apim-Subscription-Key": key})
    assert resp.is_success, (
        f"sandbox returned HTTP {resp.status_code} for {url} — a 401 means the "
        "key is a production key, not a sandbox one (MBIE issues these "
        "separately; subscribe to the Sandbox products in the portal)"
    )
    return resp.json()


async def test_nzbn_sandbox_director_carries_an_alternative_address_type():
    """MBIE's test entity has a director with an alternative address, so the
    sandbox is where we can see `addressType` populated before 18 Nov 2026.

    Asserts the shape *and* that our own selection reads it — `_role_address`
    must return the alternative block and `_norm_roles` must surface
    `address_type`, which is the whole point of Phase 271.
    """
    key = get_settings().nzbn_sandbox_api_key
    if not key:
        pytest.skip("NZBN_SANDBOX_API_KEY not set — skipping NZ sandbox smoke test")

    from opencheck.sources.nz_companies import (
        _address_type,
        _norm_roles,
        _role_address,
    )

    full = await _sandbox_get(
        f"{_nzbn_sandbox_base()}/entities/{_NZ_SANDBOX_DIRECTOR_NZBN}", key
    )
    roles = full.get("roles") or []
    assert roles, "sandbox entity returned no roles"

    seen = {
        _address_type(a)
        for r in roles
        for a in (r.get("roleAddress") or [])
        if isinstance(a, dict)
    }
    assert "ALTERNATIVE" in seen, (
        "no ALTERNATIVE addressType on MBIE's own alternative-address test "
        f"entity — observed types were {sorted(str(x) for x in seen)}. Either "
        "the sandbox data changed or the field is not populated as the "
        "30 Sept 2026 notice described."
    )

    alt_roles = [
        r for r in roles
        if any(_address_type(a) == "ALTERNATIVE"
               for a in (r.get("roleAddress") or []) if isinstance(a, dict))
    ]
    for r in alt_roles:
        # A public viewer sees only the alternative block, so it is also the
        # one `_role_address` must choose.
        types = {_address_type(a) for a in r["roleAddress"] if isinstance(a, dict)}
        if types == {"ALTERNATIVE"}:
            assert _address_type(_role_address(r)) == "ALTERNATIVE"

    normalised = _norm_roles(alt_roles)
    assert normalised, "alternative-address roles normalised to nothing"
    assert any(row.get("address_type") == "ALTERNATIVE" for row in normalised), (
        "address_type did not survive _norm_roles — the Phase 271 passthrough "
        "is broken against real sandbox data"
    )


async def test_nzbn_sandbox_alternative_address_does_not_reuse_the_residential_paf_id():
    """The open question MBIE did not answer: does an alternative address carry
    its own `pafId`?

    The answer we must not get is "it reuses the residential one" — that would
    make a `pafId` match look like a residential match while naming a service
    address. If both block types are visible (an authority holder sees both for
    a director), their `pafId`s must differ.
    """
    key = get_settings().nzbn_sandbox_api_key
    if not key:
        pytest.skip("NZBN_SANDBOX_API_KEY not set — skipping NZ sandbox smoke test")

    from opencheck.sources.nz_companies import _address_type, _paf

    alt_pafs: set[str] = set()
    residential_pafs: set[str] = set()
    for nzbn in (_NZ_SANDBOX_DIRECTOR_NZBN, _NZ_SANDBOX_DIRECTOR_NZBN_2):
        full = await _sandbox_get(f"{_nzbn_sandbox_base()}/entities/{nzbn}", key)
        for r in full.get("roles") or []:
            for a in (r.get("roleAddress") or []):
                if not isinstance(a, dict):
                    continue
                paf = _paf(a)
                if not paf:
                    continue
                # A public viewer sees ALTERNATIVE or null; null *is* the
                # residential address (the pre-Act spelling), so comparing
                # against PHYSICAL would never fire at this access level.
                if _address_type(a) == "ALTERNATIVE":
                    alt_pafs.add(paf)
                else:
                    residential_pafs.add(paf)

    assert alt_pafs or residential_pafs, "no pafId on any sandbox role address"
    overlap = alt_pafs & residential_pafs
    assert not overlap, (
        f"an ALTERNATIVE and a residential address share a pafId ({sorted(overlap)}) "
        "— a pafId match could then name a service address while reading as a "
        "residential one, and _tier() in nz_associations could no longer treat a "
        "pafId match as address corroboration at all"
    )


async def test_role_search_sandbox_still_has_no_address_type():
    """Entity Role Search (v3) is the API `/nz-associations` actually searches,
    and MBIE's notice does not mention it. As of 1 October 2026 its
    `physicalAddress` block has no `addressType` field, so a consumer cannot
    tell which kind of address a record carries.

    This asserts that state deliberately: when MBIE adds the field, this test
    fails and points at the decision it unblocks — whether `_tier()` should
    demote a match on an alternative address, not merely relabel it.
    """
    key = get_settings().nzbn_role_search_sandbox_api_key
    if not key:
        pytest.skip(
            "NZBN_ROLE_SEARCH_SANDBOX_API_KEY not set — skipping Role Search "
            "sandbox smoke test"
        )

    from opencheck.nz_associations import _extract_records

    url = (
        f"{_role_search_sandbox_url()}?name=SMITH%20John&role-type=ALL"
        "&page=0&page-size=20&registered-only=true"
    )
    payload = await _sandbox_get(url, key)
    records = _extract_records(payload)
    assert records, "Role Search sandbox returned no parseable records"

    keys: set[str] = set()
    for rec in records:
        phys = rec.get("physicalAddress")
        if isinstance(phys, dict):
            keys |= set(phys.keys())
    assert keys, "no physicalAddress block on any Role Search record"
    assert "addressType" not in keys, (
        "Entity Role Search now returns an addressType "
        f"(physicalAddress keys: {sorted(keys)}). That answers the question put "
        "to MBIE on 1 Oct 2026: decide whether _tier() should demote a match "
        "on an ALTERNATIVE address rather than only relabel its basis, and "
        "update docs/nz-associations.md."
    )


async def test_role_search_sandbox_does_not_serve_the_alternative_address():
    """The question MBIE's notice left open, answered against real payloads.

    For MBIE's own alternative-address test entities, the `pafId` that Entity
    Role Search returns for the director does **not** match the `pafId` on the
    NZBN `ALTERNATIVE` block. In other words Role Search serves a *different*
    address — on 1 October 2026, in the sandbox, it is not the alternative one.

    Two things follow, and both matter enough to pin:

    * for `/nz-associations`, the subject's NZBN address and the Role Search
      address will disagree for any director who elects an alternative address,
      so genuine matches fall from high/medium to name-only — the false-negative
      branch in `docs/nz-associations.md`;
    * more seriously, if the address Role Search keeps serving is the
      residential one, the Act's protection does not hold across MBIE's own
      APIs. That is the question put to MBIE, and this test is the evidence.

    When this fails, Role Search has started serving the alternative address:
    the false-*positive* branch is then live (every client of one agent shares a
    `pafId`) and `_tier()` needs revisiting, not just the basis wording.
    """
    nzbn_key = get_settings().nzbn_sandbox_api_key
    rs_key = get_settings().nzbn_role_search_sandbox_api_key
    if not (nzbn_key and rs_key):
        pytest.skip("both NZ sandbox keys required — skipping cross-API check")

    from opencheck.nz_associations import _extract_records, _paf_of
    from opencheck.sources.nz_companies import _address_type, _paf

    # The alternative-address pafIds, and the companies they sit on.
    alt_pafs: set[str] = set()
    company_names: set[str] = set()
    for nzbn in (_NZ_SANDBOX_DIRECTOR_NZBN, _NZ_SANDBOX_DIRECTOR_NZBN_2):
        full = await _sandbox_get(f"{_nzbn_sandbox_base()}/entities/{nzbn}", nzbn_key)
        if full.get("entityName"):
            company_names.add(str(full["entityName"]))
        for r in full.get("roles") or []:
            for a in (r.get("roleAddress") or []):
                if isinstance(a, dict) and _address_type(a) == "ALTERNATIVE" and _paf(a):
                    alt_pafs.add(_paf(a))  # type: ignore[arg-type]
    if not alt_pafs:
        pytest.skip("no ALTERNATIVE block carried a pafId — nothing to compare")
    assert company_names, "sandbox entities returned no entityName"

    # What Role Search serves for that director at those same companies.
    rs_pafs: set[str] = set()
    for page in range(7):
        url = (
            f"{_role_search_sandbox_url()}?name=LASTNAME%20Firstname&role-type=ALL"
            f"&page={page}&page-size=50&registered-only=true"
        )
        records = _extract_records(await _sandbox_get(url, rs_key))
        if not records:
            break
        for rec in records:
            names = {rec.get("associatedCompanyName")} | {
                s.get("associatedCompanyName") for s in (rec.get("shareholdings") or [])
            }
            if names & company_names:
                paf = _paf_of(rec.get("physicalAddress"))
                if paf:
                    rs_pafs.add(paf)
    if not rs_pafs:
        pytest.skip(
            "Role Search returned no pafId for MBIE's test entities — the "
            "sandbox fixture data may have changed"
        )

    assert not (alt_pafs & rs_pafs), (
        "Entity Role Search now serves the alternative address "
        f"(shared pafId {sorted(alt_pafs & rs_pafs)}). The false-positive branch "
        "is live: every client of one accountant or agent now shares a pafId, so "
        "unrelated directors will grade as address-matched and sort to the top of "
        "the panel. Revisit _tier() in nz_associations — relabelling the basis is "
        "no longer enough — and update docs/nz-associations.md."
    )


# --- OpenAleph GLEIF mirror (Phase 286; public, no key) ----------------------


async def test_openaleph_gleif_mirror_still_keyed_as_gleif():
    """OpenAleph drops its republished GLEIF Concatenated Data File by the
    collection's ``foreign_id``. If OpenAleph renames that collection, the
    mirror slips back in silently and every subject it knows only from GLEIF
    (NIPPON SUISAN (U.S.A.), INC, 549300I5BCFMO2W0QI94) counts OpenAleph as a
    second answering source again. This pins the key the rule depends on."""
    import httpx

    from opencheck.sources.openaleph import _API_BASE, _OA_USER_AGENT, _is_mirror

    lei = "549300I5BCFMO2W0QI94"
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(
            f"{_API_BASE}/entities",
            params={"filter:properties.leiCode": lei, "limit": 10},
            headers={"User-Agent": _OA_USER_AGENT},
        )
    response.raise_for_status()
    results = response.json().get("results") or []
    if not results:
        pytest.skip(f"OpenAleph no longer indexes {lei} at all — pick another LEI")
    assert any(_is_mirror(item) for item in results), (
        "OpenAleph's GLEIF record for this LEI is no longer recognised as the "
        "mirror. Collections seen: "
        f"{sorted({str((i.get('collection') or {}).get('foreign_id')) for i in results})}. "
        "Update _MIRROR_COLLECTIONS in sources/openaleph.py."
    )
