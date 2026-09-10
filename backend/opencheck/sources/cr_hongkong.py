"""Hong Kong Companies Registry adapter — live local companies.

The Companies Registry publishes *Registered Office Address of Live Local
Companies* on DATA.GOV.HK as a free, key-less JSON API, updated daily:

* ``GET /cr/api/api/v1/api_builder/json/local/search`` with
  ``query[0][key1]`` (``Brn`` or ``Comp_name``), ``query[0][key2]`` (the
  operator) and ``query[0][key3]`` (the value).

Seven fields come back per company and nothing else: ``Brn``,
``Chinese_Company_Name``, ``English_Company_Name``,
``Address_of_Registered_Office``, ``Company_Type``, ``Date_of_Incorporation``
and ``Re-domiciliation_Date``. **No officers, secretaries, shareholders or
beneficial owners** — those sit behind the Registry's e-Services portal, whose
terms do not permit reuse — so the BODS mapping is a single entity statement.

The dataset lists **live** companies only: there is no status field, and none
of twelve companies GLEIF records as ``INACTIVE`` returned a record when
probed on 2026-09-10. Presence is therefore a liveness reading; absence is
"not in the live register" and nothing more — it may equally be a dissolved
company, a limited partnership fund (never in this dataset) or a GLEIF record
carrying the wrong number, so it is never turned into a signal.

The identifier
--------------

``Brn`` is the Business Registration Number, which the Registrar adopted as
the **Unique Business Identifier** (phase one 1 November 2021 for limited
partnership funds, phase two 27 December 2023 for companies). Companies that
never had a BRN were given a *dummy* one — the old Company Registration
Number with a letter prefix, e.g. ``C1572528``. OpenCheck labels it
``HK-BRN``; it is **not** the old seven-character CR No., which org-id.guide
lists as ``HK-CR``.

The flow with GLEIF
-------------------

GLEIF files Hong Kong companies under two registration authorities, and both
carry the BRN in ``registeredAs`` (600 active HK records sampled, 2026-09-10):
``RA000388`` Companies Registry (63%) and ``RA000389`` Business Registration
Office of the Inland Revenue Department (20%). A few ``RA000389`` records hold
the full 16-digit BR certificate number or a hyphenated form
(``58879114-000-08-25-2``); the BRN is the first eight digits of either.

Because an identifier match alone once pointed at the wrong company — GLEIF's
record for Chery Global Innovations (Hong Kong) Limited carries the BRN of
TMF Secretaries (HK) Limited, its company secretary — ``fetch`` compares the
register's names with the GLEIF legal name and **drops the record** when they
disagree. GLEIF frequently writes the Chinese name in simplified characters
while the register holds traditional ones, so both sides are folded to
simplified (OpenCC ``t2s``) before comparing; without that, 15 of 66 genuine
matches in the live sample failed. The fold runs traditional → simplified, the
many-to-one direction: converting both sides the other way is phrase-sensitive
and turned the register's own ``維克托`` into ``維克託`` while leaving GLEIF's
``维克托`` as ``維克托``, so a true pair still disagreed.

API behaviour that differs from the published data dictionary
-------------------------------------------------------------

All observed live on 2026-09-10:

* A miss is **HTTP 400** ``{"status":400,"message":"No result found."}``, not a
  404. It is a clean answer, not a degradation.
* ``Brn`` accepts ``equal`` only; ``Comp_name`` accepts ``begins_with`` only
  (case-insensitive, English or Chinese, at most 100 rows). The dictionary
  lists both operators for both keys.
* An **empty User-Agent is refused with 403** by the CloudFront layer in front
  of the API; the shared client's ``OpenCheck/…`` UA is accepted.
* ``Chinese_Company_Name`` can be the literal string ``"NULL"``.
* Dates are ``DD-MM-YYYY``.
* The non-Hong Kong companies endpoint (``/json/foreign/search``) rejects every
  name search; it is not used here.

License: DATA.GOV.HK Terms and Conditions of Use — free reuse for commercial
and non-commercial purposes, with attribution to the Government, the Relevant
Organisations and DATA.GOV.HK. https://data.gov.hk/en/terms-and-conditions
"""

from __future__ import annotations

import hashlib
import logging
import re
import unicodedata
from functools import lru_cache
from typing import Any
from urllib.parse import urlencode

from .. import degradation, names
from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.cr_hongkong import CrHongKongBundle

_LOG = logging.getLogger(__name__)

_API_BASE = "https://data.cr.gov.hk/cr/api/api/v1/api_builder/json"
_CACHE_NS = "cr_hongkong"

#: GLEIF Registration Authority codes whose ``registeredAs`` is a Hong Kong
#: BRN. Verified live 2026-09-10 against 600 active HK LEI records:
#: ``RA000388`` Companies Registry, ``RA000389`` Business Registration Office
#: (Inland Revenue Department). ``RA000390`` (Securities and Futures
#: Commission fund codes) and ``RA001123`` (tax-exempt charities) are not.
HK_RA_CODES: frozenset[str] = frozenset({"RA000388", "RA000389"})

#: The BODS identifier scheme for the BRN / Unique Business Identifier.
HK_BRN_SCHEME = "HK-BRN"
HK_BRN_SCHEME_NAME = "Hong Kong Business Registration Number (Unique Business Identifier)"

#: How many name-search rows become hits. The API returns up to 100.
_SEARCH_RESULT_CAP = 20

#: The register's miss, verbatim — HTTP 400, not 404.
_NO_RESULT = "no result found."

#: Similarity at or above which two Latin-script names are treated as the same
#: company. The product-wide name-screening threshold (see ``names``).
_NAME_SIMILARITY_THRESHOLD = 0.88

_DUMMY_BRN_RE = re.compile(r"^[A-Z]\d{7}$")
_ID_QUERY_RE = re.compile(r"^(?:[A-Z]\d{7}|\d{7,8})$")
_DENSE_PUNCT_RE = re.compile(r"[\s\W_]+", re.UNICODE)


def normalise_hk_brn(value: str) -> str:
    """Canonicalise a Hong Kong BRN to the form the register returns.

    * A dummy BRN (one letter + seven digits, e.g. ``c1572528``) is upper-cased.
    * Otherwise the BRN is the first eight digits: that recovers it from the
      16-digit BR certificate number (``7255270400001264`` → ``72552704``) and
      the hyphenated form (``58879114-000-08-25-2`` → ``58879114``).
    * A bare number shorter than eight digits is zero-padded — the register
      stores ``07341475`` and answers ``7341475`` with it.

    Raises ``ValueError`` for anything else, so the lookup pipeline skips the
    adapter rather than querying a guessed number.
    """
    text = "".join(str(value or "").split()).upper()
    if not text:
        raise ValueError("empty BRN")
    if _DUMMY_BRN_RE.match(text):
        return text
    if re.fullmatch(r"[\d\-/.]+", text):
        digits = re.sub(r"\D", "", text)
        if len(digits) >= 8:
            return digits[:8]
        if digits and text.isdigit():
            return digits.zfill(8)
    raise ValueError(f"not a Hong Kong BRN: {value!r}")


@lru_cache(maxsize=2)
def _opencc(config: str) -> Any:
    from opencc import OpenCC  # pure Python; imported lazily

    return OpenCC(config)


def to_traditional(text: str | None) -> str:
    """Simplified → traditional Chinese, for querying the register (which
    writes names in traditional characters). Non-Chinese text passes through."""
    if not text:
        return ""
    if not names.has_dense_script(text):
        return text
    return _opencc("s2t").convert(text)


def to_simplified(text: str | None) -> str:
    """Traditional → simplified Chinese: the comparison fold. Non-Chinese text
    passes through."""
    if not text:
        return ""
    if not names.has_dense_script(text):
        return text
    return _opencc("t2s").convert(text)


def clean_field(value: Any) -> str:
    """The register's text field, with its literal ``"NULL"`` read as empty."""
    text = str(value or "").strip()
    return "" if text.upper() == "NULL" else text


def _dense_key(text: str) -> str:
    """Comparable form for a Chinese name: simplified, NFKC, no punctuation.

    NFKC folds full-width brackets and spaces (``（香港）`` → ``(香港)``), then
    everything that is not a letter or ideograph is removed, so
    ``开普实业（香港）有限公司`` and ``開普實業(香港)有限公司`` agree.
    """
    folded = unicodedata.normalize("NFKC", to_simplified(text)).casefold()
    return _DENSE_PUNCT_RE.sub("", folded)


def _latin_agree(a: str, b: str) -> bool:
    ca, cb = names.org_comparable_name(a), names.org_comparable_name(b)
    if ca and ca == cb:
        return True
    if ca and names.despace(ca) == names.despace(cb):
        return True
    if names.distinctive_token_agreement(a, b):
        return True
    return names.name_similarity(a, b) >= _NAME_SIMILARITY_THRESHOLD


def names_agree(legal_name: str | None, company: dict[str, Any]) -> bool:
    """Does the register's company carry the name GLEIF gave the subject?

    True when either register name (English or Chinese) agrees with
    ``legal_name``. Chinese names compare on :func:`_dense_key`; Latin names
    through the shared organisation-name helpers. An empty ``legal_name``
    cannot be checked and is treated as agreeing — the caller has nothing to
    contradict the identifier with.
    """
    wanted = (legal_name or "").strip()
    if not wanted:
        return True
    candidates = [
        clean_field(company.get("English_Company_Name")),
        clean_field(company.get("Chinese_Company_Name")),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        if names.has_dense_script(wanted) or names.has_dense_script(candidate):
            key_a, key_b = _dense_key(wanted), _dense_key(candidate)
            if key_a and key_a == key_b:
                return True
            # A Latin legal name against a Chinese register name (or the
            # reverse) cannot agree character by character; the other
            # candidate gets its turn.
            continue
        if _latin_agree(wanted, candidate):
            return True
    return False


def parse_hk_date(value: Any) -> str | None:
    """``DD-MM-YYYY`` → ``YYYY-MM-DD``; anything else → None."""
    match = re.fullmatch(r"(\d{2})-(\d{2})-(\d{4})", clean_field(value))
    if not match:
        return None
    day, month, year = match.groups()
    return f"{year}-{month}-{day}"


def company_url(brn: str) -> str:
    """A stable, dereferenceable URL for the company's register record."""
    query = urlencode(
        {"query[0][key1]": "Brn", "query[0][key2]": "equal", "query[0][key3]": brn}
    )
    return f"{_API_BASE}/local/search?{query}"


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode("utf-8")).hexdigest()[:16]


class CrHongKongAdapter(SourceAdapter):
    """Source adapter for the Hong Kong Companies Registry open data API."""

    id = "cr_hongkong"

    lookup_derivers = (
        LookupDeriver(HK_RA_CODES, "hk_brn", normalise_hk_brn),
    )
    lookup_pass_legal_name = True

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="Hong Kong Companies Registry",
            homepage="https://www.cr.gov.hk/",
            description=(
                "Hong Kong company data from the Companies Registry's open data "
                "API on DATA.GOV.HK. Provides core entity details for live local "
                "companies — English and Chinese names, Business Registration "
                "Number, company type, registered office and incorporation date."
            ),
            license="DATA.GOV.HK-Terms",
            attribution=(
                "Contains data from the Companies Registry of the Government of "
                "the Hong Kong Special Administrative Region, made available via "
                "DATA.GOV.HK (data.cr.gov.hk)."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
            is_national_register=True,
            country="HK",
        )

    # ------------------------------------------------------------------
    # HTTP — every outbound call funnels through here
    # ------------------------------------------------------------------

    async def _query(self, key: str, operator: str, value: str) -> list[dict[str, Any]] | None:
        """One search call. ``[]`` for the register's clean miss, None on failure.

        Every None path records a degradation so a register that did not
        answer is never confused with one that had nothing to say.
        """
        params = {
            "query[0][key1]": key,
            "query[0][key2]": operator,
            "query[0][key3]": value,
        }
        async with build_client() as client:
            response = await client.get(f"{_API_BASE}/local/search", params=params)

        if response.status_code == 400:
            message = _error_message(response)
            if message.lower() == _NO_RESULT:
                return []
            degradation.record(
                self.id,
                f"Companies Registry rejected the query (HTTP 400: {message or 'no message'})",
            )
            return None
        if not response.is_success:
            _LOG.warning("Companies Registry returned %s", response.status_code)
            degradation.record(
                self.id, f"Companies Registry returned HTTP {response.status_code}"
            )
            return None
        try:
            payload = response.json()
        except ValueError:
            degradation.record(self.id, "Companies Registry returned a non-JSON body")
            return None
        if not isinstance(payload, list):
            degradation.record(self.id, "Companies Registry returned an unexpected shape")
            return None
        return [row for row in payload if isinstance(row, dict)]

    # ------------------------------------------------------------------
    # Search — name prefix, or a BRN typed as the query
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Search live local companies by name prefix (English or Chinese).

        The register supports only ``begins_with`` on names. A simplified
        Chinese query is converted to traditional first, because that is how
        the register writes names. A query shaped like a BRN is looked up
        exactly instead.
        """
        if kind != SearchKind.ENTITY:
            return []
        cleaned = " ".join(str(query or "").split())
        if not cleaned:
            return []

        compact = "".join(cleaned.split()).upper()
        if _ID_QUERY_RE.match(compact):
            key, operator, value = "Brn", "equal", normalise_hk_brn(compact)
        else:
            key, operator, value = "Comp_name", "begins_with", to_traditional(cleaned)

        cache_key = f"{_CACHE_NS}/search/{key}/{_slug(value)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return []

        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            rows = cached[0] or []
        else:
            fetched = await self._query(key, operator, value)
            if fetched is None:
                return []
            rows = fetched
            self._cache.put(cache_key, rows)

        hits: list[SourceHit] = []
        for company in rows[:_SEARCH_RESULT_CAP]:
            brn = clean_field(company.get("Brn"))
            name = clean_field(company.get("English_Company_Name")) or clean_field(
                company.get("Chinese_Company_Name")
            )
            if not brn or not name:
                continue
            hits.append(
                SourceHit(
                    source_id=self.id,
                    hit_id=brn,
                    kind=SearchKind.ENTITY,
                    name=name,
                    summary=f"{HK_BRN_SCHEME} {brn} · live",
                    identifiers={"hk_brn": brn},
                    raw={"company": company},
                    is_stub=False,
                )
            )
        return hits

    # ------------------------------------------------------------------
    # Fetch — one company by BRN, dropped if the name contradicts GLEIF
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        try:
            brn = normalise_hk_brn(hit_id)
        except ValueError:
            return self._bundle(None, str(hit_id or ""), legal_name)

        cache_key = f"{_CACHE_NS}/company/{brn}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return self._bundle(None, brn, legal_name)

        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            company = cached[0] or None
        else:
            rows = await self._query("Brn", "equal", brn)
            if rows is None:
                return self._bundle(None, brn, legal_name)
            company = next(
                (r for r in rows if clean_field(r.get("Brn")).upper() == brn), None
            )
            # A miss is cached too: the register is updated daily, and "not in
            # the live register" is as much an answer as a record.
            self._cache.put(cache_key, company or {})

        if not company:
            _LOG.info("Companies Registry: %s not in the live register", brn)
            return self._bundle(None, brn, legal_name)

        if not names_agree(legal_name, company):
            # The BRN reached a different company (the Chery / TMF Secretaries
            # case): drop the record rather than attach it to the subject.
            _LOG.info(
                "Companies Registry: %s names a different company — record dropped", brn
            )
            return self._bundle(None, brn, legal_name, name_mismatch=True)

        return self._bundle(company, brn, legal_name)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _bundle(
        self,
        company: dict[str, Any] | None,
        brn: str,
        legal_name: str,
        *,
        name_mismatch: bool = False,
    ) -> dict[str, Any]:
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "hk_brn": brn,
            "company": company,
            "legal_name": legal_name,
            "is_stub": company is None,
        }
        if name_mismatch:
            bundle["name_mismatch"] = True
        if company is not None:
            validate_raw(self.id, CrHongKongBundle, bundle)
        return bundle


def _error_message(response: Any) -> str:
    try:
        body = response.json()
    except ValueError:
        return ""
    if isinstance(body, dict):
        return str(body.get("message") or "").strip()
    return ""
