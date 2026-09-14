"""Romania — ANAF, the National Agency for Fiscal Administration (live).

The live half of Romania's coverage. ANAF publishes a keyless public web
service that answers about any Romanian taxpayer by **CUI** (fiscal code):
name, registered office and fiscal domicile, trade-register number, legal
form, CAEN activity code, registration and de-registration dates, VAT
registration and its history, the inactive-taxpayer register, split-VAT and
RO e-Factura status.

Endpoint and limits, from ANAF's own specification at
``static.anaf.ro/static/10/Anaf/Informatii_R/Servicii_web/doc_WS_V9.txt``
(note the ``Servicii_web/`` path segment — the shorter
``Informatii_R/doc_WS_V9.txt`` 404s)::

    POST https://webservicesp.anaf.ro/api/PlatitorTvaRest/v9/tva
    a) Un request poate contine maxim 100 de CUI-uri.
       Un client poate executa maxim 1 request pe secunda.
    b) Orice tentativa de suprasolicitare a serverului va fi pedepsita
       conform reglementarilor in vigoare.

**100 CUIs per request, one request per second.** Clause (b) — any attempt to
overload the server "will be punished under the regulations in force" — is why
that is treated here as a contractual ceiling rather than a throughput target:
``_BUCKET`` paces every call at one per second whatever the caller does.

A datacentre IP is fine
-----------------------
An earlier probe of this endpoint returned an HTML "Request Rejected" page and
the working assumption was an IP block. It is not: the rejection is a WAF
response to a crawler-shaped ``GET``. A plain ``POST`` with
``Content-Type: application/json`` is never challenged — measured from an
Anthropic datacentre IP on 14 September 2026, a full 100-CUI batch answered in
2.0 s, 100 found of 100. No special handling is needed on Render.

Licence, and why this adapter says so carefully
-----------------------------------------------
**ANAF publishes no data licence.** Its API terms
(``static.anaf.ro/static/10/Anaf/termeni_conditii_API.pdf``) are written for
software vendors integrating with ANAF, are silent on reuse, redistribution,
caching and commercial use, and do not say whether they cover the keyless
public services at all. They do explicitly prohibit two things: marketing
software as "accredited by MF or ANAF", and using the Ministry of Finance's or
ANAF's logos.

OpenCheck's position, recorded here so it is not re-derived: this is
**public-register fact**, cited to ANAF, cached only for the life of a lookup,
never republished in bulk, and never described as accredited. ``license`` below
says exactly that rather than naming a licence that does not exist.

What ``registeredAs`` holds, and the index
------------------------------------------
ANAF is keyed on the CUI and nothing else. Of the two Romanian registration
authorities whose numbers can reach a CUI, only one files a CUI directly:

* ``RA000719`` — Tax Payer Register, Ministry of Finance. A bare CUI, 51/51 in
  the live sample. Queried here with no index at all.
* ``RA000497`` — ONRC Trade Register. A CUI **or** a trade-register number,
  with nothing on the record to say which. 463 of 1,140 sampled records carry
  a CUI; the other 677 carry a ``J`` number, which ANAF cannot be asked about.

So ``_normalise_ro_id`` accepts either and defers the decision to ``fetch``,
which sniffs the shape and, for a registration number, asks the ONRC index for
the matching CUI. Without that index a J-number lookup cannot proceed, and this
adapter says so — a coverage note, the KvK/INPI shape — rather than returning
an empty result that would read as "ANAF has nothing on this company".

Reach, measured over 1,800 live RO LEI records on 14 September 2026: 47.8%
resolve with no index, 98.1% with one.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import date
from typing import Any

from .. import degradation
from ..config import get_settings
from ..http import build_client
from ..outbound_rate import TokenBucket, current_budget
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .onrc_romania import (
    RO_RA_CODES,
    is_cui,
    is_registration_number,
    normalise_cui,
    normalise_registration_number,
)
from . import onrc_romania
from .schemas import validate_raw
from .schemas.anaf_romania import AnafRomaniaBundle

logger = logging.getLogger(__name__)

_API_URL = "https://webservicesp.anaf.ro/api/PlatitorTvaRest/v9/tva"
_BILANT_URL = "https://webservicesp.anaf.ro/bilant"
_DOC_URL = (
    "https://static.anaf.ro/static/10/Anaf/Informatii_R/Servicii_web/doc_WS_V9.txt"
)
_HOMEPAGE = "https://www.anaf.ro/anaf/internet/ANAF/servicii_online/servicii_web_anaf/"

#: ANAF's own stated ceiling: one request per second, per client.
_RATE_PER_MINUTE = 60.0

#: ANAF's own stated ceiling: 100 CUIs in one request.
MAX_BATCH = 100

#: A lookup asks ANAF once. The cap exists so a future caller that loops
#: cannot turn one lookup into a rate-limit incident.
_LOOKUP_CALL_BUDGET = 3

_LOOKUP_TIMEOUT_S = 30.0

_BUCKET = TokenBucket(_RATE_PER_MINUTE, capacity=1, name="anaf_romania")

#: Said on the row and in the drawer when a trade-register number reached us
#: but no ONRC index is configured to turn it into a fiscal code.
COVERAGE_NO_INDEX = (
    "This company is identified by its trade-register number rather than its "
    "fiscal code (CUI), and ANAF's public service can only be queried by "
    "fiscal code. The ONRC open-data index that maps one to the other is not "
    "configured, so ANAF was not asked about this company."
)


def _normalise_ro_id(raw: str) -> str:
    """Normalise a Romanian identifier from GLEIF's ``registeredAs``.

    Accepts a CUI (with or without the ``RO`` prefix, with or without leading
    zeros) or an ONRC registration number in either of the register's two
    formats. Raises ``ValueError`` on anything else — including the NGO
    register's ``4325/A/2003`` and the ASF's ``CSC06FDIR/120135``, which are
    valid identifiers from other Romanian registers but cannot reach a CUI.

    The two are kept in one dispatch key on purpose. A deriver is a pure
    function of ``registeredAs`` and cannot consult an index, so the decision
    of *which* kind of identifier this is has to travel to ``fetch``.
    """
    text = str(raw or "").strip()
    if is_cui(text):
        return normalise_cui(text)
    return normalise_registration_number(text)


class AnafRomaniaAdapter(SourceAdapter):
    """Source adapter for ANAF's keyless public taxpayer web service."""

    id = "anaf_romania"

    lookup_derivers = (
        LookupDeriver(RO_RA_CODES, "ro_fiscal_or_reg_id", _normalise_ro_id),
    )
    lookup_pass_legal_name = True
    lookup_timeout_s = _LOOKUP_TIMEOUT_S

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="ANAF — Agenția Națională de Administrare Fiscală (Romania)",
            homepage=_HOMEPAGE,
            description=(
                "Romanian taxpayer register, queried live by fiscal code (CUI) "
                "through ANAF's keyless public web service: registered name, "
                "fiscal domicile and registered office, trade-register number, "
                "legal form, activity code, registration and de-registration "
                "dates, VAT registration history, and the inactive-taxpayer "
                "and RO e-Factura registers."
            ),
            # ANAF publishes no data licence for this service; see the module
            # docstring. Saying "public register, attribution requested" is
            # the honest description of the position, and deliberately not a
            # licence identifier that would imply terms nobody has granted.
            license="No stated licence — public register, attribution requested",
            attribution=(
                "Contains information from the taxpayer registers published by "
                "the Agenția Națională de Administrare Fiscală (ANAF) through "
                "its public web service at webservicesp.anaf.ro. ANAF states no "
                "licence for this data; OpenCheck republishes it as public "
                "register fact with attribution and does not redistribute it in "
                "bulk. OpenCheck is not accredited by ANAF or by the Romanian "
                "Ministry of Finance."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
            is_national_register=True,
            country="RO",
        )

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    async def _post(self, payload: list[dict[str, Any]]) -> dict[str, Any] | None:
        """One paced, budget-checked POST. The JSON object, or None on failure.

        Every None path records a degradation, so a register that did not
        answer is never mistaken for one that had nothing to say.
        """
        budget = current_budget(self.id, _LOOKUP_CALL_BUDGET)
        if budget is not None and not budget.take():
            degradation.record(
                self.id,
                (
                    f"ANAF per-lookup call budget of {_LOOKUP_CALL_BUDGET} "
                    "requests reached; the record was not fetched"
                ),
                reason=degradation.REASON_RATE_LIMITED,
            )
            return None

        await _BUCKET.acquire()
        try:
            async with build_client() as client:
                response = await client.post(
                    _API_URL,
                    content=json.dumps(payload).encode("utf-8"),
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    },
                )
        except Exception as exc:  # noqa: BLE001 - any transport failure degrades
            logger.warning("anaf_romania: request failed: %s", exc)
            degradation.record(
                self.id,
                f"ANAF did not answer: {type(exc).__name__}",
                reason=degradation.REASON_UPSTREAM_ERROR,
            )
            return None

        if response.status_code == 429:
            degradation.record(
                self.id,
                "ANAF returned HTTP 429 (the documented limit is 1 request per second)",
                reason=degradation.REASON_RATE_LIMITED,
            )
            return None
        if not response.is_success:
            degradation.record(
                self.id, f"ANAF returned HTTP {response.status_code}"
            )
            return None
        try:
            body = response.json()
        except ValueError:
            # The WAF answers a crawler-shaped request with an HTML page. A
            # correct POST is not challenged, so this is worth naming rather
            # than reporting as an empty result.
            degradation.record(
                self.id,
                "ANAF returned a non-JSON body (usually its web-application firewall)",
            )
            return None
        if not isinstance(body, dict):
            degradation.record(self.id, "ANAF returned an unexpected shape")
            return None
        return body

    async def fetch_many(
        self, cuis: list[str], *, as_of: str | None = None
    ) -> dict[str, dict[str, Any]]:
        """Look up several CUIs in one request. ``{cui: date_generale-bearing record}``.

        Batches at ANAF's stated ceiling of 100 and paces each batch through
        the same one-per-second bucket. Codes ANAF does not know come back in
        its ``notFound`` list and are simply absent from the result.
        """
        wanted: list[str] = []
        for raw in cuis:
            try:
                wanted.append(normalise_cui(raw))
            except ValueError:
                continue
        if not wanted:
            return {}
        stamp = as_of or date.today().isoformat()
        found: dict[str, dict[str, Any]] = {}
        for start in range(0, len(wanted), MAX_BATCH):
            chunk = wanted[start : start + MAX_BATCH]
            body = await self._post(
                [{"cui": int(c), "data": stamp} for c in chunk]
            )
            if body is None:
                continue
            for record in body.get("found") or []:
                general = (record or {}).get("date_generale") or {}
                cui = str(general.get("cui") or "").strip()
                if cui:
                    found[cui] = record
        return found

    # ------------------------------------------------------------------
    # SourceAdapter
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """ANAF has no name search — it answers only about a fiscal code."""
        return []

    def _stub(
        self,
        local_id: str,
        legal_name: str,
        *,
        coverage_note: str | None = None,
        not_found: bool = False,
    ) -> dict[str, Any]:
        return {
            "source_id": self.id,
            "cui": None,
            "queried_as": local_id,
            "name": legal_name or "",
            "record": None,
            "registration_number": None,
            "legal_name": legal_name,
            "link": _DOC_URL,
            "coverage_note": coverage_note,
            "not_found": not_found,
            "is_stub": coverage_note is None and not not_found,
        }

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the ANAF bundle for one Romanian identifier.

        ``hit_id`` is either a CUI or an ONRC registration number — see
        ``_normalise_ro_id``. A registration number is resolved to a CUI
        through the ONRC index; without that index, the bundle carries a
        coverage note instead of a record.
        """
        raw = str(hit_id or "").strip()
        cui: str | None = None

        if is_cui(raw):
            cui = normalise_cui(raw)
        elif is_registration_number(raw):
            number = normalise_registration_number(raw)
            if not onrc_romania.index_available():
                return self._stub(number, legal_name, coverage_note=COVERAGE_NO_INDEX)
            cui = onrc_romania.resolve_cui(number, legal_name=legal_name)
            if cui is None:
                return self._stub(
                    number,
                    legal_name,
                    coverage_note=(
                        "The ONRC open-data index holds no company under "
                        f"registration number {number} whose name matches this "
                        "entity, so no fiscal code was available to ask ANAF "
                        "about."
                    ),
                )
        else:
            return self._stub(raw, legal_name)

        found = await self.fetch_many([cui])
        record = found.get(cui)
        if record is None:
            return self._stub(cui, legal_name, not_found=True)

        general = record.get("date_generale") or {}
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "cui": cui,
            "queried_as": raw,
            "name": general.get("denumire") or legal_name or f"RO {cui}",
            "record": record,
            "registration_number": (general.get("nrRegCom") or "").strip() or None,
            "legal_name": legal_name,
            "link": _DOC_URL,
            "coverage_note": None,
            "not_found": False,
            "is_stub": False,
        }
        validate_raw("anaf_romania", AnafRomaniaBundle, bundle)
        return bundle

    async def fetch_financials(
        self, cui: str, year: int
    ) -> dict[str, Any] | None:
        """Return ANAF's filed balance-sheet indicators for one year, or None.

        A separate, equally keyless endpoint. Not part of ``fetch`` — the
        lookup does not need it, and calling it would double the request count
        against a one-per-second ceiling for data no current surface renders.
        """
        try:
            code = normalise_cui(cui)
        except ValueError:
            return None
        await _BUCKET.acquire()
        try:
            async with build_client() as client:
                response = await client.get(
                    _BILANT_URL, params={"an": str(year), "cui": code}
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("anaf_romania: bilant request failed: %s", exc)
            return None
        if not response.is_success:
            return None
        try:
            body = response.json()
        except ValueError:
            return None
        return body if isinstance(body, dict) else None
