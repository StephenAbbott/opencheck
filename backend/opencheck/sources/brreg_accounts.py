"""Brreg's annual accounts register (Regnskapsregisteret), read for the brreg card.

Since 1 October 2026 Brønnøysundregistrene's open accounts API gives everyone
the figures, group accounts and key figures from each company's three most
recently filed annual accounts (before then only the latest year's key
figures were open; the rest was for public authorities). No key.

* ``GET https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}`` — every
  record for the last three filed years, one per period and per type
  (``SELSKAP`` company accounts, ``KONSERN`` group accounts).
* ``GET …/aarsregnskap/kopi/{orgnr}/aar`` — the years for which a copy of
  the filed accounts can be downloaded as a PDF (Equinor: 2011–2025).

What OpenCheck takes from it (Stephen, 26 Oct 2026, the "Brreg annual
accounts" ticket): **the facts about the latest filed period only** — the
period, the filing's journal number, whether group accounts were filed, the
audit opt-out, unaudited or liquidation accounts, the reporting currency and
the accounting rules — and a link to the filed PDF. **No figures** (turnover,
result, assets are never read into the bundle), **nothing in BODS**, no risk
signal, no verdict clause.

The answers, and what each means:

* **200** — accounts on file: ``status: "filed"``. Cached for a day.
* **404** — no accounts in the last three filed years: ``status: "none"``.
  Cached as an absence (Phase 262: a 404 is a definitive answer). It says
  nothing about whether the company had to file, so it is never worded as
  "overdue".
* **500 naming an unsupported ``oppstillingsplan``** — banks (``BANK``), life
  insurers (``LIV``) and non-life insurers (``SKADE``) file accounts in a
  format the open API does not serve: ``status: "unsupported"``. A definitive
  answer about the format, not an outage — cached, no degradation.
* **Anything else** (another 5xx, 429, timeout, a non-JSON body):
  ``status: "unavailable"``, a ``source_read`` degradation, never cached.

Licence: Brreg publishes its open datasets under NLOD (the old key-figures
distribution of this register was listed as NLOD 2.0, as Enhetsregisteret is).
"""

from __future__ import annotations

import logging
import re
from typing import Any

import httpx

from .. import degradation
from ..cache import Cache
from ..http import build_client
from ..secret_scrub import describe_exception

log = logging.getLogger(__name__)

ACCOUNTS_BASE = "https://data.brreg.no/regnskapsregisteret/regnskap"

#: Cache key prefix; bump the version when the summary's shape changes.
CACHE_PREFIX = "brreg/regnskap-v1"

#: A filed record is re-read after a day: accounts arrive once a year, but a
#: company that files today should not read "none" for a week.
CACHE_MAX_AGE_DAYS = 1.0

#: The statement formats (``oppstillingsplan``) the open API refuses, in the
#: words the card uses. An unknown code is shown as the register wrote it.
UNSUPPORTED_FORMAT_WORDS: dict[str, str] = {
    "BANK": "bank",
    "LIV": "life insurance",
    "SKADE": "non-life insurance",
}

#: ``regnkapsprinsipper.regnskapsregler`` (the API's spelling), in English.
RULES_WORDS: dict[str, str] = {
    "IFRS": "IFRS",
    "forenkletAnvendelseIFRS": "simplified IFRS",
    "regnskapslovenAlminneligRegler": "Norwegian Accounting Act, general rules",
}

_UNSUPPORTED_RE = re.compile(
    r"oppstillingsplan som ikke er st[oø]t+et\s*\(([A-Za-z_]+)\)", re.IGNORECASE
)

#: The degradation detail. Count-free and name-free (the DegradedSource rule).
DETAIL_UNAVAILABLE = (
    "Brreg's annual accounts register did not answer; the card says nothing "
    "about the company's accounts. This is not a statement that none were filed."
)


def accounts_url(orgnr: str) -> str:
    return f"{ACCOUNTS_BASE}/{orgnr}"


def pdf_url(orgnr: str, year: int | str) -> str:
    """The register's copy of the filed annual accounts for ``year`` (a PDF)."""
    return f"{ACCOUNTS_BASE}/aarsregnskap/kopi/{orgnr}/{year}"


def unsupported_format(body: str) -> str | None:
    """The format code a 500 names as unsupported, or ``None``.

    The register answers ``{"status": "500", "message": "Regnskapet inneholder
    en oppstillingsplan som ikke er stottet (BANK)", …}`` for a bank. Only that
    message makes a 500 an answer; any other 500 is an outage.
    """
    match = _UNSUPPORTED_RE.search(body or "")
    return match.group(1).upper() if match else None


def _flag(value: Any) -> bool:
    return value is True


def summarise(records: list[dict[str, Any]], pdf_years: list[Any] | None, orgnr: str) -> dict[str, Any]:
    """Reduce the register's records to the facts about the latest filed period.

    Reads no figure. The latest period is the one with the latest end date;
    its company record (``SELSKAP``) carries the facts, and group accounts
    are "filed" when a ``KONSERN`` record exists for the same period. A
    period with only a group record is read from that record.
    """
    dated = [
        r for r in records
        if isinstance(r, dict) and (r.get("regnskapsperiode") or {}).get("tilDato")
    ]
    if not dated:
        return {"status": "none", "source_url": accounts_url(orgnr)}
    latest_end = max(str(r["regnskapsperiode"]["tilDato"]) for r in dated)
    period_records = [r for r in dated if str(r["regnskapsperiode"]["tilDato"]) == latest_end]
    company = next(
        (r for r in period_records if r.get("regnskapstype") == "SELSKAP"),
        period_records[0],
    )
    period = company.get("regnskapsperiode") or {}
    revisjon = company.get("revisjon") or {}
    # The API spells the field ``regnkapsprinsipper``; read the correct
    # spelling too in case Brreg fixes it.
    principles = company.get("regnkapsprinsipper") or company.get("regnskapsprinsipper") or {}
    year = latest_end[:4]
    years = {str(y) for y in (pdf_years or [])}
    summary: dict[str, Any] = {
        "status": "filed",
        "period_start": period.get("fraDato"),
        "period_end": period.get("tilDato"),
        "year": int(year) if year.isdigit() else None,
        "journal_number": company.get("journalnr") or None,
        "group_accounts": any(r.get("regnskapstype") == "KONSERN" for r in period_records),
        "group_parent": _flag((company.get("virksomhet") or {}).get("morselskap")),
        "audit_opted_out": _flag(revisjon.get("fravalgRevisjon")),
        "not_audited": _flag(revisjon.get("ikkeRevidertAarsregnskap")),
        "liquidation_accounts": _flag(company.get("avviklingsregnskap")),
        "small_company": _flag(principles.get("smaaForetak")),
        "currency": company.get("valuta") or None,
        "rules": principles.get("regnskapsregler") or None,
        "source_url": accounts_url(orgnr),
        # Linked only when the register lists a copy for that year: a link
        # to a PDF the register does not hold is a link to an error page.
        "pdf_url": pdf_url(orgnr, year) if year in years else None,
    }
    return summary


async def read_accounts(
    orgnr: str, cache: Cache, *, live: bool, source_id: str = "brreg"
) -> dict[str, Any] | None:
    """The accounts summary for ``orgnr`` — a dict with a ``status``, or ``None``.

    The cache is read first; the register only when ``live`` is true. With
    live calls off and nothing cached the answer is ``None`` — not read, and
    the card says nothing (the offline rule every adapter follows).
    """
    key = f"{CACHE_PREFIX}/{orgnr}"
    cached = cache.get_payload(key, max_age_days=CACHE_MAX_AGE_DAYS)
    if cached is not None:
        payload = cached[0]
        if payload is None:  # a cached 404
            return {"status": "none", "source_url": accounts_url(orgnr)}
        return payload
    if not live:
        return None

    try:
        async with build_client() as client:
            response = await client.get(
                accounts_url(orgnr), headers={"Accept": "application/json"}
            )
            if response.status_code == 404:
                cache.put_absent(key)
                return {"status": "none", "source_url": accounts_url(orgnr)}
            if response.status_code >= 500:
                code = unsupported_format(response.text)
                if code:
                    summary = {
                        "status": "unsupported",
                        "format": code,
                        "format_words": UNSUPPORTED_FORMAT_WORDS.get(code, code),
                        "source_url": accounts_url(orgnr),
                    }
                    cache.put(key, summary)
                    return summary
            response.raise_for_status()
            records = response.json()
            if not isinstance(records, list):
                raise ValueError("accounts answer is not a list")
            pdf_years: list[Any] | None = None
            # The PDF list is a courtesy: if it fails, the facts stand and
            # the card simply carries no PDF link.
            try:
                years_response = await client.get(
                    f"{ACCOUNTS_BASE}/aarsregnskap/kopi/{orgnr}/aar",
                    headers={"Accept": "application/json"},
                )
                if years_response.is_success:
                    body = years_response.json()
                    pdf_years = body if isinstance(body, list) else None
            except (httpx.HTTPError, ValueError) as exc:
                log.info("Brreg accounts PDF list unavailable: %s", describe_exception(exc))
    except (httpx.HTTPError, ValueError, OSError) as exc:
        failure = describe_exception(exc)
        log.warning("Brreg accounts unavailable: %s", failure)
        degradation.record(
            source_id,
            DETAIL_UNAVAILABLE,
            check=degradation.CHECK_SOURCE_READ,
            reason=degradation.reason_for_failure(failure),
        )
        return {
            "status": "unavailable",
            "reason": degradation.reason_for_failure(failure),
            "source_url": accounts_url(orgnr),
        }

    summary = summarise(records, pdf_years, orgnr)
    cache.put(key, summary)
    return summary
