"""What a debarment says: who barred the party, why, and until when.

The ``DEBARMENT`` and ``RELATED_DEBARMENT`` signals fire on OpenSanctions'
``debarment`` topic. Until this module the signal said only that "OpenSanctions
lists this record as debarred from public contracts". The record OpenSanctions
serves at ``/entities/{id}`` nests its ``Sanction`` entities, and for a
debarment list they carry what a reader needs to act on the finding: the
authority, the decision number, the reason as filed and the ban's start and
end dates. This module reads them.

Moldova's procurement ban list (the MTender Black List ticket, 13 Nov 2026) is
why: the Public Procurement Agency's list on tender.gov.md is the one current
copy (the ``/api/banned-operators`` endpoint on mtender.gov.md is a snapshot
loaded on 27 May 2021 whose every ban ended by April 2024), it publishes no
IDNO, it answers 403 to every datacentre address, and OpenSanctions crawls it
daily as ``md_interdictie``, in its ``default`` and ``debarment`` collections.
So OpenCheck already matched the companies on it by name; what it did not say
was that the match is a three-year ban decided by the Ministry of Finance for
presenting false documents, and when it ends.

Rules, each of which will be re-derived otherwise:

* **Describes, never decides.** No new code, no change of confidence: the
  terms add a sentence to the signal's summary and ``debarment_terms`` to its
  evidence. A ban whose every term has ended is worded as ended
  (``debarment_ended: true``) and keeps its code — the listing is still on the
  record OpenSanctions serves, and dropping it is OpenSanctions' call.
* **Which ``Sanction`` is a debarment.** On a record that carries no sanctions
  topic, every nested ``Sanction`` is one. On a record that is also on a
  sanctions list, only those from a dataset in OpenSanctions' ``debarment``
  collection (``DEBARMENT_DATASETS``, read from the catalogue on 13 Nov 2026)
  — an OFAC programme is not a ban from public contracts.
* **Words as filed.** The authority and the reason are OpenSanctions' copy of
  the publisher's words, in the publisher's language (Moldova's reasons are
  Romanian), quoted and never translated.
* **Related parties are read, not searched.** A search or ``/match`` answer
  carries no nested sanctions, so ``describe_debarment_signals`` reads each
  matched record once through the OpenSanctions adapter's cached ``fetch`` (at
  most ``MAX_READS``, the PEP role check's pattern). A record that cannot be
  read leaves its signal as it was — the finding stands without its dates.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timezone
from typing import Any, Iterable

from .findings import human_date

_LOG = logging.getLogger(__name__)

DEBARMENT_CODES: frozenset[str] = frozenset({"DEBARMENT", "RELATED_DEBARMENT"})

#: OpenSanctions' ``debarment`` collection, read from
#: https://data.opensanctions.org/datasets/latest/index.json on 13 Nov 2026
#: (61 datasets). Consulted only for a record that is *also* on a sanctions
#: list; a dataset missing here leaves that record's ban undated, never
#: misread as a sanction's dates.
DEBARMENT_DATASETS: frozenset[str] = frozenset(
    {
        "adb_sanctions", "afdb_sanctions", "au_abf_sanctioned_sponsors",
        "br_bcb_prohibited", "br_ceis", "br_tcu_debarred", "ebrd_ineligible",
        "eu_edes", "iadb_sanctions", "li_entsg", "md_interdictie",
        "ph_gppb_debarred", "ps_ohchr_settlement", "si_conflicts",
        "us_ak_med_exclusions", "us_al_med_exclusions", "us_ar_med_exclusions",
        "us_az_med_exclusions", "us_ca_med_exclusions", "us_co_med_exclusions",
        "us_dc_exclusions", "us_ddtc_debarred", "us_de_med_exclusions",
        "us_dod_chinese_milcorps", "us_fcc_covered_list", "us_fda_restricted",
        "us_ga_med_exclusions", "us_hhs_exclusions", "us_hi_med_exclusions",
        "us_ia_med_exclusions", "us_in_med_exclusions", "us_ks_med_exclusions",
        "us_ky_med_exclusions", "us_la_med_exclusions", "us_ma_med_exclusions",
        "us_md_med_exclusions", "us_me_med_exclusions", "us_mi_med_exclusions",
        "us_mn_med_exclusions", "us_mo_med_exclusions", "us_ms_med_exclusions",
        "us_mt_med_exclusions", "us_nc_med_exclusions", "us_nd_med_exclusions",
        "us_ne_med_exclusions", "us_nh_med_exclusions", "us_nj_med_exclusions",
        "us_nv_med_exclusions", "us_ny_med_exclusions", "us_oh_med_exclusions",
        "us_or_med_exclusions", "us_pa_med_exclusions", "us_sam_exclusions",
        "us_sc_med_exclusions", "us_special_leg", "us_tn_med_exclusions",
        "us_tx_med_exclusions", "us_wa_med_exclusions", "us_wv_med_exclusions",
        "us_wy_med_exclusions", "worldbank_debarred",
    }
)

#: Records read for related-party debarment matches, per lookup.
MAX_READS = 25
_READ_CONCURRENCY = 4

#: Longest reason quoted in a summary; the full text stays in the evidence.
_REASON_MAX = 160


def _today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _first(props: dict[str, Any], key: str) -> str | None:
    values = props.get(key)
    if isinstance(values, list):
        for v in values:
            if isinstance(v, str) and v.strip():
                return v.strip()
        return None
    if isinstance(values, str) and values.strip():
        return values.strip()
    return None


def _day(value: str | None) -> str | None:
    """A full ISO day, or ``None`` (a year or year-month is not a ban date)."""
    if not value or len(value) < 10:
        return None
    try:
        return date.fromisoformat(value[:10]).isoformat()
    except ValueError:
        return None


def _on_sanctions_list(entity: dict[str, Any]) -> bool:
    props = entity.get("properties") or {}
    topics = props.get("topics") or []
    return any(
        isinstance(t, str) and (t == "sanction" or t.startswith("sanction."))
        and t != "sanction.counter"
        for t in topics
    )


def debarment_terms(
    entity: dict[str, Any] | None, *, today: str | None = None
) -> list[dict[str, Any]]:
    """The bans an OpenSanctions record's nested ``Sanction`` entities state,
    newest first. Empty for a search card (nothing nested) or a record with no
    sanctions."""
    if not isinstance(entity, dict):
        return []
    props = entity.get("properties") or {}
    nested = props.get("sanctions") or []
    if not isinstance(nested, list):
        return []
    strict = _on_sanctions_list(entity)
    today = today or _today()
    out: list[dict[str, Any]] = []
    for item in nested:
        if not isinstance(item, dict):
            continue
        datasets = sorted({str(d) for d in item.get("datasets") or [] if d})
        if strict and not set(datasets) & DEBARMENT_DATASETS:
            continue
        p = item.get("properties") or {}
        start = _day(_first(p, "startDate")) or _day(_first(p, "listingDate"))
        end = _day(_first(p, "endDate"))
        term: dict[str, Any] = {
            "datasets": datasets,
            "authority": _first(p, "authority"),
            "decision": _first(p, "authorityId") or _first(p, "unscId"),
            "program": _first(p, "program"),
            "reason": _first(p, "reason") or _first(p, "summary"),
            "start_date": start,
            "end_date": end,
            "ended": bool(end and end < today),
            "source_url": _first(p, "sourceUrl"),
        }
        out.append({k: v for k, v in term.items() if k == "ended" or v not in (None, [], "")})
    out.sort(key=lambda t: (t.get("start_date") or "", t.get("end_date") or ""), reverse=True)
    return out


def _clip(text: str) -> str:
    text = " ".join(text.split())
    if len(text) <= _REASON_MAX:
        return text
    return text[: _REASON_MAX - 1].rstrip() + "…"


def term_sentence(term: dict[str, Any]) -> str:
    """One ban in words: ``Barred by X (decision 11/26) from 2 October 2026
    until 2 October 2029; reason given: "…".``"""
    by = f" by {term['authority']}" if term.get("authority") else ""
    decision = f" (decision {term['decision']})" if term.get("decision") else ""
    start = human_date(term.get("start_date"))
    end = human_date(term.get("end_date"))
    if start and end:
        span = f" from {start} until {end}"
    elif end:
        span = f" until {end}"
    elif start:
        span = f" from {start}, with no end date given"
    else:
        span = ""
    verb = "Was barred" if term.get("ended") else "Barred"
    sentence = f"{verb}{by}{decision}{span}"
    if term.get("ended") and end:
        sentence += "; the ban has ended"
    if term.get("reason"):
        sentence += f'; reason given: "{_clip(term["reason"])}"'
    return sentence + "."


def terms_summary(terms: list[dict[str, Any]]) -> str:
    """The sentence the signal's summary gains. The latest ban is spelled
    out; earlier ones are counted (all of them are in the evidence)."""
    if not terms:
        return ""
    head = term_sentence(terms[0])
    if len(terms) == 1:
        return head
    return f"{head} {len(terms) - 1} earlier ban{'s' if len(terms) > 2 else ''} on record."


def enrich(signal: dict[str, Any], entity: dict[str, Any] | None, *, today: str | None = None) -> dict[str, Any]:
    """A copy of a ``DEBARMENT`` / ``RELATED_DEBARMENT`` signal dict with the
    record's terms added; the signal itself when there are none."""
    terms = debarment_terms(entity, today=today)
    if not terms:
        return signal
    evidence = dict(signal.get("evidence") or {})
    evidence["debarment_terms"] = terms
    if all(t.get("ended") for t in terms):
        evidence["debarment_ended"] = True
    summary = str(signal.get("summary") or "").rstrip()
    return {**signal, "summary": f"{summary} {terms_summary(terms)}".strip(), "evidence": evidence}


async def _read_record(upstream_id: str) -> dict[str, Any] | None:
    from .sources import REGISTRY

    adapter = REGISTRY.get("opensanctions")
    if adapter is None:
        return None
    try:
        bundle = await adapter.fetch(upstream_id)
    except Exception as exc:  # noqa: BLE001 — a refinement; never fail the lookup
        _LOG.info("Debarment terms: could not read %s (%s)", upstream_id, type(exc).__name__)
        return None
    if not isinstance(bundle, dict) or bundle.get("is_stub"):
        return None
    entity = bundle.get("entity")
    return entity if isinstance(entity, dict) else None


async def describe_debarment_signals(
    signals: Iterable[dict[str, Any]], *, today: str | None = None
) -> list[dict[str, Any]]:
    """Add the ban's terms to every name-matched debarment signal.

    Reads each distinct OpenSanctions record once (cached ``fetch``), at most
    ``MAX_READS`` per call. A signal that already carries terms, or whose
    record cannot be read, is returned unchanged.
    """
    from .pep_merge import upstream_record_id

    out = list(signals)
    targets: dict[str, list[int]] = {}
    for i, sig in enumerate(out):
        if sig.get("code") not in DEBARMENT_CODES:
            continue
        if (sig.get("evidence") or {}).get("debarment_terms"):
            continue
        uid = upstream_record_id(sig)
        if uid:
            targets.setdefault(uid, []).append(i)
    if not targets:
        return out

    ids = list(targets)[:MAX_READS]
    gate = asyncio.Semaphore(_READ_CONCURRENCY)

    async def read(uid: str) -> dict[str, Any] | None:
        async with gate:
            return await _read_record(uid)

    records = await asyncio.gather(*(read(u) for u in ids))
    for uid, entity in zip(ids, records):
        if entity is None:
            continue
        for i in targets[uid]:
            out[i] = enrich(out[i], entity, today=today)
    return out


__all__ = [
    "DEBARMENT_CODES",
    "DEBARMENT_DATASETS",
    "MAX_READS",
    "debarment_terms",
    "describe_debarment_signals",
    "enrich",
    "term_sentence",
    "terms_summary",
]
