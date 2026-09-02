"""The subject's profile — what the registers say the company *is*.

Phase 154. The subject card carried name, flag and LEI, and nothing else;
legal form, register status, incorporation date and registered address sat
only on the OpenCorporates / Companies House cards further down, inside a
disclosure. A due diligence report opens with those four facts, and the one
of them that changes a reading — the register says the company is dissolved
— was shown only on a structured-records card most readers never open.

This module assembles them once, from the merged BODS bundle, so the page,
the API and the MCP surface read the same profile:

* **Only the subject's own statements are read.** ``consistency.referent_groups``
  groups entity statements that share a strong identifier — the rule the
  FullCheck network merges on — and the group holding the GLEIF statement for
  the looked-up LEI is the subject. A name match is never a referent.
* **Register status: the worst class wins.** A dissolved company with an
  ACTIVE LEI is the case Phase 151 was started for, so ``terminal`` outranks
  ``pending`` outranks ``live`` whichever source said it, and the source that
  said it is the one named.
* **Everything else: the register first.** A national register's legal form,
  founding date and registered address are preferred to GLEIF's, and GLEIF's
  to an aggregator's; the sources that state the same value are listed with
  it, counted for independence through ``sources.lineage`` so OpenCorporates
  republishing Companies House is one observation, not two.
* **It states facts, never findings.** A dissolved company is reported as
  dissolved. Whether that matters is the analyst's call — the same rule as
  ``verdict.py``.
"""

from __future__ import annotations

from typing import Any

from .bods import liveness as _liveness
from .consistency import referent_groups, source_id_of
from .reconcile import _entity_jurisdiction
from .sources.lineage import independent_count, national_register_ids

#: Liveness classes, worst first.
_LIVENESS_RANK = {_liveness.TERMINAL: 0, _liveness.PENDING: 1, _liveness.LIVE: 2}


def _rd(stmt: dict[str, Any]) -> dict[str, Any]:
    return stmt.get("recordDetails") or {}


def _is_subject_lei(stmt: dict[str, Any], lei: str) -> bool:
    for ident in _rd(stmt).get("identifiers") or []:
        if not isinstance(ident, dict):
            continue
        if ident.get("scheme") == "XI-LEI" and str(ident.get("id") or "").upper() == lei:
            return True
    return False


def subject_statements(lei: str, bods: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The entity statements that describe the looked-up LEI.

    The referent group containing a statement that carries the LEI as an
    ``XI-LEI`` identifier; when no group has formed (one source only), the
    statements carrying the LEI themselves.
    """
    lei = lei.strip().upper()
    if not lei:
        return []
    for group in referent_groups(bods):
        if any(_is_subject_lei(s, lei) for s in group):
            return group
    return [
        s for s in bods
        if s.get("recordType") == "entity" and _is_subject_lei(s, lei)
    ]


def _source_rank(source_id: str, registers: frozenset[str]) -> int:
    if source_id in registers:
        return 0
    if source_id == "gleif":
        return 1
    return 2


def _legal_form(stmt: dict[str, Any]) -> str | None:
    rd = _rd(stmt)
    et = rd.get("entityType")
    if isinstance(et, dict):
        details = str(et.get("details") or "").strip()
        if details:
            return details
    label = str(rd.get("legalFormLabel") or "").strip()
    return label or None


def _founding(stmt: dict[str, Any]) -> str | None:
    raw = str(_rd(stmt).get("foundingDate") or "").strip()
    return raw[:10] if raw else None


def _registered_address(stmt: dict[str, Any]) -> dict[str, str] | None:
    addresses = _rd(stmt).get("addresses") or []
    for addr in addresses:
        if not isinstance(addr, dict) or addr.get("type") != "registered":
            continue
        text = str(addr.get("address") or "").strip()
        if not text:
            continue
        country = addr.get("country") or {}
        code = str(country.get("code") or "").strip().upper() if isinstance(country, dict) else ""
        return {"value": text, "country": code}
    return None


def _norm_text(v: str) -> str:
    return " ".join(v.casefold().replace(",", " ").split())


def _dates_agree(a: str, b: str) -> bool:
    n = min(len(a), len(b), 10)
    return a[:n] == b[:n]


def _pick(
    candidates: list[tuple[str, str]],
    registers: frozenset[str],
    *,
    same,
    prefer_longer: bool = False,
) -> dict[str, Any] | None:
    """Choose a value and list the sources that state it.

    ``candidates`` is ``[(source_id, value)]``. The register's value wins,
    then GLEIF's, then anyone's; within a rank, ``prefer_longer`` picks the
    more precise value (a full date over a bare year).
    """
    if not candidates:
        return None
    ordered = sorted(
        candidates,
        key=lambda c: (_source_rank(c[0], registers), -len(c[1]) if prefer_longer else 0, c[0]),
    )
    chosen_source, chosen = ordered[0]
    # Among agreeing values, keep the most precise one as the display value.
    agreeing = [(sid, v) for sid, v in candidates if same(v, chosen)]
    if prefer_longer:
        chosen = max((v for _, v in agreeing), key=len)
    sources = sorted({sid for sid, _ in agreeing})
    others = [
        {"source_id": sid, "value": v}
        for sid, v in sorted(candidates, key=lambda c: c[0])
        if not same(v, chosen)
    ]
    return {
        "value": chosen,
        "sources": sources,
        "independent_sources": independent_count(sources),
        "other_values": others,
    }


def build_subject_profile(lei: str, bods: list[dict[str, Any]]) -> dict[str, Any] | None:
    """The four profile fields for ``lei``, or ``None`` with no subject statement.

    Shape::

        {
          "legal_form":  {"value", "sources", "independent_sources", "other_values"} | None,
          "register_status": {"liveness", "since", "raw", "source_id", "sources",
                              "independent_sources"} | None,
          "founding_date": {...} | None,
          "registered_address": {"value", "country", "sources", ...} | None,
          "jurisdiction": "GB" | None,
          "statement_ids": [...],
        }
    """
    stmts = subject_statements(lei, bods)
    if not stmts:
        return None
    registers = national_register_ids()

    legal_forms: list[tuple[str, str]] = []
    foundings: list[tuple[str, str]] = []
    addresses: list[tuple[str, str]] = []
    address_country: dict[str, str] = {}
    statuses: list[tuple[str, dict[str, Any]]] = []
    jurisdiction: str | None = None

    for stmt in stmts:
        sid = source_id_of(stmt)
        lf = _legal_form(stmt)
        if lf:
            legal_forms.append((sid, lf))
        fd = _founding(stmt)
        if fd:
            foundings.append((sid, fd))
        addr = _registered_address(stmt)
        if addr:
            addresses.append((sid, addr["value"]))
            address_country[addr["value"]] = addr["country"]
        status = _liveness.read_register_status(stmt)
        if status:
            statuses.append((sid, status))
        if not jurisdiction:
            jurisdiction = _entity_jurisdiction(_rd(stmt)) or None

    register_status: dict[str, Any] | None = None
    if statuses:
        # Worst class first; within a class, the register before GLEIF before
        # the rest, so the chip names the authority a reader would go to.
        statuses.sort(
            key=lambda s: (
                _LIVENESS_RANK.get(s[1]["liveness"], 3),
                _source_rank(s[0], registers),
                s[0],
            )
        )
        sid, status = statuses[0]
        agreeing = sorted({s for s, st in statuses if st["liveness"] == status["liveness"]})
        register_status = {
            "liveness": status["liveness"],
            "since": status.get("since"),
            "raw": status.get("raw"),
            "source_id": sid,
            "sources": agreeing,
            "independent_sources": independent_count(agreeing),
            "other_values": [
                {"source_id": s, "value": st["liveness"]}
                for s, st in statuses
                if st["liveness"] != status["liveness"]
            ],
        }

    address = _pick(
        addresses, registers, same=lambda a, b: _norm_text(a) == _norm_text(b)
    )
    if address:
        address["country"] = address_country.get(address["value"], "")

    return {
        "legal_form": _pick(
            legal_forms, registers, same=lambda a, b: _norm_text(a) == _norm_text(b)
        ),
        "register_status": register_status,
        "founding_date": _pick(foundings, registers, same=_dates_agree, prefer_longer=True),
        "registered_address": address,
        "jurisdiction": jurisdiction,
        "statement_ids": [str(s.get("statementId") or "") for s in stmts],
    }
