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
  said it is the one named. GLEIF's ACTIVE on an LEI no issuer maintains is
  ``declared`` (Phase 291) and ranks last of the stated classes.
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

from . import lei_registration as _lei_reg
from .bods import former_names as _former_names
from .bods import gleif_events as _gleif_events
from .bods import liveness as _liveness
from .names import display_name_key
from .consistency import referent_groups, source_id_of
from .matching import canonical_identifier
from .reconcile import _entity_jurisdiction, _identifier_keys
from .sources.lineage import independent_count, national_register_ids

#: Phase 291: GLEIF's entity status when no issuer re-checks it any more —
#: the LEI is lapsed, retired, annulled and so on. A profile-only class, not
#: a BODS one: the GLEIF statement still says what GLEIF says (ACTIVE), and
#: ``bods.liveness`` and ``consistency`` read it unchanged. It is neither live
#: nor dissolved; it is the last declaration, dated by the missed renewal.
DECLARED = "declared"

#: Liveness classes, worst first. ``declared`` ranks after ``live`` so any
#: source that actually reads the register — Companies House, OpenCorporates —
#: is shown before GLEIF's last declaration, whichever class it reports.
_LIVENESS_RANK = {
    _liveness.TERMINAL: 0,
    _liveness.PENDING: 1,
    _liveness.LIVE: 2,
    DECLARED: 3,
}


def _rd(stmt: dict[str, Any]) -> dict[str, Any]:
    return stmt.get("recordDetails") or {}


def _is_subject_lei(stmt: dict[str, Any], lei: str) -> bool:
    for ident in _rd(stmt).get("identifiers") or []:
        if not isinstance(ident, dict):
            continue
        if ident.get("scheme") == "XI-LEI" and str(ident.get("id") or "").upper() == lei:
            return True
    return False


def subject_keys(ref: str) -> frozenset[str]:
    """The identifier-merge keys (``reconcile._identifier_keys``) that name
    the subject ``ref`` refers to.

    A subject reference (Phase 290) is either an LEI — ``LEI:<lei>`` — or a
    register-scoped identifier written ``<SCHEME>:<id>`` (``GB-COH:OC346224``),
    which an entity statement carries under that scheme, as the mappers'
    ``REG-<country>`` fallback for an unnamed register (``REG-GB:OC346224``,
    what OpenAleph's UK records and a PSC filed as registered in "England And
    Wales" carry), or as the jurisdiction-scoped bare register number
    (``JUR:GB:OC346224``).
    One function, so ``subject_statements``, ``subject_identity`` and the
    screens agree on what "the subject" is without an LEI.
    """
    ref = (ref or "").strip().upper()
    if not ref:
        return frozenset()
    if ":" not in ref:
        return frozenset({f"LEI:{ref}"})
    scheme, raw = ref.split(":", 1)
    value = canonical_identifier(raw, min_len=0) or raw
    keys = {f"{scheme}:{value}"}
    country = scheme[4:] if scheme.startswith("REG-") else scheme.split("-", 1)[0]
    if country and len(country) == 2:
        keys.add(f"JUR:{country}:{value}")
        keys.add(f"REG-{country}:{value}")
    return frozenset(keys)


def is_subject(stmt: dict[str, Any], ref: str) -> bool:
    """Does this entity statement carry the subject reference ``ref``?"""
    if not ref:
        return False
    if ":" not in ref:
        return _is_subject_lei(stmt, ref.strip().upper())
    return bool(_identifier_keys(stmt) & subject_keys(ref))


def subject_statements(lei: str, bods: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The entity statements that describe the looked-up subject.

    ``lei`` is a subject reference: an LEI, or — Phase 290, for a lookup
    anchored on a register number — ``<SCHEME>:<id>`` (see ``subject_keys``).
    The referent group containing a statement that carries the reference;
    when no group has formed (one source only), the statements carrying it
    themselves.
    """
    lei = lei.strip().upper()
    if not lei:
        return []
    for group in referent_groups(bods):
        if any(is_subject(s, lei) for s in group):
            return group
    return [
        s for s in bods
        if s.get("recordType") == "entity" and is_subject(s, lei)
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


def build_subject_profile(
    lei: str,
    bods: list[dict[str, Any]],
    *,
    lei_registration: dict[str, Any] | None = None,
    lei_successor: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """The four profile fields for ``lei``, or ``None`` with no subject statement.

    Shape::

        {
          "legal_form":  {"value", "sources", "independent_sources", "other_values"} | None,
          "register_status": {"liveness", "since", "raw", "source_id", "sources",
                              "independent_sources", "other_values"} | None,
              # liveness "declared" (Phase 291) adds "lei_registration_status"
              # and "sentence"; "since" is then the missed renewal date.
          "founding_date": {...} | None,
          "registered_address": {"value", "country", "sources", ...} | None,
          "jurisdiction": "GB" | None,
          "lei_registration": {...} | None,
          "lei_successor": {...} | None,
          "former_names": [{"name", "until", "from", "sources"}],   # Phase 309
          "name_changed_on": "2025-05-06" | None,                   # Phase 309
          "statement_ids": [...],
        }

    ``lei_registration`` (Phase 242) is the LEI record's own status —
    ``opencheck.lei_registration`` — passed in from the GLEIF anchor, because
    BODS has no field for it. It sits beside ``register_status``, and a lapsed
    LEI is never read as a dissolved company — but since Phase 291 it does
    stop GLEIF's own ACTIVE reading as ``live``: with no issuer re-checking
    it, that status is ``declared``, the last thing the company told its
    issuer. A register or OpenCorporates status is unaffected and outranks it.

    ``former_names`` (Phase 309) are the names the registers say the company
    *had* — read back from the mappers' former-name annotations
    (``bods.former_names``), never from the untyped ``alternateNames`` list,
    so a trading name is never called former. Deduplicated across sources on
    case and spacing, dated ones first (latest first), each naming the
    sources that state it. ``name_changed_on`` is the day of the latest
    completed legal-name change GLEIF records (the Phase 305 annotation) —
    the change is dated, which former name it closed is not.

    ``lei_successor`` (Phase 307) is the successor GLEIF names on the anchor,
    followed through the Golden Copy mirror — ``opencheck.lei_successor`` —
    passed in the same way and for the same reason. ``None`` when GLEIF names
    none, which is the ordinary dissolved company; it is carried as GLEIF's
    assertion and never read as the company's status.
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

    # Phase 291: GLEIF's ACTIVE on an LEI nobody maintains is the last thing
    # the company declared to its issuer, not a live reading. Only ``live`` is
    # demoted — a GLEIF INACTIVE stays terminal, and a lapse is never read as
    # dissolution (Phase 242).
    declared = _lei_reg.entity_status_is_declared(lei_registration)
    if declared:
        statuses = [
            (sid, {**st, "liveness": DECLARED})
            if sid == "gleif" and st.get("liveness") == _liveness.LIVE
            else (sid, st)
            for sid, st in statuses
        ]

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
        if status["liveness"] == DECLARED and lei_registration:
            # Dated by the renewal GLEIF says was missed (LAPSED only), never
            # by ``lastUpdateDate``; the sentence is frozen with the run.
            register_status["since"] = lei_registration.get("since")
            register_status["lei_registration_status"] = lei_registration.get("status")
            register_status["sentence"] = _lei_reg.declared_sentence(
                status.get("raw"), lei_registration
            )

    address = _pick(
        addresses, registers, same=lambda a, b: _norm_text(a) == _norm_text(b)
    )
    if address:
        address["country"] = address_country.get(address["value"], "")

    former_names = _former_names_across(stmts)
    name_changed_on = _latest_name_change(stmts)

    return {
        "legal_form": _pick(
            legal_forms, registers, same=lambda a, b: _norm_text(a) == _norm_text(b)
        ),
        "register_status": register_status,
        "founding_date": _pick(foundings, registers, same=_dates_agree, prefer_longer=True),
        "registered_address": address,
        "jurisdiction": jurisdiction,
        "lei_registration": lei_registration,
        "lei_successor": lei_successor,
        "former_names": former_names,
        "name_changed_on": name_changed_on,
        "statement_ids": [str(s.get("statementId") or "") for s in stmts],
    }


def _former_names_across(stmts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Former names from every subject statement, merged on case and spacing
    (``display_name_key``): the first spelling seen is kept, every source that
    states the name is listed, and a date from any source fills a gap. Dated
    names first, latest ``until`` first, then file order."""
    merged: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for stmt in stmts:
        sid = source_id_of(stmt)
        for entry in _former_names.former_names_of(stmt):
            key = display_name_key(entry["name"])
            if not key:
                continue
            if key not in merged:
                merged[key] = {"name": entry["name"], "until": entry.get("until"), "from": entry.get("from"), "sources": []}
                order.append(key)
            row = merged[key]
            if sid and sid not in row["sources"]:
                row["sources"].append(sid)
            for field in ("until", "from"):
                if not row.get(field) and entry.get(field):
                    row[field] = entry[field]
    rows = [merged[k] for k in order]
    dated = sorted((r for r in rows if r.get("until")), key=lambda r: str(r["until"]), reverse=True)
    undated = [r for r in rows if not r.get("until")]
    return dated + undated


def _latest_name_change(stmts: list[dict[str, Any]]) -> str | None:
    """The latest completed CHANGE_LEGAL_NAME day GLEIF records on the
    subject (Phase 305's annotations), or ``None``."""
    best: str | None = None
    for stmt in stmts:
        for annotation in stmt.get("annotations") or []:
            event = annotation.get(_gleif_events.EVENT_PROPERTY)
            if not isinstance(event, dict):
                continue
            if str(event.get("type") or "").upper() != "CHANGE_LEGAL_NAME":
                continue
            if str(event.get("status") or "").upper() != "COMPLETED":
                continue
            day = event.get("effectiveDay")
            if day and (best is None or day > best):
                best = day
    return best
