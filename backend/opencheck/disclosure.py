"""What each source discloses about the subject's owners — one normalised value
per source (the "Normalised disclosure-status field and chip per source"
ticket, from the Open Ownership usable-data check, 3 Nov 2026).

Open Ownership's point: absence from a dataset is not an exemption. OpenCheck
already knew most of the answer through separate channels — GLEIF reporting
exceptions, Companies House PSC statements mapped to ``unspecifiedReason``,
Ukraine's filed absences, Estonia's "due, none published" wording, the
``degraded_sources`` list and the knowability access status. This module reads
them all and says, per source, which of a closed set of values applies, for
what the source covers.

Stephen's decisions (3 Nov 2026), which the code implements:

* **Describes, raises nothing.** No signal, no verdict clause, no badge.
  ``OPAQUE_OWNERSHIP`` and ``GLEIF_REPORTING_EXCEPTION`` are unchanged; the
  risk engine, the verdict and the narrative must not import this module
  (``tests/test_disclosure_status.py`` pins it).
* **Every value a source supports is listed**, never one picked by
  precedence: Companies House naming three PSCs and filing "at least one
  beneficial owner unidentified" is ``declared`` *and* ``not_provided``.
* **Each entry says what it covers** — ``beneficial_owners``,
  ``shareholders`` or ``parent`` — so GLEIF's consolidation exceptions sit on
  the same scale without reading as beneficial-ownership exemptions.
* **``not_covered`` is a value, not a chip**: a source whose job does not
  include owners (``DISCLOSES`` has no row for it) carries it in the API and
  is named on one line in the UI.
* **``restricted`` only for a national register OpenCheck reads** whose
  country's beneficial-ownership tier is closed under the knowability access
  status — never inferred from a jurisdiction for any other source. Frozen
  with the run, like everything here.
* **Two values beyond the ticket's seven** (Stephen, 3 Nov 2026):
  ``none_filed`` — the source holds the company's record but no owner and no
  reason; ``no_record`` — the source has no record of the company. Neither is
  ever worded as an exemption or as non-compliance.
* **BODS gets one ``commenting`` annotation** on OpenCheck's own subject
  statement (``apply_to_bods``), never a new statement.

Only current relationships count; an officer role is never an owner. The
mapping from every channel to the values is ``docs/disclosure-status.md``.
"""

from __future__ import annotations

import copy
from collections.abc import Iterable, Mapping
from typing import Any

from .bods.lifecycle import interest_ended, record_closed
from .bods.officer_roles import officer_position
from .bods.refs import party_ref, resolver, unspecified_party
from .bods.source_ids import source_id_of
from .degradation import CHECK_SOURCE_FETCH, CHECK_SOURCE_READ

#: The closed vocabulary, in reading order. Entries are sorted by it.
VALUES: tuple[str, ...] = (
    "declared",
    "none_exist",
    "exempt",
    "not_provided",
    "none_filed",
    "restricted",
    "unreachable",
    "no_record",
    "not_covered",
)

#: What a value is about.
COVERS: tuple[str, ...] = ("beneficial_owners", "shareholders", "parent")

#: The words, shared with ``frontend/src/lib/disclosureStatus.ts`` (parsed by
#: the test). Describes; never judges — ``knowability.BANNED_VOCABULARY``.
VALUE_WORDS: dict[str, str] = {
    "declared": "Declared",
    "none_exist": "Source says none exist",
    "exempt": "Exempt",
    "not_provided": "Not provided",
    "none_filed": "None on record",
    "restricted": "Not public",
    "unreachable": "Not read",
    "no_record": "No record of the company",
    "not_covered": "Not an ownership source",
}

COVERS_WORDS: dict[str, str] = {
    "beneficial_owners": "Beneficial owners",
    "shareholders": "Shareholders",
    "parent": "Parent",
}

#: Sources whose job includes owners, and what they cover, primary first.
#: Every ``bo_register`` regime in ``bods.bo_regimes`` is here with
#: ``beneficial_owners`` (pinned). A source missing from the table is
#: ``not_covered`` — it may still carry an owner now and then (an
#: OpenSanctions ownership record), but it is not read as a source of them.
#: Taiwan's GCIS is left out on purpose: it publishes directors' and
#: supervisors' holdings, never "the shareholders". SEC EDGAR's 13D/13G
#: filers are the SEC's beneficial owners of 5% or more of a class — holders
#: of shares, so ``shareholders``, never the AML sense.
DISCLOSES: dict[str, tuple[str, ...]] = {
    "companies_house": ("beneficial_owners",),
    "ariregister": ("beneficial_owners", "shareholders"),
    "edr_ukraine": ("beneficial_owners", "shareholders"),
    "ur_latvia": ("beneficial_owners", "shareholders"),
    "rpvs_slovakia": ("beneficial_owners",),
    "cac_nigeria": ("beneficial_owners",),
    "dlcp_dc": ("beneficial_owners",),
    "eiti_bo": ("beneficial_owners",),
    "gleif": ("parent",),
    "sec_edgar": ("shareholders",),
    "nz_companies": ("shareholders",),
    "ares": ("shareholders",),
    "firmenbuch": ("shareholders",),
    "cnpj_brazil": ("shareholders",),
    "gemi_greece": ("shareholders",),
    "asp_moldova": ("shareholders",),
    "climatetrace": ("shareholders",),
    "wikidata": ("shareholders",),
}

#: ``unspecifiedReason`` → value. ``informationUnknownToPublisher`` is a party
#: that exists unnamed (Ukraine's "ФІЗИЧНІ ОСОБИ" founders, GEM's
#: placeholders) — an owner declared, so ``declared``. Anything else
#: (``unknown``, a blank absence) is ``none_filed``.
REASON_VALUES: dict[str, str] = {
    "noBeneficialOwners": "none_exist",
    "subjectExemptFromDisclosure": "exempt",
    "interestedPartyExemptFromDisclosure": "exempt",
    "subjectUnableToConfirmOrIdentifyBeneficialOwner": "not_provided",
    "interestedPartyHasNotProvidedInformation": "not_provided",
    "informationUnknownToPublisher": "declared",
}

#: The knowability access statuses under which a register's beneficial-owner
#: tier is not public (``bo_access._RESTRICTED_NOW``, pinned).
RESTRICTED_ACCESS: frozenset[str] = frozenset(
    {
        "legitimate_interest",
        "restricted_no_lia_route_yet",
        "authorities_and_obliged_entities_only",
    }
)

_OFFICER_TYPES = frozenset({"seniorManagingOfficial", "boardMember", "boardChair"})


# --- per-bundle facts the statements do not carry ---------------------------


def bundle_hints(source_id: str, raw: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    """Entries a source's raw bundle says and its statements cannot.

    Estonia's general data says whether a company must file beneficial owners
    at all, and its fallback rows (``J`` / ``C``) name board members because
    no owner was identified — neither becomes a relationship that reads as
    such. Read where the pipeline still holds the raw bundle (the deepen), so
    the entries ride the frozen event. "Due, none published" is
    ``none_filed``, not ``not_provided``: an entry can be withheld for a
    person at risk or a minor, and the open data does not say so.
    """
    if source_id != "ariregister" or not isinstance(raw, Mapping):
        return []
    if raw.get("bo_not_read"):
        return [_entry("unreachable", "beneficial_owners",
                       detail="beneficial owners not read from this service")]
    from .bods.mappers.estonia import ee_bo_is_fallback  # local: avoids a cycle

    rows = list(raw.get("beneficial_owners") or [])
    named = [r for r in rows if ee_bo_is_fallback(r)]
    if rows and len(named) == len(rows):
        n = len(named)
        return [_entry(
            "none_exist", "beneficial_owners",
            detail=f"no owner identified; {n} board or council member{'' if n == 1 else 's'} named",
        )]
    if rows:
        return []
    general = raw.get("general") or {}
    if general.get("bo_obliged") is False:
        return [_entry("exempt", "beneficial_owners",
                       detail="the register records that the company need not file beneficial owners")]
    if general.get("bo_obliged") is True:
        return [_entry("none_filed", "beneficial_owners", detail="due, none published")]
    return []


# --- reading one source's statements ----------------------------------------


def _entry(value: str, covers: str | None, *, count: int | None = None,
           detail: str | None = None, reason: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"value": value, "covers": covers}
    if count is not None:
        out["count"] = count
    if detail:
        out["details"] = [detail]
    if reason:
        out["reasons"] = [reason]
    return out


def _current_ownership_interests(rel: Mapping[str, Any], as_of: str) -> list[dict[str, Any]]:
    """The relationship's current interests that are not officer roles."""
    closed = record_closed(dict(rel))
    out = []
    for interest in (rel.get("recordDetails") or {}).get("interests") or []:
        if not isinstance(interest, dict) or interest_ended(interest, closed, as_of):
            continue
        if interest.get("type") in _OFFICER_TYPES or officer_position(interest):
            continue
        out.append(interest)
    return out


def _covers_of(interests: list[dict[str, Any]], offered: tuple[str, ...]) -> str:
    """What one relationship is about, among what the source offers."""
    if "beneficial_owners" in offered and any(
        i.get("beneficialOwnershipOrControl") is True for i in interests
    ):
        return "beneficial_owners"
    if "shareholders" in offered and any(i.get("type") == "shareholding" for i in interests):
        return "shareholders"
    return offered[0]


def entries_from_statements(
    source_id: str, statements: list[dict[str, Any]], as_of: str
) -> list[dict[str, Any]]:
    """The entries for one source's own statements.

    The subject is whatever the statements' ``declarationSubject`` names — the
    declaration-subject phase stamps every source's statements with that
    source's own subject record. Only relationships *about* the subject count.
    """
    from .bods.mapper import gleif_reporting_exception  # local: mapper is heavy

    offered = DISCLOSES.get(source_id, ())
    if not offered:
        return []
    resolve = resolver(statements)
    subjects = {
        resolve(s.get("declarationSubject"))
        for s in statements
        if s.get("declarationSubject")
    }

    entries: list[dict[str, Any]] = []
    declared: dict[str, set[str]] = {}
    for rel in statements:
        if rel.get("recordType") != "relationship":
            continue
        rd = rel.get("recordDetails") or {}
        if resolve(rd.get("subject")) not in subjects:
            continue
        exc = gleif_reporting_exception(rel)
        if exc is not None:
            value = "not_provided" if exc["undisclosed"] else "exempt"
            entries.append(_entry(value, "parent", detail=exc.get("meaning") or None,
                                  reason=exc["reason"]))
            continue
        interests = _current_ownership_interests(rel, as_of)
        all_interests = rd.get("interests") or []
        ip = rd.get("interestedParty")
        unspec = unspecified_party(ip)
        if unspec is not None:
            # A filed absence carries no interest; a PSC statement an
            # ``unknownInterest``. Either way it is about what the source covers.
            if all_interests and not interests:
                continue  # ended, or an officer placeholder
            reason = unspec.get("reason") or ""
            value = REASON_VALUES.get(reason, "none_filed")
            covers = _covers_of(interests, offered)
            if value == "declared":
                declared.setdefault(covers, set()).add(rel.get("statementId") or "")
                continue
            entries.append(_entry(value, covers, detail=unspec.get("description"),
                                  reason=reason or None))
            continue
        if not interests and all_interests:
            continue  # ended, or an officer role
        if not interests and record_closed(dict(rel)):
            continue
        party = resolve(party_ref(ip)) or rel.get("statementId") or ""
        declared.setdefault(_covers_of(interests, offered), set()).add(party)

    for covers, parties in declared.items():
        entries.append(_entry("declared", covers, count=len(parties)))
    return entries


# --- one lookup --------------------------------------------------------------


def _merge(entries: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """One entry per (value, covers), counts summed, details and reasons kept
    once each, in ``VALUES`` then ``COVERS`` order."""
    merged: dict[tuple[str, str | None], dict[str, Any]] = {}
    for e in entries:
        key = (e["value"], e.get("covers"))
        cur = merged.get(key)
        if cur is None:
            merged[key] = copy.deepcopy(e)
            continue
        if "count" in e:
            cur["count"] = cur.get("count", 0) + e["count"]
        for field in ("details", "reasons"):
            for v in e.get(field) or []:
                if v not in cur.setdefault(field, []):
                    cur[field].append(v)
    cov_order = {c: i for i, c in enumerate(COVERS)}
    val_order = {v: i for i, v in enumerate(VALUES)}
    return sorted(
        merged.values(),
        key=lambda e: (val_order[e["value"]], cov_order.get(e.get("covers") or "", 99)),
    )


def for_lookup(
    *,
    bods: list[dict[str, Any]],
    coverage: Mapping[str, Any],
    degraded: Iterable[Mapping[str, Any]],
    knowability: Mapping[str, Any] | None,
    hints: Mapping[str, list[dict[str, Any]]] | None,
    as_of: str,
    source_countries: Mapping[str, str | None] | None = None,
) -> dict[str, Any]:
    """The ``disclosure_status`` event payload: a value for every applicable
    source.

    ``coverage`` is ``coverage.source_coverage(...)``; ``source_countries``
    maps a source id to its register's country (``SourceInfo.country``),
    defaulting to the REGISTRY.
    """
    if source_countries is None:
        from .sources import REGISTRY

        source_countries = {sid: a.info.country for sid, a in REGISTRY.items()}
    hints = hints or {}

    by_source: dict[str, list[dict[str, Any]]] = {}
    for stmt in bods:
        sid = source_id_of(stmt)
        if sid:
            by_source.setdefault(sid, []).append(stmt)

    fetch_failed: set[str] = set()
    read_failed: set[str] = set()
    for d in degraded:
        sid = d.get("source_id")
        if not sid:
            continue
        if d.get("check") == CHECK_SOURCE_FETCH:
            fetch_failed.add(sid)
        elif d.get("check") == CHECK_SOURCE_READ:
            read_failed.add(sid)

    k = knowability or {}
    k_country = str(k.get("code") or "").split("-", 1)[0].upper() or None
    k_closed = (k.get("access") in RESTRICTED_ACCESS) and not k.get("stated_absence")

    no_record_ids = set(coverage.get("no_record_ids") or [])
    failed_ids = set(coverage.get("failed_ids") or [])
    applicable = list(coverage.get("applicable_ids") or [])

    sources: dict[str, dict[str, Any]] = {}
    for sid in applicable:
        offered = DISCLOSES.get(sid, ())
        restricted_here = bool(
            k_closed and k_country and (source_countries.get(sid) or "").upper() == k_country
        )
        about = offered or (("beneficial_owners",) if restricted_here else ())
        entries: list[dict[str, Any]] = []
        if sid in fetch_failed or sid in failed_ids:
            entries.append(_entry("unreachable", about[0]) if about else _entry("not_covered", None))
        elif sid in no_record_ids and sid not in by_source:
            entries.append(_entry("no_record" if about else "not_covered", None))
        else:
            entries.extend(entries_from_statements(sid, by_source.get(sid, []), as_of))
            entries.extend(copy.deepcopy(hints.get(sid) or []))
            covered = {e.get("covers") for e in entries}
            for covers in offered:
                if covers in covered:
                    continue
                if covers == "beneficial_owners" and restricted_here:
                    entries.append(_entry("restricted", covers))
                else:
                    entries.append(_entry("none_filed", covers))
            if restricted_here and "beneficial_owners" not in offered:
                entries.append(_entry("restricted", "beneficial_owners"))
            if sid in read_failed and offered:
                entries.append(_entry("unreachable", offered[0], detail="answered only in part"))
            if not entries:
                entries.append(_entry("not_covered", None))
        merged = _merge(entries)
        sources[sid] = {
            "values": list(dict.fromkeys(e["value"] for e in merged)),
            "entries": merged,
            "ownership_source": bool(offered),
        }

    not_covered = [sid for sid, s in sources.items() if s["values"] == ["not_covered"]]
    return {"sources": sources, "not_covered": not_covered, "as_of": as_of}


# --- words -------------------------------------------------------------------


def entry_text(entry: Mapping[str, Any]) -> str:
    """"Beneficial owners: Declared (3)" — one entry in words, with the
    source's own words after a dash."""
    words = VALUE_WORDS.get(entry.get("value", ""), str(entry.get("value")))
    if entry.get("count"):
        words += f" ({entry['count']})"
    covers = entry.get("covers")
    head = f"{COVERS_WORDS[covers]}: " if covers in COVERS_WORDS else ""
    detail = "; ".join(entry.get("details") or [])
    return head + words + (f" — {detail}" if detail else "")


def source_line(entries: Iterable[Mapping[str, Any]]) -> str:
    return "; ".join(entry_text(e) for e in entries)


def report_rows(payload: Mapping[str, Any] | None) -> list[tuple[str, str]]:
    """``(source_id, line)`` for every source that is not ``not_covered``."""
    out = []
    for sid, s in ((payload or {}).get("sources") or {}).items():
        if s.get("values") == ["not_covered"]:
            continue
        out.append((sid, source_line(s.get("entries") or [])))
    return out


REPORT_HEADING = "What each source says about owners"
REPORT_COLUMNS: tuple[str, ...] = ("Source", "Disclosure")


def report_table(payload: Mapping[str, Any] | None) -> list[tuple[str, str]]:
    """``(source name, line)`` rows for the PDF/Markdown section."""
    from .bods.statements import SOURCE_NAMES

    return [(SOURCE_NAMES.get(sid, sid), line) for sid, line in report_rows(payload)]


def report_footer(payload: Mapping[str, Any] | None) -> list[str]:
    """The notes under the table: the not-covered line, then the rule."""
    from .bods.statements import SOURCE_NAMES

    out = []
    names = [SOURCE_NAMES.get(sid, sid) for sid in (payload or {}).get("not_covered") or []]
    if names:
        out.append("Not ownership sources: " + ", ".join(names) + ".")
    out.append(
        "Each value is what the source itself says, for what it covers. Absence "
        "from a source is not an exemption: \"None on record\" means the record "
        "carries no owner and no reason. These values describe; none feeds the "
        "risk signals or the verdict."
        + (f" As of {payload['as_of']}." if payload and payload.get("as_of") else "")
    )
    return out


def apply_to_bods(
    bods: list[dict[str, Any]],
    payload: Mapping[str, Any] | None,
    subject_ids: Iterable[str],
) -> list[dict[str, Any]]:
    """*bods* with one ``commenting`` annotation on OpenCheck's own subject
    statement (``integrated_ownership.annotation_target``) stating each
    source's disclosure status. Copied, never mutated; nothing is added when
    no source says anything."""
    from .bods.statements import SOURCE_NAMES
    from .integrated_ownership import annotation_target

    rows = report_rows(payload)
    ids = set(subject_ids)
    if not rows or not ids:
        return bods
    idx = annotation_target(bods, ids)
    if idx is None:
        return bods
    stmt = copy.deepcopy(bods[idx])
    lines = " | ".join(f"{SOURCE_NAMES.get(sid, sid)} — {line}" for sid, line in rows)
    ann: dict[str, Any] = {
        "statementPointerTarget": "/recordDetails",
        "motivation": "commenting",
        "description": (
            "Disclosure status by source, added by OpenCheck: what each source "
            f"says about this company's owners. {lines}. Absence from a source is "
            "not an exemption; see https://github.com/StephenAbbott/opencheck/blob/main/"
            "docs/disclosure-status.md for how each value maps to the BODS "
            "unspecifiedReason codelist."
        ),
        "createdBy": {"name": "OpenCheck", "uri": "https://opencheck.world"},
    }
    if payload and payload.get("as_of"):
        ann["creationDate"] = payload["as_of"]
    stmt["annotations"] = [*(stmt.get("annotations") or []), ann]
    out = list(bods)
    out[idx] = stmt
    return out
