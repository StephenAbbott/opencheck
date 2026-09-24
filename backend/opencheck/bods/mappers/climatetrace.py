"""Climate TRACE / Global Energy Monitor → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any

from ..annotations import annotate, commenting, pointer
from ..statements import (
    BODSBundle,
    _country_obj,
    make_entity_statement,
    make_relationship_statement,
)


# ----------------------------------------------------------------------
# Climate TRACE / Global Energy Monitor → BODS
# ----------------------------------------------------------------------


# GEM's Entity Type column follows BODS definitions by GEM's own documentation
# (August 2026 About sheet). "legal entity" maps to registeredEntity when the
# row carries a registry identifier (LEI), else legalEntity; blank or
# unrecognised values fall back the same way. "person" (2 rows in August 2026)
# has no honest entityStatement mapping and yields no statements at all.
_GEM_ENTITY_TYPE_MAP: dict[str, str] = {
    "state": "state",
    "state body": "stateBody",
    "arrangement": "arrangement",
    "unknown entity": "unknownEntity",
}


def _gem_entity_type(gem_row: dict[str, Any], lei: str) -> str | None:
    """BODS entityType.type for a GEM entities-CSV row, or None for 'person'.

    Deliberate consequence, accepted 2026-08-28: mapping GEM ``arrangement``
    honestly means the risk engine's TRUST_OR_ARRANGEMENT signal can fire
    from this ESG-category source.
    """
    raw = str(gem_row.get("Entity Type") or "").strip().lower()
    if raw == "person":
        return None
    mapped = _GEM_ENTITY_TYPE_MAP.get(raw)
    if mapped:
        return mapped
    return "registeredEntity" if len(lei) == 20 else "legalEntity"


def _gem_status_note(entity_status: dict[str, Any]) -> str:
    """One-sentence annotation text for a dissolved/amalgamated GEM entity."""
    status = entity_status.get("status")
    if status == "amalgamated":
        successor = (
            entity_status.get("merged_into_name")
            or entity_status.get("merged_into")
            or "another entity"
        )
        text = f"Global Energy Monitor records this entity as amalgamated into {successor}"
        if entity_status.get("merged_into_name") and entity_status.get("merged_into"):
            text += f" ({entity_status['merged_into']})"
    else:
        text = "Global Energy Monitor records this entity as dissolved"
    urls = entity_status.get("urls") or []
    if urls:
        text += ". Source: " + "; ".join(urls)
    return text + "."


#: GEM's placeholder owners. They are not parties but descriptions of who
#: holds the rest — one GEM entity each, shared across every company it
#: appears on (September 2026: ``natural person(s)`` on 1,840 companies'
#: owner rows, ``small shareholder(s)`` on 1,016, ``unknown`` in 1,448 parent
#: columns). Emitted as an entity statement, one node would join unrelated
#: companies in any merged graph, and GEM types two of them ``person``, which
#: the person refusal would silently drop along with the share. So the holding
#: is kept and the party is unspecified. Matched on GEM ID, or on the exact
#: name should GEM ever re-key them. Keys are casefolded, stripped names.
_GEM_UNIDENTIFIED_OWNERS: dict[str, str] = {
    "natural person(s)": (
        "Natural person(s) not individually identified by Global Energy Monitor"
    ),
    "member/employee owned": (
        "Members or employees, not individually identified by Global Energy Monitor"
    ),
    "small shareholder(s)": (
        "Small shareholders, not individually identified by Global Energy Monitor"
    ),
    "unknown": "An owner Global Energy Monitor has not identified",
}
_GEM_UNIDENTIFIED_OWNER_IDS: dict[str, str] = {
    "E100000123261": "natural person(s)",
    "E100002001974": "member/employee owned",
    "E100001015587": "small shareholder(s)",
    "E100000132388": "unknown",
}

#: BODS ``unspecifiedReason``: "A publisher does not have access to information
#: on this person or entity." The reason is known — GEM does not name them —
#: so the codelist's ``unknown`` ("the reason … is not known") would be wrong.
_GEM_UNIDENTIFIED_REASON = "informationUnknownToPublisher"


def _gem_unidentified_owner(entity_id: str, name: str) -> str | None:
    """The unspecified-party description for a GEM placeholder owner, else None."""
    key = _GEM_UNIDENTIFIED_OWNER_IDS.get(entity_id.strip()) or name.strip().casefold()
    return _GEM_UNIDENTIFIED_OWNERS.get(key)


def _emit_gem_interested_party(
    result: BODSBundle,
    emitted: set[str],
    *,
    party: dict[str, Any],
    subject_statement_id: str,
    local_id: str,
    interest: dict[str, Any],
    source_url: str,
) -> dict[str, Any] | None:
    """One GEM parent or owner: its entity statement (once) and the edge.

    Returns the relationship statement, or None when nothing was emitted.
    ``party`` is an adapter parent/owner dict: ``entity_id``, ``name``,
    ``entity_type`` (GEM's raw Entity Type from the party's *own* row, None
    when it has none) and ``country``.
    """
    party_eid = (party.get("entity_id") or "").strip()
    party_name = (party.get("name") or party_eid).strip()

    unidentified = _gem_unidentified_owner(party_eid, party_name)
    if unidentified is not None:
        relationship = make_relationship_statement(
            source_id="climatetrace",
            local_id=local_id,
            subject_statement_id=subject_statement_id,
            interested_party_unspecified={
                "reason": _GEM_UNIDENTIFIED_REASON,
                "description": unidentified,
            },
            interests=[interest],
            source_url=source_url,
        )
        result.statements.append(relationship)
        return relationship

    # Type the party from GEM's own Entity Type column, exactly as the subject
    # is typed. Before Phase 169 every parent was ``unknownEntity``, which
    # silently defeated the BODS SOE modelling requirement: the Republic of
    # Indonesia above PT Pertamina (Persero) is `state` in GEM's data and must
    # be `state` in ours for the structure to say "state-owned enterprise".
    party_type_raw = party.get("entity_type")
    if party_type_raw is None:
        # No row of its own in the entities CSV. Say "unknown", never guess a
        # type from the subject's row.
        party_type: str | None = "unknownEntity"
    else:
        party_type = _gem_entity_type({"Entity Type": party_type_raw}, "")
        if party_type is None:
            # GEM types a party as a natural person (placeholders aside). An
            # entity statement would misdescribe it and neither GEM column
            # asserts the facts a person statement needs, so emit neither the
            # node nor the edge — as for subjects.
            return None

    # Jurisdiction carries which state a `state` / `stateBody` node is
    # (BODS: "jurisdiction is used to represent the particular state").
    party_country = _country_obj(party.get("country") or "")
    party_jur = (
        (party_country["name"], party_country.get("code")) if party_country else None
    )

    party_entity = make_entity_statement(
        source_id="climatetrace",
        local_id=party_eid,
        name=party_name,
        jurisdiction=party_jur,
        identifiers=[
            {
                "id": party_eid,
                "scheme": "GEM-ENTITY",
                "schemeName": "Global Energy Monitor Entity ID",
            }
        ],
        entity_type=party_type,
        entity_details=(
            "State or government body (per Global Energy Monitor)"
            if party_type in {"state", "stateBody"}
            else None
        ),
        source_url=source_url,
    )
    if party_entity["statementId"] not in emitted:
        emitted.add(party_entity["statementId"])
        result.statements.append(party_entity)

    relationship = make_relationship_statement(
        source_id="climatetrace",
        local_id=local_id,
        subject_statement_id=subject_statement_id,
        interested_party_statement_id=party_entity["statementId"],
        interested_party_type="entity",
        interests=[interest],
        source_url=source_url,
    )
    result.statements.append(relationship)
    return relationship


def map_climatetrace(bundle: dict[str, Any]) -> BODSBundle:
    """Map a Climate TRACE / GEM fetch bundle to BODS statements.

    Emits:
    * One entity statement for the subject company (GEM entity identifier),
      typed from GEM's Entity Type column (which follows BODS definitions);
      joint ventures are noted in ``entityType.details``.
    * For each declared GEM parent: one stub entity statement, typed and
      located from the *parent's own* entities-CSV row (so a government parent
      arrives as ``state`` / ``stateBody`` with its jurisdiction, satisfying
      the BODS *representing state-owned enterprises* requirement that an SOE
      connect to an entity statement typed ``state`` or ``stateBody``), plus
      one ``otherInfluenceOrControl`` relationship
      (``beneficialOwnershipOrControl`` is ``False`` — parent declarations in
      GEM are corporate structure data, not beneficial ownership assertions).
      A parent with no row of its own stays ``unknownEntity``; a parent GEM
      types as a natural person is skipped entirely, as for subjects.
    * For each **direct owner** in GEM's relationships CSV (Phase 199): the
      owner's entity statement, typed and located from its own row the same
      way, plus one ``shareholding`` relationship (``directOrIndirect``
      ``direct``, ``beneficialOwnershipOrControl`` ``False``, the share when
      GEM publishes one) with GEM's citation as a ``commenting`` annotation.
      This is the file that says the Government of Indonesia holds PT
      Pertamina (Persero): its parent column names Pertamina itself. Where a
      pair is in both the column and the CSV, only the CSV edge is emitted.
      Direct owners only — the chain above them is not walked.
    * GEM's placeholder owners (``natural person(s)``, ``small
      shareholder(s)``, ``member/employee owned``, ``unknown``) become an
      unspecified interested party on either path, keeping the share, never a
      shared entity node.
    * For a dissolved or amalgamated entity (August 2026 GEOT fields): a
      ``commenting`` annotation on the subject's statement — never a
      ``dissolutionDate``, which requires a date GEM does not publish, and
      never ``recordStatus: "closed"``, which would misuse the record
      lifecycle on a first-and-only statement. An amalgamated entity's
      successor additionally gets a stub entity statement so it exists as a
      node; no relationship statement links them, because a merger is not an
      ownership or control interest and no BODS interest type fits.

    Emissions data is attached as an annotation via ``source.description``
    rather than as a BODS interest — BODS v0.4 has no concept of an
    "emissions interest" and the data is ESG context rather than ownership
    or control.
    """
    if not bundle or bundle.get("is_stub"):
        return BODSBundle()

    result = BODSBundle()

    entity_id: str = bundle.get("entity_id") or ""
    entity_name: str = bundle.get("entity_name") or entity_id
    lei: str = (bundle.get("lei") or "").strip().upper()

    if not entity_id:
        return result

    source_url = f"https://globalenergymonitor.org/"

    # Build identifiers list.
    identifiers: list[dict[str, str]] = [
        {
            "id": entity_id,
            "scheme": "GEM-ENTITY",
            "schemeName": "Global Energy Monitor Entity ID",
            "uri": f"https://globalenergymonitor.org/",
        }
    ]
    if len(lei) == 20:
        identifiers.append(
            {
                "id": lei,
                "scheme": "XI-LEI",
                "schemeName": "Global Legal Entity Identifier Index",
            }
        )

    # Determine jurisdiction from GEM row if available.
    gem_row: dict[str, str] = bundle.get("gem_row") or {}
    # GEM CSV uses "Headquarters Country" (ISO 3166-1 alpha-3), not "Country".
    country_raw: str = (
        gem_row.get("Headquarters Country")
        or gem_row.get("Registration Country")
        or gem_row.get("Country")
        or ""
    ).strip()
    jurisdiction = _country_obj(country_raw) if country_raw else None
    jur_tuple: tuple[str, str | None] | None = (
        (jurisdiction["name"], jurisdiction.get("code")) if jurisdiction else None
    )

    entity_type = _gem_entity_type(gem_row, lei)
    if entity_type is None:
        # GEM types a handful of records as natural persons — an
        # entityStatement would misdescribe them, so emit nothing.
        return result

    entity_status: dict[str, Any] = bundle.get("entity_status") or {}

    entity = make_entity_statement(
        source_id="climatetrace",
        local_id=entity_id,
        name=entity_name,
        jurisdiction=jur_tuple,
        identifiers=identifiers,
        entity_type=entity_type,
        entity_details=(
            "Joint venture (per Global Energy Monitor)"
            if entity_status.get("jv")
            else None
        ),
        source_url=source_url,
    )
    if entity_status.get("status") in ("dissolved", "amalgamated"):
        annotate(
            entity,
            commenting(pointer("recordDetails"), _gem_status_note(entity_status)),
        )
    result.statements.append(entity)
    subject_statement_id: str = entity["statementId"]

    # A stub statement for the amalgamation successor, so "merged into X"
    # names a node that exists in the bundle. Deliberately NO relationship
    # statement: a merger is not an ownership or control interest.
    successor_id = (entity_status.get("merged_into") or "").strip()
    if successor_id:
        successor_lei = (entity_status.get("merged_into_lei") or "").strip().upper()
        successor_identifiers: list[dict[str, str]] = [
            {
                "id": successor_id,
                "scheme": "GEM-ENTITY",
                "schemeName": "Global Energy Monitor Entity ID",
            }
        ]
        if len(successor_lei) == 20:
            successor_identifiers.append(
                {
                    "id": successor_lei,
                    "scheme": "XI-LEI",
                    "schemeName": "Global Legal Entity Identifier Index",
                }
            )
        successor = make_entity_statement(
            source_id="climatetrace",
            local_id=successor_id,
            name=entity_status.get("merged_into_name") or successor_id,
            identifiers=successor_identifiers,
            entity_type="registeredEntity" if len(successor_lei) == 20 else "unknownEntity",
            source_url=source_url,
        )
        annotate(
            successor,
            commenting(
                pointer("recordDetails"),
                (
                    f"Successor entity: Global Energy Monitor records "
                    f"{entity_name} as merged into this entity."
                ),
            ),
        )
    else:
        successor = None

    # A successor that is also a parent or a direct owner (Simhapuri Energy Ltd
    # merged into its own parent, Jindal Power Ltd) is emitted by the party
    # path instead, which types and locates it from its own row — the stub
    # above knows only an LEI — and the successor note is carried across.
    party_ids = {
        (p.get("entity_id") or "").strip() for p in bundle.get("parents") or []
    } | {(o.get("entity_id") or "").strip() for o in bundle.get("owners") or []}
    deferred_successor = successor if successor and successor_id in party_ids else None
    if successor and deferred_successor is None:
        result.statements.append(successor)

    # Every GEM entity statement emitted so far, by statementId. A party can
    # be reached twice — named in the parent column *and* as a direct owner,
    # or as an owner and the amalgamation successor — and an entity statement
    # is emitted once whichever path reaches it first. The IDs coincide by
    # construction (``_stable_id("climatetrace", "entity", <GEM ID>)``) and a
    # test pins that, so this set is what stops a duplicate, not luck.
    emitted: set[str] = {s["statementId"] for s in result.statements}

    # GEM's relationships CSV — the subject's *direct* owners (Phase 199).
    owners = [
        o for o in bundle.get("owners") or [] if (o.get("entity_id") or "").strip()
    ]
    owner_ids = {(o.get("entity_id") or "").strip() for o in owners}

    # Emit stub entity + relationship for each declared parent.
    for parent in bundle.get("parents") or []:
        parent_eid = (parent.get("entity_id") or "").strip()
        if not parent_eid:
            continue

        if parent_eid == entity_id:
            # GEM's "Gem parents" column names the top of the group *within
            # GEM's own universe*, so a group parent is listed as its own
            # parent (5,132 rows in the September 2026 snapshot, PT Pertamina
            # (Persero) among them). Emitting that would produce a
            # relationship whose subject and interestedParty are the same
            # statement — an entity owning itself, which is meaningless in
            # BODS and draws a self-loop in any BOVS diagram.
            continue

        if parent_eid in owner_ids:
            # The same subject→party pair is in the relationships CSV, which
            # carries the precise share (28.85, where the column rounds to
            # 28.8 — or has no share at all), GEM's citation and the stronger
            # claim. One relationship per pair: the CSV's, emitted below.
            # 8,514 of the column's 18,784 non-self edges are replaced this way.
            continue

        interest: dict[str, Any] = {
            "type": "otherInfluenceOrControl",
            "beneficialOwnershipOrControl": False,
            "details": (
                "Parent organisation declared in GEM ownership tracker "
                "(not a beneficial ownership assertion)"
            ),
        }
        # GEM publishes the parent's share for most entities (parsed from the
        # "Gem parents IDs" column by the adapter, e.g. "E1000… [55.0%]").
        share = parent.get("share")
        if isinstance(share, (int, float)):
            interest["share"] = {"exact": float(share)}

        _emit_gem_interested_party(
            result,
            emitted,
            party=parent,
            subject_statement_id=subject_statement_id,
            local_id=f"{entity_id}-parent-{parent_eid}",
            interest=interest,
            source_url=source_url,
        )

    for owner in owners:
        owner_eid = (owner.get("entity_id") or "").strip()
        if owner_eid == entity_id:
            # 64 rows in the relationships CSV name an entity as its own
            # owner — the same self-loop the parent column is guarded for.
            continue

        interest = {
            # A named owner with a share and a citation is a holding, not the
            # "parent organisation" corporate-structure claim the parent column
            # makes. GEM records these as immediate relationships, so the
            # holding is direct; where a group also holds through a subsidiary
            # (CPFL: 51 % direct, 49 % via CPFL Geração) that is a second,
            # separate edge in GEM's data, never summed here.
            "type": "shareholding",
            "directOrIndirect": "direct",
            "beneficialOwnershipOrControl": False,
            "details": (
                "Direct owner recorded in GEM ownership tracker "
                "(not a beneficial ownership assertion)"
            ),
        }
        share = owner.get("share")
        if isinstance(share, (int, float)):
            interest["share"] = {"exact": float(share)}

        relationship = _emit_gem_interested_party(
            result,
            emitted,
            party=owner,
            subject_statement_id=subject_statement_id,
            local_id=f"{entity_id}-owner-{owner_eid}",
            interest=interest,
            source_url=source_url,
        )
        urls = [u for u in owner.get("source_urls") or [] if isinstance(u, str) and u]
        if relationship is not None and urls:
            annotate(
                relationship,
                commenting(
                    pointer("recordDetails", "interests", 0),
                    "Global Energy Monitor cites: " + "; ".join(urls),
                ),
            )

    if deferred_successor is not None:
        party_statement = next(
            (
                s
                for s in result.statements
                if s["statementId"] == deferred_successor["statementId"]
            ),
            None,
        )
        if party_statement is None:
            # The party path emitted no node for it (GEM types it a person).
            result.statements.append(deferred_successor)
        else:
            annotate(party_statement, *deferred_successor.get("annotations", []))
            known = {
                (i.get("scheme"), i.get("id"))
                for i in party_statement["recordDetails"].get("identifiers", [])
            }
            for identifier in deferred_successor["recordDetails"].get("identifiers", []):
                if (identifier.get("scheme"), identifier.get("id")) not in known:
                    party_statement["recordDetails"].setdefault("identifiers", []).append(
                        identifier
                    )

    return result
