"""ESTMA (payments to governments, Canada) → BODS v0.4.

One entity statement per ESTMA entity the crosswalk ties to the LEI. A
payment to a government is activity, not ownership or control, so nothing
else is emitted — the ``eiti`` and ``eiti_zambia`` precedent. An entity whose
payments ESTMA files in another entity's report is **not** related to it:
that is a reporting arrangement under the Act, said in a ``commenting``
annotation and never drawn as a relationship.

Identifier corroboration: the ESTMA ID is the dataset's own key for every
row it files, so ``CA-ESTMA`` is asserted. The LEI is OpenCheck's reviewed
name match and rides as an ``identifying`` annotation.

No ``jurisdiction``: ESTMA's ``JURISDICTION`` column is the jurisdiction
whose reporting requirements a report satisfies (Canada, or the one whose
report was substituted), not where the entity is incorporated.
"""

from __future__ import annotations

from typing import Any, Iterable

from ..annotations import annotate, commenting, identifying, pointer
from ..statements import content_addressed, dated_by_cut, make_entity_statement

#: OpenCheck's scheme for the ESTMA entity ID. org-id.guide lists none, so the
#: code is coined here, as ``ZM-TPIN`` is for Zambia.
ESTMA_SCHEME = ("CA-ESTMA", "ESTMA entity ID — Natural Resources Canada")

_ESTMA_NRCAN_URL = (
    "https://natural-resources.canada.ca/minerals-mining/services-mining-industry/"
    "extractive-sector-transparency/links-estma-reports"
)


def _estma_year_span(years: list[str]) -> str:
    years = sorted(set(y for y in years if y))
    if not years:
        return ""
    return years[0] if len(years) == 1 else f"{years[0]}–{years[-1]}"


@content_addressed
def map_estma(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Every statement dated by Statistics Canada's publication of the file
    (Phase 315). See ``_map_estma`` for the mapping."""
    yield from dated_by_cut(_map_estma(bundle), (bundle or {}).get("cut"))


def _map_estma(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    if not bundle or bundle.get("is_stub"):
        return
    lei = str(bundle.get("lei") or "").strip().upper()
    for entity in bundle.get("entities") or []:
        estma_id = str(entity.get("estma_id") or "").strip()
        if not estma_id:
            continue
        reports = entity.get("reports") or []
        own_years = [r["year"] for r in reports if not r.get("included_in")]
        included = [r for r in reports if r.get("included_in")]
        if own_years:
            details = f"ESTMA reporting entity; payments to governments reported for {_estma_year_span(own_years)}"
        elif included:
            details = "ESTMA entity whose payments are included in another entity's report"
        else:
            details = "ESTMA entity"
        stmt = make_entity_statement(
            source_id="estma",
            local_id=estma_id,
            name=str(entity.get("name") or estma_id),
            identifiers=[{"id": estma_id, "scheme": ESTMA_SCHEME[0], "schemeName": ESTMA_SCHEME[1]}],
            entity_details=details,
            source_url=bundle.get("dataset_page") or _ESTMA_NRCAN_URL,
        )
        if included:
            latest = included[0]
            parent = latest["included_in"]
            annotate(
                stmt,
                commenting(
                    pointer("recordDetails"),
                    (
                        f"ESTMA files this entity's payments for {_estma_year_span([r['year'] for r in included])} "
                        f"in the report of {parent.get('name') or parent['estma_id']} "
                        f"({parent['estma_id']}). That is a reporting arrangement under the "
                        "Extractive Sector Transparency Measures Act, not a statement of "
                        "ownership or control, and the entity's own payments are not "
                        "separated out in that report."
                    ),
                ),
            )
        if lei:
            match = entity.get("match") or {}
            link = identifying(
                pointer("recordDetails"),
                (
                    f"OpenCheck links this ESTMA entity to LEI {lei}: "
                    f"{match.get('basis') or 'a match made by OpenCheck'}. The ESTMA "
                    "data carries no LEI or business number, so the LEI is OpenCheck's "
                    "match, not an identifier the dataset asserts."
                ),
            )
            link["url"] = f"https://search.gleif.org/#/record/{lei}"
            annotate(stmt, link)
        yield stmt
