"""EITI — payments organisations, the SOE list and the Company Assessment → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from ..annotations import annotate, commenting, identifying, pointer
from ..statements import (
    _country_obj,
    make_entity_statement,
    make_relationship_statement,
)


# ----------------------------------------------------------------------
# EITI — Extractive Industries Transparency Initiative
# ----------------------------------------------------------------------

# National identifier schemes for the countries whose EITI identification
# format has been verified against an org-id.guide scheme OpenCheck already
# emits. Other countries carry the identification without a scheme code.
_EITI_SCHEME_BY_COUNTRY: dict[str, tuple[str, str]] = {
    "GB": ("GB-COH", "Companies House"),
    "NO": ("NO-BRC", "Brønnøysundregistrene"),
    "NL": ("NL-KVK", "Kamer van Koophandel"),
}


def map_eiti(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an EITI fetch bundle to BODS v0.4 statements.

    Emits one entity statement for the disclosing company. EITI payment
    data describes fiscal flows, not ownership or control, so no person
    or relationship statements are emitted. (When EITI's data strategy
    delivers BODS-native company/SOE publication, this mapper is the
    natural place to consume it.)
    """
    if not bundle or bundle.get("is_stub"):
        return

    identification: str = (bundle.get("identification") or "").strip()
    country: str = (bundle.get("country") or "").strip().upper()
    name: str = (bundle.get("entity_name") or "").strip()
    if not identification or not name:
        return

    scheme = _EITI_SCHEME_BY_COUNTRY.get(country)
    identifier: dict[str, str] = {"id": identification}
    if scheme:
        identifier["scheme"] = scheme[0]
        identifier["schemeName"] = f"{scheme[1]} (via EITI disclosure)"
    else:
        # EITI does not say which register issued the number (CLAUDE.md, "What
        # EITI does and does not publish as an identifier"), so the scheme
        # names where it came from rather than guessing a register (Phase 239;
        # it had none). Not ``REG-<country>``: that aliases into a register hop.
        identifier["scheme"] = "EITI-IDENTIFICATION"
        identifier["schemeName"] = "Identification given in an EITI disclosure"

    jurisdiction_obj = _country_obj(country) if country else None
    jur_tuple: tuple[str, str | None] | None = (
        (jurisdiction_obj["name"], jurisdiction_obj.get("code"))
        if jurisdiction_obj
        else None
    )

    entity = make_entity_statement(
        source_id="eiti",
        local_id=f"{country}:{identification}",
        name=name,
        jurisdiction=jur_tuple,
        identifiers=[identifier],
        source_url="https://eiti.org/",
    )
    yield entity


#: The SOE roster now lives in the new global database (Phase 172).
_EITI_SOE_URL = "https://eiti-database.eiti.org/eiti_database/view_soeList"


def map_eiti_soe(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an EITI SOE Database bundle to BODS v0.4 statements.

    Emits the state-owned enterprise as an entity, the controlling government
    body as a ``stateBody`` entity, and a ``controlByLegalFramework``
    relationship between them. That relationship shape is exactly what the risk
    engine's ``_state_controlled_signals`` reads to raise ``STATE_CONTROLLED`` —
    so the state-ownership signal falls out of the BODS graph with no bespoke
    risk rule (and EITI is a far more authoritative source for it than the
    existing Wikidata path).

    The SOE database does **not** publish the LEI (OpenCheck derives it at
    index-build time), so ``lei`` is deliberately NOT asserted as a BODS
    identifier here — only the identifiers EITI itself publishes (its EITI id
    and, where present, the OpenCorporates id).

    When EITI ships its planned BODS-native SOE dataset, this mapper is the
    natural place to consume it — the graph shape emitted here already matches.
    """
    if not bundle or bundle.get("is_stub"):
        return

    lei: str = (bundle.get("lei") or "").strip().upper()
    name: str = (bundle.get("entity_name") or "").strip()
    if not lei or not name:
        return

    country: str = (bundle.get("country") or "").strip().upper()
    jurisdiction_obj = _country_obj(country) if country else None
    jur_tuple: tuple[str, str | None] | None = (
        (jurisdiction_obj["name"], jurisdiction_obj.get("code"))
        if jurisdiction_obj
        else None
    )

    # No identifiers at all. The ``XI-EITI`` scheme this used to emit carried
    # ``eiti_id_company``, which EITI regenerated wholesale when the new
    # database launched (UUIDv4 → UUIDv5-over-a-name): every exported statement
    # asserting one now names a key that cannot be looked up in the database it
    # came from. A deduplication key is not a registry number. The
    # OpenCorporates branch went with it — EITI publishes no OpenCorporates id
    # for any state-owned enterprise. The LEI stays barred as it always was,
    # being OpenCheck-derived. See ``routers/hit_builders.py::_bh_eiti_soe``.
    identifiers: list[dict[str, str]] = []

    soe = make_entity_statement(
        source_id="eiti_soe",
        local_id=lei,
        name=name,
        jurisdiction=jur_tuple,
        identifiers=identifiers,
        entity_type="registeredEntity",
        entity_details="State-owned enterprise (EITI SOE database)",
        source_url=_EITI_SOE_URL,
    )
    yield soe

    # The controlling party.
    #
    # The old SOE database carried a `government_entity` per company and the new
    # one does not: `metadata_gov_entities` holds the agencies that *collect*
    # revenue (tax authorities, ministries), with no link saying which body owns
    # which enterprise. Dropping the relationship when that field is empty would
    # silently switch off `STATE_CONTROLLED` — the one signal this adapter
    # exists to raise — for every company in the repointed index.
    #
    # So where EITI names the body, it is used. Where EITI does not, the
    # controlling party is the state EITI files the enterprise under, named as
    # such, and the relationship's own `details` says that EITI does not name
    # the organ. That is the whole of what is known: EITI's SOE roster asserts
    # state ownership — that is what the roster *is* — and asserts nothing about
    # which ministry or fund holds it. The alternative is not a more cautious
    # graph, it is a graph missing a fact the source states plainly.
    gov_name = (bundle.get("government_entity") or "").strip()
    named_by_eiti = bool(gov_name)
    if not gov_name:
        country_label = (bundle.get("country_name") or "").strip()
        if not country_label and jurisdiction_obj:
            country_label = jurisdiction_obj["name"]
        gov_name = f"Government of {country_label}" if country_label else ""

    # Only assert state control (which raises the STATE_CONTROLLED signal) when
    # the LEI match is reasonably trustworthy. A low-confidence name match still
    # surfaces the SOE entity and its enrichment, but must not raise a
    # state-control signal on a possibly-wrong entity.
    if not gov_name or (bundle.get("match_confidence") or "medium").lower() == "low":
        return

    gov_local = f"{lei}:gov:{(bundle.get('eiti_id_government') or gov_name)}"
    government = make_entity_statement(
        source_id="eiti_soe",
        local_id=gov_local,
        name=gov_name,
        jurisdiction=jur_tuple,
        entity_type="stateBody",
        entity_details=(
            None
            if named_by_eiti
            else "The state EITI files this enterprise under; EITI does not "
                 "name the controlling government body."
        ),
        source_url=_EITI_SOE_URL,
    )
    yield government

    yield make_relationship_statement(
        source_id="eiti_soe",
        local_id=f"{lei}:state-control",
        subject_statement_id=soe["statementId"],
        interested_party_statement_id=government["statementId"],
        interested_party_type="entity",
        interests=[
            {
                "type": "controlByLegalFramework",
                "directOrIndirect": "direct",
                "beneficialOwnershipOrControl": True,
                "details": (
                    f"State-owned enterprise controlled by {gov_name} "
                    "(EITI SOE database)."
                    if named_by_eiti
                    else (
                        "Classified by EITI as a state-owned enterprise in "
                        f"{gov_name.removeprefix('Government of ').strip()}. "
                        "EITI's roster asserts state ownership but does not "
                        "name the controlling government body, so the "
                        "controlling party here is the state itself."
                    )
                ),
            }
        ],
        source_url=_EITI_SOE_URL,
    )


# ==========================================================================
# EITI Company Assessment (eiti_assessment) → BODS v0.4
# ==========================================================================


def map_eiti_assessment(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an EITI Company Assessment bundle to BODS v0.4 statements.

    Emits **one entity statement for the subject company and nothing else** —
    following the Wikirate and TED precedent that a source describing something
    other than ownership or control emits no person or relationship statements.
    What EITI assesses here is a company's *disclosure posture*: whether it says
    it discloses its beneficial owners, and whether it publishes a subsidiary
    list. Neither is an ownership assertion, so neither becomes a relationship.

    .. important::
       **The declared subsidiaries are deliberately not mapped.** EITI publishes
       1,230 parent→child rows as free-text names with no identifier, no
       percentage and no share class. Emitting them as entity and relationship
       statements would assert that OpenCheck has identified those companies,
       which it has not — matching 1,230 free-text names is a different problem
       from the ~99 parents, and one this adapter does not attempt. They are
       rendered as evidence on the Climate & ESG card and stay out of the
       graph. ``tests/test_eiti_assessment.py::test_subsidiaries_never_enter_bods``
       pins this; widening it should require editing that test, deliberately.

    Identifier corroboration: **no identifiers are asserted at all.** The LEI is
    OpenCheck-derived (bars it under the corroboration rule); EITI's
    ``legal_entity_id`` is populated for 3 companies of 10,116; and its
    ``eiti_id_company`` is a UUIDv5 over a name-derived key that EITI
    regenerated wholesale when this database launched, so it is a deduplication
    key rather than a registry number. See ``sources/eiti_assessment.py``.

    No ``jurisdiction`` either. EITI publishes a headquarters country, which is
    not the jurisdiction of incorporation — asserting one as the other would be
    a guess dressed as a fact, and several of these companies are incorporated
    somewhere other than where they are headquartered.

    The beneficial ownership assessment rides as a ``commenting`` annotation
    rather than a field: BODS has no place to record "a third party assessed
    this company's disclosure of its owners", and inventing one would misuse
    the schema. The annotation states who assessed, in which year, and what
    they concluded, in EITI's own words.
    """
    if not bundle or bundle.get("is_stub"):
        return

    lei: str = (bundle.get("lei") or "").strip().upper()
    name: str = (bundle.get("name") or "").strip()
    if not lei or not name:
        return

    stmt = make_entity_statement(
        source_id="eiti_assessment",
        # Keyed on the LEI because it is the only stable handle this record
        # has — but note this is the statement's *local id*, not an asserted
        # identifier: nothing is added to `identifiers`.
        local_id=lei,
        name=name,
        identifiers=[],
        source_url="https://eiti-database.eiti.org/",
    )

    annotations: list[dict[str, Any]] = []

    year = _eiti_assessment_latest_year(bundle)
    if year:
        exp6 = ((bundle.get("assessments") or {}).get(year) or {}).get("exp_6") or {}
        result = (exp6.get("result") or "").strip()
        if result:
            # EITI's wording is passed through verbatim. "Not available" means
            # EITI did not assess, which is not the same as a company failing,
            # and paraphrasing the two into one phrase would state something
            # untrue about whichever company got the other.
            annotations.append(
                commenting(
                    "/",
                    (
                        f"EITI Company Assessment {year}, expectation 6 "
                        f"(company discloses beneficial ownership): {result}. "
                        "An assessment of the company's disclosure, not a "
                        "statement of its beneficial ownership."
                    ),
                )
            )

        exp2 = ((bundle.get("assessments") or {}).get(year) or {}).get("exp_2") or {}
        declared = bundle.get("subsidiaries") or []
        if declared:
            countries = sorted({
                (s.get("country") or "").strip()
                for s in declared
                if (s.get("country") or "").strip()
            })
            where = (
                f" across {len(countries)} EITI implementing countries"
                if len(countries) > 1
                else ""
            )
            annotations.append(
                commenting(
                    "/",
                    (
                        f"Declared {len(declared)} controlled subsidiaries to "
                        f"EITI{where} ({year}). Names only — EITI publishes no "
                        "identifier for them, so they are not mapped as "
                        "statements and do not appear in this graph."
                    ),
                )
            )
        elif (exp2.get("result") or "").strip():
            annotations.append(
                commenting(
                    "/",
                    (
                        f"EITI Company Assessment {year}, expectation 2 "
                        f"(company publishes a list of controlled "
                        f"subsidiaries): {exp2['result'].strip()}. No list is "
                        "carried in the EITI data for this company."
                    ),
                )
            )

    # The matched GLEIF name, when it differs from the name EITI uses. Several
    # supporting companies have no LEI of their own and are anchored on the only
    # LEI-bearing entity in the group (Chevron Corporation → Chevron U.S.A.
    # Inc.); a statement that showed one silently as the other would misstate
    # which legal entity this record is about.
    matched = ((bundle.get("match") or {}).get("gleif_legal_name") or "").strip()
    if matched and _norm_for_compare(matched) != _norm_for_compare(name):
        annotations.append(
            commenting(
                "/",
                (
                    f"EITI names this supporting company \u201c{name}\u201d. "
                    f"OpenCheck resolved it to the LEI of \u201c{matched}\u201d, "
                    "reviewed by hand. Where the two differ, the EITI record "
                    "describes the corporate group and the LEI identifies one "
                    "legal entity within it."
                ),
            )
        )

    # The LEI OpenCheck matched this record to, published as an `identifying`
    # annotation rather than an identifier: `identifiers` stays empty under the
    # corroboration rule in the docstring above. But a statement
    # with no identifier and no jurisdiction can never be joined to the
    # company it is about: in the FullCheck graph it floated as a second,
    # unconnected node beside the subject (PT Pertamina (Persero), 2026-09-10).
    # The annotation's `url` names the GLEIF record, so a consumer — the
    # FullCheck display merge among them — can join the two on OpenCheck's
    # word, visibly as a match, or reject it.
    annotations.append(_eiti_assessment_lei_match(lei, bundle.get("match") or {}))

    annotate(stmt, *annotations)
    yield stmt


#: How the EITI→LEI link was made, in words (``match.method`` from the index).
_EITI_MATCH_METHODS: dict[str, str] = {
    "published_lei": "the LEI EITI publishes for it, confirmed in GLEIF",
    "gleif_name_exact": "an exact legal-name match in GLEIF, reviewed by hand",
    "candidates_only": "chosen by hand from GLEIF name candidates",
}


def _eiti_assessment_lei_match(lei: str, match: dict[str, Any]) -> dict[str, Any]:
    """The `identifying` annotation linking an EITI assessment record to its LEI."""
    method = str(match.get("method") or "")
    how = _EITI_MATCH_METHODS.get(method, "a match made by OpenCheck")
    why = (
        ""
        if method == "published_lei"
        else ", because it is OpenCheck's match rather than an identifier EITI asserts"
    )
    annotation = identifying(
        pointer("recordDetails"),
        (
            f"OpenCheck links this EITI record to LEI {lei} \u2014 {how}. The LEI "
            f"is not carried in identifiers{why}; it is published here so a "
            "consumer can join this statement to that legal entity or reject "
            "the match."
        ),
    )
    annotation["url"] = f"https://search.gleif.org/#/record/{lei}"
    return annotation


def _eiti_assessment_latest_year(bundle: dict[str, Any]) -> str | None:
    """Most recent assessment year in the bundle, as a string, or None."""
    years = [y for y in (bundle.get("assessments") or {}) if str(y).strip()]
    return max(years, key=lambda y: (len(y), y)) if years else None


def _norm_for_compare(value: str) -> str:
    """Loose name comparison for 'did the matched name differ' only."""
    return "".join(ch for ch in (value or "").lower() if ch.isalnum())
