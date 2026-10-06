"""One table: how each national register addresses a company's own page.

Phase 296. Before this, the URL of a company's record on its home register was
written out in three places that drifted independently — ``sourceEntityUrl`` in
``frontend/src/components/cdd/SourceBucketCard.tsx``, ``recordUrl`` in
``frontend/src/lib/historyMode.ts`` and each adapter's ``raw.link`` — and a link
only existed when that register's adapter answered. A degraded, skipped or
keyless source left the reader no route to the official record at all.

This module is the single table. The lookup uses it to put an
``register_record`` on the anchor event (``gleif_done`` / ``register_done``),
built from the LEI record's registration authority and ``registeredAs`` alone,
so the subject header can link to the register whatever the adapter did. The
frontend mirrors the table in ``frontend/src/lib/registerLinks.ts``;
``tests/test_register_links.py`` parses that file and fails if the two differ,
the same arrangement ``test_ra_codes.py`` uses for ``raCodes.ts``.

GLEIF's own "LEI linking to registration authorities" initiative does the same
thing for six registers (UK, NL, CH, FR, EE, NZ), but only on search.gleif.org:
the URL is built client-side from a hardcoded six-entry table and is not in the
API, the RA list or the Golden Copy, so there is nothing to ingest. The entries
below cover those six and every other register OpenCheck dispatches to for
which a per-company page could be found.

**How an entry reads.** ``identifier_key`` is the derived key the lookup
already stores the register's number under (``ctx.derived``, and the hit's
``identifiers``) — so normalisation is the adapter's own ``LookupDeriver``, not
a second copy. ``record`` is an ordered list of ``(pattern, template)``: the
first pattern the number fully matches picks the template, and a number that
matches none gets **no link**, never a guessed one. ``history`` overrides
``record`` on the History tab where that tab wants a deeper page (Companies
House filing history) or a different one (New York, which has no per-entity
page but does have per-entity open data).

**What does not get an entry.** A register whose only address is a search
form, a login, or a page that renders without the company: a link that opens
and shows the reader nothing about the company looks like it worked and is
worse than no link. Phase 296 removed four such links from the source cards
(Firmenbuch, Sudski registar, Bolagsverket, the old YTJ page) and replaced two
broken ones (KvK search, the old Latvian portal).

**Verification, 6 Oct 2026.** Every template below was loaded in a headless
browser with a real GLEIF sample and checked for the company's name, except
four whose registers refuse datacentre clients with a bot challenge (CRO, CVR,
data.inpi.fr, Registrų centras). Those four are the templates OpenCheck already
used before this phase, carried over unchanged; they are marked below.
"""

from __future__ import annotations

import re
from typing import Mapping, NamedTuple

__all__ = [
    "REGISTER_LINKS",
    "RegisterLink",
    "history_url",
    "record_url",
    "register_record",
    "register_record_for_anchor",
    "source_for_identifier_key",
]


class RegisterLink(NamedTuple):
    """How one register addresses a company's own page."""

    register: str
    identifier_key: str
    #: ``(pattern, template)`` pairs, tried in order; ``{id}`` is substituted.
    record: tuple[tuple[str, str], ...]
    #: History-tab override; empty means "use ``record``".
    history: tuple[tuple[str, str], ...] = ()


#: source_id → how its register addresses a company. Mirrored in
#: ``frontend/src/lib/registerLinks.ts`` (parsed by tests/test_register_links.py).
REGISTER_LINKS: dict[str, RegisterLink] = {
    "companies_house": RegisterLink(
        register="Companies House",
        identifier_key="gb_coh",
        record=(
            (r"^[A-Z0-9]{8}$", "https://find-and-update.company-information.service.gov.uk/company/{id}"),
        ),
        history=(
            (r"^[A-Z0-9]{8}$", "https://find-and-update.company-information.service.gov.uk/company/{id}/filing-history"),
        ),
    ),
    "kvk": RegisterLink(
        register="KvK Handelsregister",
        identifier_key="kvk_number",
        # GLEIF's own template. The KvK search page (kvk.nl/zoeken/?q=), which
        # the source card used until Phase 296, renders without the company.
        record=((r"^\d{8}$", "https://www.kvk.nl/bestellen/?kvknummer={id}"),),
    ),
    "zefix": RegisterLink(
        register="Swiss UID register (FSO)",
        identifier_key="che_uid",
        # GLEIF's template for RA000548; it resolves RA000549 (cantonal
        # commercial register) entities too. The Zefix name search the card
        # used before could not be confirmed to resolve and refuses datacentre
        # clients. The UID page states its extract has no legal effect and
        # links on to the cantonal commercial register.
        record=((r"^CHE\d{9}$", "https://www.uid.admin.ch/Detail.aspx?uid_id={id}"),),
    ),
    "inpi": RegisterLink(
        register="INPI (Registre national des entreprises)",
        identifier_key="siren",
        # Carried over; bot-challenged from datacentres, not re-verified.
        record=((r"^\d{9}$", "https://data.inpi.fr/entreprises/{id}"),),
    ),
    "ariregister": RegisterLink(
        register="e-Business Register (RIK)",
        identifier_key="ee_registry_code",
        record=((r"^\d{8}$", "https://ariregister.rik.ee/eng/company/{id}"),),
    ),
    "nz_companies": RegisterLink(
        register="NZ Companies Register",
        identifier_key="nz_company_number",
        # GLEIF files some NZ companies under their 13-digit NZBN rather than
        # the company number, and the two address different pages.
        record=(
            (r"^94\d{11}$", "https://www.nzbn.govt.nz/mynzbn/nzbndetails/{id}/"),
            (r"^\d{1,8}$", "https://app.companiesoffice.govt.nz/companies/app/ui/pages/companies/{id}"),
        ),
    ),
    "brreg": RegisterLink(
        register="Brønnøysundregistrene",
        identifier_key="no_orgnr",
        record=((r"^\d{9}$", "https://virksomhet.brreg.no/nb/oppslag/enheter/{id}"),),
    ),
    "prh": RegisterLink(
        register="YTJ (PRH)",
        identifier_key="fi_ytunnus",
        # The old tietopalvelu.ytj.fi/yritystiedot.aspx address now lands on
        # the YTJ home page.
        record=((r"^\d{7}-\d$", "https://tietopalvelu.ytj.fi/yritys/{id}"),),
    ),
    "ur_latvia": RegisterLink(
        register="Latvian Register of Enterprises",
        identifier_key="lv_regcode",
        # The latvija.lv address the adapter and card used redirects to the
        # portal home page.
        record=((r"^\d{11}$", "https://info.ur.gov.lv/#/legal-entity/{id}"),),
    ),
    "ares": RegisterLink(
        register="ARES",
        identifier_key="cz_ico",
        record=((r"^\d{8}$", "https://ares.gov.cz/ekonomicke-subjekty?ico={id}"),),
    ),
    "bce_belgium": RegisterLink(
        register="Crossroads Bank for Enterprises (KBO/BCE)",
        identifier_key="be_enterprise_number",
        record=((r"^\d{10}$", "https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html?ondernemingsnummer={id}"),),
    ),
    "corporations_canada": RegisterLink(
        register="Corporations Canada",
        identifier_key="ca_corp_id",
        # The corporation number with its check digit; without it the page
        # renders empty.
        record=((r"^\d{8}$", "https://ised-isde.canada.ca/cc/lgcy/fdrlCrpDtls.html?corpId={id}"),),
    ),
    "abr_australia": RegisterLink(
        register="ABN Lookup",
        identifier_key="au_abn",
        # ABN only: an ACN-registered company (RA000014) has no public page
        # addressable by its ACN.
        record=((r"^\d{11}$", "https://abr.business.gov.au/ABN/View?abn={id}"),),
    ),
    "cro": RegisterLink(
        register="CRO",
        identifier_key="ie_crn",
        # Carried over; bot-challenged from datacentres, not re-verified.
        record=((r"^\d{1,7}$", "https://core.cro.ie/company/{id}"),),
    ),
    "cvr_denmark": RegisterLink(
        register="CVR",
        identifier_key="dk_cvr",
        # Carried over; bot-challenged from datacentres, not re-verified.
        record=((r"^\d{8}$", "https://datacvr.virk.dk/enhed/virksomhed/{id}"),),
    ),
    "jar_lithuania": RegisterLink(
        register="Register of Legal Entities (JAR)",
        identifier_key="lt_code",
        # Carried over; bot-challenged from datacentres, not re-verified.
        record=((r"^\d{9}$", "https://www.registrucentras.lt/jar/p/index.php?kod={id}"),),
    ),
    "ny_dos": RegisterLink(
        register="NY Department of State",
        identifier_key="us_ny_dos_id",
        # DOS's public inquiry is a form with no per-entity address, so there
        # is no record link; History links the open-data query for the
        # entity's filings, the rows that tab is built from.
        record=(),
        history=((r"^\d+$", "https://data.ny.gov/resource/63wc-4exh.json?corpid_num={id}"),),
    ),
}


_BY_IDENTIFIER_KEY: dict[str, str] = {
    link.identifier_key: source_id for source_id, link in REGISTER_LINKS.items()
}


def source_for_identifier_key(identifier_key: str) -> str | None:
    """The source whose register is addressed by ``identifier_key``, if any."""
    return _BY_IDENTIFIER_KEY.get(identifier_key)


def _candidates(identifier: str) -> tuple[str, ...]:
    """The number as given, then with spaces, dots and hyphens removed."""
    raw = (identifier or "").strip()
    compact = re.sub(r"[\s.\-]", "", raw)
    return (raw, compact) if compact != raw else (raw,)


def _pick(rules: tuple[tuple[str, str], ...], identifier: str) -> str | None:
    for candidate in _candidates(identifier):
        if not candidate:
            continue
        for pattern, template in rules:
            if re.fullmatch(pattern, candidate):
                return template.replace("{id}", candidate)
    return None


def record_url(source_id: str, identifier: str) -> str | None:
    """The register's own page for the company, or ``None``.

    ``None`` when the source has no entry, the register has no per-company
    page, or the number does not have the shape the register addresses.
    """
    link = REGISTER_LINKS.get(source_id)
    if link is None:
        return None
    return _pick(link.record, identifier)


def history_url(source_id: str, identifier: str) -> str | None:
    """The page the History tab links a register's dated rows to."""
    link = REGISTER_LINKS.get(source_id)
    if link is None:
        return None
    return _pick(link.history or link.record, identifier)


def register_record(
    source_id: str | None,
    identifier: str | None,
    *,
    ra_code: str | None = None,
) -> dict[str, str] | None:
    """The ``register_record`` payload for the subject header, or ``None``.

    Built from the register's own number alone — it never depends on the
    adapter having answered, which is the point of it.
    """
    if not source_id or not identifier:
        return None
    url = record_url(source_id, identifier)
    if url is None:
        return None
    payload = {
        "source_id": source_id,
        "register": REGISTER_LINKS[source_id].register,
        "identifier": identifier,
        "url": url,
    }
    if ra_code:
        payload["ra_code"] = ra_code
    return payload


def register_record_for_anchor(
    registered_at: str,
    derived: Mapping[str, str],
    identifier_key_by_ra: Mapping[str, str],
) -> dict[str, str] | None:
    """``register_record`` from a GLEIF anchor: its RA code and derived numbers.

    ``identifier_key_by_ra`` maps an RA code to the derived key its register's
    number is stored under — built by the lookup from the adapters' own
    ``LookupDeriver`` declarations, so the RA codes are not copied here.
    """
    ra = (registered_at or "").strip().upper()
    key = identifier_key_by_ra.get(ra)
    if not key:
        return None
    return register_record(
        source_for_identifier_key(key), derived.get(key), ra_code=ra
    )
