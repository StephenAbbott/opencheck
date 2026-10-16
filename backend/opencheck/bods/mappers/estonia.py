"""Estonia — e-Business Register (ariregister) → BODS.

Split out of ``mapper.py`` in Phase 246; ``mapper.py`` re-exports every
name defined here. Phase 330 taught it the open-data files' vocabulary —
the same row shape the SOAP-era bundle had, now read from the daily index
(``sources/ariregister.py``) — while keeping the printable page's rows
mapping exactly as before.

Emitted statements:

1. One entityStatement for the company itself.
2. One personStatement (or entityStatement for a legal person) and one
   ownershipOrControlStatement per entry in the register of shareholders
   (``osanikud``): ``shareholding`` with the exact percentage.
3. One personStatement / entityStatement and one ownershipOrControlStatement
   per person on the registry card (``kaardile_kantud_isikud``), role-mapped
   to BODS interest types (``_EE_OFFICER_ROLE_MAP``); roles that are not an
   interest in the company (a contact person, the share registrar) are
   skipped. Officers are serving-only (Phase 317).
4. One personStatement and one ownershipOrControlStatement per declared
   beneficial owner (``kasusaajad``), typed from the register's
   manner-of-control code (``_EE_BO_CONTROL_MAP``) with the register's own
   wording in ``details``, and ``beneficialOwnershipOrControl`` from the
   regimes registry.
5. From the persons outside the registry card (``kaardivalised_isikud``,
   index tier only): supervisory board members and chairs as
   ``boardMember`` / ``boardChair``; the shareholders of a public limited
   company the register itself records (role ``S``, aktsionär) as
   ``shareholding`` exactly as an OÜ shareholder row; and founders (``A`` /
   ``B``, asutaja) as ownership history — a ``commenting`` annotation on
   the founder's current shareholding when they still hold, else a
   ``shareholding`` record with ``startDate`` and ``recordStatus: closed``
   and no ``endDate`` (the register publishes none — the CAC rule). A
   founder's contribution is the founding capital, never a current share,
   so it is said in ``details`` and never folded into a current figure.
   Auditors, bankruptcy committee members and the other off-card roles are
   no interest in the company and are not mapped (they stay in the raw
   bundle for the Data drawer).

Personal identity: since November 2024 the open data carries no personal
identification codes; a natural person is identified across the three
lists only by ``isikukood_hash``, a pseudonymous UUID that is the same for
the same person on every list of the same company. It is the person's
local id — so a director who is also a shareholder and the declared owner
is one node — and it is passed as an identifier only for
``make_person_statement`` to move into an ``identifying`` annotation
(Phase 255): it is never written to ``personStatement.identifiers``.

Countries: the files use ISO 3166-1 alpha-3 with a few IOC-style strays
(``DEN``, ``GER``, ``LAT``, ``NED``…); :func:`_ee_country` resolves what
it can and a code it cannot resolve becomes a name alone, never a stand-in
(the Phase 239 contract).

Date format in the files: ``DD.MM.YYYY`` (converted here); the index and
the page path already hand over ISO days.
"""

from __future__ import annotations

import re
from typing import Any, Iterable

import pycountry

from .. import liveness as _liveness
from ..annotations import commenting, pointer
from ..statements import (
    SOURCE_NAMES,
    _addr,
    _country_obj,
    _stable_id,
    dated_by_cut,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
    set_beneficial_ownership,
)


# ----------------------------------------------------------------------
# Vocabularies
# ----------------------------------------------------------------------

#: Registry-card role code → (BODS interest type, descriptive label). The
#: label is the English gloss with the register's Estonian term; a role not
#: listed is not an interest in the company and is skipped (``KISIK``
#: contact person, ``ORP``/``ARP`` share registrar, ``MDKPI`` recipient of
#: procedural documents, ``DOKH`` keeper of documents).
_EE_OFFICER_ROLE_MAP: dict[str, tuple[str, str]] = {
    "JUHL":   ("boardMember",              "Board member (juhatuse liige)"),
    "JUHJ":   ("boardMember",              "Board member, manager (juhatuse liige (juhataja))"),
    "JUHA":   ("boardMember",              "Sole board member (juhatuse ainuliige)"),
    "JUHE":   ("boardChair",               "Chair of the board (juhatuse esimees)"),
    "HNKL":   ("boardMember",              "Supervisory board member (haldusnõukogu liige)"),
    "PROK":   ("seniorManagingOfficial",   "Procurist (prokurist)"),
    "LIKV":   ("seniorManagingOfficial",   "Liquidator (likvideerija)"),
    "LIKVJ":  ("boardMember",              "Liquidator (board member)"),
    "PANKR":  ("seniorManagingOfficial",   "Bankruptcy trustee (pankrotihaldur)"),
    "AJUTPH": ("seniorManagingOfficial",   "Interim bankruptcy trustee (ajutine pankrotihaldur)"),
    "AJPH":   ("seniorManagingOfficial",   "Interim bankruptcy trustee acting as liquidator"),
    "JPNKR":  ("seniorManagingOfficial",   "Supervising bankruptcy trustee (järelevalveõiguslik pankrotihaldur)"),
    "ERIH":   ("seniorManagingOfficial",   "Special-regime administrator (erirežiimihaldur)"),
    "TOSAN":  ("boardMember",              "General partner (täisosanik)"),
    "UOSAN":  ("boardMember",              "Limited partner (usaldusosanik)"),
    "EUSOS":  ("boardMember",              "Limited partner authorised to represent (esindama volitatud usaldusosanik)"),
    "EUSOS2": ("boardMember",              "Limited partner authorised to represent (esindama volitatud usaldusosanik)"),
    "ASES":   ("seniorManagingOfficial",   "Authorised representative"),
    "ESIS":   ("seniorManagingOfficial",   "Person entitled to represent (esindama õigustatud isik)"),
    "ESIS2":  ("seniorManagingOfficial",   "Person entitled to represent (esindama õigustatud isik)"),
    "SJESI":  ("seniorManagingOfficial",   "Legal representative"),
    "VFILJ":  ("seniorManagingOfficial",   "Branch manager (filiaali juhataja)"),
    "FV":     ("seniorManagingOfficial",   "Fund manager (fondivalitseja)"),
    "VALIT":  ("seniorManagingOfficial",   "Manager (valitseja)"),
    "FIE":    ("otherInfluenceOrControl",  "Sole proprietor (füüsilisest isikust ettevõtja)"),
    "ETTEV":  ("otherInfluenceOrControl",  "Entrepreneur (ettevõtja)"),
    "KOAS":   ("otherInfluenceOrControl",  "Superior authority (kõrgemalseisev asutus)"),
    "YHL":    ("otherInfluenceOrControl",  "Cooperative member (ühistu liige)"),
    "YHLLV":  ("otherInfluenceOrControl",  "Cooperative member with additional liability (lisavastutusega ühistu liige)"),
}

#: Beneficial-owner manner-of-control code → (BODS interest type,
#: directOrIndirect or None, English gloss). The register's own Estonian
#: wording travels beside the gloss in ``details`` when the row carries it.
#: ``O``/``K``/``H``/``M`` are also what the printable page's English labels
#: are folded to; the rest were first read from the open data (13 Oct 2026:
#: O 276,499 · J 56,361 · C 47,430 · K 23,677 · F 2,285 · Z 1,950 · X 1,168
#: · Y 861 · S 311).
_EE_BO_CONTROL_MAP: dict[str, tuple[str, str | None, str]] = {
    "O": ("shareholding",            "direct",   "Direct participation"),
    "K": ("shareholding",            "indirect", "Indirect participation"),
    "H": ("votingRights",            None,       "Through voting rights"),
    "X": ("votingRights",            "indirect", "More than 50% of the voting rights in a consolidated entity"),
    "Y": ("appointmentOfBoard",      None,       "Right to appoint or remove the majority of the management or governing body"),
    "J": ("seniorManagingOfficial",  None,       "Member of the highest governing body (board or council member), no owner identified"),
    "C": ("seniorManagingOfficial",  None,       "Board member(s), no owner identified"),
    "Z": ("otherInfluenceOrControl", None,       "Holder of founder's rights, council member or board member"),
    "S": ("rightsToProfitOrIncome",  None,       "Beneficiary (soodustatud isik)"),
    "F": ("otherInfluenceOrControl", None,       "Control or influence by other means (contractual, family or other relations)"),
    "M": ("otherInfluenceOrControl", None,       "Other means"),
}

#: Off-card role code (``kaardivalised_isikud``) → (BODS interest type,
#: label) for the roles that are a role in the company. A supervisory
#: board (nõukogu) sits above the management board; BODS has one
#: ``boardMember`` / ``boardChair`` pair, so the label says which board.
_EE_OFF_CARD_ROLE_MAP: dict[str, tuple[str, str]] = {
    "N": ("boardMember", "Supervisory board member (nõukogu liige)"),
    "E": ("boardChair",  "Chair of the supervisory board (nõukogu esimees)"),
}

#: Off-card role codes that are a shareholding: ``S`` aktsionär, the
#: shareholder of a public limited company (AS) the register itself records
#: — a sole shareholder, or an entry from Nasdaq CSD (``kontrolli_allikas``
#: V1). The row has the OÜ shareholder row's shape and is mapped as one.
_EE_OFF_CARD_SHAREHOLDER_ROLES = frozenset({"S"})

#: Off-card role codes that are a founder (asutaja): ``A`` with a founding
#: contribution, ``B`` without one. A dated fact about the original
#: shareholding, not a current holding — see ``_off_card_statements``.
_EE_OFF_CARD_FOUNDER_LABELS: dict[str, str] = {
    "A": "Founder (asutaja)",
    "B": "Founder, no contribution made (asutaja (sissemakseta))",
}
#: The rest — ``D`` audit firm, ``J`` auditor (revident), ``M`` auditor of a
#: non-monetary contribution, ``P`` bankruptcy committee member, ``V``
#: authorised person, ``R`` audit committee member, ``L`` congregation of a
#: religious association — are no interest in the company and are not
#: mapped; they stay in the raw bundle.

#: Legal forms (``ettevotja_oiguslik_vorm``) that are public bodies —
#: typed ``stateBody``. State enterprises stay ``registeredEntity`` (the
#: Phase 240 rule).
_EE_STATE_BODY_FORMS = frozenset({
    "Kohaliku omavalitsuse asutus",
    "Täidesaatva riigivõimu asutus või riigi muu institutsioon",
    "Avalik-õiguslik juriidiline isik, põhiseaduslik institutsioon või nende asutus",
})

#: Codes the files use that are not ISO 3166-1 alpha-3 (IOC and older
#: spellings), seen 13 Oct 2026 — resolved to the ISO alpha-2 they mean.
#: ``XXK`` (Kosovo) and ``ANT`` (the former Netherlands Antilles) have no
#: current ISO code; they and ``XXX``/``XXS``/``XYY``/``~`` resolve to
#: nothing and the row keeps the register's country name alone.
_EE_COUNTRY_FIXUPS: dict[str, str] = {
    "DEN": "DK", "GER": "DE", "LAT": "LV", "NED": "NL", "POR": "PT", "SUI": "CH",
    "MON": "MC", "BUL": "BG", "IVB": "VG", "UAE": "AE", "SLO": "SI", "ROM": "RO",
    "SIN": "SG", "CHI": "CL", "LIB": "LB", "ISV": "VI", "SKN": "KN",
}

#: Codes that mean "no country" rather than a country.
_EE_NO_COUNTRY = frozenset({"", "XXX", "XXS", "XYY", "~"})


def _ee_country(code3: str | None) -> str | None:
    """ISO 3166-1 alpha-2 for one of the files' country codes, or None."""
    code = (code3 or "").strip().upper()
    if not code or code in _EE_NO_COUNTRY:
        return None
    if code in _EE_COUNTRY_FIXUPS:
        return _EE_COUNTRY_FIXUPS[code]
    if len(code) == 2 and pycountry.countries.get(alpha_2=code):
        return code
    found = pycountry.countries.get(alpha_3=code)
    return found.alpha_2 if found else None


def _ee_country_entry(code3: str | None, text: str | None) -> dict[str, str] | None:
    """A BODS country object for a nationality: ``{name, code}`` when the
    code resolves, the register's name alone when it does not, None when
    the row names no country."""
    alpha2 = _ee_country(code3)
    if alpha2:
        obj = _country_obj(alpha2)
        if obj:
            return obj
    name = " ".join((text or "").split())
    if name:
        return {"name": name}
    return None


def _ee_date(s: str | None) -> str | None:
    """Convert an Estonian date string to ISO YYYY-MM-DD.

    Handles two input formats:
    * ``DD.MM.YYYY`` — bulk open-data exports
    * ``YYYY-MM-DD[Z]`` / ``YYYY-MM-DDTHH:MM:SSZ`` — live API responses
    """
    if not s:
        return None
    s = str(s).strip()
    # Already ISO (or API format with optional time/timezone suffix).
    if len(s) >= 10 and s[4] == "-":
        return s[:10].rstrip("Z")
    # Estonian bulk-data: DD.MM.YYYY
    parts = s.split(".")
    if len(parts) == 3:
        d, m, y = parts
        if len(y) == 4 and d.isdigit() and m.isdigit():
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    return None


def _ee_person_id(person: dict[str, Any]) -> str:
    """Derive a stable local ID for a person record.

    Prefers isikukood_hash (UUID present in all modern records) then falls
    back to combining kirje_id with eesnimi+nimi to avoid collisions.
    """
    h = person.get("isikukood_hash")
    if h:
        return h
    kirje = person.get("kirje_id")
    first = person.get("eesnimi") or ""
    last = person.get("nimi_arinimi") or person.get("nimi") or ""
    return f"{kirje or 'x'}-{first}-{last}"


def _ee_full_name(person: dict[str, Any]) -> str:
    first = (person.get("eesnimi") or "").strip()
    last = (person.get("nimi_arinimi") or person.get("nimi") or "").strip()
    if first and last:
        return f"{first} {last}"
    return last or first or "Unknown"


def _ee_person_identifiers(person: dict[str, Any]) -> list[dict[str, str]]:
    """The pseudonymous hash as the one identifier a person row carries —
    ``make_person_statement`` moves it to an ``identifying`` annotation."""
    if person.get("isikukood_hash"):
        return [{
            "id": person["isikukood_hash"],
            "scheme": "EE-ARIREGISTER-HASH",
            "schemeName": "Estonian e-Business Register person hash",
        }]
    return []


def _ee_nationalities(person: dict[str, Any]) -> list[dict[str, str]]:
    """The country of a foreign person's identifier, as a nationality; an
    Estonian (``EST``) or unnamed one has none stated."""
    code = (person.get("valis_kood_riik") or "").strip().upper()
    if not code or code == "EST":
        return []
    entry = _ee_country_entry(code, person.get("valis_kood_riik_tekstina"))
    return [entry] if entry else []


def _ee_residence(person: dict[str, Any]) -> list[dict[str, Any]]:
    """A residence address carrying the country alone (the files publish
    no street for a beneficial owner)."""
    alpha2 = _ee_country(person.get("aadress_riik"))
    if not alpha2:
        return []
    obj = _country_obj(alpha2)
    return [{"type": "residence", "country": obj}] if obj else []


def _ee_foreign_entity(
    party: dict[str, Any], kirje_id: str, prefix: str,
) -> tuple[str, list[dict[str, str]], tuple[str, str | None] | None, str | None]:
    """For a legal-person row: (local id, identifiers, jurisdiction, note).

    An Estonian registry code is the id and an ``EE-ARIREGISTER`` identifier.
    A foreign code (``valis_kood`` + its country) names the party stably
    across lists but is not asserted as an identifier — the files state no
    scheme for it — and is said in a ``commenting`` annotation instead.
    """
    corp_code = (party.get("isikukood_registrikood") or "").strip()
    if corp_code:
        return (
            corp_code,
            [{
                "id": corp_code,
                "scheme": "EE-ARIREGISTER",
                "schemeName": "Estonian e-Business Register",
            }],
            ("Estonia", "EE"),
            None,
        )
    foreign = (party.get("valis_kood") or "").strip()
    riik = (party.get("valis_kood_riik") or "").strip().upper()
    alpha2 = _ee_country(riik)
    country_name = " ".join((party.get("valis_kood_riik_tekstina") or "").split())
    jurisdiction: tuple[str, str | None] | None = None
    if alpha2:
        obj = _country_obj(alpha2)
        if obj:
            jurisdiction = (obj["name"], alpha2)
    elif country_name:
        jurisdiction = (country_name, None)
    if foreign:
        where = country_name or riik
        note = (
            f"Foreign register code {foreign}" + (f" ({where})" if where else "")
            + ", as filed in the e-Business Register"
        )
        identifiers, derived = _ee_foreign_register_identifier(foreign, alpha2)
        if derived:
            note += (
                f"; the {derived['schemeName']} identifier {derived['id']} (scheme {derived['scheme']})"
                " is derived from it — the register states no scheme, and the code is read as a"
                " number on that country's company register because it has the shape of one."
            )
        else:
            note += "; no identifier scheme is stated."
        return f"foreign-{riik or 'xx'}-{foreign}", identifiers, jurisdiction, note
    return f"{prefix}-corp-{kirje_id}", [], jurisdiction, None


#: The shape a company number takes on the registers a foreign code may be
#: read as a number on — checked *after* the hop's own ``normalise``, which
#: for most registers only tidies (``ur_latvia`` accepts ``ABC``). Estonia's
#: owners are mostly from these countries; a scheme absent here derives no
#: identifier, however the code looks, because a tax or VAT number on the
#: wrong register either misses or reaches a stranger.
_EE_FOREIGN_CODE_SHAPES: dict[str, re.Pattern[str]] = {
    "LV-UR": re.compile(r"^[45]\d{10}$"),       # Latvian registration number
    "LT-JAR": re.compile(r"^\d{9}$"),            # Lithuanian JAR code
    "FI-PRH": re.compile(r"^\d{7}-\d$"),         # Finnish Y-tunnus
    "SE-BLV": re.compile(r"^\d{10}$"),           # Swedish organisationsnummer, hyphen removed
    "DK-CVR": re.compile(r"^\d{8}$"),            # Danish CVR number
    "NO-BRC": re.compile(r"^\d{9}$"),            # Norwegian organisasjonsnummer
    "NL-KVK": re.compile(r"^\d{8}$"),            # Dutch KvK number
    "PL-KRS": re.compile(r"^\d{10}$"),           # Polish KRS number
    "GB-COH": re.compile(r"^[A-Z0-9]{8}$"),      # Companies House (its normaliser validates)
    "IE-CRO": re.compile(r"^\d{1,7}$"),          # Irish CRO number
    "CZ-ICO": re.compile(r"^\d{8}$"),            # Czech IČO
    "SK-ICO": re.compile(r"^\d{8}$"),            # Slovak IČO
    "AT-FB": re.compile(r"^\d{1,6}[a-z]$"),      # Austrian Firmenbuchnummer
    "BE-BCE_KBO": re.compile(r"^\d{10}$"),       # Belgian enterprise number
    "CH-FDJP": re.compile(r"^CHE-?\d{3}\.?\d{3}\.?\d{3}$"),  # Swiss UID
    "FR-INSEE": re.compile(r"^\d{9}$"),          # SIREN
    "MT-MBR": re.compile(r"^C ?\d{1,6}$"),       # Maltese company number
    "HR-MBS": re.compile(r"^\d{9}$"),            # Croatian MBS
    "RS-APR": re.compile(r"^\d{8}$"),            # Serbian matični broj
    "MD-IDNO": re.compile(r"^\d{13}$"),          # Moldovan IDNO
}


def _ee_foreign_register_identifier(
    foreign: str, alpha2: str | None,
) -> tuple[list[dict[str, str]], dict[str, str] | None]:
    """A foreign legal person's ``valis_kood`` as an identifier on its
    country's company register, where OpenCheck has exactly one register
    hop for that country (``register_hops``' ``REG-<country>`` alias — a
    country with several registers, or none, gives nothing), the scheme
    is in ``_EE_FOREIGN_CODE_SHAPES`` and the code, normalised by the hop,
    has that register's shape. The hop is tried on the code as filed and,
    failing that, with a leading country prefix removed (``LV40003…``,
    ``FI1234567-8``). Nothing is asserted otherwise: the files state no
    scheme, and a code that is a VAT or tax number must not be looked up
    as a register number. The identifier is what lets a FullCheck register
    hop follow the owner across the border."""
    if not alpha2 or not foreign:
        return [], None
    from ...register_hops import hop_for

    hop = hop_for(f"REG-{alpha2}")
    if hop is None or hop.scheme not in _EE_FOREIGN_CODE_SHAPES:
        return [], None
    shape = _EE_FOREIGN_CODE_SHAPES[hop.scheme]
    candidates = [foreign]
    stripped = foreign[2:].lstrip(" -:") if foreign[:2].upper() == alpha2 else ""
    if stripped:
        candidates.append(stripped)
    for candidate in candidates:
        try:
            number = hop.normalise(candidate)
        except (ValueError, TypeError):
            continue
        if not number or not shape.match(str(number)):
            continue
        identifier = {
            "id": str(number),
            "scheme": hop.scheme,
            "schemeName": _ee_scheme_name(hop),
        }
        return [identifier], identifier
    return [], None


def _ee_scheme_name(hop: Any) -> str:
    """The register's own name for a hop's scheme — the adapter's display
    name (``bods/mapper.py``'s RA table cannot be read from a mapper module)."""
    from ...sources import REGISTRY

    adapter = REGISTRY.get(hop.source_id)
    return adapter.info.name if adapter is not None else str(hop.scheme)


def _ee_party_key(row: dict[str, Any], kirje_id: str, prefix: str) -> str:
    """The key under which a row's party is one node: a natural person's
    hash (else its kirje-based id), a legal person's registry code or
    foreign key — the same keys the statements are built on."""
    if (row.get("isiku_tyyp") or "F") == "J":
        return "J:" + _ee_foreign_entity(row, kirje_id, prefix)[0]
    return "F:" + _ee_person_id(row)


def _ee_shareholding_interest(row: dict[str, Any]) -> dict[str, Any]:
    """One ``shareholding`` interest from a shareholder-shaped row — an OÜ
    shareholder (``osanikud``), an AS shareholder (off-card role ``S``) or
    a founder — with the exact percentage where the register states one,
    the share's value, the ownership kind and the entry's source in
    ``details``, and the row's dates."""
    pct_str = row.get("osaluse_protsent") or ""
    share_size = row.get("osaluse_suurus") or ""
    currency = row.get("osaluse_valuuta") or ""
    interest: dict[str, Any] = {"type": "shareholding", "directOrIndirect": "direct"}
    try:
        pct = float(pct_str) if pct_str else None
    except ValueError:
        pct = None
    if pct is not None:
        # BODS v0.4: ``share.exclusiveMinimum``/``exclusiveMaximum`` are
        # *numbers* (exclusive percentage bounds), not booleans as in v0.3.
        # With an exact value known, the inclusive bounds suffice.
        interest["share"] = {"exact": pct, "minimum": pct, "maximum": pct}
    details: list[str] = []
    if share_size:
        details.append(f"Share value: {share_size} {currency}".strip())
    kind = (row.get("osaluse_omandiliik") or "").strip()
    kind_text = (row.get("osaluse_omandiliik_tekstina") or "").strip()
    if kind and kind != "L":
        # Joint (Y) or common (K / KY) ownership of one share — the
        # percentage is the share's, held together with another entry.
        gloss = {"Y": "joint ownership", "K": "common ownership", "KY": "share in common ownership held jointly"}.get(kind, "")
        details.append(" ".join(p for p in (gloss, f"({kind_text})" if kind_text else "") if p))
    allikas_text = (row.get("kontrolli_allikas_tekstina") or "").strip()
    if allikas_text:
        when = _ee_date(row.get("kontrolli_allika_kpv"))
        details.append(f"Entry source: {allikas_text}" + (f", {when}" if when else ""))
    if details:
        interest["details"] = "; ".join(details)
    start_date = _ee_date(row.get("algus_kpv"))
    end_date = _ee_date(row.get("lopp_kpv"))
    if start_date:
        interest["startDate"] = start_date
    if end_date:
        interest["endDate"] = end_date
    return interest


def _ee_founder_note(row: dict[str, Any]) -> str:
    """The sentence a founder row is said in: the role, the founding
    contribution where one was made, the register's date."""
    label = _EE_OFF_CARD_FOUNDER_LABELS.get((row.get("isiku_roll") or "").strip(), "Founder (asutaja)")
    parts = [label]
    size = (row.get("osaluse_suurus") or "").strip()
    if size:
        parts.append(f"founding contribution {size} {row.get('osaluse_valuuta') or ''}".rstrip())
    when = _ee_date(row.get("algus_kpv"))
    if when:
        parts.append(f"recorded {when}")
    return ", ".join(parts)


def _ee_status(bundle: dict[str, Any]) -> str | None:
    """The register's status in the printable page's English, which both
    tiers hand over, so the liveness classes below read either."""
    return bundle.get("status") or None


# ----------------------------------------------------------------------
# Mapper
# ----------------------------------------------------------------------


def map_ariregister(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map an AriregisterAdapter fetch bundle to BODS v0.4 statements.

    A bundle read from the index (``tier: index``) is dated by the files'
    cut (Phase 315); one read from the page keeps the factories' dates.
    Returns an empty iterable for stub bundles or missing company data.
    """
    if bundle and bundle.get("tier") == "index" and bundle.get("cut"):
        yield from dated_by_cut(_map_ariregister(bundle), bundle.get("cut"))
    else:
        yield from _map_ariregister(bundle)


def _map_ariregister(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    if not bundle or bundle.get("is_stub") or bundle.get("not_found"):
        return
    if not (bundle.get("registry_code") or "") or not (bundle.get("name") or ""):
        return

    # One set of seen parties across the subject and the chain above it: a
    # person on two companies' lists (by hash) and a company that is both
    # the subject's shareholder and a related company's record are one node.
    seen_person_ids: set[str] = set()
    seen_entity_ids: set[str] = set()

    company_stmt, source_url = _ee_company_statement(bundle)
    yield company_stmt
    seen_entity_ids.add(company_stmt["statementId"])

    # The related companies' own full statements come before the subject's
    # party lists, so the thin entity statement a shareholder row builds for
    # the same registry code is skipped in favour of the register's record.
    related = bundle.get("related_companies") or {}
    related_stmts: list[tuple[dict[str, Any], dict[str, Any], str]] = []
    for sub in related.values():
        if not sub or not (sub.get("registry_code") or "") or not (sub.get("name") or ""):
            continue
        sub_stmt, sub_url = _ee_company_statement(sub)
        if sub_stmt["statementId"] in seen_entity_ids:
            continue
        yield sub_stmt
        seen_entity_ids.add(sub_stmt["statementId"])
        related_stmts.append((sub, sub_stmt, sub_url))

    yield from _ee_company_parties(
        bundle, company_stmt["statementId"], source_url, seen_person_ids, seen_entity_ids,
        include_founders=True,
    )
    # A related company's parties: its shareholders, officers and declared
    # owners, never its founders (history of an intermediary is not the
    # subject's network).
    for sub, sub_stmt, sub_url in related_stmts:
        yield from _ee_company_parties(
            sub, sub_stmt["statementId"], sub_url, seen_person_ids, seen_entity_ids,
            include_founders=False,
        )


def _ee_company_statement(bundle: dict[str, Any]) -> tuple[dict[str, Any], str]:
    """The entity statement for one company bundle (the subject or a
    related company), with its register status applied, and its source URL."""
    registry_code: str = bundle.get("registry_code") or ""
    name: str = bundle.get("name") or ""
    source_url = bundle.get("link") or (
        f"https://ariregister.rik.ee/eng/company/{registry_code}"
    )

    # ── 1. Entity statement for the company ─────────────────────────────
    reg_date = bundle.get("registration_date")
    if reg_date and len(reg_date) == 10 and reg_date[4] == "-":
        founding_date = reg_date  # already ISO
    else:
        founding_date = _ee_date(reg_date)

    vat = bundle.get("vat_number") or ""
    identifiers: list[dict[str, str]] = [
        {
            # The registry code (registrikood). EE-KMKR until Phase 239 —
            # but KMKR is Estonia's VAT number, which the next identifier
            # carries; the registry code is the one this mapper already
            # writes as EE-ARIREGISTER for corporate shareholders, and the
            # scheme GLEIF's RA000181 now maps to.
            "id": registry_code,
            "scheme": "EE-ARIREGISTER",
            "schemeName": "Estonian e-Business Register (Äriregister)",
        }
    ]
    if vat:
        identifiers.append({
            "id": vat,
            "scheme": "EE-KMKR",
            "schemeName": "Estonian VAT number (Äriregister)",
        })

    address_str = bundle.get("address") or ""
    addresses = (
        [_addr("registered", address_str, "EE")]
        if address_str
        else []
    )
    legal_form = (bundle.get("legal_form") or "").strip()
    subtype = (bundle.get("legal_form_subtype") or "").strip()
    entity_details = legal_form + (f" — {subtype}" if subtype else "") or None
    entity_type = "stateBody" if legal_form in _EE_STATE_BODY_FORMS else "registeredEntity"

    company_stmt = make_entity_statement(
        source_id="ariregister",
        local_id=registry_code,
        name=name,
        jurisdiction=("Estonia", "EE"),
        identifiers=identifiers,
        founding_date=founding_date,
        addresses=addresses,
        entity_type=entity_type,
        entity_details=entity_details,
        source_url=source_url,
    )
    # e-Business Register status label as shown on the company page
    # (Phase 151): "Entered into the register" is live; liquidation /
    # bankruptcy are pending; "Deleted from the register" is terminal.
    _liveness.apply_register_status(
        company_stmt,
        source_label=SOURCE_NAMES["ariregister"],
        liveness=_liveness.classify(
            _ee_status(bundle),
            live=("Entered into the register", "Registered"),
            pending=("In liquidation", "Liquidation", "Bankrupt", "In bankruptcy", "Being deleted"),
            terminal=("Deleted from the register", "Deleted", "Removed from the register"),
        ),
        raw=bundle.get("status_text") or bundle.get("status") or None,
    )
    return company_stmt, source_url


def _ee_company_parties(
    bundle: dict[str, Any],
    company_stmt_id: str,
    source_url: str,
    seen_person_ids: set[str],
    seen_entity_ids: set[str],
    *,
    include_founders: bool,
) -> Iterable[dict[str, Any]]:
    """Sections 2–5 for one company: its shareholders (OÜ list and the AS
    rows the register records), the persons on its registry card, its
    declared beneficial owners and — for the subject only — its founders."""

    # A person is emitted once, on the first list naming them; the residence
    # country only the beneficial-owner list carries is gathered first so a
    # shareholder-director-owner's one statement has it.
    residences: dict[str, list[dict[str, Any]]] = {}
    for bo in bundle.get("beneficial_owners") or []:
        address = _ee_residence(bo)
        if address:
            residences.setdefault(_ee_person_id({
                "isikukood_hash": bo.get("isikukood_hash"),
                "kirje_id": str(bo.get("kirje_id") or ""),
                "eesnimi": (bo.get("eesnimi") or "").strip(),
                "nimi_arinimi": (bo.get("nimi") or "").strip(),
            }), address)

    # The persons outside the registry card (index tier only), split by what
    # they are to the company. A founder still on the shareholder list is
    # said on that holding; one no longer holding becomes a closed record
    # in section 5. ``current_keys`` is every party with a current
    # shareholding (OÜ list + AS rows), so a founder is matched before the
    # list is walked.
    off_card = bundle.get("off_card_persons") or []
    as_shareholders = [
        row for row in off_card
        if (row.get("isiku_roll") or "").strip() in _EE_OFF_CARD_SHAREHOLDER_ROLES
    ]
    founders: dict[str, dict[str, Any]] = {}
    for row in off_card if include_founders else []:
        if (row.get("isiku_roll") or "").strip() in _EE_OFF_CARD_FOUNDER_LABELS:
            founders.setdefault(_ee_party_key(row, str(row.get("kirje_id") or ""), "fnd"), row)
    shareholder_rows = [("sh", sh) for sh in bundle.get("shareholders") or []]
    shareholder_rows += [("as", sh) for sh in as_shareholders]
    # A row with an end date is an ended holding, not a current one (the
    # page path can carry those; the files do not).
    current_keys = {
        _ee_party_key(sh, str(sh.get("kirje_id") or ""), prefix)
        for prefix, sh in shareholder_rows if not _ee_date(sh.get("lopp_kpv"))
    }

    for prefix, sh in shareholder_rows:
        isiku_tyyp = sh.get("isiku_tyyp") or "F"
        kirje_id = str(sh.get("kirje_id") or "")
        interests: list[dict[str, Any]] = [_ee_shareholding_interest(sh)]
        if prefix == "as":
            # Recorded by the register itself, not the OÜ shareholder list:
            # the sole shareholder of an AS, or a Nasdaq CSD entry.
            interests[0]["details"] = "; ".join(p for p in (
                "Shareholder of a public limited company (aktsionär), as recorded by the register",
                interests[0].get("details"),
            ) if p)
        founder_row = founders.get(_ee_party_key(sh, kirje_id, prefix))
        founder_notes = (
            [commenting(
                pointer("recordDetails", "interests", 0),
                "Also recorded as a founder of the company: " + _ee_founder_note(founder_row) + ".",
            )] if founder_row else []
        )
        if isiku_tyyp == "J":
            # Corporate shareholder — emit an entity statement
            corp_name = (sh.get("nimi_arinimi") or "").strip()
            if not corp_name:
                continue
            corp_local_id, corp_ids, jurisdiction, note = _ee_foreign_entity(sh, kirje_id, "sh")
            corp_code = corp_ids[0]["id"] if corp_ids else ""
            corp_stmt = make_entity_statement(
                source_id="ariregister",
                local_id=corp_local_id,
                name=corp_name,
                jurisdiction=jurisdiction,
                identifiers=corp_ids,
                source_url=(
                    f"https://ariregister.rik.ee/eng/company/{corp_code}"
                    if corp_code
                    else None
                ),
            )
            if note:
                corp_stmt.setdefault("annotations", []).append(
                    commenting(pointer("recordDetails"), note)
                )
            if corp_stmt["statementId"] not in seen_entity_ids:
                yield corp_stmt
                seen_entity_ids.add(corp_stmt["statementId"])
            rel = make_relationship_statement(
                source_id="ariregister",
                local_id=f"{prefix}-{kirje_id}",
                subject_statement_id=company_stmt_id,
                interested_party_statement_id=corp_stmt["statementId"],
                interested_party_type="entity",
                interests=interests,
                source_url=source_url,
            )
            if founder_notes:
                rel.setdefault("annotations", []).extend(founder_notes)
            yield rel
        else:
            # Natural person shareholder
            person_id = _ee_person_id(sh)
            full_name = _ee_full_name(sh)
            if not full_name or full_name == "Unknown":
                continue
            if person_id not in seen_person_ids:
                person_stmt = make_person_statement(
                    source_id="ariregister",
                    local_id=person_id,
                    full_name=full_name,
                    nationalities=_ee_nationalities(sh),
                    birth_date=_ee_date(sh.get("synniaeg")),
                    addresses=residences.get(person_id, []),
                    identifiers=_ee_person_identifiers(sh),
                    source_url=source_url,
                )
                yield person_stmt
                seen_person_ids.add(person_id)
            else:
                person_stmt = {
                    "statementId": _stable_id("ariregister", "person", person_id)
                }
            rel = make_relationship_statement(
                source_id="ariregister",
                local_id=f"{prefix}-{kirje_id}",
                subject_statement_id=company_stmt_id,
                interested_party_statement_id=person_stmt["statementId"],
                interested_party_type="person",
                interests=interests,
                source_url=source_url,
            )
            if founder_notes:
                rel.setdefault("annotations", []).extend(founder_notes)
            yield rel

    # ── 3. Officers (kaardile_kantud_isikud) ─────────────────────────────
    for officer in bundle.get("officers") or []:
        role_code = officer.get("isiku_roll") or ""
        if role_code not in _EE_OFFICER_ROLE_MAP:
            continue  # skip roles that are no interest (KISIK contact, ORP share registrar)
        interest_type, role_label = _EE_OFFICER_ROLE_MAP[role_code]
        start_date = _ee_date(officer.get("algus_kpv"))
        end_date = _ee_date(officer.get("lopp_kpv"))
        kirje_id = str(officer.get("kirje_id") or "")

        interests = [{"type": interest_type, "details": role_label}]
        contribution = (officer.get("osamaks") or "").strip()
        if contribution:
            # A partner's contribution to a partnership (täisosanik / usaldusosanik).
            interests[0]["details"] += f"; contribution {contribution} {officer.get('osamaksu_valuuta') or ''}".rstrip()
        if start_date:
            interests[0]["startDate"] = start_date
        if end_date:
            interests[0]["endDate"] = end_date

        if (officer.get("isiku_tyyp") or "F") == "J":
            # A legal person in the role — a fund's general partner, a fund
            # manager, a corporate liquidator, a state agency's superior
            # authority — is a company and is published as one (follow-up to
            # Phase 293). Keyed on its registry code, the key a corporate
            # shareholder gets, so a GP that is also a partner is one entity.
            corp_name = (officer.get("nimi_arinimi") or "").strip()
            if not corp_name:
                continue
            corp_local_id, corp_ids, jurisdiction, note = _ee_foreign_entity(officer, kirje_id, "off")
            corp_code = corp_ids[0]["id"] if corp_ids else ""
            corp_stmt = make_entity_statement(
                source_id="ariregister",
                local_id=corp_local_id,
                name=corp_name,
                jurisdiction=jurisdiction,
                identifiers=corp_ids,
                source_url=(
                    f"https://ariregister.rik.ee/eng/company/{corp_code}"
                    if corp_code
                    else None
                ),
            )
            if note:
                corp_stmt.setdefault("annotations", []).append(
                    commenting(pointer("recordDetails"), note)
                )
            if corp_stmt["statementId"] not in seen_entity_ids:
                yield corp_stmt
                seen_entity_ids.add(corp_stmt["statementId"])
            yield make_relationship_statement(
                source_id="ariregister",
                local_id=f"off-{kirje_id}",
                subject_statement_id=company_stmt_id,
                interested_party_statement_id=corp_stmt["statementId"],
                interested_party_type="entity",
                interests=interests,
                source_url=source_url,
            )
            continue

        person_id = _ee_person_id(officer)
        full_name = _ee_full_name(officer)
        if not full_name or full_name == "Unknown":
            continue

        if person_id not in seen_person_ids:
            person_stmt = make_person_statement(
                source_id="ariregister",
                local_id=person_id,
                full_name=full_name,
                nationalities=_ee_nationalities(officer),
                birth_date=_ee_date(officer.get("synniaeg")),
                addresses=residences.get(person_id, []),
                identifiers=_ee_person_identifiers(officer),
                source_url=source_url,
            )
            yield person_stmt
            seen_person_ids.add(person_id)
        else:
            person_stmt = {
                "statementId": _stable_id("ariregister", "person", person_id)
            }
        yield make_relationship_statement(
            source_id="ariregister",
            local_id=f"off-{kirje_id}",
            subject_statement_id=company_stmt_id,
            interested_party_statement_id=person_stmt["statementId"],
            interested_party_type="person",
            interests=interests,
            source_url=source_url,
        )

    # ── 4. Beneficial owners (kasusaajad) ─────────────────────────────────
    # NOTE: Include only while Estonian law makes this data publicly available.
    # Set include_beneficial_owners=False in the adapter call to suppress.
    for bo in bundle.get("beneficial_owners") or []:
        kirje_id = str(bo.get("kirje_id") or "")
        start_date = _ee_date(bo.get("algus_kpv"))
        end_date = _ee_date(bo.get("lopp_kpv"))
        control_code = (bo.get("kontrolli_teostamise_viis") or "").strip()
        interest_type, direct_or_indirect, control_label = _EE_BO_CONTROL_MAP.get(
            control_code, ("otherInfluenceOrControl", None, "")
        )

        # kasusaajad records ARE the Estonian BO declarations — the flag
        # comes from the regimes registry (ariregister/kasusaaja_bo -> true).
        interest: dict[str, Any] = set_beneficial_ownership(
            {"type": interest_type}, "ariregister", record_kind="kasusaaja_bo"
        )
        if direct_or_indirect:
            interest["directOrIndirect"] = direct_or_indirect
        control_text = " ".join((bo.get("kontrolli_teostamise_viis_tekstina") or "").split())
        if control_label and control_text:
            interest["details"] = f"{control_label} ({control_text})"
        elif control_label or control_text:
            interest["details"] = control_label or control_text
        if start_date:
            interest["startDate"] = start_date
        if end_date:
            interest["endDate"] = end_date
        interests = [interest]

        first = (bo.get("eesnimi") or "").strip()
        last = (bo.get("nimi") or "").strip()
        full_name = f"{first} {last}".strip() if first or last else ""
        if not full_name:
            continue

        person_id = _ee_person_id({
            "isikukood_hash": bo.get("isikukood_hash"),
            "kirje_id": kirje_id,
            "eesnimi": first,
            "nimi_arinimi": last,
        })
        if person_id not in seen_person_ids:
            person_stmt = make_person_statement(
                source_id="ariregister",
                local_id=person_id,
                full_name=full_name,
                nationalities=_ee_nationalities(bo),
                birth_date=_ee_date(bo.get("synniaeg")),
                addresses=_ee_residence(bo),
                identifiers=_ee_person_identifiers(bo),
                source_url=source_url,
            )
            yield person_stmt
            seen_person_ids.add(person_id)
        else:
            person_stmt = {
                "statementId": _stable_id("ariregister", "person", person_id)
            }
        rel = make_relationship_statement(
            source_id="ariregister",
            local_id=f"bo-{kirje_id}",
            subject_statement_id=company_stmt_id,
            interested_party_statement_id=person_stmt["statementId"],
            interested_party_type="person",
            interests=interests,
            source_url=source_url,
        )
        if bo.get("lahknevusteade_esitatud") is True:
            # A discrepancy report (lahknevusteade) has been filed against
            # this entry by an obliged entity or authority — the register
            # publishes the flag, not the report. Said beside the interest,
            # never read as a finding against the person.
            rel.setdefault("annotations", []).append(commenting(
                pointer("recordDetails", "interests", 0),
                "A discrepancy report (lahknevusteade) is recorded against "
                "this beneficial-owner entry in the e-Business Register; the "
                "register publishes the flag, not the report's content.",
            ))
        yield rel

    # ── 5. Persons outside the registry card (kaardivalised_isikud) ──────
    # The AS shareholders (role S) went through section 2 with the OÜ
    # list. Here: the supervisory board, serving-only like every officer
    # (Phase 317), and founders who no longer hold — ownership history,
    # closed with no end date because the register publishes none.
    for row in off_card:
        role_code = (row.get("isiku_roll") or "").strip()
        kirje_id = str(row.get("kirje_id") or "")
        if role_code in _EE_OFF_CARD_ROLE_MAP:
            interest_type, role_label = _EE_OFF_CARD_ROLE_MAP[role_code]
            interest = {"type": interest_type, "details": role_label}
            start_date = _ee_date(row.get("algus_kpv"))
            end_date = _ee_date(row.get("lopp_kpv"))
            if start_date:
                interest["startDate"] = start_date
            if end_date:
                interest["endDate"] = end_date
            record_status = "new"
        elif role_code in _EE_OFF_CARD_FOUNDER_LABELS:
            if not include_founders or _ee_party_key(row, kirje_id, "fnd") in current_keys:
                continue  # not the subject, or said on the founder's current shareholding
            interest = _ee_shareholding_interest(row)
            interest["details"] = "; ".join(p for p in (
                _ee_founder_note(row),
                interest.get("details"),
                "a founding shareholding, no longer on the current list; the "
                "register publishes no end date",
            ) if p)
            record_status = "closed"
        else:
            continue  # auditors, bankruptcy committee, authorised persons…
        party = _ee_party_statements(row, kirje_id, "fnd", seen_person_ids, seen_entity_ids, residences, source_url)
        if party is None:
            continue
        party_stmts, party_id, party_type = party
        yield from party_stmts
        yield make_relationship_statement(
            source_id="ariregister",
            local_id=f"fnd-{kirje_id}",
            subject_statement_id=company_stmt_id,
            interested_party_statement_id=party_id,
            interested_party_type=party_type,
            interests=[interest],
            source_url=source_url,
            record_status=record_status,
        )


def _ee_party_statements(
    row: dict[str, Any],
    kirje_id: str,
    prefix: str,
    seen_person_ids: set[str],
    seen_entity_ids: set[str],
    residences: dict[str, list[dict[str, Any]]],
    source_url: str,
) -> tuple[list[dict[str, Any]], str, str] | None:
    """The party a row names, as (statements to publish, its statementId,
    ``person`` / ``entity``) — the statements list empty when the party
    was already published under the same key. None for a nameless row."""
    if (row.get("isiku_tyyp") or "F") == "J":
        corp_name = (row.get("nimi_arinimi") or "").strip()
        if not corp_name:
            return None
        corp_local_id, corp_ids, jurisdiction, note = _ee_foreign_entity(row, kirje_id, prefix)
        corp_code = corp_ids[0]["id"] if corp_ids else ""
        corp_stmt = make_entity_statement(
            source_id="ariregister",
            local_id=corp_local_id,
            name=corp_name,
            jurisdiction=jurisdiction,
            identifiers=corp_ids,
            source_url=f"https://ariregister.rik.ee/eng/company/{corp_code}" if corp_code else None,
        )
        if note:
            corp_stmt.setdefault("annotations", []).append(
                commenting(pointer("recordDetails"), note)
            )
        if corp_stmt["statementId"] in seen_entity_ids:
            return [], corp_stmt["statementId"], "entity"
        seen_entity_ids.add(corp_stmt["statementId"])
        return [corp_stmt], corp_stmt["statementId"], "entity"
    person_id = _ee_person_id(row)
    full_name = _ee_full_name(row)
    if not full_name or full_name == "Unknown":
        return None
    if person_id in seen_person_ids:
        return [], _stable_id("ariregister", "person", person_id), "person"
    person_stmt = make_person_statement(
        source_id="ariregister",
        local_id=person_id,
        full_name=full_name,
        nationalities=_ee_nationalities(row),
        birth_date=_ee_date(row.get("synniaeg")),
        addresses=residences.get(person_id) or _ee_residence(row),
        identifiers=_ee_person_identifiers(row),
        source_url=source_url,
    )
    seen_person_ids.add(person_id)
    return [person_stmt], person_stmt["statementId"], "person"
