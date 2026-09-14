"""Ukraine — ЄДР, the Unified State Register of Legal Entities (data.gov.ua).

Bulk/offline adapter over a pre-built SQLite index, in the ``cyprus_drcor``
shape: not in ``REGISTRY``, not wired into the lookup dispatch, and returning a
stub when no index is configured. Build the index with
``scripts/build_edr_ukraine_index.py`` and point ``EDR_UKRAINE_DB_FILE`` at it.

Why bulk and not live
---------------------
There is no government API for the ЄДР. The only official channel is a weekly
ZIP of XML on data.gov.ua: ``UO.zip`` is 327 MB compressed and **3.16 GB of XML**
holding 2,017,706 legal entities (measured against the 8 Sep 2026 export). The
whole file streams through ``iterparse`` in 113 s at 209 MB peak RSS, so the
cost is storage, not parsing — which is why the index is built once, offline.

What the register publishes
---------------------------
Unusually for a commercial register, the ЄДР files **beneficial owners** as a
first-class element: 533,958 entities carry a named UBO (690,421 records), and
a further 117,266 record a *stated reason* for the absence of one. Access to
the open data was suspended after the full-scale invasion in February 2022 and
restored on 19 January 2026 under Law 4576-IX, which also withholds precise
locations, addresses and KVED activity codes for the duration of martial law
and one year after. **Nothing here reconstructs a withheld field**: the absence
of an address is the publisher's decision, not a gap for OpenCheck to fill.

Format traps, each measured against the real export
---------------------------------------------------
* The XML is declared ``windows-1251`` — the *schema* zip is UTF-8, the data
  file is not. ``iterparse`` honours the declaration; anything that assumes
  UTF-8 gets mojibake.
* **EDRPOU is not a primary key.** 24,885 codes appear more than once (26,678
  extra rows), almost always a ``припинено`` (terminated) record beside a live
  one, and 150 records carry a blank code. The builder dedupes, preferring
  ``зареєстровано``.
* EDRPOU is occasionally fewer than 8 digits and is zero-padded here to match
  GLEIF ``registeredAs``.
* Founder holdings are **hryvnia amounts, not percentages** — see
  ``parse_founder``.

The four repeating elements (``BENEFICIARIES``, ``FOUNDERS``, ``SIGNERS``,
``MEMBERS``) are all ``xs:string`` in the register's own schema: semicolon-
delimited text, not structured XML. The parsers below are deliberately the
only place that text is interpreted, so the index stays dumb and the reading
can change without a 3 GB rebuild.

Licence: CC BY 4.0, Ministry of Justice of Ukraine, via data.gov.ua.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from pathlib import Path
from typing import Any

from ..config import get_settings
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.edr_ukraine import EdrUkraineBundle

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GLEIF registration authorities
# ---------------------------------------------------------------------------
# THREE authorities register Ukrainian LEIs and **all three carry the 8-digit
# EDRPOU in ``registeredAs``** (verified against all 303 live UA legal-address
# LEI records, 14 Sep 2026: RA000567 238, RA001027 34, RA001026 11, the rest
# unassigned). Keying on the register's own code alone would silently miss 15%
# of Ukrainian LEIs — the Hong Kong / Singapore shape. Treat this as a set, as
# ``zefix`` does with ``CH_RA_CODES``, never as a single constant.
UA_RA_CODES: frozenset[str] = frozenset(
    {
        "RA000567",  # ЄДР — Unified State Register (State Enterprise IRC)
        "RA001027",  # National Bank of Ukraine
        "RA001026",  # National Securities and Stock Market Commission
    }
)
UA_EDR_RA_CODE: str = "RA000567"

_DATASET_URL = "https://data.gov.ua/dataset/a1799820-195b-4982-8141-6e84f58103e7"
_SEARCH_URL = "https://usr.minjust.gov.ua/content/free-search"

#: ``STAN`` value for a live registration. Every other value is some flavour of
#: terminated, in-termination, cancelled or bankrupt.
ACTIVE_STAN = "зареєстровано"

# ---------------------------------------------------------------------------
# Field grammar
# ---------------------------------------------------------------------------
# BENEFICIARY:
#   NAME; громадянство: X; <influence>; [відсоток частки - N];
#   [відсоток частки (непрямий вплив) - N]; [інший характер та міра впливу: …]
# or, where no UBO is filed:
#   причина відсутності: <reason>
#
# 684,256 of 690,421 named records have exactly four fields, 5,906 have five,
# 258 have six, and exactly ONE is malformed. A plain ``split(";")`` is
# sufficient; the pre-2022 format needed ML because beneficial ownership was
# buried in free-text FOUNDERS, and that is no longer true.

_ABSENCE_PREFIX = "причина відсутності"
_CITIZENSHIP_PREFIX = "громадянство"
_OTHER_INFLUENCE_PREFIX = "інший характер та міра впливу"

#: The register's closed vocabulary of influence types, in its own words.
INFLUENCE_DIRECT = "Прямий вирішальний вплив"
INFLUENCE_INDIRECT = "Непрямий вирішальний вплив"
INFLUENCE_BOTH = "Прямий та непрямий вирішальний вплив"

_INFLUENCE_MAP: dict[str, str] = {
    INFLUENCE_DIRECT.lower(): "direct",
    INFLUENCE_INDIRECT.lower(): "indirect",
    INFLUENCE_BOTH.lower(): "both",
}

# "відсоток частки - N" (direct) and "відсоток частки (непрямий вплив) - N".
# The parenthetical form must be tried FIRST — the direct pattern would
# otherwise match the shared "відсоток частки" prefix of the indirect one.
_PCT_INDIRECT = re.compile(
    r"відсоток\s+частки\s*\(непрямий\s+вплив\)\s*[-–—:]?\s*([\d.,]+)", re.I
)
_PCT_DIRECT = re.compile(r"відсоток\s+частки\s*(?!\()\s*[-–—:]?\s*([\d.,]+)", re.I)

# FOUNDER: NAME; [EDRPOU]; [громадянство: X]; розмір частки - N,NN грн.
_FOUNDER_AMOUNT = re.compile(
    r"розмір\s+частки\s*[-–—:]?\s*([\d\s  .,]+?)\s*грн", re.I
)
_BARE_CODE = re.compile(r"^\d{6,10}$")
_NUMBER = re.compile(r"[\d\s  .,]+")


def normalise_edrpou(raw: str) -> str:
    """Return an 8-digit EDRPOU, or "" when there are no digits.

    The register occasionally files fewer than 8 digits; GLEIF's
    ``registeredAs`` is consistently 8, so pad rather than compare ragged.
    """
    digits = re.sub(r"\D", "", str(raw or "").strip())
    if not digits:
        return ""
    return digits.zfill(8)[-8:] if len(digits) <= 8 else digits


def _to_float(raw: str | None) -> float | None:
    """Parse a Ukrainian decimal ("3 620,10") to a float, or None."""
    if not raw:
        return None
    cleaned = (
        str(raw)
        .replace(" ", "")
        .replace(" ", "")
        .replace(" ", "")
        .replace(",", ".")
    )
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_beneficiary(raw: str) -> dict[str, Any]:
    """Parse one ``BENEFICIARY`` string.

    Returns ``{"kind": "absence", "reason_text": …}`` for a filed reason, or
    ``{"kind": "named", "name", "citizenship", "influence", "pct_direct",
    "pct_indirect", "note"}`` for a declared UBO. A string that cannot be read
    as either returns ``{"kind": "unparsed", "raw": …}`` rather than guessing —
    there is exactly one such record in the register and it should stay
    visible, not be silently coerced into a person.
    """
    text = (raw or "").strip()
    if not text:
        return {"kind": "unparsed", "raw": text}
    if text.lower().startswith(_ABSENCE_PREFIX):
        return {
            "kind": "absence",
            "reason_text": text.split(":", 1)[-1].strip() if ":" in text else "",
            "raw": text,
        }

    parts = [p.strip() for p in text.split(";") if p.strip()]
    if len(parts) < 3:
        return {"kind": "unparsed", "raw": text}

    citizenship: str | None = None
    influence: str | None = None
    note: str | None = None
    for part in parts[1:]:
        low = part.lower()
        if low.startswith(_CITIZENSHIP_PREFIX):
            citizenship = part.split(":", 1)[-1].strip() or None
        elif low.startswith(_OTHER_INFLUENCE_PREFIX):
            note = part.split(":", 1)[-1].strip() or None
        elif low in _INFLUENCE_MAP:
            influence = _INFLUENCE_MAP[low]

    m_ind = _PCT_INDIRECT.search(text)
    # Blank the indirect clause before looking for the direct one, so the
    # shared "відсоток частки" prefix cannot be double-counted.
    direct_hunting_ground = _PCT_INDIRECT.sub(" ", text)
    m_dir = _PCT_DIRECT.search(direct_hunting_ground)

    return {
        "kind": "named",
        "name": parts[0],
        "citizenship": citizenship,
        "influence": influence,
        "pct_direct": _to_float(m_dir.group(1)) if m_dir else None,
        "pct_indirect": _to_float(m_ind.group(1)) if m_ind else None,
        "note": note,
        "raw": text,
    }


def parse_founder(raw: str) -> dict[str, Any]:
    """Parse one ``FOUNDER`` string.

    A founder is a ``засновник`` — a participant holding equity, **not** an
    officer. The holding is filed as a hryvnia capital contribution
    (``розмір частки - 134493517,83 грн.``), never as a percentage, so
    ``amount_uah`` is a money amount and has no home in a BODS ``share``
    without the authorised capital to divide it by (see
    ``edr_ukraine_shares`` in the mapper).

    ``code`` is present only where the founder is itself a legal entity.
    """
    text = (raw or "").strip()
    if not text:
        return {"name": "", "code": None, "citizenship": None, "amount_uah": None}
    parts = [p.strip() for p in text.split(";") if p.strip()]
    code: str | None = None
    citizenship: str | None = None
    for part in parts[1:]:
        if _BARE_CODE.match(part):
            code = normalise_edrpou(part)
        elif part.lower().startswith(_CITIZENSHIP_PREFIX):
            citizenship = part.split(":", 1)[-1].strip() or None
    m = _FOUNDER_AMOUNT.search(text)
    return {
        "name": parts[0] if parts else "",
        "code": code,
        "citizenship": citizenship,
        "amount_uah": _to_float(m.group(1)) if m else None,
        "raw": text,
    }


def parse_role_holder(raw: str) -> dict[str, Any]:
    """Parse one ``SIGNER`` or ``MEMBER`` string into name + role text.

    SIGNERS carry the head of the legal entity and anyone who may act for it
    (``керівник``, ``представник``, with the powers spelled out); MEMBERS carry
    members of governing bodies (``Голова Правління``, ``Член Ради``). Both are
    ``name; <role and qualifiers>``; the role half is kept verbatim because the
    register writes it freely and translating it would invent precision.
    """
    text = (raw or "").strip()
    if not text:
        return {"name": "", "role": None}
    parts = [p.strip() for p in text.split(";") if p.strip()]
    return {
        "name": parts[0] if parts else "",
        "role": "; ".join(parts[1:]) or None,
        "raw": text,
    }


def parse_capital(raw: str) -> float | None:
    """Parse ``AUTHORIZED_CAPITAL`` to a number of hryvnia, or None."""
    m = _NUMBER.search(str(raw or ""))
    return _to_float(m.group(0)) if m else None


# ---------------------------------------------------------------------------
# Absence reasons — BODS missing-information modelling (data-standard #389)
# ---------------------------------------------------------------------------
# Every one of the 117,266 absence records carries a stated reason; the field
# is literally "причина відсутності". Classified on the real corpus, with the
# field label and its boilerplate restatement stripped first — the word
# "відсутності" appears in almost every reason as part of the label, and
# matching on it naively claims everything.
#
# The BODS reason codes are the v0.4 camelCase values already used by
# ``_ch_psc_statement_statements`` for Companies House PSC statements.

_ABSENCE_BOILERPLATE = re.compile(
    r"(причина\s+)?відсутност[іи]\s+кінцевого\s+бенефіціарного\s+власника"
    r"(\s+юридичної\s+особи)?\s*(є|[-–—])?\s*",
    re.I,
)

#: Ordered most-specific-first. Order matters: nearly every reason contains
#: some form of "відсутн", so the catch-all must be last.
_ABSENCE_RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("structure_filed", re.compile(r"структур\w*\s+власност", re.I)),
    (
        "state_or_municipal",
        re.compile(
            r"держав|комунальн|орган\w*\s+(місцев|виконавч)|територіальн\w*\s+громад",
            re.I,
        ),
    ),
    (
        "legal_form_exemption",
        re.compile(
            r"неприбутков|громадськ\w*\s+(організац|формуванн)|релігійн|профспілк"
            r"|кооператив|осбб|споживч\w*\s+товариств|партія|адвокат"
            r"|об'єднанн\w*\s+співвласник",
            re.I,
        ),
    ),
    (
        "statutory_exemption",
        re.compile(
            r"не\s+передбач|не\s+вимага|закон|ст\.?\s*9|п\.?\s*9|кодекс"
            r"|відповідно\s+до\s+законод|згідно\s+(з\s+)?(чинн|закон)",
            re.I,
        ),
    ),
    (
        "not_stated",
        re.compile(
            r"не\s+зазнач|незазнач|не\s+надан|не\s+надавал|не\s+подан"
            r"|не\s+визначен|не\s+встановлен|не\s+виявлен",
            re.I,
        ),
    ),
    (
        "no_qualifying_person",
        re.compile(r"відсутн|не\s+має|немає|не\s+відповіда|жоден|жодна", re.I),
    ),
)

#: Classified reason → BODS v0.4 ``unspecifiedReason``.
#:
#: The values are the seven in the v0.4 codelist, checked against
#: ``libcovebods``' own ``schema-0-4-0/components.json`` rather than against a
#: summary — an earlier draft of this mapping used ``unknownUnknown``, which
#: does not exist, and collapsed the exemptions onto ``noBeneficialOwners`` in
#: the belief that v0.4 had no exemption value. It has two.
#:
#: The split that matters: "no natural person meets the definition" is a
#: finding (``noBeneficialOwners``), while "this legal form is not required to
#: file" is an exemption (``subjectExemptFromDisclosure``) — data-standard
#: #389's first category, and 13,726 Ukrainian records. Collapsing the two
#: would say "we looked and there is nobody" about companies that were never
#: required to look.
ABSENCE_BODS_REASON: dict[str, str] = {
    # A finding of absence: the company looked and no natural person qualifies.
    "no_qualifying_person": "noBeneficialOwners",
    # Still a finding of absence — the filing says the REASON is documented in
    # the separately filed ownership-structure declaration, not that the owner
    # is recorded elsewhere.
    "structure_filed": "noBeneficialOwners",
    # A state or municipal founder has no natural-person owner to name.
    "state_or_municipal": "noBeneficialOwners",
    # Exemptions: the duty to file does not apply to this subject.
    "statutory_exemption": "subjectExemptFromDisclosure",
    "legal_form_exemption": "subjectExemptFromDisclosure",
    # An absence was filed but the reason was left out. That is not a claim
    # that no owner exists, and there is no identified party who withheld it,
    # so neither "noBeneficialOwners" nor "interestedPartyHasNotProvided…".
    "not_stated": "unknown",
    "blank": "unknown",
    "other": "unknown",
}


def classify_absence(reason_text: str) -> str:
    """Classify a filed absence reason. See ``ABSENCE_BODS_REASON``.

    ``structure_filed`` is a deliberate reading: "причина відсутності …
    зазначена у структурі власності" says the *reason for the absence* is
    documented in the separately filed ownership-structure declaration. It
    still asserts that there is no UBO — it is not saying the UBO is recorded
    elsewhere — so it maps to ``noBeneficialOwners`` with the pointer kept in
    the description, not to a withheld-information code.
    """
    text = (reason_text or "").strip()
    if not text or text in {"-", "--", ".", "_"}:
        return "blank"
    core = _ABSENCE_BOILERPLATE.sub("", text).strip(" .,-–—:") or text
    for code, pattern in _ABSENCE_RULES:
        if pattern.search(core):
            return code
    return "other"


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class EdrUkraineAdapter(SourceAdapter):
    """Source adapter for the Ukrainian ЄДР (local SQLite index)."""

    id = "edr_ukraine"

    def __init__(self) -> None:
        self._db: sqlite3.Connection | None = None

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        db_path = getattr(settings, "edr_ukraine_db_file", None)
        live = bool(db_path and Path(db_path).exists())
        return SourceInfo(
            id=self.id,
            name="ЄДР — Unified State Register of Legal Entities (Ukraine)",
            homepage="https://usr.minjust.gov.ua/",
            description=(
                "Ukrainian company register published as open data by the "
                "Ministry of Justice on data.gov.ua. Carries founders with "
                "their hryvnia holdings, heads and signatories, governing-body "
                "members, and beneficial owners — or the register's stated "
                "reason for their absence. Addresses and activity codes are "
                "withheld at source under martial law."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Contains information from the Unified State Register of Legal "
                "Entities, Individual Entrepreneurs and Public Organisations, "
                "published by the Ministry of Justice of Ukraine on data.gov.ua "
                "under a Creative Commons Attribution 4.0 licence."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=live,
            is_national_register=True,
            country="UA",
        )

    # -- storage ----------------------------------------------------------
    def _conn(self) -> sqlite3.Connection | None:
        if self._db is not None:
            return self._db
        settings = get_settings()
        db_path = getattr(settings, "edr_ukraine_db_file", None)
        if not db_path:
            return None
        path = Path(db_path)
        if not path.exists():
            logger.warning("edr_ukraine: index not found at %s", db_path)
            return None
        conn = sqlite3.connect(str(path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        self._db = conn
        return self._db

    def _rows(self, table: str, edrpou: str) -> list[dict[str, Any]]:
        conn = self._conn()
        if conn is None:
            return []
        try:
            cur = conn.execute(
                f"SELECT * FROM {table} WHERE edrpou = ? ORDER BY seq", (edrpou,)
            )
        except sqlite3.OperationalError as exc:
            logger.warning("edr_ukraine: query on %s failed: %s", table, exc)
            return []
        return [dict(r) for r in cur.fetchall()]

    def _entity_row(self, edrpou: str) -> dict[str, Any] | None:
        conn = self._conn()
        if conn is None:
            return None
        try:
            cur = conn.execute("SELECT * FROM entity WHERE edrpou = ?", (edrpou,))
        except sqlite3.OperationalError as exc:
            logger.warning("edr_ukraine: entity query failed: %s", exc)
            return None
        row = cur.fetchone()
        return dict(row) if row is not None else None

    # -- SourceAdapter ----------------------------------------------------
    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []
        conn = self._conn()
        if conn is None:
            return self._stub_search(query)
        try:
            cur = conn.execute(
                "SELECT edrpou, name, stan FROM entity WHERE name LIKE ? LIMIT 10",
                (f"%{query}%",),
            )
        except sqlite3.OperationalError:
            return []
        hits: list[SourceHit] = []
        for row in cur.fetchall():
            hits.append(
                SourceHit(
                    source_id=self.id,
                    hit_id=row["edrpou"],
                    kind=SearchKind.ENTITY,
                    name=row["name"],
                    summary=f"UA · EDRPOU {row['edrpou']}",
                    identifiers={"ua_edrpou": row["edrpou"]},
                    raw={"edrpou": row["edrpou"], "stan": row["stan"]},
                    is_stub=False,
                )
            )
        return hits

    def _stub(self, edrpou: str, legal_name: str) -> dict[str, Any]:
        return {
            "source_id": self.id,
            "edrpou": edrpou,
            "name": legal_name or "",
            "entity": None,
            "beneficiaries": [],
            "founders": [],
            "signers": [],
            "members": [],
            "executive_power": None,
            "legal_name": legal_name,
            "link": _SEARCH_URL,
            "is_stub": True,
        }

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the ЄДР bundle for one EDRPOU code.

        ``hit_id`` is the EDRPOU (zero-padded here); ``legal_name`` is the
        GLEIF legal name, used for the display name and the stub.
        """
        edrpou = normalise_edrpou(hit_id)
        if not edrpou:
            return self._stub(edrpou, legal_name)

        entity = self._entity_row(edrpou)
        if entity is None:
            return self._stub(edrpou, legal_name)

        exec_rows = self._rows("executive_power", edrpou)
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "edrpou": edrpou,
            "name": entity.get("name") or legal_name or f"UA {edrpou}",
            "entity": entity,
            "beneficiaries": self._rows("beneficiary", edrpou),
            "founders": self._rows("founder", edrpou),
            "signers": self._rows("signer", edrpou),
            "members": self._rows("member", edrpou),
            "executive_power": exec_rows[0] if exec_rows else None,
            "legal_name": legal_name,
            "link": _DATASET_URL,
            "is_stub": False,
        }
        validate_raw("edr_ukraine", EdrUkraineBundle, bundle)
        return bundle

    def _stub_search(self, query: str) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="00000000",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary=(
                    "Stub ЄДР record — build the index with "
                    "scripts/build_edr_ukraine_index.py to enable live data."
                ),
                identifiers={"ua_edrpou": "00000000"},
                raw={"edrpou": "00000000"},
                is_stub=True,
            )
        ]
