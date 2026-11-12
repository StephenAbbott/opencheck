"""Shared identifier validation (LEI, ISIN, Wikidata QID, national IDs).

Phase A of the rigour adoption plan (Notion: "Apply rigour to OpenCheck").
Before this module, the LEI shape regex was copy-pasted across eleven call
sites in two incompatible shapes (``^[A-Z0-9]{20}$`` at the API boundaries,
``^[0-9A-Z]{18}[0-9]{2}$`` in the BODS mappers/reconciler) and the ISO 17442
mod 97-10 check digits were never verified anywhere — a mistyped LEI passed
every gate and burned a GLEIF round-trip before failing with a generic 404.

This module centralises all of it:

* ``LEI_PATH_SHAPE`` / ``LEI_STRICT_SHAPE`` — the two historical regexes,
  defined once. Path-safety call sites (``dispositions``) keep the permissive
  shape so stored disposition keys written before this phase stay readable.
* ``lei_check_digits_ok`` — ISO 17442 / ISO 7064 mod 97-10 validation.
  Prefers ``rigour.ids.LEI`` (the same implementation OpenSanctions runs,
  backed by python-stdnum) and falls back to a pure-Python mod-97 that is
  parity-tested against it, so ICU-less dev environments behave identically
  to production. Same belt-and-braces pattern as ``opencheck/matching.py``.
* ``classify_lei`` — "is this identifier value an LEI?" for the data paths
  that route identifiers by shape (BODS mappers, reconciler merge keys,
  Wikidata/TED adapters).

Checksum enforcement is governed by ``Settings.identifier_checksums_enforced``
(``OPENCHECK_IDENTIFIER_CHECKSUMS_ENFORCED``, default on). The test suite
turns it off in ``conftest.py`` — dozens of long-standing fixtures use
deliberately fake, shape-valid LEIs (``2138000000000000A001``,
``LEI0000000000000ACME``…) — and the dedicated tests re-enable it, the same
arrangement the rate limiter uses.
"""

from __future__ import annotations

import re
from collections.abc import Callable

from .config import get_settings

try:  # pragma: no cover - exercised via the ftm extra in CI/prod
    from rigour.ids import ISIN as _RIGOUR_ISIN
    from rigour.ids import LEI as _RIGOUR_LEI

    _HAS_RIGOUR = True
except ImportError:  # pragma: no cover - base install without the ftm extra
    _RIGOUR_ISIN = None  # type: ignore[assignment, misc]
    _RIGOUR_LEI = None  # type: ignore[assignment, misc]
    _HAS_RIGOUR = False

try:  # python-stdnum arrives with rigour (ftm extra); optional in base installs
    from stdnum import luhn as _stdnum_luhn  # noqa: F401
    from stdnum.br import cnpj as _stdnum_cnpj
    from stdnum.fi import ytunnus as _stdnum_ytunnus
    from stdnum.se import orgnr as _stdnum_orgnr

    _HAS_STDNUM = True
except ImportError:  # pragma: no cover - base install without the ftm extra
    _stdnum_cnpj = None  # type: ignore[assignment]
    _stdnum_ytunnus = None  # type: ignore[assignment]
    _stdnum_orgnr = None  # type: ignore[assignment]
    _HAS_STDNUM = False

# The permissive 20-character shape the API boundaries have always used.
# Kept for path-safety validation (dispositions keys) and the user-facing
# gates, so behaviour with checksum enforcement off is exactly as before.
LEI_PATH_SHAPE = re.compile(r"^[A-Z0-9]{20}$")

# ISO 17442 proper: 18 alphanumeric characters + 2 check DIGITS. The shape
# the BODS mappers and the reconciler have always classified on.
LEI_STRICT_SHAPE = re.compile(r"^[0-9A-Z]{18}[0-9]{2}$")

_ISIN_SHAPE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
_QID_SHAPE = re.compile(r"^Q[1-9][0-9]{0,17}$")


def checksums_enforced() -> bool:
    """Master switch for checksum *enforcement* (shape checks always apply)."""
    return get_settings().identifier_checksums_enforced


def normalise_lei(value: object) -> str:
    """Uppercase, whitespace-stripped candidate LEI ('' for empty input)."""
    if value is None:
        return ""
    return str(value).strip().upper()


#: Schemes whose values are numbers written with or without leading zeros —
#: one number, several spellings across publishers. The SEC's Central Index
#: Key is the case: EDGAR's filings and SEC EDGAR's own statements write
#: ``59478``, its URLs and Wikirate's metric ``0000059478``. Until the MCP
#: testing of 7 Nov 2026 the two never formed one merge key, so Eli Lilly's
#: SEC EDGAR record never joined the subject and its 13D/13G holders reached
#: no owners list, map or integrated figure. Only schemes where a leading zero
#: carries no meaning belong here: a Companies House number (``00102498``)
#: does not. Mirror of ``ZERO_PADDED_SCHEMES`` in
#: ``frontend/src/lib/reconcile.ts``; ``identifierMatchCases.json`` is run by
#: both sides.
ZERO_PADDED_SCHEMES: frozenset[str] = frozenset({"US-SEC-CIK"})


def match_value(scheme: object, value: str) -> str:
    """``value`` as identifier-merge keys compare it under ``scheme``.

    Strips leading zeros from an all-digit value under a
    ``ZERO_PADDED_SCHEMES`` scheme (keeping one zero for an all-zero value);
    returns every other value unchanged. The published identifier is never
    rewritten — this is the comparison form only.
    """
    if str(scheme or "").strip().upper() in ZERO_PADDED_SCHEMES and value.isdigit():
        return value.lstrip("0") or "0"
    return value


def _mod97_ok(value: str) -> bool:
    """ISO 7064 mod 97-10: letters map to 10..35, whole number ≡ 1 (mod 97)."""
    try:
        digits = "".join(str(int(ch, 36)) for ch in value)
    except ValueError:
        return False
    return int(digits) % 97 == 1


def lei_check_digits_ok(lei: str) -> bool:
    """ISO 17442 check-digit validation for an already shape-valid LEI."""
    if _HAS_RIGOUR:
        return _RIGOUR_LEI.is_valid(lei)
    return _mod97_ok(lei)


def is_valid_lei(value: object, *, checksum: bool | None = None) -> bool:
    """Strict-shape (and, by default, checksum) validation of an LEI.

    ``checksum=None`` (the default) consults the
    ``identifier_checksums_enforced`` setting; pass ``True``/``False`` to
    force either behaviour regardless of configuration.
    """
    lei = normalise_lei(value)
    if not LEI_STRICT_SHAPE.match(lei):
        return False
    if checksum is None:
        checksum = checksums_enforced()
    return lei_check_digits_ok(lei) if checksum else True


def classify_lei(value: object) -> bool:
    """True when an identifier value should be routed/keyed as an LEI.

    For data paths that *classify* identifier values (BODS mappers, reconciler
    merge keys, source adapters filtering registry data): strict shape plus —
    when enforcement is on — the check digits, so a coincidentally LEI-shaped
    registration number or a typo'd Wikidata claim is no longer mislabelled
    ``leiCode`` / promoted to a strong cross-source merge key.
    """
    return is_valid_lei(value)


def canonical_lei(value: object) -> str | None:
    """The normalised LEI when valid (per current enforcement), else None."""
    lei = normalise_lei(value)
    return lei if is_valid_lei(lei) else None


def lei_check_digit_error(value: object) -> str | None:
    """Check-digit error message for a user-supplied, *shape-valid* LEI.

    Call this after the boundary's own shape gate (each site keeps its
    historical shape wording). Returns a message only when enforcement is on
    and the ISO 17442 check digits fail, so a typo fails fast with an
    explanation instead of a silent GLEIF 404 round-trip; else ``None``.
    """
    if not checksums_enforced():
        return None
    lei = normalise_lei(value)
    if LEI_STRICT_SHAPE.match(lei) and lei_check_digits_ok(lei):
        return None
    return (
        f"{lei!r} is not a valid LEI: the ISO 17442 check digits do not "
        "match — the identifier has likely been mistyped. LEIs end in two "
        "check digits computed from the first 18 characters."
    )


def _isin_luhn_ok(isin: str) -> bool:
    """ISO 6166: expand letters to two digits (A=10..Z=35), then plain Luhn
    over the resulting digit string (check digit rightmost, never doubled)."""
    digits = "".join(str(int(ch, 36)) for ch in isin)
    total = 0
    double = False
    for ch in reversed(digits):
        d = int(ch)
        if double:
            d *= 2
            if d > 9:
                d -= 9
        total += d
        double = not double
    return total % 10 == 0


def is_valid_isin(value: object, *, checksum: bool | None = None) -> bool:
    """ISIN validation: shape always, Luhn check digit per enforcement."""
    if value is None:
        return False
    isin = str(value).strip().upper()
    if not _ISIN_SHAPE.match(isin):
        return False
    if checksum is None:
        checksum = checksums_enforced()
    if not checksum:
        return True
    if _HAS_RIGOUR:
        return _RIGOUR_ISIN.is_valid(isin)
    return _isin_luhn_ok(isin)


def is_valid_qid(value: object) -> bool:
    """Wikidata QID shape validation (``Q`` + digits, no leading zero)."""
    if value is None:
        return False
    qid = str(value).strip().upper()
    # Deliberately NOT delegated to rigour here: rigour's WikidataQID accepts
    # ``Q0`` and leading zeros, which OpenCheck uses/treats as stub sentinels
    # (see ``reconcile``). The stricter shape is identical with or without
    # the ftm extra installed.
    return bool(_QID_SHAPE.match(qid))


# --- National registration numbers -----------------------------------------
#
# Check-digit validators for the national schemes where the number OpenCheck
# handles is unambiguously the scheme python-stdnum validates. Deliberately
# NOT included: PL (GLEIF's Polish RA keys on KRS court numbers, which have
# no check digit — NIP/REGON validation belongs in the KRS adapter), HR
# (court MBS numbers, not OIB), AU-in-resolve (GLEIF records mix ABN/ACN).
def _v_ytunnus(value: str) -> bool:
    return bool(_stdnum_ytunnus.is_valid(value))


def _v_orgnr(value: str) -> bool:
    return bool(_stdnum_orgnr.is_valid(value))


def _v_cnpj(value: str) -> bool:
    return bool(_stdnum_cnpj.is_valid(value))


_NATIONAL_VALIDATORS: dict[str, tuple[str, Callable[[str], bool]]] = {
    "FI": ("Finnish Y-tunnus", _v_ytunnus),
    "SE": ("Swedish organisationsnummer", _v_orgnr),
    "BR": ("Brazilian CNPJ", _v_cnpj),
}


_ADAPTER_VALIDATORS: dict[str, Callable[[str], bool]] = {
    "fi_ytunnus": _v_ytunnus,
    "se_orgnr": _v_orgnr,
    "br_cnpj": _v_cnpj,
}


def national_checksum_ok(scheme: str, value: str) -> bool:
    """Check-digit gate for source adapters, keyed by identifier scheme.

    Returns ``False`` only when enforcement is on, python-stdnum is
    available, a validator exists for ``scheme`` and the check digit fails —
    i.e. the number cannot exist in the registry and the query is skipped.
    In every other case (enforcement off, base install, unknown scheme) the
    gate passes, preserving pre-Phase-A behaviour exactly.
    """
    if not _HAS_STDNUM or not checksums_enforced():
        return True
    validator = _ADAPTER_VALIDATORS.get(scheme)
    if validator is None or not (value or "").strip():
        return True
    try:
        return bool(validator(value.strip()))
    except Exception:  # pragma: no cover - stdnum raises only on odd input
        return True


def national_id_checksum_warning(country: str, number: str) -> str | None:
    """A human-readable warning when a national registration number fails its
    scheme's check digit, else ``None``.

    Advisory only — callers proceed with the query regardless (the registry
    is the authority; this just explains an otherwise-empty result). Returns
    ``None`` when no validator applies, python-stdnum is unavailable, or
    checksum enforcement is off.
    """
    if not _HAS_STDNUM or not checksums_enforced():
        return None
    entry = _NATIONAL_VALIDATORS.get((country or "").strip().upper())
    if entry is None:
        return None
    label, validator = entry
    num = (number or "").strip()
    if not num:
        return None
    try:
        ok = validator(num)
    except Exception:  # pragma: no cover - stdnum raises only on odd input
        return None
    if ok:
        return None
    return (
        f"{num!r} fails the {label} check digit — the number may be "
        "mistyped, so an empty result is expected."
    )


# --- UK Companies House numbers, as filed in PSC identification blocks ------
#
# Phase 177. A Companies House number is eight characters: either eight
# digits (``00070274``) or a two-letter prefix plus six digits (``SC123456``,
# ``OC403762``, ``NI012345``). The register itself always keys on that form,
# but a PSC filing's ``identification.registration_number`` is free text the
# filer typed, and leading zeros are routinely dropped: Vosper Thornycroft
# (UK) Limited's two corporate PSCs are filed as ``2999029`` and ``1915771``
# (live PSC API, 2026-09-07), and the chain above them keeps the habit
# (``2669327``). Anything that gates on "exactly 8 alphanumerics" treats
# every one of those as a foreign or unparseable registrant and silently
# stops walking the chain — which is why a subject with six UK holding
# layers above it rendered as two upward nodes.

_CH_NUMBER_SHAPE = re.compile(r"^([A-Z]{0,2})([0-9]{1,8})$")
_CH_NUMBER_LEN = 8

# Strings Companies House PSC filings use in ``country_registered`` /
# ``place_registered`` / ``legal_authority`` to mean the UK register. Free
# text, so matched as substrings of a lower-cased value. "New South Wales"
# (an Australian registrant) contains "wales" and is excluded explicitly.
_CH_UK_HINTS: tuple[str, ...] = (
    "united kingdom",
    "great britain",
    "england",
    "scotland",
    "wales",
    "northern ireland",
    "companies house",
)
_CH_UK_SHORT: frozenset[str] = frozenset({"gb", "uk", "u.k.", "u.k", "britain"})
_CH_NOT_UK: tuple[str, ...] = ("new south wales",)


def normalise_ch_company_number(value: object) -> str | None:
    """The canonical eight-character Companies House number for a filed
    registration number, or ``None`` when the value cannot be one.

    Accepts the shapes filers actually produce: digits with dropped leading
    zeros (``2999029`` → ``00070274``-style ``02999029``), a lower-case or
    spaced prefix (``sc 12345`` → ``SC012345``), and the canonical form
    unchanged. Rejects anything longer than eight characters once
    normalised, a prefix without digits, non-alphanumerics, and bare words
    (``Uk``) — those are the residue the adapter reports rather than guesses
    about.
    """
    if value is None:
        return None
    raw = str(value).strip().upper().replace(" ", "")
    if not raw:
        return None
    m = _CH_NUMBER_SHAPE.match(raw)
    if m is None:
        return None
    prefix, digits = m.group(1), m.group(2)
    width = _CH_NUMBER_LEN - len(prefix)
    if len(digits) > width:
        return None
    return f"{prefix}{digits.zfill(width)}"


_CH_PSC_PATH = "/persons-with-significant-control/"


def ch_psc_item_id(item: dict | None) -> str | None:
    """A Companies House PSC item's own id — the tail of ``links.self``
    (``/company/{n}/persons-with-significant-control/individual/{id}``, or
    ``corporate-entity`` / ``legal-person`` / ``super-secure``), or ``None``
    when the item carries no self link.

    This is the register's address for the item and is stable across edits;
    the item's ``etag`` is not — Companies House issues a new etag whenever the
    item changes, so a share band moving on the same PSC read as a new party
    until the mapper keyed on this instead. The PSC stream's events carry the
    same ``links.self`` (``psc_stream.psc_id_of``), so a stream event and the
    mapped statement name the item by one key.
    """
    if not item:
        return None
    links = item.get("links") or {}
    self_link = str(links.get("self") or "").strip() if isinstance(links, dict) else ""
    if not self_link or _CH_PSC_PATH not in self_link:
        return None
    tail = self_link.rstrip("/").rsplit("/", 1)[-1]
    return tail or None


def ch_identification_is_uk(identification: dict | None) -> bool:
    """True when a PSC ``identification`` block says the registrant is on the
    UK register — by ``country_registered`` first, then ``place_registered``
    and ``legal_authority`` when the country is blank or uninformative.

    The values are free text: ``England``, ``United Kingdom``, ``England And
    Wales``, ``Great Britain``, ``Uk``, ``United Kingdom (England)`` all
    occur, and so do ``Companies House`` in ``place_registered`` with an
    empty country. Crown Dependencies (Jersey, Guernsey, Isle of Man) and
    ``New South Wales`` are not the UK register and return ``False``.
    """
    if not identification:
        return False

    def _norm(key: str) -> str:
        return str(identification.get(key) or "").strip().lower()

    country = _norm("country_registered")
    if country:
        if country in _CH_UK_SHORT:
            return True
        if any(bad in country for bad in _CH_NOT_UK):
            return False
        return any(hint in country for hint in _CH_UK_HINTS)
    # No country filed: fall back to where it says it is registered.
    for key in ("place_registered", "legal_authority"):
        text = _norm(key)
        if not text or any(bad in text for bad in _CH_NOT_UK):
            continue
        if text in _CH_UK_SHORT or any(hint in text for hint in _CH_UK_HINTS):
            return True
    return False


# ---------------------------------------------------------------------------
# Colombia — NIT (Número de Identificación Tributaria)
# ---------------------------------------------------------------------------

#: DIAN's check-digit weights, from the rightmost digit of the NIT.
_NIT_DV_WEIGHTS = (3, 7, 13, 17, 19, 23, 29, 37, 41, 43, 47, 53, 59, 67, 71)


def nit_check_digit(base: str) -> int:
    """DIAN's check digit (dígito de verificación) for a Colombian NIT."""
    digits = str(base)
    if not digits.isdigit() or len(digits) > len(_NIT_DV_WEIGHTS):
        raise ValueError(f"not a NIT: {base!r}")
    total = sum(int(d) * _NIT_DV_WEIGHTS[i] for i, d in enumerate(reversed(digits)))
    remainder = total % 11
    return remainder if remainder < 2 else 11 - remainder


def colombian_nit(value: object) -> str | None:
    """A NIT with a valid check digit, written ``900396759-5``, or None.

    Accepts the shapes GLEIF's LEI issuers file under the Colombian
    registration authorities: dots, spaces, a hyphen or a space before the
    check digit, leading zeros (``00000900625884 - 1``), or the check digit
    appended to nine digits (``8300977829``). A value with no check digit is
    not a NIT here — nine bare digits could as well be a matrícula number.
    """
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    match = re.fullmatch(r"([\d.]+)\s*[-\s]\s*(\d)", text)
    if match:
        base, dv = match.group(1).replace(".", "").lstrip("0"), int(match.group(2))
    elif re.fullmatch(r"[\d.]{10,}", text):
        compact = text.replace(".", "").lstrip("0")
        if len(compact) != 10:
            return None
        base, dv = compact[:-1], int(compact[-1])
    else:
        return None
    if not (6 <= len(base) <= 10):
        return None
    try:
        if nit_check_digit(base) != dv:
            return None
    except ValueError:
        return None
    return f"{base}-{dv}"


# ---------------------------------------------------------------------------
# Argentina — CUIT (Clave Única de Identificación Tributaria)
# ---------------------------------------------------------------------------

#: ARCA's (formerly AFIP's) check-digit weights over the first ten digits.
_CUIT_WEIGHTS = (5, 4, 3, 2, 7, 6, 5, 4, 3, 2)


def cuit_check_digit(base: str) -> int:
    """The check digit for the first ten digits of a CUIT.

    ``11 - (Σ digit × weight mod 11)``, with 11 read as 0 and 10 as 9 — the
    old 23/33 re-prefixing leaves a 9 there (Toyota Argentina is
    ``33-67913936-9``).
    """
    if not (len(base) == 10 and base.isdigit()):
        raise ValueError(f"not a CUIT body: {base!r}")
    total = sum(int(d) * w for d, w in zip(base, _CUIT_WEIGHTS, strict=True))
    digit = 11 - total % 11
    return {11: 0, 10: 9}.get(digit, digit)


def argentine_cuit(value: object) -> str | None:
    """The 11 digits of a CUIT whose check digit agrees, or None.

    Accepts every spelling GLEIF's LEI issuers file under the Argentine
    authorities — bare (``30500001735``) or hyphenated (``33-67913936-9``),
    with stray spaces or dots. Anything else (an IGJ correlativo, a provincial
    number, free text) is not a CUIT here.
    """
    text = re.sub(r"[\s.\-]", "", str(value or ""))
    if not re.fullmatch(r"\d{11}", text):
        return None
    try:
        if cuit_check_digit(text[:10]) != int(text[10]):
            return None
    except ValueError:  # pragma: no cover - guarded by the regex
        return None
    return text


_RUC_WEIGHTS = (5, 4, 3, 2, 7, 6, 5, 4, 3, 2)


def ruc_check_ok(value: str) -> bool:
    """SUNAT's modulus-11 check digit over the first ten digits of a
    Peruvian RUC (11 → 1, 10 → 0)."""
    if not re.fullmatch(r"\d{11}", value or ""):
        return False
    total = sum(int(d) * w for d, w in zip(value[:10], _RUC_WEIGHTS, strict=True))
    check = 11 - total % 11
    check = {10: 0, 11: 1}.get(check, check)
    return check == int(value[10])


def peruvian_ruc(value: object) -> str | None:
    """An 11-digit Peruvian RUC with a valid check digit, spaces, dots and
    hyphens removed; else None (the Peru RNP wiring)."""
    text = re.sub(r"[\s.\-]+", "", str(value or ""))
    return text if ruc_check_ok(text) else None


def format_cuit(digits: str) -> str:
    """``30500001735`` → ``30-50000173-5``, the spelling the CUIT is printed
    in and the one OpenCheck writes under ``AR-CUIT``."""
    return f"{digits[:2]}-{digits[2:10]}-{digits[10]}"
