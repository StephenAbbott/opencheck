"""Polish National Court Register (Krajowy Rejestr Sądowy / KRS) adapter.

KRS is Poland's statutory register of incorporated entities — private
companies (spółki), associations (stowarzyszenia), foundations (fundacje),
European companies (SE), and other legal forms.  It is maintained by the
Ministry of Justice and operated by the court system (sądy rejestrowe).

This adapter uses the KRS Open API (REST/JSON, no authentication required):

  Base URL: https://api-krs.ms.gov.pl/api/krs/
  Fetch:    GET /OdpisAktualny/{krs}?rejestr={P|S|C}&format=json

    rejestr=P — entrepreneurs register (spółki; most commercial entities)
    rejestr=S — associations register (stowarzyszenia, fundacje)
    rejestr=C — other entities

  The adapter tries P first; on 404 it falls back to S then C.

No public name-based search endpoint is exposed by the KRS REST API; the
adapter is therefore identifier-keyed (``search()`` returns []).  It is
activated by the GLEIF bridge when ``registeredAt.id == "RA000484"``.

Privacy note
------------
The KRS API **masks personal data** in public extracts.  Individual names
appear as "Ł*******" and PESEL numbers as "7**********".  This is a
deliberate design choice by the Polish Ministry of Justice under GDPR /
Polish personal data protection law.  As a result this adapter emits a BODS
entity statement only — no person or ownership-or-control statements.  The
CRBR (Central Register of Beneficial Owners) adapter (Phase 32) provides the
unmasked beneficial ownership data for Polish entities.

Data available
--------------
  • Entity basics: name, NIP (tax ID), REGON (statistical number), legal form,
    registered address, registration date.
  • Share capital: total capital, currency, number of shares (S.A. only).
  • Primary business activity: PKD code + description.
  • Directors / supervisory board: masked names + roles (stored in bundle,
    not converted to BODS person statements for the reason above).
  • Shareholders (sp. z o.o. only): masked names + share counts (stored in
    bundle for informational display).

GLEIF integration
-----------------
  GLEIF Registration Authority code: RA000484
    (National Court Register / Krajowy Rejestr Sądowy, Ministry of Justice)
  The ``registeredAs`` field in GLEIF Level 1 records for Polish entities
  contains the 10-digit zero-padded KRS number (e.g. "0000028860").
  ``app.py`` extracts ``pl_krs`` from this and calls ``fetch()`` here.

Authentication: none — fully public API.
KRS portal:     https://ekrs.ms.gov.pl/
OpenAPI docs:   https://prs.ms.gov.pl/krs/openApi
License:        Polish open government data (Ministerstwo Sprawiedliwości).
Attribution:    "Contains data from the National Court Register (KRS),
                 Polish Ministry of Justice (Ministerstwo Sprawiedliwości).
                 Source: api-krs.ms.gov.pl."
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

_log = logging.getLogger(__name__)

# GLEIF Registration Authority code for Poland's KRS (National Court Register).
PL_KRS_RA_CODE: str = "RA000484"

_API_BASE = "https://api-krs.ms.gov.pl/api/krs"
_FETCH_URL = f"{_API_BASE}/OdpisAktualny"

# Register types to try in order when fetching a KRS number.
_REJESTRY = ("P", "S", "C")

_CACHE_NS = "krs_poland"

# KRS number: exactly 10 digits, zero-padded.
_KRS_LEN = 10

# Polish legal form full names (uppercase as returned by API) → short English label.
_LEGAL_FORM_MAP: dict[str, str] = {
    "SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ": "sp. z o.o. (limited liability company)",
    "PROSTA SPÓŁKA AKCYJNA": "P.S.A. (simple joint-stock company)",
    "SPÓŁKA AKCYJNA": "S.A. (joint-stock company)",
    "SPÓŁKA KOMANDYTOWO-AKCYJNA": "S.K.A. (limited joint-stock partnership)",
    "SPÓŁKA KOMANDYTOWA": "sp. k. (limited partnership)",
    "SPÓŁKA JAWNA": "sp. j. (general partnership)",
    "SPÓŁKA PARTNERSKA": "sp. p. (professional partnership)",
    "FUNDACJA": "fundacja (foundation)",
    "STOWARZYSZENIE": "stowarzyszenie (association)",
    "SPÓŁDZIELNIA": "spółdzielnia (cooperative)",
    "SPÓŁKA EUROPEJSKA": "SE (European company)",
    "EUROPEJSKIE ZGRUPOWANIE INTERESÓW GOSPODARCZYCH": "EZIG (EEIG)",
    "PRZEDSIĘBIORSTWO PAŃSTWOWE": "PP (state enterprise)",
    "JEDNOSTKA BADAWCZO-ROZWOJOWA": "JBR (research & development unit)",
    "FUNDUSZ INWESTYCYJNY": "fundusz inwestycyjny (investment fund)",
    "TOWARZYSTWO UBEZPIECZEŃ WZAJEMNYCH": "TUW (mutual insurance company)",
}


# ---------------------------------------------------------------------------
# Identifier helpers
# ---------------------------------------------------------------------------


def normalise_krs(krs: str | int) -> str:
    """Return KRS number normalised to a 10-digit zero-padded string."""
    return str(krs).strip().zfill(_KRS_LEN)


def _normalise_nip(nip: str) -> str:
    """Strip whitespace and hyphens from a NIP tax identifier."""
    return str(nip).strip().replace("-", "")


def _normalise_regon(regon: str) -> str:
    """Strip whitespace from a REGON statistical number (9 or 14 digits)."""
    # GLEIF / KRS sometimes stores 14-char REGON (includes sub-unit suffix);
    # the canonical entity-level REGON is the first 9 digits.
    raw = str(regon).strip()
    return raw[:9] if len(raw) >= 9 else raw


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _build_address(adres: dict[str, Any]) -> str | None:
    """Compose a human-readable address string from a KRS ``adres`` dict.

    KRS address fields:
      ulica       — street name (may include "UL." prefix)
      nrDomu      — building number
      nrLokalu    — apartment / unit number (optional)
      kodPocztowy — postal code (format "NN-NNN")
      miejscowosc — city / town
      kraj        — country ("POLSKA" for domestic; foreign country name for
                    registered foreign offices)
    """
    parts: list[str] = []
    street = adres.get("ulica", "").strip()
    number = adres.get("nrDomu", "").strip()
    apt = adres.get("nrLokalu", "").strip()
    city = adres.get("miejscowosc", "").strip()
    postal = adres.get("kodPocztowy", "").strip()
    country = adres.get("kraj", "").strip()

    if street and number:
        line1 = f"{street} {number}"
        if apt:
            line1 += f"/{apt}"
        parts.append(line1)
    elif street:
        parts.append(street)

    if postal and city:
        parts.append(f"{postal} {city}")
    elif city:
        parts.append(city)

    if country and country not in ("POLSKA", "POLAND"):
        parts.append(country.title())

    return ", ".join(parts) or None


def _legal_form_label(forma_prawna: str) -> str:
    """Return a short English label for a Polish legal form string."""
    return _LEGAL_FORM_MAP.get(forma_prawna.upper(), forma_prawna)


def _extract_board_member(member: dict[str, Any]) -> dict[str, Any] | None:
    """Extract a board member record from a KRS ``sklad`` entry.

    Note: The KRS API masks names in public extracts (e.g. "Ł*******").
    This function preserves the masked names for informational display;
    they are NOT converted to BODS person statements.
    """
    nazwisko = (member.get("nazwisko") or {})
    czlon1 = nazwisko.get("nazwiskoICzlon", "")
    czlon2 = nazwisko.get("nazwiskoIICzlon", "")
    surname = " ".join(p for p in (czlon1, czlon2) if p).strip()

    imiona = (member.get("imiona") or {})
    first = imiona.get("imie", "").strip()
    second = imiona.get("imieDrugie", "").strip()
    given = " ".join(p for p in (first, second) if p).strip()

    full_name = " ".join(p for p in (given, surname) if p).strip()

    role = member.get("funkcjaWOrganie", "").strip()
    suspended = member.get("czyZawieszona", False)

    if not full_name and not role:
        return None

    return {
        "name": full_name or "(masked)",
        "given_name": given or None,
        "family_name": surname or None,
        "role": role or "Board member",
        "suspended": suspended,
        "name_masked": "*" in full_name,
    }


def _extract_shareholder(wspolnik: dict[str, Any]) -> dict[str, Any] | None:
    """Extract a sp. z o.o. shareholder record from a ``wspolnicySpzoo`` entry.

    As with board members, names are masked in the public API.
    """
    nazwisko = (wspolnik.get("nazwisko") or {})
    czlon1 = nazwisko.get("nazwiskoICzlon", "")
    czlon2 = nazwisko.get("nazwiskoIICzlon", "")
    surname = " ".join(p for p in (czlon1, czlon2) if p).strip()

    imiona = (wspolnik.get("imiona") or {})
    first = imiona.get("imie", "").strip()
    second = imiona.get("imieDrugie", "").strip()
    given = " ".join(p for p in (first, second) if p).strip()

    full_name = " ".join(p for p in (given, surname) if p).strip()

    shares_desc = wspolnik.get("posiadaneUdzialy", "").strip()
    all_shares = wspolnik.get("czyPosiadaCaloscUdzialow", False)

    return {
        "name": full_name or "(masked)",
        "given_name": given or None,
        "family_name": surname or None,
        "shares_description": shares_desc or None,
        "holds_all_shares": all_shares,
        "name_masked": "*" in full_name,
    }


def _extract_pkd(dzial3: dict[str, Any]) -> dict[str, str] | None:
    """Extract the primary PKD (business activity) code and description."""
    pd = dzial3.get("przedmiotDzialalnosci") or {}
    primary = pd.get("przedmiotPrzewazajacejDzialalnosci") or []
    if not primary:
        return None
    item = primary[0]
    dzial = item.get("kodDzial", "")
    klasa = item.get("kodKlasa", "")
    podklasa = item.get("kodPodklasa", "")
    code = f"{dzial}.{klasa}{podklasa}".strip(".")
    desc = item.get("opis", "")
    return {"code": code, "description": desc} if (code or desc) else None


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class KrsPolandAdapter(SourceAdapter):
    """Source adapter for the Polish National Court Register (KRS)."""

    id = "krs_poland"

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="KRS — Polish National Court Register",
            homepage="https://ekrs.ms.gov.pl/",
            description=(
                "Entity data from Poland's National Court Register (Krajowy "
                "Rejestr Sądowy / KRS), maintained by the Ministry of Justice. "
                "Provides company name, NIP, REGON, legal form, registered "
                "address, share capital, board composition, and primary "
                "business activity (PKD code). Note: personal data (names, "
                "PESEL) is masked in the public API extract."
            ),
            license="PL-OGD",
            attribution=(
                "Contains data from the National Court Register (KRS), "
                "Polish Ministry of Justice (Ministerstwo Sprawiedliwości). "
                "Source: api-krs.ms.gov.pl."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
        )

    # ------------------------------------------------------------------
    # Search — not supported (identifier-keyed via GLEIF RA000484)
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """KRS is identifier-keyed; free-text search is not available."""
        return []

    # ------------------------------------------------------------------
    # Fetch — by KRS number
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the full KRS record bundle for a given KRS number.

        ``hit_id`` is a KRS number (1–10 digits; zero-padded to 10 internally).
        ``legal_name`` is the GLEIF legal name, used only when we return a stub.

        The adapter tries register type P (entrepreneurs) first; if the KRS
        number is not found in P, it falls back to S (associations) then C
        (other entities).  404 on all three types → stub.
        """
        krs = normalise_krs(hit_id)
        bundle_key = f"{_CACHE_NS}/bundle/{krs}"
        settings = get_settings()

        # --- 1. Bundle cache ---
        cached = self._cache.get_payload(bundle_key)
        if cached is not None:
            return cached[0]

        if not settings.allow_live:
            return self._stub(krs, legal_name)

        # --- 2. Live fetch (try P → S → C) ---
        raw: dict[str, Any] | None = None
        used_rejestr: str = "P"

        async with build_client() as client:
            for rejestr in _REJESTRY:
                url = f"{_FETCH_URL}/{krs}?rejestr={rejestr}&format=json"
                try:
                    resp = await client.get(url, timeout=20)
                    if resp.status_code == 404:
                        continue
                    resp.raise_for_status()
                    raw = resp.json()
                    used_rejestr = rejestr
                    break
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 404:
                        continue
                    _log.warning(
                        "krs_poland: HTTP %s for KRS %s (rejestr=%s): %s",
                        exc.response.status_code, krs, rejestr, exc,
                    )
                    break
                except httpx.HTTPError as exc:
                    _log.warning(
                        "krs_poland: network error for KRS %s (rejestr=%s): %s",
                        krs, rejestr, exc,
                    )
                    break

        if raw is None:
            return self._stub(krs, legal_name)

        bundle = self._build_bundle(krs, raw, used_rejestr)
        self._cache.put(bundle_key, bundle)
        return bundle

    # ------------------------------------------------------------------
    # Bundle construction
    # ------------------------------------------------------------------

    def _stub(self, krs: str, legal_name: str) -> dict[str, Any]:
        return {
            "source_id": self.id,
            "hit_id": krs,
            "pl_krs": krs,
            "name": legal_name or "",
            "is_stub": True,
        }

    def _build_bundle(
        self,
        krs: str,
        raw: dict[str, Any],
        rejestr: str,
    ) -> dict[str, Any]:
        odpis = raw.get("odpis") or {}
        header = odpis.get("naglowekA") or {}
        dane = odpis.get("dane") or {}

        dzial1 = dane.get("dzial1") or {}
        dzial2 = dane.get("dzial2") or {}
        dzial3 = dane.get("dzial3") or {}

        # --- Entity basics ---
        podmiot = dzial1.get("danePodmiotu") or {}
        name: str = (podmiot.get("nazwa") or "").strip()
        forma_prawna: str = (podmiot.get("formaPrawna") or "").strip()
        legal_form_label: str = _legal_form_label(forma_prawna) if forma_prawna else ""

        ids = podmiot.get("identyfikatory") or {}
        raw_nip: str = (ids.get("nip") or "").strip()
        raw_regon: str = (ids.get("regon") or "").strip()
        nip: str = _normalise_nip(raw_nip) if raw_nip else ""
        regon: str = _normalise_regon(raw_regon) if raw_regon else ""

        # --- Address ---
        siedziba_adres = dzial1.get("siedzibaIAdres") or {}
        adres_dict = siedziba_adres.get("adres") or {}
        address: str | None = _build_address(adres_dict) if adres_dict else None
        email: str | None = (siedziba_adres.get("adresPocztyElektronicznej") or "").strip() or None
        website: str | None = (siedziba_adres.get("adresStronyInternetowej") or "").strip() or None

        # --- Registration dates ---
        registration_date: str | None = _parse_date(header.get("dataRejestracjiWKRS"))
        last_change_date: str | None = _parse_date(header.get("dataOstatniegoWpisu"))

        # --- Share capital ---
        kapital = dzial1.get("kapital") or {}
        capital_info: dict[str, Any] | None = None
        kzak = kapital.get("wysokoscKapitaluZakladowego") or {}
        if kzak:
            capital_info = {
                "amount": (kzak.get("wartosc") or "").replace(",", "."),
                "currency": kzak.get("waluta") or "PLN",
                "total_shares": (kapital.get("lacznaLiczbaAkcjiUdzialow") or None),
                "share_nominal": (
                    (kapital.get("wartoscJednejAkcji") or {}).get("wartosc") or None
                ),
            }

        # --- Primary business activity (PKD) ---
        pkd = _extract_pkd(dzial3)

        # --- Board members (names masked in public API) ---
        # ``reprezentacja`` is normally a single dict, but apply the same
        # list-normalisation as ``organNadzoru`` for safety.
        directors: list[dict[str, Any]] = []
        raw_repr = dzial2.get("reprezentacja") or {}
        repr_list: list[dict[str, Any]] = (
            raw_repr if isinstance(raw_repr, list) else [raw_repr] if raw_repr else []
        )
        for repr_section in repr_list:
            organ_name = (
                repr_section.get("nazwaOrganu")
                or repr_section.get("nazwa")
                or "Zarząd"
            )
            for member in repr_section.get("sklad") or []:
                rec = _extract_board_member(member)
                if rec:
                    rec["organ"] = organ_name
                    directors.append(rec)

        # Supervisory board (organ nadzoru).
        # For cooperatives (spółdzielnia) and some other forms, ``organNadzoru``
        # is a *list* of organ dicts rather than a single dict.  We normalise
        # both shapes so the rest of the pipeline stays uniform.
        supervisory: list[dict[str, Any]] = []
        raw_organ_nadzoru = dzial2.get("organNadzoru") or {}
        organ_nadzoru_list: list[dict[str, Any]] = (
            raw_organ_nadzoru
            if isinstance(raw_organ_nadzoru, list)
            else [raw_organ_nadzoru] if raw_organ_nadzoru else []
        )
        for organ_nadzoru in organ_nadzoru_list:
            sn_name = (
                organ_nadzoru.get("nazwaOrganu")
                or organ_nadzoru.get("nazwa")
                or "Rada Nadzorcza"
            )
            for member in organ_nadzoru.get("sklad") or []:
                rec = _extract_board_member(member)
                if rec:
                    rec["organ"] = sn_name
                    supervisory.append(rec)

        # --- Shareholders (sp. z o.o. only; names masked) ---
        shareholders: list[dict[str, Any]] = []
        for wspolnik in dzial1.get("wspolnicySpzoo") or []:
            rec = _extract_shareholder(wspolnik)
            if rec:
                shareholders.append(rec)

        return {
            "source_id": self.id,
            "hit_id": krs,
            "pl_krs": krs,
            "is_stub": False,
            "name": name,
            "nip": nip or None,
            "regon": regon or None,
            "legal_form": forma_prawna or None,
            "legal_form_label": legal_form_label or None,
            "address": address,
            "email": email,
            "website": website,
            "registration_date": registration_date,
            "last_change_date": last_change_date,
            "rejestr": rejestr,
            "capital": capital_info,
            "pkd": pkd,
            "directors": directors,
            "supervisory_board": supervisory,
            "shareholders": shareholders,
            "link": f"https://ekrs.ms.gov.pl/web/wyszukiwarka-krs/strona-glowna,search.html?krs={krs}",
        }


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------


def _parse_date(raw: str | None) -> str | None:
    """Convert a KRS date string (DD.MM.YYYY) to ISO format (YYYY-MM-DD).

    Returns ``None`` if the input is absent or cannot be parsed.
    """
    if not raw:
        return None
    # Format "DD.MM.YYYY" or "DD.MM.YYYY R." (the "R." suffix means "year" in Polish)
    s = raw.strip().rstrip(" R.").strip()
    parts = s.split(".")
    if len(parts) == 3:
        day, month, year = parts
        try:
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
        except ValueError:
            pass
    return None
