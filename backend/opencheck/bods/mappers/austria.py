"""Austria — Firmenbuch (Commercial Register) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

from typing import Any, Iterable

from .. import liveness as _liveness
from ..statements import (
    SOURCE_NAMES,
    _stable_id,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
)


# ----------------------------------------------------------------------
# Firmenbuch — Austrian Commercial Register
# ----------------------------------------------------------------------


def _at_date_iso(raw: str) -> str | None:
    """Convert Austrian DD.MM.YYYY date to ISO 8601 (YYYY-MM-DD), or None."""
    raw = (raw or "").strip()
    if not raw:
        return None
    parts = raw.split(".")
    if len(parts) == 3:
        day, month, year = parts
        try:
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
        except ValueError:
            pass
    return raw  # already ISO or unrecognised — pass through


def map_firmenbuch(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a FirmenbuchAdapter fetch bundle to BODS v0.4 statements.

    Yields:
    * One entityStatement for the Austrian company.
    * One personStatement per unique officer or individual shareholder.
    * One entityStatement per unique corporate shareholder.
    * One relationshipStatement per officer role.
    * One relationshipStatement per shareholder / partner record.
    """
    if not bundle or bundle.get("is_stub"):
        return

    fn: str = bundle.get("fn") or ""
    extract: dict[str, Any] = bundle.get("extract") or {}
    if not fn or not extract:
        return

    name: str = extract.get("name") or bundle.get("legal_name") or fn
    uid: str = extract.get("uid") or ""
    founding_date_iso = _at_date_iso(extract.get("founding_date") or "")
    address_str: str = extract.get("address") or ""
    stamm_kapital: float | None = extract.get("stamm_kapital")
    officers: list[dict[str, Any]] = extract.get("officers") or []
    shareholders: list[dict[str, Any]] = extract.get("shareholders") or []

    source_url = f"https://justizonline.gv.at/jop/web/firmenbuchabfrage?firmennummer={fn}"

    # ── Identifiers ────────────────────────────────────────────────────────
    identifiers: list[dict[str, str]] = [
        {"id": fn, "scheme": "AT-FB", "schemeName": "Firmenbuch (Austrian Commercial Register)"},
    ]
    if uid:
        identifiers.append(
            {"id": uid, "scheme": "AT-UID", "schemeName": "Umsatzsteuer-Identifikationsnummer (Austrian VAT ID)"}
        )

    # ── 1. Subject entity statement ────────────────────────────────────────
    addresses = [{"address": address_str, "country": "AT", "type": "registered"}] if address_str else []

    company_stmt = make_entity_statement(
        source_id="firmenbuch",
        local_id=fn,
        name=name,
        jurisdiction=("Austria", "AT"),
        identifiers=identifiers,
        founding_date=founding_date_iso,
        addresses=addresses,
        source_url=source_url,
    )
    # Firmenbuch AUFRECHT attribute → "aktiv" / "gelöscht" (Phase 151).
    _liveness.apply_register_status(
        company_stmt,
        source_label=SOURCE_NAMES["firmenbuch"],
        liveness=_liveness.classify(
            extract.get("status"),
            live=("aktiv", "aufrecht"),
            terminal=("gelöscht", "geloescht", "gelöscht (amtswegig)"),
        ),
        raw=extract.get("status") or None,
    )
    yield company_stmt
    company_stmt_id: str = company_stmt["statementId"]

    # Track already-emitted local IDs to avoid duplicate statements.
    seen_ids: set[str] = set()

    # ── 2. Officer statements ──────────────────────────────────────────────
    from opencheck.sources.firmenbuch import _role_to_interest  # local import avoids circular

    for idx, officer in enumerate(officers):
        full_name: str = officer.get("full_name") or ""
        if not full_name:
            continue

        role_code: str = officer.get("role_code") or ""
        role_name: str = officer.get("role_name") or role_code
        interest_type, display_label = _role_to_interest(role_code, role_name)

        dob = _at_date_iso(officer.get("dob") or "")
        person_local_id = f"firmenbuch:officer:{full_name.lower().replace(' ', '_')}:{officer.get('dob', '')}"

        if person_local_id not in seen_ids:
            person_stmt = make_person_statement(
                source_id="firmenbuch",
                local_id=person_local_id,
                full_name=full_name,
                birth_date=dob,
                source_url=source_url,
            )
            yield person_stmt
            seen_ids.add(person_local_id)
        else:
            person_stmt = {"statementId": _stable_id("firmenbuch", "person", person_local_id)}

        yield make_relationship_statement(
            source_id="firmenbuch",
            local_id=f"{fn}:officer:{idx}:{person_local_id}",
            subject_statement_id=company_stmt_id,
            interested_party_statement_id=person_stmt["statementId"],
            interested_party_type="person",
            interests=[{
                "type": interest_type,
                "directOrIndirect": "direct",
                "beneficialOwnershipOrControl": False,
                "details": display_label,
            }],
            source_url=source_url,
        )

    # ── 3. Shareholder statements ──────────────────────────────────────────
    for idx, sh in enumerate(shareholders):
        display_name: str = sh.get("display_name") or ""
        if not display_name:
            continue

        is_person: bool = sh.get("is_person", True)
        einlage: float | None = sh.get("einlage")
        sh_fn: str = sh.get("fn") or ""
        kind: str = sh.get("kind") or "gesellschafter"

        if kind == "komplementaer":
            interest_type = "otherInfluenceOrControl"
            detail_label = "Komplementär (General Partner)"
        elif kind == "kommanditist":
            interest_type = "shareholding"
            detail_label = "Kommanditist (Limited Partner)"
        else:
            interest_type = "shareholding"
            detail_label = "Gesellschafter (Shareholder)"

        interest: dict[str, Any] = {
            "type": interest_type,
            "directOrIndirect": "direct",
            "beneficialOwnershipOrControl": False,
            "details": detail_label,
        }

        if interest_type == "shareholding" and einlage is not None and stamm_kapital:
            try:
                pct = round(einlage / stamm_kapital * 100, 4)
                interest["share"] = {"exact": pct}
            except ZeroDivisionError:
                pass

        if is_person:
            dob_sh = _at_date_iso(sh.get("dob") or "")
            sh_local_id = f"firmenbuch:sh:{display_name.lower().replace(' ', '_')}:{sh.get('dob', '')}"
            if sh_local_id not in seen_ids:
                sh_stmt = make_person_statement(
                    source_id="firmenbuch",
                    local_id=sh_local_id,
                    full_name=display_name,
                    birth_date=dob_sh,
                    source_url=source_url,
                )
                yield sh_stmt
                seen_ids.add(sh_local_id)
            else:
                sh_stmt = {"statementId": _stable_id("firmenbuch", "person", sh_local_id)}
            interested_party_type = "person"
        else:
            sh_local_id = f"firmenbuch:sh_entity:{sh_fn or display_name.lower().replace(' ', '_')}"
            sh_identifiers: list[dict[str, str]] = []
            if sh_fn:
                sh_identifiers.append(
                    {"id": sh_fn, "scheme": "AT-FB", "schemeName": "Firmenbuch (Austrian Commercial Register)"}
                )
            if sh_local_id not in seen_ids:
                sh_stmt = make_entity_statement(
                    source_id="firmenbuch",
                    local_id=sh_local_id,
                    name=display_name,
                    jurisdiction=None,
                    identifiers=sh_identifiers,
                    source_url=source_url,
                )
                yield sh_stmt
                seen_ids.add(sh_local_id)
            else:
                sh_stmt = {"statementId": _stable_id("firmenbuch", "entity", sh_local_id)}
            interested_party_type = "entity"

        yield make_relationship_statement(
            source_id="firmenbuch",
            local_id=f"{fn}:sh:{idx}:{sh_local_id}",
            subject_statement_id=company_stmt_id,
            interested_party_statement_id=sh_stmt["statementId"],
            interested_party_type=interested_party_type,
            interests=[interest],
            source_url=source_url,
        )
