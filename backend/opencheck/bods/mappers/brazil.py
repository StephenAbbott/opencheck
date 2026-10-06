"""Brazil — CNPJ (Receita Federal) → BODS.

Split out of ``mapper.py`` in Phase 246, unchanged; ``mapper.py``
re-exports every name defined here.
"""

from __future__ import annotations

import gettext
import re
import unicodedata
from functools import lru_cache
from typing import Any, Iterable

import pycountry

from .. import liveness as _liveness
from ..statements import (
    SOURCE_NAMES,
    _addr,
    _stable_id,
    make_entity_statement,
    make_person_statement,
    make_relationship_statement,
)


# Brazil CNPJ — QSA qualification label → BODS interest type.
# Owner-type qualifications (sócio / acionista / quotista / titular) map to
# ``shareholding``; management & representation roles map to
# ``seniorManagingOfficial``.
_BR_OWNER_KEYWORDS = ("socio", "sócio", "acionista", "quotista", "cotista", "titular")


def _br_fold(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", text.lower()) if not unicodedata.combining(c)
    ).strip()


@lru_cache(maxsize=1)
def _br_country_index() -> dict[str, str]:
    """Folded Portuguese (pt_BR) country name → ISO alpha-2, from pycountry's
    own translations. Receita Federal writes a partner's ``pais`` in
    Portuguese — "Países Baixos (Holanda)", "ESPANHA" — and nothing else."""
    try:
        tr = gettext.translation("iso3166-1", pycountry.LOCALES_DIR, languages=["pt_BR"])
    except OSError:  # pragma: no cover - translations ship with pycountry
        return {}
    index: dict[str, str] = {}
    for c in pycountry.countries:
        for attr in ("name", "common_name", "official_name"):
            value = getattr(c, attr, None)
            if value:
                index.setdefault(_br_fold(tr.gettext(value)), c.alpha_2)
    return index


def _br_country_code(text: str | None) -> str:
    """ISO alpha-2 for the country Receita files on a QSA partner, or ``""``.
    The name as filed first, then without its parenthetical gloss
    ("Países Baixos (Holanda)" → "Países Baixos")."""
    if not text or not str(text).strip():
        return ""
    folded = _br_fold(str(text))
    index = _br_country_index()
    if folded in index:
        return index[folded]
    bare = re.sub(r"\s*\(.*?\)\s*", " ", folded).strip()
    return index.get(bare, "")


def _br_partner_is_entity(p: dict[str, Any]) -> bool:
    """A QSA partner is a legal person when Receita types it PJ, or when it is
    a foreign partner whose qualification says so (code 37, "Sócio Pessoa
    Jurídica Domiciliado no Exterior")."""
    if p.get("kind") == "entity":
        return True
    role = _br_fold(p.get("role") or "")
    return p.get("kind") == "foreign" and "pessoa juridica" in role


def _br_interest_type(qualificacao: str | None) -> str:
    q = (qualificacao or "").strip().lower()
    if any(k in q for k in _BR_OWNER_KEYWORDS):
        return "shareholding"
    return "seniorManagingOfficial"


def map_cnpj_brazil(bundle: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Map a CnpjBrazilAdapter bundle to BODS v0.4 statements.

    Yields:
    * One entityStatement for the Brazilian company.
    * Per QSA partner: a personStatement (natural person / foreign) or an
      entityStatement (legal-entity partner, carrying its own CNPJ) plus an
      ownership-or-control relationshipStatement. Owner-type qualifications →
      ``shareholding``; administrators/directors → ``seniorManagingOfficial``.
    """
    if not bundle or bundle.get("is_stub"):
        return

    cnpj: str = str(bundle.get("br_cnpj") or "")
    company: dict[str, Any] = bundle.get("company") or {}
    partners: list[dict[str, Any]] = bundle.get("partners") or []

    name: str = (
        (company.get("name") or "").strip()
        or bundle.get("legal_name")
        or (f"CNPJ {cnpj}" if cnpj else "")
    )
    if not cnpj or not name:
        return

    source_url = bundle.get("link") or f"https://opencnpj.org/{cnpj}"

    def _cnpj_id(value: str) -> dict[str, str]:
        return {
            "id": value,
            "scheme": "BR-CNPJ",
            "schemeName": "Cadastro Nacional da Pessoa Jurídica — CNPJ (Receita Federal do Brasil)",
        }

    # ── 1. Company entity statement ───────────────────────────────────────
    company_stmt = make_entity_statement(
        source_id="cnpj_brazil",
        local_id=cnpj,
        name=name,
        jurisdiction=("Brazil", "BR"),
        identifiers=[_cnpj_id(cnpj)],
        founding_date=company.get("founding_date"),
        addresses=(
            [_addr("registered", company["address"], "BR")]
            if company.get("address")
            else []
        ),
        alternate_names=[company["trade_name"]] if company.get("trade_name") else [],
        entity_details=company.get("legal_nature"),
        source_url=source_url,
    )
    # Receita Federal ``situacao_cadastral`` (Phase 151): ATIVA is live;
    # BAIXADA (closed) and NULA (annulled) are terminal. SUSPENSA and INAPTA
    # are irregular-but-existing registrations and are left unclassified
    # rather than guessed. ``data_situacao_cadastral`` is the effective date.
    br_status = (company.get("status") or "").strip()
    br_liveness = _liveness.classify(
        br_status,
        live=("ATIVA", "02", "2"),
        terminal=("BAIXADA", "08", "8", "NULA", "01", "1"),
    )
    _liveness.apply_register_status(
        company_stmt,
        source_label=SOURCE_NAMES["cnpj_brazil"],
        liveness=br_liveness,
        raw=br_status or None,
        since=(company.get("status_date") if br_liveness == _liveness.TERMINAL else None),
    )
    yield company_stmt
    company_stmt_id: str = company_stmt["statementId"]

    # ── 2. QSA partners / administrators ──────────────────────────────────
    seen: set[str] = set()
    for idx, p in enumerate(partners):
        pname = (p.get("name") or "").strip()
        if not pname:
            continue

        interest = {
            "type": _br_interest_type(p.get("role")),
            "directOrIndirect": "direct",
            "beneficialOwnershipOrControl": False,
        }
        if p.get("role"):
            interest["details"] = p["role"]
        if p.get("entry_date"):
            interest["startDate"] = p["entry_date"]

        if _br_partner_is_entity(p):
            partner_cnpj = p.get("cnpj")
            local_id = partner_cnpj or f"{cnpj}:pj:{idx}"
            ip_type = "entity"
            # Jurisdiction from the register's own ``pais`` for a partner
            # domiciled abroad — SHELL BRAZIL HOLDING BV is filed with
            # "Países Baixos (Holanda)" and was being published as Brazilian
            # (follow-up to Phase 293). A partner with no country filed is a
            # Brazilian PJ, as before; one whose country does not resolve
            # gets none rather than a guess.
            country_text = (p.get("country") or "").strip()
            country_code = _br_country_code(country_text)
            if not country_text or country_code == "BR":
                jurisdiction: tuple[str, str] | None = ("Brazil", "BR")
            elif country_code:
                country = pycountry.countries.get(alpha_2=country_code)
                jurisdiction = (getattr(country, "name", country_code), country_code)
            else:
                jurisdiction = None
            if local_id not in seen:
                yield make_entity_statement(
                    source_id="cnpj_brazil",
                    local_id=local_id,
                    name=pname,
                    jurisdiction=jurisdiction,
                    identifiers=[_cnpj_id(partner_cnpj)] if partner_cnpj else [],
                    source_url=source_url,
                )
                seen.add(local_id)
            ip_id = _stable_id("cnpj_brazil", "entity", local_id)
        else:
            # Natural person (PF) or foreign individual — scope the local id to
            # the company so identical names across companies don't false-merge.
            local_id = f"{cnpj}:pf:{pname}"
            ip_type = "person"
            if local_id not in seen:
                yield make_person_statement(
                    source_id="cnpj_brazil",
                    local_id=local_id,
                    full_name=pname,
                    source_url=source_url,
                )
                seen.add(local_id)
            ip_id = _stable_id("cnpj_brazil", "person", local_id)

        yield make_relationship_statement(
            source_id="cnpj_brazil",
            local_id=f"{cnpj}:rel:{idx}",
            subject_statement_id=company_stmt_id,
            interested_party_statement_id=ip_id,
            interested_party_type=ip_type,
            interests=[interest],
            source_url=source_url,
        )
