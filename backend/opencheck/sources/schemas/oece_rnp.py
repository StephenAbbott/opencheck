"""Pydantic schema for the Peruvian RNP supplier bundle.

Only the fields the BODS mapper (``map_oece_rnp``) reads are declared;
everything else passes through via ``extra="allow"``. The bundle is
assembled by ``OeceRnpAdapter.fetch`` from the SQLite index built by
``scripts/build_oece_rnp_index.py``.

**No person's document number, by construction** (Ley 29733): the RNP file
carries a DNI, carnet de extranjería or passport number on almost every row.
The index keeps a company member's RUC only, and this schema refuses a
bundle in which a document column reappears, at any depth.
"""

from __future__ import annotations

from typing import Any

from pydantic import model_validator

from . import _Base

#: The file's document columns, in either spelling, that must never reach a
#: bundle.
FORBIDDEN_KEYS: frozenset[str] = frozenset(
    {
        "NUMERO_DOCUMENTO", "TIPO_DOCUMENTO", "numero_documento", "tipo_documento",
        "document", "document_number", "document_type", "dni",
    }
)


def _forbidden_paths(value: Any, path: str = "") -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            here = f"{path}.{key}" if path else str(key)
            if key in FORBIDDEN_KEYS:
                found.append(here)
            found.extend(_forbidden_paths(item, here))
    elif isinstance(value, list):
        for i, item in enumerate(value):
            found.extend(_forbidden_paths(item, f"{path}[{i}]"))
    return found


class OeceRnpParty(_Base):
    """One declared entry: a member, a legal representative or a
    governing-body member of the supplier."""

    seq: int
    #: ``S`` partner/shareholder/owner, ``R`` legal representative,
    #: ``O`` governing-body member.
    role: str
    #: ``E`` company or public body, ``A`` an estate or marital partnership,
    #: ``P`` person, ``U`` a class of people.
    kind: str
    name: str
    #: A company member's RUC; never a person's.
    ruc: str | None = None
    #: ``FECHA_INICIO_VIGENCIA`` — when the entry took effect in the RNP.
    since: str | None = None


class OeceRnpBundle(_Base):
    """Top-level shape returned by OeceRnpAdapter.fetch."""

    ruc: str
    name: str | None = None
    legal_name: str | None = None
    form: str | None = None
    form_id: str | None = None
    parties: list[OeceRnpParty] = []
    counts: dict[str, int] = {}
    cut: str | None = None
    link: str | None = None
    profile_url: str | None = None
    not_found: bool = False
    coverage_note: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _no_document_numbers(cls, data: Any) -> Any:
        paths = _forbidden_paths(data)
        if paths:
            raise ValueError(f"a document number in a Peruvian bundle: {', '.join(paths)}")
        return data
