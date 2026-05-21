"""Source API schema validation.

Every public register adapter validates its raw HTTP response against a
Pydantic model before passing data to the BODS mapper.  If a registry
changes a field that the mapper relies on, ``validate_raw`` raises
``SourceSchemaError`` — a distinct exception type that surfaces in the
frontend as *"Source API changed"* rather than a generic fetch error.

Design rules
------------
* Models use ``model_config = ConfigDict(extra="allow")`` so registries
  adding new fields never cause false-positive failures.
* Only fields the mapper actually reads are declared as model attributes.
  Optional fields default to ``None`` or an empty list.
* Required fields (those whose absence would crash or silently corrupt the
  BODS output) are declared without defaults.
* ``validate_raw`` wraps ``ValidationError`` so callers never need to import
  Pydantic themselves — the contract is *SourceSchemaError or nothing*.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError


class SourceSchemaError(Exception):
    """A source API response failed Pydantic validation.

    Signals a registry schema change rather than a network fault.  The
    ``source_id`` attribute is set so callers can include it in error messages
    without re-parsing the exception string.
    """

    def __init__(self, source_id: str, details: str) -> None:
        self.source_id = source_id
        super().__init__(
            f"{source_id}: source API schema changed — {details}"
        )


class _Base(BaseModel):
    """Shared config for all source schema models."""

    model_config = ConfigDict(
        extra="allow",        # unknown fields from the registry are silently ignored
        populate_by_name=True,
    )


def validate_raw(
    source_id: str,
    model_cls: type[_Base],
    raw: dict[str, Any],
) -> _Base:
    """Validate *raw* against *model_cls*.

    Returns the validated model on success.  Raises :exc:`SourceSchemaError`
    (never ``pydantic.ValidationError``) on failure so the adapter layer
    never needs to import Pydantic.

    Usage::

        from opencheck.sources.schemas import validate_raw
        from opencheck.sources.schemas.companies_house import CHBundle

        validate_raw("companies_house", CHBundle, bundle)
    """
    try:
        return model_cls.model_validate(raw)
    except ValidationError as exc:
        n = exc.error_count()
        # Show at most three field paths to keep the error concise.
        first_paths = [
            " → ".join(str(loc) for loc in e["loc"])
            for e in exc.errors()[:3]
        ]
        details = f"{n} field(s) failed: {', '.join(first_paths)}"
        raise SourceSchemaError(source_id, details) from exc


__all__ = ["SourceSchemaError", "validate_raw", "_Base"]
