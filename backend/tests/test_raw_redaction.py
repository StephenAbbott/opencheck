"""Raw-payload redaction for sources whose licence forbids re-publication.

OpenCorporates permits OpenCheck's derived (BODS) output but not bulk
re-publication of the raw records, so the raw payload must be redacted from all
API responses and exports while the BODS output is unaffected.
"""

from __future__ import annotations

import json

from pydantic import BaseModel

# Importing the package populates base.RAW_SUPPRESSED_SOURCE_IDS from the registry.
import opencheck.sources  # noqa: F401
from opencheck.sources import REGISTRY
from opencheck.sources import base
from opencheck.sources.base import SourceHit, raw_redaction_notice


def _hit(source_id: str) -> SourceHit:
    return SourceHit(
        source_id=source_id,
        hit_id="x1",
        kind="entity",
        name="Demo Co",
        summary="demo",
        raw={"name": "Demo Co", "secret_field": "raw value"},
        is_stub=False,
    )


def test_opencorporates_adapter_does_not_republish_raw() -> None:
    assert REGISTRY["opencorporates"].republish_raw is False
    assert REGISTRY["gleif"].republish_raw is True
    assert "opencorporates" in base.RAW_SUPPRESSED_SOURCE_IDS


def test_opencorporates_raw_is_redacted_on_model_dump() -> None:
    dumped = _hit("opencorporates").model_dump()
    assert "_redacted" in dumped["raw"]
    assert "secret_field" not in dumped["raw"]
    assert dumped["raw"]["source_id"] == "opencorporates"


def test_opencorporates_raw_is_redacted_in_json() -> None:
    js = _hit("opencorporates").model_dump_json()
    assert "_redacted" in js
    assert "raw value" not in js  # the original raw content must not appear


def test_republishable_source_keeps_raw() -> None:
    dumped = _hit("companies_house").model_dump()
    assert dumped["raw"] == {"name": "Demo Co", "secret_field": "raw value"}


def test_redaction_applies_to_nested_hits() -> None:
    class _Wrap(BaseModel):
        hits: list[SourceHit]

    wrap = _Wrap(hits=[_hit("opencorporates"), _hit("gleif")])
    dumped = json.loads(wrap.model_dump_json())
    by_src = {h["source_id"]: h for h in dumped["hits"]}
    assert "_redacted" in by_src["opencorporates"]["raw"]
    assert by_src["gleif"]["raw"]["secret_field"] == "raw value"


def test_redaction_notice_shape() -> None:
    n = raw_redaction_notice("opencorporates")
    assert n["source_id"] == "opencorporates"
    assert "not redistributed" in n["_redacted"].lower()
