"""The output data dictionary (``docs/data-dictionary.md``) is generated from
the code and cannot drift from it.

Four things are pinned:

* the committed file equals the rendering (what ``--check`` asserts in CI);
* every ``opencheck*`` key the package writes is a registered extension —
  found statically, as a string literal anywhere under ``opencheck/``, and at
  run time by the mapper guard on every statement every mapper yields;
* every column or key the export writers produce for the sample bundle has a
  description (the render raises otherwise), and every field of the core
  response models has one;
* the ``DATA-DICTIONARY.md`` in the CSV, BigQuery and AML AI zips is the
  dictionary's own rendering of that family.
"""

from __future__ import annotations

import io
import re
import zipfile
from pathlib import Path

import pytest

from opencheck import data_dictionary as dd
from opencheck.bods.statements import make_entity_statement
from tests import _entity_subtype_guard as guard

PACKAGE = Path(dd.__file__).resolve().parent
_LITERAL = re.compile(r"""["'](opencheck[A-Z][A-Za-z0-9]*)["']""")


def test_the_committed_dictionary_is_current():
    assert not dd.is_stale(), (
        "docs/data-dictionary.md is stale — run "
        "`uv run python scripts/generate_data_dictionary.py` from backend/"
    )


def test_every_extension_key_literal_in_the_package_is_registered():
    found: dict[str, set[str]] = {}
    for path in PACKAGE.rglob("*.py"):
        if path.name == "data_dictionary.py":
            continue
        for key in _LITERAL.findall(path.read_text(encoding="utf-8")):
            found.setdefault(key, set()).add(str(path.relative_to(PACKAGE)))
    unregistered = {k: sorted(v) for k, v in found.items() if k not in dd.EXTENSION_KEYS}
    assert not unregistered, (
        f"opencheck* keys written without a data-dictionary entry: {unregistered}"
    )
    # And the registry documents nothing the code does not write.
    assert set(found) >= {k for k in dd.EXTENSION_KEYS if k.startswith(dd.EXTENSION_PREFIX)}


def test_the_sample_bundle_carries_every_registered_extension():
    seen = {key for _p, key in dd.extension_keys_in(dd.sample_bundle())}
    seen |= {
        k
        for stmt in dd.sample_bundle()
        for ann in stmt.get("annotations") or []
        for k in ann
        if k in dd.EXTENSION_KEYS
    }
    assert seen == dd.EXTENSION_KEYS


def test_unregistered_extension_keys_are_found_at_any_depth():
    stmt = {"source": {"opencheckSourceId": "gleif"}, "recordDetails": {
        "interests": [{"type": "shareholding", "opencheckNewThing": 1}]
    }}
    assert dd.unregistered_extension_keys(stmt) == [
        "/recordDetails/interests/0/opencheckNewThing"
    ]


def test_the_mapper_guard_fails_an_unregistered_extension_key():
    stmt = make_entity_statement(source_id="gleif", local_id="dd-guard", name="X")
    stmt["recordDetails"]["opencheckMystery"] = True
    guard.check_statement("map_test", stmt)
    violations = [v for v in guard.drain() if "opencheckMystery" in v[2]]
    assert violations and "EXTENSIONS" in violations[0][2]


def test_every_export_column_is_described():
    for family, _label, _blurb in dd.EXPORT_FAMILIES:
        for table, rows in dd.export_tables(family):
            assert rows, (family, table)
            for column, words in rows:
                assert words.strip(), (table, column)


def test_an_undescribed_column_is_an_error(monkeypatch):
    monkeypatch.setitem(dd.COLUMN_DESCRIPTIONS, "rows.csv", {})
    with pytest.raises(dd.UndescribedColumnError, match="rows.csv"):
        dd.export_tables("batch")


def test_every_field_of_a_strict_model_is_described():
    assert dd.undescribed_fields(dd.documented_models()) == []


def test_the_field_comment_is_read_from_the_source():
    (model,) = dd.harvest_models("routers.lookup", ("LookupResponse",))
    fields = {f.name: f for f in model.fields}
    assert fields["replayed"].type == "bool"
    assert fields["replayed"].default == "False"
    assert "replay cache" in fields["replayed"].description
    (hit,) = dd.harvest_models("sources.base", ("SourceHit",))
    assert "SENTENCE" in {f.name: f for f in hit.fields}["finding"].description


def test_mcp_tools_are_all_harvested_with_their_keys():
    from opencheck.mcp.server import mcp

    tools = {t.name: t for t in dd.harvest_mcp_tools()}
    assert set(tools) == {t.name for t in mcp._tool_manager.list_tools()}
    assert "risk_signals" in tools["opencheck_lookup"].keys
    assert "scheme" in tools["opencheck_register_lookup"].keys  # one hop into shaping
    assert "calculated_owners" in tools["opencheck_owners"].keys  # one hop into owners
    assert tools["opencheck_ownership_map"].content_blocks
    for name in dd.MCP_RESPONSE_NOTES:
        assert name in tools


def test_the_zip_dictionary_is_the_family_rendering():
    from opencheck.routers.export import _build_csv_zip

    body = _build_csv_zip(dd.sample_bundle(), slug="x", stamp="d", licenses_md="# L")
    with zipfile.ZipFile(io.BytesIO(body)) as zf:
        names = {n.rsplit("/", 1)[-1] for n in zf.namelist()}
        assert {"entity_nodes.csv", "ownership_edges.csv", "LICENSES.md", "DATA-DICTIONARY.md"} <= names
        text = next(zf.read(n).decode() for n in zf.namelist() if n.endswith("DATA-DICTIONARY.md"))
    assert text == dd.render_zip_dictionary("csv")
    assert "`ownership_edges.csv`" in text and dd.DOC_URL in text
