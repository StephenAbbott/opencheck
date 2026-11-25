"""The public OpenAPI document says, for every operation, what a caller needs.

``opencheck/openapi_docs.py`` rewrites FastAPI's schema; these tests fail when
a new public route is not described there, an entry outlives its route, a
development note (a phase number) reaches the document, a parameter has no
description, a stream or download claims to be JSON, or a recorded example no
longer matches its response model.
"""

from __future__ import annotations

import json
import re

import pytest

from opencheck import openapi_docs
from opencheck.app import app


@pytest.fixture(scope="module")
def schema() -> dict:
    app.openapi_schema = None
    return app.openapi()


def _operations(schema: dict):
    for path, ops in schema["paths"].items():
        for method, op in ops.items():
            yield method, path, op


def test_every_public_operation_is_described(schema: dict) -> None:
    public = {(m, p) for m, p, _ in _operations(schema)}
    assert public, "no operations"
    missing = public - set(openapi_docs.OPERATIONS)
    assert not missing, f"add these to openapi_docs.OPERATIONS: {sorted(missing)}"


def test_every_entry_names_a_route(schema: dict) -> None:
    public = {(m, p) for m, p, _ in _operations(schema)}
    stale = set(openapi_docs.OPERATIONS) - public
    assert not stale, f"no such route: {sorted(stale)}"


def test_the_website_pages_are_not_in_the_document(schema: dict) -> None:
    for path in schema["paths"]:
        assert not openapi_docs.is_hidden(path), path
    assert "/blog" not in schema["paths"] and "/entity/{token}" not in schema["paths"]
    assert all(m != "head" for m, _, _ in _operations(schema))


def test_tags_summaries_and_descriptions(schema: dict) -> None:
    tags = [t["name"] for t in schema["tags"]]
    used = set()
    for method, path, op in _operations(schema):
        assert op.get("tags") and op["tags"][0] in tags, (method, path)
        assert op.get("summary") and len(op["summary"]) <= 60, (method, path)
        assert op.get("description"), (method, path)
        used.add(op["tags"][0])
    assert used == set(tags), set(tags) - used


def test_no_development_notes_reach_the_document(schema: dict) -> None:
    text = json.dumps(schema)
    assert not re.search(r"\bPhases? \d+", text), re.findall(r".{40}Phases? \d+.{20}", text)


def test_every_parameter_is_described(schema: dict) -> None:
    for method, path, op in _operations(schema):
        for param in op.get("parameters", []):
            assert param.get("description"), (method, path, param["name"])


def test_limited_operations_state_their_tier_and_declare_429(schema: dict) -> None:
    tiers = openapi_docs.route_tiers(app)
    assert tiers[("get", "/lookup")] == "lookup"
    for method, path, op in _operations(schema):
        tier = tiers.get((method, path))
        if tier is None:
            assert "429" not in op["responses"], (method, path)
            continue
        assert op["x-rate-limit-tier"] == tier
        assert "Rate limit:" in op["description"], (method, path)
        assert "Retry-After" in op["responses"]["429"]["headers"], (method, path)


def test_streams_and_downloads_declare_their_types(schema: dict) -> None:
    for (method, path), types in openapi_docs.CONTENT_TYPES.items():
        op = schema["paths"][path][method]
        ok = op["responses"].get("200") or op["responses"].get("201")
        assert set(ok["content"]) == set(types), (method, path)
    stream = schema["paths"]["/lookup-stream"]["get"]["responses"]
    assert set(stream["200"]["content"]) == {"text/event-stream"}
    assert "403" in stream


def test_errors_share_one_schema(schema: dict) -> None:
    assert schema["components"]["schemas"]["Error"]["required"] == ["detail"]
    lookup = schema["paths"]["/lookup"]["get"]["responses"]
    for code in ("400", "429", "502", "503"):
        assert lookup[code]["content"]["application/json"]["schema"] == {
            "$ref": "#/components/schemas/Error"
        }, code


def _response_models() -> dict[str, type]:
    out: dict[str, type] = {}
    for route in app.routes:
        model = getattr(route, "response_model", None)
        for method in getattr(route, "methods", None) or ():
            if model is not None:
                out[f"{method} {route.path}"] = model
    return out


def test_every_example_matches_its_response_model(schema: dict) -> None:
    examples = openapi_docs.load_examples()
    models = _response_models()
    assert {"GET /lookup", "GET /search", "GET /resolve-national-id"} <= set(examples)
    for key, example in examples.items():
        assert key in models, f"{key} has no response model to check the example against"
        models[key].model_validate(example)
        method, path = key.split(" ", 1)
        shown = schema["paths"][path][method.lower()]["responses"]["200"]["content"]["application/json"]
        assert shown["example"] == example


def test_the_examples_carry_no_restricted_source() -> None:
    import importlib.util
    from pathlib import Path

    spec = importlib.util.spec_from_file_location(
        "build_api_sample",
        Path(__file__).resolve().parents[1] / "scripts" / "build_api_sample.py",
    )
    assert spec and spec.loader
    builder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(builder)
    restricted = builder.restricted_sources()
    examples = openapi_docs.load_examples()
    assert builder.leaked(examples["GET /lookup"], restricted) == []
    assert not {h["source_id"] for h in examples["GET /search"]["hits"]} & restricted
