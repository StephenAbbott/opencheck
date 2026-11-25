"""The /api page's reference facts stay true of the server.

``frontend/src/lib/apiReference.ts`` states the rate tiers, the lookup budget,
the replay window, the routes each tier covers, the response keys it explains
and the sample answer it serves. This test parses that file and checks each
against the code, so the page cannot under- or over-state a limit, name a
route under the wrong tier, describe a key ``/lookup`` does not return, or
serve a sample that carries a restricted source's records.
"""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path

import pytest

from opencheck import lookup_replay
from opencheck.config import Settings
from opencheck.ratelimit import limiter
from opencheck.routers.lookup import LookupResponse

_ROOT = Path(__file__).resolve().parents[2]
_TS = _ROOT / "frontend" / "src" / "lib" / "apiReference.ts"
_PUBLIC = _ROOT / "frontend" / "public"


def _src() -> str:
    return _TS.read_text(encoding="utf-8")


def _tiers() -> dict[str, tuple[int, str]]:
    found = re.findall(
        r'tier:\s*"(\w+)",\s*perMinute:\s*(\d+),\s*covers:\s*"([^"]*)"', _src(), re.S
    )
    return {tier: (int(n), covers) for tier, n, covers in found}


def _const_int(name: str) -> int:
    m = re.search(rf"export const {name} = (\d+);", _src())
    assert m, name
    return int(m.group(1))


def _sample() -> dict[str, str]:
    block = _src()[_src().index("export const SAMPLE_LOOKUP") :]
    block = block[: block.index("};")]
    return dict(re.findall(r'(\w+):\s*"([^"]*)"', block))


def _defaults() -> dict[str, str]:
    fields = Settings.model_fields
    return {
        "default": fields["rate_limit_default"].default,
        "lookup": fields["rate_limit_lookup"].default,
        "heavy": fields["rate_limit_heavy"].default,
    }


def test_the_page_states_the_configured_tiers() -> None:
    tiers = _tiers()
    assert set(tiers) == {"default", "lookup", "heavy"}
    for tier, limit in _defaults().items():
        assert limit == f"{tiers[tier][0]}/minute", (tier, limit)


def test_the_budget_and_replay_window_match_the_server() -> None:
    assert _const_int("LOOKUP_BUDGET_PER_MINUTE") == int(_defaults()["lookup"].split("/")[0])
    assert _const_int("REPLAY_WINDOW_MINUTES") * 60 == lookup_replay._REPLAY_TTL_SECONDS


def _route_tiers() -> dict[str, str]:
    from opencheck.app import app

    out: dict[str, str] = {}
    for route in app.routes:
        endpoint = getattr(route, "endpoint", None)
        path = getattr(route, "path", None)
        if endpoint is None or path is None:
            continue
        key = f"{getattr(endpoint, '__module__', '')}.{getattr(endpoint, '__name__', '')}"
        groups = limiter._route_limits.get(key) or limiter._dynamic_route_limits.get(key) or []
        for group in groups:
            provider = getattr(group, "_LimitGroup__limit_provider", None)
            name = getattr(provider, "__name__", "")
            if name.endswith("_tier"):
                out[path] = name.removesuffix("_tier")
    return out


def test_every_route_named_under_a_tier_is_in_that_tier() -> None:
    routes = _route_tiers()
    for tier, (_, covers) in _tiers().items():
        for path in re.findall(r"(/[a-z][a-z0-9/_-]*)", covers):
            if path in {"/sources", "/health"}:
                assert path not in routes, f"{path} is limited ({routes.get(path)})"
                continue
            assert routes.get(path) == tier, f"{path}: page says {tier}, server {routes.get(path)}"


def test_the_described_keys_are_lookup_response_fields() -> None:
    block = _src()[_src().index("export const RESPONSE_KEYS") :]
    block = block[: block.index("];")]
    keys = re.findall(r'\[\s*"(\w+)",', block)
    assert keys
    assert set(keys) <= set(LookupResponse.model_fields), set(keys) - set(LookupResponse.model_fields)


def _builder():
    spec = importlib.util.spec_from_file_location(
        "build_api_sample", _ROOT / "backend" / "scripts" / "build_api_sample.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def sample() -> dict:
    path = _PUBLIC / _sample()["path"].lstrip("/")
    assert path.is_file(), f"{path} is missing: run backend/scripts/build_api_sample.py"
    return json.loads(path.read_text(encoding="utf-8"))


def test_the_sample_is_the_named_company(sample: dict) -> None:
    meta = _sample()
    assert sample["lei"] == meta["lei"]
    assert sample["legal_name"] == meta["name"]
    assert sample["_sample"]["request"].endswith(f"/lookup?lei={meta['lei']}")


def test_the_sample_carries_every_described_key(sample: dict) -> None:
    block = _src()[_src().index("export const RESPONSE_KEYS") :]
    keys = re.findall(r'\[\s*"(\w+)",', block[: block.index("];")])
    assert set(keys) <= set(sample), set(keys) - set(sample)


def test_the_sample_carries_no_restricted_source(sample: dict) -> None:
    builder = _builder()
    restricted = builder.restricted_sources()
    assert "opensanctions" in restricted and "opencorporates" in restricted
    assert builder.leaked(sample, restricted) == []
    assert not set(sample["_sample"]["licences"]) & restricted


def test_stripping_drops_a_relationship_to_a_removed_party() -> None:
    builder = _builder()
    oc = {"source": {"opencheckSourceId": "opencorporates"}}
    payload = {
        "bods": [
            {"statementId": "p1", "recordId": "r1", "recordType": "person", **oc},
            {"statementId": "e1", "recordId": "e1", "recordType": "entity",
             "source": {"opencheckSourceId": "gleif"}},
            {"statementId": "x", "recordId": "x", "recordType": "relationship",
             "source": {"opencheckSourceId": "gleif"},
             "recordDetails": {"subject": "e1", "interestedParty": "r1"}},
        ],
        "hits": [{"source_id": "opencorporates"}, {"source_id": "gleif"}],
        "risk_signals": [
            {"source_id": "opencheck", "evidence": {"source_ids": ["gleif", "opencorporates"]}},
            {"source_id": "gleif", "evidence": {}},
        ],
        "subject_profile": {
            "register_status": {"source_id": "opencorporates", "sources": ["opencorporates"]},
            "legal_form": {"value": "Ltd", "sources": ["gleif", "opencorporates"]},
        },
    }
    out = builder.strip_restricted(payload, builder.restricted_sources())
    assert [s["statementId"] for s in out["bods"]] == ["e1"]
    assert out["hits"] == [{"source_id": "gleif"}]
    assert out["risk_signals"] == [{"source_id": "gleif", "evidence": {}}]
    assert out["subject_profile"]["register_status"] is None
    assert out["subject_profile"]["legal_form"]["sources"] == ["gleif"]
    assert builder.leaked(out, builder.restricted_sources()) == []
