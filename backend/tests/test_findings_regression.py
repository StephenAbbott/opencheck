"""Phase 277 — the weekly findings regression's runner, offline.

The run itself reads production weekly (``.github/workflows/findings-regression.yml``);
these tests pin the parts that must not drift silently: the golden files'
schema, the golden set against the homepage cards, each failure class on a
fixture, the pacing and retry behaviour, and the week-over-week diff.
"""

from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from opencheck import findings_regression as fr

LEI = "5493001KJTIIGC8Y1R12"


# --- fixtures ------------------------------------------------------------------


def _exp(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "lei": LEI,
        "name": "Example Ltd",
        "role": "anchor",
        "why": "fixture",
        "example_card": False,
        "no_risk": False,
        "risk": {"SANCTIONED": "high"},
        "context": ["GLEIF_REPORTING_EXCEPTION"],
        "absent": ["FATF_GREY_LIST"],
        "verdict": {"matches": [r"sanctions findings"], "not_matches": [r"watch list"]},
        "sources_found": ["gleif", "opensanctions"],
        "liveness": {"gleif": ["live", "cached"]},
        "lei_confirmed_min": 2,
        "intermediate_layers_min": None,
    }
    base.update(over)
    return base


def _sig(
    code: str, kind: str | None = "risk", conf: str = "high", source: str = "opensanctions"
) -> dict[str, Any]:
    sig: dict[str, Any] = {"code": code, "confidence": conf, "source_id": source, "hit_id": "h"}
    if kind is not None:
        sig["kind"] = kind
    return sig


def _payload(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "lei": LEI,
        "legal_name": "EXAMPLE LTD",
        "verdict": "The records show sanctions findings on the company itself.",
        "risk_signals": [
            _sig("SANCTIONED"),
            _sig("GLEIF_REPORTING_EXCEPTION", "context", source="gleif"),
        ],
        "degraded_sources": [],
        "hits": [
            {"source_id": "gleif", "is_stub": False},
            {"source_id": "opensanctions", "is_stub": False},
            {"source_id": "climatetrace", "is_stub": True},
        ],
        "source_liveness": {
            "gleif": {"liveness": "live"},
            "opensanctions": {"liveness": "live"},
            "climatetrace": {"liveness": "stub"},
        },
        "cross_source_links": [
            {
                "key": "lei",
                "key_value": LEI,
                "hits": [{"source_id": "gleif"}, {"source_id": "wikidata"}],
            }
        ],
        "graph_shape": {},
        "subject_profile": {"lei_registration": {"status": "ISSUED"}},
        "sources_applicable": ["opensanctions", "climatetrace"],
        "replayed": False,
    }
    base.update(over)
    return base


RULES = fr.Rules(
    structural_codes=frozenset(
        {"NON_EU_JURISDICTION", "STATE_CONTROLLED", "COMPLEX_OWNERSHIP_LAYERS"}
    ),
    retired_codes=frozenset({"COMPLEX_CORPORATE_STRUCTURE"}),
)


def _checks(findings: list[fr.Finding]) -> list[str]:
    return [f.check for f in findings]


# --- the golden files -------------------------------------------------------------


def test_every_golden_file_validates() -> None:
    exps = fr.load_expectations()
    assert len(exps) == 13
    roles = {e["role"] for e in exps}
    assert roles == {"curated", "anchor", "control"}
    assert sum(1 for e in exps if e["role"] == "control") == 2
    for e in exps:
        assert e["_file"].endswith(".json")


def test_curated_golden_files_are_exactly_the_homepage_cards() -> None:
    """The golden set and the cards cannot drift apart: a card added or
    removed without its golden file (or the reverse) fails here, offline."""
    cards = fr.load_example_cards()
    curated = {e["lei"] for e in fr.load_expectations() if e["role"] == "curated"}
    carded = {e["lei"] for e in fr.load_expectations() if e["example_card"]}
    assert curated == set(cards)
    assert carded == set(cards)


def test_controls_expect_no_risk_and_the_rules_hold_against_the_engine() -> None:
    rules = fr.Rules.from_code()
    assert "COMPLEX_CORPORATE_STRUCTURE" in rules.retired_codes
    assert "SUBSIDIARY_LISTED_JURISDICTION" in rules.structural_codes
    for e in fr.load_expectations():
        if e["role"] == "control":
            assert e["no_risk"] and not e["risk"]
        # A golden file may not expect a structural code as risk below the
        # firing confidence, nor expect a retired code at all.
        assert not (set(e["risk"]) | set(e["context"])) & rules.retired_codes


@pytest.mark.parametrize(
    ("over", "message"),
    [
        ({"risk": {"SANCTIONED": "certain"}}, "confidence"),
        ({"lei_confirmed_min": True}, "integer"),
        ({"absent": ["SANCTIONED"]}, "both expected and ruled out"),
        ({"context": ["SANCTIONED", "GLEIF_REPORTING_EXCEPTION"]}, "both risk and context"),
        ({"role": "showcase"}, "role"),
        ({"lei": "not-an-lei"}, "not an LEI"),
        ({"verdict": {"matches": ["("], "not_matches": []}}, "bad verdict pattern"),
        ({"no_risk": True}, "no_risk"),
        ({"liveness": {"gleif": []}}, "non-empty"),
        ({"surprise": 1}, "unknown field"),
    ],
)
def test_validate_expectation_rejects(over: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        fr.validate_expectation(_exp(**over))


def test_validate_expectation_rejects_a_missing_field() -> None:
    data = _exp()
    del data["verdict"]
    with pytest.raises(ValueError, match="missing 'verdict'"):
        fr.validate_expectation(data)


def test_load_expectations_rejects_a_duplicate_lei(tmp_path: Path) -> None:
    for name in ("a.json", "b.json"):
        (tmp_path / name).write_text(json.dumps(_exp()))
    with pytest.raises(ValueError, match="two files"):
        fr.load_expectations(tmp_path)


# --- the homepage cards -----------------------------------------------------------


def test_parse_example_cards_reads_the_real_file() -> None:
    cards = fr.load_example_cards()
    assert len(cards) == 6
    assert cards["213800LH1BZH3DI6G760"]["OFFSHORE_LEAKS"] in fr.CONFIDENCE_RANK
    assert all(cards.values()), "every curated card lists at least one risk code"


def test_parse_example_cards_splits_entries_and_fails_loudly() -> None:
    tsx = """
export const EXAMPLE_LEIS: ExampleLei[] = [
  {
    lei: "AAAAAAAAAAAAAAAAAA11",
    signals: [
      { code: "SANCTIONED", confidence: "high" },
    ],
  },
  {
    lei: "BBBBBBBBBBBBBBBBBB22",
  },
];
"""
    assert fr.parse_example_cards(tsx) == {
        "AAAAAAAAAAAAAAAAAA11": {"SANCTIONED": "high"},
        "BBBBBBBBBBBBBBBBBB22": {},
    }
    with pytest.raises(ValueError, match="not found"):
        fr.parse_example_cards("export const OTHER = [];")


# --- each failure class ----------------------------------------------------------------


def test_a_lookup_that_meets_its_expectations_passes() -> None:
    findings, summary = fr.evaluate(_exp(), _payload(), rules=RULES)
    assert findings == []
    assert summary["signals"]["SANCTIONED"] == {
        "kinds": ["risk"],
        "confidence": "high",
        "sources": ["opensanctions"],
        "count": 1,
        "leads": 0,
    }
    assert summary["lei_confirmed_by"] == ["gleif", "wikidata"]


def test_kind_mismatch_on_the_files_claim_a_missing_kind_and_a_mixed_kind() -> None:
    payload = _payload(
        risk_signals=[
            _sig("SANCTIONED", "context"),
            _sig("GLEIF_REPORTING_EXCEPTION", "context", source="gleif"),
            _sig("SANCTIONED_SECURITY", None),
            _sig("RELATED_PEP", "risk"),
            _sig("RELATED_PEP", "context", source="openaleph"),
        ]
    )
    findings, _ = fr.evaluate(_exp(), payload, rules=RULES)
    messages = [f.message for f in findings if f.check == "kind_mismatch"]
    assert any("SANCTIONED is expected as a risk finding" in m for m in messages)
    assert any("SANCTIONED_SECURITY" in m and "no valid kind" in m for m in messages)
    assert any("RELATED_PEP arrives as both" in m for m in messages)


def test_missing_and_unexpected_signals() -> None:
    payload = _payload(
        risk_signals=[
            _sig("SANCTIONED", conf="medium"),
            _sig("FATF_GREY_LIST", source="gleif"),
            _sig("COMPLEX_CORPORATE_STRUCTURE", source="companies_house"),
        ]
    )
    findings, _ = fr.evaluate(_exp(), payload, rules=RULES)
    messages = [(f.check, f.message) for f in findings]
    assert ("missing_signal", "SANCTIONED is at medium, below the expected floor high") in messages
    assert any(c == "missing_signal" and "GLEIF_REPORTING_EXCEPTION" in m for c, m in messages)
    assert any(
        c == "unexpected_signal" and m.startswith("FATF_GREY_LIST is present") for c, m in messages
    )
    assert (
        "unexpected_signal",
        "retired code COMPLEX_CORPORATE_STRUCTURE is emitted again",
    ) in messages


def test_a_control_with_a_risk_finding_fails_but_context_is_fine() -> None:
    exp = _exp(risk={}, no_risk=True, verdict={"matches": [], "not_matches": []})
    clean = _payload(
        risk_signals=[_sig("GLEIF_REPORTING_EXCEPTION", "context", source="gleif")],
        verdict="No risk signals surfaced across the sources that answered.",
    )
    assert fr.evaluate(exp, clean, rules=RULES)[0] == []
    dirty = _payload(
        risk_signals=[*clean["risk_signals"], _sig("OFFSHORE_LEAKS", conf="medium", source="icij")]
    )
    findings, _ = fr.evaluate(exp, dirty, rules=RULES)
    assert any(f.check == "unexpected_signal" and "OFFSHORE_LEAKS" in f.message for f in findings)


def test_structural_code_repeated_in_one_lookup() -> None:
    payload = _payload(
        risk_signals=[
            *_payload()["risk_signals"],
            _sig("NON_EU_JURISDICTION", "context", "low", "gleif"),
            _sig("NON_EU_JURISDICTION", "context", "low", "opensanctions"),
        ]
    )
    findings, _ = fr.evaluate(_exp(), payload, rules=RULES)
    assert _checks(findings) == ["structural_repeated"]
    assert "appears 2 times (gleif, opensanctions)" in findings[0].message


def test_degraded_reads_clean_only_when_the_verdict_states_an_absence() -> None:
    exp = _exp(risk={}, verdict={"matches": [], "not_matches": []})
    degraded = [{"source_id": "wikirate", "check": "source_read"}]
    absence = _payload(
        risk_signals=[_sig("GLEIF_REPORTING_EXCEPTION", "context", source="gleif")],
        degraded_sources=degraded,
        verdict="No risk signals surfaced across the sources that answered.",
    )
    assert _checks(fr.evaluate(exp, absence, rules=RULES)[0]) == ["degraded_reads_clean"]

    caveated = {
        **absence,
        "verdict": "No risk signals surfaced, but one source answered only in part.",
    }
    assert fr.evaluate(exp, caveated, rules=RULES)[0] == []

    # A sentence stating a finding carries no caveat by design (verdict.py).
    found = _payload(degraded_sources=degraded)
    assert fr.evaluate(_exp(), found, rules=RULES)[0] == []

    # Structural-only risk is not a finding the caveat may be dropped for.
    layered = {
        **absence,
        "risk_signals": [_sig("COMPLEX_OWNERSHIP_LAYERS", conf="medium", source="companies_house")],
    }
    layered["verdict"] = "Its ownership chain has 3 intermediate corporate layers."
    assert "degraded_reads_clean" in _checks(fr.evaluate(exp, layered, rules=RULES)[0])


def test_verdict_patterns() -> None:
    findings, _ = fr.evaluate(
        _exp(),
        _payload(verdict="The records show a jurisdiction on an international watch list."),
        rules=RULES,
    )
    assert [f.check for f in findings if f.check == "verdict"] == ["verdict", "verdict"]
    findings, _ = fr.evaluate(_exp(), _payload(verdict=None), rules=RULES)
    assert any(f.message == "no verdict sentence" for f in findings)


def test_sources_liveness_and_the_placeholder_badge() -> None:
    payload = _payload(
        hits=[
            {"source_id": "gleif", "is_stub": False},
            {"source_id": "companies_house", "is_stub": False},
        ],
        source_liveness={
            "gleif": {"liveness": "snapshot"},
            "companies_house": {"liveness": "stub"},
        },
    )
    findings, _ = fr.evaluate(_exp(), payload, rules=RULES)
    checks = _checks(findings)
    assert "source_not_found" in checks  # opensanctions returned nothing
    assert "liveness" in checks  # gleif snapshot, not live/cached
    assert any(
        f.check == "placeholder_badge" and f.message.startswith("companies_house") for f in findings
    )


def test_lei_confirmation_counts_independent_origins_only() -> None:
    # OpenAleph's register collections are GLEIF: two sources, one origin.
    links = [
        {
            "key": "lei",
            "key_value": LEI.lower(),
            "hits": [{"source_id": "gleif"}, {"source_id": "openaleph"}],
        }
    ]
    findings, summary = fr.evaluate(_exp(), _payload(cross_source_links=links), rules=RULES)
    assert summary["lei_confirmed_by"] == ["gleif"]
    assert _checks(findings) == ["lei_confirmation"]


def test_layers_and_lei_registration() -> None:
    exp = _exp(intermediate_layers_min=2)
    payload = _payload(graph_shape={"intermediate_layers": 1}, subject_profile={})
    checks = _checks(fr.evaluate(exp, payload, rules=RULES)[0])
    assert "layers" in checks and "lei_registration" in checks
    ok = _payload(graph_shape={"intermediate_layers": 7})
    assert fr.evaluate(exp, ok, rules=RULES)[0] == []


def test_card_drift_in_all_three_directions() -> None:
    exp = _exp(example_card=True)
    payload = _payload(
        risk_signals=[
            *_payload()["risk_signals"],
            _sig("RELATED_PEP", conf="high", source="openaleph"),
        ]
    )
    cards = {LEI: {"SANCTIONED": "medium", "RELATED_PEP": "high", "DEBARMENT": "high"}}
    payload["risk_signals"] = [s for s in payload["risk_signals"] if s["code"] != "RELATED_PEP"]
    payload["risk_signals"].append(_sig("EXPORT_RISK", conf="medium"))
    findings, _ = fr.evaluate(exp, payload, rules=RULES, cards=cards)
    drift = sorted(f.message for f in findings if f.check == "card_drift")
    assert drift == [
        "SANCTIONED: the card says medium, production's highest is high",
        "production returns EXPORT_RISK (medium); the card does not list it",
        "the card lists DEBARMENT (high); production no longer returns it",
        "the card lists RELATED_PEP (high); production no longer returns it",
    ]
    findings, _ = fr.evaluate(exp, _payload(), rules=RULES, cards={})
    assert [f.message for f in findings] == ["no EXAMPLE_LEIS card for this curated subject"]


def test_card_drift_ignores_context_codes() -> None:
    exp = _exp(example_card=True)
    findings, _ = fr.evaluate(exp, _payload(), rules=RULES, cards={LEI: {"SANCTIONED": "high"}})
    assert findings == []


def test_mcp_disagreeing_with_lookup() -> None:
    payload = _payload()
    mcp = {
        "verdict": payload["verdict"],
        "risk_signals": [
            {"code": "SANCTIONED", "kind": "risk"},
            {"code": "GLEIF_REPORTING_EXCEPTION", "kind": "context"},
        ],
        "degraded_sources": [],
        "counts": {"risk_signals": 1, "sources_applicable": 3},
    }
    assert fr.evaluate(_exp(), payload, rules=RULES, mcp=mcp)[0] == []

    bad = copy.deepcopy(mcp)
    bad["risk_signals"][1]["kind"] = "risk"  # the Phase 153 class
    bad["verdict"] = "Something else."
    bad["counts"]["sources_applicable"] = 11
    messages = [f.message for f in fr.evaluate(_exp(), payload, rules=RULES, mcp=bad)[0]]
    assert any("risk codes differ" in m and "GLEIF_REPORTING_EXCEPTION" in m for m in messages)
    assert any("MCP verdict" in m for m in messages)
    assert any("applicable sources" in m for m in messages)
    assert any("counts.risk_signals 1" in m for m in messages)


def test_an_mcp_failure_is_a_warning_not_a_failure() -> None:
    findings, _ = fr.evaluate(_exp(), _payload(), rules=RULES, mcp={"error": "ReadTimeout"})
    assert [(f.check, f.severity) for f in findings] == [("mcp_mismatch", fr.WARN)]


# --- fetching and pacing -----------------------------------------------------------


def _client(handler: Any) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_fetch_lookup_waits_out_retry_after_then_succeeds() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "7"})
        return httpx.Response(200, json=_payload())

    slept: list[float] = []
    got = fr.fetch_lookup(_client(handler), "https://api.example", LEI, sleep=slept.append)
    assert got.payload is not None and got.error is None
    assert slept == [7.0] and got.attempts == 2
    assert calls[0].url.params["refresh"] == "true" and calls[0].url.params["lei"] == LEI
    assert calls[0].url.path == "/lookup"


def test_fetch_lookup_caps_the_wait_and_gives_up_after_the_retries() -> None:
    slept: list[float] = []
    got = fr.fetch_lookup(
        _client(lambda r: httpx.Response(503, headers={"Retry-After": "9999"})),
        "https://api.example",
        LEI,
        sleep=slept.append,
    )
    assert got.payload is None and got.error == "HTTP 503"
    assert slept == [120.0, 120.0]


def test_fetch_lookup_does_not_retry_a_plain_error() -> None:
    slept: list[float] = []
    got = fr.fetch_lookup(
        _client(lambda r: httpx.Response(404)), "https://api.example", LEI, sleep=slept.append
    )
    assert got.error == "HTTP 404" and slept == [] and got.attempts == 1


def test_run_paces_between_subjects_and_reports_a_failed_lookup() -> None:
    other = "5493002KJTIIGC8Y1R13"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["lei"] == other:
            return httpx.Response(500)
        return httpx.Response(200, json=_payload())

    mcp_calls: list[str] = []

    async def mcp_call(lei: str) -> dict[str, Any]:
        mcp_calls.append(lei)
        return {"error": "not configured"}

    slept: list[float] = []
    report = asyncio.run(
        fr.run(
            [_exp(), _exp(lei=other, name="Other")],
            client=_client(handler),
            base_url="https://api.example",
            rules=RULES,
            cards={},
            mcp_call=mcp_call,
            pace_s=42.0,
            sleep=slept.append,
            stamps={"verdict_template": 4, "signal_rules": 2, "signal_rules_changed": {}},
            now=lambda: "2026-10-05T08:30:00Z",
        )
    )
    assert slept == [42.0], "one pause between two subjects, none before the first"
    assert mcp_calls == [LEI], "no MCP call when /lookup itself failed"
    first, second = report["subjects"]
    assert [f["severity"] for f in first["findings"]] == ["warn"]
    assert second["findings"] == [
        {"check": "lookup_failed", "severity": "fail", "message": "/lookup failed: HTTP 500"}
    ]
    assert report["totals"] == {
        "subjects": 2,
        "passed": 1,
        "failed": 1,
        "failed_leis": [other],
        "by_check": {"lookup_failed": 1, "mcp_mismatch": 1},
    }
    assert fr.failed(report)
    md = fr.render_markdown(report)
    assert "**1 of 2 subjects as expected**" in md
    assert "No previous report to compare against." in md


# --- week over week -------------------------------------------------------------------


def _report(
    signals: dict[str, Any], verdict: str, rules: dict[str, Any], when: str
) -> dict[str, Any]:
    return {
        "generated_at": when,
        "rules": rules,
        "subjects": [
            {
                "lei": LEI,
                "name": "Example",
                "summary": {"signals": signals, "verdict": verdict},
                "findings": [],
            }
        ],
    }


def _row(conf: str, *sources: str, kinds: tuple[str, ...] = ("risk",)) -> dict[str, Any]:
    return {
        "kinds": list(kinds),
        "confidence": conf,
        "sources": list(sources),
        "count": len(sources),
    }


def test_diff_names_what_appeared_disappeared_and_moved() -> None:
    stamps = {"verdict_template": 4, "signal_rules": 2, "signal_rules_changed": {}}
    before = _report(
        {"RELATED_PEP": _row("medium", "openaleph"), "DEBARMENT": _row("high", "opensanctions")},
        "Old.",
        stamps,
        "2026-09-28T08:30:00Z",
    )
    now = _report(
        {
            "RELATED_PEP": _row("high", "openaleph", "opensanctions"),
            "RELATED_EXPORT_CONTROLLED": _row("high", "openaleph"),
        },
        "New.",
        stamps,
        "2026-10-05T08:30:00Z",
    )
    assert fr.diff_against(before, now) == {
        LEI: [
            "+RELATED_EXPORT_CONTROLLED (openaleph)",
            "−DEBARMENT (opensanctions)",
            "RELATED_PEP: confidence medium → high",
            "RELATED_PEP: sources +opensanctions",
            "verdict changed: 'New.'",
        ]
    }
    assert fr.diff_against(None, now) == {}


def test_diff_labels_a_rule_change_as_a_rule_change() -> None:
    before = _report(
        {"FATF_GREY_LIST": _row("high", "gleif")},
        "Watch list.",
        {"verdict_template": 3, "signal_rules": 1, "signal_rules_changed": {}},
        "a",
    )
    now = _report(
        {"SUBSIDIARY_LISTED_JURISDICTION": _row("low", "gleif", kinds=("context",))},
        "No risk signals.",
        {
            "verdict_template": 4,
            "signal_rules": 2,
            "signal_rules_changed": {"2": ["FATF_GREY_LIST", "SUBSIDIARY_LISTED_JURISDICTION"]},
        },
        "b",
    )
    assert fr.diff_against(before, now) == {
        LEI: [
            "+SUBSIDIARY_LISTED_JURISDICTION (gleif) (rule change)",
            "−FATF_GREY_LIST (gleif) (rule change)",
            "verdict changed (verdict template changed): 'No risk signals.'",
        ]
    }


def test_rule_stamps_come_from_the_watchlist() -> None:
    from opencheck import watchlist
    from opencheck.verdict import VERDICT_TEMPLATE

    stamps = fr.rule_stamps()
    assert stamps["verdict_template"] == VERDICT_TEMPLATE
    assert stamps["signal_rules"] == watchlist.SIGNAL_RULES
    assert set(stamps["signal_rules_changed"]) == {str(k) for k in watchlist.SIGNAL_RULES_CHANGED}


def test_the_script_refuses_to_start_without_golden_files(tmp_path: Path) -> None:
    import importlib.util

    path = Path(fr.__file__).resolve().parent.parent / "scripts" / "findings_regression.py"
    spec = importlib.util.spec_from_file_location("findings_regression_script", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.main(["--golden", str(tmp_path), "--out", str(tmp_path / "r")]) == 2
    (tmp_path / "bad.json").write_text("{}")
    assert module.main(["--golden", str(tmp_path), "--out", str(tmp_path / "r")]) == 2
