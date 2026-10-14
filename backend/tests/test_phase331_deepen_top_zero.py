"""Phase 331 — ``deepen_top=0`` still knows who the subject is.

Ticket "GET /lookup?deepen_top=0 returns an empty subject_profile" (found on
8 Oct 2026 checking Diamond Bank PLC, ``029200738G7T8AI6H992``): the GLEIF
anchor's own hit was the first pair in ``deepened_bundles`` but not one of
the carve-outs ``_select_deepen_pairs`` deepens past the cap, so at
``deepen_top=0`` a company with no register record had no subject statement.
The profile came back empty — "nothing known", not "not computed" — and the
jurisdiction checks read the subject's country as absent.
"""

from __future__ import annotations

from types import SimpleNamespace

from tests.test_lei_registration import AFPC, afpc_client  # noqa: F401 — fixture

#: The profile keys GLEIF alone can fill for AFPC.
GLEIF_KEYS = (
    "register_status",
    "lei_registration",
    "registered_address",
    "jurisdiction",
)


def test_the_anchor_is_deepened_past_a_zero_cap() -> None:
    from opencheck.routers.lookup import _select_deepen_pairs

    ctx = SimpleNamespace(lei="L" * 20, derived={})
    bundles = [("gleif", "L" * 20), ("wikirate", "w"), ("openaleph", "a")]
    assert _select_deepen_pairs(bundles, 0, ctx) == [("gleif", "L" * 20)]
    # Only the subject's own GLEIF record: another LEI's GLEIF pair (none is
    # dispatched today) is not the anchor and keeps to the cap.
    assert ("gleif", "OTHER") not in _select_deepen_pairs(
        [*bundles, ("gleif", "OTHER")], 0, ctx
    )


def test_deepen_top_zero_returns_the_gleif_profile(afpc_client) -> None:  # noqa: F811
    zero = afpc_client.get("/lookup", params={"lei": AFPC, "deepen_top": 0}).json()
    one = afpc_client.get("/lookup", params={"lei": AFPC, "deepen_top": 1}).json()

    profile = zero["subject_profile"]
    assert profile, "deepen_top=0 must not return an empty profile"
    for key in GLEIF_KEYS:
        assert profile.get(key) is not None, key
        assert profile[key] == one["subject_profile"][key], key
    assert profile["lei_registration"]["status"] == "LAPSED"
    assert profile["register_status"]["sources"] == ["gleif"]


def test_deepen_top_zero_gives_the_risk_engine_the_subject(afpc_client) -> None:  # noqa: F811
    """The jurisdiction checks (FATF, EU high-risk, non-EU) read entity
    statements' ``jurisdiction``; with no subject statement they had nothing
    to read, so the screen came back clean without having run."""
    zero = afpc_client.get("/lookup", params={"lei": AFPC, "deepen_top": 0}).json()
    one = afpc_client.get("/lookup", params={"lei": AFPC, "deepen_top": 1}).json()

    subject = [
        s for s in zero["bods"]
        if s.get("recordType") == "entity"
        and any(i.get("id") == AFPC for i in s["recordDetails"].get("identifiers", []))
    ]
    assert subject, "the anchor's entity statement must reach the bundle"
    assert subject[0]["recordDetails"]["jurisdiction"]["code"] == "US-DC"
    assert {s["code"] for s in zero["risk_signals"]} == {
        s["code"] for s in one["risk_signals"]
    }
