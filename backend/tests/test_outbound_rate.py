"""Outbound rate limiting: the budget scope has to be OPEN on the real path.

The unit tests for :class:`CallBudget` live next to the ΓΕΜΗ adapter that uses
it, and they passed for twenty-eight phases while the cap was inert in
production — because each one opened its own scope before calling ``fetch()``.
``current_budget()`` returns ``None`` outside a scope and the adapter's
``if budget is not None`` guard then skips the check silently, so a test that
supplies the scope proves the *arithmetic* and nothing about the wiring.

These tests therefore never call ``budget_scope()``. They assert that the
lookup pipeline itself opens a scope, and that the value reaches a task created
after it — the two properties the whole mechanism rests on.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from opencheck import outbound_rate
from opencheck.config import get_settings

#: Shape-valid, mod-97-clean, and not a real registration — the conftest
#: relaxes the check-digit gate, but a fixture LEI that could collide with a
#: live one invites a real network call the day a stub path regresses.
_PIPELINE_LEI = "5493001KJTIIGC8Y1R12"


@pytest.fixture(autouse=True)
def _isolate_scope():
    """No scope before, no scope after — each test starts and leaves clean.

    ``begin()`` sets a ContextVar in the caller's context, which in a test is
    the test function's own. Without this, a test that opens a scope would
    leak it into the next one and a genuinely broken ``begin()`` could still
    look green.
    """
    outbound_rate.end()
    yield
    outbound_rate.end()


def _seed_gleif_bundle(tmp_path, lei: str) -> None:
    """The one entity statement ``_resolve_ctx`` needs to anchor a lookup.

    Borrowed from tests/test_openaleph_check.py, and for the same reason: the
    anchor comes off disk so the pipeline never asks GLEIF for an LEI that has
    no record — tolerable locally, a read timeout in CI.
    """
    target = tmp_path / "cache" / "bods_data" / "gleif" / f"{lei}.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(
            {
                "statementId": "seed-entity",
                "recordType": "entity",
                "recordDetails": {
                    "name": "Budget Scope Fixture Ltd",
                    "jurisdiction": {"code": "GR"},
                    "identifiers": [{"scheme": "XI-LEI", "id": lei}],
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )


def test_no_scope_outside_a_lookup() -> None:
    """The baseline the other tests are measured against.

    A standalone ``fetch()`` — a script, a probe, the weekly sweep — is not
    competing with a fan-out and must stay uncapped, so ``None`` here is the
    correct answer rather than a missing feature.
    """
    assert outbound_rate.current_budget("gemi_greece", 8) is None


def test_begin_is_shared_with_a_task_created_after_it() -> None:
    """One counter per lookup, spent by every concurrent source fetch.

    ``asyncio.create_task`` copies the context at creation, so the tasks see
    the same dict object and the same mutable ``CallBudget`` inside it. This
    is why the budget can be per-lookup rather than per-task, and it is the
    property that makes opening the scope in the pipeline's outermost frame
    sufficient for the fan-out beneath it.
    """

    async def scenario() -> tuple[int | None, int]:
        outbound_rate.begin()

        async def spend() -> None:
            budget = outbound_rate.current_budget("src", 3)
            assert budget is not None, "a child task lost the budget scope"
            budget.take()

        await asyncio.gather(*(asyncio.create_task(spend()) for _ in range(3)))
        parent = outbound_rate.current_budget("src", 3)
        return (parent.spent if parent else None), 3

    spent, tasks = asyncio.run(scenario())
    assert spent == tasks, "the tasks did not share one counter"


def test_end_closes_the_scope() -> None:
    outbound_rate.begin()
    assert outbound_rate.current_budget("src", 2) is not None
    outbound_rate.end()
    assert outbound_rate.current_budget("src", 2) is None


async def test_lookup_pipeline_opens_a_budget_scope(monkeypatch, tmp_path) -> None:
    """⛔ The regression test for Phase 165. Do not let this one open a scope.

    Phase 137 shipped ``CallBudget``, both call sites in the ΓΕΜΗ adapter, and
    a green test — but nothing ever called ``begin()`` on a request, so every
    production lookup ran uncapped and a deep Greek chain queued on the token
    bucket until it hit its 30s source timeout, reporting a timeout instead of
    the honest "ΓΕΜΗ was rate-limited" the cap exists to produce.

    Asserting from the *consumer's* context is deliberate: the pipeline is an
    async generator, so its frame runs in whichever task resumes it, and this
    checks the exact propagation the source tasks depend on. Offline
    throughout — the anchor is seeded on disk and any HTTP request fails by
    name rather than hanging someone else's pull request.
    """
    import httpx

    from opencheck.routers import lookup as lookup_mod

    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "false")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    _seed_gleif_bundle(tmp_path, _PIPELINE_LEI)

    async def _no_network(*args, **kwargs):
        raise AssertionError(
            "the budget-scope test made an HTTP request — this path must stay "
            "offline; see the docstring"
        )

    # Patch `send`, not `build_client`: adapters bind `build_client` at import
    # time, so patching it in its home module intercepts nothing.
    monkeypatch.setattr(httpx.AsyncClient, "send", _no_network)

    assert outbound_rate.current_budget("gemi_greece", 8) is None, (
        "precondition: no scope before the pipeline runs"
    )

    seen: list[bool] = []
    async for _event, _payload in lookup_mod._lookup_pipeline(
        _PIPELINE_LEI, deepen_top=0
    ):
        seen.append(outbound_rate.current_budget("gemi_greece", 8) is not None)

    assert seen, "the pipeline yielded nothing, so it proved nothing"
    assert seen[0], (
        "routers/lookup.py did not open an outbound budget scope — every "
        "per-lookup call budget is inert and rate-capped sources will stall "
        "into a source timeout instead of degrading honestly"
    )


async def test_report_path_opens_a_budget_scope(monkeypatch) -> None:
    """The free-text ``/report`` and ``/export`` path fans out too.

    It runs every adapter's ``search()`` and then deepens the top hits, so a
    rate-capped source can be asked several times in one request there as
    well. Same defect, same fix, separate call site — and separate test,
    because nothing about the streaming pipeline covers it.
    """
    from opencheck.routers import lookup as lookup_mod
    from opencheck.routers import search as search_mod
    from opencheck.sources.base import SearchKind

    observed: dict[str, bool] = {}

    async def fake_run_adapters(q, kind):
        observed["scope_open"] = (
            outbound_rate.current_budget("gemi_greece", 8) is not None
        )
        return {}, {}

    monkeypatch.setattr(search_mod, "_run_adapters", fake_run_adapters)

    await lookup_mod._build_report(
        "Budget Scope Fixture Ltd", SearchKind.ENTITY, 0
    )

    assert observed.get("scope_open"), (
        "_build_report did not open an outbound budget scope before fanning "
        "out across the adapters"
    )
