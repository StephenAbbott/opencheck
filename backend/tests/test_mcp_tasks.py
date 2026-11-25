"""MCP tasks for the slow tools (the "Update MCP approaches" ticket, step 3).

Over HTTP, behind the rate guard and the client middleware, as ``app.py``
assembles them: the task answer, polling, cancelling, the fast path, a client
that did not declare tasks, the poll tier and the caller the work is charged
to.
"""

from __future__ import annotations

import asyncio
import json
import time
from contextlib import asynccontextmanager
from typing import Any

import pytest
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.testclient import TestClient

from opencheck import lookup_budget
from opencheck.config import get_settings
from opencheck.lookup_replay import _IN_FLIGHT, _Flight
from opencheck.mcp import tasks
from opencheck.mcp.tasks import EXTENSION_ID, status_message

LEI = "529900T8BM49AURSDO55"
IP = "198.51.100.60"


def _meta(declare: bool = True) -> dict[str, Any]:
    return {
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {
            "extensions": {EXTENSION_ID: {}} if declare else {}
        },
        "io.modelcontextprotocol/clientInfo": {"name": "test", "version": "1"},
    }


@pytest.fixture
def mcp_http(monkeypatch):
    """A fresh MCP app with ``opencheck_lookup`` replaced by a slow fake."""
    from opencheck.mcp import asgi_app, server
    from opencheck.mcp.guard import guard_routes

    gate = {"release": None, "calls": [], "seconds": 1.0}

    async def _slow(**kw: Any) -> dict[str, Any]:
        gate["calls"].append(lookup_budget.current_client())
        await asyncio.sleep(gate["seconds"])
        return {"ok": True, "lei": kw.get("lei")}

    async def _fast(**kw: Any) -> dict[str, Any]:
        return {"fast": True}

    monkeypatch.setattr(server.mcp._tool_manager._tools["opencheck_lookup"], "fn", _slow)
    monkeypatch.setattr(server.mcp._tool_manager._tools["opencheck_list_sources"], "fn", _fast)
    monkeypatch.setattr(server.mcp._lowlevel_server, "_session_manager", None)
    monkeypatch.setattr(tasks, "FAST_PATH_S", 0.2)
    tasks.reset()
    mcp_app = asgi_app()
    manager = server.mcp.session_manager

    @asynccontextmanager
    async def _lifespan(_app):
        async with manager.run():
            yield

    app = Starlette(
        routes=guard_routes(list(mcp_app.routes)),
        middleware=[Middleware(lookup_budget.ClientScopeMiddleware)],
        lifespan=_lifespan,
    )
    with TestClient(app) as client:
        yield client, gate
    tasks.reset()


def _post(client: TestClient, method: str, params: dict[str, Any], rid: int = 1, name: str | None = None):
    headers = {
        "accept": "application/json, text/event-stream",
        "content-type": "application/json",
        "mcp-protocol-version": "2026-07-28",
        "mcp-method": method,
        "x-forwarded-for": IP,
    }
    if name:
        headers["mcp-name"] = name
    return client.post(
        "/mcp", headers=headers, json={"jsonrpc": "2.0", "id": rid, "method": method, "params": params}
    )


def _call(client: TestClient, tool: str, declare: bool = True, rid: int = 1):
    args = {"lei": LEI} if tool == "opencheck_lookup" else {}
    return _post(
        client, "tools/call", {"_meta": _meta(declare), "name": tool, "arguments": args}, rid, tool
    )


def _task(client: TestClient, method: str, task_id: str, rid: int = 2):
    return _post(client, method, {"_meta": _meta(), "taskId": task_id}, rid, task_id)


def _until(client: TestClient, task_id: str, status: str, limit_s: float = 5.0) -> dict[str, Any]:
    deadline = time.monotonic() + limit_s
    while True:
        out = _task(client, "tasks/get", task_id).json()["result"]
        if out["status"] == status or time.monotonic() > deadline:
            return out
        time.sleep(0.05)


def test_a_slow_call_becomes_a_task_and_its_result_is_polled(mcp_http) -> None:
    client, gate = mcp_http
    answer = _call(client, "opencheck_lookup").json()["result"]
    assert answer["resultType"] == "task"
    assert answer["status"] == "working"
    assert answer["pollIntervalMs"] == tasks.POLL_INTERVAL_MS
    assert answer["ttlMs"] == tasks.TTL_MS
    assert answer["statusMessage"]
    assert len(answer["taskId"]) <= 128 and " " not in answer["taskId"]
    for key in ("createdAt", "lastUpdatedAt"):
        assert answer[key].endswith("Z")

    working = _task(client, "tasks/get", answer["taskId"]).json()["result"]
    assert working["status"] == "working" and working["resultType"] == "complete"

    done = _until(client, answer["taskId"], "completed")
    assert done["status"] == "completed"
    assert done["result"]["structuredContent"] == {"ok": True, "lei": LEI}
    assert done["result"]["isError"] is False
    assert "pollIntervalMs" not in done
    # The tool ran once, as its caller: the work is charged to that address.
    assert gate["calls"] == [IP]


def test_a_call_that_finishes_fast_is_answered_plainly(mcp_http) -> None:
    client, gate = mcp_http
    gate["seconds"] = 0.0
    answer = _call(client, "opencheck_lookup").json()["result"]
    assert answer["resultType"] == "complete"
    assert answer["structuredContent"] == {"ok": True, "lei": LEI}
    assert tasks._TASKS == {}


def test_a_client_that_did_not_declare_tasks_waits_for_the_result(mcp_http) -> None:
    client, _ = mcp_http
    answer = _call(client, "opencheck_lookup", declare=False).json()["result"]
    assert answer["resultType"] == "complete"
    assert answer["structuredContent"]["ok"] is True
    assert tasks._TASKS == {}


def test_a_quick_tool_is_never_a_task(mcp_http) -> None:
    client, _ = mcp_http
    assert "opencheck_list_sources" not in tasks.TASK_TOOLS
    answer = _call(client, "opencheck_list_sources").json()["result"]
    assert answer["resultType"] == "complete"


def test_a_cancelled_task_says_so(mcp_http) -> None:
    client, gate = mcp_http
    gate["seconds"] = 30.0
    task_id = _call(client, "opencheck_lookup").json()["result"]["taskId"]
    cancelled = _task(client, "tasks/cancel", task_id).json()["result"]
    assert cancelled["status"] == "cancelled"
    assert _task(client, "tasks/get", task_id).json()["result"]["status"] == "cancelled"


def test_an_unknown_task_is_invalid_params(mcp_http) -> None:
    client, _ = mcp_http
    response = _task(client, "tasks/get", "no-such-task")
    assert response.json()["error"]["code"] == -32602


def test_tasks_update_is_acknowledged(mcp_http) -> None:
    client, gate = mcp_http
    gate["seconds"] = 30.0
    task_id = _call(client, "opencheck_lookup").json()["result"]["taskId"]
    response = _post(
        client,
        "tasks/update",
        {"_meta": _meta(), "taskId": task_id, "inputResponses": {}},
        name=task_id,
    )
    assert "error" not in response.json()


def test_the_extension_is_advertised(mcp_http) -> None:
    client, _ = mcp_http
    found = _post(client, "server/discover", {"_meta": _meta()}).json()["result"]
    assert EXTENSION_ID in found["capabilities"]["extensions"]


def test_polls_have_their_own_tier(mcp_http, monkeypatch) -> None:
    """A default tier of one request a minute still lets a task be polled."""
    client, gate = mcp_http
    monkeypatch.setenv("OPENCHECK_RATE_LIMIT_ENABLED", "1")
    monkeypatch.setenv("OPENCHECK_RATE_LIMIT_DEFAULT", "1/minute")
    get_settings.cache_clear()
    lookup_budget.reset_for_tests()
    try:
        task_id = _call(client, "opencheck_lookup").json()["result"]["taskId"]
        for _ in range(3):
            assert _task(client, "tasks/get", task_id).status_code == 200
        # … while a second ordinary request is refused.
        assert _call(client, "opencheck_list_sources", rid=9).status_code == 429
    finally:
        get_settings.cache_clear()
        lookup_budget.reset_for_tests()


def test_the_status_message_reads_the_lookup_run(monkeypatch) -> None:
    async def scenario() -> None:
        flight = _Flight()
        monkeypatch.setitem(_IN_FLIGHT, f"{LEI}:5", flight)
        args = {"lei": LEI.lower()}
        assert status_message("opencheck_lookup", args) == "Resolving the company."
        flight.events.append(("queued", {"position": 2}))
        assert status_message("opencheck_lookup", args) == "Waiting for a free slot: 2 checks ahead."
        flight.events.append(("sources_applicable", {"source_ids": ["a", "b", "c"]}))
        flight.events.append(("source_completed", {"source_id": "a"}))
        flight.events.append(("source_error", {"source_id": "b"}))
        assert status_message("opencheck_lookup", args) == "Checking sources: 2 of 3 answered."

    asyncio.run(scenario())
    assert status_message("opencheck_batch_lookup", {"leis": ["x", "y"]}) == "Screening 2 companies."
    assert status_message("opencheck_register_lookup", {"scheme": "GB-COH"}) == "Running the check."


def test_every_task_tool_exists_and_spends_the_lookup_budget() -> None:
    from opencheck.mcp import TOOL_NAMES
    from opencheck.mcp.guard import TOOL_TIERS

    assert set(TOOL_NAMES) >= tasks.TASK_TOOLS
    assert set(TOOL_TIERS) >= tasks.TASK_TOOLS


def test_a_finished_task_is_dropped_after_its_ttl(monkeypatch) -> None:
    task = tasks._Task(id="t1", tool="opencheck_lookup", arguments={}, version="2026-07-28")
    task.mark("completed")
    monkeypatch.setitem(tasks._TASKS, "t1", task)
    task.touched -= tasks.TTL_MS / 1000 + 1
    tasks._expire()
    assert "t1" not in tasks._TASKS
    json.dumps(task.view())
