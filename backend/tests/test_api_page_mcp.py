"""The /api page's MCP section stays in step with the server.

The page used to type its own list of tools and drifted: by 5 Nov 2026 it
named 12 of the 15 the server lists, and said nothing about the order of calls
or the lookup budget the handshake instructions describe. The page now reads
``frontend/src/lib/mcpTools.ts``; this test parses it against ``TOOL_NAMES``
and the handshake's "Cost." paragraph, so a new tool cannot be left off the
page and a tool that starts spending the budget cannot stay unmarked.
"""

from __future__ import annotations

import re
from pathlib import Path

from opencheck.mcp import server as mcp_server
from opencheck.mcp.server import TOOL_NAMES

_TS = Path(__file__).resolve().parents[2] / "frontend" / "src" / "lib" / "mcpTools.ts"


def _tools() -> list[tuple[str, bool]]:
    src = _TS.read_text(encoding="utf-8")
    pattern = re.compile(
        r'name:\s*"(opencheck_[a-z_]+)".*?spendsBudget:\s*(true|false)', re.S
    )
    return [(name, flag == "true") for name, flag in pattern.findall(src)]


def _budget_tools_in_handshake() -> set[str]:
    text = mcp_server._INSTRUCTIONS
    cost = text[text.index("Cost.") :]
    cost = cost[: cost.index("each spend")]
    return set(re.findall(r"opencheck_[a-z_]+", cost))


def test_the_page_names_every_tool_exactly_once() -> None:
    names = [name for name, _ in _tools()]
    assert sorted(names) == sorted(TOOL_NAMES), (
        f"missing from the page: {sorted(set(TOOL_NAMES) - set(names))}; "
        f"not on the server: {sorted(set(names) - set(TOOL_NAMES))}"
    )
    assert len(names) == len(set(names))


def test_the_page_marks_exactly_the_tools_the_handshake_says_spend_the_budget() -> None:
    marked = {name for name, spends in _tools() if spends}
    assert marked == _budget_tools_in_handshake()


def test_the_page_follows_the_handshake_steps_in_order() -> None:
    src = _TS.read_text(encoding="utf-8")
    ids = re.findall(r'^\s{4}id:\s*"([a-z]+)"', src, re.M)
    assert ids == ["find", "expect", "check", "read", "keep", "many"]
    text = mcp_server._INSTRUCTIONS
    for step in ("1. Find", "2. Set expectations", "3. Check", "4. Read it honestly",
                 "5. Keep", "6. Many"):
        assert step in text


def test_the_page_names_the_skills_the_server_serves() -> None:
    from opencheck.mcp.skills import load_skills

    src = _TS.read_text(encoding="utf-8")
    block = src[src.index("MCP_SKILLS: McpSkill[]") :]
    named = re.findall(r'name:\s*"([a-z-]+)"', block)
    assert sorted(named) == sorted(s.name for s in load_skills())


def test_the_page_states_the_protocol_versions_and_task_timings() -> None:
    from opencheck.mcp import descriptor, tasks

    src = _TS.read_text(encoding="utf-8")
    versions = re.search(r"MCP_PROTOCOL_VERSIONS: string\[\] = \[([^\]]*)\]", src)
    assert versions is not None
    assert re.findall(r'"([^"]+)"', versions.group(1)) == descriptor()["protocolVersions"]
    fast = re.search(r"MCP_TASK_FAST_PATH_S = (\d+)", src)
    poll = re.search(r"MCP_TASK_POLL_S = (\d+)", src)
    assert fast and float(fast.group(1)) == tasks.FAST_PATH_S
    assert poll and int(poll.group(1)) * 1000 == tasks.POLL_INTERVAL_MS


def test_the_tools_that_can_become_tasks_are_the_budget_tools() -> None:
    """The page says a Lookup-budget tool can become a task: keep that true."""
    from opencheck.mcp import tasks

    assert {name for name, spends in _tools() if spends} == tasks.TASK_TOOLS
