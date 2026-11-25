"""OpenCheck's skills over MCP (the "Update MCP approaches" ticket, step 2).

Checked through the protocol (``Client(mcp)``), not by calling the handlers:
a skill a client cannot fetch, or a digest that does not match the bytes
``resources/read`` serves, is the failure that matters.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

import mcp.types as types
import pytest
from mcp.client import Client
from mcp.shared.exceptions import MCPError
from pydantic import TypeAdapter

from opencheck.mcp import TOOL_NAMES, descriptor
from opencheck.mcp import server as mcp_server
from opencheck.mcp.skills import (
    EXTENSION_ID,
    MAX_BYTES,
    MAX_FILES,
    SKILLS_DIR,
    GetSkillParams,
    load_skills,
)

_ANY = TypeAdapter(dict[str, Any])
EXPECTED = {
    "opencheck-due-diligence",
    "opencheck-ownership-questions",
    "reading-opencheck-results",
}


async def _list(client: Client) -> dict[str, Any]:
    return await client.session.send_request(
        types.Request(method="skills/list", params={}), _ANY
    )


async def _get(client: Client, uri: str) -> dict[str, Any]:
    return await client.session.send_request(
        types.Request(method="skills/get", params=GetSkillParams(uri=uri)), _ANY
    )


def test_the_three_skills_load_and_name_their_directories() -> None:
    skills = load_skills()
    assert {s.name for s in skills} == EXPECTED
    for s in skills:
        assert s.frontmatter["name"] == s.name
        assert s.frontmatter["description"].strip()
        assert s.files[0].uri == s.uri
        assert len(s.files) <= MAX_FILES
        assert sum(f.size for f in s.files) <= MAX_BYTES


def test_every_tool_a_skill_names_exists() -> None:
    for s in load_skills():
        for f in s.files:
            named = set(re.findall(r"\bopencheck_[a-z_]+\b", f.text))
            assert named <= set(TOOL_NAMES), (f.uri, sorted(named - set(TOOL_NAMES)))


def test_every_link_between_skills_resolves() -> None:
    for s in load_skills():
        for f in s.files:
            for target in re.findall(r"\]\((\.\./[^)]+)\)", f.text):
                assert (SKILLS_DIR / s.name / target).resolve().is_file(), (f.uri, target)


def test_a_skill_whose_directory_disagrees_with_its_name_is_refused(tmp_path: Path) -> None:
    d = tmp_path / "one-name"
    d.mkdir()
    (d / "SKILL.md").write_text("---\nname: another-name\ndescription: x\n---\nBody\n")
    with pytest.raises(ValueError, match="does not match"):
        load_skills(tmp_path)


def test_a_skill_without_a_description_is_refused(tmp_path: Path) -> None:
    d = tmp_path / "bare"
    d.mkdir()
    (d / "SKILL.md").write_text("---\nname: bare\n---\nBody\n")
    with pytest.raises(ValueError, match="no description"):
        load_skills(tmp_path)


def test_the_descriptor_lists_the_skills() -> None:
    assert sorted(descriptor()["skills"]) == sorted(
        f"skill://{n}/SKILL.md" for n in EXPECTED
    )


async def test_a_2026_client_lists_gets_and_reads_every_skill() -> None:
    async with Client(mcp_server.mcp) as client:
        assert client.protocol_version == "2026-07-28"
        caps = client.server_capabilities
        assert caps.extensions[EXTENSION_ID] == {"directoryRead": False}
        assert caps.resources is not None

        listed = await _list(client)
        assert "nextCursor" not in listed
        entries = {e["uri"]: e for e in listed["skills"]}
        assert set(entries) == {f"skill://{n}/SKILL.md" for n in EXPECTED}

        for uri, entry in entries.items():
            got = await _get(client, uri)
            assert got["skill"] == entry
            for item in entry["resources"]:
                read = await client.read_resource(item["uri"])
                body = read.contents[0].text.encode("utf-8")
                assert item["digest"] == "sha256:" + hashlib.sha256(body).hexdigest()
                assert item["size"] == len(body)
                assert read.contents[0].mime_type == "text/markdown"


async def test_an_unknown_skill_or_cursor_is_invalid_params() -> None:
    async with Client(mcp_server.mcp) as client:
        with pytest.raises(MCPError) as err:
            await _get(client, "skill://no-such-skill/SKILL.md")
        assert err.value.error.code == -32602
        with pytest.raises(MCPError) as err:
            await client.session.send_request(
                types.Request(
                    method="skills/list",
                    params=types.PaginatedRequestParams(cursor="page-2"),
                ),
                _ANY,
            )
        assert err.value.error.code == -32602


async def test_a_2025_client_gets_no_skills_methods_but_still_sees_the_files() -> None:
    async with Client(mcp_server.mcp, mode="legacy") as client:
        assert client.protocol_version == "2025-11-25"
        with pytest.raises(MCPError):
            await _list(client)
        uris = {str(r.uri) for r in (await client.list_resources()).resources}
        assert {f"skill://{n}/SKILL.md" for n in EXPECTED} <= uris


def test_the_handshake_points_at_the_skills() -> None:
    for name in EXPECTED:
        assert name in mcp_server._INSTRUCTIONS
