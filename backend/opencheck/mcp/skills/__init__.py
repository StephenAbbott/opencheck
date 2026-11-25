"""OpenCheck's skills, served over MCP (``io.modelcontextprotocol/skills``).

Second step of the "Update MCP approaches" ticket (25 Nov 2026). The
handshake instructions say *which tool, in which order*; a skill says *how
to read and report what comes back*, at the length that takes, and a client
loads it only when the task needs it. Three skills live beside this file,
one directory each, ``<name>/SKILL.md``:

- ``opencheck-due-diligence``: from a name or number to a saved report.
- ``reading-opencheck-results``: degraded sources, risk versus context, name
  matches, per-source figures, licences.
- ``opencheck-ownership-questions``: which ownership tool answers which
  question, and the words to use.

The extension (SEP-2640) adds two methods on protocol revision 2026-07-28,
``skills/list`` and ``skills/get``, and serves every file as a ``skill://``
resource through the ordinary ``resources/read``. Each entry carries the
skill's frontmatter and a manifest naming every file with its SHA-256 and
size, so a client can check what it read. The files are read once, at
import: the digests are of the exact bytes ``resources/read`` serves, and
every instance of the server answers the same.

A client on 2025-11-25, or one that does not read skills, still sees the
files as resources and still gets the handshake instructions, which keep the
call order and the cost rules on their own.

Rules, checked here at import and pinned by ``tests/test_mcp_skills.py``:
the directory name is the frontmatter ``name``; frontmatter has ``name`` and
``description``; at most 512 files and 16 MiB per skill (the SEP's limits);
every ``opencheck_*`` a skill names is a real tool.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]
from mcp.server.context import ServerRequestContext
from mcp.server.extension import Extension, MethodBinding, ResourceBinding
from mcp.server.mcpserver.resources import TextResource
from mcp.shared.exceptions import MCPError
from mcp_types import INVALID_PARAMS
from mcp_types._types import PaginatedRequestParams, RequestParams

EXTENSION_ID = "io.modelcontextprotocol/skills"

#: The extension's methods exist on this revision only (the SDK answers
#: METHOD_NOT_FOUND on any other).
PROTOCOL_VERSIONS = frozenset({"2026-07-28"})

SKILLS_DIR = Path(__file__).parent

#: How long a client may keep a list or get answer. The files change only on
#: deploy; five minutes is the SEP's example and keeps an edit from hiding
#: long. ``public``: the answer is the same for every caller.
TTL_MS = 5 * 60 * 1000
CACHE_SCOPE = "public"

MAX_FILES = 512
MAX_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True)
class SkillFile:
    uri: str
    text: str
    digest: str
    size: int


@dataclass(frozen=True)
class Skill:
    name: str
    frontmatter: dict[str, Any]
    files: tuple[SkillFile, ...]

    @property
    def uri(self) -> str:
        return f"skill://{self.name}/SKILL.md"

    def entry(self) -> dict[str, Any]:
        """The SEP's skill entry: uri, frontmatter unchanged, full manifest."""
        return {
            "uri": self.uri,
            "frontmatter": self.frontmatter,
            "resources": [
                {"uri": f.uri, "digest": f.digest, "size": f.size} for f in self.files
            ],
        }


def parse_frontmatter(text: str) -> dict[str, Any]:
    """The YAML block between the opening ``---`` lines, as a dict."""
    if not text.startswith("---\n"):
        raise ValueError("SKILL.md must open with a --- frontmatter block")
    end = text.find("\n---\n", 4)
    if end < 0:
        raise ValueError("SKILL.md frontmatter block is not closed")
    data = yaml.safe_load(text[4:end])
    if not isinstance(data, dict):
        raise ValueError("SKILL.md frontmatter is not a mapping")
    return data


def _read_skill(directory: Path) -> Skill:
    name = directory.name
    paths = sorted(p for p in directory.rglob("*") if p.is_file())
    if len(paths) > MAX_FILES:
        raise ValueError(f"skill {name} has {len(paths)} files, over {MAX_FILES}")
    files: list[SkillFile] = []
    total = 0
    frontmatter: dict[str, Any] | None = None
    for path in paths:
        raw = path.read_bytes()
        text = raw.decode("utf-8")
        total += len(raw)
        rel = path.relative_to(directory).as_posix()
        files.append(
            SkillFile(
                uri=f"skill://{name}/{rel}",
                text=text,
                digest="sha256:" + hashlib.sha256(raw).hexdigest(),
                size=len(raw),
            )
        )
        if rel == "SKILL.md":
            frontmatter = parse_frontmatter(text)
    if total > MAX_BYTES:
        raise ValueError(f"skill {name} is {total} bytes, over {MAX_BYTES}")
    if frontmatter is None:
        raise ValueError(f"skill {name} has no SKILL.md")
    if frontmatter.get("name") != name:
        raise ValueError(
            f"skill directory {name} does not match frontmatter name {frontmatter.get('name')!r}"
        )
    if not str(frontmatter.get("description") or "").strip():
        raise ValueError(f"skill {name} has no description")
    # SKILL.md first, then the rest in path order: the manifest reads the way
    # a client should.
    files.sort(key=lambda f: (not f.uri.endswith(f"{name}/SKILL.md"), f.uri))
    return Skill(name=name, frontmatter=frontmatter, files=tuple(files))


def load_skills(root: Path = SKILLS_DIR) -> tuple[Skill, ...]:
    """Every ``<root>/<name>/SKILL.md`` skill, in name order."""
    return tuple(
        _read_skill(d)
        for d in sorted(root.iterdir())
        if d.is_dir() and (d / "SKILL.md").is_file()
    )


class GetSkillParams(RequestParams):
    uri: str


class Skills(Extension):
    """Serve the skills in ``SKILLS_DIR`` to clients that ask for them."""

    identifier = EXTENSION_ID

    def __init__(self, skills: tuple[Skill, ...] | None = None) -> None:
        self._skills = load_skills() if skills is None else skills
        self._by_uri = {s.uri: s for s in self._skills}

    @property
    def skills(self) -> tuple[Skill, ...]:
        return self._skills

    def settings(self) -> dict[str, Any]:
        # No resources/directory/read: every file is in the manifest.
        return {"directoryRead": False}

    def resources(self) -> list[ResourceBinding]:
        return [
            ResourceBinding(
                resource=TextResource(
                    uri=f.uri,
                    name=f.uri.removeprefix("skill://"),
                    description=s.frontmatter["description"]
                    if f.uri == s.uri
                    else None,
                    mime_type="text/markdown",
                    text=f.text,
                )
            )
            for s in self._skills
            for f in s.files
        ]

    def methods(self) -> list[MethodBinding]:
        return [
            MethodBinding(
                "skills/list",
                PaginatedRequestParams,
                self._list,
                protocol_versions=PROTOCOL_VERSIONS,
            ),
            MethodBinding(
                "skills/get",
                GetSkillParams,
                self._get,
                protocol_versions=PROTOCOL_VERSIONS,
            ),
        ]

    async def _list(
        self, ctx: ServerRequestContext[Any, Any], params: PaginatedRequestParams
    ) -> dict[str, Any]:
        # Three skills: one page, so a cursor is never issued and none is
        # accepted.
        if params.cursor:
            raise MCPError(code=INVALID_PARAMS, message="Unknown cursor")
        return {
            "skills": [s.entry() for s in self._skills],
            "ttlMs": TTL_MS,
            "cacheScope": CACHE_SCOPE,
        }

    async def _get(
        self, ctx: ServerRequestContext[Any, Any], params: GetSkillParams
    ) -> dict[str, Any]:
        skill = self._by_uri.get(params.uri)
        if skill is None:
            raise MCPError(code=INVALID_PARAMS, message=f"Unknown skill: {params.uri}")
        return {"skill": skill.entry(), "ttlMs": TTL_MS, "cacheScope": CACHE_SCOPE}
