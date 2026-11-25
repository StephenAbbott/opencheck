"""MCP tasks for the slow tools (``io.modelcontextprotocol/tasks``).

Third step of the "Update MCP approaches" ticket (25 Nov 2026). A full
lookup can take longer than a client will hold a request open: Claude Code
wants a first byte within 60 seconds and drops a call after five idle
minutes. With tasks, the server answers a slow ``tools/call`` at once with a
task, keeps working, and the client polls ``tasks/get`` until the result is
there. A client that did not declare the extension never sees a task: its
call blocks as it always has.

How it works here:

- **Only the tools that run a pipeline** (``TASK_TOOLS``: the tools the
  handshake says spend the lookup budget). Every other tool answers in
  well under a second and never becomes a task.
- **Fast calls stay plain.** The call starts at once; if it finishes within
  ``FAST_PATH_S`` (a replayed lookup, say) its result is the answer, with no
  task. Only a call still running then is handed back as a task.
- **The work runs once.** The tool call keeps running in the background and
  is what the task reports; a poll runs nothing and charges no lookup
  budget. A lookup task follows the same detached run (``lookup_replay``) a
  browser opening the same company would join, and cancelling the task
  stops only the task's wait, never a run someone else may be following.
- **The status message is the run's progress**, read from that run's
  events: "Waiting for a free slot", "Resolving the company", "Checking
  sources: 9 of 13 answered".
- **In memory, on the one instance.** OpenCheck runs one Render instance;
  a deploy loses the tasks in flight, and a poll for one is answered
  -32602, which Claude Code reads as "outcome unknown" after retrying for
  five minutes. A finished task is kept ``TTL_MS`` after its last change;
  at most ``MAX_TASKS`` are held, and past that a call simply blocks.
- **Task ids are capabilities**: 32 random bytes, URL-safe. Anyone holding
  one can read the result, as anyone holding a saved-report id can; a task
  holds nothing the caller could not have asked for directly.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from mcp.server.context import CallNext, HandlerResult, ServerRequestContext
from mcp.server.extension import Extension, MethodBinding
from mcp.shared.exceptions import MCPError
from mcp_types import INVALID_PARAMS, CallToolRequestParams
from mcp_types import methods as _methods
from mcp_types._types import RequestParams
from pydantic import BaseModel, ConfigDict

from ..secret_scrub import describe_exception

logger = logging.getLogger(__name__)

EXTENSION_ID = "io.modelcontextprotocol/tasks"
PROTOCOL_VERSIONS = frozenset({"2026-07-28"})

#: The tools that run a lookup pipeline and so may outlast a request.
TASK_TOOLS = frozenset(
    {
        "opencheck_lookup",
        "opencheck_register_lookup",
        "opencheck_export_bods",
        "opencheck_owners",
        "opencheck_ownership_map",
        "opencheck_batch_lookup",
        "opencheck_save_report",
        "opencheck_discrepancy_note",
    }
)

#: A call that finishes inside this is answered plainly, with no task.
FAST_PATH_S = 3.0
#: How often a client should poll. A lookup takes tens of seconds; five
#: seconds keeps the poll count low (and inside the MCP poll tier).
POLL_INTERVAL_MS = 5_000
#: How long a task is kept after its last change.
TTL_MS = 60 * 60 * 1000
MAX_TASKS = 500

TERMINAL = frozenset({"completed", "failed", "cancelled"})


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass
class _Task:
    id: str
    tool: str
    arguments: dict[str, Any]
    version: str
    created_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)
    touched: float = field(default_factory=time.monotonic)
    status: str = "working"
    result: dict[str, Any] | None = None
    error: str | None = None
    runner: asyncio.Task[Any] | None = None

    def mark(self, status: str) -> None:
        self.status = status
        self.updated_at = _now_iso()
        self.touched = time.monotonic()

    def view(self) -> dict[str, Any]:
        """The task as the extension's wire shape."""
        out: dict[str, Any] = {
            "taskId": self.id,
            "status": self.status,
            "createdAt": self.created_at,
            "lastUpdatedAt": self.updated_at,
            "ttlMs": TTL_MS,
        }
        if self.status == "working":
            out["pollIntervalMs"] = POLL_INTERVAL_MS
            out["statusMessage"] = status_message(self.tool, self.arguments)
        elif self.status == "completed" and self.result is not None:
            out["result"] = self.result
        elif self.status == "failed":
            out["error"] = {"message": self.error or "The tool call failed."}
        return out


_TASKS: dict[str, _Task] = {}


def reset() -> None:
    """Drop every task (tests)."""
    for task in _TASKS.values():
        if task.runner is not None and not task.runner.done():
            task.runner.cancel()
    _TASKS.clear()


def _expire() -> None:
    cutoff = time.monotonic() - TTL_MS / 1000
    for tid in [t.id for t in _TASKS.values() if t.status in TERMINAL and t.touched < cutoff]:
        del _TASKS[tid]


def client_declared(ctx: ServerRequestContext[Any, Any]) -> bool:
    capabilities = ctx.session.client_capabilities
    declared = capabilities.extensions if capabilities else None
    return declared is not None and EXTENSION_ID in declared


# --- progress ---------------------------------------------------------------


def _lookup_progress(lei: str) -> str | None:
    from ..lookup_replay import _IN_FLIGHT, _subject_key

    prefix = _subject_key(lei) + ":"
    flight = next((f for k, f in _IN_FLIGHT.items() if k.startswith(prefix)), None)
    if flight is None:
        return None
    applicable: set[str] = set()
    answered: set[str] = set()
    queued: dict[str, Any] | None = None
    started = False
    for event, payload in flight.events:
        if event == "queued":
            queued = payload
        elif event == "sources_applicable":
            applicable = set(payload.get("source_ids") or [])
        elif event == "source_started":
            started = True
        elif event in ("source_completed", "source_error"):
            started = True
            if payload.get("source_id"):
                answered.add(payload["source_id"])
    if applicable:
        n = len(applicable & answered)
        return f"Checking sources: {n} of {len(applicable)} answered."
    if queued is not None and not started:
        ahead = queued.get("position")
        if isinstance(ahead, int) and ahead > 0:
            return f"Waiting for a free slot: {ahead} checks ahead."
        return "Waiting for a free slot."
    return "Resolving the company."


def status_message(tool: str, arguments: dict[str, Any]) -> str:
    """One sentence on where the call stands, for ``statusMessage``."""
    lei = arguments.get("lei")
    if isinstance(lei, str) and lei.strip():
        message = _lookup_progress(lei)
        if message is not None:
            return message
    if tool == "opencheck_batch_lookup":
        n = len(arguments.get("leis") or [])
        return f"Screening {n} companies."
    return "Running the check."


# --- the extension ----------------------------------------------------------


class TaskIdParams(RequestParams):
    task_id: str  # wire name ``taskId`` (the base model is camel-cased)


class UpdateParams(TaskIdParams):
    model_config = ConfigDict(extra="allow")


def _wire_result(version: str, result: HandlerResult) -> dict[str, Any]:
    dumped: dict[str, Any]
    if result is None:
        dumped = {}
    elif isinstance(result, BaseModel):
        dumped = result.model_dump(by_alias=True, mode="json", exclude_none=True)
    else:
        dumped = dict(result)
    dumped = _methods.serialize_server_result("tools/call", version, dumped)
    dumped.setdefault("resultType", "complete")
    return dumped


class Tasks(Extension):
    """Hand slow tool calls back as tasks to clients that declared tasks."""

    identifier = EXTENSION_ID

    def methods(self) -> list[MethodBinding]:
        return [
            MethodBinding("tasks/get", TaskIdParams, self._get, protocol_versions=PROTOCOL_VERSIONS),
            MethodBinding("tasks/cancel", TaskIdParams, self._cancel, protocol_versions=PROTOCOL_VERSIONS),
            MethodBinding("tasks/update", UpdateParams, self._update, protocol_versions=PROTOCOL_VERSIONS),
        ]

    async def intercept_tool_call(
        self,
        params: CallToolRequestParams,
        ctx: ServerRequestContext[Any, Any],
        call_next: CallNext,
    ) -> HandlerResult:
        if (
            params.name not in TASK_TOOLS
            or ctx.protocol_version not in PROTOCOL_VERSIONS
            or not client_declared(ctx)
        ):
            return await call_next(ctx)
        _expire()
        if len(_TASKS) >= MAX_TASKS:
            return await call_next(ctx)

        runner = asyncio.ensure_future(call_next(ctx))
        done, _ = await asyncio.wait({runner}, timeout=FAST_PATH_S)
        if runner in done:
            return runner.result()

        task = _Task(
            id=secrets.token_urlsafe(32),
            tool=params.name,
            arguments=dict(params.arguments or {}),
            version=ctx.protocol_version,
            runner=runner,
        )
        _TASKS[task.id] = task
        runner.add_done_callback(lambda fut: self._settle(task, fut))
        return {"resultType": "task", **task.view()}

    @staticmethod
    def _settle(task: _Task, fut: asyncio.Future[Any]) -> None:
        if task.status in TERMINAL:
            return
        if fut.cancelled():
            task.mark("cancelled")
            return
        exc = fut.exception()
        if exc is not None:
            if isinstance(exc, MCPError):
                task.error = exc.error.message
            else:
                logger.warning("MCP task for %s failed: %s", task.tool, describe_exception(exc))
                task.error = describe_exception(exc)
            task.mark("failed")
            return
        try:
            task.result = _wire_result(task.version, fut.result())
        except Exception as err:  # a malformed result is a server bug
            logger.exception("MCP task for %s returned an invalid result", task.tool)
            task.error = describe_exception(err)
            task.mark("failed")
            return
        task.mark("completed")

    @staticmethod
    def _find(task_id: str) -> _Task:
        _expire()
        task = _TASKS.get(task_id)
        if task is None:
            raise MCPError(code=INVALID_PARAMS, message="Unknown task id")
        return task

    async def _get(self, ctx: ServerRequestContext[Any, Any], params: TaskIdParams) -> dict[str, Any]:
        return self._find(params.task_id).view()

    async def _cancel(self, ctx: ServerRequestContext[Any, Any], params: TaskIdParams) -> dict[str, Any]:
        task = self._find(params.task_id)
        if task.status not in TERMINAL:
            # Cancels the task's wait only: a lookup run is detached and
            # finishes into the replay cache for anyone else following it.
            if task.runner is not None:
                task.runner.cancel()
            task.mark("cancelled")
        return task.view()

    async def _update(self, ctx: ServerRequestContext[Any, Any], params: UpdateParams) -> dict[str, Any]:
        # OpenCheck's tasks never ask for input; acknowledge, as the spec asks.
        self._find(params.task_id)
        return {}
