"""Routes for saved reports (Phase 216): ``/saved-reports``.

A saved report is the record of exactly what OpenCheck showed for one LEI on
one date — see ``opencheck/saved_reports.py`` for what is frozen, why it is
copied from the server's own replay cache rather than posted by a client, and
how the content hash is verified.

- ``POST /saved-reports``                   save a held run (heavy tier; bot gate)
- ``GET /saved-reports/{id}``               the report: metadata + the frozen payload
- ``GET /saved-reports/{id}.json``          the exact bytes ``content_hash`` covers
- ``POST /saved-reports/{id}/extend``       keep it for another retention period
- ``DELETE /saved-reports/{id}``            delete it

Extend and delete need the manage token, sent as ``X-OpenCheck-Manage-Token``.
Every success carries ``X-Robots-Tag: noindex`` and ``/saved-reports`` is
disallowed in robots.txt: a saved report is shared by link, never indexed.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from .. import saved_reports as sr
from ..config import get_settings
from ..memwatch import is_bot
from ..ratelimit import default_tier, heavy_tier, limiter

router = APIRouter(tags=["saved-reports"])

NOINDEX = {"X-Robots-Tag": "noindex, nofollow"}
MANAGE_HEADER = "X-OpenCheck-Manage-Token"


class SaveRequest(BaseModel):
    """Names a run the server holds. There is deliberately no field for a
    payload — ``extra="forbid"`` refuses one — so a saved report is always the
    server's own copy."""

    model_config = ConfigDict(extra="forbid")

    lei: str = Field(..., min_length=20, max_length=40)
    run_completed_at: str = Field(
        ...,
        max_length=64,
        description="The run_completed_at the run's `done` event carried.",
    )
    deepen_top: int = Field(5, ge=0, le=10)
    narrative_run_id: str | None = Field(
        default=None,
        max_length=64,
        description="Also save the narrative this server generated from the same run.",
    )


def _store() -> sr.SavedReportsStore:
    store = sr.get_store()
    if store is None:
        raise HTTPException(
            status_code=503,
            detail="Saved reports are not enabled on this instance (OPENCHECK_SAVED_REPORTS_DB_FILE is unset).",
        )
    return store


def _refuse(exc: sr.SavedReportError) -> HTTPException:
    return HTTPException(
        status_code=exc.status,
        detail=str(exc),
        headers={"X-OpenCheck-Refusal": exc.code},
    )


def _refuse_bots(request: Request) -> None:
    if get_settings().bot_gate_lookup_stream and is_bot(request.headers.get("user-agent")):
        raise HTTPException(
            status_code=403,
            detail=(
                "/saved-reports serves the interactive OpenCheck app and is disallowed for "
                "automated clients (see /robots.txt)."
            ),
        )


def _frontend() -> str:
    frontend = (get_settings().frontend_origin or "").rstrip("/")
    return frontend if frontend.startswith("http") else "https://opencheck.world"


def _with_url(meta: dict[str, Any]) -> dict[str, Any]:
    return {**meta, "url": f"{_frontend()}{meta['report_path']}"}


def _api_base() -> str:
    return (get_settings().public_api_base or "https://api.opencheck.world").rstrip("/")


@dataclass(frozen=True)
class SavedForExport:
    """A saved report opened for rendering (Phase 218): the folded response the
    PDF/Markdown/format exports read, and the ``saved`` block that marks them."""

    meta: dict[str, Any]
    payload: dict[str, Any]
    response: Any  # LookupResponse
    saved: dict[str, Any]

    @property
    def narrative(self) -> dict[str, Any] | None:
        return self.payload.get("narrative")

    @property
    def dispositions(self) -> dict[str, Any] | None:
        return self.payload.get("dispositions")

    @property
    def stamp(self) -> str:
        """YYYYMMDD of the save — a download's name never reads today's clock."""
        return str(self.meta["saved_at"])[:10].replace("-", "")


async def open_for_export(report_id: str, lei: str | None = None) -> SavedForExport:
    """Load, verify and fold a saved report. HTTP errors carry the saved-report
    refusal codes; a ``lei`` that names another company is a 400."""
    from .lookup import fold_lookup_events

    store = _store()
    try:
        meta, data = await asyncio.to_thread(sr.load_report, store, report_id)
    except sr.SavedReportError as exc:
        raise _refuse(exc) from exc
    if lei is not None and lei.strip().upper() != meta["lei"]:
        raise HTTPException(
            status_code=400,
            detail=f"Saved report {report_id} is for {meta['lei']}, not {lei.strip().upper()}.",
        )
    payload = json.loads(data)
    response = fold_lookup_events(meta["lei"], sr.deserialise_events(payload.get("events") or []))
    saved = {
        "report_id": meta["report_id"],
        "content_hash": meta["content_hash"],
        "saved_at": payload.get("saved_at") or meta["saved_at"],
        "run_completed_at": payload.get("run_completed_at") or "",
        "report_url": f"{_frontend()}{meta['report_path']}",
        "json_url": f"{_api_base()}/saved-reports/{meta['report_id']}.json",
        "licensing": payload.get("licensing"),
    }
    return SavedForExport(meta=meta, payload=payload, response=response, saved=saved)


# Every handler under @limiter.limit that returns a dict takes
# ``response: Response`` — slowapi writes its headers through it and 500s
# without it (PR #280). tests/test_saved_reports.py runs every route with the
# limiter on to pin that.
@router.post("/saved-reports", status_code=201)
@limiter.limit(heavy_tier)
async def save_report(request: Request, response: Response, body: SaveRequest) -> dict[str, Any]:
    """Save the run the server holds for ``lei`` that completed at
    ``run_completed_at``. 409 when that run is no longer held — the reader is
    told to run the check again; nothing is silently re-run."""
    _refuse_bots(request)
    store = _store()
    try:
        out = await asyncio.to_thread(
            sr.save_from_replay,
            store,
            lei=body.lei,
            run_completed_at=body.run_completed_at,
            deepen_top=body.deepen_top,
            narrative_run_id=body.narrative_run_id,
        )
    except sr.SavedReportError as exc:
        raise _refuse(exc) from exc
    response.headers.update(NOINDEX)
    return _with_url(out)


# Defined before ``/saved-reports/{report_id}``, which would otherwise match
# ``<id>.json`` — route order inside a router is definition order.
@router.get("/saved-reports/{report_id}.json")
@limiter.limit(default_tier)
async def saved_report_json(request: Request, report_id: str) -> Response:
    """The canonical payload bytes. ``shasum -a 256`` on this download equals
    ``content_hash``."""
    store = _store()
    try:
        meta, data = await asyncio.to_thread(sr.load_report, store, report_id)
    except sr.SavedReportError as exc:
        raise _refuse(exc) from exc
    return Response(
        content=data,
        media_type="application/json",
        headers={
            **NOINDEX,
            "X-OpenCheck-Content-SHA256": meta["content_hash"],
            "Content-Disposition": (
                f'attachment; filename="opencheck-saved-{meta["lei"]}-{meta["saved_at"][:10]}.json"'
            ),
            "Cache-Control": "private, max-age=60",
        },
    )


@router.get("/saved-reports/{report_id}")
@limiter.limit(default_tier)
async def get_saved_report(request: Request, response: Response, report_id: str) -> dict[str, Any]:
    """Metadata plus the frozen payload, after the expiry and integrity checks."""
    store = _store()
    try:
        meta, data = await asyncio.to_thread(sr.load_report, store, report_id)
    except sr.SavedReportError as exc:
        raise _refuse(exc) from exc
    response.headers.update(NOINDEX)
    response.headers["Cache-Control"] = "private, max-age=60"
    return {**_with_url(meta), "json_path": f"/saved-reports/{report_id}.json", "payload": json.loads(data)}


@router.post("/saved-reports/{report_id}/extend")
@limiter.limit(default_tier)
async def extend_saved_report(
    request: Request,
    response: Response,
    report_id: str,
    manage_token: str | None = Header(default=None, alias=MANAGE_HEADER),
) -> dict[str, Any]:
    """Keep the report for another retention period, counted from now."""
    store = _store()
    try:
        meta = await asyncio.to_thread(sr.extend_report, store, report_id, manage_token)
    except sr.SavedReportError as exc:
        raise _refuse(exc) from exc
    response.headers.update(NOINDEX)
    return _with_url(meta)


@router.delete("/saved-reports/{report_id}")
@limiter.limit(default_tier)
async def delete_saved_report(
    request: Request,
    response: Response,
    report_id: str,
    manage_token: str | None = Header(default=None, alias=MANAGE_HEADER),
) -> dict[str, Any]:
    store = _store()
    try:
        await asyncio.to_thread(sr.delete_report, store, report_id, manage_token)
    except sr.SavedReportError as exc:
        raise _refuse(exc) from exc
    response.headers.update(NOINDEX)
    return {"report_id": report_id, "deleted": True}
