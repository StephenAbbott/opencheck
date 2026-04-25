"""Filesystem-backed cache for adapter responses.

Layout (relative to the project root):

    data/cache/
        demos/   Curated fixtures shipped with the repo — treated as read-only.
        live/    Runtime cache populated from real API calls — gitignored.

Cache keys are namespaced per adapter (e.g. ``companies_house/company/00102498.json``).
Lookup order: ``demos/`` first, then ``live/``. Writes always go to ``live/``.

This lets us demo the product offline (no network, no API keys) while also
transparently caching real calls when ``allow_live=true``.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _project_root() -> Path:
    # Walk up from this file until we find a directory containing ``data``
    # or bail out at the filesystem root. This makes tests cache-friendly
    # regardless of CWD.
    here = Path(__file__).resolve()
    for parent in [here, *here.parents]:
        if (parent / "data").is_dir():
            return parent
        if (parent / "pyproject.toml").is_file() and (parent.parent / "data").is_dir():
            return parent.parent
    # Last resort: backend/ layout → ../data
    return Path(__file__).resolve().parents[2]


_DATA_ROOT_ENV = "OPENCHECK_DATA_ROOT"


def data_root() -> Path:
    override = os.environ.get(_DATA_ROOT_ENV)
    if override:
        return Path(override)
    return _project_root() / "data"


@dataclass(frozen=True)
class CacheHit:
    """A cache lookup result with provenance metadata."""

    payload: Any
    tier: str  # "demos" or "live"
    path: Path
    retrieved_at: float


class Cache:
    """Two-tier filesystem cache (demos → live).

    The root is re-resolved on every read/write so that environment
    overrides (``OPENCHECK_DATA_ROOT``) take effect even when the cache
    instance was constructed before the override was set — important for
    long-lived adapter instances under test.
    """

    def __init__(self, root: Path | None = None) -> None:
        self._override_root = root

    def _root(self) -> Path:
        return (self._override_root or data_root()) / "cache"

    def _demos(self) -> Path:
        return self._root() / "demos"

    def _live(self) -> Path:
        return self._root() / "live"

    # ---- reads ----

    def get(self, key: str) -> CacheHit | None:
        """Return a cache hit from ``demos/`` first, else ``live/``."""
        for tier, base in (("demos", self._demos()), ("live", self._live())):
            path = base / f"{key}.json"
            if path.is_file():
                with path.open("r", encoding="utf-8") as fh:
                    return CacheHit(
                        payload=json.load(fh),
                        tier=tier,
                        path=path,
                        retrieved_at=path.stat().st_mtime,
                    )
        return None

    def has(self, key: str) -> bool:
        """Cheap presence check — used by adapters to decide whether to
        fall back to the Phase 0 stub path. A demo fixture for ``key``
        means we should serve it regardless of ``live_available``."""
        for base in (self._demos(), self._live()):
            if (base / f"{key}.json").is_file():
                return True
        return False

    # ---- writes ----

    def put(self, key: str, payload: Any) -> Path:
        """Persist ``payload`` under ``live/<key>.json``. Returns the path."""
        path = self._live() / f"{key}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            json.dump(
                {
                    "_cached_at": time.time(),
                    "payload": payload,
                },
                fh,
                indent=2,
                default=str,
            )
        return path

    # ``put`` wraps the payload; ``get`` unwraps it transparently.
    def get_payload(self, key: str) -> tuple[Any, str] | None:
        hit = self.get(key)
        if hit is None:
            return None
        # Both tiers may or may not have the ``_cached_at`` wrapper.
        if isinstance(hit.payload, dict) and "payload" in hit.payload and "_cached_at" in hit.payload:
            return hit.payload["payload"], hit.tier
        return hit.payload, hit.tier
