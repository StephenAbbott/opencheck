"""Top-level pytest configuration.

Disables ``.env`` file loading for the entire test session. The runtime
``Settings`` class resolves an absolute path to the repo's real
``.env`` so that ``uv run uvicorn`` from ``backend/`` still picks up
``OPENCHECK_ALLOW_LIVE=true`` and the API keys the user has set. That
behaviour is great for the dev server, but tests rely on monkeypatched
env vars and shouldn't have their setup quietly shadowed by whatever
the developer happens to have on disk.

Setting ``OPENCHECK_DISABLE_DOTENV=1`` *before any test imports
opencheck.config* tells the Settings class to skip the env-file
lookup entirely. The flag is checked at class-definition time, so
this conftest must run before any test module imports the package —
pytest collects ``conftest.py`` first by design.
"""

from __future__ import annotations

import os

import pytest

# Set the flag at import time, before any test code runs and before
# opencheck.config is imported. No fixture wrapping needed.
os.environ.setdefault("OPENCHECK_DISABLE_DOTENV", "1")


@pytest.fixture(autouse=True)
def _clear_lookup_replay_cache():
    """The lookup replay cache is keyed by LEI only; tests reuse the same
    demo LEIs with different fixtures, so cached events must never leak
    across tests."""
    from opencheck.routers import lookup as _lookup_mod

    _lookup_mod._REPLAY_CACHE.clear()
    yield
    _lookup_mod._REPLAY_CACHE.clear()
