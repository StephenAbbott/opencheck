"""Tests for opencheck/memwatch.py — the OOM-forensics instrumentation."""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from opencheck import memwatch
from opencheck.app import app
from opencheck.config import get_settings


@pytest.fixture(autouse=True)
def _reset_window():
    memwatch.window.snapshot_and_reset()
    memwatch.window.inflight = 0
    memwatch.window.inflight_peak = 0
    yield
    memwatch.window.snapshot_and_reset()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


# ----------------------------------------------------------------------
# Helpers: bucket + bot detection
# ----------------------------------------------------------------------


def test_bucket_collapses_to_first_segment():
    assert memwatch.bucket("/entity/529900ABC-some-name") == "/entity"
    assert memwatch.bucket("/og/529900ABC.png") == "/og"
    assert memwatch.bucket("/sitemaps/entities-3.xml") == "/sitemaps"
    assert memwatch.bucket("/browse/DE") == "/browse"
    assert memwatch.bucket("/lookup") == "/lookup"
    assert memwatch.bucket("/") == "/"
    assert memwatch.bucket("") == "/"


def test_is_bot_flags_crawlers_and_scripts():
    assert memwatch.is_bot(
        "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; "
        "Googlebot/2.1; +http://www.google.com/bot.html)"
    )
    assert memwatch.is_bot("Mozilla/5.0 (compatible; bingbot/2.0)")
    assert memwatch.is_bot("python-requests/2.32.0")
    assert memwatch.is_bot("curl/8.4.0")
    assert memwatch.is_bot("Scrapy/2.11 (+https://scrapy.org)")
    assert memwatch.is_bot("")  # empty UA: real browsers always send one
    assert memwatch.is_bot(None)


def test_is_bot_passes_real_browsers():
    assert not memwatch.is_bot(
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    )
    assert not memwatch.is_bot(
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 "
        "Safari/604.1"
    )


# ----------------------------------------------------------------------
# Memory readings
# ----------------------------------------------------------------------


def test_current_rss_is_positive_and_plausible():
    rss = memwatch.current_rss_mb()
    assert 1 < rss < 100_000  # a running Python process, not garbage


def test_memory_limit_env_override(monkeypatch):
    monkeypatch.setenv("OPENCHECK_MEMORY_LIMIT_MB", "512")
    get_settings.cache_clear()
    try:
        assert memwatch.memory_limit_mb() == 512.0
    finally:
        get_settings.cache_clear()


# ----------------------------------------------------------------------
# Access-log middleware (via the real app stack)
# ----------------------------------------------------------------------


def test_middleware_counts_and_logs_with_ua(client: TestClient, caplog):
    with caplog.at_level(logging.INFO, logger="opencheck.access"):
        r = client.get("/sources", headers={"user-agent": "Mozilla/5.0 (compatible; bingbot/2.0)"})
    assert r.status_code == 200
    lines = [rec.getMessage() for rec in caplog.records if "access " in rec.getMessage()]
    assert len(lines) == 1
    line = lines[0]
    assert 'path="/sources"' in line
    assert "status=200" in line
    assert "bot=1" in line
    assert "bingbot" in line
    # Window counters reflect the request.
    snap = memwatch.window.snapshot_and_reset()
    assert snap["requests"] == 1
    assert snap["bots"] == 1
    assert snap["by_bucket"] == {"/sources": 1}
    assert snap["inflight_peak"] >= 1


def test_middleware_skips_health_log_but_still_counts(client: TestClient, caplog):
    with caplog.at_level(logging.INFO, logger="opencheck.access"):
        client.get("/health")
    assert not [r for r in caplog.records if "access " in r.getMessage()]
    snap = memwatch.window.snapshot_and_reset()
    assert snap["by_bucket"] == {"/health": 1}


def test_middleware_disabled_by_setting(client: TestClient, caplog, monkeypatch):
    monkeypatch.setenv("OPENCHECK_ACCESS_LOG", "0")
    get_settings.cache_clear()
    try:
        with caplog.at_level(logging.INFO, logger="opencheck.access"):
            client.get("/sources")
        assert not [r for r in caplog.records if "access " in r.getMessage()]
        # Counting continues even when the per-request lines are off.
        assert memwatch.window.snapshot_and_reset()["requests"] == 1
    finally:
        get_settings.cache_clear()


# ----------------------------------------------------------------------
# The periodic memwatch line
# ----------------------------------------------------------------------


def test_tick_logs_info_line_with_fields(caplog, monkeypatch):
    monkeypatch.setenv("OPENCHECK_MEMORY_LIMIT_MB", "100000")  # ample → INFO
    get_settings.cache_clear()
    try:
        memwatch.window.requests = 5
        memwatch.window.bots = 4
        memwatch.window.by_bucket.update({"/og": 3, "/entity": 2})
        memwatch.window.bot_by_bucket.update({"/og": 3, "/entity": 1})
        with caplog.at_level(logging.INFO, logger="opencheck.memwatch"):
            memwatch.tick()
    finally:
        get_settings.cache_clear()
    lines = [r for r in caplog.records if r.getMessage().startswith("memwatch ")]
    assert len(lines) == 1
    assert lines[0].levelno == logging.INFO
    msg = lines[0].getMessage()
    for field in ("rss_mb=", "limit_mb=", "pct=", "og_cache=", "replay_cache=",
                  "reqs=5", "bots=4", "top=/og:3,/entity:2", "bot_top=/og:3,/entity:1"):
        assert field in msg, f"missing {field} in: {msg}"
    # The window was reset by the tick.
    assert memwatch.window.requests == 0


def test_tick_escalates_to_warning_on_high_water(caplog, monkeypatch):
    monkeypatch.setenv("OPENCHECK_MEMORY_LIMIT_MB", "1")  # everything is >85%
    get_settings.cache_clear()
    try:
        with caplog.at_level(logging.INFO, logger="opencheck.memwatch"):
            memwatch.tick()
    finally:
        get_settings.cache_clear()
    lines = [r for r in caplog.records if r.getMessage().startswith("memwatch ")]
    assert lines and lines[0].levelno == logging.WARNING


def test_tick_survives_missing_limit(caplog, monkeypatch):
    """No cgroup limit (dev laptop) → pct=? but the line still logs."""
    monkeypatch.delenv("OPENCHECK_MEMORY_LIMIT_MB", raising=False)
    monkeypatch.setattr(memwatch, "memory_limit_mb", lambda: None)
    with caplog.at_level(logging.INFO, logger="opencheck.memwatch"):
        memwatch.tick()
    lines = [r for r in caplog.records if r.getMessage().startswith("memwatch ")]
    assert lines and "pct=?" in lines[0].getMessage()
