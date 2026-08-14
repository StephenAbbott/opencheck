"""Phase 99 — provenance and the four date clocks.

Before this phase every BODS statement asserted ``retrievedAt`` = the moment
the mapper ran, and ``statementDate`` = today, regardless of whether the
payload came from a live API call, a week-old cache entry, a months-old bulk
snapshot, a committed fixture, or a stub. Four different provenance claims,
one timestamp, and it was wrong for all but the first.

These tests pin the corrected behaviour. The canaries at the bottom are the
important ones: they fail loudly if a future change re-collapses the clocks.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone

import pytest

from opencheck import provenance
from opencheck.bods.mapper import _source_block, _statement_date, _today
from opencheck.cache import Cache


UTC = timezone.utc


class TestRecorderResolution:
    def test_no_observations_is_stub(self):
        with provenance.recording() as rec:
            pass
        assert rec.resolve() == provenance.STUB_PROVENANCE

    def test_live_observation(self):
        with provenance.recording() as rec:
            provenance.record_live("api")
        resolved = rec.resolve()
        assert resolved.liveness == "live"
        assert resolved.retrieved_at is not None

    def test_stub_flag_overrides_everything(self):
        """A stub payload must never carry a retrieval time.

        Adapters routinely build an HTTP client and then fall back to stub
        output; without this short-circuit the placeholder would inherit the
        client's 'live' record.
        """
        with provenance.recording() as rec:
            provenance.record_live("api")
        resolved = rec.resolve(is_stub=True)
        assert resolved.liveness == "stub"
        assert resolved.retrieved_at is None

    def test_worst_liveness_wins(self):
        """A bundle is only as fresh as its stalest component.

        Companies House issues several requests per lookup; if any of them was
        served from cache the bundle is not 'live'.
        """
        old = datetime(2026, 1, 1, tzinfo=UTC)
        with provenance.recording() as rec:
            provenance.record_live("fresh call")
            provenance.record_cached(old, "cache read")
        resolved = rec.resolve()
        assert resolved.liveness == "cached"

    def test_oldest_timestamp_wins(self):
        old = datetime(2026, 1, 1, tzinfo=UTC)
        newer = datetime(2026, 6, 1, tzinfo=UTC)
        with provenance.recording() as rec:
            provenance.record_cached(newer)
            provenance.record_cached(old)
        assert rec.resolve().retrieved_at == old

    def test_curated_claims_no_retrieval_time_by_default(self):
        with provenance.recording() as rec:
            provenance.record_curated("committed fixture")
        resolved = rec.resolve()
        assert resolved.liveness == "curated"
        assert resolved.retrieved_at is None

    def test_curated_may_declare_a_real_harvest_date(self):
        harvested = datetime(2026, 8, 12, tzinfo=UTC)
        with provenance.recording() as rec:
            provenance.record_curated("cac", harvested_at=harvested)
        assert rec.resolve().retrieved_at == harvested

    def test_severity_order_is_total(self):
        # Every liveness value must be rankable, or "worst wins" silently
        # degrades to "whichever was recorded first".
        for value in ("live", "cached", "snapshot", "curated", "stub"):
            assert value in provenance._SEVERITY
        assert len(set(provenance._SEVERITY.values())) == 5

    def test_record_outside_a_scope_is_a_noop(self):
        # Scripts, tests and the OKF generator call cache/HTTP helpers without
        # opening a scope; that must not raise.
        provenance.record_live("no scope open")

    def test_scopes_do_not_leak(self):
        with provenance.recording() as outer:
            with provenance.recording() as inner:
                provenance.record_live()
            provenance.record_curated()
        assert inner.resolve().liveness == "live"
        assert outer.resolve().liveness == "curated"


class TestIsoFormatting:
    def test_trailing_z(self):
        p = provenance.Provenance(
            liveness="live", retrieved_at=datetime(2026, 6, 3, 9, 30, tzinfo=UTC)
        )
        assert p.retrieved_at_iso() == "2026-06-03T09:30:00Z"

    def test_naive_treated_as_utc(self):
        p = provenance.Provenance(
            liveness="live", retrieved_at=datetime(2026, 6, 3, 9, 30)
        )
        assert p.retrieved_at_iso() == "2026-06-03T09:30:00Z"

    def test_offset_converted_to_utc(self):
        moment = datetime(2026, 6, 3, 10, 30, tzinfo=timezone(timedelta(hours=1)))
        p = provenance.Provenance(liveness="live", retrieved_at=moment)
        assert p.retrieved_at_iso() == "2026-06-03T09:30:00Z"

    def test_none_when_nothing_fetched(self):
        assert provenance.STUB_PROVENANCE.retrieved_at_iso() is None


class TestCacheRecordsProvenance:
    def test_demo_tier_is_curated_with_no_claimed_retrieval(self, tmp_path):
        root = tmp_path / "cache"
        (root / "demos" / "demo_src").mkdir(parents=True)
        (root / "demos" / "demo_src" / "x.json").write_text(json.dumps({"a": 1}))

        cache = Cache(root=tmp_path)
        with provenance.recording() as rec:
            hit = cache.get("demo_src/x")
        assert hit is not None and hit.tier == "demos"
        resolved = rec.resolve()
        assert resolved.liveness == "curated"
        # A checked-out file's mtime says when git wrote it here, not when the
        # data left the register.
        assert resolved.retrieved_at is None

    def test_live_tier_reports_when_it_was_cached(self, tmp_path):
        cache = Cache(root=tmp_path)
        cache.put("live_src/y", {"a": 1})

        with provenance.recording() as rec:
            hit = cache.get("live_src/y")
        assert hit is not None and hit.tier == "live"
        resolved = rec.resolve()
        assert resolved.liveness == "cached"
        assert resolved.retrieved_at is not None
        # Written moments ago, so it should be very recent — but it is the
        # cache write time, not "now at mapping time".
        age = datetime.now(UTC) - resolved.retrieved_at
        assert timedelta(seconds=-5) < age < timedelta(seconds=60)

    def test_cached_at_wrapper_beats_file_mtime(self, tmp_path):
        """mtime moves when a cache tree is copied; the wrapper's own stamp
        is the real fetch time, so it wins."""
        root = tmp_path / "cache"
        (root / "live" / "s").mkdir(parents=True)
        long_ago = datetime(2026, 1, 2, tzinfo=UTC)
        (root / "live" / "s" / "k.json").write_text(
            json.dumps({"_cached_at": long_ago.timestamp(), "payload": {"a": 1}})
        )
        # Touch the file so its mtime is now, unlike the recorded fetch time.
        (root / "live" / "s" / "k.json").touch()

        cache = Cache(root=tmp_path)
        with provenance.recording() as rec:
            cache.get("s/k")
        assert rec.resolve().retrieved_at == long_ago

    def test_miss_records_nothing(self, tmp_path):
        cache = Cache(root=tmp_path)
        with provenance.recording() as rec:
            assert cache.get("nope/nothing") is None
        assert rec.resolve() == provenance.STUB_PROVENANCE


class TestSourceBlockHonoursProvenance:
    def test_no_claim_without_an_observed_retrieval(self):
        assert "retrievedAt" not in _source_block("gleif", None)

    def test_claim_matches_the_observation(self):
        moment = datetime(2026, 6, 3, 9, 30, tzinfo=UTC)
        with provenance.mapping_provenance(
            provenance.Provenance(liveness="snapshot", retrieved_at=moment)
        ):
            block = _source_block("bods_gleif", None)
        assert block["retrievedAt"] == "2026-06-03T09:30:00Z"

    def test_mapping_provenance_resets(self):
        with provenance.mapping_provenance(
            provenance.Provenance(
                liveness="live", retrieved_at=datetime.now(UTC)
            )
        ):
            pass
        assert "retrievedAt" not in _source_block("gleif", None)


class TestStatementDate:
    def test_explicit_source_date_wins(self):
        assert _statement_date("2016-04-06") == "2016-04-06"

    def test_falls_back_to_retrieval_date(self):
        """A months-old snapshot is not claimed-true today.

        The BODS dates guidance's consolidation reading — "the date on which
        several sources of information were resolved to make a coherent
        claim" — covers using the retrieval date when the register supplies
        none of its own.
        """
        moment = datetime(2026, 2, 28, 11, 0, tzinfo=UTC)
        with provenance.mapping_provenance(
            provenance.Provenance(liveness="snapshot", retrieved_at=moment)
        ):
            assert _statement_date() == "2026-02-28"

    def test_falls_back_to_today_only_when_nothing_was_fetched(self):
        assert _statement_date() == _today()


class TestClockCanaries:
    """The four clocks must stay distinguishable.

    statementDate (when the source claimed it), source.retrievedAt (when we
    downloaded it) and publicationDetails.publicationDate (when we published
    it) were previously all ``date.today()``. If a future change re-collapses
    them, these fail.
    """

    def test_statement_date_tracks_retrieval_not_today(self):
        moment = datetime(2025, 2, 28, tzinfo=UTC)
        with provenance.mapping_provenance(
            provenance.Provenance(liveness="snapshot", retrieved_at=moment)
        ):
            statement_date = _statement_date()
        assert statement_date == "2025-02-28"
        assert statement_date != _today(), (
            "statementDate collapsed back onto today's date"
        )

    def test_snapshot_statement_differs_from_publication_date(self):
        from opencheck.bods.mapper import make_entity_statement

        moment = datetime(2025, 2, 28, tzinfo=UTC)
        with provenance.mapping_provenance(
            provenance.Provenance(liveness="snapshot", retrieved_at=moment)
        ):
            stmt = make_entity_statement(
                source_id="bods_gleif",
                local_id="X",
                name="Example Ltd",
            )
        assert stmt["statementDate"] == "2025-02-28"
        assert stmt["publicationDetails"]["publicationDate"] == _today()
        assert stmt["source"]["retrievedAt"].startswith("2025-02-28")
        assert (
            stmt["statementDate"]
            != stmt["publicationDetails"]["publicationDate"]
        ), "statementDate and publicationDate collapsed onto the same value"

    def test_stub_statement_makes_no_retrieval_claim(self):
        from opencheck.bods.mapper import make_entity_statement

        with provenance.mapping_provenance(provenance.STUB_PROVENANCE):
            stmt = make_entity_statement(
                source_id="gleif", local_id="X", name="Example Ltd"
            )
        assert "retrievedAt" not in stmt["source"], (
            "stub output must never claim a retrieval time"
        )


class TestSourceHitCarriesProvenance:
    def test_defaults_are_conservative(self):
        from opencheck.sources.base import SearchKind, SourceHit

        hit = SourceHit(
            source_id="x", hit_id="1", kind=SearchKind.ENTITY, name="n", summary="s"
        )
        # A source that declares nothing must under-claim, not over-claim.
        assert hit.liveness == "stub"
        assert hit.retrieved_at is None

    def test_serialises_as_iso_z(self):
        from opencheck.sources.base import SearchKind, SourceHit

        hit = SourceHit(
            source_id="x",
            hit_id="1",
            kind=SearchKind.ENTITY,
            name="n",
            summary="s",
            liveness="live",
            retrieved_at=datetime(2026, 6, 3, 9, 30, tzinfo=UTC),
        )
        assert hit.model_dump()["retrieved_at"] == "2026-06-03T09:30:00Z"

    def test_null_serialises_as_none(self):
        from opencheck.sources.base import SearchKind, SourceHit

        hit = SourceHit(
            source_id="x", hit_id="1", kind=SearchKind.ENTITY, name="n", summary="s"
        )
        assert hit.model_dump()["retrieved_at"] is None
