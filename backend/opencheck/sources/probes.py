"""Per-source health probes — one known-good exercise of every adapter.

This table is the input to the weekly source-health sweep
(``scripts/source_health.py`` + ``.github/workflows/source-health.yml``) and to
the offline coverage guard in ``tests/test_source_probes.py``.

Why this exists, and why it asserts what it asserts
---------------------------------------------------

The obvious version of a source-health check asks "did the adapter return
anything?". That check would have gone green every week on the bug that
prompted this module. ``AriregisterAdapter`` was working — it reached
``ariregister.rik.ee``, parsed the response and returned real officers and
beneficial owners. What was wrong was the **provenance**: the adapter builds
its own ``httpx.AsyncClient`` (it needs a bare HTML ``Accept`` header and a
longer timeout), bypassing ``http.build_client()`` and with it the implicit
``provenance.record_live()``. With no observation recorded,
``provenance.resolve()`` falls back to ``stub`` and the UI stamped
"Placeholder data" on genuinely live results (fixed in PR #153).

OpenCheck's output is a claim about provenance as much as about content, so a
source can be entirely functional and still produce misleading output. Every
probe therefore runs inside a ``provenance.recording()`` scope and asserts the
**resolved liveness**, alongside reachability and non-emptiness.

Choosing probe subjects
-----------------------

Most adapters are not name-searchable: they are entered via an identifier
derived from the GLEIF anchor record (``ee_registry_code``, ``cz_ico``,
``dk_cvr`` …), so a probe is a known-good **identifier**, not a query. Pick
large, old, boring subjects — state-owned utilities and listed incumbents — so
a fixture does not rot because a company was struck off.

``known_gap``
-------------

Several adapters currently record *no* provenance observation at all on their
normal path, so they resolve ``stub`` even when they answer correctly — the
same shape as the Ariregister bug, not yet fixed. Rather than assert the
correct-but-false expectation (which would make the sweep permanently red) or
the current-but-wrong one silently, those probes declare ``known_gap``: the
expectation matches today's behaviour and the report prints the gap so it stays
visible. Closing a gap means fixing the adapter and tightening
``expect_liveness`` in the same commit.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from collections.abc import Mapping
from typing import Any, Literal

Tier = Literal["live", "curated", "snapshot", "index", "inactive"]

#: Liveness values that mean "a network round-trip actually happened".
LIVE = frozenset({"live"})
#: A cache hit means the upstream was *not* contacted, so a live-tier probe
#: served from cache is reported as degraded rather than ok — see the sweep.
LIVE_OR_CACHED = frozenset({"live", "cached"})


@dataclass(frozen=True)
class SourceProbe:
    """A minimal, known-good exercise of one adapter.

    ``method``/``args`` name the adapter coroutine to call. Most sources use
    the plain ``fetch(hit_id)`` entry point; identifier-bridged sources
    (ClimateTRACE, EITI, TED, Wikirate, OpenAleph, the curated BO sets) expose
    a purpose-built ``fetch_by_*`` that the lookup pipeline uses instead, and
    the probe should exercise the path production actually takes.
    """

    tier: Tier
    subject: str
    """Human label for the probe subject, e.g. 'Equinor ASA'. Report only."""

    method: str = "fetch"
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] = field(default_factory=dict)
    """Keyword arguments for the call.

    A few adapters need one. ``jar_lithuania.fetch`` is the instructive
    case: JAR CAPTCHA-gates direct code lookups, so the adapter searches by
    name and matches on the code — without ``legal_name`` it returns a stub
    without touching the network, and the probe would test nothing.
    """

    expect_liveness: frozenset[str] = LIVE
    expect_fields: tuple[str, ...] = ()
    """Top-level keys of the returned mapping that must be present and truthy.

    Deliberately sparse: populated only where the shape has been verified
    against a real response. The sweep records the observed top-level keys for
    every source in its JSON artifact, which is how the rest get filled in.
    """

    allow_empty: bool = False
    """Whether an empty answer is a legitimate result for this subject.

    True for screening and procurement sources, where "nothing on file" is a
    real answer: OpenAleph finding no documents, TED finding no award notices,
    RPVS finding that an IČO is not a public-sector partner. For a register
    lookup of a company that exists, an empty answer is a failure.
    """

    requires_env: tuple[str, ...] = ()
    """Settings aliases that must be set for live mode. Missing → skipped."""

    requires_files: tuple[str, ...] = ()
    """Data-root-relative paths the adapter needs. Missing → skipped.

    ``data/gem/`` is gitignored, so ClimateTRACE's ownership artifacts are
    absent from a fresh CI checkout — that is a skip, not a failure.
    """

    anchor_lei: str | None = None
    """GLEIF anchor for this subject, for the dispatch-drift check.

    If GLEIF renames a registration-authority code or a registrar changes its
    number formatting, the adapter stops being dispatched at all while testing
    perfectly green on its own. Consumed by the Phase C check.
    """

    bods_mapper: str | None = None
    """Function name in ``opencheck.bods.mapper``, for the statement-count diff."""

    known_gap: str = ""
    """Known provenance/coverage defect this probe documents but tolerates."""

    notes: str = ""


def _p(**kwargs: Any) -> SourceProbe:
    return SourceProbe(**kwargs)


#: One entry per id in ``sources.REGISTRY``. Parity is enforced by
#: ``tests/test_source_probes.py`` so a new adapter cannot ship untested.
PROBES: dict[str, SourceProbe] = {
    # --- live national registers, entered by identifier -------------------
    "abr_australia": _p(
        tier="live",
        subject="Commonwealth Bank of Australia (ABN)",
        args=("31976733718",),
        requires_env=("ABN_GUID",),
        bods_mapper="map_abr_australia",
    ),
    "ares": _p(
        tier="live",
        subject="Czech company by IČO",
        args=("27082440",),
        bods_mapper="map_ares",
    ),
    "ariregister": _p(
        tier="live",
        subject="Estonian company by registry code",
        args=("10584597",),
        bods_mapper="map_ariregister",
        notes=(
            "The regression fence for PR #153: this adapter bypasses "
            "build_client() and records provenance explicitly, so a refactor "
            "that drops the record_live() call turns this probe red."
        ),
    ),
    "bolagsverket": _p(
        tier="live",
        subject="Telefonaktiebolaget LM Ericsson",
        args=("5560160680",),
        requires_env=("BOLAGSVERKET_API_KEY", "BOLAGSVERKET_CLIENT_SECRET"),
        bods_mapper="map_bolagsverket",
    ),
    "brreg": _p(
        tier="live",
        subject="Equinor ASA",
        args=("974760673",),
        bods_mapper="map_brreg",
    ),
    "cnpj_brazil": _p(
        tier="live",
        subject="Petróleo Brasileiro S.A. (Petrobras)",
        args=("33000167000101",),
        bods_mapper="map_cnpj_brazil",
    ),
    "companies_house": _p(
        tier="live",
        subject="BP P.L.C.",
        args=("00102498",),
        requires_env=("COMPANIES_HOUSE_API_KEY",),
        anchor_lei="213800LH1BZH3DI6G760",
        bods_mapper="map_companies_house",
    ),
    "corporations_canada": _p(
        tier="live",
        subject="Canadian federal corporation",
        args=("1007",),
        requires_env=("CORPORATIONS_CANADA_API_KEY",),
        bods_mapper="map_corporations_canada",
    ),
    "cro": _p(
        tier="live",
        subject="Irish company by CRN",
        args=("249885",),
        bods_mapper="map_cro",
    ),
    "cvr_denmark": _p(
        tier="live",
        subject="Novo Nordisk A/S",
        args=("24256790",),
        requires_env=("CVR_DENMARK_API_KEY",),
        bods_mapper="map_cvr_denmark",
    ),
    "firmenbuch": _p(
        tier="live",
        subject="Austrian company by Firmenbuchnummer",
        args=("473888w",),
        requires_env=("FIRMENBUCH_API_KEY",),
        bods_mapper="map_firmenbuch",
    ),
    "gleif": _p(
        tier="live",
        subject="BP P.L.C.",
        args=("213800LH1BZH3DI6G760",),
        expect_fields=("lei", "record"),
        anchor_lei="213800LH1BZH3DI6G760",
        bods_mapper="map_gleif",
        notes="The anchor for the whole lookup pipeline — first to check when many sources fail at once.",
    ),
    "inpi": _p(
        tier="live",
        subject="Bolloré SA",
        args=("055804124",),
        requires_env=("INPI_USERNAME", "INPI_PASSWORD"),
        bods_mapper="map_inpi",
    ),
    "jar_lithuania": _p(
        tier="live",
        subject="Lietuvos energija",
        args=("111950694",),
        kwargs={"legal_name": "Lietuvos energija"},
        expect_fields=("status",),
        bods_mapper="map_jar_lithuania",
        notes=(
            "expect_fields is load-bearing here: when JAR refuses the request "
            "the adapter still returns a non-stub bundle carrying the GLEIF "
            "name and null register fields, which looks like an answer. "
            "Asserting a register-only field is what separates 'the register "
            "replied' from 'we fell back to what GLEIF already told us'."
        ),
    ),
    "krs_poland": _p(
        tier="live",
        subject="PKN Orlen",
        args=("0000017219",),
        bods_mapper="map_krs_poland",
    ),
    "kvk": _p(
        tier="live",
        subject="ASML Holding",
        args=("17085815",),
        bods_mapper="map_kvk",
    ),
    "malta_mbr": _p(
        tier="live",
        subject="Maltese company by CRN",
        args=("C 113927",),
        bods_mapper="map_malta_mbr",
    ),
    "mca_india": _p(
        tier="live",
        subject="Infosys Limited",
        args=("L85110KA1981PLC013115",),
        requires_env=("DATA_GOV_IN_API_KEY",),
        bods_mapper="map_mca_india",
    ),
    "nz_companies": _p(
        tier="live",
        subject="Fonterra Co-operative Group",
        args=("1166320",),
        requires_env=("NZBN_API_KEY",),
        bods_mapper="map_nz_companies",
    ),
    "prh": _p(
        tier="live",
        subject="Finnish company by Y-tunnus",
        args=("0112038-9",),
        bods_mapper="map_prh",
    ),
    "rpo_slovakia": _p(
        tier="live",
        subject="Slovak legal person by IČO",
        args=("31320155",),
        bods_mapper="map_rpo_slovakia",
    ),
    "rpvs_slovakia": _p(
        tier="live",
        subject="Slovak public-sector partner by IČO",
        allow_empty=True,
        args=("31320155",),
        bods_mapper="map_rpvs_slovakia",
        notes="Shares sk_ico dispatch with rpo_slovakia; a nil result is legitimate (not every IČO is an RPVS partner).",
    ),
    "sudreg_croatia": _p(
        tier="live",
        subject="INA-Industrija nafte d.d.",
        args=("80000604",),
        requires_env=("SUDREG_CLIENT_ID", "SUDREG_CLIENT_SECRET"),
        bods_mapper="map_sudreg_croatia",
    ),
    "ur_latvia": _p(
        tier="live",
        subject="Latvian company by regcode",
        args=("40003009556",),
        bods_mapper="map_ur_latvia",
    ),
    "zefix": _p(
        tier="live",
        subject="Swiss company by UID",
        args=("CHE313550547",),
        requires_env=("ZEFIX_USERNAME", "ZEFIX_PASSWORD"),
        bods_mapper="map_zefix",
    ),
    # --- live cross-border / aggregator sources ---------------------------
    "opencorporates": _p(
        tier="live",
        subject="BP P.L.C. (gb/00102498)",
        args=("gb/00102498",),
        requires_env=("OPENCORPORATES_API_KEY",),
        bods_mapper="map_opencorporates",
        notes="Licence: derived output only — never echo the raw payload into a report.",
    ),
    "openaleph": _p(
        tier="live",
        subject="Ericsson AB",
        allow_empty=True,
        method="fetch_by_lei",
        args=("549300MLH00Y3BN4HD49",),
        anchor_lei="549300MLH00Y3BN4HD49",
        bods_mapper="map_openaleph",
        notes="Returns a list of hits, and an empty list is a legitimate answer for a clean entity.",
    ),
    "opensanctions": _p(
        tier="live",
        subject="Rosneft (NK-rosneft)",
        allow_empty=True,
        args=("NK-rosneft",),
        requires_env=("OPENSANCTIONS_API_KEY",),
        bods_mapper="map_opensanctions",
        notes="Licence CC BY-NC: shape-only assertions, nothing recorded or echoed.",
    ),
    "everypolitician": _p(
        tier="live",
        subject="A PEP record served via OpenSanctions",
        allow_empty=True,
        args=("Q7747-pep",),
        requires_env=("OPENSANCTIONS_API_KEY",),
        bods_mapper="map_everypolitician",
        notes="Licence CC BY-NC. Person-kind source; no personal detail goes in the report.",
    ),
    "sec_edgar": _p(
        tier="live",
        subject="US issuer by CIK",
        allow_empty=True,
        args=("1793659",),
        bods_mapper="map_sec_edgar",
    ),
    "wikidata": _p(
        tier="live",
        subject="Unilever (Q152057)",
        args=("Q152057",),
        bods_mapper="map_wikidata",
    ),
    "wikirate": _p(
        tier="live",
        subject="BP P.L.C. on Wikirate",
        allow_empty=True,
        method="fetch_by_lei",
        args=("213800LH1BZH3DI6G760", "Q152057"),
        requires_env=("WIKIRATE_API_KEY",),
        anchor_lei="213800LH1BZH3DI6G760",
        bods_mapper="map_wikirate",
    ),
    "ted_eu": _p(
        tier="live",
        subject="Orange S.A. award notices",
        allow_empty=True,
        method="fetch_by_identifiers",
        args=("969500MCOONR8990S771", "380129866", "FR"),
        anchor_lei="969500MCOONR8990S771",
        bods_mapper="map_ted_eu",
        notes="A zero notice count is a legitimate answer; the probe asserts the search itself still works.",
    ),
    "climatetrace": _p(
        tier="live",
        subject="BP P.L.C. asset-level emissions",
        allow_empty=True,
        method="fetch_by_lei",
        args=("213800LH1BZH3DI6G760",),
        requires_files=("gem/ownership.zip",),
        anchor_lei="213800LH1BZH3DI6G760",
        bods_mapper="map_climatetrace",
        notes="data/gem/ is gitignored, so this skips on a fresh CI checkout until the artifacts are fetched.",
    ),
    # --- index-matched sources -------------------------------------------
    "eiti": _p(
        tier="index",
        subject="Equinor UK Ltd (GB 01285743)",
        allow_empty=True,
        method="fetch_by_registration",
        args=("GB", "01285743"),
        expect_liveness=LIVE_OR_CACHED,
        bods_mapper="map_eiti",
        notes="Committed organisation index resolves the company; payment rows are then fetched live.",
    ),
    "eiti_soe": _p(
        tier="index",
        subject="Equinor Energy AS",
        allow_empty=True,
        method="fetch_by_lei",
        args=("98450073EGD581D89F03",),
        expect_liveness=frozenset({"live", "cached", "stub"}),
        bods_mapper="map_eiti_soe",
        known_gap=(
            "An index-only match records no provenance observation, so it "
            "resolves 'stub' even though the answer comes from a committed "
            "snapshot — the same shape as the Ariregister bug. Fix by "
            "recording a snapshot observation on the index hit, then tighten "
            "expect_liveness here."
        ),
    ),
    # --- curated fixtures -------------------------------------------------
    "cac_nigeria": _p(
        tier="curated",
        subject="Dangote Cement PLC",
        method="fetch_by_lei",
        args=("029200697A0R1BH0A835",),
        expect_liveness=frozenset({"curated"}),
        anchor_lei="029200697A0R1BH0A835",
        bods_mapper="map_cac_nigeria",
        notes="Offline by design — the CAC's API is restricted to Nigerian government agencies.",
    ),
    "eiti_bo": _p(
        tier="curated",
        subject="Dangote Cement PLC (EITI pooled BO)",
        method="fetch_by_lei",
        args=("029200697A0R1BH0A835",),
        expect_liveness=frozenset({"curated"}),
        anchor_lei="029200697A0R1BH0A835",
        bods_mapper="map_eiti_bo",
    ),
    # --- registered but env-gated ----------------------------------------
    "bce_belgium": _p(
        tier="inactive",
        subject="Belgian enterprise by number",
        args=("0403019488",),
        expect_liveness=frozenset({"live", "cached", "snapshot", "stub"}),
        requires_env=("BCE_BELGIUM_DB_FILE",),
        bods_mapper="map_bce_belgium",
        known_gap=(
            "Reads a local SQLite snapshot but records no provenance "
            "observation, so an answer resolves 'stub' — an Ariregister-class "
            "defect waiting for the day the source is activated. Fix by "
            "recording a snapshot observation, then tighten expect_liveness."
        ),
    ),
}


def missing_env(probe: SourceProbe, environ: dict[str, str]) -> tuple[str, ...]:
    """Which of the probe's required settings are absent or blank."""
    return tuple(name for name in probe.requires_env if not environ.get(name, "").strip())


def key_gated_ids() -> frozenset[str]:
    """Registry ids whose probe needs at least one credential to run."""
    return frozenset(sid for sid, probe in PROBES.items() if probe.requires_env)


#: Guard against a secret quietly disappearing. If more sources skip for want
#: of a credential than this, the sweep fails rather than reporting green over
#: reduced coverage. Lower it deliberately, in the commit that adds the secret.
MAX_SKIPPED_FOR_CREDENTIALS = len(key_gated_ids())

__all__ = [
    "LIVE",
    "LIVE_OR_CACHED",
    "MAX_SKIPPED_FOR_CREDENTIALS",
    "PROBES",
    "SourceProbe",
    "Tier",
    "key_gated_ids",
    "missing_env",
]
