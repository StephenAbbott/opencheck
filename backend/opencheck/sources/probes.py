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

For an adapter whose provenance or coverage is knowingly wrong and not yet
fixed, ``known_gap`` lets the probe assert today's behaviour while the report
prints the defect, so it stays visible instead of being asserted away. No probe
currently needs it: the two gaps this table originally recorded — ``eiti_soe``
over-claiming ``live`` for an index-derived answer, and ``bce_belgium``
recording nothing at all — were both closed in Phase 121. Closing a gap means
fixing the adapter and tightening ``expect_liveness`` in the same commit.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from collections.abc import Mapping
from typing import Any, Literal

from .base import SearchKind

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

    Choose fields **only the source itself can supply**. Several adapters echo
    the GLEIF ``legal_name`` into their bundle as a display fallback, so
    asserting it proves nothing about the register — that is precisely the
    Lithuania failure mode, where a 403 yields a bundle carrying the GLEIF name
    and null register fields that reads as a successful lookup from outside.
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

    requires_env_files: tuple[str, ...] = ()
    """Settings that name a FILE, which must exist for the source to answer.

    A configured path is not a working source. `BCE_BELGIUM_DB_FILE` pointing
    at a SQLite build that was never made is the case that taught this: the
    setting is present, so the credential check passed, the adapter fell back
    to a stub, and the probe reported a provenance failure — technically true,
    and completely misleading about the cause. Missing file is a skip with the
    path named, not a failure.
    """

    requires_files: tuple[str, ...] = ()
    """Data-root-relative paths the adapter needs. Missing → skipped.

    ``data/gem/`` is gitignored, so ClimateTRACE's ownership artifacts are
    absent from a fresh CI checkout — that is a skip, not a failure.
    """

    snapshot_max_age_days: int | None = None
    """Report `degraded: refresh due` once the snapshot is older than this.

    Snapshot and curated sources don't fail with a 500 — they age out in
    silence, which is how a committed index quietly stops reflecting the
    register. The check uses the retrieval time the adapter records (the
    index's own ``built`` / ``harvested`` / ``source_snapshot`` date, never a
    file mtime), so it asks "how old is this data?" rather than "when did git
    write this file?".

    Deliberately generous: a prompt to re-run the builder, not an SLA.
    """

    freshness_url: str | None = None
    """Optional upstream bulk file to HEAD for a `Last-Modified` newer than
    ours. Only meaningful where the source publishes a stable file URL; the
    committed EITI and CAC indexes are built from portals and APIs, so they
    rely on the age check above instead."""

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
        subject="Commonwealth Bank of Australia (ACN)",
        args=("123123124",),
        requires_env=("ABN_GUID",),
        anchor_lei="MSFSBD3QN1GSN7Q6C537",
        bods_mapper="map_abr_australia",
        notes=(
            "Phase 163: probed by ACN, not ABN, because that is how the pipeline reaches it — CBA's own "
            "GLEIF record is registered at ASIC (RA000014) as '123 123 124', and the adapter routes a "
            "9-digit identifier to AcnDetails. The earlier note that only aircraft-leasing SPVs matched "
            "was wrong: a fulltext search surfaces the SPVs first, the bank's record is MSFSBD3QN1GSN7Q6C537."
        ),
    ),
    "ares": _p(
        tier="live",
        subject="ČEZ Energy, a. s.",
        args=("29700949",),
        expect_fields=("name", "entity", "directors"),
        anchor_lei="315700JE48EH8QJ95N70",
        bods_mapper="map_ares",
    ),
    "ariregister": _p(
        tier="live",
        subject="Eesti Energia AS",
        args=("10421629",),
        anchor_lei="5493005044RTLQ5RZU70",
        expect_fields=("name", "status", "officers", "beneficial_owners"),
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
        anchor_lei="549300W9JLPW15XIFM52",
        requires_env=("BOLAGSVERKET_API_KEY", "BOLAGSVERKET_CLIENT_SECRET"),
        bods_mapper="map_bolagsverket",
    ),
    "brreg": _p(
        tier="live",
        subject="Equinor ASA",
        args=("923609016",),
        anchor_lei="OW6OFBNCKXC4US5C7523",
        expect_fields=("entity", "roles"),
        bods_mapper="map_brreg",
    ),
    "cnpj_brazil": _p(
        tier="live",
        subject="Petróleo Brasileiro S.A. (Petrobras)",
        args=("33000167000101",),
        anchor_lei="5493000J801JZRCMFE49",
        expect_fields=("company", "partners"),
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
        subject="Canadian National Railway Company",
        args=("0105333",),
        requires_env=("CORPORATIONS_CANADA_API_KEY",),
        anchor_lei="3SU7BEP7TH9YEQOZCS77",
        bods_mapper="map_corporations_canada",
    ),
    "cro": _p(
        tier="live",
        subject="Ryanair Finance DAC",
        args=("633425",),
        expect_fields=("company",),
        anchor_lei="635400UKS1476OXEKM35",
        bods_mapper="map_cro",
    ),
    "cvr_denmark": _p(
        tier="live",
        subject="Ørsted Services A/S",
        args=("27446485",),
        requires_env=("CVR_DENMARK_API_KEY",),
        anchor_lei="213800SBT2QLGP2Y4974",
        bods_mapper="map_cvr_denmark",
    ),
    "firmenbuch": _p(
        tier="live",
        subject="OMV Austria Exploration & Production GmbH",
        args=("241929d",),
        requires_env=("FIRMENBUCH_API_KEY",),
        anchor_lei="5493004CC7P3EWMB7033",
        bods_mapper="map_firmenbuch",
    ),
    "gemi_greece": _p(
        tier="live",
        subject="ΓΚΟΛΕΜΗΣ ΕΤΑΙΡΕΙΑ ΑΕΡΟΠΟΡΙΚΩΝ ΕΞΥΠΗΡΕΤΗΣΕΩΝ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ",
        args=("003031801000",),
        requires_env=("GEMI_API_KEY",),
        expect_fields=("company",),
        anchor_lei="635400NMLGFBATPGJD19",
        bods_mapper="map_gemi_greece",
        notes=(
            "Zero-padded Αριθμός ΓΕΜΗ, exactly as GLEIF stores registeredAs — "
            "the API accepts the padded and unpadded forms interchangeably, so "
            "the probe uses the padded one the lookup pipeline actually passes. "
            "20 requests/minute (raised from 8 on 2026-09-03): a sweep that "
            "also probes another Greek subject must pace itself."
        ),
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
        subject="Bolloré Participations SE",
        args=("352730394",),
        anchor_lei="9695009YHLBEVMOOCF13",
        requires_env=("INPI_USERNAME", "INPI_PASSWORD"),
        bods_mapper="map_inpi",
    ),
    "jar_lithuania": _p(
        tier="live",
        subject="AB Ignitis grupė",
        args=("301844044",),
        kwargs={"legal_name": "Ignitis grupe"},
        expect_fields=("status",),
        anchor_lei="5493005RZJHJT5PNHY10",
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
        subject="ORLEN Spółka Akcyjna",
        args=("0000028860",),
        anchor_lei="259400VVMM70CQREJT74",
        expect_fields=("name", "legal_form", "registration_date"),
        bods_mapper="map_krs_poland",
    ),
    "kvk": _p(
        tier="live",
        subject="ASML Holding",
        args=("17085815",),
        anchor_lei="724500Y6DUVHQD6OXN27",
        expect_fields=("company",),
        bods_mapper="map_kvk",
    ),
    "malta_mbr": _p(
        tier="live",
        subject="Bank of Valletta p.l.c. (C 2833)",
        args=("C 2833",),
        expect_fields=("company",),
        anchor_lei="529900RWC8ZYB066JF16",
        bods_mapper="map_malta_mbr",
        notes=(
            "Phase 163: subject changed from an anonymous CRN to Malta's largest bank, incorporated 1974, "
            "whose GLEIF record is registered at RA000443 as 'C 2833'. The earlier note that no GLEIF "
            "record was registered there was wrong — the API's registeredAt filter returns nothing, but "
            "nearly every Maltese company record carries RA000443."
        ),
    ),
    "mca_india": _p(
        tier="live",
        subject="Infosys Limited",
        args=("L85110KA1981PLC013115",),
        requires_env=("DATA_GOV_IN_API_KEY",),
        anchor_lei="335800TYLGG93MM7PR89",
        bods_mapper="map_mca_india",
        notes=(
            "Phase 163: Infosys's own GLEIF record is registered at RA000394 with the CIN verbatim as "
            "registeredAs, so the anchor derives the probe subject exactly. The earlier note said no such "
            "record existed."
        ),
    ),
    "nz_companies": _p(
        tier="live",
        subject="Fonterra Co-operative Group",
        args=("1166320",),
        anchor_lei="549300NCQQ9E4O5JX172",
        requires_env=("NZBN_API_KEY",),
        bods_mapper="map_nz_companies",
    ),
    "prh": _p(
        tier="live",
        subject="Neste Oyj",
        args=("1852302-9",),
        expect_fields=("company",),
        anchor_lei="5493009GY1X8GQ66AM14",
        bods_mapper="map_prh",
    ),
    "rpo_slovakia": _p(
        tier="live",
        subject="Slovenský plynárenský priemysel, a.s.",
        args=("35815256",),
        expect_fields=("name", "status", "source_register"),
        anchor_lei="529900BJGD0X650NVB68",
        bods_mapper="map_rpo_slovakia",
        notes=(
            "Two things learned from the first sweep. The original subject "
            "(IČO 31320155) resolves to a *dissolved branch* of VÚB banka "
            "rather than the live parent, which is worth a look on its own. "
            "And `address` is None for every active entity tried — SPP, "
            "Slovenská pošta, Slovnaft, Slovak Telekom — so it is either "
            "absent upstream or unparsed, and cannot be asserted."
        ),
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
        args=("080000604",),
        anchor_lei="213800RUSOIJPJD19H13",
        requires_env=("SUDREG_CLIENT_ID", "SUDREG_CLIENT_SECRET"),
        bods_mapper="map_sudreg_croatia",
    ),
    "ur_latvia": _p(
        tier="live",
        subject="Akciju sabiedrība \"Latvenergo\"",
        args=("40003032949",),
        expect_fields=("entity", "officers"),
        anchor_lei="213800DJRB539Q1EMW75",
        bods_mapper="map_ur_latvia",
        notes=(
            "The original subject (regcode 40003009556, taken from a test "
            "fixture) is not in the register: it returned a bundle with an "
            "empty entity, no officers and `is_stub: False` — the same "
            "hollow-answer shape as the Lithuania failure, but caused by a "
            "dead fixture rather than a dead source. Beneficial owners are "
            "not asserted: a state-owned utility legitimately has none."
        ),
    ),
    "zefix": _p(
        tier="live",
        subject="Nestlé S.A.",
        args=("CHE105909036",),
        requires_env=("ZEFIX_USERNAME", "ZEFIX_PASSWORD"),
        anchor_lei="KY37LUS27QQX7BB93L28",
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
        subject="Rosneft (sanctions search)",
        method="search",
        args=("Rosneft", SearchKind.ENTITY),
        requires_env=("OPENSANCTIONS_API_KEY",),
        bods_mapper="map_opensanctions",
        notes=(
            "Searched, not fetched by id. The original probe fetched "
            "`NK-rosneft` and got a 404 — and OpenSanctions 308-redirects a "
            "MERGED id, so a 404 means that id never existed; it came from an "
            "respx-mocked test file. Canonical ids also churn as entities are "
            "deduplicated, so a search for a name that must always match "
            "something is the stable way to exercise this API. An empty result "
            "is a failure: if Rosneft stops matching, screening is broken."
        ),
    ),
    "everypolitician": _p(
        tier="live",
        subject="A head of state (PEP search)",
        method="search",
        args=("Vladimir Putin", SearchKind.PERSON),
        requires_env=("OPENSANCTIONS_API_KEY",),
        bods_mapper="map_everypolitician",
        notes=(
            "Same story as opensanctions: the fetch-by-id probe used "
            "`Q7747-pep`, which 404s. Person-kind search instead — the "
            "adapter returns [] for entity searches. No personal detail "
            "reaches the report, which carries source ids and statuses only."
        ),
    ),
    "sec_edgar": _p(
        tier="live",
        subject="US issuer by CIK",
        expect_fields=("issuer_cik", "coverage_note"),
        allow_empty=True,
        args=("1793659",),
        bods_mapper="map_sec_edgar",
    ),
    "wikidata": _p(
        tier="live",
        subject="Unilever (Q152057)",
        args=("Q152057",),
        expect_fields=("qid", "bindings"),
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
        expect_fields=("entity_name", "organisations"),
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
        expect_liveness=frozenset({"snapshot"}),
        expect_fields=("entity_name", "is_state_owned", "country"),
        snapshot_max_age_days=180,
        bods_mapper="map_eiti_soe",
        notes=(
            "Snapshot, not live: the payment rows are fetched live but the "
            "bundle's central assertion — that this company is an SOE — comes "
            "from the committed index, and provenance takes the worst liveness "
            "across a fetch. Before this was recorded the adapter over-claimed "
            "'live' on the strength of the payments alone."
        ),
    ),
    # --- curated fixtures -------------------------------------------------
    "cac_nigeria": _p(
        tier="curated",
        subject="Dangote Cement PLC",
        method="fetch_by_lei",
        args=("029200697A0R1BH0A835",),
        expect_fields=("lei", "record"),
        expect_liveness=frozenset({"curated"}),
        anchor_lei="029200697A0R1BH0A835",
        snapshot_max_age_days=240,
        bods_mapper="map_cac_nigeria",
        notes="Offline by design — the CAC's API is restricted to Nigerian government agencies.",
    ),
    "eiti_bo": _p(
        tier="curated",
        subject="Dangote Cement PLC (EITI pooled BO)",
        method="fetch_by_lei",
        args=("029200697A0R1BH0A835",),
        expect_fields=("lei", "record", "register_name"),
        expect_liveness=frozenset({"curated"}),
        anchor_lei="029200697A0R1BH0A835",
        snapshot_max_age_days=240,
        bods_mapper="map_eiti_bo",
    ),
    # --- registered but env-gated ----------------------------------------
    "bce_belgium": _p(
        tier="inactive",
        subject="Proximus",
        args=("0202239951",),
        expect_liveness=frozenset({"snapshot"}),
        expect_fields=("name", "enterprise_number"),
        requires_env=("BCE_BELGIUM_DB_FILE",),
        requires_env_files=("BCE_BELGIUM_DB_FILE",),
        anchor_lei="549300CWRXC5EP004533",
        bods_mapper="map_bce_belgium",
        notes=(
            "Reads a local KBO Open Data build. It recorded nothing at all "
            "until Phase 121 — an Ariregister-class defect that would have "
            "badged the data 'Placeholder' the day the source was switched on."
        ),
    ),
}


def configured_credentials() -> dict[str, str]:
    """Every credential the app can see, however it was supplied.

    Resolved through ``Settings`` rather than ``os.environ``, because OpenCheck
    loads ``.env`` from the project root: a key present only there is real as
    far as every adapter is concerned, and reading the raw environment would
    report it "not configured" and skip the source. That is the difference
    between a local run testing nothing and testing everything — CI never sees
    it, since Actions supplies the same names as real environment variables.
    """
    from ..config import get_settings

    settings = get_settings()
    out: dict[str, str] = {}
    for field_name, field in type(settings).model_fields.items():
        alias = field.alias or field_name
        value = getattr(settings, field_name, None)
        if isinstance(value, str) and value.strip():
            out[alias] = value
    return out


def missing_env(probe: SourceProbe, environ: dict[str, str] | None = None) -> tuple[str, ...]:
    """Which of the probe's required settings are absent or blank."""
    available = dict(configured_credentials())
    if environ:
        available.update({k: v for k, v in environ.items() if v and v.strip()})
    return tuple(name for name in probe.requires_env if not available.get(name, "").strip())


def key_gated_ids() -> frozenset[str]:
    """Registry ids whose probe needs at least one credential to run."""
    return frozenset(sid for sid, probe in PROBES.items() if probe.requires_env)


#: Guard against a secret quietly disappearing. If more sources skip for want
#: of a credential than this, the sweep fails rather than reporting green over
#: reduced coverage. Lower it deliberately, in the commit that adds the secret.
#:
#: 2026-08-22: every credential is now a repository secret except
#: ``DATA_GOV_IN_API_KEY`` (India MCA, blocked on JanParichay registration), so
#: exactly one source may legitimately skip. Any more means a secret expired or
#: was deleted.
MAX_SKIPPED_FOR_CREDENTIALS = 1

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
