"""Deterministic risk-signal rules.

OpenCheck never invents risk — it surfaces what the open data already
asserts. Every rule is keyed off either a raw source payload (topics,
collections, positions) or BODS v0.4 statements assembled by the
mapper, with the explicit goal of mirroring the AMLA CDD RTS (currently
under EU consultation).

Source-derived signals
======================

* ``PEP`` — politically exposed person.
  Fires when:
    - OpenSanctions hit has a ``role.pep`` family topic
    - EveryPolitician hit (the dataset is, by construction, PEPs only)
    - Wikidata person bundle (``/deepen``) has at least one position
      with no end date — i.e. a *currently held* office

* ``SANCTIONED`` — the entity is itself the subject of a sanctions listing.
  Fires only when an OpenSanctions hit/bundle has a direct ``sanction``
  topic.

* ``COUNTER_SANCTIONED`` — designated under a counter-sanctions regime.
  Fires on the OpenSanctions ``sanction.counter`` topic: a listing issued
  by a state with weak democratic institutions, typically as a punitive
  response to foreign sanctions or to suppress domestic opposition.
  Structurally a direct listing of the entity — which is why it sat inside
  ``SANCTIONED`` until Phase 105 — but a materially different fact. It is
  not a designation by an authority most users owe an obligation to, and
  appearing on such a list is frequently a *consequence* of journalism,
  sanctions enforcement or human-rights work. Ranked at the bottom of the
  sanctions ladder in a deliberately non-red palette so it reads as
  context rather than as an adverse finding.

* ``SANCTIONS_CONTROLLED`` — inside a sanctioned party's ownership chain:
  a direct or indirect subsidiary, asset or vessel. Fires on the
  OpenSanctions ``sanction.control`` topic, which carries no percentage
  threshold and no depth limit (an end-dated holding stops the chain).
  The starting point for an ownership-and-control test such as OFAC's
  50 Percent Rule — not the answer to one. Ranked between ``SANCTIONED``
  and ``SANCTIONS_LINKED``: being *owned by* a designated party is a
  materially stronger fact than standing next to one.

* ``SANCTIONS_LINKED`` — adjacent to a sanctioned party but neither
  designated nor owned by one. Fires on the OpenSanctions
  ``sanction.linked`` topic (and, conservatively, on any unrecognised
  ``sanction.*`` subtopic). Kept distinct from ``SANCTIONED`` so an
  associated entity (e.g. Vale S.A.) is never reported as sanctioned, and
  suppressed when ``SANCTIONS_CONTROLLED`` fires because upstream declares
  it a superset of ``sanction.control``.

* ``DEBARMENT`` — debarred / excluded from public contracts or procurement
  (e.g. World Bank, AfDB, EU debarment lists). Fires on the OpenSanctions
  ``debarment`` topic. A confirmed adverse listing, distinct from sanctions.

* ``OFFSHORE_LEAKS`` — appears in an ICIJ-style leak.
  Fires for OpenAleph hits whose collection is one of the known leak
  collections (Panama / Paradise / Pandora / Bahamas / Offshore Leaks).

* ``OPAQUE_OWNERSHIP`` — a party exists whose identity is deliberately
  withheld or could not be obtained. Fires on: GLEIF ``NON_PUBLIC``-family
  reporting exceptions (a known consolidating parent is not published);
  ``anonymousPerson`` / ``anonymousEntity`` statements (e.g. Companies
  House super-secure PSCs); and relationships whose unspecified
  ``interestedParty`` reason says an owner exists but is unidentified or
  withholding information. Deliberately does NOT fire on ``unknownPerson``
  / ``unknownEntity`` statements or on GLEIF's benign exception reasons —
  see ``GLEIF_REPORTING_EXCEPTION``.

* ``GLEIF_REPORTING_EXCEPTION`` — ``kind="context"``. A GLEIF Level 2
  reporting exception from the benign/structural family
  (``NATURAL_PERSONS``, ``NO_KNOWN_PERSON``, ``NON_CONSOLIDATING``,
  ``NO_LEI``): permitted reasons under the LEI ROC policy for having no
  accounting-consolidation parent record. Informational only — GLEIF
  relationships record accounting consolidation, not beneficial
  ownership, and only entities (never people) are identified in them.

AMLA CDD RTS signals (BODS v0.4 derived)
========================================

Mirror of the objective conditions in AMLA's draft CDD RTS for
"complex corporate structures". Each fires independently so a UI can
show them as discrete chips, and a composite ``COMPLEX_CORPORATE_STRUCTURE``
fires when the AMLA "≥3 layers + ≥1 of {trust, non-EU, nominee}"
threshold is met.

* ``TRUST_OR_ARRANGEMENT`` — any ``entityStatement`` whose entityType is
  ``arrangement``, or whose ``legalForm``/``entitySubtype``/``details``
  mentions ``trust``, ``foundation``, ``stiftung`` or ``anstalt``.
  Maps to AMLA condition (a).
* ``NON_EU_JURISDICTION`` — any ``entityStatement.jurisdiction.code``
  outside the EU+EEA set. Maps to AMLA condition (b).
* ``NOMINEE`` — any ``relationshipStatement`` with an interest type or
  ``details`` field mentioning ``nominee``, or a ``personStatement``
  whose names/details mention nominee. Maps to AMLA condition (c).
* ``COMPLEX_OWNERSHIP_LAYERS`` — the longest chain of entity nodes in
  the BODS relationship graph has ≥3 corporate layers.
* ``COMPLEX_CORPORATE_STRUCTURE`` — composite, fires when
  ``COMPLEX_OWNERSHIP_LAYERS`` and **≥2** of AMLA conditions (a)–(c)
  {``TRUST_OR_ARRANGEMENT``, non-EU on the layered path, ``NOMINEE``}
  are met. Article 12(1) of the draft RTS requires "three or more
  layers ... and, in addition, **more than one** of the following
  conditions" — i.e. at least two. Condition (d) is deliberately not
  counted here; see ``POSSIBLE_OBFUSCATION`` below.
* ``POSSIBLE_OBFUSCATION`` — advisory mirror of AMLA's subjective
  condition ("structure obfuscates or diminishes transparency of
  ownership with no legitimate economic rationale"). Cannot be judged
  from data alone — fires ``low`` when ``OPAQUE_OWNERSHIP`` plus
  non-EU layer or nominee are present, with the summary explicitly
  noting that a human should confirm legitimate rationale.

Each signal is intentionally explained — confidence + a one-line
``summary`` + the ``evidence`` dict — because users want to be told
*why* something is flagged, not just see a red dot.

Confidence ladder
-----------------

* ``high`` — the source asserts it directly (e.g. ``topic == sanction``,
  ``entityType == arrangement``).
* ``medium`` — strong proxy (e.g. ICIJ leak collection, BODS chain
  meeting the layer threshold).
* ``low`` — advisory inference, requires human review (e.g.
  ``POSSIBLE_OBFUSCATION``).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Iterable

from .bods.mapper import GLEIF_UNDISCLOSED_REASONS
from .bods.mapper import _stable_id as _bods_stable_id
from .bods.nominees import NOMINEE_NATURE_CODES, is_nominee_nature
from .config import get_settings
from .sources import SearchKind, SourceHit

_LOG = logging.getLogger(__name__)


# Codes — source-derived
PEP = "PEP"
SANCTIONED = "SANCTIONED"
COUNTER_SANCTIONED = "COUNTER_SANCTIONED"
SANCTIONS_CONTROLLED = "SANCTIONS_CONTROLLED"
SANCTIONS_LINKED = "SANCTIONS_LINKED"
DEBARMENT = "DEBARMENT"
OFFSHORE_LEAKS = "OFFSHORE_LEAKS"
OPAQUE_OWNERSHIP = "OPAQUE_OWNERSHIP"

# Code — GLEIF Level 2 reporting exception, benign family (BODS/raw-derived).
# ``kind="context"``: the LEI ROC policy defines these as *permitted* reasons
# not to report an accounting-consolidation parent (controlled directly by
# natural persons, diversified shareholding, non-consolidating parents, or a
# parent without an LEI). Surfaced so a reviewer sees WHY no parent appears
# in GLEIF — but never presented as a risk finding.
GLEIF_REPORTING_EXCEPTION = "GLEIF_REPORTING_EXCEPTION"

# Code — ownership structure (BODS-derived). Fires when an owner / controlling
# party is modelled as a state or state body (BODS entityType 'state'/'stateBody',
# per the SOE modelling requirement). Presence-only, corroborating — currently
# sourced from Wikidata, which is crowd-sourced and famous-names-only, so its
# absence means nothing. Medium confidence; not part of the AMLA composite.
STATE_CONTROLLED = "STATE_CONTROLLED"

# Codes — AMLA CDD RTS (BODS-derived)
TRUST_OR_ARRANGEMENT = "TRUST_OR_ARRANGEMENT"
NON_EU_JURISDICTION = "NON_EU_JURISDICTION"
NOMINEE = "NOMINEE"
COMPLEX_OWNERSHIP_LAYERS = "COMPLEX_OWNERSHIP_LAYERS"
COMPLEX_CORPORATE_STRUCTURE = "COMPLEX_CORPORATE_STRUCTURE"
POSSIBLE_OBFUSCATION = "POSSIBLE_OBFUSCATION"

# Codes — jurisdiction-list signals (BODS-derived).
#
# These are the ONLY geographic risk claims OpenCheck makes, and each
# comes from an authoritative, externally maintained, dated list. Being
# outside the EU is not itself one of them — see NON_EU_JURISDICTION,
# which is classified ``kind="context"``.
FATF_BLACK_LIST = "FATF_BLACK_LIST"
FATF_GREY_LIST = "FATF_GREY_LIST"
EU_HIGH_RISK_THIRD_COUNTRY = "EU_HIGH_RISK_THIRD_COUNTRY"


# Default EU + EEA member states (ISO 3166-1 alpha-2). The AMLA RTS
# scopes "outside the European Union" — we extend with EEA (NO/IS/LI)
# because they share AML supervisory frameworks under the EU's
# third-country regime, which most practitioners include here. Keep this
# list visible rather than buried so reviewers can audit it.
#
# Operators can adjust this at runtime via two env vars:
#
# * ``OPENCHECK_AMLA_EQUIVALENT_JURISDICTIONS`` — comma-separated codes
#   ADDED to the default set (e.g. ``GB,CH`` for UK + Swiss equivalence).
# * ``OPENCHECK_AMLA_EU_EEA_OVERRIDE`` — when set, REPLACES the default
#   set entirely. Use only when you want strict AMLA EU-only or a fully
#   custom basis.
DEFAULT_EU_EEA_COUNTRY_CODES: frozenset[str] = frozenset(
    {
        # EU-27
        "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR",
        "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL",
        "PL", "PT", "RO", "SK", "SI", "ES", "SE",
        # EEA non-EU
        "IS", "LI", "NO",
    }
)

# Back-compat alias — older code (and external callers) imported the
# original constant. Keep the name pointing at the defaults; the rule
# itself now resolves at call-time via ``_eu_eea_codes()``.
EU_EEA_COUNTRY_CODES = DEFAULT_EU_EEA_COUNTRY_CODES


def _split_codes(raw: str | None) -> set[str]:
    if not raw:
        return set()
    return {part.strip().upper() for part in raw.split(",") if part.strip()}


def _eu_eea_codes() -> frozenset[str]:
    """Resolve the active EU+EEA-equivalent jurisdiction set.

    Reads settings every call (settings is itself ``lru_cache``'d, so
    this is cheap and stays in sync if the cache is cleared in tests).
    """
    settings = get_settings()
    if settings.amla_eu_eea_override is not None:
        return frozenset(_split_codes(settings.amla_eu_eea_override))
    extras = _split_codes(settings.amla_equivalent_jurisdictions)
    if not extras:
        return DEFAULT_EU_EEA_COUNTRY_CODES
    return frozenset(DEFAULT_EU_EEA_COUNTRY_CODES | extras)

# FATF High-Risk Jurisdictions subject to a Call for Action ("black list")
# as of February 2026 — Democratic People's Republic of Korea, Iran, Myanmar.
# Source: https://www.fatf-gafi.org/en/countries/black-and-grey-lists.html
FATF_BLACK_LIST_CODES: frozenset[str] = frozenset({"KP", "IR", "MM"})

# FATF Jurisdictions under Increased Monitoring ("grey list") as of the
# June 2026 plenary (19 June 2026).  Note that Bulgaria (BG) is an EU
# member-state — if NON_EU_JURISDICTION is suppressed via
# OPENCHECK_AMLA_EQUIVALENT_JURISDICTIONS, FATF_GREY_LIST will still fire for it.
# June 2026 changes: added Bosnia and Herzegovina (BA) and Iraq (IQ); removed
# Algeria (DZ) and Namibia (NA).
# Source: https://www.fatf-gafi.org/en/publications/High-risk-and-other-monitored-jurisdictions/increased-monitoring-june-2026.html
FATF_GREY_LIST_CODES: frozenset[str] = frozenset(
    {
        "AO",  # Angola
        "BA",  # Bosnia and Herzegovina
        "BO",  # Bolivia
        "BG",  # Bulgaria
        "CM",  # Cameroon
        "CI",  # Côte d'Ivoire
        "CD",  # Democratic Republic of Congo
        "HT",  # Haiti
        "IQ",  # Iraq
        "KE",  # Kenya
        "KW",  # Kuwait
        "LA",  # Laos (Lao PDR)
        "LB",  # Lebanon
        "MC",  # Monaco
        "NP",  # Nepal
        "PG",  # Papua New Guinea
        "SS",  # South Sudan
        "SY",  # Syria
        "VE",  # Venezuela
        "VN",  # Vietnam
        "VG",  # British Virgin Islands
        "YE",  # Yemen
    }
)

# EU high-risk third countries — the Annex to Commission Delegated
# Regulation (EU) 2016/1675, as amended. This is a DIFFERENT INSTRUMENT
# from the FATF lists above and is deliberately a separate signal, not a
# widening of the FATF code sets.
#
# Why separate: the EU list is the legally decisive trigger for mandatory
# enhanced due diligence under the AML framework; a FATF listing is not,
# by itself, binding in EU law. The two also diverge in practice, because
# the Commission adopts its updates months after the FATF plenary that
# prompted them:
#
#   * DZ (Algeria) and NA (Namibia) are on this list but were REMOVED
#     from the FATF grey list at the June 2026 plenary.
#   * MM (Myanmar) is FATF black but sits in the EU's Section I.
#   * BA, BG, IQ, KW, PG are FATF grey but not EU-listed.
#
# Folding them together would make the summary text unable to say which
# instrument applies, and a delisting from one would silently move a
# signal attributed to the other.
#
# Current as of Delegated Regulation (EU) 2026/83 of 4 December 2025
# (published 9 January 2026, applies from 29 January 2026), which added
# Bolivia and the British Virgin Islands and removed Burkina Faso, Mali,
# Mozambique, Nigeria, South Africa and Tanzania. Verified against the
# EUR-Lex consolidated text 02016R1675-20260129.
#
# Re-check after each Commission update — they follow FATF plenaries
# (typically February, June, October) at a lag of several months.
# Source: https://eur-lex.europa.eu/eli/reg_del/2026/83/oj/eng
EU_HRTC_INSTRUMENT = "Delegated Regulation (EU) 2016/1675, as amended to 29 January 2026"

EU_HIGH_RISK_THIRD_COUNTRY_CODES: frozenset[str] = frozenset(
    {
        # Section I — written commitment + FATF action plan
        "AF",  # Afghanistan
        "DZ",  # Algeria
        "AO",  # Angola
        "BO",  # Bolivia
        "VG",  # British Virgin Islands
        "CM",  # Cameroon
        "CI",  # Côte d'Ivoire
        "CD",  # Democratic Republic of the Congo
        "HT",  # Haiti
        "KE",  # Kenya
        "LA",  # Laos
        "LB",  # Lebanon
        "MC",  # Monaco
        "MM",  # Myanmar
        "NA",  # Namibia
        "NP",  # Nepal
        "SS",  # South Sudan
        "SY",  # Syria
        "TT",  # Trinidad and Tobago
        "VU",  # Vanuatu
        "VE",  # Venezuela
        "VN",  # Vietnam
        "YE",  # Yemen
        # Section II — seeking technical assistance
        "IR",  # Iran
        # Section III — ongoing and substantial risk
        "KP",  # Democratic People's Republic of Korea
    }
)

# Free-text fragments that signal a trust / non-corporate arrangement
# in legal-form / details fields. Lower-cased.
_TRUST_LEGAL_FORM_FRAGMENTS = (
    "trust",
    "foundation",
    "stiftung",
    "anstalt",
    "fideicomiso",  # ES/LATAM trust
    "treuhand",     # German trust-equivalent
)

# "Nominee" terms across English / common European legal-vocabulary.
# Nominee arrangements come in two grades of evidence, and OpenCheck should not
# pretend they are the same thing.
#
# STRUCTURED: a register states it with a code. The Register of Overseas
# Entities' six registered-owner-as-nominee-* nature-of-control codes are the
# case OpenCheck can read today (see bods/psc_natures.NOMINEE_NATURE_CODES).
#
# TEXTUAL: the word turns up in a name or a free-text descriptor. Real evidence,
# but weaker — "Nominee Services Ltd" is a company name, not a filed fact.
#
# Until this phase there was only the textual path, and it worked on UK ROE
# filings *by accident*: the mapper renders each nature code as an English
# descriptor into interest.details, and those descriptors happen to contain the
# word "nominee". A register stating the identical fact in a code, a boolean or
# another language was invisible. The fragments below stay as the fallback for
# every source that publishes only prose.
_NOMINEE_FRAGMENTS = (
    "nominee",
    "nomineeshareholder",
    "nominee shareholder",
    "nomineedirector",
    "nominee director",
    "prête-nom",
    "prete-nom",
    "fiduciaire",
)


# OpenSanctions topic taxonomy. Anything in the "role.pep" family — pep,
# rca (relative or close associate), spouse, family — is treated as a
# PEP signal.
_PEP_TOPICS = {"role.pep", "role.rca", "role.spouse", "role.family"}

# Sanction-family topics carry very different meanings and must NOT be
# conflated (this is exactly the Vale S.A. false positive):
#   * "sanction"          — the entity is itself the subject of a sanctions
#                           listing.
#   * "sanction.counter"  — the entity is listed under a counter-sanctions
#                           regime: a designation issued by a state with weak
#                           democratic institutions, usually retaliation for
#                           foreign sanctions or suppression of domestic
#                           opposition. Structurally a direct listing of the
#                           entity, which is why it was bundled with
#                           "sanction" until Phase 105 — but not the same
#                           fact, and not one that should render red.
#   * "sanction.control"  — the entity is a direct or *indirect* subsidiary,
#                           asset or vessel of a sanctioned party. Any stake,
#                           any depth; an end-dated holding stops the chain.
#                           OpenSanctions applies no percentage threshold, so
#                           this is the starting point for an ownership-and-
#                           control test (OFAC's 50 Percent Rule and friends),
#                           not the answer to one.
#   * "sanction.linked"   — plain one-hop adjacency to a sanctioned party
#                           (ownership, directorship, membership, employment,
#                           association, family, succession, securities).
#                           Declared upstream as a SUPERSET of
#                           "sanction.control", so a controlled entity always
#                           carries both topics.
#
# Taxonomy per https://www.opensanctions.org/docs/topics/ and
# https://www.opensanctions.org/articles/2026-08-13-sanction-control/.
#
# Direct listings → SANCTIONED; counter-designations → COUNTER_SANCTIONED.
# An unrecognised "sanction.*" subtopic is still treated as linked
# (conservative — we never assert a listing we can't confirm), but it lands in
# ``SanctionTopics.unknown`` and is logged, so a new upstream subtopic can no
# longer be absorbed *silently* the way "sanction.control" was.
_DIRECT_SANCTION_TOPICS = frozenset({"sanction"})
_COUNTER_SANCTION_TOPICS = frozenset({"sanction.counter"})
_CONTROL_SANCTION_TOPICS = frozenset({"sanction.control"})
_LINKED_SANCTION_TOPICS = frozenset({"sanction.linked"})
_KNOWN_SANCTION_TOPICS = (
    _DIRECT_SANCTION_TOPICS
    | _COUNTER_SANCTION_TOPICS
    | _CONTROL_SANCTION_TOPICS
    | _LINKED_SANCTION_TOPICS
)
_SANCTION_TOPIC_PREFIX = "sanction"


@dataclass(frozen=True)
class SanctionTopics:
    """One record's sanction-family topics, split by what they mean.

    Every field is a sorted tuple so it can go straight into a signal's
    ``evidence``. ``unknown`` collects ``sanction.*`` subtopics this build
    does not recognise; callers must keep treating those as linked.

    ``counter`` is kept out of ``direct`` deliberately. Both are listings of
    the entity itself, but a counter-sanction is issued by a regime the user
    almost certainly owes no obligation to, so merging them let a Russian
    MFA retaliation list render identically to an OFAC designation.
    """

    direct: tuple[str, ...] = ()
    counter: tuple[str, ...] = ()
    control: tuple[str, ...] = ()
    linked: tuple[str, ...] = ()
    unknown: tuple[str, ...] = ()

    def __bool__(self) -> bool:
        return bool(
            self.direct or self.counter or self.control or self.linked or self.unknown
        )


def classify_sanction_topics(topics: Iterable[str]) -> SanctionTopics:
    """Split sanction-family topics into direct / counter / control / linked /
    unknown.

    Single source of truth for ``risk.py``, ``cross_check.py`` and
    ``openaleph_check.py``. Those three each carried their own copy of the
    taxonomy plus a ``startswith("sanction")`` catch-all — which is exactly
    how ``sanction.control`` came to be classified as plain adjacency in all
    three at once. Classify here, rank at the call site.
    """
    direct: list[str] = []
    counter: list[str] = []
    control: list[str] = []
    linked: list[str] = []
    unknown: list[str] = []
    for topic in topics:
        if topic in _DIRECT_SANCTION_TOPICS:
            direct.append(topic)
        elif topic in _COUNTER_SANCTION_TOPICS:
            counter.append(topic)
        elif topic in _CONTROL_SANCTION_TOPICS:
            control.append(topic)
        elif topic in _LINKED_SANCTION_TOPICS:
            linked.append(topic)
        elif topic.startswith(_SANCTION_TOPIC_PREFIX):
            unknown.append(topic)
            _LOG.warning(
                "Unrecognised OpenSanctions sanction-family topic %r — treating "
                "it as sanction.linked. Classify it in risk.py "
                "(_KNOWN_SANCTION_TOPICS) and update the drift canary in "
                "tests/test_opensanctions_live.py.",
                topic,
            )
    return SanctionTopics(
        direct=tuple(sorted(direct)),
        counter=tuple(sorted(counter)),
        control=tuple(sorted(control)),
        linked=tuple(sorted(linked)),
        unknown=tuple(sorted(unknown)),
    )

# Debarment: excluded from public contracts / procurement (e.g. World Bank,
# AfDB, EU debarment lists). A confirmed adverse listing of the entity, but a
# distinct category from financial sanctions — its own signal.
_DEBARMENT_TOPICS = {"debarment"}

# Known ICIJ leak collections on OpenAleph. Match on either the
# collection foreign_id (preferred) or a fragment of the label.
_LEAK_FOREIGN_ID_PREFIXES = (
    "icij",
    "panama_papers",
    "paradise_papers",
    "pandora_papers",
    "bahamas_leaks",
    "offshore_leaks",
)
_LEAK_LABEL_FRAGMENTS = (
    "icij",
    "panama papers",
    "paradise papers",
    "pandora papers",
    "bahamas leaks",
    "offshore leaks",
)


@dataclass
class RiskSignal:
    """One risk assertion about a hit, with explanation.

    ``evidence`` carries the raw bits the rule keyed off (topic name,
    collection foreign_id, position label) so the UI can show a tooltip
    without re-running the rule.
    """

    code: str
    confidence: str
    summary: str
    source_id: str
    hit_id: str
    evidence: dict[str, Any] = field(default_factory=dict)
    #: ``"risk"`` (default) or ``"context"``. A *context* signal is a
    #: structural observation that is NOT an adverse finding — it is worth
    #: showing, and it may feed a composite rule, but it must not be
    #: presented as a risk or counted in "N risk signals".
    #:
    #: This lives on the signal rather than in a per-surface exclusion list
    #: because the count is rendered independently by the results page, the
    #: OG share card and the share-page meta description; three hand-kept
    #: lists would drift the way the curated homepage claims have before.
    #: Classify once here, and every surface agrees by construction.
    kind: str = "risk"

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "confidence": self.confidence,
            "summary": self.summary,
            "source_id": self.source_id,
            "hit_id": self.hit_id,
            "evidence": self.evidence,
            "kind": self.kind,
        }


# ----------------------------------------------------------------------
# Degraded upstream screens (issue #50)
# ----------------------------------------------------------------------
#
# Derived risk checks (cross-source name screening, ICIJ offshore-leaks
# reconciliation) produce "evidence of absence" signals: an empty result
# normally means "screened, nothing found". When the upstream call fails,
# that empty result silently masquerades as a clean screen. DegradedSource
# is the structured record that breaks the ambiguity — it rides on the
# same response block as the risk signals so the UI, PDF and narrative can
# all say "this screen did not fully run".

#: Closed reason vocabulary — additions need UI + docs updates.
DEGRADED_UPSTREAM_ERROR = "upstream_error"
DEGRADED_TIMEOUT = "timeout"
DEGRADED_NOT_CONFIGURED = "not_configured"
DEGRADED_RATE_LIMITED = "rate_limited"

#: Tie-break order when one source fails for several reasons in one run —
#: most systemic first (a config gap explains everything else).
_DEGRADED_REASON_PRIORITY = [
    DEGRADED_NOT_CONFIGURED,
    DEGRADED_RATE_LIMITED,
    DEGRADED_TIMEOUT,
    DEGRADED_UPSTREAM_ERROR,
]


@dataclass
class DegradedSource:
    """One upstream screen that did not fully run for this lookup.

    PRIVACY: ``detail`` must only ever carry counts and source/check
    names — never the related-party names that were being screened
    (they are people and companies from the subject's ownership graph).
    ``test_degraded_sources.py`` enforces this.
    """

    #: Adapter id of the upstream that failed ("opensanctions", "icij", …);
    #: "opencheck" when the failure happened before reaching any upstream.
    source_id: str
    #: Which derived check degraded ("cross_source_names", "icij_offshore_leaks").
    check: str
    #: Risk codes whose absence is now unreliable (e.g. RELATED_SANCTIONED).
    affected_signals: list[str]
    #: Human-readable summary — counts only, never names.
    detail: str
    #: One of the DEGRADED_* constants above.
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "check": self.check,
            "affected_signals": self.affected_signals,
            "detail": self.detail,
            "reason": self.reason,
        }


#: Plain-English phrasing per reason, shared by the narrative packet and
#: the PDF/markdown report builders.
DEGRADATION_REASON_LABELS: dict[str, str] = {
    DEGRADED_UPSTREAM_ERROR: "the upstream service errored",
    DEGRADED_TIMEOUT: "the upstream service timed out",
    DEGRADED_NOT_CONFIGURED: "the required API credential is not configured",
    DEGRADED_RATE_LIMITED: "the upstream service rate-limited the request",
}


def classify_degradation_reason(exc: BaseException) -> str:
    """Map an upstream exception onto the closed reason vocabulary."""
    import asyncio

    import httpx

    if isinstance(exc, (httpx.TimeoutException, asyncio.TimeoutError)):
        return DEGRADED_TIMEOUT
    if isinstance(exc, httpx.HTTPStatusError):
        if exc.response.status_code == 429:
            return DEGRADED_RATE_LIMITED
        return DEGRADED_UPSTREAM_ERROR
    return DEGRADED_UPSTREAM_ERROR


def pick_degradation_reason(reason_counts: dict[str, int]) -> str:
    """Single headline reason for a source that failed in mixed ways:
    the most frequent, tie-broken by how systemic the reason is."""
    if not reason_counts:
        return DEGRADED_UPSTREAM_ERROR
    return max(
        reason_counts,
        key=lambda r: (
            reason_counts[r],
            -_DEGRADED_REASON_PRIORITY.index(r)
            if r in _DEGRADED_REASON_PRIORITY
            else -len(_DEGRADED_REASON_PRIORITY),
        ),
    )


# ----------------------------------------------------------------------
# Rules over SourceHit (search-time data)
# ----------------------------------------------------------------------


def assess_hit(hit: SourceHit) -> list[RiskSignal]:
    """Risk signals derivable from a single search-time hit.

    Stub hits never produce signals — the raw payload is fictional.
    """
    if hit.is_stub:
        return []

    signals: list[RiskSignal] = []

    if hit.source_id == "opensanctions":
        signals.extend(_opensanctions_topic_signals(hit, hit.raw))
    elif hit.source_id == "everypolitician" and hit.kind == SearchKind.PERSON:
        # The EveryPolitician dataset (now sourced from OpenSanctions
        # peps) is, by construction, persons-with-political-positions.
        signals.append(
            RiskSignal(
                code=PEP,
                confidence="high",
                summary="Listed in the EveryPolitician / OpenSanctions PEPs dataset.",
                source_id=hit.source_id,
                hit_id=hit.hit_id,
                evidence={"dataset": "peps"},
            )
        )

    return signals


def assess_hits(hits: Iterable[SourceHit]) -> list[RiskSignal]:
    """Risk signals across an entire fan-out result.

    Deduplicates by (code, source_id, hit_id) — a hit that is both PEP
    and sanctioned still produces both signals, but the same PEP signal
    isn't emitted twice for one hit.
    """
    seen: set[tuple[str, str, str]] = set()
    out: list[RiskSignal] = []
    for hit in hits:
        for signal in assess_hit(hit):
            key = (signal.code, signal.source_id, signal.hit_id)
            if key in seen:
                continue
            seen.add(key)
            out.append(signal)
    return out


# ----------------------------------------------------------------------
# Rules over deepen bundles (post-fetch data)
# ----------------------------------------------------------------------


def assess_bundle(
    source_id: str, raw: dict[str, Any], bods: list[dict[str, Any]] | None = None,
    hit_id: str = "",
) -> list[RiskSignal]:
    """Risk signals derivable from a ``/deepen`` payload.

    Fed both the raw source-shaped bundle and the BODS statements so it
    can reason over either layer. ``bods`` may be empty/None for sources
    we haven't mapped yet — rules that need BODS will simply not fire.
    """
    signals: list[RiskSignal] = []

    if raw.get("is_stub"):
        return signals

    if source_id == "opensanctions":
        entity = raw.get("entity") or {}
        hit_id = raw.get("entity_id") or entity.get("id") or ""
        signals.extend(_opensanctions_topic_signals_from_entity(hit_id, entity))

    elif source_id == "everypolitician":
        entity = raw.get("entity") or {}
        hit_id = raw.get("entity_id") or entity.get("id") or ""
        if hit_id:
            signals.append(
                RiskSignal(
                    code=PEP,
                    confidence="high",
                    summary="Listed in the EveryPolitician / OpenSanctions PEPs dataset.",
                    source_id=source_id,
                    hit_id=hit_id,
                    evidence={"dataset": "peps"},
                )
            )
            # Some PEP records also carry sanction topics — surface both.
            signals.extend(
                _opensanctions_topic_signals_from_entity(
                    hit_id, entity, source_id=source_id
                )
            )

    elif source_id == "openaleph":
        signals.extend(_openaleph_leak_signals(raw))

    elif source_id == "wikidata":
        signals.extend(_wikidata_position_signals(raw))

    if bods:
        signals.extend(_opaque_ownership_signals(source_id, raw, bods, hit_id=hit_id))
        signals.extend(assess_amla(source_id, raw, bods, hit_id=hit_id))
        signals.extend(_state_controlled_signals(source_id, hit_id, bods))

    # Subjective AMLA "obfuscation" signal looks at the assembled
    # signal set (after every other rule has fired) — last to run.
    obfuscation = _possible_obfuscation_signal(
        source_id, hit_id or raw.get("entity_id") or raw.get("hit_id") or "", signals
    )
    if obfuscation is not None:
        signals.append(obfuscation)

    return signals


# ----------------------------------------------------------------------
# Per-source rule helpers
# ----------------------------------------------------------------------


def _opensanctions_topic_signals(
    hit: SourceHit, raw: dict[str, Any]
) -> list[RiskSignal]:
    """OpenSanctions search-card topics → PEP / SANCTIONED."""
    return _opensanctions_topic_signals_from_entity(hit.hit_id, raw, source_id=hit.source_id)


def _opensanctions_topic_signals_from_entity(
    hit_id: str, entity: dict[str, Any], *, source_id: str = "opensanctions"
) -> list[RiskSignal]:
    topics = _extract_topics(entity)
    out: list[RiskSignal] = []
    # statement_id lets the frontend highlight the exact BODS graph node.
    stmt_id = _bods_stable_id(source_id, hit_id)
    if any(t in _PEP_TOPICS for t in topics):
        matched = sorted(t for t in topics if t in _PEP_TOPICS)
        out.append(
            RiskSignal(
                code=PEP,
                confidence="high",
                summary=f"OpenSanctions tags this record as {', '.join(matched)}.",
                source_id=source_id,
                hit_id=hit_id,
                evidence={"topics": matched, "statement_id": stmt_id},
            )
        )
    sanctions = classify_sanction_topics(topics)
    # Direct listing ("sanction") → SANCTIONED.
    direct_topics = list(sanctions.direct)
    if direct_topics:
        out.append(
            RiskSignal(
                code=SANCTIONED,
                confidence="high",
                summary=(
                    "OpenSanctions lists this record as sanctioned"
                    f" ({', '.join(direct_topics)})."
                ),
                source_id=source_id,
                hit_id=hit_id,
                evidence={"topics": direct_topics, "statement_id": stmt_id},
            )
        )
    # Counter-designation ("sanction.counter") → COUNTER_SANCTIONED. Also a
    # listing of the entity itself, so it fires alongside SANCTIONED rather
    # than instead of it — but stated as what it is. High confidence: the
    # listing is a fact; what the summary declines to assert is that it
    # carries a compliance obligation for the reader.
    counter_topics = list(sanctions.counter)
    if counter_topics:
        out.append(
            RiskSignal(
                code=COUNTER_SANCTIONED,
                confidence="high",
                summary=(
                    "OpenSanctions lists this record under a counter-sanctions "
                    "regime — a designation issued by a state with weak "
                    "democratic institutions, typically retaliation for foreign "
                    "sanctions or suppression of domestic opposition. It is not "
                    "a designation by an EU, UK, US, UN or other mainstream "
                    "sanctions authority, and being listed is frequently a "
                    "consequence of journalism, sanctions enforcement or "
                    f"human-rights work ({', '.join(counter_topics)})."
                ),
                source_id=source_id,
                hit_id=hit_id,
                evidence={"topics": counter_topics, "statement_id": stmt_id},
            )
        )
    # Ownership chain ("sanction.control") → SANCTIONS_CONTROLLED. Fires
    # independently of SANCTIONED: an entity can be designated in its own
    # right *and* sit inside another designated party's ownership chain, and
    # both facts are worth reporting.
    control_topics = list(sanctions.control)
    if control_topics:
        out.append(
            RiskSignal(
                code=SANCTIONS_CONTROLLED,
                confidence="high",
                summary=(
                    "OpenSanctions places this record in a sanctioned party's "
                    "ownership chain — a direct or indirect subsidiary, asset "
                    "or vessel. It is not itself designated. No percentage "
                    "threshold is applied, so whether an ownership-and-control "
                    "test (such as OFAC's 50 Percent Rule) brings it into scope "
                    f"depends on the stake and the regime ({', '.join(control_topics)})."
                ),
                source_id=source_id,
                hit_id=hit_id,
                evidence={"topics": control_topics, "statement_id": stmt_id},
            )
        )
    # Plain adjacency ("sanction.linked", or any unrecognised "sanction.*")
    # → SANCTIONS_LINKED. Not itself sanctioned, so a separate, softer signal.
    #
    # ``sanction.linked`` is a declared SUPERSET of ``sanction.control``
    # upstream, so a controlled entity always carries both topics. Suppress
    # the weaker chip when control fires — it is the same fact stated less
    # precisely, not an additional one. (Contrast SANCTIONED above, which is
    # a genuinely different fact and so co-exists with everything.)
    linked_topics = (
        [] if control_topics else sorted(sanctions.linked + sanctions.unknown)
    )
    if linked_topics:
        out.append(
            RiskSignal(
                code=SANCTIONS_LINKED,
                confidence="medium",
                summary=(
                    "OpenSanctions links this record to sanctioned entities; the "
                    f"record is not itself sanctioned ({', '.join(linked_topics)})."
                ),
                source_id=source_id,
                hit_id=hit_id,
                evidence={"topics": linked_topics, "statement_id": stmt_id},
            )
        )
    # Debarment ("debarment") → excluded from public contracts/procurement.
    if any(t in _DEBARMENT_TOPICS for t in topics):
        out.append(
            RiskSignal(
                code=DEBARMENT,
                confidence="high",
                summary=(
                    "OpenSanctions lists this record as debarred from public "
                    "contracts/procurement (debarment)."
                ),
                source_id=source_id,
                hit_id=hit_id,
                evidence={"topics": ["debarment"], "statement_id": stmt_id},
            )
        )
    return out


def _extract_topics(payload: dict[str, Any]) -> list[str]:
    """Topics may live at the top level or under ``properties``."""
    topics = payload.get("topics")
    if not topics:
        topics = (payload.get("properties") or {}).get("topics") or []
    if isinstance(topics, str):
        topics = [topics]
    return [t for t in topics if isinstance(t, str)]


def _openaleph_leak_signals(raw: dict[str, Any]) -> list[RiskSignal]:
    """OpenAleph bundle → OFFSHORE_LEAKS when the collection is a leak."""
    collection = raw.get("collection") or {}
    inline = (raw.get("entity") or {}).get("collection") or {}
    # Some hosts return the collection block inline on the entity.
    foreign_id = (
        collection.get("foreign_id")
        or inline.get("foreign_id")
        or ""
    ).lower()
    label = (collection.get("label") or inline.get("label") or "").lower()

    matched_via: dict[str, str] | None = None
    if any(foreign_id.startswith(prefix) for prefix in _LEAK_FOREIGN_ID_PREFIXES):
        matched_via = {"foreign_id": foreign_id}
    elif any(frag in label for frag in _LEAK_LABEL_FRAGMENTS):
        matched_via = {"label": label}

    if not matched_via:
        return []

    hit_id = raw.get("entity_id") or (raw.get("entity") or {}).get("id") or ""
    return [
        RiskSignal(
            code=OFFSHORE_LEAKS,
            confidence="medium",
            summary=(
                "Mentioned in the "
                f"{collection.get('label') or inline.get('label') or foreign_id} "
                "leak collection on OpenAleph."
            ),
            source_id="openaleph",
            hit_id=hit_id,
            evidence={
                "collection": collection.get("label")
                or inline.get("label")
                or foreign_id,
                "match": matched_via,
            },
        )
    ]


def _wikidata_position_signals(raw: dict[str, Any]) -> list[RiskSignal]:
    """Wikidata person with a current position → PEP."""
    if not raw.get("is_person"):
        return []
    positions = raw.get("positions") or []
    current = [p for p in positions if not p.get("end")]
    if not current:
        return []
    labels = [p.get("label") for p in current if p.get("label")]
    qid = raw.get("qid") or ""
    return [
        RiskSignal(
            code=PEP,
            confidence="medium",
            summary=(
                "Wikidata records a currently-held political or public"
                f" position ({', '.join(labels) or 'unspecified'})."
            ),
            source_id="wikidata",
            hit_id=qid,
            evidence={"positions": labels},
        )
    ]


# GLEIF reporting-exception reason families (Level 2 Reporting Exceptions
# Format 2.1 + the GLEIF Reporting Exception Ontology). The undisclosed set
# is imported from the mapper so the two modules cannot drift.
#
# * Exempt/structural — "there is no parent according to the definition
#   used" (accounting consolidation), or the parent is real but simply not
#   identified in GLEIF. Permitted exceptions under the LEI ROC policy and
#   NOT evidence of opacity: Eli Lilly, for instance, reports
#   ``NATURAL_PERSONS`` for both parents because a widely held issuer has
#   no consolidating parent entity. → ``GLEIF_REPORTING_EXCEPTION``
#   (kind="context").
# * Undisclosed — a consolidating parent exists and is known but is
#   deliberately withheld from publication (``NON_PUBLIC`` and its five
#   deprecated pre-2022 variants). → ``OPAQUE_OWNERSHIP``.
_GLEIF_EXEMPT_REASONS: frozenset[str] = frozenset(
    {"NATURAL_PERSONS", "NO_KNOWN_PERSON", "NON_CONSOLIDATING", "NO_LEI"}
)

#: Human phrasing for each exempt reason, used in the context-signal summary.
_GLEIF_EXEMPT_REASON_TEXT: dict[str, str] = {
    "NATURAL_PERSONS": (
        "the entity is controlled directly by natural person(s), with no"
        " intermediate legal entity that consolidates it — common for"
        " founder-, family-owned and widely held companies"
    ),
    "NO_KNOWN_PERSON": (
        "no known person controls the entity (e.g. diversified shareholding)"
    ),
    "NON_CONSOLIDATING": (
        "its controlling legal entities are not subject to preparing"
        " consolidated financial statements"
    ),
    "NO_LEI": (
        "a parent exists but does not consent to have an LEI, so it is not"
        " identified in GLEIF — national registers may still identify it"
    ),
}

_GLEIF_UNDISCLOSED_REASON_TEXT: dict[str, str] = {
    "NON_PUBLIC": (
        "a parent exists but the relationship is non-public and is not"
        " disclosed"
    ),
    "BINDING_LEGAL_COMMITMENTS": (
        "binding legal commitments prevent disclosure of the parent"
    ),
    "LEGAL_OBSTACLES": (
        "obstacles in laws or regulations prevent disclosure of the parent"
    ),
    "DISCLOSURE_DETRIMENTAL": (
        "the entity declares disclosure would be detrimental to it or its"
        " parent"
    ),
    "DETRIMENT_NOT_EXCLUDED": (
        "detriment to the parent from disclosure could not be excluded"
    ),
    "CONSENT_NOT_OBTAINED": (
        "the parent's consent to disclose the relationship was not obtained"
    ),
}


def _gleif_exceptions_from_raw(
    raw: dict[str, Any]
) -> list[dict[str, str]]:
    """Extract normalised reporting-exception entries from a GLEIF bundle.

    Returns one entry per declared exception with ``relationship``
    ("direct"/"ultimate"), ``reason``, ``category``, ``reference`` and the
    deterministic ``statement_id`` of the bridging statement the mapper
    emits for it (same ``_stable_id`` inputs — see
    ``mapper._gleif_exception_statements``).
    """
    lei = (raw.get("lei") or "").strip().upper()
    entries: list[dict[str, str]] = []
    for kind in ("direct", "ultimate"):
        exc = raw.get(f"{kind}_parent_exception")
        if not exc:
            continue
        attrs = exc.get("attributes") or exc
        reason = (attrs.get("reason") or attrs.get("exceptionReason") or "").upper()
        category = (
            attrs.get("category") or attrs.get("exceptionCategory") or ""
        ).upper()
        reference = attrs.get("reference") or attrs.get("exceptionReference") or ""
        ip_kind = (
            "person"
            if reason in {"NATURAL_PERSONS", "NO_KNOWN_PERSON"}
            else "entity"
        )
        entries.append(
            {
                "relationship": kind,
                "reason": reason,
                "category": category,
                "reference": reference,
                "statement_id": _bods_stable_id(
                    "gleif",
                    ip_kind,
                    f"{lei}:{kind}-parent-exception:{reason or 'unspecified'}",
                ),
            }
        )
    return entries


def _gleif_exception_signals(
    raw: dict[str, Any], hit_id: str
) -> list[RiskSignal]:
    """Classify GLEIF Level 2 reporting exceptions into signals.

    Undisclosed-parent reasons (the ``NON_PUBLIC`` family) →
    ``OPAQUE_OWNERSHIP`` (risk). Everything else — including unrecognised
    future codes — → ``GLEIF_REPORTING_EXCEPTION`` (context): the ROC
    policy defines exceptions as *permitted* reasons not to report an
    accounting-consolidation parent, so absence of a parent record is not
    by itself opacity. GLEIF Level 2 only ever names entities (LEI
    holders), never people, so no wording here may claim an "unknown
    person in the ownership chain".
    """
    entries = _gleif_exceptions_from_raw(raw)
    if not entries:
        return []
    hit = hit_id or (raw.get("lei") or "")
    signals: list[RiskSignal] = []

    undisclosed = [e for e in entries if e["reason"] in GLEIF_UNDISCLOSED_REASONS]
    exempt = [e for e in entries if e["reason"] not in GLEIF_UNDISCLOSED_REASONS]

    if undisclosed:
        parts = []
        for e in undisclosed:
            text = _GLEIF_UNDISCLOSED_REASON_TEXT.get(
                e["reason"], "the parent is withheld from publication"
            )
            parts.append(f"{e['reason']} for the {e['relationship']} parent ({text})")
        signals.append(
            RiskSignal(
                code=OPAQUE_OWNERSHIP,
                confidence="high",
                summary=(
                    "GLEIF records a reporting exception withholding a known"
                    " parent: " + "; ".join(parts) + ". A consolidating parent"
                    " exists but is deliberately not published — permitted"
                    " under the LEI ROC policy, but a transparency gap worth"
                    " reviewing."
                ),
                source_id="gleif",
                hit_id=hit,
                evidence={
                    "exceptions": undisclosed,
                    # Same shape as the AMLA signals so signalScope.ts /
                    # buildSignalMap badge the bridging graph node.
                    "matches": [
                        {"statement_id": e["statement_id"]} for e in undisclosed
                    ],
                },
            )
        )

    if exempt:
        # One line per distinct reason; both categories often carry the same
        # reason (e.g. Eli Lilly: NATURAL_PERSONS for direct and ultimate).
        by_reason: dict[str, list[str]] = {}
        for e in exempt:
            by_reason.setdefault(e["reason"], []).append(e["relationship"])
        parts = []
        for reason, rels in by_reason.items():
            text = _GLEIF_EXEMPT_REASON_TEXT.get(
                reason, "a permitted reporting exception was declared"
            )
            parts.append(
                f"{reason or 'unspecified'} for the {' and '.join(rels)}"
                f" parent ({text})"
            )
        signals.append(
            RiskSignal(
                code=GLEIF_REPORTING_EXCEPTION,
                confidence="high",
                kind="context",
                summary=(
                    "No accounting-consolidation parent is reported to GLEIF: "
                    + "; ".join(parts)
                    + ". Structural context, not a risk finding — these are"
                    " permitted exceptions under the LEI ROC policy, and"
                    " GLEIF Level 2 records accounting consolidation, not"
                    " beneficial ownership."
                ),
                source_id="gleif",
                hit_id=hit,
                evidence={
                    "exceptions": exempt,
                    "matches": [
                        {"statement_id": e["statement_id"]} for e in exempt
                    ],
                },
            )
        )
    return signals


# BODS unspecified-interestedParty reasons that assert genuine opacity: the
# register says an owner exists but is unidentified or withholding
# information (Companies House "PSC exists but not identified" / "PSC
# contacted but no response" families — see mapper._PSC_STATEMENT_REASON).
# ``noBeneficialOwners`` (nobody meets the threshold) and
# ``interestedPartyExemptFromDisclosure`` (a lawful exemption, e.g. listed
# companies) are deliberately absent.
_OPAQUE_UNSPECIFIED_REASONS: frozenset[str] = frozenset(
    {
        "subjectUnableToConfirmOrIdentifyBeneficialOwner",
        "interestedPartyHasNotProvidedInformation",
    }
)


def _unspecified_party_reason(stmt: dict[str, Any]) -> tuple[str, str]:
    """Return ``(reason, description)`` for an unspecified interestedParty.

    Handles both the v0.4 shape the mapper emits (``interestedParty`` is a
    ``{"reason": ..., "description": ...}`` dict) and the legacy wrapped
    ``{"unspecified": {"reason": ...}}`` form. Returns ``("", "")`` when the
    interested party is a normal statement reference.
    """
    rd = _record_details(stmt)
    ip = rd.get("interestedParty")
    if not isinstance(ip, dict):
        return "", ""
    if "unspecified" in ip and isinstance(ip["unspecified"], dict):
        ip = ip["unspecified"]
    if any(k.startswith("describedBy") for k in ip):
        return "", ""
    return str(ip.get("reason") or ""), str(ip.get("description") or "")


def _opaque_ownership_signals(
    source_id: str,
    raw: dict[str, Any],
    bods: list[dict[str, Any]],
    hit_id: str = "",
) -> list[RiskSignal]:
    """Genuine opacity: a party exists whose identity is withheld or unobtainable.

    Three families fire, each directly asserted by the register (so
    ``high`` confidence):

    * GLEIF ``NON_PUBLIC``-family reporting exceptions — handled via the
      raw bundle so the reason code drives classification (the benign
      exception reasons instead produce the ``GLEIF_REPORTING_EXCEPTION``
      context signal; see ``_gleif_exception_signals``).
    * ``anonymousPerson`` / ``anonymousEntity`` statements — identifying
      details deliberately withheld (e.g. a Companies House super-secure
      PSC protected by court order).
    * Relationship statements whose unspecified ``interestedParty`` reason
      says an owner exists but is unidentified or non-cooperative
      (``subjectUnableToConfirmOrIdentifyBeneficialOwner`` /
      ``interestedPartyHasNotProvidedInformation``).

    ``unknownPerson`` / ``unknownEntity`` statements do NOT fire: unknown-
    to-this-source is not the same claim as deliberately-withheld, and the
    GLEIF/OO reporting-exception bridges use exactly those types for the
    benign exception reasons.
    """
    hit = hit_id or raw.get("entity_id") or raw.get("hit_id") or ""

    # GLEIF bundles carry the exception records raw — classify from the
    # reason codes and skip the generic statement scan (the bridging
    # statements would otherwise double-count).
    if source_id == "gleif":
        return _gleif_exception_signals(raw, hit)

    # The OO bulk GLEIF dataset flattens away the exception reason, so a
    # placeholder statement there cannot be classified — stay silent rather
    # than mislabel a permitted exception as opacity (the live ``gleif``
    # source covers the same ground with full reason codes).
    if source_id == "bods_gleif":
        return []

    findings: list[str] = []
    matches: list[dict[str, str]] = []
    for stmt in bods:
        kind = _stmt_kind(stmt)
        if kind == "person" and _person_type(stmt) == "anonymousPerson":
            findings.append(
                "a person whose identifying details are withheld"
                " (anonymousPerson — e.g. a super-secure PSC protected by"
                " court order)"
            )
            matches.append({"statement_id": _statement_id(stmt)})
        elif kind == "entity" and _entity_type(stmt) == "anonymousEntity":
            findings.append(
                "an entity whose identifying details are withheld"
                " (anonymousEntity)"
            )
            matches.append({"statement_id": _statement_id(stmt)})
        elif kind == "relationship":
            reason, description = _unspecified_party_reason(stmt)
            if reason in _OPAQUE_UNSPECIFIED_REASONS:
                findings.append(
                    description
                    or "an owner exists but has not been identified"
                )
                matches.append({"statement_id": _statement_id(stmt)})
    if not findings:
        return []
    # Dedupe but keep order.
    deduped: list[str] = []
    for f in findings:
        if f not in deduped:
            deduped.append(f)
    return [
        RiskSignal(
            code=OPAQUE_OWNERSHIP,
            confidence="high",
            summary=(
                "The register discloses that ownership information is"
                " withheld or could not be obtained: "
                + "; ".join(deduped)
                + "."
            ),
            source_id=source_id,
            hit_id=hit,
            evidence={"findings": deduped, "matches": matches},
        )
    ]


# ----------------------------------------------------------------------
# BODS shape readers (tolerate v0.4 nested + flat fixtures)
# ----------------------------------------------------------------------


def _stmt_kind(stmt: dict[str, Any]) -> str:
    """Return ``"entity"``, ``"person"``, ``"relationship"`` or ``""``.

    v0.4 puts the kind under ``recordType``. Older flat fixtures may
    use ``statementType: "entityStatement"`` etc.
    """
    rt = stmt.get("recordType")
    if rt:
        return rt
    st = stmt.get("statementType", "")
    return st.replace("Statement", "") if st else ""


def _record_details(stmt: dict[str, Any]) -> dict[str, Any]:
    rd = stmt.get("recordDetails")
    return rd if isinstance(rd, dict) else {}


def _entity_type(stmt: dict[str, Any]) -> str:
    rd = _record_details(stmt)
    et = rd.get("entityType")
    if isinstance(et, dict):
        return et.get("type", "")
    if isinstance(et, str):
        return et
    return stmt.get("entityType", "") or ""


def _person_type(stmt: dict[str, Any]) -> str:
    rd = _record_details(stmt)
    return rd.get("personType") or stmt.get("personType", "") or ""


def _entity_jurisdiction(stmt: dict[str, Any]) -> dict[str, str] | None:
    rd = _record_details(stmt)
    j = rd.get("jurisdiction") or stmt.get("incorporatedInJurisdiction")  # v0.4: jurisdiction; v0.3 pass-through: incorporatedInJurisdiction
    if isinstance(j, dict):
        return j
    return None


def _entity_legal_form_fields(stmt: dict[str, Any]) -> list[tuple[str, str]]:
    """Return ``(field_name, value)`` pairs carrying an entity's legal form.

    Deliberately EXCLUDES ``name``: matching a trust/foundation fragment in the
    entity's *name* over-fires — any company with "Foundation", "Trust" etc. in
    its trading name would trip the signal regardless of its actual legal form
    (e.g. GLEIF, "Global Legal Entity Identifier Foundation"). We key only off
    genuine legal-form fields: the ``legalFormLabel`` annotation mappers attach,
    plus BODS ``entityType.subtype`` / ``entityType.details`` and any
    forward-compat ``legalForm`` / ``entitySubtype`` / ``details`` fields.
    """
    rd = _record_details(stmt)
    out: list[tuple[str, str]] = []

    def _add(field: str, v: Any) -> None:
        if isinstance(v, str) and v:
            out.append((field, v))
        elif isinstance(v, dict):
            for sub in v.values():
                if isinstance(sub, str) and sub:
                    out.append((field, sub))

    for key in ("legalForm", "legalFormLabel", "entitySubtype", "details"):
        _add(key, rd.get(key))
    et = rd.get("entityType")
    if isinstance(et, dict):
        _add("entityType.subtype", et.get("subtype"))
        _add("entityType.details", et.get("details"))
    return out


def _statement_id(stmt: dict[str, Any]) -> str:
    return stmt.get("statementId") or stmt.get("statement_id") or ""


def _relationship_endpoints(stmt: dict[str, Any]) -> tuple[str, str, str]:
    """Return (subject_id, ip_id, ip_kind) for a relationship statement.

    Handles both the BODS v0.4 bare-string format (``subject: "id"``) and the
    older wrapped format (``subject: {"describedByEntityStatement": "id"}``).
    ``ip_kind`` is ``"entity"``, ``"person"`` or ``""`` (unknown).
    """
    rd = _record_details(stmt)

    # BODS v0.4: subject is a bare string record-id.
    raw_subj = rd.get("subject") or {}
    if isinstance(raw_subj, str):
        subj = raw_subj
    else:
        subj = raw_subj.get("describedByEntityStatement") or ""

    # BODS v0.4: interestedParty is a bare string record-id.
    raw_ip = rd.get("interestedParty") or {}
    if isinstance(raw_ip, str):
        # For bare string we can't determine ip_kind from the key name alone;
        # resolve it later via statement lookup.  Return "" for ip_kind.
        return subj, raw_ip, ""

    # Legacy wrapped format.
    if "describedByEntityStatement" in raw_ip:
        return subj, raw_ip["describedByEntityStatement"], "entity"
    if "describedByPersonStatement" in raw_ip:
        return subj, raw_ip["describedByPersonStatement"], "person"
    if "describedByAnonymousEntityStatement" in raw_ip:
        return subj, raw_ip["describedByAnonymousEntityStatement"], "entity"
    return subj, "", ""


def _interests(stmt: dict[str, Any]) -> list[dict[str, Any]]:
    rd = _record_details(stmt)
    interests = rd.get("interests")
    return interests if isinstance(interests, list) else []


# BODS entityType.type values that denote a state / government body (per the
# SOE modelling requirement). A relationship to such an entity = state control.
_STATE_ENTITY_TYPES = {"state", "stateBody"}


def _state_controlled_signals(
    source_id: str, hit_id: str, bods: list[dict[str, Any]]
) -> list[RiskSignal]:
    """STATE_CONTROLLED — a controlling owner is a state or state body.

    BODS-derived and source-agnostic: per the BODS *Representing state-owned
    enterprises* requirement, an SOE connects (directly or indirectly) to an
    entity statement with ``entityType.type`` ``state`` / ``stateBody``. We fire
    when such an entity is the interested party of a relationship in the bundle.

    Presence-only and corroborating — currently sourced from Wikidata (crowd-
    sourced, famous-names-only), so its **absence is not evidence** the entity is
    privately owned. Medium confidence; never a determination.
    """
    ents: dict[str, dict[str, Any]] = {}
    state_ids: set[str] = set()
    for stmt in bods:
        if _stmt_kind(stmt) != "entity":
            continue
        sid = _statement_id(stmt)
        ents[sid] = stmt
        etype = (_record_details(stmt).get("entityType") or {}).get("type")
        if etype in _STATE_ENTITY_TYPES:
            state_ids.add(sid)
    if not state_ids:
        return []

    owners: list[str] = []
    state_node: str = ""
    subject_node: str = ""
    for stmt in bods:
        if _stmt_kind(stmt) != "relationship":
            continue
        subj, ip, _ = _relationship_endpoints(stmt)
        if ip in state_ids:
            name = _record_details(ents.get(ip, {})).get("name") or ip
            owners.append(name)
            state_node = state_node or ip
            subject_node = subject_node or subj
    if not owners:
        return []

    return [
        RiskSignal(
            code=STATE_CONTROLLED,
            confidence="medium",
            summary=(
                "A controlling owner is a state or state body — a possible "
                "state-owned enterprise. Corroborating indicator (Wikidata-"
                "sourced); not a determination, and its absence is not evidence "
                "the entity is privately owned."
            ),
            source_id=source_id,
            hit_id=hit_id,
            evidence={
                "state_owners": sorted(set(owners)),
                "statement_id": state_node,            # the state/stateBody node
                "subject_statement_id": subject_node,  # the controlled entity
            },
        )
    ]


# ----------------------------------------------------------------------
# AMLA CDD RTS rules
# ----------------------------------------------------------------------


def assess_amla(
    source_id: str, raw: dict[str, Any], bods: list[dict[str, Any]],
    hit_id: str = "",
) -> list[RiskSignal]:
    """Run all AMLA-aligned rules over a BODS bundle.

    Called from ``assess_bundle``; broken out so callers (CLI, tests,
    a future export pipeline) can invoke it directly on a hand-built
    BODS bundle without going through a deepen response.

    ``hit_id`` should be passed explicitly by ``assess_bundle`` (which
    knows the true hit id). The raw-dict fallback exists for direct
    callers (tests, CLI) that pass ``entity_id`` in the raw dict.
    """
    if not bods:
        return []
    hit_id = hit_id or raw.get("entity_id") or raw.get("hit_id") or ""

    trust_signal = _trust_or_arrangement_signal(source_id, hit_id, bods)
    non_eu_signal = _non_eu_jurisdiction_signal(source_id, hit_id, bods)
    nominee_signal = _nominee_signal(source_id, hit_id, bods, raw)
    layers_signal = _layers_signal(source_id, hit_id, bods)

    out: list[RiskSignal] = []
    for sig in (trust_signal, non_eu_signal, nominee_signal, layers_signal):
        if sig is not None:
            out.append(sig)

    # Jurisdiction-list signals — independent of the AMLA composite rule.
    # These are the only geographic RISK claims OpenCheck makes: both come
    # from an authoritative, externally maintained, dated list. Anything
    # else about where a chain reaches is context, not risk.
    out.extend(_fatf_jurisdiction_signals(source_id, hit_id, bods))
    out.extend(_eu_high_risk_third_country_signals(source_id, hit_id, bods))

    # AMLA CDD RTS Article 12(1): treat a structure as a complex corporate
    # structure where there are "three or more layers between the customer
    # and the beneficial owner and, in addition, MORE THAN ONE of the
    # following conditions is met":
    #
    #   (a) a legal arrangement or similar entity (e.g. a foundation) in
    #       any of the layers                      -> _trust_condition_met
    #   (b) the customer and any legal entities present at any of these
    #       layers are registered outside the EU   -> _non_eu_condition_met
    #   (c) nominee shareholders or nominee directors involved in the
    #       structure                              -> nominee_signal
    #
    # (a) and (b) are scoped to the layered path because both say "in any
    # of the(se) layers"; (c) says "involved in the structure", which is
    # looser, so it stays bundle-wide. The standalone signals remain
    # bundle-wide in every case — "this bundle contains a trust" and "a
    # trust sits on the layered chain" are different claims.
    #   (d) the structure obfuscates or diminishes transparency of
    #       ownership with no legitimate economic rationale
    #
    # "More than one" means at least TWO conditions, not one. Condition
    # (d) is NOT counted: its "no legitimate economic rationale" limb is a
    # judgement that cannot be made from data alone, so it is surfaced
    # separately and advisorily as POSSIBLE_OBFUSCATION rather than being
    # allowed to push a structure over this threshold. That makes this
    # rule deliberately conservative — it can under-fire relative to the
    # RTS text, but it will not assert a legal conclusion we cannot
    # evidence.
    if layers_signal is not None:
        path_ids = layers_signal.evidence.get("longest_path") or []
        triggers: list[str] = []
        if _trust_condition_met(bods, path_ids):
            triggers.append("trust/arrangement")
        if _non_eu_condition_met(bods, path_ids):
            triggers.append("non-EU jurisdiction")
        if nominee_signal is not None:
            triggers.append("nominee")

        if len(triggers) >= 2:
            out.append(
                RiskSignal(
                    code=COMPLEX_CORPORATE_STRUCTURE,
                    confidence="high",
                    summary=(
                        "Meets AMLA CDD RTS threshold for a complex corporate "
                        f"structure: {layers_signal.evidence['layers']} layers "
                        "of ownership combined with " + ", ".join(triggers) + "."
                    ),
                    source_id=source_id,
                    hit_id=hit_id,
                    evidence={
                        "layers": layers_signal.evidence["layers"],
                        "triggers": triggers,
                    },
                )
            )

    return out


def _trust_or_arrangement_signal(
    source_id: str, hit_id: str, bods: list[dict[str, Any]]
) -> RiskSignal | None:
    matches: list[dict[str, str]] = []
    for stmt in bods:
        if _stmt_kind(stmt) != "entity":
            continue
        et = _entity_type(stmt)
        if et == "arrangement":
            matches.append(
                {
                    "statement_id": _statement_id(stmt),
                    "match": "entityType=arrangement",
                }
            )
            continue
        matched = False
        for field, value in _entity_legal_form_fields(stmt):
            low = value.lower()
            for frag in _TRUST_LEGAL_FORM_FRAGMENTS:
                if frag in low:
                    matches.append(
                        {
                            "statement_id": _statement_id(stmt),
                            "match": f"{field} contains '{frag}'",
                        }
                    )
                    matched = True
                    break
            if matched:
                break
    if not matches:
        return None
    return RiskSignal(
        code=TRUST_OR_ARRANGEMENT,
        confidence="high",
        summary=(
            "Ownership chain includes a trust or non-corporate "
            f"arrangement ({len(matches)} entity statement(s)). "
            "AMLA CDD RTS condition (a)."
        ),
        source_id=source_id,
        hit_id=hit_id,
        evidence={"matches": matches},
    )


def _non_eu_jurisdiction_signal(
    source_id: str, hit_id: str, bods: list[dict[str, Any]]
) -> RiskSignal | None:
    """Fires when the chain has any entity outside the EU+EEA set.

    The "EU+EEA set" is resolved at call time from settings — see
    ``_eu_eea_codes()`` and the ``OPENCHECK_AMLA_*`` env vars.
    """
    eu_eea = _eu_eea_codes()
    non_eu: list[dict[str, str]] = []
    for stmt in bods:
        if _stmt_kind(stmt) != "entity":
            continue
        j = _entity_jurisdiction(stmt)
        if not j:
            continue
        code = (j.get("code") or "").upper()
        name = j.get("name") or ""
        if code and code not in eu_eea:
            non_eu.append(
                {
                    "statement_id": _statement_id(stmt),
                    "code": code,
                    "name": name,
                }
            )
    if not non_eu:
        return None
    # Pull a short, deduped list of country codes for the summary.
    codes = sorted({m["code"] for m in non_eu})
    # NB: this is the standalone signal and stays bundle-wide — it reports
    # "the chain touches these jurisdictions", which is a different
    # question from AMLA Article 12(1)(b). The *condition* used by the
    # COMPLEX_CORPORATE_STRUCTURE composite is scoped to the layered
    # path; see ``_non_eu_condition_met``.
    return RiskSignal(
        code=NON_EU_JURISDICTION,
        confidence="low",
        kind="context",
        summary=(
            "Ownership chain reaches jurisdictions outside the EU/EEA: "
            + ", ".join(codes)
            + ". Structural context, not a risk finding — neither the AMLA "
            "CDD RTS nor AMLR Annex III treats non-EU status as a risk "
            "factor in itself. Contributes to AMLA Article 12(1) condition "
            "(b) only where it appears on the layered ownership path."
        ),
        source_id=source_id,
        hit_id=hit_id,
        evidence={"jurisdictions": non_eu},
    )


def _trust_condition_met(
    bods: list[dict[str, Any]], path_ids: list[str]
) -> bool:
    """AMLA CDD RTS Article 12(1), point (a) — scoped to the layered path.

    "there is a legal arrangement or a similar legal entity such as a
    foundation **in any of the layers**". Like point (b), the wording is
    explicitly scoped to the layers, so a trust sitting on a side branch
    of the bundle does not satisfy it.

    Point (c) is deliberately NOT scoped this way: it reads "nominee
    shareholders or nominee directors involved **in the structure**",
    which is looser than "in any of these layers" and is left bundle-wide.

    Reuses ``_trust_or_arrangement_signal`` rather than duplicating the
    legal-form keyword matching, so the condition and the standalone
    signal can never disagree about what counts as a trust.
    """
    if not path_ids:
        return False
    sig = _trust_or_arrangement_signal("", "", bods)
    if sig is None:
        return False
    on_path = set(path_ids)
    return any(
        m.get("statement_id") in on_path for m in sig.evidence.get("matches", [])
    )


def _non_eu_condition_met(
    bods: list[dict[str, Any]], path_ids: list[str]
) -> bool:
    """AMLA CDD RTS Article 12(1), point (b) — scoped to the layered path.

    The condition reads: "the customer and any legal entities present at
    **any of these layers** are registered in jurisdictions outside the
    EU". Two scoping choices follow from that wording.

    1. "at any of these layers" — we restrict the test to entity nodes on
       the longest ownership path found by ``_layers_signal``, rather
       than scanning the whole bundle. A non-EU entity hanging off a side
       branch that is not part of the layered structure does not satisfy
       this condition.

    2. The sentence is grammatically conjunctive ("the customer **and**
       any legal entities"), which read strictly would require *every*
       entity on the path to be non-EU. That reading would almost never
       be satisfied and is unlikely to be the drafters' intent, so we
       treat the condition as met when **any** entity on the path is
       registered outside the EU. This is the looser of the two readings;
       revisit when the final RTS is adopted.

    Returns a bool rather than a ``RiskSignal`` because this is an input
    to the composite, not a finding in its own right.
    """
    if not path_ids:
        return False
    eu_eea = _eu_eea_codes()
    on_path = set(path_ids)
    for stmt in bods:
        if _stmt_kind(stmt) != "entity":
            continue
        if _statement_id(stmt) not in on_path:
            continue
        j = _entity_jurisdiction(stmt)
        if not j:
            continue
        code = (j.get("code") or "").upper()
        if code and code not in eu_eea:
            return True
    return False


def _structured_nominee_matches(
    source_id: str, raw: dict[str, Any]
) -> list[dict[str, str]]:
    """Nominee arrangements the SOURCE stated, read from its own codes.

    Companies House / Register of Overseas Entities publish
    ``natures_of_control`` on each PSC record. Six of those codes say the
    overseas entity holds UK land or property as a nominee. Reading them
    directly is the difference between "this register filed a nominee
    arrangement" and "a sentence somewhere contained the word nominee".

    ``raw`` is the adapter payload, which ``assess_bundle`` already receives —
    so the codes are read at full fidelity rather than recovered from the
    English descriptor the mapper renders into ``interest.details``.
    """
    if source_id != "companies_house":
        return []

    matches: list[dict[str, str]] = []
    for psc in ((raw.get("pscs") or {}).get("items") or []):
        if not isinstance(psc, dict):
            continue
        # A ceased PSC's nominee arrangement is historical, not current.
        if psc.get("ceased_on"):
            continue
        for nature in psc.get("natures_of_control") or []:
            if is_nominee_nature(str(nature)):
                matches.append(
                    {
                        "statement_id": "",
                        "psc_name": str(psc.get("name") or ""),
                        "nature_code": str(nature),
                        "match": (
                            "register filed nature-of-control code "
                            f"'{nature}'"
                        ),
                    }
                )
    return matches


def _nominee_signal(
    source_id: str,
    hit_id: str,
    bods: list[dict[str, Any]],
    raw: dict[str, Any] | None = None,
) -> RiskSignal | None:
    """AMLA CDD RTS condition (c) — nominee shareholders or directors.

    Structured evidence first: where the source filed a nominee code, that is
    what the signal reports, and the code travels in the evidence so a reviewer
    can check the filing rather than trusting our reading of it. Textual
    matching remains for sources that publish only prose, but a signal built
    that way says so.
    """
    structured = _structured_nominee_matches(source_id, raw or {})
    matches: list[dict[str, str]] = []
    for stmt in bods:
        kind = _stmt_kind(stmt)
        if kind == "relationship":
            for interest in _interests(stmt):
                blob = " ".join(
                    str(v).lower()
                    for k, v in interest.items()
                    if k in ("type", "details") and isinstance(v, str)
                )
                if any(frag in blob for frag in _NOMINEE_FRAGMENTS):
                    matches.append(
                        {
                            "statement_id": _statement_id(stmt),
                            "match": f"interest mentions nominee ({interest.get('type', '')})",
                        }
                    )
                    break
        elif kind == "person":
            blob_parts: list[str] = []
            rd = _record_details(stmt)
            for name in rd.get("names") or []:
                if isinstance(name, dict):
                    blob_parts.extend(
                        str(v) for v in name.values() if isinstance(v, str)
                    )
            for key in ("details", "publicationDetails"):
                v = rd.get(key)
                if isinstance(v, str):
                    blob_parts.append(v)
            blob = " ".join(blob_parts).lower()
            if any(frag in blob for frag in _NOMINEE_FRAGMENTS):
                matches.append(
                    {
                        "statement_id": _statement_id(stmt),
                        "match": "person record mentions nominee",
                    }
                )
    # Structured matches are deduplicated against the textual ones they would
    # otherwise double-count: the mapper renders each nature code into
    # interest.details, so a ROE filing trips both paths for the same fact.
    if structured:
        matched_codes = sorted({m["nature_code"] for m in structured})
        return RiskSignal(
            code=NOMINEE,
            confidence="high",
            summary=(
                f"Register filed a nominee arrangement "
                f"({len(structured)} record(s), "
                f"{len(matched_codes)} nature-of-control code(s)). "
                "AMLA CDD RTS condition (c)."
            ),
            source_id=source_id,
            hit_id=hit_id,
            evidence={
                "matches": structured,
                "nature_codes": matched_codes,
                "basis": "structured",
            },
        )

    if not matches:
        return None
    return RiskSignal(
        code=NOMINEE,
        # Textual evidence is weaker than a filed code and should not claim
        # the same confidence: "Nominee Services Ltd" is a company name, not a
        # declaration. Structured matches above stay high.
        confidence="medium",
        summary=(
            f"Ownership chain mentions nominee shareholders/directors "
            f"({len(matches)} statement(s)) — matched on descriptive text, "
            "not a filed nominee code. "
            "AMLA CDD RTS condition (c)."
        ),
        source_id=source_id,
        hit_id=hit_id,
        evidence={"matches": matches, "basis": "textual"},
    )


def _layers_signal(
    source_id: str, hit_id: str, bods: list[dict[str, Any]]
) -> RiskSignal | None:
    """Longest entity-only chain in the BODS relationship graph.

    AMLA defines a complex corporate structure as having "three or more
    layers of ownership". We treat that as: there exists a chain of
    relationship edges through ≥3 distinct entity nodes.

    Edge direction: ``interestedParty --(owns)--> subject``. So walking
    from a leaf interestedParty up through subject_ids approximates the
    ownership-direction chain. We DFS over the entity-only subgraph and
    track the longest simple path (cycles guarded via per-path visited
    set).
    """
    # Map statementId -> entity type to filter to entity nodes.
    entity_ids: set[str] = set()
    for stmt in bods:
        if _stmt_kind(stmt) == "entity":
            sid = _statement_id(stmt)
            if sid:
                entity_ids.add(sid)

    # Build adjacency: ip -> {subject1, subject2, ...} restricted to
    # entity nodes (ignore person interestedParties because they end
    # the chain, not extend it).
    adj: dict[str, set[str]] = {}
    for stmt in bods:
        if _stmt_kind(stmt) != "relationship":
            continue
        subj, ip, ip_kind = _relationship_endpoints(stmt)
        if not subj or not ip:
            continue
        # BODS v0.4 bare-string format: ip_kind is "" — resolve from entity_ids.
        if ip_kind == "":
            ip_kind = "entity" if ip in entity_ids else "person"
        if ip_kind != "entity":
            continue
        if subj not in entity_ids or ip not in entity_ids:
            continue
        adj.setdefault(ip, set()).add(subj)

    if not adj and len(entity_ids) < 3:
        return None

    longest = 0
    longest_path: list[str] = []

    def dfs(node: str, visited: list[str]) -> None:
        nonlocal longest, longest_path
        path_len = len(visited)
        if path_len > longest:
            longest = path_len
            longest_path = list(visited)
        for nxt in adj.get(node, ()):
            if nxt in visited:
                continue  # cycle guard
            visited.append(nxt)
            dfs(nxt, visited)
            visited.pop()

    # Start from every entity node — the graph may have multiple roots.
    for start in entity_ids:
        dfs(start, [start])

    if longest < 3:
        return None
    return RiskSignal(
        code=COMPLEX_OWNERSHIP_LAYERS,
        confidence="medium",
        summary=(
            f"Ownership chain has {longest} corporate layers "
            "(AMLA threshold: ≥3)."
        ),
        source_id=source_id,
        hit_id=hit_id,
        evidence={"layers": longest, "longest_path": longest_path},
    )


def _fatf_jurisdiction_signals(
    source_id: str, hit_id: str, bods: list[dict[str, Any]]
) -> list[RiskSignal]:
    """Fire FATF_BLACK_LIST / FATF_GREY_LIST when any entity in the BODS
    bundle is incorporated in a FATF-listed jurisdiction.

    Two separate signals — one per list — so the UI can present them with
    different severities.  Both can fire on the same bundle (e.g. an entity
    that is itself grey-listed but has an owner in a black-listed jurisdiction).

    Lists current as of the June 2026 plenary. Update ``FATF_BLACK_LIST_CODES``
    and ``FATF_GREY_LIST_CODES`` at each FATF plenary (typically February, June,
    October) when the lists are refreshed.
    """
    black_hits: list[dict[str, str]] = []
    grey_hits: list[dict[str, str]] = []

    for stmt in bods:
        if _stmt_kind(stmt) != "entity":
            continue
        j = _entity_jurisdiction(stmt)
        if not j:
            continue
        code = (j.get("code") or "").upper()
        name = j.get("name") or ""
        if not code:
            continue
        entry = {"statement_id": _statement_id(stmt), "code": code, "name": name}
        if code in FATF_BLACK_LIST_CODES:
            black_hits.append(entry)
        elif code in FATF_GREY_LIST_CODES:
            grey_hits.append(entry)

    out: list[RiskSignal] = []

    if black_hits:
        codes = sorted({h["code"] for h in black_hits})
        names = sorted({h["name"] for h in black_hits if h["name"]})
        label = ", ".join(names) if names else ", ".join(codes)
        out.append(
            RiskSignal(
                code=FATF_BLACK_LIST,
                confidence="high",
                summary=(
                    f"Ownership chain reaches into {label}, "
                    "a jurisdiction on the FATF High-Risk list "
                    "(Call for Action / black list, June 2026)."
                ),
                source_id=source_id,
                hit_id=hit_id,
                evidence={"jurisdictions": black_hits, "list": "black"},
            )
        )

    if grey_hits:
        codes = sorted({h["code"] for h in grey_hits})
        names = sorted({h["name"] for h in grey_hits if h["name"]})
        label = ", ".join(names) if names else ", ".join(codes)
        out.append(
            RiskSignal(
                code=FATF_GREY_LIST,
                confidence="medium",
                summary=(
                    f"Ownership chain reaches into {label}, "
                    "a jurisdiction under FATF Increased Monitoring "
                    "(grey list, June 2026)."
                ),
                source_id=source_id,
                hit_id=hit_id,
                evidence={"jurisdictions": grey_hits, "list": "grey"},
            )
        )

    return out


def _eu_high_risk_third_country_signals(
    source_id: str, hit_id: str, bods: list[dict[str, Any]]
) -> list[RiskSignal]:
    """Fire EU_HIGH_RISK_THIRD_COUNTRY for the EU's own Article 29 list.

    Separate from the FATF signals by design — see the comment on
    ``EU_HIGH_RISK_THIRD_COUNTRY_CODES``. Confidence is ``high`` because,
    unlike a FATF listing, an EU designation is a binding legal trigger
    for enhanced due diligence rather than an international assessment.

    One signal per bundle, aggregating every matching entity, so the
    graph can badge each node from ``evidence.jurisdictions``.
    """
    hits: list[dict[str, str]] = []
    for stmt in bods:
        if _stmt_kind(stmt) != "entity":
            continue
        j = _entity_jurisdiction(stmt)
        if not j:
            continue
        code = (j.get("code") or "").upper()
        if not code or code not in EU_HIGH_RISK_THIRD_COUNTRY_CODES:
            continue
        hits.append(
            {
                "statement_id": _statement_id(stmt),
                "code": code,
                "name": j.get("name") or "",
            }
        )

    if not hits:
        return []

    codes = sorted({h["code"] for h in hits})
    names = sorted({h["name"] for h in hits if h["name"]})
    label = ", ".join(names) if names else ", ".join(codes)
    return [
        RiskSignal(
            code=EU_HIGH_RISK_THIRD_COUNTRY,
            confidence="high",
            summary=(
                f"Ownership chain reaches into {label}, on the EU list of "
                "high-risk third countries with strategic AML/CFT "
                f"deficiencies ({EU_HRTC_INSTRUMENT}). EU-listed "
                "jurisdictions attract mandatory enhanced due diligence."
            ),
            source_id=source_id,
            hit_id=hit_id,
            evidence={"jurisdictions": hits, "instrument": EU_HRTC_INSTRUMENT},
        )
    ]


def _possible_obfuscation_signal(
    source_id: str, hit_id: str, signals: list[RiskSignal]
) -> RiskSignal | None:
    """Advisory mirror of AMLA's subjective condition.

    Cannot be judged from data alone — fires ``low`` when the bundle
    already has signals that, taken together, suggest a structure
    "obfuscating ownership". Always notes the human-judgment caveat.
    """
    codes = {s.code for s in signals}
    has_opacity = OPAQUE_OWNERSHIP in codes
    has_layered_concern = (
        COMPLEX_CORPORATE_STRUCTURE in codes
        or (COMPLEX_OWNERSHIP_LAYERS in codes and (NON_EU_JURISDICTION in codes or NOMINEE in codes))
    )
    if not (has_opacity and has_layered_concern):
        return None
    return RiskSignal(
        code=POSSIBLE_OBFUSCATION,
        confidence="low",
        summary=(
            "Advisory: structure combines opacity (unknown/anonymous "
            "parties) with complex layering. AMLA CDD RTS subjective "
            "condition — confirm whether there is a legitimate "
            "economic rationale before relying on this signal."
        ),
        source_id=source_id,
        hit_id=hit_id,
        evidence={
            "triggered_by": sorted(
                codes
                & {
                    OPAQUE_OWNERSHIP,
                    COMPLEX_CORPORATE_STRUCTURE,
                    COMPLEX_OWNERSHIP_LAYERS,
                    NON_EU_JURISDICTION,
                    NOMINEE,
                }
            )
        },
    )
