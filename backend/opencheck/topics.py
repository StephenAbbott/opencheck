"""FollowTheMoney topic slugs, in English (Phase 133).

Two user-facing strings printed the slugs verbatim. On a live Rosneft lookup
the OpenSanctions row read:

    topics: corp.disqual, debarment, export.control · 28 datasets
    Appears on 28 published listings, recorded under the topics corp.disqual
    and 5 others.

`corp.disqual` is a key in a taxonomy, not a word. The frontend has had
`topicLabel()` for the same vocabulary since Phase 124 — for OpenAleph's
collection tags — and these two strings are built server-side, so they never
reached it.

**Translating is not judging.** The comment this replaces argued that rendering
`sanction` as "sanctioned" would turn a source's classification into
OpenCheck's judgement, and it was right about that: "sanctioned" is an
adjective applied to the company. What it does not follow from is that the
slug must be printed. These labels are noun phrases naming *the topic* —
"sanctions listing", not "sanctioned" — so the sentence still says
OpenSanctions recorded the record under a topic and still leaves the judgement
to the signals layer. OpenSanctions publishes display names for exactly this
purpose in its own model; printing the internal key quotes it less faithfully,
not more.

**The set is pinned.** Every topic in `sources.opensanctions._RISK_TOPICS` —
OpenSanctions' published `target_topics`, which the adapter mirrors rather
than subsets — must have a label here, enforced by the canary in
`tests/test_opensanctions_live.py`. A new upstream topic therefore fails the
build instead of reaching a reader as a slug, the same way an unclassified one
already fails rather than being fetched and silently not understood.

The map is deliberately wider than that set: it labels **every** topic in
FollowTheMoney's topic type as OpenSanctions publishes it (`model.types.topic.values`
in the same model.json, 75 topics on 11 Nov 2026). Records carry non-target
topics too — `fin.bank` on a bank, `role.pol` on a politician — and they reach
the OpenSanctions summary and finding beside the target ones, and OpenAleph's
archive-matches list. Until the raw-slugs ticket (10 Nov 2026) only the
targets were labelled and the rest read "fin bank" and "role pol".
`tests/test_topic_labels.py` pins the whole list offline and, in the weekly
live-smoke run, against the live model, so a new upstream topic fails a build
rather than reaching a reader as a slug. The wording is OpenCheck's (lower
case, British spelling, a full phrase for "PEP"), not upstream's display name.
"""

from __future__ import annotations

from collections.abc import Iterable

#: Topic slug → the noun phrase a reader sees. Lower case, because every one
#: of these is rendered inside a sentence or a comma-separated fragment.
TOPIC_LABEL: dict[str, str] = {
    # Sanctions and adjacent designations. `sanction.control` says what the
    # relationship is rather than borrowing the chip's shorter label: this
    # runs inside prose, where "sanction ownership or control" does not parse.
    "sanction": "sanctions listing",
    "sanction.linked": "linked to a sanctions listing",
    "sanction.control": "owned or controlled by a sanctioned party",
    "sanction.counter": "counter-sanctions listing",
    # Export controls and trade restrictions.
    "export.control": "export control listing",
    "export.control.linked": "linked to an export control listing",
    "export.risk": "trade risk",
    # Investment restrictions.
    "invest.ban": "investment ban",
    "invest.risk": "investment risk",
    # Procurement exclusion and corporate disqualification. "disqualified",
    # not "disqualified director": upstream applies the topic to companies as
    # well as to people, and naming the role would invent one.
    "debarment": "debarred from public contracts",
    "corp.disqual": "disqualified",
    # The impersonator, never the firm it copies: upstream applies the topic
    # to the clone only, and the label must not read as a fact about the
    # company being looked up.
    "corp.clone": "clone of a real firm",
    # Political exposure.
    "role.pep": "politically exposed person",
    "role.rca": "relative or close associate",
    "role.oligarch": "oligarch",
    # Criminality. These name the *category the record was filed under*, which
    # is why they are bare nouns: "financial crime", never "committed fraud".
    "crime": "crime",
    "crime.boss": "organised crime",
    "crime.fin": "financial crime",
    "crime.fraud": "fraud",
    "crime.terror": "terrorism",
    "crime.theft": "theft",
    "crime.traffick": "trafficking",
    "crime.war": "war crimes",
    "wanted": "wanted by law enforcement",
    # Maritime risk.
    "mare.shadow": "shadow-fleet vessel",
    "mare.detained": "detained vessel",
    # Regulatory action and residual watchlisting.
    "reg.action": "regulatory action",
    "reg.warn": "regulatory warning",
    "poi": "person of interest",
    # A designated place (FtM ``Territory``), not a party.
    "geo.risk": "high-risk area",
    # Beyond OpenSanctions' target topics: the rest of FtM's topic list.
    # Records carry these beside the target topics, and OpenAleph collections
    # carry them too.
    "asset.frozen": "frozen asset",
    "role.diplo": "diplomatic service",
    "role.judge": "judiciary",
    # Corporate form and listing. The register's own words for what the record is.
    "corp.offshore": "offshore company",
    "corp.public": "public listed company",
    "corp.shell": "shell company",
    # Further crime categories, bare nouns like the ones above.
    "crime.cyber": "cybercrime",
    "crime.env": "environmental violations",
    "crime.traffick.drug": "drug trafficking",
    "crime.traffick.human": "human trafficking",
    "forced.labor": "forced labour",
    # Financial services. "fin.adivsor" is spelled as upstream publishes it.
    "fin": "financial services",
    "fin.adivsor": "financial advisor",
    "fin.bank": "bank",
    "fin.fund": "fund",
    # Government and the state.
    "gov": "government",
    "gov.admin": "civil service",
    "gov.executive": "executive branch of government",
    "gov.financial": "central banking and financial integrity",
    "gov.head": "head of government or state",
    "gov.igo": "intergovernmental organisation",
    "gov.judicial": "judicial branch of government",
    "gov.legislative": "legislative branch of government",
    "gov.muni": "municipal government",
    "gov.national": "national government",
    "gov.religion": "religious leadership",
    "gov.security": "security services",
    "gov.soe": "state-owned enterprise",
    "gov.state": "state government",
    "mil": "military",
    # Maritime activity.
    "mare.sts": "ship-to-ship transfer",
    # Political and civil-society organisations.
    "pol.party": "political party",
    "pol.union": "union",
    "rel": "religion",
    # Occupations and roles. A PEP's level keeps the full phrase.
    "role.acct": "accountant",
    "role.act": "activist",
    "role.civil": "civil servant",
    "role.journo": "journalist",
    "role.lawyer": "lawyer",
    "role.lobby": "lobbyist",
    "role.pep.frmr": "former politically exposed person",
    "role.pep.intl": "international politically exposed person",
    "role.pep.natl": "national politically exposed person",
    "role.pol": "politician",
    "role.spy": "spy",
}


def topic_label(topic: str) -> str:
    """One topic, in English.

    An unmapped slug is prettified rather than hidden: that a record was
    tagged is information even when this build does not know the tag's name.
    The pinned topic list makes the fallback unreachable for anything
    OpenSanctions publishes, so it only ever fires for a topic that arrived
    after the last check (the weekly live test reports it).
    """
    label = TOPIC_LABEL.get(topic)
    if label:
        return label
    return topic.replace("_", " ").replace(".", " ")


def topic_phrase(topics: Iterable[str]) -> str:
    """Several topics as one English fragment: "a and b", "a, b and c".

    For the one sentence where the topics *are* the sentence rather than a
    parenthetical after it — `risk.py`'s PEP summary reads "OpenSanctions tags
    this record as \u2026", and there is nothing left if the topics come out.
    """
    labels = topic_list(topics)
    if not labels:
        return "a risk topic"
    if len(labels) == 1:
        return labels[0]
    return f"{', '.join(labels[:-1])} and {labels[-1]}"


def topic_list(topics: Iterable[str]) -> list[str]:
    """Labels for several topics, deduplicated, in first-seen order.

    Deduplicated because two slugs can share a label only by accident today
    but a merge upstream would make it real, and "financial crime, financial
    crime" is not a longer answer.
    """
    out: list[str] = []
    for topic in topics:
        label = topic_label(topic)
        if label not in out:
            out.append(label)
    return out
