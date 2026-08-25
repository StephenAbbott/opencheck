"""Every topic OpenCheck can show a reader must have a label, in both languages.

Two strings printed FollowTheMoney slugs verbatim for three phases — the
OpenSanctions summary line and its finding sentence — while the frontend had a
translation table the whole time. It never reached them because both are built
server-side.

Now there are two tables, one per language, and two tables of one vocabulary
drift. They already had: `export_control` and `asset__frozen` sat in the
frontend map, and neither is a FollowTheMoney slug (`export.control`,
`asset.frozen`), so the two topics that map existed to translate fell through
to the prettifier and were never noticed.

Three things are pinned here:

* every topic OpenSanctions publishes as a target has a label — so a new
  upstream topic fails the build rather than reaching a reader as a slug, the
  same way `test_opensanctions_live` already fails when one is fetched but not
  classified;
* the two maps agree wherever they overlap;
* neither map has an entry the other lacks, for a slug in the published set.

The backend map is the authoritative one: it is next to the taxonomy it
describes, and the canary that enumerates the published set is a backend test.
"""

from __future__ import annotations

import re
from pathlib import Path

from opencheck.sources.opensanctions import _RISK_TOPICS
from opencheck.topics import TOPIC_LABEL, topic_label, topic_list

REPO = Path(__file__).resolve().parents[2]
_VOCAB = REPO / "frontend" / "src" / "lib" / "vocab.ts"


def _frontend_map() -> dict[str, str]:
    """`OPENALEPH_TOPIC` from vocab.ts, as a dict.

    Deliberately a small parser rather than a JSON dump built at test time:
    the point is to read what the shipped source says, and a build step that
    exports it would be one more thing that can be stale.
    """
    text = _VOCAB.read_text(encoding="utf-8")
    start = text.index("export const OPENALEPH_TOPIC")
    assign = text.index("= {", start)
    depth = 0
    body_start = None
    end = assign
    for i in range(assign, len(text)):
        if text[i] == "{":
            depth += 1
            if depth == 1:
                body_start = i
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                end = i
                break
    body = text[body_start : end + 1]
    # Keys are quoted when they contain a dot and bare when they do not —
    # both forms are valid TypeScript and both appear in the file.
    pairs = re.findall(r'^\s{2}(?:"([^"]+)"|([A-Za-z_][\w]*))\s*:\s*"([^"]*)"', body, re.M)
    return {quoted or bare: label for quoted, bare, label in pairs}


def test_the_frontend_map_is_readable() -> None:
    """Guards the parser itself — a silent zero would pass every test below."""
    parsed = _frontend_map()
    assert len(parsed) >= 25, f"only parsed {len(parsed)}: {sorted(parsed)}"
    assert parsed["role.pep"] == "politically exposed person"
    assert parsed["sanction.control"] == "owned or controlled by a sanctioned party"


def test_every_published_topic_has_a_label() -> None:
    """A slug a reader can be shown must be a phrase by the time they see it."""
    missing = sorted(set(_RISK_TOPICS) - set(TOPIC_LABEL))
    assert not missing, f"opencheck.topics.TOPIC_LABEL is missing: {missing}"

    missing_ts = sorted(set(_RISK_TOPICS) - set(_frontend_map()))
    assert not missing_ts, f"vocab.ts OPENALEPH_TOPIC is missing: {missing_ts}"


def test_the_two_maps_agree() -> None:
    """One vocabulary, one wording, whichever side of the wire built the string."""
    ts = _frontend_map()
    disagree = {
        topic: (label, ts[topic])
        for topic, label in TOPIC_LABEL.items()
        if topic in ts and ts[topic] != label
    }
    assert not disagree, f"backend/frontend labels differ: {disagree}"

    only_backend = sorted(set(TOPIC_LABEL) - set(ts))
    only_frontend = sorted(set(ts) - set(TOPIC_LABEL))
    assert not only_backend, f"missing from vocab.ts: {only_backend}"
    assert not only_frontend, f"missing from opencheck/topics.py: {only_frontend}"


def test_the_keys_are_followthemoney_slugs() -> None:
    """`export_control` and `asset__frozen` were in the frontend map and in no
    upstream taxonomy — underscore forms of `export.control` / `asset.frozen`,
    so both fell through to the prettifier."""
    for topic in {**TOPIC_LABEL, **_frontend_map()}:
        assert "_" not in topic, f"{topic!r} is not a FollowTheMoney slug"
        assert topic == topic.lower(), f"{topic!r} is not lower case"


def test_labels_name_the_topic_and_do_not_judge_the_subject() -> None:
    """The distinction the raw slugs were defended on.

    Printing `sanction` was justified in `findings.py` on the grounds that
    "sanctioned" would turn a source's classification into OpenCheck's
    judgement. That is true of the adjective and not of the slug: these are
    noun phrases naming the topic, so the sentence still reports what
    OpenSanctions recorded.
    """
    for topic, label in TOPIC_LABEL.items():
        assert label == label.lower(), f"{topic}: {label!r} is not lower case"
        assert not label.startswith("is "), f"{topic}: {label!r} predicates"
        assert label != "sanctioned", "the exact wording rule 7 rejects"


def test_an_unknown_slug_is_prettified_not_hidden() -> None:
    # That a record was tagged is information even when this build does not
    # know the tag's name.
    assert topic_label("some.new.topic") == "some new topic"
    assert topic_label("role.pep") == "politically exposed person"


def test_topic_list_deduplicates_labels() -> None:
    assert topic_list(["poi", "poi", "crime.fin"]) == [
        "person of interest",
        "financial crime",
    ]
    assert topic_list([]) == []


# ----------------------------------------------------------------------
# No slug reaches prose (Phase 134)
# ----------------------------------------------------------------------

_TOKEN = re.compile(r"\b[a-z][a-z.]*[a-z]\b")

#: Words that are legitimately in a sentence because a *label* contains them.
#: Four topics are their own label — `crime`, `fraud`, `theft`, `wanted` — so a
#: bare-word rule would flag the translation it is meant to enforce. `sanction`
#: is not among them: the label is "sanctions listing", so the singular
#: appearing alone means the slug leaked.
_LABEL_WORDS = {word for label in TOPIC_LABEL.values() for word in label.split()}


def _prose_slugs(text: str) -> list[str]:
    """Topic slugs in a sentence a reader sees.

    Not just the dotted ones: `sanction`, `debarment` and `poi` are slugs
    too, and a rule that only caught `sanction.linked` would have passed the
    string this test exists to have failed.
    """
    return [
        token
        for token in _TOKEN.findall(text)
        if token in _RISK_TOPICS and token not in _LABEL_WORDS
    ]


def test_no_signal_summary_prints_a_topic_slug() -> None:
    """Every signal every published topic can produce, in English.

    The parentheticals removed in Phase 134 (`"… as sanctioned (sanction,
    sanction.linked)."`) were not one string but sixteen, spread over three
    modules, and they arrived one at a time as each topic family was
    classified. A rule about how the sentences are *built* would not have
    caught them, so this drives the builders with real topics and reads the
    output.

    `evidence["topics"]` is deliberately not checked here — that is the
    machine-readable record, and it must keep the slugs.
    """
    from opencheck.cross_check import _signals_from_os
    from opencheck.risk import _opensanctions_topic_signals_from_entity
    from opencheck.sources import SearchKind, SourceHit

    target = {
        "statement_id": "s1",
        "name": "Acme Ltd",
        "kind": "entity",
        "birth_year": None,
    }
    offenders: list[tuple[str, str, str]] = []
    for topic in sorted(_RISK_TOPICS):
        entity = {
            "id": "os-1",
            "caption": "Acme Ltd",
            "properties": {"topics": [topic]},
        }
        for sig in _opensanctions_topic_signals_from_entity("os-1", entity):
            for slug in _prose_slugs(sig.summary):
                offenders.append((topic, sig.code, slug))
        hit = SourceHit(
            source_id="opensanctions",
            hit_id="os-1",
            kind=SearchKind.ENTITY,
            name="Acme Ltd",
            summary="",
            raw=entity,
        )
        for sig in _signals_from_os(hit, target, min_score=0.8):
            for slug in _prose_slugs(sig.summary):
                offenders.append((topic, sig.code, slug))
    assert not offenders, f"topic slugs in prose: {offenders}"
