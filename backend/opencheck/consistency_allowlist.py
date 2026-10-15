"""Which record comparisons may reach a reader (Phase 337, Phase D of the plan).

The record-consistency check (``consistency.py``) compares every pair of
independent statements about one referent on four fields and has counted the
outcomes at ``/consistencystats`` since 2 Sept 2026. The gate agreed on the
Notion plan before anything was built: a ``field|source_a|source_b`` row may
enter the UI only when its measured ``disagree / (agree + disagree)`` is
under 10 % **and** it has fired at least once, after two weeks of traffic. A
row above 10 % is two sources answering different questions and is
re-aligned or dropped, never shown.

This module is that gate, written down. It is **generated from a reading**,
not from a rule run at request time: the counters describe production as a
whole, and a lookup must not decide what to show by consulting a mutable
global. The reading is named (:data:`READING`) so the next one can be
compared with it, and the test pins the lists so a change here is a visible,
reviewed edit.

The 15 Oct 2026 reading (``since`` 1 Oct 09:48 UTC, 13 d 21 h, 41,100
lookups, persisted counters) decided:

* **Liveness** — the comparison the whole effort was started for — holds:
  17,479 decided pairs, 4.6 % disagree. The fifteen register↔GLEIF rows
  under 10 % with at least one firing enter, plus the two Romanian registers
  against each other (522/2). ``firmenbuch|gleif`` (11.1 %),
  ``cnpj_brazil|gleif`` (10.2 %), ``corporations_canada|gleif`` (10.3 %) and
  ``gleif|zefix`` (17/2) stay out until re-read after the lapsed-LEI change
  in ``consistency._extract_liveness``.
* **Jurisdiction** holds as a field — 36,348 decided, 73 disagree, 0.2 % —
  so every pair with at least twenty decided comparisons enters, whether or
  not it has fired. The floor is a thin-row guard (``cr_hongkong|openaleph``
  was 5/2), not a change to the gate.
* **Founding date** does not enter as a field. Register↔OpenAleph split five
  passing / six above 10 % (``bolagsverket|openaleph`` 27 %), and OpenAleph's
  per-record lineage — which collection the date came from — is unknown, so
  a register↔OpenAleph founding-date comparison is a comparison with an
  unknown second party. Counted and logged; not shown.
* **Identifier clash** does not enter. With the bridge dropped from agreement
  (Phase 268) a pair sharing only the register number has nothing left to
  agree on, so ``agree`` is structurally 0 and the rate reads 100 % on a
  single clash; the field needs a referent-pair denominator
  (``consistencystats`` ``compared``, added in this phase) and the Romanian
  canonical-form fix (also this phase) before its rows mean anything.
  Counted and logged; not shown.

Pairs are stored sorted, as the counters key them.
"""

from __future__ import annotations

#: The ``/consistencystats`` reading these lists were generated from.
READING = "2026-10-15"

#: ``liveness`` pairs that passed the gate on the reading above
#: (``agree/disagree`` in the comment).
LIVENESS_PAIRS: frozenset[tuple[str, str]] = frozenset(
    {
        ("abr_australia", "gleif"),  # 347/8
        ("acra_singapore", "gleif"),  # 54/1
        ("anaf_romania", "gleif"),  # 514/4
        ("anaf_romania", "onrc_romania"),  # 522/2
        ("ares", "gleif"),  # 2453/2
        ("bolagsverket", "gleif"),  # 1216/9
        ("brreg", "gleif"),  # 1318/33
        ("companies_house", "gleif"),  # 270/2
        ("cro", "gleif"),  # 227/15
        ("cvr_denmark", "gleif"),  # 1010/25
        ("gleif", "malta_mbr"),  # 453/28
        ("gleif", "ny_dos"),  # 129/4
        ("gleif", "nz_companies"),  # 172/7
        ("gleif", "onrc_romania"),  # 572/31
        ("gleif", "opencorporates"),  # 3482/175
    }
)

#: ``jurisdiction`` pairs with at least twenty decided comparisons and under
#: 10 % disagreement on the reading above.
JURISDICTION_PAIRS: frozenset[tuple[str, str]] = frozenset(
    {
        ("abr_australia", "gleif"),  # 358/0
        ("abr_australia", "openaleph"),  # 86/0
        ("acra_singapore", "gleif"),  # 55/0
        ("anaf_romania", "gleif"),  # 712/1
        ("anaf_romania", "onrc_romania"),  # 761/0
        ("anaf_romania", "openaleph"),  # 134/0
        ("anaf_romania", "wikidata"),  # 66/0
        ("ares", "gleif"),  # 2612/2
        ("ares", "openaleph"),  # 489/1
        ("ares", "wikidata"),  # 1767/0
        ("ariregister", "climatetrace"),  # 25/0
        ("ariregister", "gleif"),  # 27/0
        ("ariregister", "opensanctions"),  # 25/0
        ("ariregister", "wikidata"),  # 24/0
        ("bolagsverket", "gleif"),  # 1477/1
        ("bolagsverket", "openaleph"),  # 346/0
        ("bolagsverket", "wikidata"),  # 21/0
        ("brreg", "climatetrace"),  # 20/0
        ("brreg", "gleif"),  # 1364/1
        ("brreg", "openaleph"),  # 342/0
        ("brreg", "opensanctions"),  # 31/3
        ("brreg", "wikidata"),  # 47/0
        ("climatetrace", "companies_house"),  # 46/0
        ("climatetrace", "cvr_denmark"),  # 21/0
        ("climatetrace", "gleif"),  # 292/2
        ("climatetrace", "openaleph"),  # 35/0
        ("climatetrace", "opencorporates"),  # 78/0
        ("climatetrace", "opensanctions"),  # 279/6
        ("climatetrace", "wikidata"),  # 175/3
        ("cnpj_brazil", "gleif"),  # 264/0
        ("cnpj_brazil", "openaleph"),  # 73/0
        ("companies_house", "gleif"),  # 1151/0
        ("companies_house", "wikidata"),  # 101/3
        ("corporations_canada", "gleif"),  # 159/0
        ("corporations_canada", "openaleph"),  # 40/0
        ("cr_hongkong", "gleif"),  # 22/0
        ("cro", "gleif"),  # 256/1
        ("cro", "openaleph"),  # 34/0
        ("cvr_denmark", "gleif"),  # 1035/0
        ("cvr_denmark", "openaleph"),  # 332/0
        ("cvr_denmark", "opensanctions"),  # 28/0
        ("cvr_denmark", "wikidata"),  # 77/0
        ("firmenbuch", "gleif"),  # 3783/5
        ("firmenbuch", "openaleph"),  # 997/2
        ("gleif", "inpi"),  # 50/0
        ("gleif", "krs_poland"),  # 546/0
        ("gleif", "kvk"),  # 75/0
        ("gleif", "malta_mbr"),  # 535/1
        ("gleif", "ny_dos"),  # 133/0
        ("gleif", "nz_companies"),  # 183/1
        ("gleif", "onrc_romania"),  # 641/0
        ("gleif", "opencorporates"),  # 6314/1
        ("gleif", "prh"),  # 1500/0
        ("gleif", "wikidata"),  # 2846/19
        ("gleif", "zefix"),  # 20/0
        ("krs_poland", "openaleph"),  # 140/0
        ("malta_mbr", "openaleph"),  # 135/0
        ("ny_dos", "openaleph"),  # 32/0
        ("nz_companies", "openaleph"),  # 50/0
        ("onrc_romania", "openaleph"),  # 116/0
        ("onrc_romania", "wikidata"),  # 62/0
        ("openaleph", "prh"),  # 82/0
        ("openaleph", "wikidata"),  # 657/2
        ("opencorporates", "wikidata"),  # 1415/4
        ("opensanctions", "wikidata"),  # 275/7
        ("prh", "wikidata"),  # 30/0
    }
)

#: Field → the pairs that may be shown for it. A field absent here is
#: counted and logged, never shown.
SHOWN: dict[str, frozenset[tuple[str, str]]] = {
    "liveness": LIVENESS_PAIRS,
    "jurisdiction": JURISDICTION_PAIRS,
}


def is_shown(field: str, source_a: str, source_b: str) -> bool:
    """Whether a comparison on ``field`` between the two sources has passed
    the gate. Order-insensitive, as the counters are."""
    pairs = SHOWN.get(field)
    if not pairs:
        return False
    a, b = sorted((source_a, source_b))
    return (a, b) in pairs
