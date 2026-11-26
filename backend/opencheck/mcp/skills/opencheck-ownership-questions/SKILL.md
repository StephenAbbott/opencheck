---
name: opencheck-ownership-questions
description: Answer who owns a company, what its ownership structure looks like, or what it owns, using opencheck_owners, opencheck_ownership_map, opencheck_subsidiaries and opencheck_export_bods. Use when a question is about shareholders, beneficial owners, a threshold such as 25%, a group structure or subsidiaries.
---

# Ownership questions with OpenCheck

Pick the tool by the question. Each one runs or reuses the same lookup, so
the cost is one check, and a repeat within 15 minutes is free.

## "Who owns it?" → `opencheck_owners(lei)`

It answers with separate lists. Keep them apart:

- `calculated_owners`: holders whose integrated ownership, computed from one
  source's stated shareholdings, is at or above the threshold (`above`) or
  might be (`may_be_above`, when the source gives a range or no size).
  Calculated, not declared.
- `declared_beneficial_owners`: parties a register names as beneficial owners
  of this company.
- `registered_controllers`: companies a register names as controllers (a UK
  relevant legal entity, for example). They are a step in the chain, not the
  people at the top of it.
- `in_both`: the same party on both lists, matched on a register key, never
  on a name alone.

The threshold is the jurisdiction's own rule (for example more than 25% in
the UK) unless you pass `threshold`; `rule="amlr"` uses the EU AML
Regulation's 25% or more. Say which rule applied. `depth=2` or `3` also looks
up each corporate owner with an LEI at or above the threshold. Each extra
hop is a lookup, at most ten per call. `chain_stops` says where and why the
walk stopped ("no LEI to follow"). A state or state body is the top of a
chain, not a stop.

`listed_without_a_figure` holds voting-only and former holders. Mention them,
without a percentage.

## "What does the structure look like?" → `opencheck_ownership_map(lei)`

You get one JSON part and one SVG image. The image draws at most 16 current
ownership and control edges, with officers and board roles left out, and
labels each edge with its source. The JSON carries every edge as a row,
including ended ones. Show the image if the client can and summarise the
rows. Do not describe edges the image does not draw as "shown".
`reporting_exceptions` are GLEIF filings that name no parent: a filing, not
a party. `undisclosed_parents` are parents GLEIF records as withheld.

## "What does it own?" → `opencheck_subsidiaries(lei)`

GLEIF Level 2 is **accounting consolidation**: a consolidating parent need
not be a shareholder, and GLEIF publishes no percentages. Say "consolidates"
or "is consolidated by", never "owns". `include_declared=True` adds other
lists (OECD-UNSD MEIP group membership, EITI declared subsidiaries, Global
Energy Monitor). Keep each list separate and never merge them, because they
measure different things.

## "Give me the data" → `opencheck_export_bods(lei)`

This is the full Beneficial Ownership Data Standard v0.4 graph, as JSON or
JSON Lines, with statements from every source and their licences. Use it
when the user wants to load the graph somewhere else, not to answer a
question in prose.

The answer is a link, not the graph: counts by record type, the licence
notices and a `download_url` that serves this run for about 15 minutes
(`link_expires_at`) without spending a lookup. If you can run code, fetch the
file there and filter it, so only the rows you need reach the conversation.
If you cannot, call again with `delivery="inline"`, narrowed with
`record_types` and `max_statements`; `truncated` says when rows were left out.

## Words to use

- "holds", "is recorded as holding", "consolidated by". Use "owns" only for a
  stated shareholding.
- A percentage only where a source stated one, with that source named.
- Never add two sources' figures for the same holder.
- An ended relationship is "former" or "ended on <date>". It is never
  current.

Before reporting, apply [reading-opencheck-results](../reading-opencheck-results/SKILL.md).
