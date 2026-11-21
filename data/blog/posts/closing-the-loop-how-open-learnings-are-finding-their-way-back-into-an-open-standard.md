---
title: "Closing the loop: how open learnings are finding their way back into an open standard"
subtitle: "What building OpenCheck, and testing the EU’s new rules against BODS, has taught me about the standard, and where those lessons now sit"
published: "2026-11-21T13:25:01.000Z"
medium_url: "https://medium.com/@stephenabbottpugh/closing-the-loop-how-open-learnings-are-finding-their-way-back-into-an-open-standard-18065f6efda0"
medium_id: "18065f6efda0"
topics:
  - "Open Standards"
  - "Beneficial Ownership"
  - "Beneficial Ownership Data"
  - "Open Data"
image: "/blog/media/closing-the-loop-how-open-learnings-are-finding-their-way-back-into-an-open-standard/01.webp"
image_alt: ""
---

Last week, to coincide with the opening of the [Corporate Registers Forum](https://www.crfsingapore2026.com/) in Singapore, I [posted](https://lnkd.in/p/gWt7QfkB) that a standard is only a standard once others use it. Three register vendors that support the [Beneficial Ownership Data Standard](https://standard.openownership.org/en/0.4.0/) (BODS) sponsored the forum.

Use is half the story. The other half is what use teaches. Every implementer finds places where the standard is unclear, silent or wrong. It improves only if those findings travel back to its maintainers.

This post is about closing that loop.

## What building OpenCheck taught me

[OpenCheck](https://opencheck.world/), my open-source demonstration of what ownership data can do in an open standard, reads dozens of registers/datasets and publishes everything as BODS. Working that hard against one schema surfaces things reading it never does.

Two examples:

**Same value, hidden bug**— For 207 development phases, every OpenCheck statement had the same statementId and recordId. Code that compared the wrong one still worked. When the OECD's statements arrived with different values, Shell and its Norwegian subsidiary appeared as two unlinked companies. That is now [issue #776](https://github.com/openownership/data-standard/issues/776): the guidance should warn that equal identifiers hide consumer bugs.

**A closed list, read as open**— Three of my own mappers put a register’s own wording into entityType.subtype, which only accepts a fixed set of codes. If one person makes that mistake three times, the documentation can be clearer. That became [issue #769](https://github.com/openownership/data-standard/issues/769) and [pull request #778](https://github.com/openownership/data-standard/pull/778).

Smaller fixes followed: saying what an [omitted or null value means](https://github.com/openownership/data-standard/pull/765), [aligning a field description](https://github.com/openownership/data-standard/pull/766) with what the schema already enforces, [fixing broken links](https://github.com/openownership/data-standard/pull/767) on the schema browser, and [recommending start dates](https://github.com/openownership/data-standard/pull/768) on interests. A [guidance page on intermediaries](https://github.com/openownership/data-standard/pull/781) such as lawyers and accountants answers [an issue](https://github.com/openownership/data-standard/issues/626) I opened years ago.

![](/blog/media/closing-the-loop-how-open-learnings-are-finding-their-way-back-into-an-open-standard/02.webp)

## What the EU’s rules taught me

This month I mapped two EU texts against BODS, field by field.

The [formats for central registers](https://stephenabbottpugh.medium.com/the-eu-has-written-down-what-a-beneficial-ownership-data-standard-must-hold-9d59b015fa11) list 66 requirements: 33 map directly, 21 in part and 12 have no field yet, seven on trusts. AMLA’s [customer due diligence standards](https://stephenabbottpugh.medium.com/amla-has-listed-the-fields-nobody-has-named-the-format-2c8c8da1c48a) list 57 relevant requirements: 34 map, 13 in part and 10 don’t. Both crosswalks are public Gists ([formats](https://gist.github.com/StephenAbbott/069f2692038c25757919b805765d9311), [due diligence](https://gist.github.com/StephenAbbott/a85da75161469de76e335def4102a7a8)).

A gap in a blog post changes nothing, so each gap with a clear legal basis became a feature request citing its article:

- [Trust governing law](https://github.com/openownership/data-standard/issues/772)
- [Classes of beneficiaries, objects of a power and default takers](https://github.com/openownership/data-standard/issues/773)
- [Legal form as an ISO 20275 code](https://github.com/openownership/data-standard/issues/774), with a [pull request](https://github.com/openownership/data-standard/pull/779) showing one way to do it
- [The number and percentage of shares listed](https://github.com/openownership/data-standard/issues/775)
- Share class and the EU’s control codes, as [comments](https://github.com/openownership/data-standard/issues/466#issuecomment-6778029259) on the existing work to redesign interests

The crosswalk also found a [mistake in the trusts guidance](https://github.com/openownership/data-standard/issues/771). [Issue #780](https://github.com/openownership/data-standard/issues/780) tracks how BODS v0.4 covers both texts in one place.

## Why a registry?

Not every learning is ready to be an issue. Some need more evidence; others concern how people extend BODS, which the standard [allows](https://standard.openownership.org/en/main/standard/system/conformance.html#extending-the-schema) but gives no place to record.

So I started [bods-registry](https://github.com/StephenAbbott/bods-registry), a community record of BODS in use. It holds three things:

- **Implementations**— who uses BODS, which version, what they extended and what they couldn’t model
- **Extensions**— fields and codes added beyond the core schema, with a template and a validator
- **Learnings**— dated notes on what an implementation revealed about the standard, each pointing at the upstream issue it feeds

The rules are simple: public evidence only; describe, don’t judge; feed upstream. In the README’s words, the registry “is a staging area, not a destination”. The first [eight learnings](https://github.com/StephenAbbott/bods-registry/pull/1) came from OpenCheck. I’d like most of the next ones to come from someone else.

![](/blog/media/closing-the-loop-how-open-learnings-are-finding-their-way-back-into-an-open-standard/03.webp)

## Working in the open

Every issue and pull request is public, on the [data standard repository](https://github.com/openownership/data-standard) and Open Ownership’s [feature tracker](https://github.com/orgs/openownership/projects/9/views/1). [Open Ownership](https://www.openownership.org/) stewards BODS and [Open Data Services](https://opendataservices.coop/) maintains it: they decide what lands. My job is to make each request easy to judge.

## What next

- **Implementers**— if BODS surprised you, add a learning to the registry, or open an issue. A one-line note with a link is enough
- **Register vendors**— add a profile of how your product uses BODS, so registrars can see it
- **Anyone mapping a new law**— publish the crosswalk: a gap found twice is easier to fix once

A standard is only a standard once others use it. It only stays useful if what they learn comes back.

*For four years, I was the product owner of the* [*Beneficial Ownership Data Standard*](https://standard.openownership.org/)*, the world’s leading open standard for beneficial ownership information and continue to advocate to improve beneficial ownership data quality and standardisation. To learn more or if you need technical guidance or consultancy support, contact me via* [*beneficialownership.co.uk*](https://www.beneficialownership.co.uk/)*.*
