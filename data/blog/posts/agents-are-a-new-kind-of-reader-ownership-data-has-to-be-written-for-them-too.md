---
title: "Agents are a new kind of reader. Ownership data has to be written for them too"
subtitle: "How OpenCheck tells AI agents where to look, what a question costs and how to read the answer and what twelve evals showed"
published: "2026-11-06T08:50:52.980Z"
medium_url: "https://medium.com/@stephenabbottpugh/agents-are-a-new-kind-of-reader-ownership-data-has-to-be-written-for-them-too-f2fe3e26fc46"
medium_id: "f2fe3e26fc46"
topics:
  - "Beneficial Ownership"
  - "AI"
  - "MCP"
  - "Open Standards"
image: "/blog/media/agents-are-a-new-kind-of-reader-ownership-data-has-to-be-written-for-them-too/01.webp"
image_alt: ""
---

*How* [*OpenCheck*](https://opencheck.world/) *tells AI agents where to look, what a question costs and how to read the answer — and what twelve evals showed*

More of the questions [OpenCheck](https://opencheck.world/) answers could now be asked by an AI agent, not by a person at a screen. Someone types “who owns this company?” into Claude, and the agent decides which tools to call and how to read what comes back.

That changes who has to [understand ownership data](https://www.beneficialownership.co.uk/). A compliance analyst learns over time that a sanctions screen with a gap in it isn’t a clean screen, or that a parent company listed by the [Global Legal Entity Identifier Foundation](https://www.gleif.org/en) (GLEIF) isn’t a shareholder. An agent meets each answer cold, every time. If it misreads one, the person reading its summary has no way to know.

Next week at [Connected Data London](https://2026.connected-data.london/), I’m co-presenting a masterclass with Paco Nathan, [*How to get started in Money Laundering: Investigative Multimodal Graph Engineering*](https://2026.connected-data.london/talks/how-to-get-started-in-money-laundering-investigative-multimodal-graph-engineering/). The plan is that agents will be calling [OpenCheck](https://opencheck.world/) live in the room. So I’ve spent the last few days on one question: how do you help an agent find the right answer, and then read it honestly?

## What goes wrong

Ownership data is full of answers that look like other answers. The mistakes agents make are the ones people make too:

- **Empty isn’t zero -** A screen that couldn’t reach a source, or covered 25 of 133 related parties, has not found nothing. It has not looked.
- **A name match isn’t an identity -** “Possible name match only” means two records share a name. It doesn’t mean they are the same person.
- **Consolidation isn’t ownership -** A GLEIF parent consolidates a company in its accounts. It need not own a single share, and GLEIF publishes no percentages.
- **The LEI isn’t the company -** A lapsed Legal Entity Identifier (LEI) record can still say “ACTIVE”, because nobody has checked it since it lapsed.
- **Three sources aren’t three owners -** If three sources each say the Norwegian state holds 67% of [Equinor](https://opencheck.world/?lei=OW6OFBNCKXC4US5C7523&mode=full), the state holds 67%, not 201%.
- **Silence can be the register’s limit -** Cyprus publishes officers, not beneficial owners. No owners in the answer says nothing about the company.

None of these is an edge case. Each came from a real OpenCheck response.

## Write the answer for the reader

OpenCheck’s first defence is in the answers themselves. Every lookup carries a summary that says what was read and what wasn’t, and a `degraded_sources` list naming any check that didn't fully run.

When it is not empty, the summary says so: *"the absence of their signals is not a clean screen."* Every risk signal says whether it is a `risk` finding or `context`, and how firmly it was matched. As I wrote about [provenance](/blog/where-did-this-come-from-and-is-it-still-true), the [Beneficial Ownership Data Standard](https://standard.openownership.org/en/0.4.0/) (BODS) then carries the evidence on every statement: who said it, when and in what words.

That goes a long way. But an agent still has to know where to start, and what each call costs.

## Tell the agent the order, and the cost

[NVIDIA’s Auto Ontology](https://github.com/NVIDIA/auto-ontology) project gave me two ideas I’ve now built into OpenCheck’s [MCP server](https://opencheck.world/api).

**Handshake instructions -** When an agent connects, the server now hands it a short guide (381 words) in six steps: find, set expectations, check, read it honestly, keep or hand on, and what to do with many companies. It ends with what each call costs. Eight of the 15 tools spend a shared lookup budget of ten fresh checks a minute per address, and the other seven are free. A test fails the build if a new tool is left out of the guide, or if it grows too long for clients to load.

![](/blog/media/agents-are-a-new-kind-of-reader-ownership-data-has-to-be-written-for-them-too/02.webp)

Cost matters more than it sounds. A room of thirty people behind one Wifi address shares that budget. An agent that loops over 200 companies one lookup at a time spends it for everyone. So the guide points anyone exploring hundreds of companies to the [published bulk datasets](https://github.com/StephenAbbott/opencheck/releases) instead, such as the [Estonian register](/blog/how-opencheck-uses-estonias-e-business-register-s-bulk-files-and-apis) or the [Azerbaijani Laundromat companies](https://github.com/StephenAbbott/opencheck/releases/tag/dataset-azerbaijani-laundromat-2026-10-06) we’ll use in London.

**A pre-flight question -** A new tool, `opencheck_knowability`, answers "what can be known here?" before any lookup. Give it a country code and it says what that country's register publishes about beneficial owners, who may see it, and which OpenCheck sources read it. It costs nothing from the budget. An agent asked about a Cypriot company now learns first that Cyprus's register is open only to authorities and obliged entities. It frames a thin answer as the register's limit, not as a finding. For a country OpenCheck holds no notes on, it says so rather than guessing.

## A skill, and a test of whether it helps

The third layer is an [agent skill](https://github.com/StephenAbbott/opencheck/tree/main/skills/opencheck-due-diligence): a folder of instructions that Claude (or another agent that supports skills) loads when a question calls for it. It holds the call order, the reading rules above, a page on the BODS shapes an agent meets in an export and a page on loading the bulk datasets.

A skill is only worth installing if it changes what the agent does. So, following NVIDIA again, it comes with evals. These are twelve test cases, each built on a real OpenCheck response I recorded from production. Among them:

- a batch of four companies where one, [Heineken](https://opencheck.world/?lei=724500K5PTPSST86UQ23), shows no risk signals but the Dutch register didn’t answer;
- [Bentcard Import LLP](https://opencheck.world/?lei=54930007FGRO3F0RZ382), whose lapsed LEI still says “ACTIVE” while the Companies House record sitting next to it in the search results says dissolved;
- [Persia International Bank](https://opencheck.world/?lei=213800HTIY6D1KMBG757), which meets the sanctions 50% ownership test through Bank Mellat but isn’t itself designated;
- a request to run live lookups on 200 Laundromat companies

I ran each case on Claude Opus 5.5 and Claude Haiku 5.5, with and without the skill. Each run was a clean session that received only the server’s handshake. Another Claude agent then graded each pair of answers without knowing which one had the skill, and checked every claim against the recorded data.

![](/blog/media/agents-are-a-new-kind-of-reader-ownership-data-has-to-be-written-for-them-too/03.webp)

## What the evals showed

**The skill helps most where the answer can’t speak for itself -** Asked to explore 200 companies, Haiku without the skill wrote a 40-minute script of live lookups. With it, both models pointed to the Laundromat release and its ready-made Neo4j files, and said the data is non-commercial.

**It cuts the errors that sound plausible -** Without the skill, answers called a counter-sanctions listing “a sanctioned counterparty”, added two banks’ stakes into “close to all of the company”, and listed a GLEIF reporting exception as an owner.

**But the baseline is already high, and that’s the real finding -** NVIDIA reported their query skill taking Claude from 47.7% to 100%. Here, Claude without the skill already passes more than 90% of checks. That’s because OpenCheck’s answers already explain themselves. Agents read the *“not a clean screen”* line and the *“possible name match only”* label, and they mostly get it right.

![](/blog/media/agents-are-a-new-kind-of-reader-ownership-data-has-to-be-written-for-them-too/04.webp)

**Evals catch your own mistakes too -** My first run looked like a tie. That was because the agents had the repository’s own developer notes in their context, which spell out every rule the skill teaches. The “without skill” run had already read the answers. I threw it away and ran each case again in a clean session. The evals also found three faults in the skill. Haiku took “never add figures across sources” too literally and wouldn’t say three state bodies were one holding. Opus filled a percentage column from its own memory. And Haiku skipped the skill altogether on four of the twelve questions. I fixed all three and checked them again.

## Write for the agent where it reads

The lesson I’m taking to London is simple. An agent understands an answer best when the answer itself says how to read it. A skill and a handshake help, but they can only point at what the response already says.

That is also the argument for open standards. [BODS](https://standard.openownership.org/) puts the evidence on every statement, so any agent, using any tool, can check what a claim rests on. As I wrote in [*Open all the way down*](/blog/open-all-the-way-down-why-opencheck-is-built-on-open-source-open-data-and-open-standards), it only works if the code, the data and the standard are all open. The skill and its evals are open too.

Connect [OpenCheck’s MCP server](https://opencheck.world/api#mcp) to your agent, [install the skill](https://github.com/StephenAbbott/opencheck/tree/main/skills/opencheck-due-diligence) and ask it who owns a company you know. Then check whether it told you what it couldn’t see.

*For four years, I was the product owner of the* [*Beneficial Ownership Data Standard*](https://standard.openownership.org/)*, the world’s leading open standard for beneficial ownership information and continue to advocate to improve beneficial ownership data quality and standardisation. To learn more or if you need technical guidance or consultancy support, contact me via* [*beneficialownership.co.uk*](https://www.beneficialownership.co.uk/)*.*
