---
title: "The reader moved on overnight. Ownership data has to keep up"
subtitle: "Within a day of Anthropic shipping skills and tasks for MCP, OpenCheck’s server spoke both. Why speed matters when the way people and agents read data changes"
published: "2026-11-25T14:41:09.000Z"
medium_url: "https://medium.com/@stephenabbottpugh/the-reader-moved-on-overnight-ownership-data-has-to-keep-up-a65b9d0203bd"
medium_id: "a65b9d0203bd"
topics:
  - "Open Data"
  - "Beneficial Ownership"
  - "MCP"
  - "Beneficial Ownership Data"
image: "/blog/media/the-reader-moved-on-overnight-ownership-data-has-to-keep-up/01.webp"
image_alt: ""
---

Three weeks ago I wrote that [agents are a new kind of reader](https://opencheck.world/blog/agents-are-a-new-kind-of-reader-ownership-data-has-to-be-written-for-them-too), and that ownership data has to be written for them too. This post follows on from that one, because the reader changed again this week.

On 24 November 2026, Anthropic published [Skills over MCP, tasks and more](https://claude.com/resources/articles/skills-over-mcp-tasks-and-more). It announced two things that matter to anyone serving data to AI agents. A server can now publish skills, the instructions an agent loads when a question calls for them. And a server can hand back a task for slow work: the agent gets an answer at once, carries on, and checks back for the result.

By the afternoon of 25 November, [OpenCheck’s MCP server](https://opencheck.world/api#mcp) supported both. The work went in as three changes, each merged, deployed and tested against production the same day. What follows covers what changed, and why I didn’t want to wait.

## Why not wait?

The tooling around data moves much faster than the data itself. A beneficial ownership register changes its rules over years, and a [data standard](https://standard.openownership.org/) might be revised every year or two. The clients that agents run in change every few weeks. Anthropic turned skills and tasks on for Claude Pro, Max and Ultra users on the day it announced them.

So the reader had already changed by the time I read the article. Someone asking Claude “who owns this company?” was already in a client that could use skills and tasks. A server that ignored them gave that person a worse experience than the client could offer. And for long checks it gave them no answer at all.

## For agents, slow means lost

![](/blog/media/the-reader-moved-on-overnight-ownership-data-has-to-keep-up/02.webp)

Here is the problem tasks solve. Claude Code waits up to 60 seconds for the first byte of an answer, and drops a call after five minutes with no progress. A full OpenCheck lookup of a large company reads more than a dozen sources: registers, GLEIF, sanctions lists, leaks databases. It can take tens of seconds, and longer when the server is busy and the check has to queue.

When a call is dropped, the server often finishes the check anyway. The person sees an error. The agent may try again, spending more of the shared lookup budget, or give up and guess.

Now, for a client that supports tasks, a slow call gets an answer within three seconds. That answer is a task that says where the check stands, using the lookup’s own progress: *Checking sources: 10 of 13 answered.*

The agent checks back every five seconds and gets the full result when the check is done. Checking back runs nothing again and spends nothing from the lookup budget. A repeat of a recent check skips all of this and is answered straight away. When I tested it on Shell in production, the task came back after 3.1 seconds, moved to “12 of 13 answered”, then completed with the full report.

Eight tools work this way: the [ones that run a full check](https://opencheck.world/api#mcp) (lookup, register lookup, owners, the ownership map, BODS export, batch, saved reports and the discrepancy note). The other seven answer in well under a second and never need a task. A client that doesn’t support tasks sees no change.

## For people, a quick update means current instructions

![](/blog/media/the-reader-moved-on-overnight-ownership-data-has-to-keep-up/03.webp)

The skills matter for a different kind of speed.

In the [last post](https://opencheck.world/blog/agents-are-a-new-kind-of-reader-ownership-data-has-to-be-written-for-them-too), I described an [OpenCheck agent skill](https://github.com/StephenAbbott/opencheck/tree/main/skills/opencheck-due-diligence) with twelve evals, which showed that it cut wrong claims about the data. But it was a folder you downloaded and installed yourself. Each installed copy went out of date the day I improved the skill, and nobody using it would know.

The server now serves three skills itself:

- **opencheck-due-diligence**: from a company name or number to a saved report, with each tool in the right order.
- **reading-opencheck-results**: how to report a result honestly. That covers sources that didn’t answer, risk versus context, possible name matches, ownership figures that are never added across sources, and licences.
- **opencheck-ownership-questions**: which tool answers which ownership question, and the words to use. For example, a GLEIF parent consolidates a company’s accounts. That is not the same as owning it.

A client loads a skill only when a question needs it, so the agent’s context isn’t filled with rules it won’t use. Each skill updates with the server. Every file carries a SHA-256 checksum, so a client can verify it read exactly what OpenCheck published. That fits OpenCheck’s approach to [provenance](https://opencheck.world/blog/where-did-this-come-from-and-is-it-still-true): you should be able to check what an answer rests on, including the instructions for reading it.

The short guide an agent receives when it connects (the “handshake” from the last post) keeps its full text, because most clients can’t read skills yet. It now also names the three skills. The [API page](https://opencheck.world/api#mcp) now explains all of this to people, too.

## Speed came from earlier work

None of this was rushed. Moving within a day was possible because of decisions made weeks and months before:

- The migration had been tried in August. Both features need the newest version of the MCP protocol, and the official Python SDK for it. I tested a move to that SDK in August and decided to wait for its first patch releases. By November, it was at version 2.3, the wait was over, and the trial showed where the work was
- The guide was already shaped like a skill. The handshake instructions already told agents what order to call tools in and how to read the answers. Turning that into skills meant giving the guidance more room, not working it out from scratch
- Long checks already ran detached. OpenCheck already ran a lookup separately from the request that started it, and kept the result for anyone else asking about the same company. A task points at that existing run, so it was a thin layer to add
- Tests pin what the agent is told. A test fails the build if a tool is missing from the guide, if the guide grows too long, or if a skill names a tool that doesn’t exist. Now the API page is pinned to the skills and tasks too. That made it safe to change several things in one day
- Each change was checked on production. After each deploy, the server was tested on both protocol versions with real lookups. Older clients still connect as they did, and newer ones get the new features

## Knowing what to skip

Moving fast also means deciding what not to build. The article announced more than skills and tasks:

- **Elicitation**, where a server pauses to ask the user something. OpenCheck has nothing to ask: no tool is destructive, and saving a report is already marked as such
- **File uploads**, where an agent hands a spreadsheet to the server. “Screen this list” is tempting, but uploaded lists often hold personal data, and the feature isn’t live yet
- **MCP Apps**, which would show OpenCheck’s ownership graph inside Claude. It stays on the list for later

There are caveats on what did ship. [Skills](https://claude.com/resources/articles/skills-over-mcp-tasks-and-more) are a public beta and work in Claude and Claude Code only for now. Tasks are held in memory on OpenCheck’s single server, so a deploy loses any task that is still running.

## Keep the bridge current

Ownership data will keep changing slowly, and it should: a register’s rules are law, and a standard needs time to settle. Agents and the clients they run in will keep changing quickly. Something has to keep the two connected, and if it falls behind, the agent fills the gap with guesses.

So when the way people read data changes, the people serving that data should change with it, as soon as they safely can. That’s easiest when the [code](https://github.com/StephenAbbott/opencheck), the [data](https://opencheck.world/sources) and the [standard](https://standard.openownership.org/) are [open all the way down](https://opencheck.world/blog/open-all-the-way-down-why-opencheck-is-built-on-open-source-open-data-and-open-standards), and when the groundwork is done before the announcement.

Connect OpenCheck’s MCP server, https://api.opencheck.world/mcp, to Claude and ask who owns a large company you know. Then see whether it tells you how far through its checks it got, and what it couldn't see.

*For four years, I was the product owner of the* [*Beneficial Ownership Data Standard*](https://standard.openownership.org/)*, the world’s leading open standard for beneficial ownership information and continue to advocate to improve beneficial ownership data quality and standardisation. To learn more or if you need technical guidance or consultancy support, contact me via* [*beneficialownership.co.uk*](https://www.beneficialownership.co.uk/)*.*
