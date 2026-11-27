"""The public OpenAPI document: what ``/docs``, ``/redoc`` and ``/openapi.json`` say.

FastAPI builds the schema from the routes, and until now it said what the code
said: a summary made from the function name, a description made from the
docstring (written for developers, with phase numbers and module names), and
no examples. This module rewrites that schema for the people who read it:

* every public operation gets a tag, a summary and a description written for
  a caller (``OPERATIONS``), and the rate tier it is on, read from the limiter;
* the website's own pages (blog, entity pages, share cards, sitemaps) are left
  out — they are HTML for browsers, not API;
* a parameter the route does not describe gets one from ``PARAMETERS``;
* streams declare ``text/event-stream`` and downloads their file types, so a
  generated client does not expect JSON;
* errors share one schema (``{"detail": …}``) and limited routes declare 429;
* the core answers carry a recorded example (``openapi_examples.json``).

``tests/test_openapi_docs.py`` fails when a public operation is not in
``OPERATIONS``, an entry names no route, a description mentions a phase, a
parameter has no description, or an example does not validate against its
response model.
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

_EXAMPLES_FILE = Path(__file__).with_name("openapi_examples.json")

#: Paths served for browsers and crawlers, not callers. Matched as prefixes.
HIDDEN_PREFIXES: tuple[str, ...] = (
    "/blog",
    "/browse",
    "/entity/",
    "/sitemap",
    "/sitemaps/",
    "/og/",
    "/share/",
    "/robots.txt",
    "/llms.txt",
    "/{key}.txt",
    "/.well-known/",
)

#: Tag order and wording, as ``/docs`` lists them.
TAGS: list[dict[str, str]] = [
    {"name": "Lookup", "description": "Check one company: who owns and controls it, from every source, with risk signals and a verdict."},
    {"name": "Search", "description": "Find a company or a person by name, or an LEI from a registration number."},
    {"name": "Export", "description": "Download a check as BODS v0.4, CSV, RDF, a graph format, or a PDF or Markdown report."},
    {"name": "Company details", "description": "One part of a company's record on its own: securities, subsidiaries, history, what its country publishes."},
    {"name": "People", "description": "Screen a person, and list a person's positions by a register's own key."},
    {"name": "Network", "description": "Grow an ownership network one layer at a time, as FullCheck does."},
    {"name": "Narrative", "description": "The AI-written summary of a check, and an analyst's review of its claims."},
    {"name": "Saved reports", "description": "Keep a check as a link and a SHA-256 hash, and read it back unchanged."},
    {"name": "Watchlist", "description": "Watch companies and be told when GLEIF, OpenSanctions or a register records a change."},
    {"name": "Daily changes", "description": "Every day's changes to GLEIF and OpenSanctions records, graded, for every LEI."},
    {"name": "Catalogue", "description": "The sources OpenCheck reads, their licences and their health."},
    {"name": "Service", "description": "Whether the server is up, and aggregate counters about its use. No company or person ever appears in them."},
]

# Shared phrases.
_LOOKUP_SEE = (
    "The answer is the same as `/lookup`'s; see the sample answer and the data "
    "dictionary linked from https://opencheck.world/api."
)
_STREAM_NOTE = (
    "Server-sent events (`text/event-stream`). Refuses automated clients "
    "(HTTP 403): scripts and agents should call the JSON route instead."
)

#: (method, path) → (tag, summary, description).
OPERATIONS: dict[tuple[str, str], tuple[str, str, str]] = {
    # Lookup
    ("get", "/lookup"): (
        "Lookup", "Check a company by LEI",
        "Runs the full check on one legal entity: GLEIF first, then every source that covers it, "
        "merged into one BODS v0.4 graph, assessed for risk and summed up in a verdict. "
        "A repeat of the same check within 15 minutes is answered from memory, free; "
        "`refresh=true` runs it again and spends the lookup budget. "
        "Read `degraded_sources` before anything else: an entry there means the screen is incomplete, not clean.",
    ),
    ("get", "/lookup-stream"): (
        "Lookup", "Check a company by LEI, streamed",
        "The `/lookup` pipeline as it runs, one event per step (`gleif_done`, `source_started`, "
        "`source_completed`, `risk_signals`, `done` and others). " + _STREAM_NOTE,
    ),
    ("get", "/lookup-register"): (
        "Lookup", "Check a company by register number",
        "For a company with no LEI (most UK LLPs, most small companies). Give the register's scheme "
        "and number (for example `GB-COH` and `OC346224`): the register that owns the scheme is read, "
        "and its record is screened and assessed as `/lookup` does. The company's own name is screened "
        "by name, because there is no LEI to match on. `/search` lists the schemes a candidate can use.",
    ),
    ("get", "/lookup-register-stream"): (
        "Lookup", "Check a company by register number, streamed",
        "The `/lookup-register` pipeline as events, with `register_done` in place of `gleif_done`. " + _STREAM_NOTE,
    ),
    ("get", "/lookup-source"): (
        "Lookup", "Re-run one source",
        "Asks one source again for one LEI, as the Try again button on a source card does, and "
        "clears the held check for that LEI so the next `/lookup` includes the new answer.",
    ),
    ("get", "/deepen"): (
        "Lookup", "Read one source's full record",
        "Fetches the full record behind one search or lookup result and maps it to BODS v0.4, with "
        "that source's own risk signals, licence and data-quality notes.",
    ),
    ("get", "/report"): (
        "Lookup", "Search and check in one call",
        "Searches by name, reconciles the results across sources, reads the top few in full and "
        "assesses risk. Meant for exploration: for due diligence on a known company, find its LEI "
        "with `/search` and call `/lookup`.",
    ),
    # Search
    ("get", "/search"): (
        "Search", "Search by name",
        "Searches every source that answers name searches, at once, and returns the results ranked "
        "across sources: exact names first, then live companies, then those with an LEI. Each result "
        "carries the identifiers the source itself publishes. The filters are passed to the sources "
        "that support them; `filters` in the answer says which did.",
    ),
    ("get", "/stream"): (
        "Search", "Search by name, streamed",
        "`/search` as server-sent events: a `filters` event, then `source_started`, `hit`, "
        "`source_error` and `source_completed` for each source as it answers. Open to scripts.",
    ),
    ("get", "/resolve-national-id"): (
        "Search", "Find the LEI for a registration number",
        "Asks GLEIF which LEIs are filed with a company-registration number. Give a country and "
        "OpenCheck picks the register (for the UK, from the number's prefix: Scotland and Northern "
        "Ireland are separate registers), or give GLEIF's registration-authority code yourself.",
    ),
    ("get", "/elf-forms"): (
        "Search", "Legal forms of a jurisdiction",
        "The ISO 20275 legal forms (ELF codes) of one jurisdiction, for the `legal_form` filter of `/search`.",
    ),
    ("get", "/leads"): (
        "Search", "Leads for a retired LEI",
        "For an LEI that GLEIF retired without naming a successor: records elsewhere that might be "
        "the same company now. They are leads to check, never a successor.",
    ),
    # Export
    ("get", "/export"): (
        "Export", "Download a check",
        "The check's BODS v0.4 graph in the format asked for: `zip` (the default: BODS with its "
        "licences), `json`, `jsonl`, `xml`, `csv` (flat tables), `xlsx`, `rdf` (TriG), `cypher` "
        "(Neo4j), `gql` (BigQuery property graph), `senzing`, `ftm` (FollowTheMoney) or `amlai` "
        "(Google AML AI tables). Each download names the licences of the sources in it. Give an LEI, "
        "or a `saved_report_id` to export a saved report exactly as it was.",
    ),
    ("post", "/export-network"): (
        "Export", "Download a grown network",
        "Formats a network you post as BODS statements (the FullCheck canvas after growing it layer by "
        "layer) in any `/export` format, with the licences of the sources that contributed.",
    ),
    ("post", "/export/pdf"): (
        "Export", "Download the report as PDF",
        "The due-diligence report for an LEI as an accessible PDF (tagged, PDF/UA-1): verdict, what each "
        "source found, risk signals, the ownership diagram, what can be known in each jurisdiction and "
        "the licences. Pass a `saved_report_id` to render a saved report as it was saved.",
    ),
    ("post", "/export/markdown"): (
        "Export", "Download the report as Markdown",
        "The same report as `/export/pdf`, as Markdown.",
    ),
    ("post", "/export/discrepancy-note"): (
        "Export", "Draft a discrepancy note for a register",
        "When two registers disagree about a company (one records it dissolved, GLEIF records it "
        "active), a one-page note to each register that holds a disputed value, quoting both records "
        "with their dates and licences. Markdown by default, or PDF. 404 when the check found no "
        "disagreement to report.",
    ),
    # Company details
    ("get", "/securities"): (
        "Company details", "Securities issued",
        "The ISINs GLEIF links to an LEI, from GLEIF's daily ISIN file, in ISIN order, with any "
        "security on a sanctions list first. Paged.",
    ),
    ("get", "/subsidiaries"): (
        "Company details", "Subsidiaries consolidated (GLEIF)",
        "The companies GLEIF records as consolidated by this one, directly and ultimately, with dates "
        "from GLEIF's relationship records. GLEIF Level 2 is accounting consolidation, not ownership: "
        "a consolidating parent need not be a shareholder, and no percentage is published.",
    ),
    ("get", "/subsidiaries/declared"): (
        "Company details", "Subsidiaries listed elsewhere",
        "The subsidiary lists OpenCheck holds from sources other than GLEIF (the OECD-UNSD register of "
        "multinational groups, EITI, Global Energy Monitor, the Estonian register), each kept apart. "
        "They measure different things and are never merged.",
    ),
    ("get", "/history"): (
        "Company details", "Changes over time",
        "A timeline of notable changes to the company's records (names, status, address, owners, "
        "officers), merged across the registers that keep a change log, each dated and ranked by how "
        "the date is known. `registers_unavailable` names any register that did not answer.",
    ),
    ("get", "/knowability"): (
        "Company details", "What a country publishes about owners",
        "For each jurisdiction code: what its registers publish about beneficial owners, who may see "
        "it, and which OpenCheck sources read it, as one dated sentence. Spends no lookup budget, so "
        "call it before a lookup to know what a thin answer means.",
    ),
    ("get", "/nz-associations"): (
        "Company details", "New Zealand: directors' and shareholders' other companies",
        "For a New Zealand company, the other companies its directors and shareholders are named on, "
        "matched by name and graded by address.",
    ),
    # People
    ("get", "/person-check"): (
        "People", "Screen a person",
        "Screens one name against every source that holds people: sanctions, politically exposed "
        "persons, offshore leaks and the registers' officer lists. A name match is evidence to weigh, "
        "not an identity: a birth year makes matches more certain.",
    ),
    ("get", "/person-positions"): (
        "People", "A politically exposed person's positions",
        "The positions recorded on one OpenSanctions or EveryPolitician person record.",
    ),
    ("get", "/person-appointments"): (
        "People", "A person's positions, by register key",
        "Every company a person is named on, found by the register's own key for that person "
        "(a Companies House officer id, or the Estonian register's person key). Never a name search.",
    ),
    ("get", "/nz-associations/person"): (
        "People", "New Zealand: one person's other companies",
        "One director's or shareholder's other New Zealand companies, for a name that is a role "
        "holder of the company given. Matches are graded by address.",
    ),
    ("get", "/legal-person-relations"): (
        "People", "Estonia: a company's roles in other companies",
        "Every company a legal person has held a role in on the Estonian e-Business Register, "
        "deleted companies included. Legal persons only.",
    ),
    # Network
    ("get", "/expand"): (
        "Network", "Grow one node",
        "Checks one company in a network and returns its owners, so the network grows by one layer "
        "at that node.",
    ),
    ("post", "/expand-layer"): (
        "Network", "Grow the whole frontier",
        "Checks every node at the edge of a network at once and returns the next layer. A node the "
        "lookup budget could not check now is returned as `deferred`; one that failed is `failed`, "
        "with the reason.",
    ),
    ("get", "/expand-schemes"): (
        "Network", "Register schemes a network can grow on",
        "The identifier schemes a node can be grown on without an LEI, and the register that answers each.",
    ),
    # Narrative
    ("get", "/narrative"): (
        "Narrative", "AI summary of a check",
        "A written summary of a check by a language model, every claim tied to the evidence it rests "
        "on. Available where the server is configured for it; 503 otherwise.",
    ),
    ("get", "/narrative/dispositions"): (
        "Narrative", "Read an analyst's review",
        "The stored review of one summary's claims (accepted, disputed, noted), or 404.",
    ),
    ("put", "/narrative/dispositions"): (
        "Narrative", "Save an analyst's review",
        "Stores an analyst's review of one summary's claims.",
    ),
    # Batch (lookup family)
    ("get", "/batch-stream"): (
        "Lookup", "Check a list of LEIs, streamed",
        "Checks up to 20 LEIs, one `row_done` or `row_failed` event per LEI. " + _STREAM_NOTE,
    ),
    ("get", "/batch-export"): (
        "Export", "Check a list of LEIs and download the lot",
        "Checks up to 20 LEIs and returns one zip: the merged BODS, `rows.csv`, a manifest and the licences.",
    ),
    # Saved reports
    ("post", "/saved-reports"): (
        "Saved reports", "Save a check",
        "Keeps the check the server holds for an LEI (named by its `run_completed_at`) as a report "
        "with a link and a SHA-256 hash. Returns the report id and, once only, the token that can "
        "extend or delete it. 409 when the check is no longer held (after 15 minutes): run it again.",
    ),
    ("get", "/saved-reports/{report_id}"): (
        "Saved reports", "Read a saved report",
        "The report's metadata and the check as it was saved, after the expiry and integrity checks.",
    ),
    ("get", "/saved-reports/{report_id}.json"): (
        "Saved reports", "Download a saved report's bytes",
        "The exact bytes that were hashed: `shasum -a 256` on this download equals `content_hash`.",
    ),
    ("post", "/saved-reports/{report_id}/extend"): (
        "Saved reports", "Keep a saved report longer",
        "Keeps the report for another retention period, counted from now. Needs the manage token.",
    ),
    ("delete", "/saved-reports/{report_id}"): (
        "Saved reports", "Delete a saved report",
        "Deletes the report. Needs the manage token.",
    ),
    # Watchlist
    ("post", "/watch/items"): (
        "Watchlist", "Watch a company",
        "Adds an LEI to a watchlist. With no token a new list is created and its token returned; this "
        "is the only time it is shown. A watched company is checked again only when GLEIF, OpenSanctions "
        "or its register records a material change, never on a clock.",
    ),
    ("get", "/watch/{token}"): (
        "Watchlist", "Read a watchlist",
        "The watched companies and the log of changes found.",
    ),
    ("get", "/watch/{token}.atom"): (
        "Watchlist", "A watchlist as a feed",
        "The watchlist's log as an Atom feed, for a feed reader.",
    ),
    ("delete", "/watch/{token}/items/{lei}"): (
        "Watchlist", "Stop watching a company", "Removes one LEI from the list.",
    ),
    ("patch", "/watch/{token}/items/{lei}"): (
        "Watchlist", "Record what you expect of a company",
        "Records what the watcher expected (jurisdiction, ultimate parent, why it is watched, the "
        "saved report used at onboarding). A field sent as `null` is cleared; one left out is kept.",
    ),
    ("post", "/watch/{token}/recheck"): (
        "Watchlist", "Check a watched company now",
        "Runs the check again now, bypassing the held answer, and logs any difference.",
    ),
    ("post", "/watch/{token}/entries/{entry_id}/decisions"): (
        "Watchlist", "Record a decision on a change",
        "Records what was decided about one logged change (reviewed, re-checked, escalated), with an "
        "optional note and saved report. Decisions are appended, never overwritten.",
    ),
    ("post", "/watch/{token}/entries/{entry_id}/decisions/recheck"): (
        "Watchlist", "Record a decision with a fresh check",
        "The same decision, with a fresh check of the company saved as the evidence.",
    ),
    ("get", "/watch/{token}/record.md"): (
        "Watchlist", "Download the review record",
        "Every decision on the list as Markdown, with the change it answers and the saved report's id and hash.",
    ),
    # Daily changes
    ("get", "/daily-changes"): (
        "Daily changes", "Days held",
        "The days held, newest first, with each day's counts by band and source.",
    ),
    ("get", "/daily-changes/{day}.json"): (
        "Daily changes", "One day's changes",
        "Every change GLEIF's and OpenSanctions' daily files recorded on one UTC day for any LEI, "
        "graded into bands (ownership and sanctions first). A day with nothing on it has no rows.",
    ),
    ("get", "/changes/{name}"): (
        "Daily changes", "Download a day's changes",
        "`<day>.jsonl.gz`: every LEI GLEIF changed that day as BODS v0.4 JSON Lines, built from "
        "OpenCheck's Golden Copy mirror. `<day>.csv`: the day's list, both sources. "
        "`latest.jsonl.gz` and `latest.csv` redirect to the newest day held.",
    ),
    # Catalogue
    ("get", "/sources"): (
        "Catalogue", "List the sources",
        "Every source OpenCheck reads: what it covers, its licence and attribution, whether it needs "
        "a key, and whether it is live on this server.",
    ),
    ("get", "/license-matrix"): (
        "Catalogue", "Licences, and what they allow together",
        "Every source's licence terms (commercial use, attribution, share-alike). With `sources`, "
        "also what the combination allows, which is what an export containing them may be used for.",
    ),
    ("get", "/source-health"): (
        "Catalogue", "Source health",
        "The last weekly test of every source: healthy, degraded, failed or not tested, with reasons "
        "and the last eight results. Nothing is tested when you ask.",
    ),
    # Service
    ("get", "/health"): ("Service", "Is the server up", "Answers when the server is running, with its version."),
    ("get", "/memstats"): ("Service", "Memory and traffic", "Memory use and request counters since the last deploy."),
    ("get", "/signalstats"): (
        "Service", "Signal and pipeline counters",
        "Which sources raised which risk signals, which checks did not run, how busy the check queue "
        "is and how the GLEIF budget is spent, since the last deploy.",
    ),
    ("get", "/consistencystats"): (
        "Service", "How often sources agree",
        "How often two sources describing the same company agree, per field and source pair, since the last deploy.",
    ),
    ("get", "/mirror"): ("Service", "GLEIF mirror", "The state of the local copy of GLEIF's Golden Copy and how it is used."),
    ("get", "/pscgraph"): ("Service", "UK PSC graph", "The state of the UK persons-with-significant-control graph file."),
    ("get", "/watchstats"): ("Service", "Watchlist counters", "Aggregate watchlist counters: deltas read, re-runs, entries, backups."),
}

#: Descriptions for parameters a route leaves undescribed, by name.
PARAMETERS: dict[str, str] = {
    "q": "The name to search for.",
    "kind": "`entity` (companies and other legal entities) or `person`.",
    "deepen_top": "How many results per source to read in full (0 to 10). The default suits most checks.",
    "token": "The watchlist's token, returned once when the list was created.",
    "lei": "The Legal Entity Identifier, 20 characters.",
    "entry_id": "The id of one entry in the watchlist's log.",
    "report_id": "The saved report's id.",
    "X-OpenCheck-Manage-Token": "The manage token returned once when the report was saved.",
    "day": "The UTC day, `YYYY-MM-DD`.",
    "name": "`<day>.jsonl.gz`, `<day>.csv`, `latest.jsonl.gz` or `latest.csv`.",
}

_EXPORT_TYPES: dict[str, str] = {
    "application/zip": "`zip` (the default), `csv`, `gql` and `amlai`.",
    "application/json": "`json`: a BODS v0.4 array.",
    "application/x-ndjson": "`jsonl`, `senzing` and `ftm`: one record per line.",
    "application/xml": "`xml`: BODS XML.",
    "application/trig": "`rdf`: BODS RDF as TriG.",
    "text/plain": "`cypher`: Neo4j Cypher statements.",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "`xlsx`: one workbook.",
}

#: Operations whose success answer is not JSON.
CONTENT_TYPES: dict[tuple[str, str], dict[str, str]] = {
    ("get", "/lookup-stream"): {"text/event-stream": "One event per pipeline step."},
    ("get", "/lookup-register-stream"): {"text/event-stream": "One event per pipeline step."},
    ("get", "/stream"): {"text/event-stream": "One event per source and result."},
    ("get", "/batch-stream"): {"text/event-stream": "One event per LEI."},
    ("get", "/export"): _EXPORT_TYPES,
    ("post", "/export-network"): _EXPORT_TYPES,
    ("post", "/export/pdf"): {"application/pdf": "The report."},
    ("post", "/export/markdown"): {"text/markdown": "The report."},
    ("post", "/export/discrepancy-note"): {"text/markdown": "The note.", "application/pdf": "The note, as PDF."},
    ("get", "/batch-export"): {"application/zip": "Merged BODS, rows.csv, manifest and licences."},
    ("get", "/watch/{token}.atom"): {"application/atom+xml": "The feed."},
    ("get", "/watch/{token}/record.md"): {"text/markdown": "The review record."},
    ("get", "/saved-reports/{report_id}.json"): {"application/json": "The canonical payload bytes."},
    ("get", "/changes/{name}"): {
        "application/gzip": "BODS v0.4 JSON Lines, gzipped.",
        "text/csv": "The day's list.",
    },
}

#: Status codes worth declaring beyond the generated 200 and 422.
_ERROR_WORDS: dict[str, str] = {
    "400": "The identifier or a parameter is malformed.",
    "403": "This stream refuses automated clients; call the JSON route.",
    "404": "Nothing to answer: an unknown source, record or report.",
    "409": "The check this refers to is no longer held: run it again.",
    "429": "A rate limit or the lookup budget is spent. Wait `Retry-After` seconds, then retry.",
    "502": "A source this answer depends on did not answer.",
    "503": "OpenCheck is busy (retry after `Retry-After` seconds), or this feature is not set up here.",
}

_STREAMS = {k for k, v in CONTENT_TYPES.items() if "text/event-stream" in v}
_HAS_404 = {
    ("get", "/deepen"), ("get", "/lookup-source"), ("get", "/narrative/dispositions"),
    ("get", "/saved-reports/{report_id}"), ("get", "/saved-reports/{report_id}.json"),
    ("post", "/saved-reports/{report_id}/extend"), ("delete", "/saved-reports/{report_id}"),
    ("get", "/watch/{token}"), ("get", "/watch/{token}.atom"), ("post", "/export/discrepancy-note"),
    ("get", "/lookup-register"), ("get", "/daily-changes/{day}.json"), ("get", "/changes/{name}"),
}
_HAS_409 = {("post", "/saved-reports")}

#: A development note in a docstring: "(Phase 290)", "(from Phase 187 …)", a leading "Phase 218: ".
_PHASE = re.compile(r"\s*\([^()]*\bPhases? \d+[^()]*\)|^Phases? \d+[:.]\s*")


def _scrub(node: Any) -> Any:
    """Every ``description`` under ``node`` without its phase notes."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "description" and isinstance(value, str):
                text = _PHASE.sub("", value)
                node[key] = text[:1].upper() + text[1:]
            else:
                _scrub(value)
    elif isinstance(node, list):
        for value in node:
            _scrub(value)
    return node

_RATE_WORDS = {
    "default": "Rate limit: default tier, 60 requests a minute per address.",
    "lookup": "Rate limit: lookup tier, 10 requests a minute per address. A fresh check also spends the shared lookup budget (10 a minute); a repeat within 15 minutes is free.",
    "heavy": "Rate limit: heavy tier, 3 requests a minute per address.",
}


def is_hidden(path: str) -> bool:
    return path.startswith(HIDDEN_PREFIXES)


def route_tiers(app: FastAPI) -> dict[tuple[str, str], str]:
    """(method, path) → the rate tier the limiter applies, for limited routes."""
    from .ratelimit import limiter

    out: dict[tuple[str, str], str] = {}
    for route in app.routes:
        endpoint = getattr(route, "endpoint", None)
        path = getattr(route, "path", None)
        methods = getattr(route, "methods", None) or ()
        if endpoint is None or path is None:
            continue
        key = f"{getattr(endpoint, '__module__', '')}.{getattr(endpoint, '__name__', '')}"
        groups = limiter._route_limits.get(key) or limiter._dynamic_route_limits.get(key) or []
        for group in groups:
            provider = getattr(group, "_LimitGroup__limit_provider", None)
            name = getattr(provider, "__name__", "")
            if name.endswith("_tier"):
                for method in methods:
                    out[(method.lower(), path)] = name.removesuffix("_tier")
    return out


def load_examples() -> dict[str, Any]:
    return json.loads(_EXAMPLES_FILE.read_text(encoding="utf-8"))


def _example_key(method: str, path: str) -> str:
    return f"{method.upper()} {path}"


def build_openapi(app: FastAPI) -> dict[str, Any]:
    """The public schema, cached on the app as FastAPI's own is."""
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=(
            f"{app.description}\n\nStart with `/search` or `/resolve-national-id` to find an LEI, "
            "then `/lookup`. The guide, a sample answer, the limits and the errors are at "
            "https://opencheck.world/api."
        ),
        routes=[r for r in app.routes if not is_hidden(getattr(r, "path", ""))],
        tags=TAGS,
        servers=[{"url": "https://api.opencheck.world"}],
    )
    tiers = route_tiers(app)
    examples = load_examples()

    components = schema.setdefault("components", {}).setdefault("schemas", {})
    components["Error"] = {
        "title": "Error",
        "type": "object",
        "required": ["detail"],
        "properties": {"detail": {"type": "string", "description": "What went wrong, in a sentence."}},
        "example": {"detail": "'BAD' is not a valid LEI. ISO 17442 LEIs are 20-character alphanumeric strings (e.g. 213800LH1BZH3DI6G760)."},
    }

    paths: dict[str, Any] = {}
    for path, ops in schema.get("paths", {}).items():
        if is_hidden(path):
            continue
        kept: dict[str, Any] = {}
        for method, op in ops.items():
            if method == "head":
                continue
            key = (method, path)
            entry = OPERATIONS.get(key)
            if entry is not None:
                tag, summary, description = entry
                op["tags"] = [tag]
                op["summary"] = summary
                tier = tiers.get(key)
                op["description"] = description + (f"\n\n{_RATE_WORDS[tier]}" if tier else "")
                if tier:
                    op["x-rate-limit-tier"] = tier
            for param in op.get("parameters", []):
                desc = param.get("description") or PARAMETERS.get(param["name"], "")
                param["description"] = desc
                _scrub(param)
            _set_responses(op, key, tiers.get(key), examples.get(_example_key(method, path)))
            kept[method] = op
        if kept:
            paths[path] = kept
    schema["paths"] = paths
    _scrub(schema["components"])
    app.openapi_schema = schema
    return schema


def _set_responses(op: dict, key: tuple[str, str], tier: str | None, example: Any) -> None:
    responses = op.setdefault("responses", {})
    ok_code = next((c for c in ("200", "201") if c in responses), None)
    types = CONTENT_TYPES.get(key)
    if ok_code and types:
        responses[ok_code]["content"] = {
            ctype: ({"schema": {"type": "string", "format": "binary"}} if ctype not in {"application/json"} else {"schema": {}})
            | {"x-description": words}
            for ctype, words in types.items()
        }
        responses[ok_code]["description"] = " ".join(types.values()) if len(types) == 1 else "The download, in the format asked for."
    if ok_code and example is not None:
        content = responses[ok_code].setdefault("content", {}).setdefault("application/json", {})
        content["example"] = copy.deepcopy(example)

    def err(code: str) -> None:
        responses.setdefault(code, {
            "description": _ERROR_WORDS[code],
            "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}},
        })

    if any(p.get("in") in {"query", "path"} for p in op.get("parameters", [])) or "requestBody" in op:
        err("400")
    if key in _STREAMS:
        err("403")
    if key in _HAS_404:
        err("404")
    if key in _HAS_409:
        err("409")
    if tier:
        err("429")
        responses["429"]["headers"] = {
            "Retry-After": {"description": "Seconds to wait before retrying.", "schema": {"type": "integer"}},
        }
    if tier in {"lookup", "heavy"}:
        err("503")
        if key[1].startswith(("/lookup", "/export", "/report", "/batch")):
            err("502")
