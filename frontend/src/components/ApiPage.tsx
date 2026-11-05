/**
 * The /api page — the read-only REST surface, documented.
 *
 * Lifted out of `App.tsx` in Phase 168, unchanged. It had no business being
 * there: it is a static document with no state of its own, mounted by one line
 * of App and touched by nobody else.
 */

import { useState } from "react";

import { BASE_URL, EXPORT_FORMATS } from "../lib/api";
import { MCP_STEPS, type McpTool } from "../lib/mcpTools";
import { BtsCard } from "./BtsCard";
import { Chip, SectionLabel } from "./ui";

// ---------------------------------------------------------------------
// API page — documents the read-only REST surface (api.opencheck.world).
// ---------------------------------------------------------------------

function CopyField({ value }: { value: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <div className="flex items-center gap-3 bg-oo-bg border border-oo-rule rounded-oo px-3 py-2">
      <code className="font-mono text-oo-small text-oo-ink flex-1 break-all">{value}</code>
      <button
        type="button"
        onClick={() => {
          navigator.clipboard?.writeText(value);
          setCopied(true);
          window.setTimeout(() => setCopied(false), 1200);
        }}
        className="text-[11px] font-medium text-oo-blue hover:text-oo-burst shrink-0"
      >
        {copied ? "Copied" : "Copy"}
      </button>
      {/* Always-mounted status region so the copy confirmation is announced */}
      <span role="status" className="sr-only">
        {copied ? "Copied to clipboard" : ""}
      </span>
    </div>
  );
}

function ApiEndpoint({
  path,
  children,
  params,
  method = "GET",
}: {
  path: string;
  children: React.ReactNode;
  params?: [string, string][];
  method?: "GET" | "POST" | "PUT";
}) {
  const methodClasses =
    method === "GET"
      ? "bg-emerald-50 text-emerald-700 border-emerald-200"
      : "bg-blue-50 text-blue-700 border-blue-200";
  return (
    <div className="py-3.5 border-t border-oo-rule first:border-t-0 first:pt-0">
      <div className="flex items-baseline gap-2 flex-wrap">
        <span className={`font-mono text-[10px] font-semibold rounded px-1.5 py-0.5 border ${methodClasses}`}>
          {method}
        </span>
        <code className="font-mono text-oo-small text-oo-ink break-all">{path}</code>
      </div>
      <p className="text-oo-small leading-[1.7] text-oo-muted mt-1.5">{children}</p>
      {params && params.length > 0 && (
        <dl className="mt-2 space-y-1">
          {params.map(([k, v]) => (
            <div key={k} className="flex gap-2 text-[12.5px] leading-[1.6]">
              <dt className="font-mono text-oo-blue shrink-0">{k}</dt>
              <dd className="min-w-0 text-oo-muted [overflow-wrap:anywhere]">{v}</dd>
            </div>
          ))}
        </dl>
      )}
    </div>
  );
}

export function ApiPage() {
  const base = BASE_URL || "https://api.opencheck.world";
  const mono = "font-mono text-oo-meta bg-oo-bg px-1 rounded";
  return (
    <section aria-labelledby="api-heading">
      <h2
        id="api-heading"
        className="font-head font-bold text-[clamp(1.35rem,3vw,1.8rem)] text-oo-ink mb-2 leading-tight"
      >
        API
      </h2>
      <p className="text-oo-body leading-[1.75] text-oo-muted mb-6 max-w-2xl">
        Everything OpenCheck shows on the website is available as a free,
        read-only API, with no key needed. Results are published in the{" "}
        <a
          href="https://standard.openownership.org/en/0.4.0/"
          target="_blank"
          rel="noreferrer"
          className="underline text-oo-blue hover:text-oo-burst"
        >
          Beneficial Ownership Data Standard (BODS) v0.4
        </a>
        , keyed on the Legal Entity Identifier (LEI). Every endpoint is a{" "}
        <code className={mono}>GET</code> that returns JSON, except{" "}
        <code className={mono}>/export</code> (a downloadable bundle) and the
        streaming endpoints (Server-Sent Events). AI agents can call the same
        checks as tools through the{" "}
        <strong className="text-oo-ink font-semibold">Model Context Protocol (MCP)</strong>{" "}
        server.
      </p>

      <nav aria-label="On this page" className="mb-6 flex flex-wrap gap-x-5 gap-y-1 text-oo-small">
        <a href="#rest" className="underline text-oo-blue hover:text-oo-burst">
          REST endpoints
        </a>
        <a href="#mcp" className="underline text-oo-blue hover:text-oo-burst">
          MCP server for AI agents
        </a>
      </nav>

      <div className="mb-8 max-w-2xl">
        <div className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-2">
          Base URL
        </div>
        <CopyField value={base} />
      </div>

      <div
        id="rest"
        className="grid gap-6 scroll-mt-6"
        style={{ gridTemplateColumns: "repeat(auto-fill, minmax(min(100%, 560px), 1fr))" }}
      >
        <BtsCard title="Lookup">
          <ApiEndpoint
            path="/lookup?lei=<LEI>"
            params={[
              ["lei", "20-character ISO 17442 LEI (required)."],
              ["deepen_top", "How many top hits to fully fetch + map + assess (default 5)."],
            ]}
          >
            <strong className="text-oo-ink font-semibold">Primary entry point.</strong>{" "}
            Resolves the company across every source and returns a unified BODS v0.4
            view — subject, related people and entities, ownership-or-control
            relationships, cross-source links and risk signals. The response also
            carries <code className={mono}>degraded_sources</code> (empty when every
            screen completed): derived checks — related-party sanctions/PEP
            screening, ICIJ offshore-leaks reconciliation — that did not fully run,
            each with <code className={mono}>source_id</code>, <code className={mono}>check</code>,{" "}
            <code className={mono}>affected_signals</code>, a counts-only{" "}
            <code className={mono}>detail</code> and a closed-vocabulary{" "}
            <code className={mono}>reason</code> (<code className={mono}>upstream_error</code>,{" "}
            <code className={mono}>timeout</code>, <code className={mono}>not_configured</code>,{" "}
            <code className={mono}>rate_limited</code>, or{" "}
            <code className={mono}>truncated</code> when a screen read only the
            highest-ranked related parties up to its per-lookup limit). An empty{" "}
            <code className={mono}>risk_signals</code> list alongside a non-empty{" "}
            <code className={mono}>degraded_sources</code> list is not a clean screen.
          </ApiEndpoint>
          <ApiEndpoint path="/lookup-stream?lei=<LEI>">
            The same synthesis streamed as Server-Sent Events (<code className={mono}>gleif_done</code>,
            per-source <code className={mono}>hit</code> / <code className={mono}>source_error</code>,
            then <code className={mono}>done</code>) so a client can render progressively.
            The <code className={mono}>risk_signals</code> event carries the same{" "}
            <code className={mono}>degraded_sources</code> field as <code className={mono}>/lookup</code>.
          </ApiEndpoint>
          <ApiEndpoint path="/lookup-register?scheme=<SCHEME>&id=<number>">
            Due diligence for a company with no LEI, anchored on its register number
            (any scheme <code className={mono}>/expand-schemes</code> lists —{" "}
            <code className={mono}>GB-COH</code>, <code className={mono}>NL-KVK</code>, …). The register that
            owns the scheme is the one source read; its bundle is then screened and assessed as{" "}
            <code className={mono}>/lookup</code>’s is. Same shape with <code className={mono}>lei</code> null,{" "}
            <code className={mono}>scheme</code> and <code className={mono}>id</code> added, no{" "}
            <code className={mono}>listing</code>; the company’s own name is screened by name. Same rate tier and
            lookup budget as <code className={mono}>/lookup</code>; replay cache keyed on{" "}
            <code className={mono}>scheme:id</code>. Optional <code className={mono}>name</code> for registers that
            search by name behind the number (KvK publishes none). <code className={mono}>/lookup-register-stream</code> is the
            SSE form (<code className={mono}>register_done</code> where <code className={mono}>/lookup-stream</code>{" "}
            says <code className={mono}>gleif_done</code>).
          </ApiEndpoint>
          <ApiEndpoint path="/lookup-source?lei=<LEI>&source_id=<id>">
            Re-run a single source for an existing lookup (the per-source “retry” in the UI). Returns{" "}
            <code className={mono}>hits</code>, <code className={mono}>error</code> /{" "}
            <code className={mono}>error_type</code>, and <code className={mono}>source_liveness</code> — what
            the retry itself read, in the shape <code className={mono}>/lookup</code> gives each source. The
            run’s verdict and coverage are not recomputed.
          </ApiEndpoint>
          <ApiEndpoint path="/batch-stream?leis=<LEI,LEI,…>">
            Screen up to 20 LEIs in one request — the same pipeline as{" "}
            <code className={mono}>/lookup</code>, looped two at a time, streamed as{" "}
            <code className={mono}>batch_start</code>, one <code className={mono}>row_done</code> or{" "}
            <code className={mono}>row_failed</code> per LEI in completion order, then{" "}
            <code className={mono}>batch_done</code>. A failed row is a row with{" "}
            <code className={mono}>degraded: true</code>, never a dropped one. Heavy rate tier.
          </ApiEndpoint>
          <ApiEndpoint path="/batch-export?leis=<LEI,LEI,…>">
            The same list as one zip: <code className={mono}>bundle.json</code> (every company’s
            BODS statements, de-duplicated by <code className={mono}>statementId</code>),{" "}
            <code className={mono}>rows.csv</code>, <code className={mono}>manifest.json</code> and a{" "}
            <code className={mono}>LICENSES.md</code> over the union of sources — the most
            restrictive licence in the set applies to the whole bundle. Rows that could not be
            screened are listed in the manifest and the CSV, never omitted. Heavy rate tier.
          </ApiEndpoint>
        </BtsCard>

        <BtsCard title="Search &amp; drill-down">
          <ApiEndpoint path="/search?q=<query>&kind=<entity|person>">
            Free-text fan-out search across every source, ranked best-first across
            sources: exact name matches first, then same name with another legal form,
            then partial matches; ties go to live companies and to rows with an LEI.
            Power-user / debugging path; the LEI-anchored{" "}
            <code className={mono}>/lookup</code> is the precise one.
          </ApiEndpoint>
          <ApiEndpoint path="/stream?q=<query>&kind=<...>">
            The same search, streamed as Server-Sent Events.
          </ApiEndpoint>
          <ApiEndpoint path="/report?q=<query>&kind=<...>">
            One-shot free-text synthesis (the pre-LEI flow): search, reconcile, deepen
            the top hits, assess risk.
          </ApiEndpoint>
          <ApiEndpoint path="/deepen?source=<id>&hit_id=<id>">
            The full record for a single hit, mapped to BODS statements, with its risk signals.
          </ApiEndpoint>
        </BtsCard>

        <BtsCard title="Export &amp; licensing">
          <ApiEndpoint
            /* Generated from the shared EXPORT_FORMATS list rather than typed
               out: this line has drifted from the backend twice (Phase 81
               fixed it once), and a reference page that under-reports the API
               is worse than no reference page. */
            path={`/export?lei=<LEI>&format=<${EXPORT_FORMATS.join("|")}>&subsidiaries=<bool>`}
            params={[
              [
                "format",
                "zip ships bods.json + bods.jsonl + bods.xml + senzing.jsonl + ftm.jsonl + manifest.json + LICENSES.md; json / jsonl / xml return the statements only; csv returns the entity / person / ownership-edge tables as a zip; cypher returns a Neo4j Cypher script; senzing returns Senzing JSON entity records for entity resolution; ftm returns FollowTheMoney entities for OpenSanctions / OpenAleph workflows; gql returns a BigQuery property-graph zip; amlai returns Google AML AI input tables; rdf returns BODS RDF as TriG — one named graph per statement, a canonical licence URI on every statement, and bods:Annotation in two separated layers: the register's own words (nature-of-control codes, imprecise-date notes) in each statement's graph, and OpenCheck's risk signals and entity-resolution links in a separate analysis graph.",
              ],
              [
                "subsidiaries",
                "opt-in (default false): also fold the GLEIF subsidiary network (direct + ultimate children) into the bundle. Off by default — a large group can add hundreds of statements.",
              ],
            ]}
          >
            A reproducible, downloadable BODS bundle. Shares its synthesis with{" "}
            <code className={mono}>/lookup</code>, so the export mirrors exactly what you
            saw on screen.
          </ApiEndpoint>
          <ApiEndpoint path="/license-matrix?sources=<a,b,c>">
            Per-source licence terms (commercial use, attribution, share-alike) plus a
            combined commercial-use assessment for the listed sources — the data behind
            the export “licensing assistant”.
          </ApiEndpoint>
        </BtsCard>

        <BtsCard title="AI narrative &amp; analyst sign-off">
          <p className="text-oo-small leading-[1.7] text-oo-muted mb-3">
            An optional AI-written plain-English summary of a lookup, plus the
            defensible audit trail an analyst builds around it. Generated only on
            request; each run is identified by a <code className={mono}>run_id</code>{" "}
            so dispositions stay pinned to the exact narrative they signed off.
          </p>
          <ApiEndpoint
            path="/narrative?lei=<LEI>"
            params={[
              ["deepen_top", "How many top hits to deepen before summarising (default 5)."],
              ["refresh", "Bypass the short-lived replay cache (default false)."],
            ]}
          >
            A cited, plain-English summary of the subject built from the same BODS
            synthesis as <code className={mono}>/lookup</code> — returns the{" "}
            <code className={mono}>summary</code>, per-sentence{" "}
            <code className={mono}>claims</code> with source citations, stated{" "}
            <code className={mono}>limitations</code>, the <code className={mono}>model</code> /{" "}
            <code className={mono}>prompt_version</code>, and a <code className={mono}>run_id</code>{" "}
            identifying this exact run.
          </ApiEndpoint>
          <ApiEndpoint
            method="PUT"
            path="/narrative/dispositions"
            params={[
              ["body", "{ lei, run_id, prompt_version, model, dispositions: [{ claim_id, status, comment }] }"],
              ["status", "accepted | disputed | needs_review — the analyst's decision per claim."],
            ]}
          >
            Persist the analyst’s claim dispositions for one narrative run (whole-sheet,
            last-write-wins). No model call — pure metadata around an existing narrative;
            <code className={mono}>decided_at</code> / <code className={mono}>updated_at</code>{" "}
            are stamped server-side.
          </ApiEndpoint>
          <ApiEndpoint
            path="/narrative/dispositions?lei=<LEI>&run_id=<id>"
          >
            The stored disposition sheet for a <code className={mono}>(lei, run_id)</code> run,
            or 404 if none has been saved. Rehydrates the sign-off state when the page reloads.
          </ApiEndpoint>
          <ApiEndpoint method="POST" path="/export/pdf">
            An accessible (tagged PDF/UA-1) due-diligence report for an LEI, built from
            the same cached lookup as <code className={mono}>/lookup</code>. The request
            body embeds the <code className={mono}>narrative</code> the client already
            generated (no fresh model call) and, when present, the analyst’s{" "}
            <code className={mono}>dispositions</code>, so the accept / dispute / needs-review
            decisions and notes are rendered into the report’s audit trail. With{" "}
            <code className={mono}>saved_report_id</code> it renders a saved report instead —
            its own summary and sign-off, its SHA-256 in every page’s footer, no new check.
          </ApiEndpoint>
        </BtsCard>

        <BtsCard title="Enrichments — on demand">
          <p className="text-oo-small leading-[1.7] text-oo-muted mb-3">
            Heavier, source-specific views kept off the main lookup and fetched
            only when asked. Each returns JSON; results are cached.
          </p>
          <ApiEndpoint
            path="/subsidiaries?lei=<LEI>&format=<summary|bods>"
            params={[
              [
                "format",
                "summary (counts + tagged children, default) or bods (adds the BODS statements for the graph / export).",
              ],
            ]}
          >
            A company’s GLEIF Level-2{" "}
            <strong className="text-oo-ink font-semibold">subsidiary network</strong> —
            direct and ultimate children merged and tagged{" "}
            <code className={mono}>direct</code> / <code className={mono}>ultimate</code> /{" "}
            <code className={mono}>both</code>, with exact counts (even when the child
            list is page-capped), a jurisdiction and a country spread, and a{" "}
            <code className={mono}>render_mode</code> hint (graph ≤ 150 nodes, else table).
            In BODS, each parent–child pair is one relationship dated from GLEIF’s
            relationship record, and a child’s direct parent is drawn where GLEIF names one.
          </ApiEndpoint>
          <ApiEndpoint path="/securities?lei=<LEI>&page=<n>">
            Securities (ISINs) mapped to the LEI from GLEIF + OpenFIGI, flagging any that
            are <strong className="text-oo-ink font-semibold">sanctioned</strong> (incl.
            GLEIF’s blind spot for issuers with no listed ISINs).
          </ApiEndpoint>
          <ApiEndpoint path="/history?lei=<LEI>&include_noise=<bool>">
            The <strong className="text-oo-ink font-semibold">change history</strong>{" "}
            behind the History tab: every register that keeps a change log — GLEIF,
            Companies House, New Zealand, Estonia, Denmark, New York, Latvia — merged onto one shared
            model, with <code className={mono}>registry_numbers</code> saying how each
            addresses the company; <code className={mono}>include_noise</code> folds in
            administrative changes.
          </ApiEndpoint>
          <ApiEndpoint path="/nz-associations?company_number=<n>">
            For a New Zealand company, the other companies its directors and shareholders
            are linked to — a nominee / mass-directorship review, graded by address.
          </ApiEndpoint>
          <ApiEndpoint path="/nz-associations/person?company_number=<n>&name=<name>">
            The same for one director or shareholder of that company, named as the
            register files them — what each person&rsquo;s BackgroundCheck card shows.
          </ApiEndpoint>
        </BtsCard>

        <BtsCard title="Catalogue &amp; health">
          <ApiEndpoint path="/sources">
            Inventory of the source adapters: id, name, licence, description, category,
            and whether each is live.
          </ApiEndpoint>
          <ApiEndpoint path="/health">Liveness probe.</ApiEndpoint>
        </BtsCard>

        <BtsCard title="Quick start">
          <p className="text-oo-small leading-[1.7] text-oo-muted mb-3">
            Search for a company by its LEI and get the unified BODS view:
          </p>
          <CopyField value={`curl "${base}/lookup?lei=HWUPKR0MPOU8FGXBT394"`} />
          <p className="text-oo-small leading-[1.7] text-oo-muted mt-4">
            Full request/response detail is in{" "}
            <a
              href="https://github.com/StephenAbbott/opencheck/blob/main/docs/how-it-works.md#api-surface"
              target="_blank"
              rel="noreferrer"
              className="underline text-oo-blue hover:text-oo-burst"
            >
              docs/how-it-works.md
            </a>
            .
          </p>
        </BtsCard>
      </div>

      <McpSection endpoint={`${base}/mcp`} mono={mono} />
    </section>
  );
}

// ---------------------------------------------------------------------
// The MCP server, explained in the order the handshake gives an agent.
//
// A full-width section rather than one card in the endpoint grid: the old
// card squeezed tool names into a 560px column beside their descriptions,
// so long names broke mid-word and the list read as a wall. Each tool is now
// a row of its own, name over description, grouped under the step of the
// handshake it belongs to. The data is `lib/mcpTools.ts`, pinned to the
// server's TOOL_NAMES and its "Cost." paragraph by
// `backend/tests/test_api_page_mcp.py`.
// ---------------------------------------------------------------------

/** A tool name that may wrap only at its underscores, never mid-word. */
function ToolName({ name }: { name: string }) {
  const parts = name.split("_");
  return (
    <code className="font-mono text-oo-small font-semibold text-oo-blue">
      {parts.map((part, i) => (
        <span key={i}>
          {part}
          {i < parts.length - 1 ? (
            <>
              _<wbr />
            </>
          ) : null}
        </span>
      ))}
    </code>
  );
}

function ToolRow({ tool }: { tool: McpTool }) {
  return (
    <li className="py-3">
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
        <ToolName name={tool.name} />
        <span className="font-mono text-oo-meta text-oo-muted">({tool.args})</span>
        {tool.spendsBudget ? (
          <Chip className="sm:ml-auto">Lookup budget</Chip>
        ) : null}
      </div>
      <p className="mt-1 text-oo-small leading-[1.65] text-oo-muted">{tool.summary}</p>
    </li>
  );
}

function McpSection({ endpoint, mono }: { endpoint: string; mono: string }) {
  return (
    <section id="mcp" aria-labelledby="mcp-heading" className="mt-12 scroll-mt-6">
      <h3
        id="mcp-heading"
        className="font-head font-bold text-oo-title text-oo-ink leading-tight mb-2"
      >
        MCP server for AI agents
      </h3>
      <p className="text-oo-body leading-[1.75] text-oo-muted mb-6 max-w-2xl">
        OpenCheck speaks the{" "}
        <a
          href="https://modelcontextprotocol.io"
          target="_blank"
          rel="noreferrer"
          className="underline text-oo-blue hover:text-oo-burst"
        >
          Model Context Protocol
        </a>
        : the same pipeline as the REST API, as typed tools an AI agent calls directly. It
        uses streamable HTTP, needs no API key, and carries the same source licence notices.
      </p>

      <div className="grid gap-6 lg:grid-cols-[minmax(0,1fr)_minmax(0,20rem)] mb-10 max-w-4xl">
        <div className="bg-white border border-oo-rule rounded-oo p-6">
          <SectionLabel as="h4" className="mb-2">
            Connect
          </SectionLabel>
          <CopyField value={endpoint} />
          <ol className="mt-4 space-y-2 list-decimal pl-5 text-oo-small leading-[1.7] text-oo-muted">
            <li>
              Add the address as a custom connector in your MCP client (in Claude: Settings →
              Connectors).
            </li>
            <li>
              When it connects, the server gives your agent a short guide to the order of calls
              and how to read the answers. The guide is reproduced below, so you can see what
              your agent is told.
            </li>
            <li>
              Agents can also find the server through{" "}
              <a
                href="https://agenticresourcediscovery.org/"
                target="_blank"
                rel="noreferrer"
                className="underline text-oo-blue hover:text-oo-burst"
              >
                Agentic Resource Discovery
              </a>
              ; its descriptor, listing every tool, is at{" "}
              <code className={mono}>/.well-known/mcp.json</code>.
            </li>
          </ol>
        </div>

        <aside
          aria-labelledby="mcp-cost-heading"
          className="bg-oo-soft border border-oo-softBorder rounded-oo p-6"
        >
          <SectionLabel as="h4" id="mcp-cost-heading" tone="accent" className="mb-2">
            What a call costs
          </SectionLabel>
          <p className="text-oo-small leading-[1.7] text-oo-ink">
            Tools marked <Chip>Lookup budget</Chip> share one budget per network address: ten
            fresh checks a minute. Repeating a check within 15 minutes is free. The other tools
            do not use it.
          </p>
          <p className="mt-3 text-oo-small leading-[1.7] text-oo-ink">
            When it runs out, the answer is a 429 with <code className={mono}>retry_after_s</code>:
            wait that long, then retry, never at once. Everyone behind one address (a
            conference room, an office) shares the budget.
          </p>
        </aside>
      </div>

      <h4 id="mcp-order" className="font-head font-bold text-oo-head text-oo-ink mb-4">
        Call the tools in this order
      </h4>
      <ol role="list" aria-labelledby="mcp-order" className="space-y-4 max-w-4xl">
        {MCP_STEPS.map((step, i) => (
          <li
            key={step.id}
            id={`mcp-step-${step.id}`}
            className="bg-white border border-oo-rule rounded-oo p-5 sm:p-6 flex gap-4"
          >
            <span
              aria-hidden="true"
              className="shrink-0 w-8 h-8 rounded-full bg-oo-soft border border-oo-softBorder text-oo-blue font-head font-bold text-oo-small flex items-center justify-center"
            >
              {i + 1}
            </span>
            <div className="min-w-0 flex-1">
              <h5 className="font-head font-bold text-oo-lead text-oo-ink leading-snug">
                <span className="sr-only">Step {i + 1}: </span>
                {step.title}
              </h5>
              <p className="mt-1 text-oo-small leading-[1.7] text-oo-muted">{step.lede}</p>

              {step.tools.length > 0 ? (
                <ul className="mt-3 border-t border-oo-rule divide-y divide-oo-rule">
                  {step.tools.map((tool) => (
                    <ToolRow key={tool.name} tool={tool} />
                  ))}
                </ul>
              ) : null}

              {step.rules ? (
                <dl className="mt-3 border-t border-oo-rule divide-y divide-oo-rule">
                  {step.rules.map((rule) => (
                    <div key={rule.term} className="py-2.5 text-oo-small leading-[1.65]">
                      <dt
                        className={`inline font-semibold text-oo-ink ${
                          /^[a-z_]+$/.test(rule.term) ? "font-mono" : ""
                        }`}
                      >
                        {rule.term}
                      </dt>{" "}
                      <dd className="inline text-oo-muted">{rule.text}</dd>
                    </div>
                  ))}
                </dl>
              ) : null}

              {step.note ? (
                <p className="mt-3 text-oo-small leading-[1.7] text-oo-muted">
                  {step.note.text}{" "}
                  <a
                    href={step.note.href}
                    target="_blank"
                    rel="noreferrer"
                    className="underline text-oo-blue hover:text-oo-burst"
                  >
                    {step.note.linkLabel}
                  </a>
                  .
                </p>
              ) : null}
            </div>
          </li>
        ))}
      </ol>
    </section>
  );
}
