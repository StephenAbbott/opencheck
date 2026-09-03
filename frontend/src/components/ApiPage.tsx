/**
 * The /api page — the read-only REST surface, documented.
 *
 * Lifted out of `App.tsx` in Phase 168, unchanged. It had no business being
 * there: it is a static document with no state of its own, mounted by one line
 * of App and touched by nobody else.
 */

import { useState } from "react";

import { BASE_URL, EXPORT_FORMATS } from "../lib/api";
import { BtsCard } from "./BtsCard";

// ---------------------------------------------------------------------
// API page — documents the read-only REST surface (api.opencheck.world).
// ---------------------------------------------------------------------

function CopyField({ value }: { value: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <div className="flex items-center gap-3 bg-oo-bg border border-oo-rule rounded-oo px-3 py-2">
      <code className="font-mono text-[13px] text-oo-ink flex-1 break-all">{value}</code>
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
        <code className="font-mono text-[13px] text-oo-ink break-all">{path}</code>
      </div>
      <p className="text-[13px] leading-[1.7] text-oo-muted mt-1.5">{children}</p>
      {params && params.length > 0 && (
        <dl className="mt-2 space-y-1">
          {params.map(([k, v]) => (
            <div key={k} className="flex gap-2 text-[12.5px] leading-[1.6]">
              <dt className="font-mono text-oo-blue shrink-0">{k}</dt>
              <dd className="text-oo-muted">{v}</dd>
            </div>
          ))}
        </dl>
      )}
    </div>
  );
}

export function ApiPage() {
  const base = BASE_URL || "https://api.opencheck.world";
  const mono = "font-mono text-[12px] bg-oo-bg px-1 rounded";
  return (
    <section aria-labelledby="api-heading">
      <h2
        id="api-heading"
        className="font-head font-bold text-[clamp(1.35rem,3vw,1.8rem)] text-oo-ink mb-2 leading-tight"
      >
        API
      </h2>
      <p className="text-[14px] leading-[1.75] text-oo-muted mb-6 max-w-2xl">
        OpenCheck exposes a small, read-only REST API. Every endpoint is a{" "}
        <code className={mono}>GET</code> that returns JSON — except{" "}
        <code className={mono}>/export</code> (a downloadable bundle) and the
        streaming endpoints (Server-Sent Events). Every result is expressed in the{" "}
        <a
          href="https://standard.openownership.org/en/0.4.0/"
          target="_blank"
          rel="noreferrer"
          className="underline text-oo-blue hover:text-oo-burst"
        >
          Beneficial Ownership Data Standard (BODS) v0.4
        </a>
        . No API key is required to read. OpenCheck also runs a{" "}
        <strong className="text-oo-ink font-semibold">Model Context Protocol (MCP)</strong>{" "}
        server, so AI agents can call the same pipeline as typed tools — see below.
      </p>

      <div className="mb-8 max-w-2xl">
        <div className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-2">
          Base URL
        </div>
        <CopyField value={base} />
      </div>

      <div
        className="grid gap-6"
        style={{ gridTemplateColumns: "repeat(auto-fill, minmax(min(100%, 560px), 1fr))" }}
      >
        <BtsCard title="Lookup">
          <ApiEndpoint
            path="/lookup?lei=<LEI>"
            params={[
              ["lei", "20-character ISO 17442 LEI (required)."],
              ["deepen_top", "How many top hits to fully fetch + map + assess (default 3)."],
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
            <code className={mono}>rate_limited</code>). An empty{" "}
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
          <ApiEndpoint path="/lookup-source?lei=<LEI>&source_id=<id>">
            Re-run a single source for an existing lookup (the per-source “retry” in the UI).
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

        <BtsCard title="MCP server — for AI agents">
          <p className="text-[13px] leading-[1.7] text-oo-muted mb-3">
            OpenCheck speaks the{" "}
            <a
              href="https://modelcontextprotocol.io"
              target="_blank"
              rel="noreferrer"
              className="underline text-oo-blue hover:text-oo-burst"
            >
              Model Context Protocol
            </a>
            , exposing the same pipeline as typed tools an AI agent can call
            directly — no glue code. It uses streamable HTTP, needs no API key,
            and carries the same source licence notices as the REST API.
          </p>
          <div className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-2">
            Endpoint
          </div>
          <CopyField value={`${base}/mcp`} />
          <div className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mt-4 mb-2">
            Tools
          </div>
          <dl className="space-y-1.5">
            {([
              ["opencheck_search", "Find a company’s LEI from a name or free text."],
              ["opencheck_resolve_national_id", "Resolve a national company-registration number to its LEI."],
              ["opencheck_lookup", "Due diligence by LEI: identity, identifiers, risk signals, source coverage."],
              ["opencheck_batch_lookup", "Up to 20 LEIs at once: one compact row each, with failed rows kept apart."],
              ["opencheck_export_bods", "The full ownership-and-control graph as BODS v0.4 statements."],
              ["opencheck_list_sources", "Inventory of the data sources, with licence and live status."],
            ] as [string, string][]).map(([name, desc]) => (
              <div key={name} className="flex gap-2 text-[12.5px] leading-[1.6]">
                <dt className="font-mono text-oo-blue shrink-0 break-all">{name}</dt>
                <dd className="text-oo-muted">{desc}</dd>
              </div>
            ))}
          </dl>
          <p className="text-[13px] leading-[1.7] text-oo-muted mt-4">
            Add it as a custom connector in any MCP client (e.g. Claude Desktop →
            Settings → Connectors). It is discoverable via{" "}
            <a
              href="https://agenticresourcediscovery.org/"
              target="_blank"
              rel="noreferrer"
              className="underline text-oo-blue hover:text-oo-burst"
            >
              Agentic Resource Discovery
            </a>
            ; the server descriptor is at{" "}
            <code className={mono}>/.well-known/mcp.json</code>.
          </p>
        </BtsCard>

        <BtsCard title="Search &amp; drill-down">
          <ApiEndpoint path="/search?q=<query>&kind=<entity|person>">
            Free-text fan-out search across every source. Power-user / debugging path;
            the LEI-anchored <code className={mono}>/lookup</code> is the precise one.
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
          <p className="text-[13px] leading-[1.7] text-oo-muted mb-3">
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
            decisions and notes are rendered into the report’s audit trail.
          </ApiEndpoint>
        </BtsCard>

        <BtsCard title="Enrichments — on demand">
          <p className="text-[13px] leading-[1.7] text-oo-muted mb-3">
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
            list is page-capped), a jurisdiction spread, and a{" "}
            <code className={mono}>render_mode</code> hint (graph ≤ 150 nodes, else table).
          </ApiEndpoint>
          <ApiEndpoint path="/securities?lei=<LEI>&page=<n>">
            Securities (ISINs) mapped to the LEI from GLEIF + OpenFIGI, flagging any that
            are <strong className="text-oo-ink font-semibold">sanctioned</strong> (incl.
            GLEIF’s blind spot for issuers with no listed ISINs).
          </ApiEndpoint>
          <ApiEndpoint path="/history?lei=<LEI>&include_noise=<bool>">
            The <strong className="text-oo-ink font-semibold">Time Machine</strong>{" "}
            change-over-time timeline (GLEIF + Companies House) on one shared model;{" "}
            <code className={mono}>include_noise</code> folds in administrative changes.
          </ApiEndpoint>
          <ApiEndpoint path="/nz-associations?company_number=<n>">
            For a New Zealand company, the other companies its directors and shareholders
            are linked to — a nominee / mass-directorship review, graded by address.
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
          <p className="text-[13px] leading-[1.7] text-oo-muted mb-3">
            Search for a company by its LEI and get the unified BODS view:
          </p>
          <CopyField value={`curl "${base}/lookup?lei=HWUPKR0MPOU8FGXBT394"`} />
          <p className="text-[13px] leading-[1.7] text-oo-muted mt-4">
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
    </section>
  );
}
