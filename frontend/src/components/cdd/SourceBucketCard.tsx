import { lazy, Suspense, useEffect, useState } from "react";
import { deepen } from "../../lib/api";
import type { BodsBreakdown, DeepenResponse, RiskSignal, SourceHit } from "../../lib/api";
import { RiskChip } from "../risk/RiskChip";
import { HistoryTimeline } from "./HistoryTimeline";
import { NzAssociations } from "./NzAssociations";
import { SubsidiaryNetwork } from "./SubsidiaryNetwork";

// BodsGraphExplorer pulls in Cytoscape + cytoscape-dagre (~the bulk of the
// bundle) but only renders when a user clicks "Visualise". Code-split it so
// the initial page load never ships the graph engine.
const BodsGraphExplorer = lazy(() => import("../BodsGraphExplorer"));

export interface SourceBucket {
  sourceId: string;
  sourceName: string;
  hits: SourceHit[];
  error?: string;
}

// ---------------------------------------------------------------------
// LicenseChip — small inline license badge
// ---------------------------------------------------------------------

function LicenseChip({ license }: { license: string }) {
  const nc = license.toLowerCase().includes("nc");
  const classes = nc
    ? "bg-amber-50 text-amber-800 border-amber-200"
    : "bg-emerald-50 text-emerald-700 border-emerald-200";
  return (
    <span
      className={`text-[11px] border rounded px-1.5 py-0.5 font-mono ${classes}`}
    >
      {license}
    </span>
  );
}

// ---------------------------------------------------------------------
// sourceEntityUrl — resolve the public URL for a source hit
// ---------------------------------------------------------------------

function sourceEntityUrl(sourceId: string, hit: SourceHit): string | null {
  const raw = hit.raw;

  // Many adapters set raw.link directly
  if (typeof raw.link === "string" && raw.link) return raw.link;
  // cvr_denmark, sec_edgar use source_url
  if (typeof raw.source_url === "string" && raw.source_url) return raw.source_url;
  // opencorporates
  if (typeof raw.opencorporates_url === "string" && raw.opencorporates_url)
    return raw.opencorporates_url;
  // openaleph: raw.links.ui
  const rawLinks = raw.links as Record<string, unknown> | undefined;
  if (rawLinks && typeof rawLinks.ui === "string" && rawLinks.ui) return rawLinks.ui;

  // Source-specific construction from hit_id
  const id = hit.hit_id;
  switch (sourceId) {
    case "gleif":
    case "bods_gleif":
      return `https://search.gleif.org/#/record/${id}`;
    case "companies_house":
    case "bods_uk_psc":
      return `https://find-and-update.company-information.service.gov.uk/company/${id}`;
    case "wikidata":
      return `https://www.wikidata.org/wiki/${(raw.qid as string) || id}`;
    case "opensanctions":
    case "everypolitician":
      return `https://www.opensanctions.org/entities/${id}`;
    case "brreg":
      return `https://w2.brreg.no/enhet/sok/detalj.jsp?orgnr=${id}`;
    case "prh":
      return `https://tietopalvelu.ytj.fi/yritystiedot.aspx?yavain=${id}`;
    case "kvk":
      return `https://www.kvk.nl/zoeken/?source=all&q=${id}`;
    case "ur_latvia":
      return `https://www.latvija.lv/lv/bizness/uznemumu-registrs/${id}`;
    case "firmenbuch":
      return `https://justizonline.gv.at/jop/web/firmenbuchabfrage?firmennummer=${encodeURIComponent(id)}`;
    case "corporations_canada":
      return `https://ised-isde.canada.ca/cc/lgcy/fdrlCrpDtls.html?corpId=${id}`;
    case "cro":
      return `https://core.cro.ie/company/${id}`;
    case "bolagsverket": {
      const orgNo = (raw.org_number as string) || id;
      return `https://webbotjanster.bolagsverket.se/foretag-och-foreningar/foreningsregistret/SokOrganisationsnummer?q=${orgNo}`;
    }
    case "sec_edgar": {
      const cik = (raw.cik as string) || id;
      return `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${cik}`;
    }
    case "inpi":
      return `https://data.inpi.fr/entreprises/${id}`;
    case "zefix":
      return `https://www.zefix.ch/en/search/entity/list?name=${encodeURIComponent(id)}`;
    case "sudreg_croatia":
      return `https://sudreg.pravosudje.hr/registar/f?p=150:28:0::NO:RP,28:P28_SBT_MBS:${id}`;
    default:
      return null;
  }
}

// ---------------------------------------------------------------------
// BODS statement cards
// ---------------------------------------------------------------------

type BODSStmt = Record<string, unknown>;

function stmtStr(obj: unknown, ...keys: string[]): string {
  let cur: unknown = obj;
  for (const k of keys) {
    if (cur == null || typeof cur !== "object") return "";
    cur = (cur as Record<string, unknown>)[k];
  }
  return typeof cur === "string" ? cur : "";
}

function stmtArr(obj: unknown, key: string): unknown[] {
  if (obj == null || typeof obj !== "object") return [];
  const v = (obj as Record<string, unknown>)[key];
  return Array.isArray(v) ? v : [];
}

function IdentifierPill({ id }: { id: unknown }) {
  const scheme = stmtStr(id, "schemeName") || stmtStr(id, "scheme");
  const value = stmtStr(id, "id");
  if (!value) return null;
  return (
    <span className="inline-flex items-center gap-1 font-mono text-[10px] bg-white border border-oo-rule rounded px-1.5 py-0.5">
      {scheme && <span className="text-oo-muted">{scheme}:</span>}
      <span className="text-oo-ink">{value}</span>
    </span>
  );
}

function FieldRow({
  label,
  value,
  mono,
}: {
  label: string;
  value: React.ReactNode;
  mono?: boolean;
}) {
  if (!value && value !== 0) return null;
  return (
    <div className="flex gap-2 items-baseline min-w-0">
      <span className="text-[10px] text-oo-muted font-semibold uppercase tracking-wide whitespace-nowrap w-28 shrink-0">
        {label}
      </span>
      <span
        className={`text-[11px] text-oo-ink break-words min-w-0 ${mono ? "font-mono" : ""}`}
      >
        {value}
      </span>
    </div>
  );
}

function EntityStatementCard({ stmt }: { stmt: BODSStmt }) {
  const rd = (stmt.recordDetails ?? {}) as Record<string, unknown>;
  const name = stmtStr(rd, "name");
  const entityType = stmtStr(rd, "entityType", "type");
  const jurisdiction = stmtStr(rd, "incorporatedInJurisdiction", "name");
  const jurisdictionCode = stmtStr(rd, "incorporatedInJurisdiction", "code");
  const foundingDate = stmtStr(rd, "foundingDate");
  const identifiers = stmtArr(rd, "identifiers");
  const addresses = stmtArr(rd, "addresses");
  const sourceDesc = stmtStr(stmt, "source", "description");
  const statementId = stmtStr(stmt, "statementId");

  return (
    <div className="rounded-oo border border-blue-200 bg-blue-50/40 overflow-hidden">
      <div className="flex items-center justify-between gap-2 px-3 py-2 bg-blue-100/60 border-b border-blue-200">
        <span className="text-[10px] font-semibold tracking-oo-eyebrow uppercase text-blue-700">
          Entity
        </span>
        {entityType && (
          <span className="text-[10px] font-mono text-blue-600">{entityType}</span>
        )}
      </div>
      <div className="px-3 py-2.5 space-y-1.5">
        <FieldRow label="Name" value={name || <span className="text-oo-muted italic">unknown</span>} />
        {(jurisdiction || jurisdictionCode) && (
          <FieldRow
            label="Jurisdiction"
            value={[jurisdiction, jurisdictionCode].filter(Boolean).join(" · ")}
          />
        )}
        {foundingDate && <FieldRow label="Founded" value={foundingDate} mono />}
        {identifiers.length > 0 && (
          <FieldRow
            label="Identifiers"
            value={
              <span className="flex flex-wrap gap-1">
                {identifiers.map((id, i) => (
                  <IdentifierPill key={i} id={id} />
                ))}
              </span>
            }
          />
        )}
        {addresses.map((addr, i) => {
          const addrStr = stmtStr(addr, "address");
          const addrType = stmtStr(addr, "type");
          const addrCountry = stmtStr(addr, "country", "name");
          const full = [addrStr, addrCountry].filter(Boolean).join(", ");
          if (!full) return null;
          return (
            <FieldRow
              key={i}
              label={`Address${addrType ? ` (${addrType})` : ""}`}
              value={full}
            />
          );
        })}
        {sourceDesc && <FieldRow label="Source" value={sourceDesc} />}
        <details className="mt-1">
          <summary className="text-[10px] font-mono text-oo-muted cursor-pointer">
            {statementId ? statementId.slice(0, 28) + "…" : "Statement ID"}
          </summary>
          <pre className="mt-1 text-[9px] font-mono bg-white border border-oo-rule rounded p-2 overflow-auto max-h-48">
            {JSON.stringify(stmt, null, 2)}
          </pre>
        </details>
      </div>
    </div>
  );
}

function PersonStatementCard({ stmt }: { stmt: BODSStmt }) {
  const rd = (stmt.recordDetails ?? {}) as Record<string, unknown>;
  const names = stmtArr(rd, "names");
  const fullName =
    names.length > 0 ? stmtStr(names[0], "fullName") : "";
  const personType = stmtStr(rd, "personType");
  const birthDate = stmtStr(rd, "birthDate");
  const nationalities = stmtArr(rd, "nationalities");
  const identifiers = stmtArr(rd, "identifiers");
  const sourceDesc = stmtStr(stmt, "source", "description");
  const statementId = stmtStr(stmt, "statementId");

  return (
    <div className="rounded-oo border border-violet-200 bg-violet-50/40 overflow-hidden">
      <div className="flex items-center justify-between gap-2 px-3 py-2 bg-violet-100/60 border-b border-violet-200">
        <span className="text-[10px] font-semibold tracking-oo-eyebrow uppercase text-violet-700">
          Person
        </span>
        {personType && (
          <span className="text-[10px] font-mono text-violet-600">{personType}</span>
        )}
      </div>
      <div className="px-3 py-2.5 space-y-1.5">
        <FieldRow label="Name" value={fullName || <span className="text-oo-muted italic">unknown</span>} />
        {birthDate && <FieldRow label="Born" value={birthDate} mono />}
        {nationalities.length > 0 && (
          <FieldRow
            label="Nationality"
            value={nationalities
              .map((n) => stmtStr(n, "name") || stmtStr(n, "code"))
              .filter(Boolean)
              .join(", ")}
          />
        )}
        {identifiers.length > 0 && (
          <FieldRow
            label="Identifiers"
            value={
              <span className="flex flex-wrap gap-1">
                {identifiers.map((id, i) => (
                  <IdentifierPill key={i} id={id} />
                ))}
              </span>
            }
          />
        )}
        {sourceDesc && <FieldRow label="Source" value={sourceDesc} />}
        <details className="mt-1">
          <summary className="text-[10px] font-mono text-oo-muted cursor-pointer">
            {statementId ? statementId.slice(0, 28) + "…" : "Statement ID"}
          </summary>
          <pre className="mt-1 text-[9px] font-mono bg-white border border-oo-rule rounded p-2 overflow-auto max-h-48">
            {JSON.stringify(stmt, null, 2)}
          </pre>
        </details>
      </div>
    </div>
  );
}

function describeInterest(interest: unknown): string {
  const type = stmtStr(interest, "type");
  const doi = stmtStr(interest, "directOrIndirect");
  const share = (interest as Record<string, unknown>)?.share as
    | Record<string, unknown>
    | undefined;
  let parts: string[] = [];
  if (type) parts.push(type);
  if (doi) parts.push(doi);
  if (share) {
    const exact = share.exact;
    const min = share.minimum;
    const max = share.maximum;
    if (exact != null) parts.push(`${exact}%`);
    else if (min != null && max != null) parts.push(`${min}–${max}%`);
    else if (min != null) parts.push(`≥${min}%`);
  }
  return parts.join(" · ");
}

function stmtLabel(
  id: string,
  lookup: Map<string, BODSStmt>
): string {
  const s = lookup.get(id);
  if (!s) return id.slice(0, 16) + "…";
  const rd = (s.recordDetails ?? {}) as Record<string, unknown>;
  if (s.recordType === "entity") return stmtStr(rd, "name") || id.slice(0, 16) + "…";
  if (s.recordType === "person") {
    const names = stmtArr(rd, "names");
    return (names.length > 0 ? stmtStr(names[0], "fullName") : "") || id.slice(0, 16) + "…";
  }
  return id.slice(0, 16) + "…";
}

function RelationshipStatementCard({
  stmt,
  lookup,
}: {
  stmt: BODSStmt;
  lookup: Map<string, BODSStmt>;
}) {
  const rd = (stmt.recordDetails ?? {}) as Record<string, unknown>;
  const subjectId = stmtStr(rd, "subject");
  const interestedPartyId = stmtStr(rd, "interestedParty");
  const interests = stmtArr(rd, "interests");
  const statementDate = stmtStr(stmt, "statementDate");
  const sourceDesc = stmtStr(stmt, "source", "description");
  const statementId = stmtStr(stmt, "statementId");

  return (
    <div className="rounded-oo border border-teal-200 bg-teal-50/40 overflow-hidden">
      <div className="flex items-center justify-between gap-2 px-3 py-2 bg-teal-100/60 border-b border-teal-200">
        <span className="text-[10px] font-semibold tracking-oo-eyebrow uppercase text-teal-700">
          Ownership / Control
        </span>
        {statementDate && (
          <span className="text-[10px] font-mono text-teal-600">{statementDate}</span>
        )}
      </div>
      <div className="px-3 py-2.5 space-y-1.5">
        {subjectId && (
          <FieldRow
            label="Subject"
            value={stmtLabel(subjectId, lookup)}
          />
        )}
        {interestedPartyId && (
          <FieldRow
            label="Interested party"
            value={stmtLabel(interestedPartyId, lookup)}
          />
        )}
        {interests.length > 0 && (
          <FieldRow
            label="Interests"
            value={
              <span className="space-y-0.5 block">
                {interests.map((int, i) => {
                  const desc = describeInterest(int);
                  const details = stmtStr(int, "details");
                  return (
                    <span key={i} className="block">
                      {desc}
                      {details && (
                        <span className="text-oo-muted ml-1">({details})</span>
                      )}
                    </span>
                  );
                })}
              </span>
            }
          />
        )}
        {sourceDesc && <FieldRow label="Source" value={sourceDesc} />}
        <details className="mt-1">
          <summary className="text-[10px] font-mono text-oo-muted cursor-pointer">
            {statementId ? statementId.slice(0, 28) + "…" : "Statement ID"}
          </summary>
          <pre className="mt-1 text-[9px] font-mono bg-white border border-oo-rule rounded p-2 overflow-auto max-h-48">
            {JSON.stringify(stmt, null, 2)}
          </pre>
        </details>
      </div>
    </div>
  );
}

function BODSStatementCards({ statements }: { statements: BODSStmt[] }) {
  const lookup = new Map<string, BODSStmt>();
  for (const s of statements) {
    const sid = stmtStr(s, "statementId");
    if (sid) lookup.set(sid, s);
  }

  return (
    <div className="space-y-2 mt-2">
      {statements.map((stmt, i) => {
        const type = stmtStr(stmt, "recordType");
        if (type === "entity")
          return <EntityStatementCard key={i} stmt={stmt} />;
        if (type === "person")
          return <PersonStatementCard key={i} stmt={stmt} />;
        if (type === "relationship")
          return (
            <RelationshipStatementCard key={i} stmt={stmt} lookup={lookup} />
          );
        return (
          <details key={i} className="text-[11px]">
            <summary className="font-mono text-oo-muted cursor-pointer">
              {type || "unknown"} statement
            </summary>
            <pre className="mt-1 text-[9px] font-mono bg-white border border-oo-rule rounded p-2 overflow-auto max-h-48">
              {JSON.stringify(stmt, null, 2)}
            </pre>
          </details>
        );
      })}
    </div>
  );
}

// ---------------------------------------------------------------------
// DeepenBlock — shows BODS graph + statements + raw JSON
// showDiagram / showStatements / showJson control which sections render.
// ---------------------------------------------------------------------

export function DeepenBlock({
  detail,
  entityName,
  showDiagram = true,
  showStatements = true,
  showJson = true,
}: {
  detail: DeepenResponse;
  entityName?: string;
  showDiagram?: boolean;
  showStatements?: boolean;
  showJson?: boolean;
}) {
  const anyVisible = showDiagram || showStatements || showJson;
  if (!anyVisible) return null;

  return (
    <div className="space-y-4">
      {detail.license_notice && (
        <div className="bg-amber-50 border border-amber-200 text-amber-900 rounded-oo p-3">
          <div className="flex items-baseline justify-between gap-2">
            <span className="font-head font-bold text-[13px]">License notice</span>
            <LicenseChip license={detail.license} />
          </div>
          <p className="mt-1 leading-[1.6]">{detail.license_notice}</p>
        </div>
      )}

      {detail.bods.length === 0 && (detail.raw.coverage_note as string | undefined) && (
        <div className="bg-sky-50 border border-sky-200 text-sky-900 rounded-oo p-3">
          <p className="text-[13px] leading-[1.6]">{detail.raw.coverage_note as string}</p>
        </div>
      )}

      {showDiagram && detail.bods.length > 0 && (
        <section>
          <h4 className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-2">
            BODS · {detail.bods.length} statement{detail.bods.length === 1 ? "" : "s"}
          </h4>
          {detail.bods_issues.length > 0 && (
            <p className="text-amber-800 mb-2">
              {detail.bods_issues.length} validation issue{detail.bods_issues.length === 1 ? "" : "s"}
            </p>
          )}
          <Suspense
            fallback={
              <div
                className="h-48 rounded-oo border border-oo-rule bg-oo-paper/40 animate-pulse flex items-center justify-center text-[12px] text-oo-muted"
                role="status"
              >
                Loading graph…
              </div>
            }
          >
            <BodsGraphExplorer statements={detail.bods} signals={detail.risk_signals} entityName={entityName} />
          </Suspense>
        </section>
      )}

      {showStatements && detail.bods.length > 0 && (
        <section>
          <h4 className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-2">
            Mapped statements
          </h4>
          <BODSStatementCards statements={detail.bods as BODSStmt[]} />
        </section>
      )}

      {showJson && (
        <section className="space-y-3">
          {detail.bods.length > 0 && (
            <div>
              <h4 className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-1.5">
                BODS statements
              </h4>
              <pre className="max-h-80 overflow-auto bg-white border border-oo-rule rounded-oo p-3 text-[10px]">
                {JSON.stringify(detail.bods, null, 2)}
              </pre>
            </div>
          )}
          <div>
            <h4 className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-1.5">
              Raw source payload
            </h4>
            <pre className="max-h-80 overflow-auto bg-white border border-oo-rule rounded-oo p-3 text-[10px]">
              {JSON.stringify(detail.raw, null, 2)}
            </pre>
          </div>
        </section>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------
// HitRow — single result row with three independent drill-down pills
// ---------------------------------------------------------------------

export function HitRow({
  hit,
  riskSignals,
  preloadedStmtCount,
  preloadedBreakdown,
  titleAccessory,
}: {
  hit: SourceHit;
  riskSignals: RiskSignal[];
  preloadedStmtCount?: number;
  preloadedBreakdown?: BodsBreakdown;
  /** Right-aligned control shown inline with the entity title (e.g. See timeline). */
  titleAccessory?: React.ReactNode;
}) {
  const [showDiagram,    setShowDiagram]    = useState(false);
  const [showStatements, setShowStatements] = useState(false);
  const [showJson,       setShowJson]       = useState(false);
  const [detail,  setDetail]  = useState<DeepenResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error,   setError]   = useState<string | null>(null);

  const anyOpen = showDiagram || showStatements || showJson;

  async function ensureFetched() {
    if (detail || loading) return;
    setLoading(true);
    setError(null);
    try {
      const data = await deepen(hit.source_id, hit.hit_id);
      setDetail(data);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }

  function toggleDiagram()    { ensureFetched(); setShowDiagram(v    => !v); }
  function toggleStatements() { ensureFetched(); setShowStatements(v => !v); }
  function toggleJson()       { ensureFetched(); setShowJson(v       => !v); }

  // Use post-click detail count when available; fall back to the pre-loaded count
  // from the bods_counts SSE event (available before any pill is clicked).
  const stmtCount = detail?.bods.length ?? preloadedStmtCount ?? 0;
  const hasKnownCount = detail !== null || preloadedStmtCount !== undefined;

  // A single (or zero) BODS statement is one entity with no relationships —
  // there is no ownership graph to draw, so suppress the Visualise strip. While
  // the count is still unknown (source not yet deepened) we keep the strip so
  // the affordance isn't withheld prematurely.
  const showGraphStrip = !hasKnownCount || stmtCount > 1;

  // If the strip was opened while the count was still unknown and the source
  // then resolves to ≤ 1 statement, the strip disappears — so close the diagram
  // too, otherwise it would be stuck open with no control to hide it.
  useEffect(() => {
    if (!showGraphStrip && showDiagram) setShowDiagram(false);
  }, [showGraphStrip, showDiagram]);

  // Graph-flavoured subtitle for the Visualise strip. Use the loaded detail
  // when available, otherwise the entity/relationship split streamed up front
  // via the bods_counts SSE event; fall back to a descriptive label only when
  // neither is known yet.
  const breakdown: BodsBreakdown | undefined = detail
    ? {
        entities: detail.bods.filter((s) => (s as Record<string, unknown>).recordType === "entity").length,
        relationships: detail.bods.filter((s) => (s as Record<string, unknown>).recordType === "relationship").length,
      }
    : preloadedBreakdown;
  const graphMeta = breakdown
    ? `${breakdown.entities} ${breakdown.entities === 1 ? "entity" : "entities"} · ${breakdown.relationships} ${breakdown.relationships === 1 ? "relationship" : "relationships"}`
    : "Interactive ownership & control graph";

  return (
    <li className="px-5 py-4">
      {/* Entity name (+ optional title accessory, e.g. See timeline), summary, risk chips */}
      <div className="flex items-start justify-between gap-3">
        <div className="font-head font-bold text-[15px] text-oo-ink leading-snug min-w-0">
          {(() => {
            const url = sourceEntityUrl(hit.source_id, hit);
            return url ? (
              <a href={url} target="_blank" rel="noopener noreferrer" className="hover:underline">
                {hit.name}
              </a>
            ) : hit.name;
          })()}
          {hit.is_stub && (
            <span className="ml-2 text-[11px] font-mono bg-amber-50 text-amber-800 border border-amber-200 rounded px-1.5 py-0.5">
              stub
            </span>
          )}
        </div>
        {titleAccessory && <div className="shrink-0">{titleAccessory}</div>}
      </div>
      <p className="text-[13px] text-oo-muted mt-1 leading-[1.6]">{hit.summary}</p>
      {riskSignals.length > 0 && (
        <div className="mt-2 flex flex-wrap gap-1">
          {riskSignals.map((sig, i) => (
            <RiskChip key={`${sig.code}-${i}`} signal={sig} compact />
          ))}
        </div>
      )}

      {/* Visualise — primary invitation strip (the graph is OpenCheck's headline
          feature, so it gets a full-width call to action rather than a peer pill).
          Hidden when the source returns ≤ 1 statement: a lone entity is not a graph. */}
      {showGraphStrip && (
      <button
        type="button"
        onClick={toggleDiagram}
        aria-pressed={showDiagram}
        className="mt-3 w-full flex items-center gap-3 rounded-oo border border-[#bcdcff] bg-[#dceeff] px-3 py-2 text-left transition-colors hover:bg-[#cfe6ff]"
      >
        <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md bg-oo-blue text-white">
          <svg width="14" height="14" viewBox="0 0 12 12" fill="none" aria-hidden="true">
            <circle cx="6" cy="2.5" r="1.8" stroke="currentColor" strokeWidth="1.2"/>
            <circle cx="2" cy="9.5" r="1.8" stroke="currentColor" strokeWidth="1.2"/>
            <circle cx="10" cy="9.5" r="1.8" stroke="currentColor" strokeWidth="1.2"/>
            <line x1="6" y1="4.3" x2="2.8" y2="7.7" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round"/>
            <line x1="6" y1="4.3" x2="9.2" y2="7.7" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round"/>
          </svg>
        </span>
        <span className="min-w-0 flex-1">
          <span className="block text-[13px] font-semibold text-[#16357a] leading-tight">
            {showDiagram ? "Hide ownership graph" : "Explore the ownership graph"}
          </span>
          <span className="block text-[11px] font-mono text-[#3a5a9a] truncate">
            {graphMeta}
          </span>
        </span>
        <svg width="14" height="14" viewBox="0 0 12 12" fill="none" aria-hidden="true"
          className={`shrink-0 text-[#16357a] transition-transform ${showDiagram ? "rotate-90" : ""}`}>
          <path d="M4.5 2.5 L8 6 L4.5 9.5" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round"/>
        </svg>
      </button>
      )}

      {/* Secondary drill-downs — quieter than the graph CTA. */}
      <div className={`flex flex-wrap gap-4 text-[11px] font-mono ${showGraphStrip ? "mt-2" : "mt-3"}`}>
        <button type="button" onClick={toggleStatements} aria-pressed={showStatements}
          className={`hover:underline ${showStatements ? "text-oo-blue" : "text-oo-muted hover:text-oo-ink"}`}>
          {showStatements ? "Hide statements" : (
            hasKnownCount ? `${stmtCount} statement${stmtCount === 1 ? "" : "s"}` : "Statements"
          )}
        </button>
        <button type="button" onClick={toggleJson} aria-pressed={showJson}
          className={`hover:underline ${showJson ? "text-oo-blue" : "text-oo-muted hover:text-oo-ink"}`}>
          {showJson ? "Hide JSON" : "Raw JSON"}
        </button>
      </div>

      {/* Expanded content */}
      {anyOpen && (
        <div className="mt-4 bg-oo-bg rounded-oo p-4 text-[12px]">
          {loading && <p className="text-oo-muted">Fetching…</p>}
          {error   && <p className="text-red-700">{error}</p>}
          {detail  && (
            <DeepenBlock
              detail={detail}
              entityName={hit.name}
              showDiagram={showDiagram}
              showStatements={showStatements}
              showJson={showJson}
            />
          )}
        </div>
      )}
    </li>
  );
}

// ---------------------------------------------------------------------
// SkeletonSourceCard — pulsing placeholder while a source is in flight
// ---------------------------------------------------------------------

export function SkeletonSourceCard() {
  return (
    <article className="bg-white border border-oo-rule rounded-oo animate-pulse" aria-hidden>
      <header className="px-5 py-3 border-b border-oo-rule flex items-start justify-between gap-3">
        <div className="h-4 bg-oo-rule rounded w-44" />
        <div className="h-3 bg-oo-rule rounded w-12 mt-0.5" />
      </header>
      <div className="px-5 py-4 space-y-2.5">
        <div className="h-3 bg-oo-rule rounded w-3/4" />
        <div className="h-3 bg-oo-rule rounded w-1/2" />
        <div className="h-3 bg-oo-rule rounded w-2/3" />
      </div>
    </article>
  );
}

// ---------------------------------------------------------------------
// SourceBucketCard — per-source result card
// ---------------------------------------------------------------------

// Sources that can show the entity-level Time Machine timeline.
const TIMELINE_SOURCES = new Set([
  "gleif",
  "companies_house",
  "nz_companies",
  "ariregister",
]);

export function SourceBucketCard({
  bucket,
  lei,
  riskByHit,
  bodsCountMap = {},
  bodsBreakdownMap = {},
  onRetry,
  retrying = false,
}: {
  bucket: SourceBucket;
  /** Resolved LEI for the current lookup — keys the Time Machine timeline. */
  lei?: string;
  riskByHit: Record<string, RiskSignal[]>;
  bodsCountMap?: Record<string, number>;
  bodsBreakdownMap?: Record<string, BodsBreakdown>;
  /** Re-run this source via /lookup-source — shown on error cards. */
  onRetry?: () => void;
  retrying?: boolean;
}) {
  const [showTimeline, setShowTimeline] = useState(false);
  // The Time Machine timeline is entity-level. Offer it on the sources that
  // contribute history (GLEIF + Companies House), keyed by the resolved LEI.
  // Fall back to the GLEIF hit_id (which is the LEI) if no lei prop is passed.
  const timelineLei =
    lei ??
    (bucket.sourceId === "gleif"
      ? (bucket.hits.find((h) => !h.is_stub) ?? bucket.hits[0])?.hit_id
      : undefined);
  const timelineName = bucket.hits[0]?.name;
  const showTimelineButton =
    TIMELINE_SOURCES.has(bucket.sourceId) && !bucket.error && !!timelineLei;

  // NZ-only enrichment: director/shareholder cross-company associations. The
  // nz_companies hit_id is the company number.
  const nzCompanyNumber =
    bucket.sourceId === "nz_companies" && !bucket.error
      ? (bucket.hits.find((h) => !h.is_stub) ?? bucket.hits[0])?.hit_id
      : undefined;

  // GLEIF-only enrichment: the subsidiary network (direct + ultimate children).
  // Keyed by the resolved LEI (the gleif hit_id is the LEI).
  const gleifLei =
    bucket.sourceId === "gleif" && !bucket.error
      ? (lei ?? (bucket.hits.find((h) => !h.is_stub) ?? bucket.hits[0])?.hit_id)
      : undefined;

  // Rendered inline with the entity title (right-aligned) on the first hit row.
  const timelineButton = showTimelineButton ? (
    <button
      type="button"
      onClick={() => setShowTimeline((v) => !v)}
      aria-pressed={showTimeline}
      className="inline-flex items-center gap-1.5 rounded-oo border border-oo-rule bg-white px-2.5 py-1 text-[11px] font-semibold text-oo-ink transition-colors hover:bg-oo-bg"
    >
      <svg width="12" height="12" viewBox="0 0 12 12" fill="none" aria-hidden="true">
        <circle cx="6" cy="6" r="4.5" stroke="currentColor" strokeWidth="1.2" />
        <path d="M6 3.5 V6 L7.8 7.2" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
      {showTimeline ? "Hide timeline" : "See timeline"}
    </button>
  ) : null;

  const stateLabel = bucket.error
    ? "error"
    : `${bucket.hits.length} result${bucket.hits.length === 1 ? "" : "s"}`;
  const stateColor = bucket.error
    ? "text-red-700"
    : "text-oo-muted";

  return (
    <>
    <article
      id={`oc-source-${bucket.sourceId}`}
      className="bg-white border border-oo-rule rounded-oo scroll-mt-24 transition-shadow"
    >
      <header className="px-5 py-3 border-b border-oo-rule flex items-start justify-between gap-3">
        <div className="min-w-0">
          <h3 className="font-head font-bold text-[15px] text-oo-ink">
            {bucket.sourceName}
          </h3>
        </div>
        {(() => {
          const firstHit = bucket.hits.find((h) => !h.is_stub);
          const bucketUrl = firstHit ? sourceEntityUrl(bucket.sourceId, firstHit) : null;
          return bucketUrl && !bucket.error ? (
            <a
              href={bucketUrl}
              target="_blank"
              rel="noopener noreferrer"
              className={`text-[11px] font-mono shrink-0 ${stateColor} hover:underline`}
            >
              {stateLabel}
            </a>
          ) : (
            <span className={`text-[11px] font-mono shrink-0 ${stateColor}`}>
              {stateLabel}
            </span>
          );
        })()}
      </header>
      {bucket.error && (
        <div className="px-5 py-3 flex flex-wrap items-center justify-between gap-3">
          <p className="text-[13px] text-red-700">{bucket.error}</p>
          {onRetry && (
            <button
              type="button"
              onClick={onRetry}
              disabled={retrying}
              className="shrink-0 rounded border border-red-300 px-3 py-1 text-[12px] font-semibold text-red-700 transition-colors hover:bg-red-50 disabled:opacity-50"
            >
              {retrying ? "Retrying…" : "Retry source"}
            </button>
          )}
        </div>
      )}
      {bucket.hits.length === 0 && !bucket.error && (
        <p className="px-5 py-3 text-[13px] text-oo-muted">No hits.</p>
      )}
      <ul className="divide-y divide-oo-rule">
        {bucket.hits.map((hit, idx) => (
          <HitRow
            key={`${hit.source_id}:${hit.hit_id}`}
            hit={hit}
            riskSignals={riskByHit[`${hit.source_id}:${hit.hit_id}`] ?? []}
            preloadedStmtCount={bodsCountMap[`${hit.source_id}:${hit.hit_id}`]}
            preloadedBreakdown={bodsBreakdownMap[`${hit.source_id}:${hit.hit_id}`]}
            titleAccessory={idx === 0 ? timelineButton : undefined}
          />
        ))}
      </ul>
      {nzCompanyNumber && (
        <div className="px-5 pb-4">
          <NzAssociations companyNumber={nzCompanyNumber} />
        </div>
      )}
      {gleifLei && (
        <div className="px-5 pb-4">
          <SubsidiaryNetwork lei={gleifLei} entityName={timelineName} />
        </div>
      )}
    </article>
    {showTimeline && timelineLei && (
      <HistoryTimeline lei={timelineLei} entityName={timelineName} />
    )}
    </>
  );
}
