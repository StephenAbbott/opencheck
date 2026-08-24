import { lazy, Suspense, useEffect, useId, useMemo, useState, useSyncExternalStore } from "react";
import { deepen } from "../../lib/api";
import { rowFinding } from "../../lib/sourceFinding";
import { graphPartiesLabel } from "../../lib/vocab";
import { ActionChip, Chip, DataTile, RowList } from "../ui";
import type { BodsBreakdown, BoAccessNotice, DeepenResponse, RiskSignal, SourceHit } from "../../lib/api";
import { RiskChip } from "../risk/RiskChip";
import { LivenessBadge, type SourceLiveness } from "./LivenessBadge";
import { HistoryTimeline } from "./HistoryTimeline";
import {
  annotatedFieldCount,
  annotationsAt,
  asFiledText,
  asFiledToggleLabel,
  getAsFiled,
  setAsFiled,
  subscribeAsFiled,
} from "../../lib/annotations";
import { mergeSignals } from "../../lib/expand";
import { scopeCrossSourceSignals } from "../../lib/signalScope";
import { NzAssociations } from "./NzAssociations";

// BodsGraphExplorer pulls in Cytoscape + cytoscape-dagre (~the bulk of the
// bundle) but only renders when a user clicks "Visualise". Code-split it so
// the initial page load never ships the graph engine.
const BodsGraphExplorer = lazy(() => import("../BodsGraphExplorer"));

export interface SourceBucket {
  sourceId: string;
  sourceName: string;
  hits: SourceHit[];
  error?: string;
  /** EU/EEA beneficial-ownership access notice for this register, if any. */
  boAccess?: BoAccessNotice | null;
}

// ---------------------------------------------------------------------
// BoAccessFootnote — quiet, informational note that a national register's
// beneficial ownership data is (or will soon be) restricted to legitimate-
// interest access. Deliberately low-key (no fill, muted text, info icon —
// never amber/red, which OpenCheck reserves for licence + risk): the company
// registration and GLEIF ownership data shown above are unaffected.
// ---------------------------------------------------------------------

function formatAccessDate(iso: string): string {
  const d = new Date(iso + "T00:00:00");
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleDateString("en-GB", { day: "numeric", month: "long", year: "numeric" });
}

function BoAccessFootnote({ notice }: { notice: BoAccessNotice }) {
  const { status, country_name, effective_date, access_url } = notice;
  const sentence =
    status === "becoming_restricted" && effective_date
      ? `Beneficial ownership data from ${country_name} is currently public but will be restricted to certain groups from ${formatAccessDate(effective_date)}.`
      : `Beneficial ownership data from ${country_name} is not public — available to certain groups only.`;
  const linkText =
    status === "becoming_restricted"
      ? "Learn how to apply for access after then"
      : "Learn how to apply for access";
  return (
    <div className="px-5 pb-4 flex gap-2 items-start">
      <svg width="14" height="14" viewBox="0 0 16 16" fill="none" aria-hidden="true"
        className="text-oo-muted mt-0.5 shrink-0">
        <circle cx="8" cy="8" r="6.5" stroke="currentColor" strokeWidth="1.2" />
        <path d="M8 7.2V11" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" />
        <circle cx="8" cy="5" r="0.7" fill="currentColor" />
      </svg>
      <p className="text-[12px] leading-[1.6] text-oo-muted">
        {sentence}
        {access_url && (
          <>
            {" "}
            <a
              href={access_url}
              target="_blank"
              rel="noopener noreferrer"
              className="text-oo-blue underline hover:text-oo-burst"
            >
              {linkText}
              <span className="sr-only"> (opens in new tab)</span>
            </a>
          </>
        )}
      </p>
    </div>
  );
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
    case "ted_eu": {
      const notices = raw.notices as { url?: string }[] | undefined;
      return notices?.[0]?.url || "https://ted.europa.eu/";
    }
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

/** Subscribes to the shared "as filed" setting (see lib/annotations). */
function useAsFiled(): boolean {
  return useSyncExternalStore(subscribeAsFiled, getAsFiled, getAsFiled);
}

/**
 * A field value that has something the register said behind it.
 *
 * When "as filed" is off this is exactly the previous rendering plus a dotted
 * underline, so the affordance is discoverable without being noisy. When on,
 * the register's words lead and OpenCheck's value follows in muted text —
 * never the other way round, because the whole point is to show whose
 * vocabulary you are reading.
 */
function AnnotatedValue({
  value,
  annotations,
}: {
  value: React.ReactNode;
  annotations: { description?: string }[];
}) {
  const asFiled = useAsFiled();
  const filed = asFiledText(annotations);
  if (!filed) return <>{value}</>;
  if (!asFiled) {
    return (
      <span className="underline decoration-dotted decoration-oo-muted/60 underline-offset-2">
        {value}
        <span className="sr-only"> — as filed: {filed}</span>
      </span>
    );
  }
  return (
    <span className="block">
      <span className="text-oo-ink">{filed}</span>
      <span className="block text-[10px] text-oo-muted mt-0.5">
        OpenCheck reads this as {value}
      </span>
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
          <summary className="text-[10px] text-oo-muted cursor-pointer">
            Raw statement JSON —{" "}
            <span className="font-mono">
              {statementId ? statementId.slice(0, 28) + "…" : "Statement ID"}
            </span>
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
        {birthDate && (
          <FieldRow
            label="Born"
            value={
              <AnnotatedValue
                value={birthDate}
                annotations={annotationsAt(stmt, "recordDetails", "birthDate")}
              />
            }
            mono
          />
        )}
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
          <summary className="text-[10px] text-oo-muted cursor-pointer">
            Raw statement JSON —{" "}
            <span className="font-mono">
              {statementId ? statementId.slice(0, 28) + "…" : "Statement ID"}
            </span>
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
                  // The register's own nature-of-control code hangs off the
                  // interest's `type`, which is what describeInterest renders.
                  const anns = annotationsAt(
                    stmt, "recordDetails", "interests", i, "type",
                  );
                  return (
                    <span key={i} className="block">
                      <AnnotatedValue value={desc} annotations={anns} />
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
          <summary className="text-[10px] text-oo-muted cursor-pointer">
            Raw statement JSON —{" "}
            <span className="font-mono">
              {statementId ? statementId.slice(0, 28) + "…" : "Statement ID"}
            </span>
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
  const asFiled = useAsFiled();
  // Only offer the toggle where there is something to toggle. Most sources
  // annotate nothing — a control that changes nothing is worse than none.
  const annotated = annotatedFieldCount(statements);

  return (
    <div className="space-y-2 mt-2">
      {annotated > 0 && (
        <div className="flex items-center justify-between gap-2 text-[10px]">
          <span className="text-oo-muted">
            {asFiled
              ? "Showing the register's own words"
              : "Dotted underline marks a value OpenCheck transformed"}
          </span>
          <button
            type="button"
            className="hover:text-oo-blue text-oo-muted underline underline-offset-2 shrink-0"
            aria-pressed={asFiled}
            onClick={() => setAsFiled(!asFiled)}
          >
            {asFiledToggleLabel(asFiled, annotated)}
          </button>
        </div>
      )}
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
            <summary className="text-oo-muted cursor-pointer">
              Raw statement JSON —{" "}
              <span className="font-mono">{type || "unknown"} statement</span>
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
  subjectSignals = [],
}: {
  detail: DeepenResponse;
  entityName?: string;
  showDiagram?: boolean;
  showStatements?: boolean;
  showJson?: boolean;
  /** The lookup's top-level risk signals. Only the cross-source (`RELATED_*`)
   *  ones that land on a statement in THIS bundle are passed to the graph —
   *  see lib/signalScope.ts. Defaults to none, so a call site that has no
   *  subject-level list (EsgPanel) behaves exactly as before. */
  subjectSignals?: RiskSignal[];
}) {
  const anyVisible = showDiagram || showStatements || showJson;

  // Cross-source signals are computed against the MERGED bundle and ride on
  // the top-level risk_signals event, so they never reach a /deepen response.
  // Without this, a node the risk panel calls sanctions-linked renders here
  // with no badge at all — which reads as "checked and clean" (Phase 109).
  const crossSourceSignals = useMemo(
    () => scopeCrossSourceSignals(subjectSignals, detail.bods),
    [subjectSignals, detail.bods],
  );
  const graphSignals = useMemo(
    () =>
      crossSourceSignals.length
        ? mergeSignals(detail.risk_signals, crossSourceSignals)
        : detail.risk_signals,
    [detail.risk_signals, crossSourceSignals],
  );

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
            Ownership diagram — {detail.bods.length} record{detail.bods.length === 1 ? "" : "s"} mapped
          </h4>
          {detail.bods_issues.length > 0 && (
            <p className="text-amber-800 mb-2">
              {detail.bods_issues.length} validation issue{detail.bods_issues.length === 1 ? "" : "s"}
            </p>
          )}
          {/* Say where the extra badges came from. A cross-source badge
              appearing on a single source's graph is otherwise unexplained,
              which invites the mirror image of the confusion being fixed. */}
          {crossSourceSignals.length > 0 && (
            <p className="text-[11px] text-oo-muted mb-2 leading-[1.5]">
              Includes {crossSourceSignals.length} related-party signal
              {crossSourceSignals.length === 1 ? "" : "s"} from the risk panel above — found by
              screening this network against other sources, not reported by this source.
            </p>
          )}
          <Suspense
            fallback={
              <div
                className="h-48 rounded-oo border border-oo-rule bg-oo-bg/40 motion-safe:animate-pulse flex items-center justify-center text-[12px] text-oo-muted"
                role="status"
              >
                Loading graph…
              </div>
            }
          >
            <BodsGraphExplorer statements={detail.bods} signals={graphSignals} entityName={entityName} />
          </Suspense>
        </section>
      )}

      {showStatements && detail.bods.length > 0 && (
        <section>
          <h4 className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-2">
            What this source published
          </h4>
          <BODSStatementCards statements={detail.bods as BODSStmt[]} />
        </section>
      )}

      {showJson && (
        <section className="space-y-3">
          {detail.bods.length > 0 && (
            <div>
              <h4 className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-1.5">
                Mapped records (BODS v0.4)
              </h4>
              <pre className="max-h-80 overflow-auto bg-white border border-oo-rule rounded-oo p-3 text-[10px]">
                {JSON.stringify(detail.bods, null, 2)}
              </pre>
            </div>
          )}
          <div>
            <h4 className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-1.5">
              The source's original response
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

/**
 * MentionsBreakdown — which archives mention this entity, and how often.
 *
 * OpenAleph 5.3 `/mentions` reports a total alongside only a page of sample
 * documents, so the counts here come from the `collection_id` **facet** —
 * exact across every mention, not extrapolated from the sample (issue #23).
 * A category breakdown was the original idea, but the flagship's category
 * facet is empty and per-document categories are near-uniformly "library";
 * collection names ("Epstein Estate documents…") are exact *and* meaningful.
 *
 * Strictly informational: mentions are name-derived, never identifier
 * corroboration, so nothing here feeds a risk signal.
 */
function MentionsBreakdown({ hit }: { hit: SourceHit }) {
  const [expanded, setExpanded] = useState(false);
  const listId = useId();
  const mentions = (hit.raw as Record<string, unknown> | undefined)
    ?.openaleph_mentions as
    | {
        total?: number;
        collections?: { label: string; count: number }[];
        documents?: { title: string; collection: string; url: string }[];
      }
    | undefined;
  const collections = mentions?.collections ?? [];
  const total = mentions?.total ?? 0;
  if (!total || collections.length === 0) return null;

  const preview = expanded ? collections : collections.slice(0, 3);

  // A list, not a row of chips. Which archives a name appears in, and how
  // often, is the finding for this source — a chip strip compresses it into
  // decoration and truncates the collection names that carry the meaning
  // ("Panama Papers" and "Paradise Papers (Appleby)" are not interchangeable).
  return (
    <RowList
      controls={listId}
      total={collections.length}
      expanded={expanded}
      onToggle={() => setExpanded((v) => !v)}
      moreLabel="archive"
      items={preview.map((c) => ({
        key: c.label,
        title: <span className="text-oo-ink">{c.label}</span>,
        meta: (
          <>
            {c.count} of {total}
            <span className="sr-only"> mentions are in “{c.label}”</span>
          </>
        ),
      }))}
      footnote="Documents mentioning this name — informational only, not a match on identifiers."
    />
  );
}

/**
 * TedAwardsList — EU procurement award notices from TED (ted_eu source).
 *
 * Shows the notices where the entity appears as a tenderer/winner, with the
 * per-notice role resolved from the eForms XML winner chain ("won" /
 * "tendered"; "unknown" when the chain could not be resolved). Two coverage
 * disclosures are mandatory: TED identifier search only covers the eForms
 * era (≈2024 onwards), and awards won via subsidiaries sit under the
 * subsidiary's own identifier — absence is not evidence of no contracts.
 */
function TedAwardsList({ hit }: { hit: SourceHit }) {
  const [expanded, setExpanded] = useState(false);
  const listId = useId();
  if (hit.source_id !== "ted_eu") return null;
  const raw = hit.raw as Record<string, unknown> | undefined;
  const notices = (raw?.notices ?? []) as {
    publication_number: string;
    publication_date?: string;
    title?: string;
    buyer_name?: string;
    buyer_country?: string;
    total_value?: number | string | null;
    currency?: string;
    role?: string;
    url?: string;
  }[];
  const total = (raw?.total_notice_count as number) ?? notices.length;
  if (!notices.length) return null;

  const shown = expanded ? notices : notices.slice(0, 5);
  // `ui/Chip` rather than three hand-rolled spans in raw palette classes. The
  // tones say what the row asserts: `ok` for a confirmed win, `neutral` for a
  // bid, `warn` for a winner chain that could not be resolved — which is
  // "we do not know", not "they lost".
  const roleBadge = (role?: string) =>
    role === "won" ? (
      <Chip tone="ok" size="sm">won</Chip>
    ) : role === "tendered" ? (
      <Chip tone="neutral" size="sm">tendered</Chip>
    ) : (
      <Chip tone="warn" size="sm">unconfirmed</Chip>
    );

  return (
    <RowList
      controls={listId}
      total={notices.length}
      expanded={expanded}
      onToggle={() => setExpanded((v) => !v)}
      moreLabel="notice"
      items={shown.map((n) => ({
        key: n.publication_number,
        title: (
          <a
            href={n.url}
            target="_blank"
            rel="noopener noreferrer"
            className="text-oo-blue hover:underline underline-offset-2"
          >
            {n.title || `Notice ${n.publication_number}`}
            <span className="sr-only"> (opens in new tab)</span>
          </a>
        ),
        meta: (
          // Wrapping, and gap-y so a wrapped status chip does not collide
          // with the line above it. Buyer + value + date + chip is a long
          // string; on a phone it has to be allowed to take two lines.
          <span className="inline-flex flex-wrap items-baseline gap-x-2 gap-y-1">
            {[
              n.buyer_name &&
                `${n.buyer_name}${n.buyer_country ? ` (${n.buyer_country})` : ""}`,
              n.total_value &&
                `${Number(n.total_value).toLocaleString()} ${n.currency || ""}`.trim(),
              n.publication_date,
            ]
              .filter(Boolean)
              .join(" · ")}
            {roleBadge(n.role)}
          </span>
        ),
      }))}
      footnote={
        <>
          {total > notices.length && (
            <>
              {total} notices in total — showing the latest {notices.length}.{" "}
            </>
          )}
          eForms-era notices only (≈2024 onwards) — earlier awards are not
          searchable by identifier. Awards won via subsidiaries appear under
          the subsidiary&apos;s identifier.
        </>
      }
    />
  );
}

/**
 * The finding line. Lives here rather than inline so the fallback chain is
 * testable — see `lib/sourceFinding.ts`, which is where the reasoning is.
 */
function HitFinding({ hit }: { hit: SourceHit }) {
  const finding = rowFinding(hit);
  if (!finding) return null;
  return (
    <>
      <p
        className={`text-[13px] mt-1 leading-[1.6] ${
          finding.sub ? "text-oo-ink" : "text-oo-muted"
        }`}
      >
        {finding.lead}
      </p>
      {finding.sub && (
        <p className="text-[12px] text-oo-muted mt-0.5 leading-[1.5]">{finding.sub}</p>
      )}
    </>
  );
}

function HitRow({
  hit,
  riskSignals,
  subjectSignals = [],
  preloadedStmtCount,
  preloadedBreakdown,
  titleAccessory,
}: {
  hit: SourceHit;
  riskSignals: RiskSignal[];
  /** The lookup's full top-level signal list — scoped per bundle in
   *  DeepenBlock so cross-source findings can badge this graph too. */
  subjectSignals?: RiskSignal[];
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
  const panelId = useId();

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
        persons: detail.bods.filter((s) => (s as Record<string, unknown>).recordType === "person").length,
        relationships: detail.bods.filter((s) => (s as Record<string, unknown>).recordType === "relationship").length,
      }
    : preloadedBreakdown;
  const graphMeta = breakdown
    ? `${graphPartiesLabel(breakdown.entities, breakdown.persons ?? 0)} · ${breakdown.relationships} ${breakdown.relationships === 1 ? "relationship" : "relationships"}`
    : "Interactive ownership & control graph";

  // The chip is labelled by what is in the diagram — "14 entities" tells a
  // reader whether opening it is worth the click, where "Explore the
  // ownership graph" does not. Falls back to the neutral noun while the count
  // is still unknown rather than inventing one.
  const partyLabel = breakdown
    ? graphPartiesLabel(breakdown.entities, breakdown.persons ?? 0)
    : "Diagram";

  // One disclosure replaces v1's two mono text links. It starts open when a
  // tile's content is already showing, so the drawer never hides what is on
  // screen — that would leave a graph visible with no control to close it.
  const [dataOpen, setDataOpen] = useState(false);
  const drawerId = `${panelId}-drawer`;
  // Closing the drawer while a tile's content is open would hide the only
  // control for it — the tiles are the sole toggles for records and JSON, and
  // the row has no chip for either. So closing also closes what it opened.
  const toggleData = () => {
    setDataOpen((open) => {
      if (open) {
        setShowStatements(false);
        setShowJson(false);
        setShowDiagram(false);
      }
      return !open;
    });
  };
  useEffect(() => {
    if (showDiagram || showStatements || showJson) setDataOpen(true);
  }, [showDiagram, showStatements, showJson]);

  return (
    <li className="px-5 py-4">
      {/* Entity name (+ optional title accessory, e.g. See timeline), summary, risk chips */}
      <div className="flex items-start justify-between gap-3">
        <div className="font-head font-bold text-[15px] text-oo-ink leading-snug min-w-0">
          {(() => {
            const url = sourceEntityUrl(hit.source_id, hit);
            return url ? (
              <a href={url} target="_blank" rel="noopener noreferrer" className="underline decoration-dotted underline-offset-2">
                {hit.name}
                <span className="sr-only"> (opens in new tab)</span>
              </a>
            ) : hit.name;
          })()}
          {hit.is_stub && (
            <span className="ml-2 text-[11px] font-mono bg-amber-50 text-amber-800 border border-amber-200 rounded px-1.5 py-0.5">
              stub
            </span>
          )}
        </div>
      </div>
      {/* Phase 122: lead with what the source said, not with what it is
          called. `finding` is a sentence built by the adapter from fields it
          already parsed; `summary` is the identifier fragment it has always
          been ("GB · registered entity"). A source with no template yet
          falls back to the fragment, so the migration can land one adapter
          at a time without any row looking broken. */}
      <HitFinding hit={hit} />
      <MentionsBreakdown hit={hit} />
      <TedAwardsList hit={hit} />
      {riskSignals.length > 0 && (
        <div className="mt-2 flex flex-wrap gap-1">
          {riskSignals.map((sig, i) => (
            <RiskChip key={`${sig.code}-${i}`} signal={sig} compact />
          ))}
        </div>
      )}

      {/* The v2 action group: three equal-weight chips labelled by what they
          contain. v1 gave the graph a full-width blue strip and demoted the
          rest to 11px mono links, which is the data model as the interface. */}
      <div className="mt-3 flex flex-wrap items-center gap-2">
        {showGraphStrip && (
          <ActionChip
            onClick={toggleDiagram}
            expanded={showDiagram}
            controls={panelId}
            tone="network"
            icon={
              <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                <circle cx="6" cy="6" r="2.3" /><circle cx="18" cy="6" r="2.3" />
                <circle cx="12" cy="18" r="2.3" />
                <path d="M8 7.5 10.7 15.6M16 7.5 13.3 15.6M8.5 6h7" />
              </svg>
            }
          >
            {showDiagram ? "Hide diagram" : partyLabel}
          </ActionChip>
        )}
        {titleAccessory}
        <ActionChip
          onClick={toggleData}
          expanded={dataOpen}
          controls={drawerId}
        >
          Data
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor"
            strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"
            className={`transition-transform ${dataOpen ? "rotate-180" : ""}`}>
            <path d="m6 9 6 6 6-6" />
          </svg>
        </ActionChip>
      </div>

      {/* The Data drawer: a menu of what this source holds, in human clauses.
          Each tile opens the thing it names; the two that only describe what a
          download contains are not buttons. */}
      {dataOpen && (
        <div id={drawerId} className="mt-3 border-t border-oo-rule pt-3">
          <div className="grid gap-2.5" style={{ gridTemplateColumns: "repeat(auto-fit, minmax(210px, 1fr))" }}>
            {showGraphStrip && (
              <DataTile
                title="Ownership diagram"
                meta={graphMeta}
                action="Open diagram →"
                tone="network"
                open={showDiagram}
                onClick={toggleDiagram}
                controls={panelId}
              />
            )}
            <DataTile
              title="Structured records"
              meta={hasKnownCount ? `${stmtCount} statement${stmtCount === 1 ? "" : "s"}, BODS v0.4` : "Mapped to BODS v0.4"}
              action="Open records →"
              open={showStatements}
              onClick={toggleStatements}
              controls={panelId}
            />
            <DataTile
              title="Original response"
              meta="JSON, as the source sent it"
              action="Open response →"
              open={showJson}
              onClick={toggleJson}
              controls={panelId}
            />
          </div>
        </div>
      )}

      {/* Expanded content */}
      {anyOpen && (
        <div id={panelId} className="mt-4 bg-oo-bg rounded-oo p-4 text-[12px]">
          {loading && <p className="text-oo-muted" role="status">Fetching…</p>}
          {error   && <p className="text-red-700" role="alert">{error}</p>}
          {detail  && (
            <DeepenBlock
              detail={detail}
              entityName={hit.name}
              showDiagram={showDiagram}
              showStatements={showStatements}
              showJson={showJson}
              subjectSignals={subjectSignals}
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
    <>
      <span role="status" className="sr-only">Source result still loading</span>
      <article className="bg-white border border-oo-rule rounded-oo motion-safe:animate-pulse" aria-hidden>
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
    </>
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
  "cvr_denmark",
]);

export function SourceBucketCard({
  bucket,
  lei,
  riskByHit,
  subjectSignals = [],
  bodsCountMap = {},
  bodsBreakdownMap = {},
  onRetry,
  retrying = false,
  footnote,
  liveness,
  extra,
}: {
  bucket: SourceBucket;
  /** Resolved LEI for the current lookup — keys the Time Machine timeline. */
  lei?: string;
  riskByHit: Record<string, RiskSignal[]>;
  /** The lookup's full top-level signal list. `riskByHit` only carries the
   *  signals attributed to a hit in this bucket; cross-source (`RELATED_*`)
   *  findings are assessed against the merged bundle and are keyed to a
   *  statement, not a hit — so they need the whole list to be scoped from. */
  subjectSignals?: RiskSignal[];
  bodsCountMap?: Record<string, number>;
  bodsBreakdownMap?: Record<string, BodsBreakdown>;
  /** Re-run this source via /lookup-source — shown on error cards. */
  onRetry?: () => void;
  retrying?: boolean;
  /** Caption rendered inside the card footer (e.g. subsidiary truncation note). */
  footnote?: string;
  /** A band rendered inside the card, below the rows — for content that
   *  belongs to the source rather than to any one of its results (OpenAleph's
   *  archive matches). It used to be a second white card stacked underneath,
   *  which read as an unrelated widget that happened to be adjacent. */
  extra?: React.ReactNode;
  /** How current this source's payload is — badged in the header when it is
   *  anything other than a fresh live call. */
  liveness?: SourceLiveness;
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

  // Rendered inline with the entity title (right-aligned) on the first hit row.
  // Sits in the row's action group beside the diagram chip, not floating at
  // the top-right of the first hit. The mockup labels it with a change count;
  // that count only exists once HistoryTimeline has fetched, so the chip says
  // what it opens until then rather than inventing a number.
  const timelineButton = showTimelineButton ? (
    <ActionChip
      onClick={() => setShowTimeline((v) => !v)}
      expanded={showTimeline}
      controls={`oc-timeline-${bucket.sourceId}`}
      tone="timeline"
      icon={
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor"
          strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
          <path d="M3 12a9 9 0 1 0 3-6.7L3 8" /><path d="M3 4v4h4" /><path d="M12 8v4l3 2" />
        </svg>
      }
    >
      {showTimeline ? "Hide changes" : "Changes over time"}
    </ActionChip>
  ) : null;

  return (
    <>
    <article
      id={`oc-source-${bucket.sourceId}`}
      className={`rounded-oo border scroll-mt-24 transition-shadow ${
        bucket.error ? "border-oo-warn-border bg-oo-warn-bg" : "border-oo-rule bg-white"
      }`}
    >
      <header className="px-5 py-3 border-b border-oo-rule flex items-start justify-between gap-3">
        {/* Freshness sits inline beside the name (Phase 126), not on its own
            line beneath it: the v2 design puts every row's freshness on the
            same baseline so the column can be scanned in one pass. */}
        <div className="min-w-0 flex flex-wrap items-center gap-2.5">
          <h3 className="font-head font-bold text-oo-lead text-oo-ink">
            {bucket.sourceName}
          </h3>
          {!bucket.error && <LivenessBadge info={liveness} />}
          {bucket.error && (
            <span className="rounded-full border border-oo-warn-border bg-white px-2.5 py-0.5 text-oo-meta font-medium text-oo-warn-text">
              Did not answer
            </span>
          )}
        </div>
        {/* No "N results" in the corner. It counted rows a reader can see, in
            the data-model word this phase set out to remove, and its only
            other job — linking to the record at source — is already done by
            each row's own name. The row's action group is what belongs on
            this side, and that is where it is. */}
      </header>
      {bucket.error && (
        <div className="px-5 py-3 flex flex-wrap items-center justify-between gap-3">
          <p className="text-oo-small text-oo-warn-text" role="alert">
            Did not answer — {bucket.error}. Nothing was checked here, so treat
            this source as unknown rather than clear.
          </p>
          {onRetry && (
            <button
              type="button"
              onClick={onRetry}
              disabled={retrying}
              className="shrink-0 rounded-oo border border-oo-warn-border px-3 py-1.5 text-oo-small font-semibold text-oo-warn-text transition-colors hover:bg-oo-warn-bg disabled:opacity-50"
            >
              {retrying ? <span role="status">Retrying…</span> : "Try again"}
            </button>
          )}
        </div>
      )}
      {bucket.hits.length === 0 && !bucket.error && (
        <p className="px-5 py-3 text-oo-small text-oo-muted">No results.</p>
      )}
      <ul className="divide-y divide-oo-rule">
        {bucket.hits.map((hit, idx) => (
          <HitRow
            key={`${hit.source_id}:${hit.hit_id}`}
            hit={hit}
            riskSignals={riskByHit[`${hit.source_id}:${hit.hit_id}`] ?? []}
            subjectSignals={subjectSignals}
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
      {extra}
      {bucket.boAccess && !bucket.error && (
        <BoAccessFootnote notice={bucket.boAccess} />
      )}
      {footnote && (
        <p className="px-5 py-2.5 border-t border-oo-rule text-[12px] text-oo-muted">
          {footnote}
        </p>
      )}
    </article>
    {showTimeline && timelineLei && (
      <div id={`oc-timeline-${bucket.sourceId}`}>
        <HistoryTimeline lei={timelineLei} entityName={timelineName} />
      </div>
    )}
    </>
  );
}
