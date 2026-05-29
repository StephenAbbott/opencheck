import { useState } from "react";
import BODSGraph from "../BODSGraph";
import { deepen } from "../../lib/api";
import type { DeepenResponse, RiskSignal, SourceHit } from "../../lib/api";
import { RiskChip } from "../risk/RiskChip";

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
// DeepenBlock — shows BODS graph + raw JSON after "Go deeper"
// ---------------------------------------------------------------------

export function DeepenBlock({ detail }: { detail: DeepenResponse }) {
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
      {detail.bods.length > 0 && (
        <section>
          <h4 className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-2">
            BODS · {detail.bods.length} statement
            {detail.bods.length === 1 ? "" : "s"}
          </h4>
          {detail.bods_issues.length > 0 && (
            <p className="text-amber-800 mb-2">
              {detail.bods_issues.length} validation issue
              {detail.bods_issues.length === 1 ? "" : "s"}
            </p>
          )}
          <BODSGraph statements={detail.bods} signals={detail.risk_signals} />
          <BODSStatementCards statements={detail.bods as BODSStmt[]} />
          <details className="mt-3">
            <summary className="text-oo-muted cursor-pointer text-[11px] font-mono">
              Show raw JSON statements
            </summary>
            <pre className="mt-1 max-h-96 overflow-auto bg-white border border-oo-rule rounded-oo p-3 text-[10px]">
              {JSON.stringify(detail.bods, null, 2)}
            </pre>
          </details>
        </section>
      )}
      <section>
        <h4 className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-2">
          Raw source payload
        </h4>
        <pre className="max-h-96 overflow-auto bg-white border border-oo-rule rounded-oo p-3 text-[10px]">
          {JSON.stringify(detail.raw, null, 2)}
        </pre>
      </section>
    </div>
  );
}

// ---------------------------------------------------------------------
// HitRow — single result row with drill-down
// ---------------------------------------------------------------------

export function HitRow({
  hit,
  riskSignals,
}: {
  hit: SourceHit;
  riskSignals: RiskSignal[];
}) {
  const [open, setOpen] = useState(false);
  const [detail, setDetail] = useState<DeepenResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function toggle() {
    const next = !open;
    setOpen(next);
    if (next && !detail && !loading) {
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
  }

  return (
    <li className="px-5 py-4">
      <div className="flex justify-between items-baseline gap-4">
        <div className="min-w-0">
          <div className="font-head font-bold text-[15px] text-oo-ink leading-snug">
            {hit.name}
            {hit.is_stub && (
              <span className="ml-2 text-[11px] font-mono bg-amber-50 text-amber-800 border border-amber-200 rounded px-1.5 py-0.5">
                stub
              </span>
            )}
          </div>
          <p className="text-[13px] text-oo-muted mt-1 leading-[1.6]">
            {hit.summary}
          </p>
          {Object.keys(hit.identifiers).length > 0 && (
            <p className="text-[11px] text-oo-muted mt-1.5 font-mono break-all">
              {Object.entries(hit.identifiers)
                .map(([k, v]) => `${k}=${v}`)
                .join(" · ")}
            </p>
          )}
          {riskSignals.length > 0 && (
            <div className="mt-2 flex flex-wrap gap-1">
              {riskSignals.map((sig, i) => (
                <RiskChip key={`${sig.code}-${i}`} signal={sig} compact />
              ))}
            </div>
          )}
        </div>
        <button
          onClick={toggle}
          className="text-[12px] font-mono text-oo-blue hover:text-oo-burst whitespace-nowrap"
        >
          {open ? "Hide" : "Go deeper →"}
        </button>
      </div>

      {open && (
        <div className="mt-4 bg-oo-bg rounded-oo p-4 text-[12px]">
          {loading && <p className="text-oo-muted">Fetching…</p>}
          {error && <p className="text-red-700">{error}</p>}
          {detail && <DeepenBlock detail={detail} />}
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

export function SourceBucketCard({
  bucket,
  riskByHit,
  sourceSignals = [],
}: {
  bucket: SourceBucket;
  riskByHit: Record<string, RiskSignal[]>;
  sourceSignals?: RiskSignal[];
}) {
  const stateLabel = bucket.error
    ? "error"
    : `${bucket.hits.length} result${bucket.hits.length === 1 ? "" : "s"}`;
  const stateColor = bucket.error
    ? "text-red-700"
    : "text-oo-muted";

  const headerSignals = sourceSignals;

  return (
    <article className="bg-white border border-oo-rule rounded-oo">
      <header className="px-5 py-3 border-b border-oo-rule flex items-start justify-between gap-3">
        <div className="min-w-0">
          <h3 className="font-head font-bold text-[15px] text-oo-ink">
            {bucket.sourceName}
          </h3>
          {headerSignals.length > 0 && (
            <div className="mt-1.5 flex flex-wrap gap-1">
              {headerSignals.map((sig, i) => (
                <RiskChip key={`${sig.code}-${i}`} signal={sig} compact />
              ))}
            </div>
          )}
        </div>
        <span className={`text-[11px] font-mono shrink-0 ${stateColor}`}>
          {stateLabel}
        </span>
      </header>
      {bucket.error && (
        <p className="px-5 py-3 text-[13px] text-red-700">{bucket.error}</p>
      )}
      {bucket.hits.length === 0 && !bucket.error && (
        <p className="px-5 py-3 text-[13px] text-oo-muted">No hits.</p>
      )}
      <ul className="divide-y divide-oo-rule">
        {bucket.hits.map((hit) => (
          <HitRow
            key={`${hit.source_id}:${hit.hit_id}`}
            hit={hit}
            riskSignals={riskByHit[`${hit.source_id}:${hit.hit_id}`] ?? []}
          />
        ))}
      </ul>
    </article>
  );
}
