import { useState } from "react";
import { getNzAssociations } from "../../lib/api";
import type {
  NzAssociatedCompany,
  NzAssociationsResponse,
  NzPersonAssociations,
} from "../../lib/api";

// ---------------------------------------------------------------------
// Small building blocks
// ---------------------------------------------------------------------

function RoleChip({ role }: { role: string }) {
  const label = role === "director" ? "Director" : role === "shareholder" ? "Shareholder" : role;
  return (
    <span className="text-[10px] font-mono rounded px-1.5 py-0.5 border border-oo-rule bg-oo-bg text-oo-muted">
      {label}
    </span>
  );
}

function ConfidenceChip({ confidence, basis }: { confidence: string; basis: string }) {
  const style =
    confidence === "high"
      ? "bg-blue-600 text-white border-blue-600"
      : confidence === "medium"
        ? "bg-blue-50 text-blue-700 border-blue-200"
        : "bg-amber-50 text-amber-700 border-amber-200";
  const label =
    confidence === "high" ? "High" : confidence === "medium" ? "Medium" : "Name only";
  return (
    <span
      title={basis}
      className={`text-[10px] font-semibold rounded px-1.5 py-0.5 border ${style}`}
    >
      {label} · {basis}
    </span>
  );
}

function CompanyRow({ c }: { c: NzAssociatedCompany }) {
  const name = c.name || `Company ${c.number}`;
  return (
    <li className="flex items-start justify-between gap-3 py-1.5 border-t border-oo-rule first:border-t-0">
      <div className="min-w-0">
        <div className="text-[12px] text-oo-ink leading-snug">
          {c.link ? (
            <a href={c.link} target="_blank" rel="noopener noreferrer" className="hover:underline">
              {name}
            </a>
          ) : (
            name
          )}
        </div>
        <div className="mt-0.5 flex flex-wrap items-center gap-1">
          {c.roles.map((r) => (
            <RoleChip key={r} role={r} />
          ))}
          {c.share_percentage != null && c.roles.includes("shareholder") && (
            <span className="text-[10px] font-mono text-oo-muted">{c.share_percentage}%</span>
          )}
        </div>
      </div>
      <div className="shrink-0">
        <ConfidenceChip confidence={c.confidence} basis={c.basis} />
      </div>
    </li>
  );
}

// ---------------------------------------------------------------------
// Per-person summary + drill-down
// ---------------------------------------------------------------------

function PersonRow({ p }: { p: NzPersonAssociations }) {
  const [open, setOpen] = useState(false);
  const linked = p.other_company_count > 0;

  return (
    <li className="px-3 py-2.5">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="font-head font-bold text-[13px] text-oo-ink leading-snug">
            {p.name}
          </div>
          <div className="mt-0.5 flex flex-wrap items-center gap-1">
            {p.role_here.map((r) => (
              <RoleChip key={r} role={r} />
            ))}
            <span className="text-[10px] text-oo-muted">in this company</span>
          </div>
          <div className="mt-1.5 text-[12px] leading-[1.5]">
            {linked ? (
              <span className="text-oo-ink">
                Also in <strong>{p.other_company_count}</strong> other active{" "}
                {p.other_company_count === 1 ? "company" : "companies"}
                <span className="text-oo-muted">
                  {" "}
                  ({p.address_match_count} address-matched, {p.name_only_count} name-only)
                </span>{" "}
                <span className="text-oo-muted">
                  — {p.as_director} as director, {p.as_shareholder} as shareholder
                </span>
              </span>
            ) : (
              <span className="text-oo-muted">No other associations found.</span>
            )}
          </div>
          {p.truncated && (
            <div className="mt-0.5 text-[11px] text-oo-muted">
              {p.total_records_under_name} role records exist under this name — only a
              sample was checked (a common name may mix several people).
            </div>
          )}
        </div>
        {linked && (
          <button
            type="button"
            onClick={() => setOpen((v) => !v)}
            aria-pressed={open}
            className="shrink-0 text-[11px] font-mono text-oo-blue hover:underline"
          >
            {open ? "Hide" : "Show"}
          </button>
        )}
      </div>

      {open && linked && (
        <ul className="mt-2 rounded-oo border border-oo-rule bg-white px-3 py-1.5">
          {p.companies.map((c) => (
            <CompanyRow key={`${c.number}-${c.roles.join()}`} c={c} />
          ))}
        </ul>
      )}
    </li>
  );
}

// ---------------------------------------------------------------------
// Invitation strip — shown before the lookup and when collapsed again
// ---------------------------------------------------------------------

function InvitationStrip({ onClick }: { onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="mt-3 w-full flex items-center gap-3 rounded-oo border border-[#c7cdf0] bg-[#eef1fb] px-3 py-2 text-left transition-colors hover:bg-[#e6eafb]"
    >
      <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md bg-[#3d30d4] text-white">
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none" aria-hidden="true">
          <circle cx="5" cy="4.5" r="2" stroke="currentColor" strokeWidth="1.2" />
          <circle cx="10.5" cy="9.5" r="1.6" stroke="currentColor" strokeWidth="1.2" />
          <path d="M3 12 c0-2 1.6-3 3.5-3" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" />
        </svg>
      </span>
      <span className="min-w-0 flex-1">
        <span className="block text-[13px] font-semibold text-[#2a2382] leading-tight">
          Check director &amp; shareholder associations
        </span>
        <span className="block text-[11px] text-[#5b54a8]">
          Searches the NZ register for other companies each role holder is linked to · live lookup
        </span>
      </span>
    </button>
  );
}

// ---------------------------------------------------------------------
// NzAssociations — lazy "check associations" panel
// ---------------------------------------------------------------------

export function NzAssociations({ companyNumber }: { companyNumber: string }) {
  const [data, setData] = useState<NzAssociationsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [collapsed, setCollapsed] = useState(false);

  async function run() {
    if (loading || data) return;
    setLoading(true);
    setError(null);
    try {
      setData(await getNzAssociations(companyNumber));
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }

  // Layer 1 — invitation (nothing fires until clicked).
  if (!data && !loading && !error) {
    return <InvitationStrip onClick={run} />;
  }

  // Collapsed after viewing — back to the invitation strip; re-opening keeps
  // the already-fetched data (no second lookup).
  if (data && collapsed) {
    return <InvitationStrip onClick={() => setCollapsed(false)} />;
  }

  return (
    <section className="mt-3 rounded-oo border border-oo-rule bg-oo-bg p-3">
      {loading && <p className="text-[12px] text-oo-muted">Searching the NZ register…</p>}
      {error && <p className="text-[12px] text-red-700">{error}</p>}

      {data && !data.available && (
        <div className="flex items-start justify-between gap-3">
          <p className="text-[12px] text-oo-muted leading-[1.6]">
            Associations lookup isn’t available{data.reason ? ` (${data.reason})` : ""}.
          </p>
          <button
            type="button"
            onClick={() => setCollapsed(true)}
            className="shrink-0 text-[11px] font-mono text-oo-blue hover:underline"
          >
            Hide
          </button>
        </div>
      )}

      {data && data.available && (
        <>
          <div className="flex items-baseline justify-between gap-2">
            <h4 className="font-head font-bold text-[13px] text-oo-ink">
              Director &amp; shareholder associations
            </h4>
            <div className="flex items-baseline gap-3 shrink-0">
              <span className="text-[11px] font-mono text-oo-muted">
                {data.people.filter((p) => p.other_company_count > 0).length} of {data.checked} linked
              </span>
              <button
                type="button"
                onClick={() => setCollapsed(true)}
                className="text-[11px] font-mono text-oo-blue hover:underline"
              >
                Hide
              </button>
            </div>
          </div>
          <p className="mt-1 text-[11px] text-oo-muted leading-[1.5]">
            Every name match from the public register is shown; a matching registered address
            upgrades a match to <span className="text-blue-700 font-semibold">address-matched</span>,
            otherwise it is <span className="text-amber-700 font-semibold">name-only</span> and may
            be a different person who shares the name. For review, not a determination.
          </p>

          {data.people.length === 0 ? (
            <p className="mt-2 text-[12px] text-oo-muted">No directors or shareholders to check.</p>
          ) : (
            <ul className="mt-2 divide-y divide-oo-rule rounded-oo border border-oo-rule bg-white">
              {data.people.map((p) => (
                <PersonRow key={p.name} p={p} />
              ))}
            </ul>
          )}
          {data.not_checked > 0 && (
            <p className="mt-2 text-[11px] text-oo-muted">
              + {data.not_checked} more role {data.not_checked === 1 ? "holder" : "holders"} not
              checked (cap reached).
            </p>
          )}
        </>
      )}
    </section>
  );
}
