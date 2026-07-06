import type { MeipMatch } from "../../lib/api";

// ---------------------------------------------------------------------
// MeipSignpost — OECD-UNSD MEIP "signpost" card.
//
// Not a data source in the usual sense: it does not contribute BODS
// statements or a graph. When the subject LEI is in the MEIP Global
// Register (a subsidiary of, or one of, the 500 largest MNEs), this card
// proves the match, surfaces the identifiers + MNE context, and points the
// user to the OECD site to download / reuse the full register. Rendered at
// the very bottom of the results page, beneath the richer source cards and
// the ESG box. Renders nothing when there is no match.
// ---------------------------------------------------------------------

/** External link for the identifier schemes we can deep-link. */
function identifierUrl(scheme: string, value: string): string | null {
  switch (scheme) {
    case "lei":
      return `https://search.gleif.org/#/record/${value}`;
    case "opencorporates":
      return `https://opencorporates.com/companies/${value}`;
    default:
      return null;
  }
}

function IdentifierPill({
  scheme,
  label,
  value,
  corroborated,
}: {
  scheme: string;
  label: string;
  value: string;
  corroborated: boolean;
}) {
  const url = identifierUrl(scheme, value);
  const classes = corroborated
    ? "bg-emerald-50 text-emerald-700 border-emerald-200"
    : "bg-oo-bg text-oo-muted border-oo-rule";
  const inner = (
    <span
      className={`inline-flex items-center gap-1 font-mono text-[11px] border rounded px-1.5 py-0.5 ${classes}`}
      title={corroborated ? "Also published by GLEIF for this LEI" : undefined}
    >
      {corroborated && (
        <svg width="11" height="11" viewBox="0 0 16 16" fill="none" aria-hidden="true">
          <circle cx="8" cy="8" r="6.5" stroke="currentColor" strokeWidth="1.4" />
          <path d="M5 8.2 L7 10.2 L11 6" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      )}
      <span className="text-oo-muted">{label}:</span>
      <span className={corroborated ? "text-emerald-800" : "text-oo-ink"}>{value}</span>
    </span>
  );
  return url ? (
    <a href={url} target="_blank" rel="noopener noreferrer" className="hover:opacity-80">
      {inner}
    </a>
  ) : (
    inner
  );
}

const MEIP_URL =
  "https://www.oecd.org/en/data/dashboards/oecd-unsd-multinational-enterprise-information-platform.html";

export function MeipSignpost({ match }: { match: MeipMatch | null }) {
  if (!match) return null;
  const isHead = match.mode === "mne_head";

  const contextLine = isHead
    ? `One of the world's 500 largest multinational enterprises${
        match.subsidiaries_total
          ? ` · ${match.subsidiaries_total.toLocaleString()} subsidiaries mapped`
          : ""
      }`
    : `Subsidiary of ${match.immediate_parent || match.parent_mne} · part of the ${match.parent_mne} group`;

  return (
    <section className="mb-8 bg-white border border-oo-rule rounded-oo overflow-hidden">
      <header className="px-5 py-3 border-b border-oo-rule flex items-center justify-between gap-3">
        <span className="inline-flex items-center gap-2 font-head font-bold text-[15px] text-oo-ink">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor"
            strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round"
            className="text-oo-muted" aria-hidden="true">
            <path d="M3 21h18" /><path d="M5 21V7l7-4 7 4v14" /><path d="M9 21v-4h6v4" />
          </svg>
          OECD-UNSD MEIP
        </span>
        <span className="text-[11px] font-mono text-oo-muted bg-oo-bg border border-oo-rule rounded px-1.5 py-0.5">
          signpost · not in graph
        </span>
      </header>

      <div className="px-5 py-4">
        <a
          href={MEIP_URL}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1.5 font-head font-bold text-[16px] text-oo-blue hover:text-oo-burst"
        >
          {match.name}
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor"
            strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
            <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
            <path d="M15 3h6v6" /><path d="M10 14 21 3" />
          </svg>
        </a>
        <p className="text-[13px] text-oo-muted mt-1 leading-[1.6]">
          {contextLine}
          {match.iso3 ? <span className="font-mono"> · {match.iso3}</span> : null}
        </p>

        {(match.alt_names.length > 0 || match.address) && (
          <div className="grid grid-cols-[110px_1fr] gap-y-1.5 gap-x-3 mt-3 text-[13px]">
            {match.alt_names.length > 0 && (
              <>
                <div className="text-oo-muted">Also known as</div>
                <div className="text-oo-ink">{match.alt_names.join(" · ")}</div>
              </>
            )}
            {match.address && (
              <>
                <div className="text-oo-muted">Address</div>
                <div className="text-oo-ink">{match.address}</div>
              </>
            )}
          </div>
        )}

        {match.identifiers.length > 0 && (
          <div className="flex flex-wrap gap-1.5 mt-3.5">
            {match.identifiers.map((id) => (
              <IdentifierPill
                key={`${id.scheme}:${id.value}`}
                scheme={id.scheme}
                label={id.label}
                value={id.value}
                corroborated={id.corroborated}
              />
            ))}
          </div>
        )}
      </div>

      <div className="flex gap-3 items-start px-5 py-3.5 bg-blue-50 border-t border-oo-rule">
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor"
          strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round"
          className="text-oo-blue mt-0.5 shrink-0" aria-hidden="true">
          <ellipse cx="12" cy="5" rx="9" ry="3" /><path d="M3 5v14a9 3 0 0 0 18 0V5" />
          <path d="M3 12a9 3 0 0 0 18 0" />
        </svg>
        <div className="min-w-0 flex-1">
          <p className="text-[13px] leading-[1.6] text-oo-ink">
            This entity is one of{" "}
            <strong className="font-semibold">126,000+ subsidiaries of the world's 500
            largest multinationals</strong> mapped by the OECD-UNSD Multinational
            Enterprise Information Platform.
          </p>
          <a
            href={MEIP_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1.5 mt-2 text-[13px] font-semibold text-white bg-oo-blue hover:bg-oo-burst rounded-oo px-3 py-1.5"
          >
            Download &amp; reuse the data on OECD.org
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor"
              strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
              <path d="M5 12h14" /><path d="m12 5 7 7-7 7" />
            </svg>
          </a>
        </div>
      </div>
    </section>
  );
}
