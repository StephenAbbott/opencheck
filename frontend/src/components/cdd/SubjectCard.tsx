import { useState } from "react";
import { BASE_URL } from "../../lib/api";

/**
 * SubjectCard — top-of-page summary of the LEI lookup subject, plus the
 * "Copy share link" affordance. The share link points at the backend
 * /share/{lei} page rather than the SPA URL: social crawlers don't run
 * JavaScript, and that page carries per-entity Open Graph tags with a
 * live summary card (/og/{lei}.png) before redirecting humans here.
 */
export function SubjectCard({ lei, legalName }: { lei: string; legalName: string | null }) {
  const [copied, setCopied] = useState(false);
  const shareUrl = `${BASE_URL || "https://api.opencheck.world"}/share/${lei}`;

  return (
    <section className="mb-8 bg-white border border-oo-rule rounded-oo p-7 transition-shadow hover:shadow-oo-card">
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <p className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-blue">
            Subject
          </p>
          <h2 className="font-head font-bold text-oo-ink mt-2 leading-tight text-[clamp(1.25rem,2.5vw,1.6rem)]">
            {legalName || `LEI ${lei}`}
          </h2>
        </div>
        <button
          type="button"
          onClick={() => {
            navigator.clipboard?.writeText(shareUrl);
            setCopied(true);
            window.setTimeout(() => setCopied(false), 1500);
          }}
          title="Copies a link whose social-media preview shows a live summary card for this entity"
          className="shrink-0 inline-flex items-center gap-1.5 text-[12px] font-medium text-oo-blue border border-[#cfd6f5] bg-[#eef1fb] hover:bg-[#e2e7f9] rounded-full px-3 py-1.5 transition-colors"
        >
          <svg width="12" height="12" viewBox="0 0 16 16" fill="none" aria-hidden="true">
            <path
              d="M6.5 9.5 L9.5 6.5 M7.5 4.5 l2-2 a2.5 2.5 0 0 1 3.5 3.5 l-2 2 M8.5 11.5 l-2 2 a2.5 2.5 0 0 1-3.5-3.5 l2-2"
              stroke="currentColor"
              strokeWidth="1.5"
              strokeLinecap="round"
            />
          </svg>
          {copied ? "Link copied" : "Copy share link"}
        </button>
      </div>
    </section>
  );
}
