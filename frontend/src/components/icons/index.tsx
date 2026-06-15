/**
 * SVG icon components for OpenCheck.
 */

/**
 * GLEIF LEI API search icon — pill-shaped search box with a magnifying
 * glass and a green cursor with click-spark lines.
 */
export function GleifIcon({ className, style }: { className?: string; style?: React.CSSProperties }) {
  return (
    <svg
      viewBox="0 0 88 40"
      className={className}
      style={style}
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden="true"
      focusable="false"
    >
      {/* Pill — white fill, dark teal border */}
      <rect x="1" y="4" width="62" height="32" rx="16" fill="white" stroke="#1b3d4f" strokeWidth="2.5" />
      {/* Magnifying glass ring */}
      <circle cx="19" cy="20" r="7" fill="none" stroke="#1b3d4f" strokeWidth="2.5" />
      {/* Magnifying glass handle */}
      <line x1="24" y1="25" x2="29" y2="30" stroke="#1b3d4f" strokeWidth="2.5" strokeLinecap="round" />
      {/* Three spark / click lines between pill and cursor */}
      <line x1="68" y1="10" x2="72" y2="7"  stroke="#34d399" strokeWidth="2" strokeLinecap="round" />
      <line x1="70" y1="18" x2="75" y2="18" stroke="#34d399" strokeWidth="2" strokeLinecap="round" />
      <line x1="68" y1="26" x2="72" y2="29" stroke="#34d399" strokeWidth="2" strokeLinecap="round" />
      {/* Arrow cursor — filled green */}
      <polygon points="77,12 77,34 81,27 86,35 88,33 83,25 88,25" fill="#34d399" />
    </svg>
  );
}

/**
 * "How it works" step icons — simple outline glyphs rendered inside the
 * numbered step circles. They use ``currentColor`` so the parent's text colour
 * (white, on the brand-blue circle) drives the stroke.
 */
function StepIcon({
  className,
  children,
}: {
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <svg
      viewBox="0 0 24 24"
      className={className}
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false"
    >
      {children}
    </svg>
  );
}

/** Step 1 — paste an LEI (key). */
export function StepKeyIcon({ className }: { className?: string }) {
  return (
    <StepIcon className={className}>
      <circle cx="8" cy="8" r="4.2" />
      <path d="M11 11l8 8" />
      <path d="M18.5 14.5l-2 2" />
      <path d="M21 17l-2 2" />
    </StepIcon>
  );
}

/** Step 2 — GLEIF bridges to national identifiers (fork). */
export function StepBridgeIcon({ className }: { className?: string }) {
  return (
    <StepIcon className={className}>
      <circle cx="12" cy="5" r="2.4" />
      <circle cx="6" cy="19" r="2.4" />
      <circle cx="18" cy="19" r="2.4" />
      <path d="M12 7.4v2.1c0 1.8-6 2.1-6 5.6" />
      <path d="M12 9.5c0 1.8 6 2.1 6 5.6" />
    </StepIcon>
  );
}

/** Step 3 — parallel queries to open sources (hub + spokes). */
export function StepNetworkIcon({ className }: { className?: string }) {
  return (
    <StepIcon className={className}>
      <circle cx="12" cy="12" r="2.4" />
      <circle cx="5" cy="6" r="1.8" />
      <circle cx="19" cy="6" r="1.8" />
      <circle cx="5" cy="18" r="1.8" />
      <circle cx="19" cy="18" r="1.8" />
      <path d="M10.3 10.4 6.4 7.3M13.7 10.4 17.6 7.3M10.3 13.6 6.4 16.7M13.7 13.6 17.6 16.7" />
    </StepIcon>
  );
}

/** Step 4 — risk signals + shareable bundle (shield with check). */
export function StepShieldIcon({ className }: { className?: string }) {
  return (
    <StepIcon className={className}>
      <path d="M12 3l7 3v5c0 4.5-3 7.4-7 8.8-4-1.4-7-4.3-7-8.8V6l7-3z" />
      <path d="M9 12l2.2 2.2L15 10.5" />
    </StepIcon>
  );
}

/**
 * Neo4j logo icon — the official Neo4j mark (neo4j-logo.png served from
 * /public/).  Used on curated example cards to link to the per-entity
 * Neo4j CSV bundle.
 */
export function Neo4jIcon({ size = 18 }: { size?: number }) {
  return (
    <img
      src="/neo4j-logo.jpg"
      alt="Neo4j"
      width={size}
      height={size}
      style={{ display: "block", objectFit: "contain" }}
    />
  );
}

/**
 * OpenCheck magnifying-glass icon — white variant for use on the dark
 * navy header. Sized via className (e.g. ``h-9 w-auto``).
 */
export function OpenCheckIcon({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 200 200"
      className={className}
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden="true"
      focusable="false"
    >
      <defs>
        <clipPath id="oc-icon-lens">
          <circle cx="80" cy="80" r="63" />
        </clipPath>
      </defs>
      {/* Handle */}
      <line x1="127" y1="127" x2="186" y2="186" stroke="white" strokeWidth="14" strokeLinecap="round" />
      {/* Ring */}
      <circle cx="80" cy="80" r="70" fill="none" stroke="white" strokeWidth="13" />
      {/* Building silhouette */}
      <g clipPath="url(#oc-icon-lens)">
        <rect x="90" y="16" width="22" height="108" fill="white" />
        <rect x="108" y="42" width="18" height="82" fill="white" />
        {/* Windows */}
        <rect x="93" y="24" width="6" height="6" fill="#1e3a8a" />
        <rect x="103" y="24" width="6" height="6" fill="#1e3a8a" />
        <rect x="93" y="35" width="6" height="6" fill="#1e3a8a" />
        <rect x="103" y="35" width="6" height="6" fill="#1e3a8a" />
        <rect x="93" y="46" width="6" height="6" fill="#1e3a8a" />
        <rect x="103" y="46" width="6" height="6" fill="#1e3a8a" />
        <rect x="112" y="50" width="5" height="5" fill="#1e3a8a" />
        <rect x="112" y="61" width="5" height="5" fill="#1e3a8a" />
        {/* Door */}
        <rect x="96" y="94" width="10" height="30" fill="#1e3a8a" />
      </g>
      {/* Ownership network — edges */}
      <line x1="48" y1="28" x2="18" y2="76" stroke="#93c5fd" strokeWidth="4.5" strokeLinecap="round" />
      <line x1="18" y1="76" x2="48" y2="124" stroke="#93c5fd" strokeWidth="4.5" strokeLinecap="round" />
      <line x1="48" y1="28" x2="48" y2="124" stroke="#93c5fd" strokeWidth="4.5" strokeLinecap="round" />
      {/* Central arrow */}
      <polygon points="34,55 34,99 76,77" fill="white" />
      {/* Nodes */}
      <circle cx="48" cy="28" r="11" fill="#22c55e" />
      <circle cx="18" cy="76" r="11" fill="#3b82f6" />
      <circle cx="48" cy="124" r="11" fill="#7c3aed" />
    </svg>
  );
}
