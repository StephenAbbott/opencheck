/**
 * Icon — one stroke-based set, drawn once.
 *
 * `components/icons/index.tsx` is imported by exactly one file (App.tsx),
 * so the other twelve components each pasted their own SVG: the chevron
 * appears five times, the network glyph twice, the external-link arrow and
 * the warning triangle several times each, and the rest of the vocabulary
 * is text glyphs (⬇ ↓ → ⚠️ ↗ ‹ › ▸ ▾) which do not scale or recolour.
 *
 * Every path here is stroke-only on a 24 grid and inherits `currentColor`,
 * so a caller sets colour and size on the parent and never touches the SVG.
 * The three mode glyphs are the ones already shipped on the v1 mode cards,
 * copied coordinate-for-coordinate so the tabs, the badges and the logo do
 * not drift apart.
 */

export const ICON_PATHS = {
  // Modes
  quickcheck: ["M13 3 4 14h7l-1 7 9-11h-7z"],
  fullcheck: ["M8 7.5 10.7 15.6M16 7.5 13.3 15.6M8.5 6h7"],
  backgroundcheck: ["M5 20c.8-3.5 3.6-5.5 7-5.5s6.2 2 7 5.5"],
  esg: [
    "M11 20A7 7 0 0 1 9.8 6.1C15.5 5 17 4.5 19 2c1 2 2 4.2 2 8 0 5.5-4.8 10-10 10Z",
    "M2 21c0-3 1.9-5.4 5.1-6C9.5 14.5 12 13 13 12",
  ],
  // Structure
  chevronDown: ["m6 9 6 6 6-6"],
  chevronRight: ["m9 6 6 6-6 6"],
  arrowRight: ["M5 12h14", "m12 5 7 7-7 7"],
  close: ["M18 6 6 18M6 6l12 12"],
  menu: ["M4 7h16M4 12h16M4 17h16"],
  plus: ["M12 5v14M5 12h14"],
  // Meaning
  check: ["M20 6 9 17l-5-5"],
  warning: [
    "M12 9v4",
    "M12 17h.01",
    "M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0Z",
  ],
  search: ["m20 20-3.5-3.5"],
  download: ["M12 3v12", "m7 12 5 5 5-5", "M4 20h16"],
  share: [
    "M6.5 9.5 9.5 6.5",
    "M7.5 4.5l2-2a2.5 2.5 0 0 1 3.5 3.5l-2 2",
    "M8.5 11.5l-2 2a2.5 2.5 0 0 1-3.5-3.5l2-2",
  ],
  external: ["M14 4h6v6", "M20 4 10 14", "M18 14v5a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V7a1 1 0 0 1 1-1h5"],
  history: ["M3 12a9 9 0 1 0 3-6.7L3 8", "M3 4v4h4", "M12 8v4l3 2"],
} as const;

/** Icons that need a circle or two the path list cannot express. */
const ICON_SHAPES: Partial<Record<IconName, { cx: number; cy: number; r: number }[]>> = {
  fullcheck: [
    { cx: 6, cy: 6, r: 2.3 },
    { cx: 18, cy: 6, r: 2.3 },
    { cx: 12, cy: 18, r: 2.3 },
  ],
  backgroundcheck: [{ cx: 12, cy: 8, r: 3.5 }],
  search: [{ cx: 11, cy: 11, r: 7 }],
};

export type IconName = keyof typeof ICON_PATHS;

export const ICON_NAMES = Object.keys(ICON_PATHS) as IconName[];

export function Icon({
  name,
  size = 16,
  className = "",
  strokeWidth = 1.75,
  title,
}: {
  name: IconName;
  size?: number;
  className?: string;
  strokeWidth?: number;
  /**
   * Only pass this when the icon is the *only* content of its control.
   * An icon beside a text label must stay `aria-hidden`, or assistive
   * technology reads the label twice.
   */
  title?: string;
}) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={strokeWidth}
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      role={title ? "img" : undefined}
      aria-hidden={title ? undefined : true}
      aria-label={title}
    >
      {(ICON_SHAPES[name] ?? []).map((c, i) => (
        <circle key={`c${i}`} cx={c.cx} cy={c.cy} r={c.r} />
      ))}
      {ICON_PATHS[name].map((d, i) => (
        <path key={`p${i}`} d={d} />
      ))}
    </svg>
  );
}
