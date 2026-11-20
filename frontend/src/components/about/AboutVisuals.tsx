/**
 * The /about page's pictures (the "Make the About page more visual" ticket).
 *
 * Every picture is drawn from marks OpenCheck already ships: the mode badges
 * (`ui/Icon`'s mode glyphs on the brand-mark navy), the Beneficial Ownership
 * Visualisation System icons the graph draws with (`lib/bovsIcons.ts`), the
 * flag set under `/bods-dagre-images/flags/` and the design tokens. No
 * third-party logo: GLEIF's and Neo4j's need a licence, Open Ownership's terms
 * do not cover theirs (checked 20 Nov 2026).
 *
 * Colours are Tailwind token classes on the SVG elements (`fill-oo-blue`,
 * `stroke-oo-softBorder`), never hex, so the design-system lint holds.
 */

import { BOVS_ICONS } from "../../lib/bovsIcons";
import { Icon, type IconName } from "../ui/Icon";

// ---------------------------------------------------------------------------
// How a lookup works
// ---------------------------------------------------------------------------

/** The five steps, read by both the diagram and the text list beneath it. */
export const LOOKUP_STEPS: readonly (readonly [string, string, string])[] = [
  ["01", "Resolves the LEI via GLEIF", "gets the canonical legal name, jurisdiction, registration authority code, and related identifiers."],
  ["02", "Derives bridge IDs", "maps the GLEIF record to national register IDs (UK company number, Dutch KvK number, Czech IČO, …) and cross-references (Wikidata Q-ID, CUSIP)."],
  ["03", "Fans out in parallel", "each source adapter receives whichever identifier it understands and fetches independently; results stream back as they arrive."],
  ["04", "Maps to BODS 0.4", "every source payload is run through a dedicated mapper, producing entity, person, and ownership/control statements in the Beneficial Ownership Data Standard."],
  ["05", "Aggregates risk signals", "the unified BODS graph is inspected for structural risk patterns: layered ownership chains, trusts and nominees, high-risk jurisdictions, sanctions exposure."],
];

/** A selection of the registers a lookup fans out to, with their flags. */
const FAN_OUT: readonly (readonly [string, string])[] = [
  ["gb", "Companies House"],
  ["no", "Brønnøysund Register"],
  ["ee", "e-Business Register"],
  ["dk", "CVR"],
  ["fr", "INPI"],
  ["us", "SEC EDGAR"],
];

const FLAG = (code: string) => `/bods-dagre-images/flags/${code}.svg`;

function Arrow({ x1, x2, y }: { x1: number; x2: number; y: number }) {
  return (
    <g className="stroke-oo-blue" strokeWidth={2} fill="none">
      <path d={`M${x1} ${y}H${x2 - 6}`} />
      <path d={`M${x2 - 8} ${y - 5} ${x2} ${y} ${x2 - 8} ${y + 5}`} strokeLinejoin="round" />
    </g>
  );
}

function StepLabel({ x, y, n, word }: { x: number; y: number; n: string; word: string }) {
  return (
    <text x={x} y={y} textAnchor="middle" fontSize={11.5} className="fill-oo-blue font-mono">
      {n} · {word.toUpperCase()}
    </text>
  );
}

function BovsDisc({ x, y, r, icon }: { x: number; y: number; r: number; icon: string }) {
  return (
    <g>
      <circle cx={x} cy={y} r={r} className="fill-oo-blue" />
      <image href={BOVS_ICONS[icon]} x={x - r} y={y - r} width={r * 2} height={r * 2} />
    </g>
  );
}

/**
 * The lookup as one picture: LEI → GLEIF record → bridge IDs → the sources →
 * BODS → risk signals. Shown from `sm` up; on a phone the numbered list
 * beneath carries the same steps.
 */
export function LookupDiagram() {
  const midY = 98;
  return (
    <svg
      viewBox="0 0 1120 232"
      width="100%"
      role="img"
      aria-label="How a lookup works: an LEI is resolved through GLEIF, mapped to national register numbers, sent to every source that understands one of them, mapped to BODS statements and read for risk signals."
      className="hidden sm:block font-body"
    >
      {/* LEI */}
      <rect x={10} y={76} width={150} height={44} rx={8} className="fill-oo-blue" />
      <text x={85} y={95} textAnchor="middle" fontSize={10} className="fill-white font-mono">LEI</text>
      <text x={85} y={110} textAnchor="middle" fontSize={11} className="fill-white font-mono">5493005044RTLQ5RZU70</text>
      <Arrow x1={162} x2={210} y={midY} />

      {/* GLEIF record */}
      <rect x={212} y={68} width={150} height={60} rx={8} className="fill-oo-soft stroke-oo-softBorder" />
      <text x={287} y={94} textAnchor="middle" fontSize={14.5} fontWeight={700} className="fill-oo-ink font-head">GLEIF record</text>
      <text x={287} y={112} textAnchor="middle" fontSize={11} className="fill-oo-muted">name · jurisdiction · RA code</text>
      <StepLabel x={287} y={150} n="01" word="Resolve" />
      <Arrow x1={364} x2={412} y={midY} />

      {/* Bridge IDs */}
      <rect x={414} y={68} width={140} height={60} rx={8} className="fill-oo-soft stroke-oo-softBorder" />
      <text x={484} y={94} textAnchor="middle" fontSize={14.5} fontWeight={700} className="fill-oo-ink font-head">Bridge IDs</text>
      <text x={484} y={112} textAnchor="middle" fontSize={10.5} className="fill-oo-muted font-mono">GB-COH · NO-BRC · …</text>
      <StepLabel x={484} y={150} n="02" word="Bridge" />

      {/* Fan out */}
      <StepLabel x={685} y={12} n="03" word="Fan out" />
      {FAN_OUT.map(([flag, name], i) => {
        const y = 22 + i * 33;
        return (
          <g key={flag}>
            <path
              d={`M556 ${midY} C578 ${midY}, 578 ${y + 13}, 600 ${y + 13}`}
              fill="none"
              strokeWidth={1.5}
              className="stroke-oo-softBorder"
            />
            <rect x={600} y={y} width={170} height={26} rx={13} className="fill-white stroke-oo-softBorder" />
            <image href={FLAG(flag)} x={609} y={y + 6} width={20} height={14} preserveAspectRatio="none" />
            <text x={636} y={y + 17.5} fontSize={12.5} className="fill-oo-ink">{name}</text>
          </g>
        );
      })}
      <text x={685} y={224} textAnchor="middle" fontSize={11} className="fill-oo-muted">
        … and every other source that holds a key
      </text>
      <Arrow x1={776} x2={824} y={midY} />

      {/* BODS */}
      <rect x={826} y={58} width={150} height={80} rx={8} className="fill-white stroke-oo-softBorder" />
      <text x={901} y={78} textAnchor="middle" fontSize={14.5} fontWeight={700} className="fill-oo-ink font-head">BODS v0.4</text>
      <BovsDisc x={860} y={106} r={16} icon="registeredEntity" />
      <BovsDisc x={901} y={106} r={16} icon="knownPerson" />
      <BovsDisc x={942} y={106} r={16} icon="arrangement" />
      <StepLabel x={901} y={160} n="04" word="Map" />
      <Arrow x1={978} x2={1006} y={midY} />

      {/* Risk signals */}
      {[
        ["Sanctioned", "fill-oo-risk-bg", "fill-oo-risk-text"],
        ["Layered chain", "fill-oo-info-bg", "fill-oo-info-text"],
        ["Trust", "fill-oo-soft", "fill-oo-blue"],
      ].map(([label, bg, fg], i) => (
        <g key={label}>
          <rect x={1008} y={60 + i * 28} width={104} height={22} rx={11} className={bg} />
          <text x={1060} y={75 + i * 28} textAnchor="middle" fontSize={11.5} className={fg}>{label}</text>
        </g>
      ))}
      <StepLabel x={1060} y={162} n="05" word="Read" />
    </svg>
  );
}

// ---------------------------------------------------------------------------
// The four checks
// ---------------------------------------------------------------------------

export type AboutMode = "quick" | "full" | "background" | "esg";

const MODE_BADGE: Record<AboutMode, { icon: IconName; ring: string }> = {
  quick: { icon: "quickcheck", ring: "border-oo-node-green text-oo-node-green" },
  full: { icon: "fullcheck", ring: "border-oo-node-blue text-oo-node-blue" },
  background: { icon: "backgroundcheck", ring: "border-oo-node-purple text-oo-node-purple" },
  esg: { icon: "esg", ring: "border-oo-node-teal text-oo-node-teal" },
};

/** A mode's badge: its glyph on the brand-mark navy, ringed in its accent. */
export function ModeBadge({ mode }: { mode: AboutMode }) {
  const { icon, ring } = MODE_BADGE[mode];
  return (
    <span
      aria-hidden="true"
      className={`shrink-0 inline-flex items-center justify-center w-10 h-10 rounded-full bg-oo-mark-navy border-2 ${ring}`}
    >
      <Icon name={icon} size={20} strokeWidth={2} />
    </span>
  );
}

// ---------------------------------------------------------------------------
// The BODS spine
// ---------------------------------------------------------------------------

/** The icon each BODS statement type is drawn with on the graph. */
export const STATEMENT_ICON: Record<"entity" | "person", string> = {
  entity: BOVS_ICONS.registeredEntity,
  person: BOVS_ICONS.knownPerson,
};

/** A person owning a company that owns a subsidiary: three statements and two arrows. */
export function BodsMiniGraph() {
  return (
    <svg
      viewBox="0 0 460 150"
      width="100%"
      role="img"
      aria-label="An example: a person statement and two entity statements, joined by two relationship statements: the person owns 75 to 100 percent of a company, which owns all of a subsidiary."
      className="mt-4 max-w-md font-body"
    >
      <BovsDisc x={60} y={60} r={34} icon="knownPerson" />
      <BovsDisc x={230} y={60} r={34} icon="registeredEntity" />
      <BovsDisc x={400} y={60} r={34} icon="registeredEntity" />
      {[
        [96, 190, 143, "Owns 75–100%"],
        [266, 360, 313, "Owns 100%"],
      ].map(([x1, x2, mid, label]) => (
        <g key={label as string}>
          <g className="stroke-oo-graph-ownership" strokeWidth={2.5} fill="none">
            <path d={`M${x1} 60H${(x2 as number) - 6}`} />
            <path d={`M${(x2 as number) - 9} 54 ${x2} 60 ${(x2 as number) - 9} 66`} strokeLinejoin="round" />
          </g>
          <text x={mid} y={50} textAnchor="middle" fontSize={10.5} className="fill-oo-graph-ownershipText">
            {label}
          </text>
        </g>
      ))}
      <g textAnchor="middle" fontSize={11.5} className="fill-oo-ink">
        <text x={60} y={118}>Person statement</text>
        <text x={230} y={118}>Entity statement</text>
        <text x={400} y={118}>Entity statement</text>
      </g>
      <text x={230} y={142} textAnchor="middle" fontSize={10.5} className="fill-oo-muted">
        Each arrow is a relationship statement
      </text>
    </svg>
  );
}

// ---------------------------------------------------------------------------
// GLEIF and the LEI
// ---------------------------------------------------------------------------

/**
 * The parts of an LEI (ISO 17442): the issuer's four-character prefix, two
 * reserved characters, twelve for the entity and two check digits. The LEI is
 * Eesti Energia AS's, one of the curated homepage examples.
 */
export const EXAMPLE_LEI = "5493005044RTLQ5RZU70";

export function LeiAnatomy() {
  const parts: [string, string, string][] = [
    [EXAMPLE_LEI.slice(0, 4), "issuer", "bg-oo-soft border-oo-softBorder text-oo-blue"],
    [EXAMPLE_LEI.slice(4, 6), "reserved", "bg-oo-bg border-oo-rule text-oo-muted"],
    [EXAMPLE_LEI.slice(6, 18), "entity", "bg-white border-oo-softBorder text-oo-ink"],
    [EXAMPLE_LEI.slice(18), "check", "bg-oo-ok-bg border-oo-ok-border text-oo-ok-text"],
  ];
  return (
    <figure className="mt-4 max-w-md">
      <div className="flex gap-1.5">
        {parts.map(([chars, label, tone]) => (
          <div key={label} className={label === "entity" ? "flex-1 min-w-0" : "shrink-0"}>
            <div className={`font-mono text-oo-lead sm:text-oo-stat text-center rounded-md border px-2 py-1.5 ${tone}`}>
              {chars}
            </div>
            <div className="text-oo-meta text-oo-muted text-center mt-1">{label}</div>
          </div>
        ))}
      </div>
      <figcaption className="text-oo-meta text-oo-muted mt-2">
        The twenty characters of an LEI (Eesti Energia AS).
      </figcaption>
    </figure>
  );
}

// ---------------------------------------------------------------------------
// GODIN
// ---------------------------------------------------------------------------

const GODIN_NODES: readonly (readonly [number, number])[] = [
  [40, 22], [40, 98], [120, 12], [120, 108], [340, 12], [340, 108], [420, 22], [420, 98],
];

/** Many open datasets joined through one identifier. */
export function GodinNetwork() {
  return (
    <svg
      viewBox="0 0 460 120"
      width="100%"
      role="img"
      aria-label="Many open datasets joined through one Legal Entity Identifier."
      className="mt-4 max-w-md font-body"
    >
      <g strokeWidth={1.5} className="stroke-oo-softBorder">
        {GODIN_NODES.map(([x, y]) => (
          <line key={`${x}-${y}`} x1={230} y1={60} x2={x} y2={y} />
        ))}
      </g>
      <g strokeWidth={1.5} className="fill-white stroke-oo-blue">
        {GODIN_NODES.map(([x, y]) => (
          <circle key={`${x}-${y}`} cx={x} cy={y} r={9} />
        ))}
      </g>
      <rect x={196} y={44} width={68} height={32} rx={16} className="fill-oo-blue" />
      <text x={230} y={64.5} textAnchor="middle" fontSize={12} className="fill-white font-mono">LEI</text>
    </svg>
  );
}
