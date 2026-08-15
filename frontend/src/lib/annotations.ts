/**
 * BODS statement annotations — "as filed" support.
 *
 * Phase 103 started emitting BODS `annotations` on statements: the Companies
 * House nature-of-control code behind a rendered `otherInfluenceOrControl`,
 * and a `commenting` note where a `birthDate` is imprecise because the
 * register published month and year only. Phase 107 carried them into the RDF
 * export. Until now nothing in the UI read them, so the register's own words
 * arrived in the browser and were discarded at the last step.
 *
 * The rule those annotations follow, from `bods/annotations.py`:
 * **the statement always carries the usable value; the annotation always
 * carries the register's words.** So "as filed" is a presentation swap, never
 * a different fact — the underlying statement is unchanged either way.
 *
 * Everything here is a pure function over the statement JSON so it can be
 * tested without a DOM (this codebase has no DOM test harness by choice).
 */

export interface BODSAnnotation {
  statementPointerTarget?: string;
  motivation?: string;
  description?: string;
  transformedContent?: string;
  creationDate?: string;
  createdBy?: { name?: string; uri?: string };
  url?: string;
}

/** RFC6901 pointer → path segments, unescaping `~1` → `/` and `~0` → `~`.
 *
 * Order matters: `~1` must be replaced before `~0`, or a literal `~1` encoded
 * as `~01` unescapes to `/` instead of `~1`. Same trap the Python side
 * documents.
 */
export function parsePointer(target: string): string[] {
  if (!target || target === "/") return [];
  return target
    .replace(/^\//, "")
    .split("/")
    .map((seg) => seg.replace(/~1/g, "/").replace(/~0/g, "~"));
}

function pathsEqual(a: string[], b: string[]): boolean {
  return a.length === b.length && a.every((seg, i) => seg === b[i]);
}

/** Every annotation on `stmt` whose pointer addresses exactly `path`.
 *
 * Exact match, not prefix: an annotation on `/recordDetails/interests/0/type`
 * describes that interest's type, and showing it against the whole interest —
 * or against a sibling interest — would attribute the register's words to
 * something it never said.
 */
export function annotationsAt(
  stmt: unknown,
  ...path: (string | number)[]
): BODSAnnotation[] {
  if (stmt == null || typeof stmt !== "object") return [];
  const raw = (stmt as Record<string, unknown>).annotations;
  if (!Array.isArray(raw)) return [];
  const wanted = path.map(String);
  return raw.filter((a): a is BODSAnnotation => {
    if (a == null || typeof a !== "object") return false;
    const target = (a as BODSAnnotation).statementPointerTarget;
    return typeof target === "string" && pathsEqual(parsePointer(target), wanted);
  });
}

/** The register's own words for a field, or "" if nothing was annotated.
 *
 * `description` is where the source's wording lives by construction — see the
 * one-rule note above. Multiple annotations on one field are joined rather
 * than silently dropping all but the first.
 */
export function asFiledText(annotations: BODSAnnotation[]): string {
  return annotations
    .map((a) => (a.description ?? "").trim())
    .filter(Boolean)
    .join(" · ");
}

/** How many fields across these statements carry an annotation.
 *
 * Drives whether the toggle renders at all: a control that changes nothing
 * is worse than no control, and most sources annotate nothing. Counts
 * distinct pointers per statement, so two annotations on one field count once.
 */
export function annotatedFieldCount(statements: unknown[]): number {
  let n = 0;
  for (const stmt of statements ?? []) {
    if (stmt == null || typeof stmt !== "object") continue;
    const raw = (stmt as Record<string, unknown>).annotations;
    if (!Array.isArray(raw)) continue;
    const seen = new Set<string>();
    for (const a of raw) {
      const target =
        a && typeof a === "object"
          ? (a as BODSAnnotation).statementPointerTarget
          : undefined;
      if (typeof target === "string" && target) seen.add(target);
    }
    n += seen.size;
  }
  return n;
}

/** Label for the toggle. States the destination, matching the graph's
 * "View as table" / "View as graph" convention rather than describing the
 * current state. */
export function asFiledToggleLabel(on: boolean, fieldCount: number): string {
  if (on) return "Show OpenCheck's reading";
  return fieldCount === 1
    ? "Show 1 field as filed"
    : `Show ${fieldCount} fields as filed`;
}

// ---------------------------------------------------------------------
// Shared toggle state
// ---------------------------------------------------------------------
//
// Deliberately module-scoped rather than per-card component state. A lookup
// renders many source cards; setting "as filed" on one and finding the next
// still in OpenCheck's vocabulary would read as a bug. One setting, applied
// wherever annotations exist, held for as long as the page lives.
//
// Not persisted to storage: the default should stay OpenCheck's reading,
// because that is the one that is always present and always machine-readable.

type Listener = () => void;

let asFiledOn = false;
const listeners = new Set<Listener>();

export function getAsFiled(): boolean {
  return asFiledOn;
}

export function setAsFiled(on: boolean): void {
  if (asFiledOn === on) return;
  asFiledOn = on;
  for (const l of listeners) l();
}

export function subscribeAsFiled(listener: Listener): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/** Test-only: reset module state between cases. */
export function __resetAsFiled(): void {
  asFiledOn = false;
  listeners.clear();
}
