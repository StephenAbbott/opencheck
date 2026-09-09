/**
 * fullCheckHeader — what the FullCheck header says, in each of its two states.
 *
 * The header used to say the same thing in every state. Before a run it carried
 * four blocks of instructions above the canvas; after one it carried the same
 * four, including "Run FullCheck to expand owners and controllers layer by
 * layer" printed underneath a network that had already been expanded. The
 * result — how much was found, and what risk it exposed — sat fourth in the
 * reading order, below two sets of instructions for a decision already made.
 *
 * So the header now has two states and this module is what they say. Before a
 * run: one action and one line of help. After one: a summary of what was built,
 * and a risk sentence that leads with the diff FullCheck actually produced.
 *
 * It lives in `lib/` because the frontend suite is logic-only and a sentence
 * that exists only as a literal inside JSX cannot be pinned — which is how the
 * four verbs in `vocab.ts` happened. Every count-built sentence here has an
 * explicit zero branch: a sentence assembled from a count reads as nonsense at
 * zero ("flagged 0 risk signals"), and stating absence in the same voice as
 * presence is the rule the source findings already follow — silence reads as
 * "nothing to see".
 *
 * The risk counts are **passed in**, never derived here. Both the verdict strip
 * and this sentence must count with `riskFindingCount`, and the way to keep two
 * numbers from disagreeing is to have one function produce them.
 */

/** Companies, people and provenance in the network as currently drawn. */
export interface NetworkShape {
  /** Entity nodes (people excluded — they are counted separately). */
  companies: number;
  people: number;
  /** Distinct sources contributing a node to the reconciled network. */
  sources: number;
  /** Nodes asserted by ≥2 *independent* sources (lineage-collapsed). */
  corroborated: number;
}

/**
 * How the network got to its current size. The two ways in are not the same
 * claim: a depth budget says how far the eager run was allowed to go, a manual
 * layer says the reader walked one hop themselves. A network can carry both.
 */
export interface ExpansionLead {
  /** Layers the eager run actually completed — not the budget it was given. */
  runDepth: number | null;
  /** Layers added one at a time from the canvas toolbar. */
  manualLayers: number;
}

function plural(n: number, one: string, many = `${one}s`): string {
  return `${n} ${n === 1 ? one : many}`;
}

/**
 * The lead clause of the summary: what was run.
 *
 * Returns `null` when nothing has been expanded, which is the caller's signal
 * that there is no summary to render — the header is still in its "before"
 * state and has an action in it, not a result.
 */
export function expansionLead({ runDepth, manualLayers }: ExpansionLead): string | null {
  const parts: string[] = [];
  if (runDepth !== null && runDepth > 0) parts.push(`depth ${runDepth}`);
  if (manualLayers > 0) parts.push(`${plural(manualLayers, "layer")} by hand`);
  return parts.length > 0 ? parts.join(" + ") : null;
}

/**
 * The summary line's clauses, in reading order, for the caller to separate.
 *
 * People are named only when the network holds any: "· 0 people" reads as a
 * finding about the company rather than a fact about the graph, which is the
 * same reason the diagram chip suppresses its own zero. Corroboration is
 * likewise omitted at zero — an EDD reader takes "0 corroborated" as a warning,
 * and on a single-source network it is merely arithmetic. Sources cannot be
 * zero on a network built from real statements, and a "0 sources" line would
 * describe the renderer rather than the company, so it is omitted too.
 */
export function summaryParts(shape: NetworkShape, lead: ExpansionLead): string[] {
  const parts: string[] = [];
  const head = expansionLead(lead);
  if (head) parts.push(head);
  const size = [plural(shape.companies, "company", "companies")];
  if (shape.people > 0) size.push(plural(shape.people, "person", "people"));
  if (shape.sources > 0) size.push(plural(shape.sources, "source"));
  parts.push(size.join(", "));
  if (shape.corroborated > 0) parts.push(`${shape.corroborated} corroborated`);
  return parts;
}

/**
 * The Network risk sentence.
 *
 * Before a run it reports what QuickCheck found and names the action that would
 * widen it. After one it leads with FullCheck's own contribution, because that
 * is the question the reader pressed the button to answer — and says so in the
 * same voice when the answer is nothing.
 *
 * `subject` and `additional` are distinct **findings** (`riskFindingCount`), not
 * raw signals: the related-party rules emit several signals per hit, so a raw
 * count overstates, and the verdict strip counts the same way.
 */
export function networkRiskSentence({
  subject,
  additional,
  hasRun,
}: {
  subject: number;
  additional: number;
  hasRun: boolean;
}): string {
  const quick =
    subject === 0
      ? "QuickCheck flagged no risk signals in the records gathered so far."
      : `QuickCheck flagged ${plural(subject, "risk signal")} in the records gathered so far.`;
  if (!hasRun) return `${quick} Run FullCheck to screen the wider network for risk.`;
  if (additional === 0) {
    return subject === 0
      ? "FullCheck screened the wider network and found no risk signals, and QuickCheck flagged none on the subject."
      : `FullCheck screened the wider network and found nothing beyond the ${plural(
          subject,
          "signal",
        )} QuickCheck flagged on the subject.`;
  }
  return subject === 0
    ? `FullCheck surfaced ${plural(additional, "risk signal")} across the wider network; QuickCheck flagged none on the subject.`
    : `FullCheck surfaced ${plural(additional, "risk signal")} across the wider network, beyond the ${subject} QuickCheck flagged on the subject.`;
}

/** What the toolbar's single-layer control shows and announces. */
export interface LayerControl {
  /** Visible text — short enough for a toolbar, honest at zero. */
  label: string;
  /** The frontier count, rendered as a badge; `null` when there is none. */
  count: number | null;
  /** The full sentence the visible label abbreviates. */
  ariaLabel: string;
  disabled: boolean;
}

/**
 * The "+1 layer" control that replaced the full-width "Add next layer" button.
 *
 * It manipulates the canvas, so it sits with the other canvas controls rather
 * than competing with the mode's primary action above the graph. The frontier
 * count survives the move as the badge — it is the one thing the old button
 * carried that "Run FullCheck" does not, because it says whether anything is
 * left to expand at all.
 *
 * At an empty frontier the *visible* text changes, not just the accessible
 * name: a disabled control is not focusable, so an `aria-label` explaining why
 * is announced to nobody.
 */
export function layerControl({
  frontier,
  noun,
  busy,
}: {
  frontier: number;
  noun: string;
  busy: boolean;
}): LayerControl {
  if (busy) {
    return { label: "Adding…", count: null, ariaLabel: "Adding the next layer", disabled: true };
  }
  if (frontier === 0) {
    return {
      label: "No more layers",
      count: null,
      ariaLabel: `No further ${noun} to reveal — every company at the edge of the network is either fully expanded or has no register OpenCheck can read`,
      disabled: true,
    };
  }
  return {
    label: "+1 layer",
    count: frontier,
    ariaLabel: `Add the next layer — resolve ${noun} for ${plural(frontier, "company", "companies")} at the edge of the network`,
    disabled: false,
  };
}

/**
 * The one line of help under the run control.
 *
 * The three-line version of this said which identifiers can be hopped and that
 * chains ending in people stop — both true, both repeated by the mode blurb
 * above and by `expandNote` at the moment they actually apply, which is after a
 * click rather than before one.
 */
export function runHelper({
  direction,
  registerHops,
  cap,
}: {
  direction: "owners" | "subsidiaries";
  registerHops: boolean;
  cap: number;
}): string {
  const what = direction === "subsidiaries" ? "subsidiaries" : "owners and controllers";
  const who = registerHops && direction === "owners" ? "an LEI or a readable company number" : "an LEI";
  return `Expands ${what} to that depth, for companies with ${who}, capped at ${cap}.`;
}
