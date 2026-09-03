/**
 * VerdictStrip — the answer-first layer, rendered (Phase 168).
 *
 * This is the first component test in the codebase, and the strip is the
 * right place to start: the suite was logic-only, and the failure that made
 * the case for these tests was **the double verdict** — the same sentence
 * rendered twice on the results page, which every unit test in `lib/`
 * passed straight through because a sentence built once and printed twice is
 * a rendering fault, not a logic one.
 *
 * So the assertions here are about the markup: how many of a thing there
 * are, what an element's accessible name is, and what a control does when
 * you press it. Anything that is true of the *values* belongs in
 * `lib/lookupProgress.test.ts` next door, and stays there.
 */
import { describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { VerdictStrip } from "./VerdictStrip";
import type { RiskSignal } from "../../lib/api";

const VERDICT =
  "A politically exposed person among the parties named, and ownership that runs through three or more layers.";

const signal = (code: string, overrides: Partial<RiskSignal> = {}): RiskSignal => ({
  code,
  confidence: "medium",
  summary: `Summary sentence for ${code}.`,
  source_id: "opensanctions",
  hit_id: "h1",
  evidence: {},
  ...overrides,
});

const RISK = [
  signal("RELATED_PEP"),
  signal("OFFSHORE_LEAKS"),
  signal("COMPLEX_OWNERSHIP_LAYERS"),
  signal("OPAQUE_OWNERSHIP"),
];
const CONTEXT = [signal("NON_EU_JURISDICTION", { kind: "context" })];

function renderStrip(props: Partial<Parameters<typeof VerdictStrip>[0]> = {}) {
  return render(
    <VerdictStrip
      verdict={VERDICT}
      riskSignals={RISK}
      contextSignals={CONTEXT}
      degraded={[]}
      sourcesAnswered={10}
      sourcesApplicable={10}
      graphShape={{ companies: 12, people: 4, relationships: 20, depth: 3 }}
      registryTotal={40}
      jurisdiction="GB"
      {...props}
    />,
  );
}

describe("VerdictStrip", () => {
  it("states the verdict exactly once", () => {
    renderStrip();
    // The regression this suite exists for. `getAllByText` rather than
    // `getByText` on purpose: `getByText` throws on multiple matches, which
    // would read as "the element is missing" in the failure output when the
    // truth is that there are two of it.
    expect(screen.getAllByText(VERDICT)).toHaveLength(1);
  });

  it("is one region a screen reader can find by name", () => {
    renderStrip();
    const regions = screen.getAllByRole("region", { name: "What this check found" });
    expect(regions).toHaveLength(1);
    // Both halves are rendered from one event, so they cannot disagree —
    // and both must be inside the region for that to be legible.
    expect(within(regions[0]).getByRole("heading", { name: "What we found" })).toBeInTheDocument();
    expect(within(regions[0]).getByRole("heading", { name: "Coverage" })).toBeInTheDocument();
  });

  it("previews three chips and counts the rest, rather than printing all of them", () => {
    renderStrip();
    const chips = screen.getAllByRole("button", { name: /Related PEP|Offshore leaks|≥3 layers|Opaque ownership/ });
    expect(chips).toHaveLength(3);
    expect(screen.getByText("+1 more")).toBeInTheDocument();
  });

  it("renders its chips as controls, the same as the section below", async () => {
    renderStrip();
    // They shipped inert here for four phases: the same chip, in the same
    // colours, opening only if it happened to sit further down the page.
    const chip = screen.getByRole("button", { name: /Related PEP/ });
    expect(chip).toHaveAttribute("aria-expanded", "false");
    await userEvent.click(chip);
    expect(chip).toHaveAttribute("aria-expanded", "true");
  });

  it("keeps a chip's summary out of its name and reachable as a description (Phase 160)", () => {
    renderStrip();
    const chip = screen.getByRole("button", { name: /Related PEP/ });
    // The confidence glyph is aria-hidden, so it is not in the name — the
    // word is, which is the point of Phase 122's glyph-plus-label pair.
    expect(chip).toHaveAccessibleName("One source only: Related PEP");
    // "Opensanctions", not "OpenSanctions": `RiskChip` calls `sourceLabel`
    // with no registry-names map, so it takes the prettified-id fallback,
    // while the identity-band rows next to it pass the map and get the
    // registry's own capitalisation. Pinned as observed rather than as
    // preferred — a real, small inconsistency, and now a visible one.
    expect(chip).toHaveAccessibleDescription("Summary sentence for RELATED_PEP. Source: Opensanctions.");
  });

  it("counts risk and structural signals apart", () => {
    renderStrip();
    // The numeral is its own span for typographic reasons, so the sentence is
    // split across elements: read the paragraph, not a text node.
    const found = screen.getByRole("heading", { name: "What we found" }).parentElement!;
    expect(found.textContent).toContain("5 signals — 4 risk, 1 structural");
  });

  it("says what a clean check means, and never leaves the column empty", () => {
    renderStrip({ riskSignals: [], contextSignals: [] });
    expect(
      screen.getByText("No risk signals surfaced across the sources that answered."),
    ).toBeInTheDocument();
    expect(screen.queryByText("+1 more")).not.toBeInTheDocument();
  });

  it("says it is still checking rather than clean while the screen is open", () => {
    renderStrip({ riskSignals: [], contextSignals: [], screening: true });
    expect(screen.getByText("Still checking.")).toBeInTheDocument();
    expect(
      screen.queryByText("No risk signals surfaced across the sources that answered."),
    ).not.toBeInTheDocument();
  });

  it("renders no sentence at all when the backend sent none", () => {
    renderStrip({ verdict: null });
    expect(screen.queryByText(VERDICT)).not.toBeInTheDocument();
    // The columns still stand: absence of a sentence is not absence of a check.
    expect(screen.getByRole("heading", { name: "Coverage" })).toBeInTheDocument();
  });

  it("offers the network column only when there is somewhere to go", async () => {
    const onOpenNetwork = vi.fn();
    const { unmount } = renderStrip({ onOpenNetwork });
    await userEvent.click(
      screen.getByRole("button", { name: "Explore the full ownership network" }),
    );
    expect(onOpenNetwork).toHaveBeenCalledOnce();
    unmount();

    renderStrip();
    expect(
      screen.queryByRole("button", { name: "Explore the full ownership network" }),
    ).not.toBeInTheDocument();
  });
});
