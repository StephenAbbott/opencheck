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
import type { KnowabilityStatement, RiskSignal } from "../../lib/api";

const VERDICT =
  "The records show a politically exposed person among the parties named. Its ownership chain is 4 layers deep.";

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

const KNOWABILITY: KnowabilityStatement = {
  code: "KY",
  name: "Cayman Islands",
  sentence:
    "Cayman Islands General Registry is accessible to authorities and obliged entities, and to others on legitimate interest, since 1 Feb 2025. OpenCheck reads no Cayman Islands register, so an owner absent from this report says nothing about what Cayman Islands holds.",
  sentences: [],
  stated_absence: false,
  review_status: "unverified",
  last_verified: null,
  access: "legitimate_interest",
  access_since: "2025-02-01",
  next_change_expected: null,
  fields: { register: "Cayman Islands General Registry", threshold_wording: "25 % or more" },
  opencheck_reads: [],
  sources: [{ url: "https://www.ciregistry.ky/beneficial-ownership", title: null }],
  as_of: "2026-09-18",
};

describe("VerdictStrip — what can be known (Phase 224, folded into Coverage in Phase 245)", () => {
  it("renders nothing for the band until the statement lands", () => {
    renderStrip();
    expect(screen.queryByTestId("knowability-band")).toBeNull();
    expect(screen.queryByRole("heading", { name: "What can be known" })).toBeNull();
  });

  it("sits inside the Coverage column, with its dated badge in view", () => {
    renderStrip({ knowability: KNOWABILITY });
    const region = screen.getByRole("region", { name: "What this check found" });
    const heading = within(region).getByRole("heading", { name: "What can be known" });
    const coverage = within(region).getByRole("heading", { name: "Coverage" }).parentElement!;
    expect(coverage).toContainElement(heading);
    expect(screen.getByText("Unverified draft")).toBeInTheDocument();
  });

  it("keeps the server sentence verbatim, once, one press away", async () => {
    const user = userEvent.setup();
    renderStrip({ knowability: KNOWABILITY });
    expect(screen.queryByText(KNOWABILITY.sentence)).toBeNull();
    const button = screen.getByRole("button", {
      name: "What can be known about Cayman Islands: the statement and the register facts behind it",
    });
    expect(button).toHaveAttribute("aria-expanded", "false");
    await user.click(button);
    expect(button).toHaveAttribute("aria-expanded", "true");
    expect(screen.getAllByText(KNOWABILITY.sentence)).toHaveLength(1);
    expect(screen.getByText("Threshold wording")).toBeInTheDocument();
    expect(screen.getByText("25 % or more")).toBeInTheDocument();
    expect(screen.getByText("no Cayman Islands register")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "www.ciregistry.ky/beneficial-ownership" })).toHaveAttribute(
      "href",
      "https://www.ciregistry.ky/beneficial-ownership",
    );
    expect(screen.getByText(/Rendered as of 18 Sept 2026/)).toBeInTheDocument();
  });

  it("is never a signal: the statement adds nothing to the What we found count", () => {
    renderStrip({ knowability: KNOWABILITY, riskSignals: [], contextSignals: [] });
    const found = screen.getByRole("heading", { name: "What we found" }).parentElement!;
    expect(found.textContent).toContain("0 signals");
  });
});

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

  it("counts the signals and links to them, never printing the chips a second time (Phase 245)", async () => {
    const onShowSignals = vi.fn();
    renderStrip({ onShowSignals });
    // The chips are the Risk signals section's. The Opus 5.5 check found the
    // same related-party chips twice in one QuickCheck, here and there.
    expect(screen.queryByRole("button", { name: /Related PEP/ })).toBeNull();
    const link = screen.getByRole("link", { name: "Read them under Risk signals" });
    expect(link).toHaveAttribute("href", "#risk-signals");
    await userEvent.click(link);
    expect(onShowSignals).toHaveBeenCalledOnce();
  });

  it("counts risk and structural signals apart", () => {
    renderStrip();
    // The numeral is its own span for typographic reasons, so the sentence is
    // split across elements: read the paragraph, not a text node.
    const found = screen.getByRole("heading", { name: "What we found" }).parentElement!;
    expect(found.textContent).toContain("5 signals — 4 risk, 1 structural");
  });

  it("leaves a clean result to the verdict rather than saying it twice", () => {
    renderStrip({
      riskSignals: [],
      contextSignals: [],
      verdict: "No risk signals surfaced across the sources that answered.",
    });
    expect(
      screen.getAllByText("No risk signals surfaced across the sources that answered."),
    ).toHaveLength(1);
    expect(screen.queryByRole("link", { name: /Risk signals/ })).toBeNull();
  });

  it("still says what a zero means when the backend sent no sentence", () => {
    renderStrip({ riskSignals: [], contextSignals: [], verdict: null });
    expect(
      screen.getByText("No risk signals surfaced across the sources that answered."),
    ).toBeInTheDocument();
  });

  it("says it is still checking rather than clean while the screen is open", () => {
    renderStrip({ riskSignals: [], contextSignals: [], screening: true });
    expect(screen.getByText("Still checking.")).toBeInTheDocument();
    expect(
      screen.queryByText("No risk signals surfaced across the sources that answered."),
    ).not.toBeInTheDocument();
  });

  it("states coverage once: answered over applicable, then which sources were in question", () => {
    renderStrip();
    const coverage = screen.getByRole("heading", { name: "Coverage" }).parentElement!;
    expect(coverage.textContent).toContain("11 of 11 sources answered");
    expect(coverage.textContent).toContain("11 of OpenCheck's 40 sources apply to a GB company.");
    // The old second line ("…; every one answered.") restated the first.
    expect(coverage.textContent).not.toContain("every one answered");
  });

  it("points at the notice for a check that did not run, rather than repeating it", async () => {
    const onShowDegraded = vi.fn();
    renderStrip({
      degraded: [
        { source_id: "wikidata", check: "source_fetch", reason: "timeout", detail: "", affected_signals: [] },
      ],
      onShowDegraded,
    });
    expect(screen.queryByText(/not a clean screen/)).toBeNull();
    const link = screen.getByRole("link", { name: "One check did not run — see which" });
    await userEvent.click(link);
    expect(onShowDegraded).toHaveBeenCalledOnce();
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
