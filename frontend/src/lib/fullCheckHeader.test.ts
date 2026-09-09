/**
 * What the FullCheck header says, pinned as values.
 *
 * The header is the surface where a count-built sentence does the most damage,
 * because every number in it is zero on some real network: a subject nobody
 * else has heard of has one source, a run that finds nothing still ran, and a
 * clean screen is a result rather than an absence of one. So the zero branches
 * are the tests, and the last guard here fails the build on any sentence that
 * ships a bare "0" to a reader.
 */
import { describe, expect, it } from "vitest";

import {
  expansionLead,
  layerControl,
  networkRiskSentence,
  runHelper,
  summaryParts,
  type NetworkShape,
} from "./fullCheckHeader";

const shape = (over: Partial<NetworkShape> = {}): NetworkShape => ({
  companies: 14,
  people: 3,
  sources: 4,
  corroborated: 6,
  ...over,
});

describe("expansionLead", () => {
  it("is null when nothing has been expanded — there is no summary to draw", () => {
    expect(expansionLead({ runDepth: null, manualLayers: 0 })).toBeNull();
    expect(expansionLead({ runDepth: 0, manualLayers: 0 })).toBeNull();
  });

  it("names the depth an eager run reached", () => {
    expect(expansionLead({ runDepth: 2, manualLayers: 0 })).toBe("depth 2");
  });

  it("counts layers walked by hand, singular and plural", () => {
    expect(expansionLead({ runDepth: null, manualLayers: 1 })).toBe("1 layer by hand");
    expect(expansionLead({ runDepth: null, manualLayers: 3 })).toBe("3 layers by hand");
  });

  it("keeps the two apart when a reader deepens a run by hand", () => {
    // They are different claims: a budget the run was allowed, and a hop the
    // reader chose. Collapsing them to "depth 3" would assert the run went
    // somewhere it did not.
    expect(expansionLead({ runDepth: 2, manualLayers: 1 })).toBe("depth 2 + 1 layer by hand");
  });
});

describe("summaryParts", () => {
  const lead = { runDepth: 2, manualLayers: 0 };

  it("reads as the line the header renders", () => {
    expect(summaryParts(shape(), lead)).toEqual([
      "depth 2",
      "14 companies, 3 people, 4 sources",
      "6 corroborated",
    ]);
  });

  it("omits people, corroboration and sources at zero rather than printing them", () => {
    expect(summaryParts(shape({ people: 0, corroborated: 0, sources: 0 }), lead)).toEqual([
      "depth 2",
      "14 companies",
    ]);
  });

  it("is singular where a network holds one of a thing", () => {
    const parts = summaryParts(shape({ companies: 1, people: 1, sources: 1 }), lead);
    expect(parts[1]).toBe("1 company, 1 person, 1 source");
  });
});

describe("networkRiskSentence", () => {
  it("before a run, reports QuickCheck and names the action that widens it", () => {
    expect(networkRiskSentence({ subject: 1, additional: 0, hasRun: false })).toBe(
      "QuickCheck flagged 1 risk signal in the records gathered so far. " +
        "Run FullCheck to screen the wider network for risk.",
    );
  });

  it("says 'no risk signals', never '0 risk signals'", () => {
    expect(networkRiskSentence({ subject: 0, additional: 0, hasRun: false })).toContain(
      "flagged no risk signals",
    );
  });

  it("after a run, leads with what FullCheck itself contributed", () => {
    const s = networkRiskSentence({ subject: 1, additional: 2, hasRun: true });
    expect(s.startsWith("FullCheck surfaced 2 risk signals")).toBe(true);
    expect(s).toContain("beyond the 1 QuickCheck flagged");
  });

  it("states a clean wider network in the same voice as a dirty one", () => {
    // Silence reads as "nothing to see" — the rule the source findings follow.
    expect(networkRiskSentence({ subject: 2, additional: 0, hasRun: true })).toBe(
      "FullCheck screened the wider network and found nothing beyond the 2 signals " +
        "QuickCheck flagged on the subject.",
    );
    expect(networkRiskSentence({ subject: 0, additional: 0, hasRun: true })).toBe(
      "FullCheck screened the wider network and found no risk signals, and QuickCheck " +
        "flagged none on the subject.",
    );
  });

  it("does not imply a subject finding when only the network is dirty", () => {
    expect(networkRiskSentence({ subject: 0, additional: 3, hasRun: true })).toContain(
      "QuickCheck flagged none on the subject",
    );
  });
});

describe("layerControl", () => {
  it("carries the frontier count, which is the one thing Run FullCheck cannot say", () => {
    const c = layerControl({ frontier: 2, noun: "owners and controllers", busy: false });
    expect(c).toMatchObject({ label: "+1 layer", count: 2, disabled: false });
    expect(c.ariaLabel).toContain("2 companies");
    expect(c.ariaLabel).toContain("owners and controllers");
  });

  it("changes the VISIBLE text at an empty frontier, not only the label", () => {
    // A disabled control is not focusable, so an aria-label explaining why is
    // announced to nobody.
    const c = layerControl({ frontier: 0, noun: "owners and controllers", busy: false });
    expect(c.label).toBe("No more layers");
    expect(c.disabled).toBe(true);
    expect(c.count).toBeNull();
  });

  it("says so while a layer is in flight", () => {
    expect(layerControl({ frontier: 4, noun: "subsidiaries", busy: true })).toMatchObject({
      label: "Adding…",
      disabled: true,
    });
  });

  it("takes its noun from the direction it digs", () => {
    expect(layerControl({ frontier: 1, noun: "subsidiaries", busy: false }).ariaLabel).toContain(
      "subsidiaries",
    );
  });
});

describe("runHelper", () => {
  it("is one line, and names the cap a reader will otherwise hit blind", () => {
    const h = runHelper({ direction: "owners", registerHops: true, cap: 150 });
    expect(h).toContain("150");
    expect(h).toContain("owners and controllers");
    expect(h.split(". ").length).toBeLessThanOrEqual(2);
  });

  it("only promises register hops where the server can make them", () => {
    expect(runHelper({ direction: "owners", registerHops: true, cap: 150 })).toContain(
      "an LEI or a readable company number",
    );
    expect(runHelper({ direction: "owners", registerHops: false, cap: 150 })).toContain("an LEI,");
  });

  it("follows the direction it was mounted in", () => {
    expect(runHelper({ direction: "subsidiaries", registerHops: true, cap: 150 })).toContain(
      "subsidiaries",
    );
  });
});

describe("no sentence ships a bare zero", () => {
  // The count-built-sentence trap: every generator here is fed its zero.
  const sentences = [
    networkRiskSentence({ subject: 0, additional: 0, hasRun: false }),
    networkRiskSentence({ subject: 0, additional: 0, hasRun: true }),
    networkRiskSentence({ subject: 0, additional: 1, hasRun: true }),
    layerControl({ frontier: 0, noun: "owners and controllers", busy: false }).label,
    layerControl({ frontier: 0, noun: "owners and controllers", busy: false }).ariaLabel,
    ...summaryParts({ companies: 1, people: 0, sources: 0, corroborated: 0 }, { runDepth: 1, manualLayers: 0 }),
  ];

  it.each(sentences)("%s", (sentence) => {
    expect(sentence).not.toMatch(/(^|\s)0(\s|$)/);
  });
});
