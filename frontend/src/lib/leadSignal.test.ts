import { describe, expect, it } from "vitest";
import {
  checkedClause,
  corroborationClause,
  evidenceFooter,
  leadSignal,
} from "./leadSignal";
import type { RiskSignal } from "./api";

const sig = (over: Partial<RiskSignal>): RiskSignal => ({
  code: "SANCTIONED",
  confidence: "high",
  summary: "Named on a sanctions list.",
  source_id: "opensanctions",
  hit_id: "h1",
  evidence: {},
  ...over,
});

describe("leadSignal", () => {
  it("says nothing when there is nothing adverse", () => {
    expect(leadSignal([])).toBeNull();
    // Structural context is not a finding against the company, so it can
    // never be the thing the section leads with.
    expect(leadSignal([sig({ code: "NON_EU_JURISDICTION", kind: "context" })])).toBeNull();
  });

  it("leads with the worst, by the same severity the graph badges stack by", () => {
    const lead = leadSignal([
      sig({ code: "COMPLEX_OWNERSHIP_LAYERS", source_id: "gleif" }),
      sig({ code: "SANCTIONED", source_id: "opensanctions" }),
      sig({ code: "OFFSHORE_LEAKS", source_id: "icij" }),
    ]);
    expect(lead?.signal.code).toBe("SANCTIONED");
  });

  it("counts corroboration in distinct sources, not in signals", () => {
    // The risk layer emits one signal per matching hit, so one source can
    // produce three for one code. "Corroborated by three sources" would then
    // be a claim about a single source's thoroughness.
    const lead = leadSignal([
      sig({ hit_id: "a" }),
      sig({ hit_id: "b" }),
      sig({ hit_id: "c" }),
    ]);
    expect(lead?.sourceCount).toBe(1);
    expect(lead?.sourceIds).toEqual(["opensanctions"]);
  });

  it("counts two sources as two", () => {
    const lead = leadSignal([sig({}), sig({ source_id: "openaleph", hit_id: "b" })]);
    expect(lead?.sourceCount).toBe(2);
    expect(lead?.sourceIds).toEqual(["opensanctions", "openaleph"]);
  });

  it("does not call two findings about DIFFERENT parties corroboration", () => {
    // Live on BP: OpenAleph flagged BP itself in the OffshoreLeaks collection
    // while ICIJ flagged a subsidiary in the Bahamas Leaks. Both are
    // OFFSHORE_LEAKS, and the box said "Corroborated by two sources" under a
    // sentence about the subsidiary. Same code is not the same finding.
    const lead = leadSignal([
      sig({
        code: "OFFSHORE_LEAKS",
        source_id: "openaleph",
        summary: "The company itself is in a leak collection.",
        evidence: { statement_id: "the-subject" },
      }),
      sig({
        code: "OFFSHORE_LEAKS",
        source_id: "icij",
        summary: "A related party matches a record in the Bahamas Leaks.",
        evidence: { subject_statement_id: "a-subsidiary" },
      }),
    ]);
    expect(lead?.sourceCount).toBe(1);
    expect(lead?.sourceIds).toHaveLength(1);
  });

  it("does count two sources naming the same party", () => {
    const lead = leadSignal([
      sig({ code: "OFFSHORE_LEAKS", source_id: "openaleph", evidence: { subject_statement_id: "x" } }),
      sig({ code: "OFFSHORE_LEAKS", source_id: "icij", evidence: { subject_statement_id: "x" } }),
    ]);
    expect(lead?.sourceCount).toBe(2);
  });

  it("only counts the sources that asserted the lead code", () => {
    const lead = leadSignal([
      sig({ code: "SANCTIONED", source_id: "opensanctions" }),
      sig({ code: "OFFSHORE_LEAKS", source_id: "icij" }),
      sig({ code: "OFFSHORE_LEAKS", source_id: "openaleph" }),
    ]);
    expect(lead?.signal.code).toBe("SANCTIONED");
    expect(lead?.sourceCount).toBe(1);
  });

  it("prefers the better-corroborated instance of the lead code", () => {
    const lead = leadSignal([
      sig({ confidence: "low", summary: "weak" }),
      sig({ confidence: "high", summary: "strong", source_id: "openaleph" }),
    ]);
    expect(lead?.signal.summary).toBe("strong");
  });

  it("takes the OLDEST observed retrieval across the contributing sources", () => {
    // A claim is only as current as its stalest component — the same rule
    // provenance.Recorder.resolve applies. The newest would overstate.
    const lead = leadSignal(
      [sig({}), sig({ source_id: "openaleph", hit_id: "b" })],
      {
        opensanctions: { liveness: "cached", label: "Cached", retrieved_at: "2026-08-19T09:00:00Z", detail: null },
        openaleph: { liveness: "live", label: "Live", retrieved_at: "2026-08-21T09:00:00Z", detail: null },
        // A source that did not assert this code must not set the date.
        icij: { liveness: "live", label: "Live", retrieved_at: "2026-08-23T09:00:00Z", detail: null },
      }
    );
    expect(lead?.checkedAt).toBe("2026-08-19T09:00:00Z");
  });

  it("reports no date at all when any contributing source reported none", () => {
    // `icij` is not a registered adapter, so it never gets a source_liveness
    // entry. Generalising OpenAleph's date onto it would state a currency for
    // the ICIJ record that nothing established.
    const lead = leadSignal(
      [sig({}), sig({ source_id: "icij", hit_id: "b" })],
      {
        opensanctions: { liveness: "live", label: "Live", retrieved_at: "2026-08-21T09:00:00Z", detail: null },
      }
    );
    expect(lead?.sourceCount).toBe(2);
    expect(lead?.checkedAt).toBeNull();
  });

  it("reports no date rather than today's when nothing was observed", () => {
    // A stub or curated source has no retrieval time. "Checked today" there
    // would be the exact claim LivenessBadge exists to avoid making.
    const lead = leadSignal([sig({})], {
      opensanctions: { liveness: "stub", label: "Stub", retrieved_at: null, detail: null },
    });
    expect(lead?.checkedAt).toBeNull();
  });

  it("breaks a severity tie stably, not on backend emit order", () => {
    // PEP and DEBARMENT are both severity 4 in SIGNAL_STYLE, and one
    // OpenSanctions entity carrying both topics emits them at the same
    // confidence from the same hit. Without a stable last step the headline
    // was decided by the order two `out.append` calls appear in risk.py.
    const pep = sig({ code: "PEP", hit_id: "h" });
    const deb = sig({ code: "DEBARMENT", hit_id: "h" });
    expect(leadSignal([pep, deb])?.signal.code).toBe("DEBARMENT");
    expect(leadSignal([deb, pep])?.signal.code).toBe("DEBARMENT");
  });

  it("does not let an unknown code outrank a known one", () => {
    const lead = leadSignal([
      sig({ code: "SANCTIONED" }),
      sig({ code: "SOME_FUTURE_CODE", source_id: "x" }),
    ]);
    expect(lead?.signal.code).toBe("SANCTIONED");
  });
});

describe("corroborationClause", () => {
  it("reserves 'corroborated' for two or more", () => {
    // It is the word the confidence legend beside these chips defines that
    // way; using it for one source would make the section disagree with its
    // own legend.
    expect(corroborationClause(1)).toBe("Reported by one source");
    expect(corroborationClause(2)).toBe("Corroborated by two sources");
    expect(corroborationClause(5)).toBe("Corroborated by five sources");
  });

  it("switches to digits past ten and says nothing at zero", () => {
    expect(corroborationClause(12)).toBe("Corroborated by 12 sources");
    expect(corroborationClause(0)).toBe("");
  });
});

describe("checkedClause", () => {
  it("formats an observed retrieval", () => {
    expect(checkedClause("2026-08-21T09:00:00Z")).toBe(
      "last checked 21 August 2026"
    );
  });

  it("says nothing for a missing or unparseable time", () => {
    expect(checkedClause(null)).toBe("");
    expect(checkedClause("not-a-date")).toBe("");
  });
});

describe("evidenceFooter", () => {
  const lead = (sourceCount: number, checkedAt: string | null) => ({
    signal: sig({}),
    sourceCount,
    sourceIds: [],
    checkedAt,
  });

  it("joins only the parts that are true", () => {
    expect(evidenceFooter(lead(2, "2026-08-21T09:00:00Z"))).toBe(
      "Corroborated by two sources, last checked 21 August 2026."
    );
    expect(evidenceFooter(lead(1, null))).toBe("Reported by one source.");
    expect(evidenceFooter(lead(0, null))).toBe("");
  });
});
