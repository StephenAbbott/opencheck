import { describe, expect, it } from "vitest";
import {
  attributionSentence,
  checkedClause,
  corroborationClause,
  evidenceFooter,
  evidenceForCode,
  splitEvidenceSources,
} from "./signalEvidence";
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

describe("evidenceForCode", () => {
  it("says nothing for a code that is not there", () => {
    // Which is also what a selection outliving a re-run looks like. The
    // section renders no box for it rather than an empty one.
    expect(evidenceForCode([], "SANCTIONED")).toBeNull();
    expect(evidenceForCode([sig({})], "DEBARMENT")).toBeNull();
  });

  it("explains the code asked for, and ranks nothing", () => {
    // Until Phase 132 the box opened on the *worst* signal by severity and
    // captioned it "the most serious signal is shown above" — OpenCheck
    // grading a company's findings. There is no ordering left to test: the
    // chip decides, and a chip for a lower-severity code gets the same
    // treatment as any other.
    const signals = [
      sig({ code: "SANCTIONED", summary: "the severe one" }),
      sig({ code: "OFFSHORE_LEAKS", source_id: "icij", summary: "the chosen one" }),
    ];
    expect(evidenceForCode(signals, "OFFSHORE_LEAKS")?.signal.summary).toBe(
      "the chosen one"
    );
    expect(evidenceForCode(signals, "SANCTIONED")?.signal.summary).toBe(
      "the severe one"
    );
  });

  it("explains structural context too", () => {
    const lead = evidenceForCode(
      [sig({ code: "NON_EU_JURISDICTION", kind: "context", source_id: "gleif" })],
      "NON_EU_JURISDICTION"
    );
    expect(lead?.signal.code).toBe("NON_EU_JURISDICTION");
  });

  it("counts corroboration in distinct sources, not in signals", () => {
    // The risk layer emits one signal per matching hit, so one source can
    // produce three for one code. "Corroborated by three sources" would then
    // be a claim about a single source's thoroughness.
    const lead = evidenceForCode(
      [sig({ hit_id: "a" }), sig({ hit_id: "b" }), sig({ hit_id: "c" })],
      "SANCTIONED"
    );
    expect(lead?.sourceCount).toBe(1);
    expect(lead?.sourceIds).toEqual(["opensanctions"]);
  });

  it("counts two sources as two", () => {
    const lead = evidenceForCode(
      [sig({}), sig({ source_id: "openaleph", hit_id: "b" })],
      "SANCTIONED"
    );
    expect(lead?.sourceCount).toBe(2);
    expect(lead?.sourceIds).toEqual(["opensanctions", "openaleph"]);
  });

  it("does not call two findings about DIFFERENT parties corroboration", () => {
    // Live on BP: OpenAleph flagged BP itself in the OffshoreLeaks collection
    // while ICIJ flagged a subsidiary in the Bahamas Leaks. Both are
    // OFFSHORE_LEAKS, and the box said "Corroborated by two sources" under a
    // sentence about the subsidiary. Same code is not the same finding.
    const lead = evidenceForCode(
      [
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
      ],
      "OFFSHORE_LEAKS"
    );
    expect(lead?.sourceCount).toBe(1);
    expect(lead?.sourceIds).toHaveLength(1);
  });

  it("does count two sources naming the same party", () => {
    const lead = evidenceForCode(
      [
        sig({ code: "OFFSHORE_LEAKS", source_id: "openaleph", evidence: { subject_statement_id: "x" } }),
        sig({ code: "OFFSHORE_LEAKS", source_id: "icij", evidence: { subject_statement_id: "x" } }),
      ],
      "OFFSHORE_LEAKS"
    );
    expect(lead?.sourceCount).toBe(2);
  });

  it("only counts the sources that asserted the selected code", () => {
    const lead = evidenceForCode(
      [
        sig({ code: "SANCTIONED", source_id: "opensanctions" }),
        sig({ code: "OFFSHORE_LEAKS", source_id: "icij" }),
        sig({ code: "OFFSHORE_LEAKS", source_id: "openaleph" }),
      ],
      "SANCTIONED"
    );
    expect(lead?.sourceCount).toBe(1);
  });

  it("shows the best-evidenced instance of the code", () => {
    const lead = evidenceForCode(
      [
        sig({ confidence: "low", summary: "weak", hit_id: "a" }),
        sig({ confidence: "high", summary: "strong", source_id: "openaleph", hit_id: "b" }),
      ],
      "SANCTIONED"
    );
    expect(lead?.signal.summary).toBe("strong");
  });

  it("breaks a tie stably, not on backend emit order", () => {
    const a = sig({ hit_id: "a", summary: "first" });
    const b = sig({ hit_id: "b", summary: "second" });
    expect(evidenceForCode([a, b], "SANCTIONED")?.signal.summary).toBe("first");
    expect(evidenceForCode([b, a], "SANCTIONED")?.signal.summary).toBe("first");
  });

  it("takes the OLDEST observed retrieval across the contributing sources", () => {
    // A claim is only as current as its stalest component — the same rule
    // provenance.Recorder.resolve applies. The newest would overstate.
    const lead = evidenceForCode(
      [sig({}), sig({ source_id: "openaleph", hit_id: "b" })],
      "SANCTIONED",
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
    const lead = evidenceForCode(
      [sig({}), sig({ source_id: "icij", hit_id: "b" })],
      "SANCTIONED",
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
    const lead = evidenceForCode([sig({})], "SANCTIONED", {
      opensanctions: { liveness: "stub", label: "Stub", retrieved_at: null, detail: null },
    });
    expect(lead?.checkedAt).toBeNull();
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

describe("attributionSentence", () => {
  it("names several sources in one sentence, not one each", () => {
    // The box rendered "From OpenSanctions. From EveryPolitician." — one full
    // stop per source, which reads as two findings about the party rather
    // than one finding two sources agree on.
    expect(
      attributionSentence(["opensanctions", "everypolitician"], {
        opensanctions: "OpenSanctions",
        everypolitician: "EveryPolitician",
      })
    ).toBe("From OpenSanctions and EveryPolitician.");
  });

  it("is the plain English list joiner, serial comma and all", () => {
    expect(attributionSentence(["icij", "a_source", "b_source"])).toBe(
      "From ICIJ, A Source and B Source."
    );
  });

  it("says nothing when there is no one to name", () => {
    expect(attributionSentence([])).toBe("");
  });

  it("does not name the same source twice", () => {
    expect(attributionSentence(["icij", "icij"])).toBe("From ICIJ.");
  });
});

describe("splitEvidenceSources", () => {
  it("separates what can be shown from what can only be named", () => {
    // `icij` is not a registered adapter, so no `#source-icij` exists to
    // scroll to; a button for it looked identical to the working one beside
    // it and did nothing.
    const { linked, named } = splitEvidenceSources(
      ["opensanctions", "icij", "openaleph"],
      (id) => id !== "icij"
    );
    expect(linked).toEqual(["opensanctions", "openaleph"]);
    expect(named).toEqual(["icij"]);
  });

  it("drops empty ids rather than rendering a blank attribution", () => {
    const { linked, named } = splitEvidenceSources(["", "icij"], () => false);
    expect(linked).toEqual([]);
    expect(named).toEqual(["icij"]);
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
