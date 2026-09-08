/**
 * EitiAssessmentCard — the claims that live in the markup (Phase 11, B3).
 *
 * Everything true of the *values* is in `lib/eitiAssessment.test.ts` next door
 * and stays there. What is here can only be seen by rendering:
 *
 * - the caption is on screen **whenever the declared count is**, because a
 *   count of company names with no caption reads as companies OpenCheck
 *   identified;
 * - the disclosure link has an accessible name that says what it is and that it
 *   leaves the page;
 * - the card never fetches `/subsidiaries` itself (Phase 185: the list and
 *   its cross-reference live on the Subsidiaries tab, which the card points
 *   at with a real link) — see `SubsidiariesPanel.test.tsx` for the
 *   cross-reference claims that used to be here.
 */
import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

const getSubsidiaries = vi.fn();
vi.mock("../../lib/api", () => ({
  getSubsidiaries: (...args: unknown[]) => getSubsidiaries(...args),
}));

import { EitiAssessmentCard } from "./EitiAssessmentCard";
import type { SourceHit } from "../../lib/api";
import { EITI_DECLARED_CAPTION } from "../../lib/vocab";

const SUBS = Array.from({ length: 20 }, (_, i) => ({
  name: `Declared Subsidiary ${i + 1} Limited`,
  country: i % 2 === 0 ? "COD" : "ZMB",
  years: ["2023"],
}));

function hit(over: Record<string, unknown> = {}): SourceHit {
  return {
    source_id: "eiti_assessment",
    hit_id: "2138002658CPO9NBH955",
    name: "GLENCORE",
    summary: "CHE · EITI supporting company",
    raw: {
      lei: "2138002658CPO9NBH955",
      name: "GLENCORE",
      hq_country: "CHE",
      sectors: ["Mining"],
      company_type: "Public",
      match: {
        method: "gleif_name_exact",
        reviewed: true,
        gleif_legal_name: "GLENCORE PLC",
      },
      assessments: {
        "2023": {
          exp_6: {
            response: "Yes",
            result: "Expectation met",
            bo_url: "https://www.glencore.com/transparency",
          },
        },
        "2025": { exp_6: { response: "Yes", result: "Expectation met" } },
      },
      subsidiaries: SUBS,
      ...over,
    },
  } as unknown as SourceHit;
}

function subsidiariesResponse(children: { lei: string; name: string }[]) {
  return {
    lei: "2138002658CPO9NBH955",
    available: true,
    reason: null,
    children_available: true,
    direct_available: true,
    ultimate_available: true,
    snapshot_fallback: false,
    snapshot_date: null,
    degraded_detail: null,
    direct_total: children.length,
    ultimate_total: 0,
    distinct_fetched: children.length,
    indirect_only: 0,
    node_estimate: children.length,
    render_mode: "table" as const,
    truncated: false,
    jurisdictions: [],
    children: children.map((c) => ({
      ...c,
      jurisdiction: null,
      status: null,
      relation: "direct" as const,
      link: null,
    })),
    bods: null,
  };
}

beforeEach(() => {
  getSubsidiaries.mockReset();
  getSubsidiaries.mockResolvedValue(subsidiariesResponse([]));
});

describe("the declared-subsidiary count and pointer", () => {
  it("shows the count, the caption, and a real link to the Subsidiaries tab", async () => {
    const onOpenSubsidiaries = vi.fn();
    render(<EitiAssessmentCard hit={hit()} onOpenSubsidiaries={onOpenSubsidiaries} />);
    expect(screen.getByText("20")).toBeVisible();
    expect(screen.getByText(EITI_DECLARED_CAPTION)).toBeVisible();
    // No list here any more — the names are a subsidiaries question.
    expect(screen.queryByTestId("eiti-declared-list")).toBeNull();
    const link = screen.getByTestId("eiti-subsidiaries-link");
    expect(link).toHaveAttribute("href", "/?lei=2138002658CPO9NBH955&mode=subsidiaries");
    await userEvent.click(link);
    expect(onOpenSubsidiaries).toHaveBeenCalledTimes(1);
  });

  it("never fetches the GLEIF network itself", () => {
    render(<EitiAssessmentCard hit={hit()} />);
    expect(getSubsidiaries).not.toHaveBeenCalled();
  });

  it("says the names are not in the graph", () => {
    render(<EitiAssessmentCard hit={hit()} />);
    expect(
      screen.getByText("published elsewhere · not in the graph"),
    ).toBeVisible();
  });

  it("renders no count, and says why, when EITI filed none", () => {
    render(
      <EitiAssessmentCard
        hit={hit({
          subsidiaries: [],
          assessments: {
            "2025": {
              exp_6: { result: "Not available" },
              exp_2: { result: "Expectation not met" },
            },
          },
        })}
      />,
    );
    expect(screen.queryByTestId("eiti-subsidiaries-link")).toBeNull();
    expect(screen.getByText(/carries no list for this company/)).toBeVisible();
  });
});

describe("the beneficial ownership disclosure", () => {
  it("names the link, and says it opens away from the page", () => {
    render(<EitiAssessmentCard hit={hit()} />);
    const link = screen.getByRole("link", {
      name: "Beneficial ownership disclosure (2023) (opens in new tab)",
    });
    expect(link).toHaveAttribute("href", "https://www.glencore.com/transparency");
    expect(link).toHaveAttribute("target", "_blank");
  });

  it("shows the result as a chip and the sentence beneath it", () => {
    render(<EitiAssessmentCard hit={hit()} />);
    expect(screen.getByText("Discloses beneficial ownership")).toBeVisible();
    expect(
      screen.getByText(/GLENCORE told EITI it discloses its beneficial owners/),
    ).toBeVisible();
  });

  it("shows both names when the LEI belongs to a differently-named entity", () => {
    render(
      <EitiAssessmentCard
        hit={hit({
          name: "Chevron Corporation",
          match: {
            method: "candidates_only",
            reviewed: true,
            gleif_legal_name: "Chevron U.S.A. Inc.",
          },
        })}
      />,
    );
    expect(screen.getByText("Chevron U.S.A. Inc.")).toBeVisible();
    expect(screen.getAllByText("Chevron Corporation").length).toBeGreaterThan(0);
  });
});
