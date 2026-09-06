/**
 * EitiAssessmentCard — the claims that live in the markup (Phase 11, B3).
 *
 * Everything true of the *values* is in `lib/eitiAssessment.test.ts` next door
 * and stays there. What is here can only be seen by rendering:
 *
 * - the caption is on screen **whenever a declared name is**, because a list of
 *   company names with no caption is a list a reader will take for companies
 *   OpenCheck identified;
 * - the disclosure link has an accessible name that says what it is and that it
 *   leaves the page;
 * - the row count is what the collapse control says it is;
 * - a failed `/subsidiaries` fetch does not print "Declared to EITI only" on
 *   every row.
 */
import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen, within } from "@testing-library/react";
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

describe("the declared-subsidiary list", () => {
  it("shows twelve rows, then all twenty when asked", async () => {
    render(<EitiAssessmentCard hit={hit()} />);
    const list = screen.getByTestId("eiti-declared-list");
    expect(within(list).getAllByRole("listitem")).toHaveLength(12);

    await userEvent.click(
      screen.getByRole("button", { name: /Show all 20 declared names/ }),
    );
    expect(within(list).getAllByRole("listitem")).toHaveLength(20);
  });

  it("keeps the caption on screen for as long as a row is", async () => {
    // Visible text, never a `title` attribute — Phase 124's rule, and the whole
    // reason this list is safe to render at all.
    render(<EitiAssessmentCard hit={hit()} />);
    expect(screen.getByText(EITI_DECLARED_CAPTION)).toBeVisible();

    await userEvent.click(
      screen.getByRole("button", { name: /Show all 20 declared names/ }),
    );
    expect(screen.getByText(EITI_DECLARED_CAPTION)).toBeVisible();
  });

  it("says the names are not in the graph", () => {
    render(<EitiAssessmentCard hit={hit()} />);
    expect(
      screen.getByText("published elsewhere · not in the graph"),
    ).toBeVisible();
  });

  it("renders no list, and says why, when EITI filed none", () => {
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
    expect(screen.queryByTestId("eiti-declared-list")).toBeNull();
    expect(screen.getByText(/carries no list for this company/)).toBeVisible();
    // No comparison to make, so no GLEIF call to spend.
    expect(getSubsidiaries).not.toHaveBeenCalled();
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

describe("the cross-reference", () => {
  it("counts the overlap in both directions", async () => {
    getSubsidiaries.mockResolvedValue(
      subsidiariesResponse([
        { lei: "5493001KJTIIGC8Y1R12", name: "Declared Subsidiary 1 Limited" },
        { lei: "5493001KJTIIGC8Y1R13", name: "Something GLEIF Only Ltd" },
      ]),
    );
    render(<EitiAssessmentCard hit={hit()} />);

    expect(
      await screen.findByText(
        "20 declared to EITI · 1 also appear in GLEIF Level 2 · 19 declared to EITI only",
      ),
    ).toBeVisible();
    // The direction of the difference, said out loud rather than hidden.
    expect(
      screen.getByText(/neither list contains the other/),
    ).toBeVisible();
  });

  it("never borrows the corroboration glyphs for a name match", async () => {
    getSubsidiaries.mockResolvedValue(
      subsidiariesResponse([
        { lei: "5493001KJTIIGC8Y1R12", name: "Declared Subsidiary 1 Limited" },
      ]),
    );
    const { container } = render(<EitiAssessmentCard hit={hit()} />);
    await screen.findByText(/also appear in GLEIF Level 2/);
    // ●◐○ mean "corroborated by two or more sources". A name match is not that.
    expect(container.textContent ?? "").not.toMatch(/[●◐○]/);
    expect(screen.getAllByText(/Name only/).length).toBeGreaterThan(0);
  });

  it("reports a failed fetch rather than printing it as a discrepancy", async () => {
    getSubsidiaries.mockRejectedValue(new Error("503 Service Unavailable"));
    const onPanelError = vi.fn();
    render(<EitiAssessmentCard hit={hit()} onPanelError={onPanelError} />);

    expect(await screen.findByRole("status")).toHaveTextContent(
      /could not be fetched, so no comparison was run/,
    );
    expect(screen.queryByText("Declared to EITI only")).toBeNull();
    expect(onPanelError).toHaveBeenCalledWith(
      expect.objectContaining({ panel: "subsidiaries" }),
    );
  });

  it("treats a degraded 200 the same way", async () => {
    getSubsidiaries.mockResolvedValue({
      ...subsidiariesResponse([]),
      children_available: false,
      degraded_detail: "GLEIF rate-limited the direct-children call",
    });
    const onPanelError = vi.fn();
    render(<EitiAssessmentCard hit={hit()} onPanelError={onPanelError} />);

    expect(await screen.findByRole("status")).toBeVisible();
    expect(onPanelError).toHaveBeenCalledWith(
      expect.objectContaining({
        panel: "subsidiaries",
        detail: "GLEIF rate-limited the direct-children call",
      }),
    );
  });
});
