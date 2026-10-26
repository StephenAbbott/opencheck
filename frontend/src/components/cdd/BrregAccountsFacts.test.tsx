/**
 * The Norwegian card's accounts line: the latest period's details and one
 * link to the register's PDF copy of the filing. Read from the hit, so it
 * renders on a saved report without a fetch.
 */
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";

import { BrregAccountsFacts } from "./SourceBucketCard";
import type { SourceHit } from "../../lib/api";

function hit(accounts: unknown): SourceHit {
  return {
    source_id: "brreg",
    hit_id: "929403185",
    kind: "entity",
    name: "ARNTZEN BAKERI AS",
    summary: "NO-ORGNR 929403185",
    identifiers: {},
    is_stub: false,
    raw: { navn: "ARNTZEN BAKERI AS", accounts },
  } as SourceHit;
}

describe("BrregAccountsFacts", () => {
  it("renders the facts and one link to the filed PDF", () => {
    render(
      <BrregAccountsFacts
        hit={hit({
          status: "filed",
          period_start: "2025-01-01",
          period_end: "2025-12-31",
          year: 2025,
          journal_number: "2026123456",
          small_company: true,
          rules: "regnskapslovenAlminneligRegler",
          currency: "NOK",
          pdf_url:
            "https://data.brreg.no/regnskapsregisteret/regnskap/aarsregnskap/kopi/929403185/2025",
        })}
      />,
    );
    expect(screen.getByText(/Journal no\. 2026123456/)).toBeTruthy();
    expect(screen.getByText(/Norwegian Accounting Act, general rules/)).toBeTruthy();
    const links = screen.getAllByRole("link");
    expect(links).toHaveLength(1);
    expect(links[0].getAttribute("href")).toMatch(/kopi\/929403185\/2025$/);
    expect(links[0].textContent).toMatch(/Filed annual accounts 2025 \(PDF\)/);
  });

  it("renders nothing for a company with no accounts", () => {
    const { container } = render(<BrregAccountsFacts hit={hit({ status: "none" })} />);
    expect(container.textContent).toBe("");
  });

  it("renders nothing for a hit without an accounts summary", () => {
    const { container } = render(<BrregAccountsFacts hit={hit(undefined)} />);
    expect(container.textContent).toBe("");
  });
});
