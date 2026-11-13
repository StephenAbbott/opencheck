/**
 * ComprasGovBrAwardsList — the Compras.gov.br card lists the awards
 * themselves, the way the AusTender card lists its notices. Checked here:
 * each award shows its buyer with the sphere of government, value,
 * procedure and legal basis; a branch's award names the establishment; the
 * values are said not to be money paid; the portal is the one link; and the
 * list expands past five.
 */
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { ComprasGovBrAwardsList } from "./SourceBucketCard";
import type { SourceHit } from "../../lib/api";

function award(i: number, over: Record<string, unknown> = {}) {
  return {
    key: `1600300500012026${i}/1`,
    process: `1600300500012026${i}`,
    awarded: `2026-09-${String(28 - i).padStart(2, "0")}`,
    buyer: "COMANDO DO EXERCITO",
    buyer_sphere: "Federal",
    buyer_uasg: "160030",
    object: `Aquisição de combustível ${i}`,
    value_brl: 1000 * (i + 1),
    procedure: "Competitive procurement (edital)",
    legal_basis: "Lei 14.133/2021, Art. 28, I",
    establishment: "33.000.167/0001-01",
    other_suppliers: 0,
    ...over,
  };
}

function hit(awards: unknown[], over: Record<string, unknown> = {}): SourceHit {
  return {
    source_id: "compras_gov_br",
    hit_id: "33000167000101",
    kind: "entity",
    name: "PETROLEO BRASILEIRO S.A. - PETROBRAS",
    summary: "",
    identifiers: {},
    is_stub: false,
    raw: {
      cnpj: "33000167000101",
      cnpj_display: "33.000.167/0001-01",
      supplier: { awards: 4210, value_brl: 912345678, buyers: 301, first_date: "2021-08-16", zero_value: 3 },
      awards,
      link: "https://www.gov.br/compras/pt-br",
      ...over,
    },
  } as SourceHit;
}

describe("ComprasGovBrAwardsList", () => {
  it("shows the buyer with its sphere, value, procedure and legal basis", () => {
    render(<ComprasGovBrAwardsList hit={hit([award(0)])} />);
    const meta = screen.getByText(/COMANDO DO EXERCITO/).textContent ?? "";
    expect(meta).toContain("COMANDO DO EXERCITO (federal)");
    expect(meta).toContain("BRL 1,000");
    expect(meta).toContain("Competitive procurement (edital)");
    expect(meta).toContain("Lei 14.133/2021, Art. 28, I");
    expect(meta).not.toContain("won by establishment");
    expect(screen.getByText(/4,210 awards since 2021/)).toBeTruthy();
    expect(screen.getByText(/not money paid/)).toBeTruthy();
    expect(screen.getByText(/3 awards were published with a value of zero/)).toBeTruthy();
  });

  it("names a branch that won, and the portal as the only link", () => {
    render(
      <ComprasGovBrAwardsList
        hit={hit([award(0, { establishment: "33.000.167/0042-21", other_suppliers: 1 })])}
      />,
    );
    const meta = screen.getByText(/COMANDO DO EXERCITO/).textContent ?? "";
    expect(meta).toContain("won by establishment 33.000.167/0042-21");
    expect(meta).toContain("shared with another company");
    const links = screen.getAllByRole("link");
    expect(links).toHaveLength(1);
    expect(links[0].getAttribute("href")).toBe("https://www.gov.br/compras/pt-br");
  });

  it("shows five awards, then the rest on request", async () => {
    render(<ComprasGovBrAwardsList hit={hit(Array.from({ length: 8 }, (_, i) => award(i)))} />);
    expect(screen.getAllByText(/Aquisição de combustível/)).toHaveLength(5);
    await userEvent.click(screen.getByRole("button"));
    expect(screen.getAllByText(/Aquisição de combustível/)).toHaveLength(8);
  });

  it("renders nothing with no awards", () => {
    const { container } = render(<ComprasGovBrAwardsList hit={hit([])} />);
    expect(container.textContent).toBe("");
  });
});
