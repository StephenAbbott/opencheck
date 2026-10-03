/**
 * ChileCompraRecordsList — the ChileCompra card lists the contracts themselves,
 * the way TED's card lists its notices (Phase 281). What is checked here can
 * only be seen by rendering: each record is a link to its Mercado Público
 * page, the role chip says won / bid / purchase order, a won tender shows the
 * amount in its own currency, a direct award shows ChileCompra's reason, and
 * the footnote states the window and the totals the list is drawn from.
 */
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { ChileCompraRecordsList } from "./SourceBucketCard";
import type { SourceHit } from "../../lib/api";

function record(i: number, over: Record<string, unknown> = {}) {
  return {
    kind: "order",
    code: `1-${i}-SE26`,
    date: `2026-09-${String(28 - i).padStart(2, "0")}`,
    buyer: "HOSPITAL GUILLERMO GRANT",
    title: `Orden ${i}`,
    procedure: "From a tender",
    status: "",
    role: "order",
    value: 1000 * (i + 1),
    currency: "CLP",
    url: `https://www.mercadopublico.cl/PurchaseOrder/Modules/PO/DetailsPurchaseOrder.aspx?codigoOC=1-${i}-SE26`,
    ...over,
  };
}

function hit(records: unknown[], over: Partial<SourceHit> = {}): SourceHit {
  return {
    source_id: "chilecompra",
    hit_id: "76481921-7",
    kind: "entity",
    name: "SIEMENS HEALTHCARE EQUIPOS MEDICOS SPA",
    summary: "",
    identifiers: {},
    is_stub: false,
    raw: {
      rut: "76481921-7",
      window: "Oct 2025 – Sep 2026",
      supplier: { orders: 1415, tenders_bid: 81, tenders_won: 47 },
      records,
    },
    ...over,
  } as SourceHit;
}

describe("ChileCompraRecordsList", () => {
  it("lists each record as a link to Mercado Público with its role", () => {
    render(
      <ChileCompraRecordsList
        hit={hit([
          record(0, {
            kind: "tender",
            code: "1641-221-LE26",
            title: "Mantenimiento tomógrafo",
            role: "won",
            value: 301046820,
            currency: "CLP",
            procedure: "Licitación Pública Mayor 1000 UTM (LP)",
            status: "Adjudicada",
            url: "https://www.mercadopublico.cl/fichaLicitacion.html?idLicitacion=1641-221-LE26",
          }),
          record(1, {
            kind: "tender",
            code: "1057-33-LP26",
            title: "Equipos de imagen",
            role: "tendered",
            value: null,
            currency: "",
            status: "Cerrada",
          }),
          record(2, {
            title: "Reparación urgente",
            procedure: "Direct award (trato directo): Emergencia, urgencia o imprevisto",
          }),
        ])}
      />,
    );
    const won = screen.getByRole("link", { name: /Mantenimiento tomógrafo/ });
    expect(won).toHaveAttribute(
      "href",
      "https://www.mercadopublico.cl/fichaLicitacion.html?idLicitacion=1641-221-LE26",
    );
    expect(screen.getByText("won")).toBeInTheDocument();
    expect(screen.getByText("bid")).toBeInTheDocument();
    expect(screen.getByText("purchase order")).toBeInTheDocument();
    expect(screen.getByText(/301,046,820 CLP/)).toBeInTheDocument();
    // A bid that was not selected says where the tender stands.
    expect(screen.getByText(/Cerrada/)).toBeInTheDocument();
    expect(screen.getByText(/Emergencia, urgencia o imprevisto/)).toBeInTheDocument();
    expect(screen.getByText(/Oct 2025 – Sep 2026/)).toBeInTheDocument();
    expect(screen.getByText(/1,415 purchase\s+orders and 81 tenders\s+bid/)).toBeInTheDocument();
  });

  it("shows five records and expands to the rest", async () => {
    render(<ChileCompraRecordsList hit={hit(Array.from({ length: 8 }, (_, i) => record(i)))} />);
    expect(screen.getAllByRole("link")).toHaveLength(5);
    await userEvent.click(screen.getByRole("button"));
    expect(screen.getAllByRole("link")).toHaveLength(8);
  });

  it("renders nothing for another source or an empty list", () => {
    const { container: other } = render(
      <ChileCompraRecordsList hit={hit([record(0)], { source_id: "ted_eu" })} />,
    );
    expect(other).toBeEmptyDOMElement();
    const { container: empty } = render(<ChileCompraRecordsList hit={hit([])} />);
    expect(empty).toBeEmptyDOMElement();
  });
});
