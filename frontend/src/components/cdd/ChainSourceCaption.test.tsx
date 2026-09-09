/**
 * The chain-provenance caption — the claim that lives in the markup (Phase 188).
 *
 * With `OPENCHECK_CH_GRAPH_FIRST` on, the corporate-PSC chain above a UK
 * subject is proposed by OpenCheck's local copy of the PSC register and then
 * read, company by company, from Companies House. The records on screen are
 * the register's own either way, so this is provenance and not a degradation
 * — but a reader comparing two lookups is owed the difference, and owed the
 * count when the local copy did not know part of the chain.
 *
 * Only rendering can show that the caption is wired to the right field, is
 * absent for the live walk (the long-standing behaviour, which needs no
 * caption at all), and does not arrive dressed as a warning. The wording
 * itself is pinned in `lib/vocab.test.ts`.
 */
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";

import { DeepenBlock } from "./SourceBucketCard";
import type { DeepenResponse } from "../../lib/api";

function detail(chainSource: Record<string, unknown> | undefined): DeepenResponse {
  return {
    source_id: "companies_house",
    hit_id: "00070274",
    raw: {
      company_number: "00070274",
      company_name: "Vosper Thornycroft (UK) Limited",
      ...(chainSource ? { chain_source: chainSource } : {}),
    },
    bods: [],
    bods_issues: [],
    license: "OGL-3.0",
    license_notice: null,
    risk_signals: [],
  };
}

const GRAPH = {
  source: "graph",
  related: 4,
  predicted: 4,
  missed: 0,
  extra: 0,
  snapshot_date: "2026-09-08",
  stream_published_at: "2026-09-08T14:03:21",
};

describe("the chain-provenance caption", () => {
  it("is on screen when the chain came from the local PSC graph", () => {
    render(<DeepenBlock detail={detail(GRAPH)} />);
    const caption = screen.getByTestId("chain-source-caption");
    expect(caption.textContent).toContain("UK PSC register");
    expect(caption.textContent).toContain("2026-09-08");
  });

  it("is absent for a chain walked live, and for every other source", () => {
    const { rerender } = render(<DeepenBlock detail={detail({ source: "live", related: 4 })} />);
    expect(screen.queryByTestId("chain-source-caption")).toBeNull();
    rerender(<DeepenBlock detail={detail({ source: "graph_unavailable" })} />);
    expect(screen.queryByTestId("chain-source-caption")).toBeNull();
    rerender(<DeepenBlock detail={detail(undefined)} />);
    expect(screen.queryByTestId("chain-source-caption")).toBeNull();
  });

  it("says how many companies the local copy did not know", () => {
    render(<DeepenBlock detail={detail({ ...GRAPH, missed: 2 })} />);
    const caption = screen.getByTestId("chain-source-caption");
    expect(caption.textContent).toContain("2 companies");
    expect(caption.textContent).toContain("found live");
  });

  it("is not styled as a warning — nothing was refused", () => {
    render(<DeepenBlock detail={detail(GRAPH)} />);
    const caption = screen.getByTestId("chain-source-caption");
    // The amber / rose families are the report's degradation and risk tones.
    expect(caption.className).not.toMatch(/amber|rose|red/);
    expect(caption.className).toContain("text-oo-muted");
  });
});
