/**
 * WatchButton — the token is a capability, and a dead one is dropped.
 *
 * The case these pin: a browser holding a token the instance no longer
 * knows (the list was pruned, or it was minted elsewhere). Before this,
 * every Watch sent the dead token, got "No watchlist with that token."
 * back, and stopped — the button could never succeed again without the
 * user clearing storage by hand.
 */

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { TOKEN_KEY, UNKNOWN_LIST_NOTICE, WATCH_LABEL, WATCHING_LABEL } from "../../lib/watchlist";
import { WatchButton } from "./WatchButton";

const LEI = "5493005044RTLQ5RZU70";

const LIST = {
  watches: [],
  entries: [],
  caps: { per_list: 10, total: 200, in_list: 0, total_watched: 0 },
  feed_url: "https://api.example/watch/fresh.atom",
  tiers: {
    gleif: { available: true, watermark: null, refresh_enabled: true, last_applied_at: null, last_delta: null, rows_applied: {}, record_count: null },
    opensanctions: { available: true, last_version: null, last_checked_at: null },
    worker: { enabled: true, interval_s: 300 },
  },
};

type Call = { url: string; method: string; body: unknown };

/** A backend that knows only the token `good`. */
function mockBackend(): Call[] {
  const calls: Call[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      const method = init?.method ?? "GET";
      const body = init?.body ? JSON.parse(String(init.body)) : null;
      calls.push({ url: String(url), method, body });
      const notFound = { ok: false, status: 404, json: async () => ({ detail: "No watchlist with that token." }) } as Response;
      if (method === "GET") {
        return /\/watch\/good$/.test(String(url))
          ? ({ ok: true, status: 200, json: async () => LIST } as Response)
          : notFound;
      }
      if (body?.token && body.token !== "good") return notFound;
      return { ok: true, status: 200, json: async () => ({ ...LIST, token: body?.token ?? "fresh" }) } as Response;
    }),
  );
  return calls;
}

describe("WatchButton", () => {
  beforeEach(() => {
    window.localStorage.clear();
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("forgets a held token the instance does not know, on load", async () => {
    window.localStorage.setItem(TOKEN_KEY, "stale");
    const calls = mockBackend();
    render(<WatchButton lei={LEI} onOpenWatchlist={() => {}} />);
    await waitFor(() => expect(screen.getByTestId("watch-add")).toHaveTextContent(WATCH_LABEL));
    expect(calls).toHaveLength(1);
    expect(window.localStorage.getItem(TOKEN_KEY)).toBeNull();
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("keeps the token when the load fails for another reason", async () => {
    window.localStorage.setItem(TOKEN_KEY, "good");
    vi.stubGlobal("fetch", vi.fn(async () => ({ ok: false, status: 503, json: async () => ({ detail: "down" }) }) as Response));
    render(<WatchButton lei={LEI} onOpenWatchlist={() => {}} />);
    await waitFor(() => expect(screen.getByTestId("watch-add")).toBeInTheDocument());
    expect(window.localStorage.getItem(TOKEN_KEY)).toBe("good");
  });

  it("on Watch, mints a new list once when the held token is dead, and says so", async () => {
    window.localStorage.setItem(TOKEN_KEY, "stale");
    const calls = mockBackend();
    // The load effect forgets the stale token; put it back after mount, as
    // a second tab or an older page state could, so Watch itself meets it.
    render(<WatchButton lei={LEI} onOpenWatchlist={() => {}} />);
    await waitFor(() => expect(screen.getByTestId("watch-add")).toBeInTheDocument());
    window.localStorage.setItem(TOKEN_KEY, "stale");

    fireEvent.click(screen.getByTestId("watch-add"));
    await waitFor(() => expect(screen.getByTestId("watch-open")).toHaveTextContent(WATCHING_LABEL));

    const posts = calls.filter((c) => c.method === "POST");
    expect(posts).toHaveLength(2);
    expect(posts[0].body).toEqual({ lei: LEI, token: "stale" });
    expect(posts[1].body).toEqual({ lei: LEI });
    expect(window.localStorage.getItem(TOKEN_KEY)).toBe("fresh");
    expect(screen.getByRole("status")).toHaveTextContent(UNKNOWN_LIST_NOTICE);
  });

  it("shows the refusal, and retries nothing, when there was no token to blame", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => ({ ok: false, status: 404, json: async () => ({ detail: "GLEIF has no record for that LEI." }) }) as Response),
    );
    render(<WatchButton lei={LEI} onOpenWatchlist={() => {}} />);
    await waitFor(() => expect(screen.getByTestId("watch-add")).toBeInTheDocument());
    fireEvent.click(screen.getByTestId("watch-add"));
    await waitFor(() => expect(screen.getByRole("alert")).toHaveTextContent("GLEIF has no record for that LEI."));
    expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1);
    expect(window.localStorage.getItem(TOKEN_KEY)).toBeNull();
  });
});
