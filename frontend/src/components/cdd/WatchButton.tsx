/**
 * WatchButton — "Watch for changes" on the SubjectCard (Phase 215).
 *
 * Three states, decided from the list this browser holds: not watching
 * (the button), watching (a link to the watchlist), and the two
 * transitions. What watching *means* — re-checked only when GLEIF or
 * OpenSanctions publish a change, nothing polled — is explained on
 * /watchlist, not here: a sentence in the subject card was tried on the
 * day Phase 215 shipped and removed as too much text for the card.
 *
 * No token yet? The first Watch mints one, and the browser keeps it. There
 * is nothing to sign up for.
 *
 * A token the instance no longer knows (the list was pruned, or it was
 * minted against another instance) is forgotten the moment the backend
 * says so — on load, quietly; on Watch, by minting a fresh list and saying
 * that the old one is gone. The browser never carries a dead capability
 * around, and the button never fails twice for the same reason.
 */

import { useEffect, useState } from "react";

import { addWatch, getWatchlist, isUnknownWatchlist } from "../../lib/api";
import { trackEvent } from "../../lib/analytics";
import {
  forgetToken,
  readToken,
  storeToken,
  UNKNOWN_LIST_NOTICE,
  WATCH_LABEL,
  WATCHING_LABEL,
} from "../../lib/watchlist";
import { Button, buttonClasses } from "../ui";

type State = "unknown" | "idle" | "saving" | "watching" | "error";

export function WatchButton({
  lei,
  onOpenWatchlist,
  onStateChange,
}: {
  lei: string;
  /** Open the /watchlist view in the app rather than by full page load. */
  onOpenWatchlist: () => void;
  /** Lets the parent show or hide the promise sentence. */
  onStateChange?: (watching: boolean) => void;
}) {
  const [state, setState] = useState<State>("unknown");
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const token = readToken();
    if (!token) {
      setState("idle");
      onStateChange?.(false);
      return;
    }
    getWatchlist(token)
      .then((list) => {
        if (cancelled) return;
        const watching = list.watches.some((w) => w.lei === lei);
        setState(watching ? "watching" : "idle");
        onStateChange?.(watching);
      })
      .catch((err) => {
        if (cancelled) return;
        // An unknown token (the list was pruned, or another instance's) is
        // not an error to show — but it is forgotten, so the next Watch
        // mints a new list instead of failing with it. Any other failure
        // (network, 5xx) keeps the token: the list may still be there.
        if (isUnknownWatchlist(err)) forgetToken();
        setState("idle");
        onStateChange?.(false);
      });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [lei]);

  const watch = async () => {
    setState("saving");
    setError(null);
    setNotice(null);
    try {
      const held = readToken();
      let res;
      try {
        res = await addWatch(lei, held);
      } catch (err) {
        // The held token names no list here: drop it and mint a new one,
        // once. A 404 without a token is a different fault and is shown.
        if (!held || !isUnknownWatchlist(err)) throw err;
        forgetToken();
        res = await addWatch(lei, null);
        setNotice(UNKNOWN_LIST_NOTICE);
      }
      storeToken(res.token);
      trackEvent("watch_add");
      setState("watching");
      onStateChange?.(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not watch this company.");
      setState("error");
    }
  };

  if (state === "unknown") return null;

  if (state === "watching") {
    return (
      <div className="flex flex-col items-end gap-1">
        <a
          href="/watchlist"
          onClick={(e) => {
            e.preventDefault();
            onOpenWatchlist();
          }}
          className={buttonClasses("secondary", "sm")}
          data-testid="watch-open"
        >
          {WATCHING_LABEL} →
        </a>
        {notice && (
          <p role="status" className="max-w-[16rem] text-right text-oo-meta text-oo-muted">
            {notice}
          </p>
        )}
      </div>
    );
  }

  return (
    <div className="flex flex-col items-end gap-1">
      <Button
        variant="secondary"
        size="sm"
        onClick={watch}
        disabled={state === "saving"}
        data-testid="watch-add"
      >
        {state === "saving" ? "Adding…" : WATCH_LABEL}
      </Button>
      {error && (
        <p role="alert" className="max-w-[16rem] text-right text-oo-meta text-oo-warn-text">
          {error}
        </p>
      )}
    </div>
  );
}
