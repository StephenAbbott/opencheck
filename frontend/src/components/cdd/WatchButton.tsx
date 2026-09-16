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
 */

import { useEffect, useState } from "react";

import { addWatch, getWatchlist } from "../../lib/api";
import { trackEvent } from "../../lib/analytics";
import { readToken, storeToken, WATCH_LABEL, WATCHING_LABEL } from "../../lib/watchlist";
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
      .catch(() => {
        if (cancelled) return;
        // An unknown token (the list was pruned, or another origin's) is
        // not an error to show: the next Watch simply mints a new list.
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
    try {
      const res = await addWatch(lei, readToken());
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
