/**
 * Viewport breakpoints the layout has to *reorder* for, not just restyle
 * (Phase 245).
 *
 * Tailwind's responsive classes restyle; they cannot move a block in the
 * document without separating DOM order from visual order, and a report whose
 * focus order runs subject → tabs → verdict while the eye reads subject →
 * verdict → tabs fails WCAG 2.4.3 at one breakpoint or the other. So where the
 * order itself changes — the mode tabs sit directly under the subject on a
 * phone, under the verdict from `sm` up — the component renders one
 * arrangement or the other, and this says which.
 *
 * `PHONE_MAX_WIDTH_PX` is Tailwind's `sm` boundary minus one: at 640px the
 * `sm:` classes switch on, so the reorder and the restyle change together.
 */
import { useSyncExternalStore } from "react";

export const PHONE_MAX_WIDTH_PX = 639;

export const PHONE_QUERY = `(max-width: ${PHONE_MAX_WIDTH_PX}px)`;

type MatchMediaWindow = {
  matchMedia?: (q: string) => {
    matches: boolean;
    addEventListener?: (type: "change", cb: () => void) => void;
    removeEventListener?: (type: "change", cb: () => void) => void;
  };
};

function currentWindow(): MatchMediaWindow | undefined {
  return typeof window === "undefined" ? undefined : (window as unknown as MatchMediaWindow);
}

/** Pure read, for tests and first render. No `matchMedia` (jsdom, SSR) → not a phone. */
export function isPhoneViewport(win: MatchMediaWindow | undefined = currentWindow()): boolean {
  try {
    return win?.matchMedia?.(PHONE_QUERY).matches ?? false;
  } catch {
    return false;
  }
}

function subscribe(onChange: () => void): () => void {
  const mq = currentWindow()?.matchMedia?.(PHONE_QUERY);
  if (!mq?.addEventListener) return () => {};
  mq.addEventListener("change", onChange);
  return () => mq.removeEventListener?.("change", onChange);
}

/** True below Tailwind's `sm`, and re-renders when the viewport crosses it. */
export function useIsPhone(): boolean {
  return useSyncExternalStore(subscribe, () => isPhoneViewport(), () => false);
}
