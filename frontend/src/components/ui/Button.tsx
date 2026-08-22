/**
 * Button — the one button in the design system.
 *
 * Before Phase 122 the app carried nine visually distinct button styles
 * (solid oo-blue with two different radii, an outline, a soft pill, an
 * emerald pill, an amber outline, a red outline, a violet solid, and a run
 * of bare underlined text buttons), which is why the same action looked
 * different depending on which component you happened to be in. The
 * variants below are those nine collapsed into the five meanings they were
 * actually expressing.
 *
 * `buttonClasses` is exported separately so an `<a>` that must look like a
 * button can reuse the exact string without a second implementation — that
 * duplication is how the styles diverged the first time.
 */

export type ButtonVariant = "primary" | "secondary" | "ghost" | "warn" | "danger";
export type ButtonSize = "sm" | "md";

/** Shared by every variant: the hit target, the focus ring, the radius. */
const BASE =
  "inline-flex items-center justify-center gap-2 rounded-oo border font-body " +
  "transition-colors focus-visible:outline-none focus-visible:ring-2 " +
  "focus-visible:ring-oo-blue focus-visible:ring-offset-1 " +
  "disabled:opacity-60 disabled:cursor-not-allowed";

/**
 * 44px minimum on touch (`md`) per WCAG 2.5.5; `sm` is 36px and is for
 * pointer-dense rows only — never for a page's primary action.
 */
const SIZES: Record<ButtonSize, string> = {
  sm: "min-h-[36px] px-3 text-oo-small",
  md: "min-h-[44px] px-4 text-oo-body",
};

const VARIANTS: Record<ButtonVariant, string> = {
  // Hover deliberately darkens the same hue. The v1 primary hovered to
  // oo-burst, a grey-navy, so a blue button turned grey under the cursor.
  primary:
    "bg-oo-blue border-oo-blue text-white font-bold hover:bg-[#3529b8] active:bg-[#2e2399]",
  secondary:
    "bg-white border-oo-softBorder text-oo-blue font-medium hover:bg-oo-soft",
  ghost:
    "bg-transparent border-transparent text-oo-blue font-medium hover:bg-oo-soft",
  // Incomplete, not failed: the re-run affordance on a degraded check.
  warn: "bg-white border-oo-warn-border text-oo-warn-text font-bold hover:bg-oo-warn-bg",
  // Reserved for a failure the user must act on. Not for empty results.
  danger: "bg-white border-rose-300 text-rose-700 font-bold hover:bg-rose-50",
};

export function buttonClasses(
  variant: ButtonVariant = "secondary",
  size: ButtonSize = "md",
  className = "",
): string {
  return [BASE, SIZES[size], VARIANTS[variant], className]
    .filter(Boolean)
    .join(" ")
    .trim();
}

export function Button({
  variant = "secondary",
  size = "md",
  className = "",
  type = "button",
  children,
  ...rest
}: {
  variant?: ButtonVariant;
  size?: ButtonSize;
} & React.ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button type={type} className={buttonClasses(variant, size, className)} {...rest}>
      {children}
    </button>
  );
}
