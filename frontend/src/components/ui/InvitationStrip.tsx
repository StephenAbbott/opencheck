/**
 * InvitationStrip — the "this costs a live lookup, do you want it?" control
 * (Phase 124).
 *
 * `SubsidiaryNetwork` and `NzAssociations` each carried their own copy: same
 * layout, same six hardcoded hex values, same behaviour. The audit called this
 * the concrete cost of having no shared component, and it was right in a
 * specific way — `NzAssociations` had `role="status"`, `role="alert"` and focus
 * restoration on collapse, and `SubsidiaryNetwork` did not. The accessibility
 * fix had landed in one copy and not the other, which is exactly what happens
 * when two components are the same component.
 *
 * `buttonRef` is part of the contract rather than an extra: a strip that
 * replaces itself with a panel, and then reappears when that panel collapses,
 * has to be able to take focus back — otherwise collapsing drops focus to
 * `<body>`. `NzAssociations` already did this; making the ref a prop of the
 * shared component means the next caller gets it without knowing to ask.
 *
 * The six hex literals are gone with the duplication: `oo.soft` /
 * `oo.softBorder` / `oo.blue` are the tokens those values already were.
 */

export default function InvitationStrip({
  title,
  detail,
  icon,
  onClick,
  buttonRef,
}: {
  /** What clicking gets you, as an action. */
  title: string;
  /** Where it comes from and what it costs — this control fires a live fetch. */
  detail: string;
  /** A 14×14 glyph on the 14 grid, `aria-hidden`, drawn in `currentColor`. */
  icon: React.ReactNode;
  onClick: () => void;
  /** So a caller can restore focus here when its panel collapses. */
  buttonRef?: React.Ref<HTMLButtonElement>;
}) {
  return (
    <button
      ref={buttonRef}
      type="button"
      onClick={onClick}
      className="mt-3 w-full flex items-center gap-3 rounded-oo border border-oo-softBorder bg-oo-soft px-3 py-2 text-left transition-colors hover:bg-oo-light"
    >
      <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md bg-oo-blue text-white">
        {icon}
      </span>
      <span className="min-w-0 flex-1">
        <span className="block text-oo-small font-semibold text-oo-blue leading-tight">
          {title}
        </span>
        <span className="block text-oo-meta text-oo-burst">{detail}</span>
      </span>
    </button>
  );
}
