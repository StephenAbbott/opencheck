"""og_image — render the shareable social card (og:image) for an entity.

Design: split panel, 1200×630. Left (white): OpenCheck logo + wordmark,
entity name (Bitter Bold, auto-shrunk to fit), LEI, "Visit opencheck.world"
CTA. Right (brand indigo #3d30d4): risk-signal count and the first three
signal chips in the exact RiskChip colours from the frontend, plus a
"+N more" line. Everything is drawn with Pillow at 2× and downsampled —
no headless browser (Render free tier can't afford one).

Fonts are bundled under ``assets/fonts`` (Bitter + DM Sans, both SIL OFL —
see ATTRIBUTIONS.md).

Two variants:

* **full** — name + signal count + chips (rendered when a completed lookup
  for the LEI is available, normally because the sharer just ran it).
* **teaser** — name (or just the LEI) + "34 open sources, one query"
  when no completed lookup is cached; invites the viewer to run the check.
"""

from __future__ import annotations

import io
from functools import lru_cache
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont

from . import names as _names_mod

_FONT_DIR = Path(__file__).resolve().parent / "assets" / "fonts"

# Canvas (before 2× supersampling).
W, H = 1200, 630
_SPLIT = 700  # left/white panel width; right panel = brand indigo

# Brand colours.
_INDIGO = "#3d30d4"
_NAVY = "#0d1b3e"
_CHECK_BLUE = "#2563eb"
_INK = "#191d23"
_MUTED = "#8a8a99"
_LAVENDER = "#cecbf6"  # secondary text on indigo

#: Signal code → (label, chip background, chip text). Mirrors
#: RISK_PRESENTATION in frontend/src/components/risk/RiskChip.tsx —
#: keep the two in sync when adding signals.
SIGNAL_STYLE: dict[str, tuple[str, str, str]] = {
    "PEP": ("PEP", "#f5f3ff", "#6d28d9"),
    "SANCTIONED": ("Sanctioned", "#fff1f2", "#be123c"),
    # Slate, deliberately outside the rose/amber sanctions ramp — a
    # counter-designation is context, not an adverse finding.
    "COUNTER_SANCTIONED": ("Counter-sanctioned", "#f1f5f9", "#334155"),
    "SANCTIONED_SECURITY": ("Sanctioned securities", "#fff1f2", "#be123c"),
    "SANCTIONS_CONTROLLED": ("Sanction control", "#ffe4e6", "#9f1239"),
    "SANCTIONS_LINKED": ("Sanctions-linked", "#fffbeb", "#92400e"),
    "DEBARMENT": ("Debarred", "#ffedd5", "#7c2d12"),
    "EXPORT_CONTROLLED": ("Export controlled", "#ffe4e6", "#9f1239"),
    "EXPORT_CONTROL_LINKED": ("Export control-linked", "#fffbeb", "#92400e"),
    "EXPORT_RISK": ("Trade risk", "#fff7ed", "#c2410c"),
    "OFFSHORE_LEAKS": ("Offshore leaks", "#fffbeb", "#92400e"),
    "OPAQUE_OWNERSHIP": ("Opaque ownership", "#f1f5f9", "#334155"),
    # Context, not risk — a permitted GLEIF Level 2 reporting exception.
    "GLEIF_REPORTING_EXCEPTION": ("No parent in GLEIF (exempt)", "#f8fafc", "#334155"),
    "TRUST_OR_ARRANGEMENT": ("Trust / arrangement", "#eef2ff", "#4338ca"),
    "NON_EU_JURISDICTION": ("Non-EU jurisdiction", "#fff7ed", "#c2410c"),
    "STATE_CONTROLLED": ("State-controlled", "#fff7ed", "#c2410c"),
    "NOMINEE": ("Nominee", "#fdf4ff", "#a21caf"),
    "COMPLEX_OWNERSHIP_LAYERS": ("≥3 ownership layers", "#f0f9ff", "#0369a1"),
    "COMPLEX_CORPORATE_STRUCTURE": ("Complex structure (AMLA)", "#fef2f2", "#b91c1c"),
    "POSSIBLE_OBFUSCATION": ("Possible obfuscation", "#fefce8", "#854d0e"),
    "RELATED_PEP": ("Related PEP", "#f5f3ff", "#6d28d9"),
    "RELATED_SANCTIONED": ("Related sanctioned", "#fff1f2", "#be123c"),
    "RELATED_COUNTER_SANCTIONED": ("Related counter-sanctioned", "#f1f5f9", "#334155"),
    "RELATED_SANCTIONS_CONTROLLED": ("Related sanction control", "#ffe4e6", "#9f1239"),
    "RELATED_SANCTIONS_LINKED": ("Related sanctions-linked", "#fffbeb", "#92400e"),
    "RELATED_DEBARMENT": ("Related debarred", "#fff7ed", "#9a3412"),
    "RELATED_EXPORT_CONTROLLED": ("Related export controlled", "#ffe4e6", "#9f1239"),
    "RELATED_EXPORT_CONTROL_LINKED": ("Related export control-linked", "#fffbeb", "#92400e"),
    "RELATED_EXPORT_RISK": ("Related trade risk", "#fff7ed", "#c2410c"),
    "FATF_BLACK_LIST": ("FATF black list", "#fee2e2", "#991b1b"),
    "EU_HIGH_RISK_THIRD_COUNTRY": ("EU high-risk country", "#fee2e2", "#b91c1c"),
    "FATF_GREY_LIST": ("FATF grey list", "#fff7ed", "#9a3412"),
}
_DEFAULT_STYLE = ("#f1f5f9", "#334155")


def _font(name: str, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(str(_FONT_DIR / f"{name}.ttf"), size)


# --- Script coverage --------------------------------------------------------
# The bundled faces do not cover every script an entity name can arrive in.
# Bitter carries Latin, Latin-ext and Cyrillic, but **no Greek** — and Greek
# is not a subset Bitter publishes upstream either, so this cannot be fixed by
# re-exporting the font. Drawing a Greek name in it produces a row of .notdef
# boxes: verified by rasterising ΓΚΟΛΕΜΗΣ ΕΤΑΙΡΕΙΑ on 2026-08-28, where 11 of
# 12 distinct characters came out as tofu.
#
# A pixel-count check is NOT a coverage check — tofu boxes are themselves
# inky, so counting dark pixels reports a Greek line as *denser* than its
# Latin equivalent. Coverage is decided by comparing each character's raster
# against the raster of a codepoint the font certainly lacks.

#: A Private Use Area codepoint no text face defines — its raster is the
#: font's .notdef glyph, which is what every missing character renders as.
_NOTDEF_PROBE = "\ue000"


@lru_cache(maxsize=8)
def _notdef_raster(font_name: str) -> bytes:
    font = _font(font_name, 40)
    return _char_raster(font, _NOTDEF_PROBE)


def _char_raster(font: ImageFont.FreeTypeFont, ch: str) -> bytes:
    image = Image.new("L", (80, 70), 255)
    ImageDraw.Draw(image).text((5, 5), ch, font=font, fill=0)
    return image.tobytes()


@lru_cache(maxsize=4096)
def _renders(font_name: str, ch: str) -> bool:
    """True when *font_name* has a real glyph for *ch*.

    Whitespace always passes. Everything else is compared against the face's
    .notdef raster, which is the only reliable signal available without
    pulling in a font-parsing dependency.
    """
    if ch.isspace():
        return True
    font = _font(font_name, 40)
    return _char_raster(font, ch) != _notdef_raster(font_name)


def renders_fully(text: str, font_name: str = "bitter-700") -> bool:
    """True when every character of *text* has a glyph in the bundled face."""
    return all(_renders(font_name, ch) for ch in text)


def card_display_name(
    name: str | None,
    lei: str,
    *,
    latin_name: str | None = None,
) -> tuple[str, bool]:
    """The name to draw on the card, and whether it was romanised.

    Order of preference:

    1. The name as filed, when the card's face can actually render it.
    2. *latin_name* — a Latin form the SOURCE published, when the caller has
       one. ΓΕΜΗ, for instance, supplies ``coNamesEn[]``, the register's own
       romanisation, which beats anything we could derive.
    3. ``names.transliterate_display()`` — the same deterministic
       transliteration that already reaches BODS output as an
       ``alternateNames`` entry and a ``type: transliteration`` person name.
       The card therefore shows a name OpenCheck already publishes rather
       than inventing one for the image.
    4. The LEI, when nothing renders.

    Returns ``(text, romanised)`` so callers can say so in the alt text.
    """
    filed = (name or "").strip()
    if not filed:
        return f"LEI {lei}", False
    if renders_fully(filed):
        return filed, False

    for candidate in (latin_name, _names_mod.transliterate_display(filed)):
        candidate = (candidate or "").strip()
        if candidate and renders_fully(candidate):
            return candidate, True

    return f"LEI {lei}", False


def card_alt_text(
    name: str | None,
    lei: str,
    signals: list[dict[str, Any]] | None,
    *,
    latin_name: str | None = None,
) -> str:
    """Alt text describing the rendered card, for ``og:image:alt``.

    Describes what a sighted viewer sees, in the same terms and the same
    order. When the drawn name was romanised the alt text says so, so a
    screen-reader user is not told a Greek company has a Latin name.
    """
    display, romanised = card_display_name(name, lei, latin_name=latin_name)
    subject = f"{display} (romanised)" if romanised else display

    if signals is None:
        return (
            f"OpenCheck shareable card for {subject}, LEI {lei}. "
            f"Invites the viewer to run a live check across "
            f"{_source_count()} open sources at opencheck.world."
        )

    risk = [s for s in signals if s.get("kind", "risk") == "risk"]
    codes: list[str] = []
    for sig in risk:
        code = str(sig.get("code") or "")
        if code and code not in codes:
            codes.append(code)
    if not codes:
        return (
            f"OpenCheck shareable card for {subject}, LEI {lei}, "
            f"showing no risk signals found."
        )

    labels = [
        str(SIGNAL_STYLE[c][0]) if c in SIGNAL_STYLE else c.replace("_", " ").lower()
        for c in codes[:3]
    ]
    named = ", ".join(labels[:-1]) + (f" and {labels[-1]}" if len(labels) > 1 else labels[0])
    if len(labels) == 1:
        named = labels[0]
    more = len(codes) - len(labels)
    tail = f", and {more} more" if more > 0 else ""
    plural = "s" if len(codes) != 1 else ""
    return (
        f"OpenCheck shareable card for {subject}, LEI {lei}, showing "
        f"{len(codes)} risk signal{plural} found: {named}{tail}. "
        f"Prompts the viewer to visit opencheck.world for details."
    )


def _source_count() -> int:
    """How many sources OpenCheck fans out across, counted not hard-coded."""
    try:
        from .sources import REGISTRY  # local import — keeps Pillow off that path

        return len(REGISTRY)
    except Exception:  # noqa: BLE001 — the card must render regardless
        return 0


def _wrap(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont,
          max_width: int) -> list[str]:
    """Greedy word wrap; single overlong words are hard-truncated."""
    lines: list[str] = []
    current = ""
    for word in text.split():
        candidate = f"{current} {word}".strip()
        if draw.textlength(candidate, font=font) <= max_width:
            current = candidate
            continue
        if current:
            lines.append(current)
        while draw.textlength(word, font=font) > max_width and len(word) > 1:
            word = word[:-1]
        current = word
    if current:
        lines.append(current)
    return lines


def _truncate(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont,
              max_width: int) -> str:
    if draw.textlength(text, font=font) <= max_width:
        return text
    while text and draw.textlength(f"{text}…", font=font) > max_width:
        text = text[:-1].rstrip()
    return f"{text}…"


def _draw_logo(draw: ImageDraw.ImageDraw, x: int, y: int, s: float) -> None:
    """The OpenCheck magnifier mark, from the social-card SVG geometry
    (120-unit viewBox), scaled by ``s`` and offset to (x, y)."""
    def pt(px: float, py: float) -> tuple[float, float]:
        return (x + px * s, y + py * s)

    r = 44 * s
    cx, cy = pt(54, 54)
    lw = max(2, round(10 * s))
    draw.ellipse([cx - r, cy - r, cx + r, cy + r], outline=_NAVY, width=lw)
    draw.line([pt(86, 86), pt(114, 114)], fill=_NAVY, width=max(2, round(11 * s)))
    draw.polygon([pt(38, 34), pt(38, 74), pt(74, 54)], fill=_NAVY)
    for (dx, dy), colour in [((34, 22), "#22c55e"), ((16, 54), "#3b82f6"), ((34, 86), "#7c3aed")]:
        dcx, dcy = pt(dx, dy)
        dr = 8 * s
        draw.ellipse([dcx - dr, dcy - dr, dcx + dr, dcy + dr], fill=colour)


def _draw_confidence_dot(draw: ImageDraw.ImageDraw, cx: float, cy: float,
                         r: float, colour: str, confidence: str) -> None:
    """● high — filled; ◐ medium — left half; ○ low — outline."""
    box = [cx - r, cy - r, cx + r, cy + r]
    if confidence == "high":
        draw.ellipse(box, fill=colour)
    elif confidence == "medium":
        draw.ellipse(box, outline=colour, width=max(2, int(r / 2.5)))
        draw.pieslice(box, 90, 270, fill=colour)
    else:
        draw.ellipse(box, outline=colour, width=max(2, int(r / 2.5)))


def render_share_card(
    name: str | None,
    lei: str,
    signals: list[dict[str, Any]] | None,
) -> bytes:
    """Render the share card PNG. ``signals=None`` renders the teaser
    variant (no completed lookup available); ``[]`` means a completed
    lookup with zero signals."""
    s = 2  # supersample factor
    img = Image.new("RGB", (W * s, H * s), "#ffffff")
    draw = ImageDraw.Draw(img)
    draw.rectangle([_SPLIT * s, 0, W * s, H * s], fill=_INDIGO)

    f_word = _font("dmsans-700", 36 * s)
    f_name = None  # chosen by fit loop
    f_lei = _font("dmsans-400", 24 * s)
    f_cta = _font("dmsans-400", 25 * s)
    f_cta_b = _font("dmsans-700", 25 * s)
    f_count_label = _font("dmsans-400", 28 * s)
    f_pill = _font("dmsans-500", 27 * s)
    f_more = _font("dmsans-400", 24 * s)

    # ── Left panel ──────────────────────────────────────────────────────
    _draw_logo(draw, 70 * s, 56 * s, 0.4 * s)
    wx = (70 + 60) * s
    draw.text((wx, 62 * s), "Open", font=f_word, fill=_NAVY)
    wx += draw.textlength("Open", font=f_word)
    draw.text((wx, 62 * s), "Check", font=f_word, fill=_CHECK_BLUE)

    # A name the bundled face cannot draw (Greek, most notably) is replaced
    # with a Latin form rather than rendered as .notdef boxes — see
    # card_display_name.
    display_name, romanised = card_display_name(name, lei)
    name_max_w = (_SPLIT - 70 - 60) * s
    longest_word = max(display_name.split(), key=len, default="")
    for size in (60, 54, 48, 42, 36, 30):
        f_name = _font("bitter-700", size * s)
        lines = _wrap(draw, display_name, f_name, name_max_w)
        # Fit = at most 3 lines AND no single word needed hard-chopping.
        if len(lines) <= 3 and draw.textlength(longest_word, font=f_name) <= name_max_w:
            break
    lines = _wrap(draw, display_name, f_name, name_max_w)[:3]
    y = 196 * s
    for line in lines:
        draw.text((70 * s, y), line, font=f_name, fill=_INK)
        y += int(f_name.size * 1.18)

    if display_name != f"LEI {lei}":  # don't print the LEI twice
        # A romanised name is flagged here rather than left implicit: the card
        # would otherwise show a different name from the one on the register,
        # which for a due-diligence tool is a claim we have not earned.
        lei_line = f"LEI {lei}" + (" · name shown romanised" if romanised else "")
        draw.text((70 * s, y + 20 * s), lei_line, font=f_lei, fill=_MUTED)

    cta_y = (H - 44 - 32) * s
    cx = 70 * s
    for text, font, colour in [
        ("Visit ", f_cta, _INK),
        ("opencheck.world", f_cta_b, _INDIGO),
        (" for more details", f_cta, _INK),
    ]:
        draw.text((cx, cta_y), text, font=font, fill=colour)
        cx += draw.textlength(text, font=font)

    # ── Right panel ─────────────────────────────────────────────────────
    px = (_SPLIT + 56) * s
    panel_w = (W - _SPLIT - 2 * 56) * s

    if signals is None:
        f_count = _font("bitter-700", 120 * s)
        # Counted from the registry, not hard-coded: this said "34" while the
        # registry held 39, so every teaser card understated the tool by five
        # sources.
        source_text = str(_source_count())
        draw.text((px, 44 * s), source_text, font=f_count, fill="#ffffff")
        nx = px + draw.textlength(source_text, font=f_count) + 18 * s
        draw.text((nx, 116 * s), "open sources,", font=f_count_label, fill=_LAVENDER)
        draw.text((nx, 152 * s), "one query", font=f_count_label, fill=_LAVENDER)
        ty = 260 * s
        for line in ["Run the live check for", "risk signals, ownership", "and source-linked data."]:
            draw.text((px, ty), line, font=_font("dmsans-500", 30 * s), fill="#ffffff")
            ty += 44 * s
    else:
        # Context signals (e.g. NON_EU_JURISDICTION) are structural
        # observations, not risk findings — they must neither inflate the
        # headline count nor occupy one of the three named chip slots.
        signals = [s for s in signals if s.get("kind", "risk") == "risk"]
        # Count DISTINCT CODES, not signal instances. Since PR #115 the
        # related-party paths emit one statement-scoped signal per finding,
        # so an entity with three parties flagged for the same thing carries
        # three instances of one code — and the results page renders that as
        # ONE chip (App.tsx aggregates by code before splitting on kind).
        # Counting instances here made the card claim "7 risk signals" for a
        # page showing three chips: the same surface-count divergence the
        # `kind` field was added to prevent, one axis over.
        total = len({str(s.get("code") or "") for s in signals if s.get("code")})
        f_count = _font("bitter-700", 120 * s)
        count_text = str(total)
        draw.text((px, 44 * s), count_text, font=f_count, fill="#ffffff")
        nx = px + draw.textlength(count_text, font=f_count) + 18 * s
        draw.text((nx, 116 * s), "risk signal" + ("s" if total != 1 else ""),
                  font=f_count_label, fill=_LAVENDER)
        draw.text((nx, 152 * s), "found", font=f_count_label, fill=_LAVENDER)

        # First three distinct signal codes, in pipeline order.
        chips: list[dict[str, Any]] = []
        seen: set[str] = set()
        for sig in signals:
            code = str(sig.get("code") or "")
            if code and code not in seen:
                seen.add(code)
                chips.append(sig)
            if len(chips) == 3:
                break

        cy = 236 * s
        pill_h = 56 * s
        for sig in chips:
            code = str(sig.get("code") or "")
            label, bg, fg = SIGNAL_STYLE.get(code, (code, *_DEFAULT_STYLE))
            confidence = str(sig.get("confidence") or "high")
            dot_r = 7 * s
            text_max = panel_w - 30 * s * 2 - dot_r * 2 - 12 * s
            # Long labels: step the font down before resorting to "…".
            pill_font = f_pill
            if draw.textlength(label, font=pill_font) > text_max:
                pill_font = _font("dmsans-500", 23 * s)
            label = _truncate(draw, label, pill_font, text_max)
            text_w = draw.textlength(label, font=pill_font)
            pill_w = 30 * s + dot_r * 2 + 12 * s + text_w + 30 * s
            draw.rounded_rectangle(
                [px, cy, px + pill_w, cy + pill_h], radius=pill_h / 2, fill=bg
            )
            _draw_confidence_dot(draw, px + 30 * s + dot_r, cy + pill_h / 2, dot_r, fg, confidence)
            draw.text(
                (px + 30 * s + dot_r * 2 + 12 * s, cy + (pill_h - pill_font.size * 1.16) / 2 + 2 * s),
                label, font=pill_font, fill=fg,
            )
            cy += pill_h + 18 * s

        remaining = total - len(chips)
        if remaining > 0:
            draw.text((px, cy + 4 * s), f"+ {remaining} more on opencheck.world",
                      font=f_more, fill=_LAVENDER)
        elif total == 0:
            draw.text((px, 250 * s), "No risk signals surfaced",
                      font=_font("dmsans-500", 30 * s), fill="#ffffff")
            draw.text((px, 296 * s), "across 34 open sources",
                      font=f_more, fill=_LAVENDER)

    img = img.resize((W, H), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()
