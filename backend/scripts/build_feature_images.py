#!/usr/bin/env python3
"""Build the six illustrations on the /features page.

These are **not screenshots**. Each one is a cropped mock of the real
component, drawn here from the shipped design tokens (the same values as
``frontend/tailwind.config.js``) and rendered HTML -> PNG. Two reasons:

* the page and its pictures cannot end up in two different colour systems,
  which is what happens when marketing art is captured once and the component
  is restyled afterwards; and
* a component redesign does not silently date the page — it dates this file,
  which is a diff someone can see.

**The artwork states per-lookup facts only, never a registry total.** The
first cut said "38 / 40 sources answered" and "2 of 40 sources could not be
reached", and was wrong within two days when ``eiti_assessment`` took the
registry to 41 — the same drift ``lib/features.ts`` avoids by taking the count
from ``/sources``, except a picture has no prop to take it from. So: "38
sources answered", "2 sources could not be reached", and no denominator
anywhere. See the ``opencheck-source-count-refs`` rule: prefer reading the
count to stating it, and where you cannot read it, do not state it.

Usage (needs ``playwright`` and ``pillow``, and a network path to Google
Fonts for Bitter / DM Sans / DM Mono)::

    python3 backend/scripts/build_feature_images.py --out frontend/public/features

Output: 1,648px wide PNGs, quantised to 200 colours, 53-74 KB each.
"""

import argparse
import pathlib
import tempfile


OUT = pathlib.Path(__file__).resolve().parents[2] / "frontend" / "public" / "features"
W, H = 1030, 560

NAVY="#191d23"; BLUE="#3d30d4"; MUTED="#696969"; RULE="#e5e5e5"; BG="#f3f3f5"
SOFT="#eef1fb"; SOFTB="#cfd6f5"; MARKNAVY="#0d1b3e"; LINE="#93c5fd"
OKBG="#ecfdf5"; OKBD="#a7f3d0"; OKTX="#047857"
WARNBG="#fffbeb"; WARNBD="#fcd34d"; WARNTX="#92400e"
INFOBG="#f0f9ff"; INFOBD="#bae6fd"; INFOTX="#0369a1"
GO="#3b82f6"; GOT="#1d4ed8"; GOTINT="#eff6ff"; GOTB="#bfdbfe"
GC="#e65100"; GCT="#9a3412"; GR="#7c3aed"; GRT="#6d28d9"; GRTINT="#f5f3ff"
HEAD="Bitter, Georgia, serif"; BODY="'DM Sans', system-ui, sans-serif"
MONO="'DM Mono', ui-monospace, monospace"
FONTS=('<link rel="stylesheet" href="https://fonts.googleapis.com/css2?'
       'family=Bitter:wght@400;700&family=DM+Sans:wght@400;500;700&'
       'family=DM+Mono:wght@400;500&display=swap">')

def chip(text, bg, bd, tx, glyph=""):
    g = (f'<span style="font-size:11px;line-height:1">{glyph}</span>' if glyph else "")
    return (f'<span style="display:inline-flex;align-items:center;gap:5px;background:{bg};'
            f'border:1px solid {bd};color:{tx};border-radius:999px;padding:3px 10px;'
            f'font-family:{BODY};font-size:11.5px;font-weight:500;white-space:nowrap">{g}{text}</span>')

def label(t, mb=10):
    return (f'<p style="font-family:{BODY};font-size:10.5px;font-weight:700;text-transform:uppercase;'
            f'letter-spacing:0.12em;color:{MUTED};margin:0 0 {mb}px">{t}</p>')

def card(inner, pad=18, extra=""):
    return (f'<div style="background:#fff;border:1px solid {RULE};border-radius:10px;'
            f'padding:{pad}px;{extra}">{inner}</div>')

def page(inner, pad=28, bg=BG):
    return f"""<!doctype html><html><head><meta charset="utf-8">{FONTS}
<style>*{{box-sizing:border-box}}body{{margin:0;background:{bg};width:{W}px}}</style>
</head><body><div style="padding:{pad}px">{inner}</div></body></html>"""

TICK = ('<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="#047857" stroke-width="3" '
        'stroke-linecap="round" stroke-linejoin="round"><path d="M20 6 9 17l-5-5"/></svg>')

# ------------------------------------------------------------------ 1 QuickCheck
def quickcheck():
    subject = card(
        f'<div style="display:flex;justify-content:space-between;align-items:flex-start;gap:20px">'
        f'<div><h2 style="font-family:{HEAD};font-weight:700;font-size:24px;color:{NAVY};margin:0 0 6px">BP P.L.C.</h2>'
        f'<div style="display:flex;align-items:center;gap:10px">'
        f'<span style="font-family:{MONO};font-size:12px;color:{MUTED}">213800LH1BZH3DI6G760</span>'
        f'{chip("United Kingdom", "#fff", RULE, MUTED)}{chip("Active on the register", OKBG, OKBD, OKTX, "●")}'
        f'</div></div>'
        f'<div style="text-align:right;flex:0 0 auto">'
        f'<div style="font-family:{HEAD};font-weight:700;font-size:26px;color:{NAVY};line-height:1">38</div>'
        f'<div style="font-family:{BODY};font-size:11px;color:{MUTED};margin-top:2px">sources answered</div>'
        f'</div></div>', 20)
    verdict = (f'<div style="background:{GOTINT};border:1px solid {GOTB};border-radius:10px;padding:14px 18px;'
               f'font-family:{BODY};font-size:14px;line-height:1.6;color:{NAVY}">'
               f'One risk finding and one piece of structural context. '
               f'<span style="color:{MUTED}">2 sources could not be reached, so this is not a complete screen.</span></div>')
    chips = (f'<div style="display:flex;gap:8px;flex-wrap:wrap">'
             + chip("Offshore leaks ◐", "#fef3c7", "#fde68a", "#92400e")
             + chip("Non-EU jurisdiction ●", "#fff7ed", "#fed7aa", "#c2410c")
             + chip("Trust or arrangement ○", "#eef2ff", "#c7d2fe", "#4338ca") + '</div>')
    def src(name, meta, state):
        if state == "run":
            right = (f'<span style="display:inline-flex;align-items:center;gap:6px;font-family:{BODY};'
                     f'font-size:11.5px;color:{BLUE}">'
                     f'<span style="width:9px;height:9px;border-radius:50%;border:2px solid {SOFTB};'
                     f'border-top-color:{BLUE};display:inline-block"></span>Checking…</span>')
        else:
            right = TICK
        return card(f'<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;margin-bottom:6px">'
                    f'<span style="font-family:{BODY};font-size:13.5px;font-weight:700;color:{NAVY}">{name}</span>{right}</div>'
                    f'<p style="font-family:{BODY};font-size:12px;line-height:1.55;color:{MUTED};margin:0">{meta}</p>', 14)
    grid = (f'<div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px">'
            + src("GLEIF", "Registered in GB as 00102498, consolidated by no parent — a reporting exception is filed.", "ok")
            + src("OpenSanctions", "Not listed on any of the sanctions and PEP datasets checked.", "ok")
            + src("Companies House", "Reading officers and persons with significant control…", "run") + '</div>')
    return page(f'{subject}<div style="height:12px"></div>{verdict}<div style="height:14px"></div>'
                f'{label("Risk signals")}{chips}<div style="height:16px"></div>'
                f'{label("Sources")}{grid}')

# ------------------------------------------------------------------ 2 FullCheck
def node(x, y, r, fill, stroke, glyph="", flag=None, badge=None, badge_col=None):
    s = (f'<circle cx="{x}" cy="{y}" r="{r}" fill="{fill}" stroke="{stroke}" stroke-width="2.5"/>')
    if glyph:
        s += (f'<g transform="translate({x} {y}) scale({r/17}) translate(-12 -12)" fill="none" '
              f'stroke="{stroke}" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round">{glyph}</g>')
    if flag:
        fx, fy = x + r*0.707, y - r*0.707
        s += (f'<rect x="{fx-11}" y="{fy-7}" width="22" height="14" rx="2.5" fill="#fff" stroke="{RULE}"/>'
              f'<text x="{fx}" y="{fy+4}" text-anchor="middle" font-family="{BODY}" font-size="9" '
              f'font-weight="700" fill="{MUTED}">{flag}</text>')
    if badge:
        bx, by = x - r*0.707, y - r*0.707
        wpx = 7.2*len(badge) + 18
        s += (f'<rect x="{bx-wpx}" y="{by-11}" width="{wpx}" height="21" rx="10.5" fill="{badge_col[0]}" '
              f'stroke="{badge_col[1]}"/>'
              f'<text x="{bx-wpx/2}" y="{by+3.5}" text-anchor="middle" font-family="{BODY}" font-size="10.5" '
              f'font-weight="700" fill="{badge_col[2]}">{badge}</text>')
    return s

ORG = '<path d="M5 21V5h9v16M14 10h5v11M8 9h3M8 13h3M8 17h3M17 14h0M17 18h0"/>'
PER = '<circle cx="12" cy="8" r="3.4"/><path d="M5 20c.8-3.5 3.6-5.5 7-5.5s6.2 2 7 5.5"/>'

def edge(x1,y1,x2,y2,col,dash=None,lab=None,labcol=None):
    d = f' stroke-dasharray="{dash}"' if dash else ""
    s = f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{col}" stroke-width="2.2"{d} marker-end="url(#ar)"/>'
    if lab:
        mx,my=(x1+x2)/2,(y1+y2)/2
        w=6.6*len(lab)+14
        s += (f'<rect x="{mx-w/2}" y="{my-10}" width="{w}" height="19" rx="4" fill="#fff" stroke="{RULE}"/>'
              f'<text x="{mx}" y="{my+3.5}" text-anchor="middle" font-family="{BODY}" font-size="10.5" '
              f'font-weight="500" fill="{labcol}">{lab}</text>')
    return s

def chipsvg(x, y, lab, col):
    w = 6.6 * len(lab) + 16
    return (f'<rect x="{x - w / 2}" y="{y - 10}" width="{w}" height="19" rx="4" fill="#fff" stroke="{RULE}"/>'
            f'<text x="{x}" y="{y + 3.5}" text-anchor="middle" font-family="{BODY}" font-size="10.5" '
            f'font-weight="500" fill="{col}">{lab}</text>')

STATE = '<path d="M4 20h16M6 20V9l6-4 6 4v11M10 20v-5h4v5"/>'

def txt(x, y, t, size=11.5, col=MUTED, anchor="middle", weight="400", fam=None):
    return (f'<text x="{x}" y="{y}" text-anchor="{anchor}" font-family="{fam or BODY}" '
            f'font-size="{size}" font-weight="{weight}" fill="{col}">{t}</text>')

def fullcheck():
    E = lambda *a, **k: edge(*a, **k)
    svg = (f'<svg width="100%" height="414" viewBox="0 0 966 414">'
           f'<defs><marker id="ar" viewBox="0 0 10 10" refX="9" refY="5" markerUnits="strokeWidth" '
           f'markerWidth="7" markerHeight="5" orient="auto"><path d="M 0 0 L 10 5 L 0 10 z" fill="#666"/></marker></defs>'
           + E(250, 84, 250, 174, GO, None, "owns 100%", GOT)
           + E(250, 222, 250, 308, GO, None, "owns 63%", GOT)
           + E(676, 76, 286, 318, GC, "6 4", None, None)
           + E(678, 208, 284, 332, GR, "2 4", None, None)
           + chipsvg(537, 162, "controls", GCT) + chipsvg(538, 252, "director", GRT)
           + node(250, 58, 24, "#fff", GO, ORG, "AE", "Sanctions control", ("#ffe4e6", "#fda4af", "#9f1239"))
           + node(250, 198, 24, "#fff", GO, ORG, "NL")
           + node(250, 336, 28, GOTINT, BLUE, ORG, "GB")
           + node(700, 58, 24, "#fff", GC, ORG, "KY")
           + node(700, 198, 22, "#fff", GR, PER, None, "PEP", ("#f5f3ff", "#ddd6fe", "#6d28d9"))
           + txt(288, 54, "Abu Dhabi National Energy Co.", 12, NAVY, "start", "500")
           + txt(288, 70, "the ultimate parent", 10.5, MUTED, "start")
           + txt(288, 194, "TAQA Bratani Holdings B.V.", 12, NAVY, "start", "500")
           + txt(288, 210, "intermediate holding company", 10.5, MUTED, "start")
           + txt(738, 54, "Intermediate holding co.", 12, NAVY, "start", "500")
           + txt(738, 70, "Cayman Islands", 10.5, MUTED, "start")
           + txt(730, 194, "Named director", 12, NAVY, "start", "500")
           + txt(730, 210, "a natural person", 10.5, MUTED, "start")
           + txt(250, 384, "TAQA BRATANI LIMITED", 12.5, NAVY, "middle", "700")
           + txt(250, 401, "the subject", 10.5, MUTED, "middle", "400", MONO)
           + '</svg>')
    def _leg(t, c, d):
        da = ' stroke-dasharray="' + d + '"' if d else ""
        return (f'<span style="display:inline-flex;align-items:center;gap:7px;font-family:{BODY};'
                f'font-size:11.5px;color:{MUTED}">'
                f'<svg width="26" height="8"><line x1="0" y1="4" x2="26" y2="4" stroke="{c}" '
                f'stroke-width="2.4"{da}/></svg>{t}</span>')
    leg = "".join(_leg(t, c, d) for t, c, d in
                  [("Ownership", GO, None), ("Control", GC, "6 4"), ("Role", GR, "2 4")])
    return page(card(f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:2px">'
                     f'{label("Ownership and control network", 0)}'
                     f'<div style="display:flex;gap:16px">{leg}</div></div>{svg}', 18))

# ------------------------------------------------------------------ 3 BackgroundCheck
def backgroundcheck():
    def person(name, role, conf, conf_lab, signals, note):
        cc = {"high": ("#ecfdf5", "#a7f3d0", "#047857"), "med": ("#fffbeb", WARNBD, WARNTX),
              "none": ("#f3f3f5", RULE, MUTED)}[conf]
        sig = "".join(signals)
        return (f'<div style="display:flex;align-items:flex-start;gap:14px;padding:14px 0;'
                f'border-bottom:1px solid {RULE}">'
                f'<div style="width:36px;height:36px;border-radius:50%;background:{GRTINT};border:1.5px solid {GR};'
                f'display:flex;align-items:center;justify-content:center;flex:0 0 auto">'
                f'<svg width="19" height="19" viewBox="0 0 24 24" fill="none" stroke="{GRT}" stroke-width="1.9" '
                f'stroke-linecap="round"><circle cx="12" cy="8" r="3.4"/>'
                f'<path d="M5 20c.8-3.5 3.6-5.5 7-5.5s6.2 2 7 5.5"/></svg></div>'
                f'<div style="flex:1 1 0;min-width:0">'
                f'<div style="display:flex;align-items:center;gap:9px;flex-wrap:wrap;margin-bottom:4px">'
                f'<span style="font-family:{BODY};font-size:14px;font-weight:700;color:{NAVY}">{name}</span>'
                f'{chip(conf_lab, cc[0], cc[1], cc[2])}{sig}</div>'
                f'<p style="font-family:{BODY};font-size:12px;line-height:1.55;color:{MUTED};margin:0">'
                f'{role} · {note}</p></div></div>')
    rows = (person("Mads Nipper", "Director, appointed 2021", "med",
                   "Possible match · 0.91", [chip("Politically exposed ◐", "#f5f3ff", "#ddd6fe", "#6d28d9")],
                   "matched on name and birth year in OpenSanctions")
            + person("Henriette Hallberg Thygesen", "Director, appointed 2023", "high",
                     "No match · checked", [], "checked against OpenSanctions, ICIJ and OpenAleph — nothing returned")
            + person("Lene Skole-Sørensen", "Person with significant control", "none",
                     "Not screened", [chip("1 source unavailable", WARNBG, WARNBD, WARNTX, "⚠")],
                     "OpenSanctions could not be reached, so this person has not been screened"))
    return page(f'{label("People named in the records")}'
                + card(rows + f'<p style="font-family:{BODY};font-size:12px;line-height:1.6;color:{MUTED};'
                       f'margin:14px 0 0">A name match is not an identity claim. Every row says which sources '
                       f'answered, and a person no source could answer for is reported as unscreened — never as clear.</p>', 20))

# ------------------------------------------------------------------ 4 Batch
def batch():
    paste = card(
        f'{label("Paste LEIs")}'
        f'<div style="border:1px solid {SOFTB};border-radius:10px;background:{BG};padding:12px 14px;'
        f'font-family:{MONO};font-size:12px;line-height:1.9;color:{NAVY}">'
        f'213800LH1BZH3DI6G760<br>529900RWC8ZYB066JF16<br>335800TYLGG93MM7PR89<br>'
        f'<span style="color:{MUTED}">W9NG6WMZIYEU8VEDOG48</span></div>'
        f'<p style="font-family:{BODY};font-size:11.5px;color:{MUTED};margin:9px 0 0;line-height:1.6">'
        f'4 valid LEIs · 1 duplicate removed · up to 20 per batch<br>About 3 minutes — OpenCheck queues behind '
        f'GLEIF rather than tripping its rate limit.</p>'
        f'<div style="margin-top:12px;display:flex;gap:8px">'
        f'<span style="display:inline-flex;align-items:center;background:{BLUE};color:#fff;border-radius:10px;'
        f'padding:0 16px;height:40px;font-family:{BODY};font-size:13px;font-weight:700">Screen 4 companies</span>'
        f'<span style="display:inline-flex;align-items:center;background:#fff;border:1px solid {SOFTB};color:{BLUE};'
        f'border-radius:10px;padding:0 14px;height:40px;font-family:{BODY};font-size:13px;font-weight:500">Download CSV</span>'
        f'</div>', 18)
    def row(nm, lei, status, sbg, sbd, stx, verdict, risk, tint=False):
        return (f'<tr style="background:{WARNBG if tint else "#fff"}">'
                f'<td style="padding:10px 12px;border-bottom:1px solid {RULE};font-family:{BODY};font-size:12.5px;'
                f'font-weight:700;color:{NAVY};white-space:nowrap">{nm}<br>'
                f'<span style="font-family:{MONO};font-size:10px;font-weight:400;color:{MUTED}">{lei}</span></td>'
                f'<td style="padding:10px 12px;border-bottom:1px solid {RULE}">{chip(status, sbg, sbd, stx)}</td>'
                f'<td style="padding:10px 12px;border-bottom:1px solid {RULE};font-family:{BODY};font-size:12px;'
                f'color:{MUTED};line-height:1.5">{verdict}</td>'
                f'<td style="padding:10px 12px;border-bottom:1px solid {RULE};text-align:right">{risk}</td></tr>')
    table = card(
        f'<table style="width:100%;border-collapse:collapse">'
        f'<tr>' + "".join(
            f'<th style="text-align:{a};padding:0 12px 9px;font-family:{BODY};font-size:10px;font-weight:700;'
            f'text-transform:uppercase;letter-spacing:0.1em;color:{MUTED};border-bottom:1px solid {RULE}">{t}</th>'
            for t, a in [("Company", "left"), ("Register", "left"), ("Verdict", "left"), ("Findings", "right")]) + '</tr>'
        + row("TAQA BRATANI LIMITED", "213800E11LI1SCET…", "Not fully checked", WARNBG, WARNBD, WARNTX,
              "3 sources unavailable — sorted first, never rendered as clean.",
              chip("2 risk", "#ffe4e6", "#fda4af", "#9f1239"), tint=True)
        + row("BP P.L.C.", "213800LH1BZH3DI…", "Active", OKBG, OKBD, OKTX,
              "One risk finding, one piece of structural context.", chip("1 risk", "#fef3c7", "#fde68a", "#92400e"))
        + row("ØRSTED A/S", "W9NG6WMZIYEU8VE…", "Active", OKBG, OKBD, OKTX,
              "No risk findings from the 38 sources that answered.", chip("None", "#f3f3f5", RULE, MUTED))
        + '</table>', 16)
    return page(f'<div style="display:grid;grid-template-columns:352px 1fr;gap:16px;align-items:start">'
                f'{paste}{table}</div>')

# ------------------------------------------------------------------ 5 Time Machine
def timemachine():
    TEAL="#0d9488"
    def ev(y, col, date, basis, title, detail, src):
        return (f'<div style="display:flex;gap:16px;padding-bottom:18px;position:relative">'
                f'<div style="flex:0 0 88px;text-align:right;padding-top:1px">'
                f'<div style="font-family:{MONO};font-size:12px;color:{NAVY};font-weight:500">{date}</div>'
                f'<div style="font-family:{BODY};font-size:10px;color:{MUTED}">{basis}</div></div>'
                f'<div style="flex:0 0 14px;position:relative">'
                f'<div style="width:12px;height:12px;border-radius:50%;background:{col};margin-top:3px"></div>'
                f'<div style="position:absolute;left:5.5px;top:17px;bottom:-18px;width:1.5px;background:{RULE}"></div></div>'
                f'<div style="flex:1 1 0;min-width:0">'
                f'<div style="font-family:{BODY};font-size:13.5px;font-weight:700;color:{NAVY};margin-bottom:3px">{title}</div>'
                f'<p style="font-family:{BODY};font-size:12px;line-height:1.55;color:{MUTED};margin:0 0 6px">{detail}</p>'
                f'{src}</div></div>')
    body = (ev(0, TEAL, "2021-11-19", "effective", "New parent — ownership",
               'Market Bidco Limited began consolidating the company. The interest start date is the '
               'relationship period’s, not the date GLEIF recorded it in 2023.',
               chip("GLEIF", "#fff", RULE, MUTED))
            + ev(0, GO, "2021-10-27", "effective", "Legal form changed",
                 'Public limited company → private limited company. The 8888 → B6ES re-encoding earlier '
                 'that year is suppressed: an encoding backfill is not a change.',
                 chip("Companies House", "#fff", RULE, MUTED) + " " + chip("GLEIF", "#fff", RULE, MUTED))
            + ev(0, GO, "2021-10-27", "effective", "Legal name changed",
                 'Wm Morrison Supermarkets PLC → Wm Morrison Supermarkets Limited. Both sources report it; '
                 'the effective date wins over the recorded one.',
                 chip("Companies House", "#fff", RULE, MUTED)))
    toggle = (f'<div style="border-top:1px dashed {SOFTB};margin-top:2px;padding-top:12px">'
              f'<span style="display:inline-flex;align-items:center;gap:8px;font-family:{BODY};font-size:12.5px;'
              f'color:{BLUE};font-weight:500">▸ Show 11 administrative changes'
              f'<span style="font-family:{BODY};font-size:11px;color:{MUTED};font-weight:400">'
              f'annual renewals, timezone backfills</span></span></div>')
    return page(f'{label("Time Machine · WM MORRISON SUPERMARKETS LIMITED")}'
                + card(body + toggle, 20))

# ------------------------------------------------------------------ 6 Network
def network():
    O = "\u00d8"
    DOWN, DOT = "\u25be", "\u00b7"
    svg = (f'<svg width="100%" height="318" viewBox="0 0 478 318">'
           f'<defs><marker id="ar2" viewBox="0 0 10 10" refX="9" refY="5" markerUnits="strokeWidth" '
           f'markerWidth="7" markerHeight="5" orient="auto"><path d="M 0 0 L 10 5 L 0 10 z" fill="#666"/></marker></defs>'
           + edge(150, 74, 150, 128, GC, "6 4", None, None).replace("ar)", "ar2)")
           + edge(135, 184, 98, 242, GO, None, None, None).replace("ar)", "ar2)")
           + edge(167, 184, 238, 242, GO, None, None, None).replace("ar)", "ar2)")
           + edge(370, 160, 184, 160, GR, "2 4", None, None).replace("ar)", "ar2)")
           + node(150, 50, 22, "#fff", GC, STATE, "DK")
           + node(150, 158, 26, GOTINT, BLUE, ORG, "DK")
           + node(84, 264, 20, "#fff", GO, ORG, "DK")
           + node(252, 264, 20, "#fff", GO, ORG, "GB")
           + node(392, 158, 20, "#fff", GR, PER, None, "PEP", ("#f5f3ff", "#ddd6fe", "#6d28d9"))
           + txt(150, 22, "Danish State", 11, MUTED)
           + txt(118, 155, "\u00d8RSTED A/S", 11.5, NAVY, "end", "700")
           + txt(118, 170, "the subject", 10, MUTED, "end")
           + txt(84, 300, "\u00d8rsted Wind Power", 10.5, MUTED)
           + txt(252, 300, "UK subsidiary", 10.5, MUTED)
           + txt(392, 124, "Named director", 10.5, MUTED)
           + '</svg>')

    def trow(indent, glyph, name, meta, sig=""):
        return (f'<div style="display:flex;align-items:center;gap:9px;padding:8px 0 8px {indent}px;'
                f'border-bottom:1px solid {RULE}">'
                f'<span style="font-family:{MONO};font-size:11px;color:{MUTED};flex:0 0 auto">{glyph}</span>'
                f'<span style="font-family:{BODY};font-size:12px;font-weight:500;color:{NAVY};flex:1 1 0;'
                f'overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{name}</span>'
                f'{sig}<span style="font-family:{BODY};font-size:10.5px;color:{MUTED};flex:0 0 auto">{meta}</span></div>')
    tree = (trow(0, DOWN, "Danish State", "controls")
            + trow(14, DOWN, O + "rsted A/S", "subject")
            + trow(28, DOT, O + "rsted Wind Power A/S", "owns 100%")
            + trow(28, DOT, "UK subsidiary", "owns 100%")
            + trow(28, DOT, "Named director", "director",
                   chip("PEP", "#f5f3ff", "#ddd6fe", "#6d28d9") + "&nbsp;"))
    return page(f'<div style="display:grid;grid-template-columns:1fr 356px;gap:16px;align-items:start">'
                + card(label("Ownership graph " + DOT + " BOVS", 4) + svg, 16)
                + card(f'{label("Read as text", 8)}{tree}'
                       f'<p style="font-family:{BODY};font-size:11.5px;line-height:1.6;color:{MUTED};margin:12px 0 0">'
                       f'Every graph carries the same statements as a list underneath, so the picture is never '
                       f'the only way to get the information.</p>', 16) + '</div>')

# ------------------------------------------------------------------ 7 Subsidiaries
def subsidiaries():
    """The Subsidiaries tab (Phase 185): the coverage sentence, the list pills
    and two of the per-source lists, with the three row states the tab
    distinguishes — linked to its own report, a name match, no LEI at all."""
    GC_BG, GC_BD = "#fdf0e8", "#fdba74"
    sentence = (f'<p style="font-family:{BODY};font-size:14px;line-height:1.6;color:{NAVY};margin:0;max-width:82ch">'
                f'Four sources list what <b>Shell plc</b> owns — 2,160 distinct names across 2,331 rows, '
                f'and only 121 names appear in more than one of them. '
                f'<span style="color:{MUTED}">That is not because any list is wrong: each measures something '
                f'different, and no public source publishes the whole picture.</span></p>')
    def pill(name, n, o):
        return (f'<span style="display:inline-flex;gap:6px;align-items:baseline;background:{BG};border:1px solid {RULE};'
                f'border-radius:10px;padding:6px 10px;font-family:{BODY};font-size:12px;color:{NAVY}">'
                f'<b>{name}</b><span style="color:{MUTED}">· {n} · {o} can be opened</span></span>')
    pills = ('<div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:12px">'
             + pill("GLEIF Level 2", "291", "291") + pill("OECD-UNSD MEIP", "294", "294")
             + pill("EITI", "144", "31") + pill("Global Energy Monitor", "12", "9") + '</div>')
    cover = card(label("What the sources say", 8) + sentence + pills, 18)

    def row(name, country, extra, right, link=True):
        nm = (f'<span style="font-family:{BODY};font-size:13px;color:{NAVY};white-space:nowrap;'
              f'{"text-decoration:underline;text-decoration-color:" + SOFTB + ";text-underline-offset:3px" if link else ""}">{name}</span>')
        return (f'<div style="display:flex;align-items:center;gap:8px;padding:8px 12px;border-bottom:1px solid {RULE};'
                f'flex-wrap:wrap;row-gap:5px">{nm}{chip(country, "#fff", RULE, MUTED)}{extra}'
                f'<span style="margin-left:auto;font-family:{MONO};font-size:11px;color:{MUTED};white-space:nowrap">{right}</span></div>')
    also = lambda t: chip("Also in " + t, OKBG, OKBD, OKTX)
    match = chip("Low · name match", "#fff", RULE, MUTED, "○")
    pct = lambda p: f'<span style="font-family:{MONO};font-size:11px;color:{MUTED}">{p}</span>'
    # Real rows from the committed MEIP and EITI data for Shell plc.
    eiti = (f'<div style="border:1px solid {RULE};border-radius:10px;background:#fff;overflow:hidden">'
            + row("A/S Norske Shell", "NOR", also("GLEIF Level 2 · OECD-UNSD MEIP") + match, "213800F4ETX85XLF5K47")
            + row("PT. Shell Indonesia", "IDN", also("OECD-UNSD MEIP") + match, "549300XSPZ26RK6HXZ74")
            + row("Atlantic 1 Holdings LLC", "TTO", "", "no LEI published", link=False)
            + row("Shell Kazakhstan Development", "KAZ", "", "no LEI published", link=False)
            + '</div>')
    gem = (f'<div style="border:1px solid {RULE};border-radius:10px;background:#fff;overflow:hidden">'
           + row("Shell Energy North America", "USA", chip("Direct", INFOBG, INFOBD, INFOTX) + pct("100%"),
                 "5493001KJTIIGC8Y1R12")
           + row("Shell Nederland Raffinaderij", "NLD", chip("Direct", INFOBG, INFOBD, INFOTX)
                 + also("GLEIF Level 2") + pct("100%"), "724500PMK2A2M1SQQ228")
           + row("Pennsylvania Chemicals", "USA", chip("Direct", INFOBG, INFOBD, INFOTX) + pct("50%"),
                 "no LEI published", link=False)
           + '</div>')
    def band(title, aside, body, note):
        return card(f'<div style="display:flex;justify-content:space-between;align-items:baseline;gap:12px;margin-bottom:8px">'
                    f'<span style="font-family:{HEAD};font-weight:700;font-size:15px;color:{NAVY}">{title}</span>'
                    f'<span style="font-family:{BODY};font-size:11px;color:{MUTED};white-space:nowrap">{aside}</span></div>'
                    f'<p style="font-family:{BODY};font-size:11.5px;line-height:1.55;color:{MUTED};margin:0 0 10px">{note}</p>{body}', 16)
    lists = (f'<div style="display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:14px;align-items:start">'
             + band("EITI · declared controlled subsidiaries", "144 listed · 31 can be opened", eiti,
                    "Names and countries only, no identifiers. A name in another list is offered as a name match, not as proof.")
             + band("Global Energy Monitor · directly owned entities", "12 listed · 9 can be opened", gem,
                    "Asset ownership in the energy sector, with a percentage where GEM has one.") + '</div>')
    return page(f'{cover}<div style="height:14px"></div>{lists}')

VIGS = {"quickcheck": quickcheck, "fullcheck": fullcheck, "backgroundcheck": backgroundcheck,
        "batch": batch, "time-machine": timemachine, "network": network,
        "subsidiaries": subsidiaries}


WIDTH = 1648
"""Rendered width. ~1.6x the 1,030px the page gives an image at its widest,
which is enough for a 2x display without doubling the byte size."""


def render(out_dir: pathlib.Path, only: list[str] | None = None) -> None:
    """Render every vignette (or just ``only``) to ``out_dir`` as an optimised PNG."""
    from PIL import Image
    from playwright.sync_api import sync_playwright

    names = [n for n in VIGS if not only or n in only]
    out_dir.mkdir(parents=True, exist_ok=True)
    # Scratch HTML goes to a temp dir, never into the published directory:
    # `frontend/public/` is served verbatim, so a stray .html would ship.
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="feature-images-"))
    for name in names:
        (tmp / f"{name}.html").write_text(VIGS[name](), encoding="utf-8")

    with sync_playwright() as p:
        browser = p.chromium.launch()
        for name in names:
            # A short viewport plus full_page crops each image to its own
            # content height, so the page gets six tight illustrations rather
            # than six letterboxed ones.
            page = browser.new_page(viewport={"width": W, "height": 80}, device_scale_factor=2)
            page.goto((tmp / f"{name}.html").as_uri())
            page.wait_for_timeout(1500)  # webfonts
            page.screenshot(path=str(tmp / f"{name}@2x.png"), full_page=True)
            page.close()
        browser.close()

    for name in names:
        im = Image.open(tmp / f"{name}@2x.png").convert("RGB")
        height = round(im.height * WIDTH / im.width)
        im = im.resize((WIDTH, height), Image.LANCZOS)
        im.quantize(colors=200, dither=Image.NONE).save(out_dir / f"{name}.png", optimize=True)
        size_kb = (out_dir / f"{name}.png").stat().st_size // 1024
        print(f"{name}.png  {WIDTH}x{height}  {size_kb} KB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=pathlib.Path, default=OUT,
                        help="directory to write the PNGs into")
    parser.add_argument("--only", nargs="*", choices=sorted(VIGS),
                        help="render only these vignettes (default: all)")
    args = parser.parse_args()
    render(args.out, args.only)
