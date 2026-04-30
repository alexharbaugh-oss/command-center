"""
PDF export for the Quality Watch report.
Mirrors the format of the scheduled V2 Databricks job.
"""

import io
from datetime import datetime
from collections import Counter

from reportlab.lib.pagesizes import letter
from reportlab.lib.colors import HexColor, white, black
from reportlab.pdfgen import canvas
from reportlab.lib.utils import simpleSplit


# Colors
HEADER_BG = HexColor("#1a2332")
DARK      = HexColor("#1a1a1a")
MED       = HexColor("#444444")
LIGHT     = HexColor("#666666")
VLIGHT    = HexColor("#999999")
BG        = HexColor("#f4f4f4")
BORDER    = HexColor("#cccccc")
WHITE     = HexColor("#ffffff")

RED        = HexColor("#c0392b")
RED_LT     = HexColor("#fae4e1")
ORANGE     = HexColor("#d4730b")
ORANGE_LT  = HexColor("#fef0e0")
YELLOW     = HexColor("#d4920b")
YELLOW_LT  = HexColor("#fef5e7")
GREEN      = HexColor("#1D9E75")
GREEN_LT   = HexColor("#e8f6f0")
PURPLE     = HexColor("#6c3483")
PURPLE_LT  = HexColor("#f0e6f5")

SEV_BG = {
    "RED":    (RED,    RED_LT),
    "ORANGE": (ORANGE, ORANGE_LT),
    "YELLOW": (YELLOW, YELLOW_LT),
    "HOLD":   (PURPLE, PURPLE_LT),
}

W, H = letter
MARGIN = 28


def build_pdf(scored, now_pt: datetime) -> bytes:
    """Build the multi-page PDF and return bytes."""
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=letter)
    page_state = {"page": 0, "total": 0}

    # First pass to estimate pages — keep simple, calc as we go
    pages = _layout_pages(scored)
    page_state["total"] = len(pages)

    shift = "AM" if now_pt.hour < 12 else "PM"
    date_str = now_pt.strftime("%m/%d/%Y")
    time_str = now_pt.strftime("%-I:%M %p PT")

    for page_idx, (kind, payload) in enumerate(pages, 1):
        page_state["page"] = page_idx
        if kind == "summary":
            _draw_summary_page(c, payload, scored, date_str, time_str, shift, page_state)
        elif kind == "alerts":
            _draw_alerts_page(c, payload, page_state, date_str, time_str, shift)
        c.showPage()

    c.save()
    return buf.getvalue()


def _layout_pages(scored):
    """Return list of (kind, payload) tuples — one per page."""
    pages = [("summary", None)]

    # Group flagged parts by section, in priority order
    sections = []

    # 1. HOLD (any stage)
    hold = scored[scored["severity"] == "HOLD"].sort_values("stage_rank")
    if not hold.empty:
        sections.append(("HOLD LIST", hold.to_dict("records")))

    # 2. On the Floor (Layup + Ready to Cure) — anything not clean
    on_floor = scored[
        scored["stage"].isin(["Layup", "Ready to Cure"]) &
        (scored["severity"] != "CLEAN") &
        (scored["severity"] != "HOLD")
    ].sort_values(["sev_rank", "scrap_count", "issue_count"], ascending=[True, False, False])
    if not on_floor.empty:
        sections.append(("ON THE FLOOR — Layup + Ready to Cure", on_floor.to_dict("records")))

    # 3. Ready to Layup — anything not clean
    ready = scored[
        (scored["stage"] == "Ready to Layup") &
        (scored["severity"] != "CLEAN") &
        (scored["severity"] != "HOLD")
    ].sort_values(["sev_rank", "scrap_count", "issue_count"], ascending=[True, False, False])
    if not ready.empty:
        sections.append(("READY TO LAYUP", ready.to_dict("records")))

    # 4. Upstream — RED/ORANGE only (HOLD already covered)
    upstream = scored[
        scored["stage"].isin(["Material Cutting", "Scheduled", "Ready to Schedule"]) &
        scored["severity"].isin(["RED", "ORANGE"])
    ].sort_values(["sev_rank", "stage_rank", "scrap_count"], ascending=[True, True, False])
    if not upstream.empty:
        sections.append(("UPSTREAM — RED + ORANGE only", upstream.to_dict("records")))

    # Pack sections into pages (max ~5 alerts per page for readability)
    PER_PAGE = 5
    for title, items in sections:
        first = True
        for i in range(0, len(items), PER_PAGE):
            chunk = items[i:i + PER_PAGE]
            heading = title if first else f"{title} (cont.)"
            pages.append(("alerts", {"title": heading, "items": chunk}))
            first = False

    if len(pages) == 1:
        # Nothing flagged — still get a single "all clear" alerts page
        pages.append(("alerts", {"title": "ALL CLEAR — no flagged parts", "items": []}))

    return pages


# ============================================================
# DRAW HEADERS / FOOTERS
# ============================================================

def _draw_header(c, date_str, time_str, shift, page_state):
    c.setFillColor(HEADER_BG)
    c.rect(0, H - 56, W, 56, fill=1, stroke=0)
    c.setFillColor(white)
    c.setFont("Helvetica-Bold", 18)
    c.drawString(MARGIN, H - 32, "Kanban Quality Watch")
    c.setFont("Helvetica", 9)
    c.drawString(MARGIN, H - 46, "Hand Layup · Production Lead Command Center")
    c.setFont("Helvetica", 9)
    c.drawRightString(W - MARGIN, H - 28, f"{date_str} · {shift} shift")
    c.setFont("Helvetica", 8)
    c.setFillColor(VLIGHT)
    c.drawRightString(W - MARGIN, H - 44, time_str)


def _draw_footer(c, page_state):
    c.setFont("Helvetica", 7)
    c.setFillColor(VLIGHT)
    c.drawString(MARGIN, 16, "Hand Layup | Quality Watch | A. Harbaugh")
    c.drawRightString(W - MARGIN, 16, f"Page {page_state['page']} of {page_state['total']}")


# ============================================================
# SUMMARY PAGE
# ============================================================

def _draw_summary_page(c, _payload, scored, date_str, time_str, shift, page_state):
    _draw_header(c, date_str, time_str, shift, page_state)
    y = H - 80

    # Counts
    sev = scored["severity"].value_counts()
    total = len(scored)
    counts = [
        ("Pipeline parts", total, DARK, BG),
        ("HOLD",   int(sev.get("HOLD", 0)),   PURPLE, PURPLE_LT),
        ("RED",    int(sev.get("RED", 0)),    RED,    RED_LT),
        ("ORANGE", int(sev.get("ORANGE", 0)), ORANGE, ORANGE_LT),
        ("YELLOW", int(sev.get("YELLOW", 0)), YELLOW, YELLOW_LT),
        ("CLEAN",  int(sev.get("CLEAN", 0)),  GREEN,  GREEN_LT),
    ]

    box_w = (W - 2 * MARGIN - 5 * 6) / 6
    x = MARGIN
    for label, val, fg, bg in counts:
        c.setFillColor(bg)
        c.roundRect(x, y - 56, box_w, 56, 4, fill=1, stroke=0)
        c.setFillColor(LIGHT)
        c.setFont("Helvetica", 8)
        c.drawString(x + 8, y - 16, label.upper())
        c.setFillColor(fg)
        c.setFont("Helvetica-Bold", 22)
        c.drawString(x + 8, y - 44, str(val))
        x += box_w + 6
    y -= 72

    # Severity rules
    c.setFillColor(DARK)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(MARGIN, y, "Severity rules")
    y -= 14
    rules = [
        ("HOLD",   "Manual flag — review before any work continues"),
        ("RED",    "2+ scrap events — stop and plan before layup"),
        ("ORANGE", "1 scrap or 3+ issues — extra eyes needed"),
        ("YELLOW", "1–2 issues, no scrap — watch list"),
        ("CLEAN",  "No quality history — standard process"),
    ]
    for label, desc in rules:
        fg, bg = SEV_BG.get(label, (GREEN, GREEN_LT))
        c.setFillColor(bg)
        c.roundRect(MARGIN, y - 4, 60, 14, 2, fill=1, stroke=0)
        c.setFillColor(fg)
        c.setFont("Helvetica-Bold", 9)
        c.drawString(MARGIN + 6, y, label)
        c.setFillColor(DARK)
        c.setFont("Helvetica", 9)
        c.drawString(MARGIN + 70, y, desc)
        y -= 16
    y -= 8

    # Pipeline by stage
    c.setFillColor(DARK)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(MARGIN, y, "Pipeline by stage")
    y -= 14
    stage_order = ["Ready to Schedule", "Scheduled", "Material Cutting",
                   "Ready to Layup", "Layup", "Ready to Cure"]
    by_stage = scored.groupby("stage")["severity"].value_counts().unstack(fill_value=0)
    for stage in stage_order:
        if stage not in by_stage.index:
            continue
        row = by_stage.loc[stage]
        total_s = int(row.sum())
        c.setFillColor(DARK)
        c.setFont("Helvetica-Bold", 9)
        c.drawString(MARGIN, y, stage)
        c.setFont("Helvetica", 9)
        c.setFillColor(LIGHT)
        c.drawString(MARGIN + 130, y, f"{total_s} total")

        x = MARGIN + 200
        for sev_label in ["HOLD", "RED", "ORANGE", "YELLOW", "CLEAN"]:
            n = int(row.get(sev_label, 0))
            if n == 0:
                x += 60
                continue
            fg = {"HOLD": PURPLE, "RED": RED, "ORANGE": ORANGE,
                  "YELLOW": YELLOW, "CLEAN": GREEN}[sev_label]
            c.setFillColor(fg)
            c.setFont("Helvetica-Bold", 9)
            c.drawString(x, y, f"{n} {sev_label.lower()}")
            x += 60
        y -= 13
    y -= 10

    # Top scrap drivers (last 6 months)
    flagged = scored[scored["severity"].isin(["RED", "ORANGE"])]
    if not flagged.empty:
        c.setFillColor(DARK)
        c.setFont("Helvetica-Bold", 11)
        c.drawString(MARGIN, y, "Top scrap drivers in pipeline")
        y -= 14
        top = flagged.sort_values(["scrap_count", "issue_count"], ascending=[False, False]).head(8)
        for _, row in top.iterrows():
            c.setFillColor(DARK)
            c.setFont("Helvetica", 9)
            summ = (row.get("summary") or "")[:80]
            c.drawString(MARGIN, y, f"• {summ}")
            c.setFillColor(LIGHT)
            c.setFont("Helvetica", 8)
            c.drawRightString(W - MARGIN, y,
                f"{row['scrap_count']}S · {row['rework_count']}R · {row['issue_count']} total · {row.get('stage','')}")
            y -= 12
            if y < 60:
                break

    _draw_footer(c, page_state)


# ============================================================
# ALERT PAGES
# ============================================================

def _draw_alerts_page(c, payload, page_state, date_str, time_str, shift):
    _draw_header(c, date_str, time_str, shift, page_state)
    y = H - 78

    # Section title
    c.setFillColor(DARK)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(MARGIN, y, payload["title"])
    y -= 18

    items = payload["items"]
    if not items:
        c.setFillColor(GREEN)
        c.setFont("Helvetica-Bold", 14)
        c.drawString(MARGIN, y, "✓ No flagged parts in pipeline.")
        _draw_footer(c, page_state)
        return

    for row in items:
        y = _draw_alert_card(c, row, y)
        y -= 8
        if y < 80:
            break

    _draw_footer(c, page_state)


def _draw_alert_card(c, row, y):
    sev = row["severity"]
    fg, bg = SEV_BG.get(sev, (GREEN, GREEN_LT))
    card_w = W - 2 * MARGIN

    # Determine card height based on history
    history = row.get("history") or []
    n_hist = min(len(history), 4)
    card_h = 50 + n_hist * 11
    if n_hist == 0 and row["issue_count"] == 0:
        card_h = 40

    # Background
    c.setFillColor(bg)
    c.roundRect(MARGIN, y - card_h, card_w, card_h, 4, fill=1, stroke=0)
    # Left bar
    c.setFillColor(fg)
    c.rect(MARGIN, y - card_h, 4, card_h, fill=1, stroke=0)

    # Severity badge
    c.setFillColor(fg)
    c.roundRect(MARGIN + 12, y - 18, 50, 13, 2, fill=1, stroke=0)
    c.setFillColor(white)
    c.setFont("Helvetica-Bold", 8)
    c.drawCentredString(MARGIN + 37, y - 14, sev)

    # Summary
    c.setFillColor(DARK)
    c.setFont("Helvetica-Bold", 10)
    summ = (row.get("summary") or "(no summary)")[:90]
    c.drawString(MARGIN + 70, y - 14, summ)

    # Meta
    pn = row.get("part_number") or ""
    issue_id = row.get("issue_id") or ""
    stage = row.get("stage") or ""
    c.setFillColor(LIGHT)
    c.setFont("Helvetica", 8)
    c.drawString(MARGIN + 12, y - 30,
                 f"{issue_id}  ·  {pn}  ·  Stage: {stage}")

    # Counts line
    bits = []
    if row["scrap_count"]:
        bits.append(f"{row['scrap_count']} scrap")
    if row["rework_count"]:
        bits.append(f"{row['rework_count']} rework")
    if row["pending_count"]:
        bits.append(f"{row['pending_count']} pending")
    if row["wrinkle_count"]:
        bits.append(f"{row['wrinkle_count']} wrinkles")
    counts = " · ".join(bits) if bits else f"{row['issue_count']} issues"

    c.setFillColor(MED)
    c.setFont("Helvetica-Bold", 8)
    c.drawString(MARGIN + 12, y - 42, counts)

    # History (most recent 4)
    if history:
        items = sorted(history, key=lambda x: x.get("created") or 0, reverse=True)[:4]
        hy = y - 54
        for h in items:
            d = h.get("disposition_clean") or "Pending"
            color = RED if d == "Scrap" else (ORANGE if d == "Rework" else MED)
            c.setFillColor(color)
            c.setFont("Helvetica-Bold", 7)
            c.drawString(MARGIN + 16, hy, f"{h.get('created_str','')}  {d}")
            c.setFillColor(DARK)
            c.setFont("Helvetica", 7)
            text = (h.get("clean_defect") or h.get("issue_title") or "")[:80]
            c.drawString(MARGIN + 110, hy, text)
            hy -= 11

    return y - card_h
