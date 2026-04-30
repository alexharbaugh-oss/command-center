"""
PDF export for the Quality Watch report.

Locked palette + 4-stage layout per spec brief (4/22/26).
Liberation Sans is registered when available (Streamlit Cloud needs
'fonts-liberation' in packages.txt — falls back to default Helvetica
silently if not installed).

Pages are dynamically packed by actual card height so the layout fills
each page rather than stopping at a fixed card count.

Back-of-report adds an analytics section: wrinkle trend vs ARTS goal,
top 10 worst pipeline parts, defect breakdown, detection-point shift,
day-of-week pattern.
"""

import io
import os
from datetime import datetime, timedelta
from collections import Counter

import streamlit as st
from databricks import sql as dbsql

from reportlab.lib.pagesizes import letter
from reportlab.lib.colors import HexColor, white, black
from reportlab.pdfgen import canvas
from reportlab.lib.utils import simpleSplit
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont


# ---- Font registration (best effort; silent fallback) ----------------
def _try_register_liberation_sans():
    candidates = [
        "/usr/share/fonts/truetype/liberation",
        "/usr/share/fonts/truetype/liberation2",
    ]
    for d in candidates:
        regular = os.path.join(d, "LiberationSans-Regular.ttf")
        bold    = os.path.join(d, "LiberationSans-Bold.ttf")
        italic  = os.path.join(d, "LiberationSans-Italic.ttf")
        bold_it = os.path.join(d, "LiberationSans-BoldItalic.ttf")
        if all(os.path.exists(p) for p in (regular, bold, italic, bold_it)):
            try:
                pdfmetrics.registerFont(TTFont("Helvetica",             regular))
                pdfmetrics.registerFont(TTFont("Helvetica-Bold",        bold))
                pdfmetrics.registerFont(TTFont("Helvetica-Oblique",     italic))
                pdfmetrics.registerFont(TTFont("Helvetica-BoldOblique", bold_it))
                return True
            except Exception:
                return False
    return False


_FONT_OK = _try_register_liberation_sans()


# ---- Colors (locked) -------------------------------------------------
HEADER_BG = HexColor("#1a2332")
DARK      = HexColor("#1a1a1a")
MED       = HexColor("#444444")
LIGHT     = HexColor("#666666")
VLIGHT    = HexColor("#999999")
BG        = HexColor("#f4f4f4")
BG2       = HexColor("#f9f9f9")
BORDER    = HexColor("#cccccc")

RED        = HexColor("#c0392b")
RED_LT     = HexColor("#fae4e1")
ORANGE     = HexColor("#d4730b")
ORANGE_LT  = HexColor("#fef0e0")
YELLOW     = HexColor("#d4920b")
YELLOW_LT  = HexColor("#fef5e7")
GREEN      = HexColor("#1D9E75")
GREEN_LT   = HexColor("#e8f6f0")
BLUE       = HexColor("#2471a3")
BLUE_LT    = HexColor("#e4eff7")

SEV_BG = {
    "RED":    (RED,    RED_LT),
    "ORANGE": (ORANGE, ORANGE_LT),
    "YELLOW": (YELLOW, YELLOW_LT),
}

W, H = letter
MARGIN = 28


# ============================================================
# DATABRICKS — pulls for back-of-report analytics
# ============================================================

def _connect():
    host = st.secrets["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    return dbsql.connect(
        server_hostname=host,
        http_path=st.secrets["DATABRICKS_HTTP_PATH"],
        access_token=st.secrets["DATABRICKS_TOKEN"],
    )


def _run_query_rows(sql: str):
    """Returns list of dicts."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


_AREA_FILTER = """
  originating_area IN (
    '527 Lamination','527 Kitting','527 Hand Trim',
    'Manufacturing Engineering - Composites Fabrication'
  )
  AND upper(coalesce(part_description,'')) NOT LIKE '%BATTERY%'
  AND upper(coalesce(part_description,'')) NOT LIKE '%BUSBAR%'
  AND upper(coalesce(part_description,'')) NOT LIKE '%SLEEVE, 5-PLY%'
"""

_WRINKLE_PRED = """
  (lower(coalesce(defect_code,'')) LIKE '%wnk%'
   OR lower(coalesce(issue_title,'')) LIKE '%wrinkle%'
   OR lower(coalesce(issue_title,'')) LIKE '%winkel%')
"""


def _fetch_wrinkle_trend(weeks: int = 12):
    sql = f"""
    SELECT
      DATE_TRUNC('WEEK', created) AS week_start,
      COUNT(*) AS wrinkles,
      SUM(CASE WHEN disposition='Scrap' THEN 1 ELSE 0 END) AS wrinkle_scraps
    FROM manufacturing.onion_silver.quality_issues_view
    WHERE CAST(created AS DATE) >= CURRENT_DATE() - INTERVAL {weeks*7} DAYS
      AND issue_status != 'deleted'
      AND {_AREA_FILTER}
      AND {_WRINKLE_PRED}
    GROUP BY week_start
    ORDER BY week_start ASC
    """
    return _run_query_rows(sql)


def _fetch_defect_categories():
    sql = f"""
    SELECT
      CASE
        WHEN defect_code LIKE '%COF-WNK%'                                 THEN 'Wrinkles'
        WHEN defect_code LIKE '%COF-FOD%'                                 THEN 'FOD / Particulate'
        WHEN defect_code LIKE '%COF-RES%'                                 THEN 'Resin Ridges'
        WHEN defect_code LIKE '%COF-MIS%'                                 THEN 'Missing Features'
        WHEN defect_code LIKE '%COF-ACP%' OR defect_code LIKE '%COF-FMV%' THEN 'Cure Profile'
        WHEN defect_code LIKE '%COF-DIM%'                                 THEN 'Dim OOT'
        WHEN defect_code LIKE '%COF-NDI%'                                 THEN 'NDI Defects'
        WHEN defect_code LIKE '%COF-RSA%'                                 THEN 'Resin Starvation'
        WHEN defect_code LIKE '%COF-EDL%'                                 THEN 'Edge Delamination'
        WHEN defect_code LIKE '%COF-SFD%'                                 THEN 'Surface Depression'
        ELSE 'Other'
      END AS category,
      COUNT(*) AS total,
      SUM(CASE WHEN disposition='Scrap' THEN 1 ELSE 0 END) AS scraps
    FROM manufacturing.onion_silver.quality_issues_view
    WHERE CAST(created AS DATE) >= CURRENT_DATE() - INTERVAL 180 DAYS
      AND issue_status != 'deleted'
      AND defect_code IS NOT NULL
      AND {_AREA_FILTER}
    GROUP BY 1
    ORDER BY total DESC
    LIMIT 9
    """
    rows = _run_query_rows(sql)
    return [r for r in rows if r["category"] != "Other"][:7]


def _fetch_detection_points():
    sql = f"""
    SELECT
      originating_area AS area,
      COUNT(*) AS total,
      SUM(CASE WHEN disposition='Scrap' THEN 1 ELSE 0 END) AS scraps,
      SUM(CASE WHEN {_WRINKLE_PRED} THEN 1 ELSE 0 END) AS wrinkles
    FROM manufacturing.onion_silver.quality_issues_view
    WHERE CAST(created AS DATE) >= CURRENT_DATE() - INTERVAL 180 DAYS
      AND issue_status != 'deleted'
      AND {_AREA_FILTER}
    GROUP BY area
    ORDER BY total DESC
    """
    return _run_query_rows(sql)


def _fetch_dow_pattern():
    sql = f"""
    SELECT
      date_format(created, 'EEE') AS dow,
      CASE date_format(created, 'EEE')
        WHEN 'Mon' THEN 1 WHEN 'Tue' THEN 2 WHEN 'Wed' THEN 3
        WHEN 'Thu' THEN 4 WHEN 'Fri' THEN 5 WHEN 'Sat' THEN 6 WHEN 'Sun' THEN 7
      END AS dow_n,
      COUNT(*) AS wrinkles
    FROM manufacturing.onion_silver.quality_issues_view
    WHERE CAST(created AS DATE) >= CURRENT_DATE() - INTERVAL 90 DAYS
      AND issue_status != 'deleted'
      AND {_AREA_FILTER}
      AND {_WRINKLE_PRED}
    GROUP BY 1, 2
    ORDER BY dow_n
    """
    return _run_query_rows(sql)


# ============================================================
# BUILD PDF
# ============================================================

def build_pdf(scored, now_pt: datetime) -> bytes:
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=letter)
    page_state = {"page": 0, "total": 0}

    # Pre-fetch analytics data (one connection, cached for the run)
    try:
        analytics = {
            "wrinkle_trend":     _fetch_wrinkle_trend(weeks=12),
            "defect_categories": _fetch_defect_categories(),
            "detection_points":  _fetch_detection_points(),
            "dow_pattern":       _fetch_dow_pattern(),
        }
    except Exception as e:
        analytics = {"error": str(e)}

    pages = _layout_pages(scored, analytics)
    page_state["total"] = len(pages)

    shift = "AM" if (now_pt.hour, now_pt.minute) < (17, 30) else "PM"
    date_str = now_pt.strftime("%m/%d/%Y")
    time_str = now_pt.strftime("%-I:%M %p PT")

    for page_idx, (kind, payload) in enumerate(pages, 1):
        page_state["page"] = page_idx
        if kind == "summary":
            _draw_summary_page(c, payload, scored, date_str, time_str, shift, page_state)
        elif kind == "alerts":
            _draw_alerts_page(c, payload, page_state, date_str, time_str, shift)
        elif kind == "analytics":
            _draw_analytics_page(c, payload, scored, date_str, time_str, shift, page_state)
        c.showPage()

    c.save()
    return buf.getvalue()


def _card_height(rec):
    n_hist = min(len(rec.get("history") or []), 4)
    h = 50 + n_hist * 11
    if n_hist == 0 and rec.get("issue_count", 0) == 0:
        h = 40
    return h


def _layout_pages(scored, analytics):
    pages = [("summary", None)]
    sections = []

    on_floor = scored[
        (scored["stage"] == "Layup") & (scored["severity"] != "CLEAN")
    ].sort_values(["sev_rank", "scrap_count", "wrinkle_count", "issue_count"],
                  ascending=[True, False, False, False])
    if not on_floor.empty:
        sections.append(("ON THE FLOOR — Layup", on_floor.to_dict("records")))

    ready = scored[
        (scored["stage"] == "Ready to Layup") & (scored["severity"] != "CLEAN")
    ].sort_values(["sev_rank", "scrap_count", "wrinkle_count", "issue_count"],
                  ascending=[True, False, False, False])
    if not ready.empty:
        sections.append(("READY TO LAYUP", ready.to_dict("records")))

    upstream = scored[
        scored["stage"].isin(["Material Cutting", "Scheduled"]) &
        scored["severity"].isin(["RED", "ORANGE"])
    ].sort_values(["sev_rank", "stage_rank", "scrap_count"],
                  ascending=[True, True, False])
    if not upstream.empty:
        sections.append(("UPSTREAM — RED + ORANGE only", upstream.to_dict("records")))

    USABLE_HEIGHT = 616.0
    GAP_BETWEEN   = 8.0

    for title, items in sections:
        first = True
        page_items = []
        page_used  = 0.0
        for rec in items:
            ch = _card_height(rec)
            need = ch + (GAP_BETWEEN if page_items else 0)
            if page_used + need > USABLE_HEIGHT and page_items:
                heading = title if first else f"{title} (cont.)"
                pages.append(("alerts", {"title": heading, "items": page_items}))
                first = False
                page_items = [rec]
                page_used  = ch
            else:
                page_items.append(rec)
                page_used += need
        if page_items:
            heading = title if first else f"{title} (cont.)"
            pages.append(("alerts", {"title": heading, "items": page_items}))

    if len(pages) == 1:
        pages.append(("alerts", {"title": "ALL CLEAR — no flagged parts", "items": []}))

    # Always add analytics back-section, regardless of pipeline state
    pages.append(("analytics", analytics))

    return pages


# ============================================================
# DRAW HEADERS / FOOTERS
# ============================================================

def _draw_header(c, date_str, time_str, shift, page_state, title="Kanban Quality Watch"):
    c.setFillColor(HEADER_BG)
    c.rect(0, H - 56, W, 56, fill=1, stroke=0)
    c.setFillColor(white)
    c.setFont("Helvetica-Bold", 18)
    c.drawString(MARGIN, H - 32, title)
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

    sev = scored["severity"].value_counts()
    total = len(scored)
    counts = [
        ("Pipeline parts", total, DARK, BG),
        ("RED",    int(sev.get("RED", 0)),    RED,    RED_LT),
        ("ORANGE", int(sev.get("ORANGE", 0)), ORANGE, ORANGE_LT),
        ("YELLOW", int(sev.get("YELLOW", 0)), YELLOW, YELLOW_LT),
        ("CLEAN",  int(sev.get("CLEAN", 0)),  GREEN,  GREEN_LT),
    ]

    box_w = (W - 2 * MARGIN - 4 * 6) / 5
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

    c.setFillColor(DARK)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(MARGIN, y, "Severity rules")
    y -= 14
    rules = [
        ("RED",    "2+ scraps OR 3+ wrinkles — stop and plan before layup"),
        ("ORANGE", "1 scrap, 3+ issues, OR 2+ wrinkles — extra eyes needed"),
        ("YELLOW", "1+ issue, no scrap — watch list"),
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

    c.setFillColor(DARK)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(MARGIN, y, "Pipeline by stage")
    y -= 14
    stage_order = ["Scheduled", "Material Cutting", "Ready to Layup", "Layup"]
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
        for sev_label in ["RED", "ORANGE", "YELLOW", "CLEAN"]:
            n = int(row.get(sev_label, 0))
            if n == 0:
                x += 60
                continue
            fg = {"RED": RED, "ORANGE": ORANGE, "YELLOW": YELLOW, "CLEAN": GREEN}[sev_label]
            c.setFillColor(fg)
            c.setFont("Helvetica-Bold", 9)
            c.drawString(x, y, f"{n} {sev_label.lower()}")
            x += 60
        y -= 13
    y -= 10

    flagged = scored[scored["severity"].isin(["RED", "ORANGE"])]
    if not flagged.empty:
        c.setFillColor(DARK)
        c.setFont("Helvetica-Bold", 11)
        c.drawString(MARGIN, y, "Top scrap drivers in pipeline")
        y -= 14
        top = flagged.sort_values(
            ["scrap_count", "wrinkle_count", "issue_count"],
            ascending=[False, False, False],
        ).head(8)
        for _, row in top.iterrows():
            c.setFillColor(DARK)
            c.setFont("Helvetica", 9)
            summ = (row.get("summary") or "")[:80]
            c.drawString(MARGIN, y, f"• {summ}")
            c.setFillColor(LIGHT)
            c.setFont("Helvetica", 8)
            c.drawRightString(W - MARGIN, y,
                f"{row['scrap_count']}S · {row['wrinkle_count']}W · {row['issue_count']} total · {row.get('stage','')}")
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

    c.setFillColor(DARK)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(MARGIN, y, payload["title"])
    y -= 18

    items = payload["items"]
    if not items:
        c.setFillColor(GREEN)
        c.setFont("Helvetica-Bold", 14)
        c.drawString(MARGIN, y, "No flagged parts in pipeline.")
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

    history = row.get("history") or []
    n_hist = min(len(history), 4)
    card_h = 50 + n_hist * 11
    if n_hist == 0 and row["issue_count"] == 0:
        card_h = 40

    c.setFillColor(bg)
    c.roundRect(MARGIN, y - card_h, card_w, card_h, 4, fill=1, stroke=0)
    c.setFillColor(fg)
    c.rect(MARGIN, y - card_h, 4, card_h, fill=1, stroke=0)

    c.setFillColor(fg)
    c.roundRect(MARGIN + 12, y - 18, 50, 13, 2, fill=1, stroke=0)
    c.setFillColor(white)
    c.setFont("Helvetica-Bold", 8)
    c.drawCentredString(MARGIN + 37, y - 14, sev)

    c.setFillColor(DARK)
    c.setFont("Helvetica-Bold", 10)
    summ = (row.get("summary") or "(no summary)")[:90]
    c.drawString(MARGIN + 70, y - 14, summ)

    pn = row.get("part_number") or ""
    issue_id = row.get("issue_id") or ""
    mfid = row.get("order_number") or "—"
    stage = row.get("stage") or ""
    c.setFillColor(LIGHT)
    c.setFont("Helvetica", 8)
    c.drawString(MARGIN + 12, y - 30,
                 f"{issue_id}  ·  MFID-{mfid}  ·  {pn}  ·  Stage: {stage}")

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


# ============================================================
# ANALYTICS PAGE — back of report
# ============================================================

def _section_banner(c, y, title, subtitle=None):
    c.setFillColor(HEADER_BG)
    c.rect(MARGIN, y - 16, W - 2 * MARGIN, 16, fill=1, stroke=0)
    c.setFillColor(white)
    c.setFont("Helvetica-Bold", 10)
    c.drawString(MARGIN + 8, y - 12, title)
    if subtitle:
        c.setFont("Helvetica", 8)
        c.drawRightString(W - MARGIN - 8, y - 12, subtitle)
    return y - 22


def _draw_analytics_page(c, analytics, scored, date_str, time_str, shift, page_state):
    _draw_header(c, date_str, time_str, shift, page_state, title="Quality Watch — Trends & Metrics")
    y = H - 75

    if "error" in analytics:
        c.setFillColor(RED)
        c.setFont("Helvetica-Bold", 11)
        c.drawString(MARGIN, y, "Analytics data unavailable")
        c.setFillColor(LIGHT)
        c.setFont("Helvetica", 9)
        c.drawString(MARGIN, y - 14, str(analytics["error"])[:120])
        _draw_footer(c, page_state)
        return

    # ---- 1. Wrinkle trend vs ARTS goal --------------------------------
    weeks = analytics.get("wrinkle_trend") or []
    y = _section_banner(c, y, "WRINKLE TREND — ARTS GOAL: 50% REDUCTION FROM 3/15/26 BASELINE",
                        f"Last {len(weeks)} weeks")

    if weeks:
        # Identify baseline (4 weeks straddling 3/15) vs latest 4 weeks
        # Use first 4 weeks as baseline, last 4 as current
        if len(weeks) >= 8:
            baseline = sum(int(w["wrinkles"]) for w in weeks[:4])
            current  = sum(int(w["wrinkles"]) for w in weeks[-4:])
            target   = baseline * 0.5
            pct_change = ((current - baseline) / baseline * 100) if baseline else 0

            # Goal panel: 3 metric tiles
            tile_w = (W - 2 * MARGIN - 12) / 3
            tiles = [
                ("BASELINE (FIRST 4 WK)", str(baseline), "wrinkles", LIGHT, BG),
                ("LAST 4 WEEKS",          str(current),
                 f"{pct_change:+.0f}% vs baseline",
                 GREEN if current <= target else (ORANGE if current <= baseline else RED),
                 GREEN_LT if current <= target else (ORANGE_LT if current <= baseline else RED_LT)),
                ("ARTS TARGET",           f"≤ {int(target)}",
                 "50% of baseline",
                 BLUE, BLUE_LT),
            ]
            x = MARGIN
            for label, val, sub, fg, bg in tiles:
                c.setFillColor(bg)
                c.roundRect(x, y - 50, tile_w, 50, 4, fill=1, stroke=0)
                c.setFillColor(LIGHT)
                c.setFont("Helvetica", 7.5)
                c.drawString(x + 8, y - 14, label)
                c.setFillColor(fg)
                c.setFont("Helvetica-Bold", 18)
                c.drawString(x + 8, y - 36, val)
                c.setFillColor(LIGHT)
                c.setFont("Helvetica", 7.5)
                c.drawString(x + 8, y - 46, sub)
                x += tile_w + 6
            y -= 60

        # Mini bar chart of weekly trend
        chart_h = 72
        chart_y = y - chart_h
        c.setFillColor(white); c.setStrokeColor(BORDER); c.setLineWidth(0.5)
        c.rect(MARGIN, chart_y, W - 2 * MARGIN, chart_h, fill=1, stroke=1)

        # Label for chart
        c.setFillColor(LIGHT); c.setFont("Helvetica", 7.5)
        c.drawString(MARGIN + 6, chart_y + chart_h - 10, "Weekly wrinkle count")

        plot_x = MARGIN + 44
        plot_y = chart_y + 16
        plot_w = W - 2 * MARGIN - 60
        plot_h = chart_h - 30

        max_v = max(int(w["wrinkles"]) for w in weeks) or 1
        n_w = len(weeks)
        col_w = plot_w / n_w

        # Goal line if we have baseline
        if len(weeks) >= 8:
            target = (sum(int(w["wrinkles"]) for w in weeks[:4]) / 4) * 0.5  # weekly target
            goal_y = plot_y + (plot_h * target / max_v)
            c.setStrokeColor(GREEN); c.setLineWidth(1); c.setDash(3, 2)
            c.line(plot_x, goal_y, plot_x + plot_w, goal_y)
            c.setDash()
            c.setFillColor(GREEN); c.setFont("Helvetica-Bold", 6.5)
            c.drawRightString(plot_x + plot_w - 4, goal_y + 2, f"target ≤ {int(target)}/wk")

        # Bars
        for i, w in enumerate(weeks):
            n = int(w["wrinkles"])
            cx = plot_x + i * col_w + col_w * 0.18
            bw = col_w * 0.64
            bh = plot_h * n / max_v
            color = RED if n > max_v * 0.66 else (ORANGE if n > max_v * 0.33 else GREEN)
            c.setFillColor(color)
            c.rect(cx, plot_y, bw, bh, fill=1, stroke=0)
            c.setFillColor(DARK); c.setFont("Helvetica-Bold", 6.5)
            c.drawCentredString(cx + bw/2, plot_y + bh + 2, str(n))

        # First/last week labels
        if weeks:
            wk_first = weeks[0]["week_start"]
            wk_last = weeks[-1]["week_start"]
            first_str = wk_first.strftime("%b %-d") if hasattr(wk_first, "strftime") else str(wk_first)[:10]
            last_str = wk_last.strftime("%b %-d") if hasattr(wk_last, "strftime") else str(wk_last)[:10]
            c.setFillColor(LIGHT); c.setFont("Helvetica", 7)
            c.drawString(plot_x, plot_y - 9, first_str)
            c.drawRightString(plot_x + plot_w, plot_y - 9, last_str)

        y = chart_y - 10

    else:
        c.setFillColor(LIGHT); c.setFont("Helvetica-Oblique", 9)
        c.drawString(MARGIN + 6, y - 12, "No wrinkle data in window.")
        y -= 24

    # ---- 2. Top 10 worst pipeline parts ------------------------------
    y -= 6
    y = _section_banner(c, y, "TOP 10 WORST PARTS IN PIPELINE", "by score = scraps×10 + wrinkles×3 + total")

    top10 = scored.copy()
    top10["score_v"] = top10["scrap_count"] * 10 + top10["wrinkle_count"] * 3 + top10["issue_count"]
    top10 = top10[top10["score_v"] > 0].sort_values("score_v", ascending=False).head(10)

    if not top10.empty:
        # Header
        c.setFillColor(BG); c.rect(MARGIN, y - 14, W - 2 * MARGIN, 14, fill=1, stroke=0)
        c.setFillColor(MED); c.setFont("Helvetica-Bold", 8)
        c.drawString(MARGIN + 6, y - 10, "#")
        c.drawString(MARGIN + 22, y - 10, "Part")
        c.drawString(MARGIN + 290, y - 10, "Stage")
        c.drawString(MARGIN + 360, y - 10, "Sev")
        c.drawString(MARGIN + 395, y - 10, "Iss")
        c.drawString(MARGIN + 425, y - 10, "Scr")
        c.drawString(MARGIN + 455, y - 10, "Wrk")
        c.drawString(MARGIN + 490, y - 10, "Score")
        y -= 14
        for i, (_, row) in enumerate(top10.iterrows(), 1):
            row_bg = BG2 if i % 2 == 0 else white
            c.setFillColor(row_bg); c.rect(MARGIN, y - 13, W - 2 * MARGIN, 13, fill=1, stroke=0)
            sev_color = SEV_BG.get(row["severity"], (LIGHT, BG))[0]
            c.setFillColor(sev_color); c.rect(MARGIN, y - 13, 3, 13, fill=1, stroke=0)

            c.setFillColor(DARK); c.setFont("Helvetica-Bold", 8.5)
            c.drawString(MARGIN + 6, y - 9, str(i))
            c.setFont("Helvetica", 8.5)
            summ = (row.get("summary") or "")[:48]
            c.drawString(MARGIN + 22, y - 9, summ)
            c.setFillColor(LIGHT); c.setFont("Helvetica", 8)
            c.drawString(MARGIN + 290, y - 9, str(row.get("stage", ""))[:14])
            c.setFillColor(sev_color); c.setFont("Helvetica-Bold", 8)
            c.drawString(MARGIN + 360, y - 9, row["severity"][:3])
            c.setFillColor(DARK); c.setFont("Helvetica", 8.5)
            c.drawString(MARGIN + 395, y - 9, str(row["issue_count"]))
            c.setFillColor(RED if row["scrap_count"] else DARK)
            c.drawString(MARGIN + 425, y - 9, str(row["scrap_count"]))
            c.setFillColor(ORANGE if row["wrinkle_count"] else DARK)
            c.drawString(MARGIN + 455, y - 9, str(row["wrinkle_count"]))
            c.setFillColor(DARK); c.setFont("Helvetica-Bold", 8.5)
            c.drawString(MARGIN + 490, y - 9, str(int(row["score_v"])))
            y -= 13
    else:
        c.setFillColor(LIGHT); c.setFont("Helvetica-Oblique", 9)
        c.drawString(MARGIN + 6, y - 12, "No flagged parts in pipeline.")
        y -= 18

    # ---- 3. Defect categories + 4. Detection points (side by side) ----
    y -= 8
    if y < 240:  # not enough room for 2 more sections + dow chart, push to a 2nd analytics page
        _draw_footer(c, page_state)
        c.showPage()
        page_state["page"] += 1
        page_state["total"] += 1
        _draw_header(c, date_str, time_str, shift, page_state, title="Quality Watch — Trends & Metrics (cont.)")
        y = H - 75

    col_w = (W - 2 * MARGIN - 12) / 2

    # Left col: Defect categories
    cat_x = MARGIN
    cat_y = y
    cat_y_start = cat_y
    c.setFillColor(HEADER_BG); c.rect(cat_x, cat_y - 16, col_w, 16, fill=1, stroke=0)
    c.setFillColor(white); c.setFont("Helvetica-Bold", 9.5)
    c.drawString(cat_x + 6, cat_y - 12, "DEFECT DRIVERS")
    c.setFont("Helvetica", 7.5)
    c.drawRightString(cat_x + col_w - 6, cat_y - 12, "6 months")
    cat_y -= 22

    cats = analytics.get("defect_categories") or []
    if cats:
        max_total = max(int(c_["total"]) for c_ in cats)
        bar_label_w = 70
        bar_x = cat_x + bar_label_w + 8
        bar_w = col_w - bar_label_w - 60
        for cat in cats:
            t_ = int(cat["total"]); s_ = int(cat["scraps"])
            rate = int(round(100 * s_ / t_)) if t_ else 0
            c.setFillColor(DARK); c.setFont("Helvetica-Bold", 7.5)
            c.drawString(cat_x + 6, cat_y - 4, cat["category"][:18])
            full_w = bar_w * t_ / max_total
            c.setFillColor(BG); c.rect(bar_x, cat_y - 7, full_w, 8, fill=1, stroke=0)
            if s_ > 0:
                scrap_w = bar_w * s_ / max_total
                c.setFillColor(RED); c.rect(bar_x, cat_y - 7, scrap_w, 8, fill=1, stroke=0)
            c.setFillColor(DARK); c.setFont("Helvetica-Bold", 7)
            c.drawString(bar_x + full_w + 4, cat_y - 4, str(t_))
            c.setFillColor(RED if rate >= 30 else LIGHT); c.setFont("Helvetica", 6.5)
            c.drawString(bar_x + full_w + 22, cat_y - 4, f"({rate}%)")
            cat_y -= 13

        c.setFillColor(LIGHT); c.setFont("Helvetica-Oblique", 6.5)
        c.drawString(cat_x + 6, cat_y - 6, "bar=total · red=scrap · % bold=≥30% scrap rate")
        cat_y -= 14
    else:
        c.setFillColor(LIGHT); c.setFont("Helvetica-Oblique", 8)
        c.drawString(cat_x + 6, cat_y, "No data.")

    # Right col: Detection points
    det_x = MARGIN + col_w + 12
    det_y = cat_y_start
    c.setFillColor(HEADER_BG); c.rect(det_x, det_y - 16, col_w, 16, fill=1, stroke=0)
    c.setFillColor(white); c.setFont("Helvetica-Bold", 9.5)
    c.drawString(det_x + 6, det_y - 12, "DETECTION POINTS")
    c.setFont("Helvetica", 7.5)
    c.drawRightString(det_x + col_w - 6, det_y - 12, "Where issues catch")
    det_y -= 22

    points = analytics.get("detection_points") or []
    if points:
        max_total = max(int(p["total"]) for p in points)
        bar_label_w = 110
        bar_x = det_x + bar_label_w + 8
        bar_w = col_w - bar_label_w - 50
        for pt in points:
            area = pt["area"].replace("Manufacturing Engineering - Composites Fabrication", "ME-Comp Fab")
            t_ = int(pt["total"]); s_ = int(pt["scraps"])
            rate = int(round(100 * s_ / t_)) if t_ else 0
            c.setFillColor(DARK); c.setFont("Helvetica-Bold", 7.5)
            c.drawString(det_x + 6, det_y - 4, area[:22])
            full_w = bar_w * t_ / max_total
            c.setFillColor(BG); c.rect(bar_x, det_y - 7, full_w, 8, fill=1, stroke=0)
            if s_ > 0:
                scrap_w = bar_w * s_ / max_total
                c.setFillColor(RED); c.rect(bar_x, det_y - 7, scrap_w, 8, fill=1, stroke=0)
            c.setFillColor(DARK); c.setFont("Helvetica-Bold", 7)
            c.drawString(bar_x + full_w + 4, det_y - 4, str(t_))
            c.setFillColor(LIGHT); c.setFont("Helvetica", 6.5)
            c.drawString(bar_x + full_w + 22, det_y - 4, f"({rate}% scr)")
            det_y -= 13

        c.setFillColor(LIGHT); c.setFont("Helvetica-Oblique", 6.5)
        c.drawString(det_x + 6, det_y - 6, "earlier catch = upstream shift = good")
        det_y -= 14
    else:
        c.setFillColor(LIGHT); c.setFont("Helvetica-Oblique", 8)
        c.drawString(det_x + 6, det_y, "No data.")

    y = min(cat_y, det_y) - 8

    # ---- 5. Day-of-week wrinkle pattern -------------------------------
    y -= 4
    if y < 100:
        _draw_footer(c, page_state)
        c.showPage()
        page_state["page"] += 1
        page_state["total"] += 1
        _draw_header(c, date_str, time_str, shift, page_state, title="Quality Watch — Trends & Metrics (cont.)")
        y = H - 75

    y = _section_banner(c, y, "DAY-OF-WEEK WRINKLE PATTERN", "Last 90 days")

    dow = analytics.get("dow_pattern") or []
    if dow:
        # Normalize to all 7 days
        dow_map = {row["dow"]: int(row["wrinkles"]) for row in dow}
        days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        counts = [dow_map.get(d, 0) for d in days]
        max_c = max(counts) or 1
        total_c = sum(counts)

        # Chart
        chart_h = 56
        chart_y = y - chart_h
        c.setFillColor(white); c.setStrokeColor(BORDER); c.setLineWidth(0.5)
        c.rect(MARGIN, chart_y, W - 2 * MARGIN, chart_h, fill=1, stroke=1)

        plot_x = MARGIN + 16
        plot_y = chart_y + 14
        plot_w = W - 2 * MARGIN - 200
        plot_h = chart_h - 22

        bar_gap = 6
        bar_w = (plot_w - bar_gap * 6) / 7
        for i, (d, n) in enumerate(zip(days, counts)):
            cx = plot_x + i * (bar_w + bar_gap)
            bh = plot_h * n / max_c if max_c > 0 else 0
            color = RED if n >= max_c * 0.8 else (ORANGE if n >= max_c * 0.4 else GREEN)
            c.setFillColor(color)
            c.rect(cx, plot_y, bar_w, bh, fill=1, stroke=0)
            c.setFillColor(DARK); c.setFont("Helvetica-Bold", 7.5)
            c.drawCentredString(cx + bar_w/2, plot_y + bh + 2, str(n))
            c.setFillColor(LIGHT); c.setFont("Helvetica", 7)
            c.drawCentredString(cx + bar_w/2, plot_y - 8, d)

        # Right-side observation
        ann_x = plot_x + plot_w + 14
        peak_idx = counts.index(max(counts))
        peak_day = days[peak_idx]
        wkday_total = sum(counts[:5])
        wknd_total  = sum(counts[5:])
        wkday_pct = int(round(100 * wkday_total / total_c)) if total_c else 0

        c.setFillColor(DARK); c.setFont("Helvetica-Bold", 8)
        c.drawString(ann_x, chart_y + chart_h - 10, "OBSERVATIONS")
        c.setFillColor(MED); c.setFont("Helvetica", 7.5)
        c.drawString(ann_x, chart_y + chart_h - 22, f"Peak: {peak_day} ({max(counts)} wrinkles)")
        c.drawString(ann_x, chart_y + chart_h - 32, f"Weekday share: {wkday_pct}%")
        c.drawString(ann_x, chart_y + chart_h - 42, f"Total: {total_c} wrinkles, 90 days")

        y = chart_y - 8
    else:
        c.setFillColor(LIGHT); c.setFont("Helvetica-Oblique", 9)
        c.drawString(MARGIN + 6, y - 12, "No wrinkle data in window.")
        y -= 24

    _draw_footer(c, page_state)
