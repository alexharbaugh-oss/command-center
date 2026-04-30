"""
Production Lead Command Center — Hand Layup
On-demand Quality Watch for Team Leads.

Deploy: Streamlit Community Cloud
Required secrets (Streamlit Cloud > App > Settings > Secrets):
  DATABRICKS_HOST       e.g. "joby-aviation-main.cloud.databricks.com"
  DATABRICKS_HTTP_PATH  e.g. "/sql/1.0/warehouses/<warehouse-id>"
  DATABRICKS_TOKEN      Service principal token (read-only on warehouse)
"""

import io
import re
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import streamlit as st
import pandas as pd
from databricks import sql as dbsql

# ============================================================
# CONFIG
# ============================================================
st.set_page_config(
    page_title="Production Command Center",
    page_icon="🛠️",
    layout="wide",
    initial_sidebar_state="expanded",
)

LOOKBACK_DAYS = 180  # 6 months of quality history

# Jira status -> pipeline stage (display name)
STATUS_TO_STAGE = {
    "Open":              "Ready to Schedule",
    "Scheduled":         "Scheduled",
    "Kit":               "Material Cutting",
    "Ready to Laminate": "Ready to Layup",
    "Laminate":          "Layup",
    "Ready to Cure":     "Ready to Cure",
}
STAGE_RANK = {s: i for i, s in enumerate([
    "Ready to Schedule", "Scheduled", "Material Cutting",
    "Ready to Layup", "Layup", "Ready to Cure",
])}

# Hand Layup originating areas in Ion
HAND_LAYUP_AREAS = ("527 Lamination", "527 Kitting")

# Patterns to exclude from pipeline (TESTdb, training, dev articles)
EXCLUDED_PATTERNS = [
    "TESTdb", "TEST PANEL", "PANEL: LAYUP TRAINING",
    "ADHESIVE PULLOFF PANEL", "NDI REFERENCE STANDARD",
    "JED00711 C", "JED00713", "JED000717", "CENTER SLEEVE, 5-PLY",
    "BUSBAR, 5-PLY", "DEV STATOR SLEEVE", "SPINNER FOR DB2",
    "JED00722", "STIFFENER RESIN RIDGE REDUCTION",
]
EXCLUDED_REGEX = "|".join(re.escape(p) for p in EXCLUDED_PATTERNS)

# Severity rule: 2+ scraps = RED, 1 scrap or 3+ issues = ORANGE,
# 1-2 issues = YELLOW, none = CLEAN
SEV_RANK = {"HOLD": 0, "RED": 1, "ORANGE": 2, "YELLOW": 3, "CLEAN": 4}
SEV_COLOR = {
    "HOLD":   "#6c3483",
    "RED":    "#c0392b",
    "ORANGE": "#d4730b",
    "YELLOW": "#d4920b",
    "CLEAN":  "#1D9E75",
}


# ============================================================
# DATABRICKS
# ============================================================

def _connect():
    host = st.secrets["DATABRICKS_HOST"].replace("https://", "").rstrip("/")
    return dbsql.connect(
        server_hostname=host,
        http_path=st.secrets["DATABRICKS_HTTP_PATH"],
        access_token=st.secrets["DATABRICKS_TOKEN"],
    )


def _run_query(sql: str) -> pd.DataFrame:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


PIPELINE_SQL = """
SELECT
    issue_id,
    part_number,
    summary,
    location,
    components,
    status,
    priority,
    due_date,
    created_at,
    sn_relation
FROM manufacturing.jira.issues
WHERE project_name = 'ME'
  AND status IN ('Open','Scheduled','Kit','Ready to Laminate','Laminate','Ready to Cure')
"""

QUALITY_SQL = f"""
SELECT
    issue_id,
    issue_title,
    created,
    disposition,
    originating_area,
    part_number,
    part_description,
    defect_code,
    issue_status,
    serialNumber,
    link_to_issue
FROM manufacturing.onion_silver.quality_issues
WHERE created >= DATE_SUB(CURRENT_DATE(), {LOOKBACK_DAYS})
  AND originating_area IN ('527 Lamination', '527 Kitting')
"""


@st.cache_data(ttl=300, show_spinner=False)
def load_pipeline() -> pd.DataFrame:
    df = _run_query(PIPELINE_SQL)
    if df.empty:
        return df
    mask = ~df["summary"].fillna("").str.contains(EXCLUDED_REGEX, case=False, regex=True)
    df = df.loc[mask].copy()
    df["stage"] = df["status"].map(STATUS_TO_STAGE)
    df["pn_norm"] = df["part_number"].apply(normalize_pn)
    df["stage_rank"] = df["stage"].map(STAGE_RANK)
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_quality() -> pd.DataFrame:
    df = _run_query(QUALITY_SQL)
    if df.empty:
        return df
    df["pn_norm"] = df["part_number"].apply(normalize_pn)
    df["clean_defect"] = df["defect_code"].apply(clean_defect_code)
    df["created_str"] = pd.to_datetime(df["created"]).dt.strftime("%-m/%-d")
    df["disposition_clean"] = df["disposition"].fillna("Pending")
    return df


# ============================================================
# HELPERS
# ============================================================

def normalize_pn(pn) -> str:
    """Strip serial/lot suffixes so cross-referencing finds the family."""
    if not pn or pn == "None":
        return ""
    s = str(pn).strip()
    for pat in [r"-X\d+-L\d+", r"-X\d+$", r"-S\d+$", r"-L\d+$", r"-TPDT$"]:
        s = re.sub(pat, "", s)
    return s


def clean_defect_code(dc) -> str:
    """'COF-WNK-Wrinkle' -> 'Wrinkle'.  Lists -> ' / '-joined."""
    if not dc:
        return ""
    s = str(dc).replace("[", "").replace("]", "").replace("'", "").replace('"', "")
    out = []
    for chunk in s.split(","):
        chunk = chunk.strip()
        bits = chunk.split("-", 2)
        if len(bits) == 3:
            out.append(bits[2])
        elif chunk:
            out.append(chunk)
    return " / ".join(out)


def classify_severity(scraps: int, total: int) -> str:
    if scraps >= 2:
        return "RED"
    if scraps >= 1 or total >= 3:
        return "ORANGE"
    if total >= 1:
        return "YELLOW"
    return "CLEAN"


def score_pipeline(pipeline: pd.DataFrame, quality: pd.DataFrame, holds: set) -> pd.DataFrame:
    """Attach quality history + severity to each pipeline part."""
    if pipeline.empty:
        return pipeline
    history = defaultdict(list)
    if not quality.empty:
        for r in quality.to_dict("records"):
            if r["pn_norm"]:
                history[r["pn_norm"]].append(r)

    out = []
    for p in pipeline.to_dict("records"):
        h = history.get(p["pn_norm"], [])
        scraps = sum(1 for i in h if i.get("disposition") == "Scrap")
        rework = sum(1 for i in h if i.get("disposition") == "Rework")
        pending = sum(1 for i in h if not i.get("disposition"))
        wrinkles = sum(1 for i in h if "wrinkle" in str(i.get("clean_defect", "")).lower())

        on_hold = (p.get("issue_id") or "") in holds
        if on_hold:
            sev = "HOLD"
        else:
            sev = classify_severity(scraps, len(h))

        out.append({
            **p,
            "issue_count":   len(h),
            "scrap_count":   scraps,
            "rework_count":  rework,
            "pending_count": pending,
            "wrinkle_count": wrinkles,
            "severity":      sev,
            "sev_rank":      SEV_RANK[sev],
            "history":       h,
        })
    return pd.DataFrame(out)


# ============================================================
# UI
# ============================================================

st.markdown("""
<style>
.block-container { padding-top: 1rem; max-width: 1100px; }
[data-testid="stHeader"] { background: transparent; }
.cmd-header {
  background: #1a2332; color: white; padding: 16px 22px;
  border-radius: 8px; margin-bottom: 14px;
}
.cmd-header h1 { font-size: 22px; font-weight: 700; margin: 0; color: white !important; }
.cmd-header .sub { font-size: 12px; color: #94a3b8; margin-top: 4px; }
.metric-row { display: flex; gap: 8px; margin-bottom: 18px; }
.metric-box { flex: 1; background: #f4f4f4; border-radius: 8px; padding: 10px 14px; }
.metric-box .label { font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 0.5px; }
.metric-box .val { font-size: 24px; font-weight: 700; line-height: 1.1; margin-top: 2px; }
.val-red    { color: #c0392b; }
.val-orange { color: #d4730b; }
.val-yellow { color: #d4920b; }
.val-green  { color: #1D9E75; }
.val-purple { color: #6c3483; }
.alert-card {
  border-left: 4px solid; padding: 10px 14px; margin-bottom: 8px;
  background: #f9f9f9; border-radius: 4px;
}
.alert-card.red    { border-color: #c0392b; background: #fae4e1; }
.alert-card.orange { border-color: #d4730b; background: #fef0e0; }
.alert-card.yellow { border-color: #d4920b; background: #fef5e7; }
.alert-card.hold   { border-color: #6c3483; background: #f0e6f5; }
.alert-card .pn       { font-weight: 700; font-size: 13.5px; color: #1a1a1a; }
.alert-card .meta     { font-size: 11px; color: #555; margin-top: 3px; }
.alert-card .badge {
  display: inline-block; font-size: 10px; font-weight: 700;
  padding: 1px 6px; border-radius: 3px; color: white; margin-right: 6px;
}
.alert-card .badge.red    { background: #c0392b; }
.alert-card .badge.orange { background: #d4730b; }
.alert-card .badge.yellow { background: #d4920b; color: #1a1a1a; }
.alert-card .badge.hold   { background: #6c3483; }
.history-line { font-size: 11px; color: #444; margin-top: 4px; padding-left: 8px; border-left: 2px solid #ccc; }
.history-scrap { color: #c0392b; font-weight: 600; }
</style>
""", unsafe_allow_html=True)

now_pt = datetime.now(timezone.utc) + timedelta(hours=-7)
ts_str = now_pt.strftime("%a %b %-d, %Y · %-I:%M %p PT")

st.markdown(f"""
<div class="cmd-header">
  <h1>🛠️ Production Lead Command Center</h1>
  <div class="sub">Hand Layup · Quality Watch · {ts_str}</div>
</div>
""", unsafe_allow_html=True)

# Sidebar — controls
with st.sidebar:
    st.subheader("Controls")
    if st.button("🔄 Refresh data", use_container_width=True, type="primary"):
        st.cache_data.clear()
        st.rerun()
    st.caption("Cached for 5 min. Hit refresh to re-pull from Databricks.")

    st.divider()
    st.subheader("Hold list")
    st.caption("Parts to flag as HOLD regardless of quality history.")
    hold_text = st.text_area(
        "ME-IDs (one per line)",
        value=st.session_state.get("holds_text", ""),
        height=100,
        placeholder="ME-43116\nME-46598",
        label_visibility="collapsed",
    )
    st.session_state["holds_text"] = hold_text
    holds = {h.strip() for h in hold_text.splitlines() if h.strip()}

    st.divider()
    st.caption("**Severity rules**")
    st.caption("🔴 RED — 2+ scrap events")
    st.caption("🟠 ORANGE — 1 scrap or 3+ issues")
    st.caption("🟡 YELLOW — 1–2 issues, no scrap")
    st.caption("🟢 CLEAN — no quality history")
    st.caption("🟣 HOLD — manual flag")

    st.divider()
    st.caption(f"Lookback: {LOOKBACK_DAYS} days")
    st.caption("Source: manufacturing.jira.issues + onion_silver.quality_issues")

# Load data
try:
    with st.spinner("Pulling pipeline + 6 months of quality history…"):
        pipeline_df = load_pipeline()
        quality_df = load_quality()
except Exception as e:
    st.error(f"❌ Couldn't connect to Databricks: {e}")
    st.info(
        "Check Streamlit Cloud Secrets:\n\n"
        "- `DATABRICKS_HOST`\n"
        "- `DATABRICKS_HTTP_PATH`\n"
        "- `DATABRICKS_TOKEN`"
    )
    st.stop()

if pipeline_df.empty:
    st.warning("No parts found in pipeline. Check Jira filters.")
    st.stop()

scored = score_pipeline(pipeline_df, quality_df, holds)

# Top metrics
sev_counts = scored["severity"].value_counts()
red    = int(sev_counts.get("RED", 0))
orange = int(sev_counts.get("ORANGE", 0))
yellow = int(sev_counts.get("YELLOW", 0))
clean  = int(sev_counts.get("CLEAN", 0))
hold_n = int(sev_counts.get("HOLD", 0))

st.markdown(f"""
<div class="metric-row">
  <div class="metric-box"><div class="label">Pipeline parts</div><div class="val">{len(scored)}</div></div>
  <div class="metric-box"><div class="label">🟣 Hold</div><div class="val val-purple">{hold_n}</div></div>
  <div class="metric-box"><div class="label">🔴 Red</div><div class="val val-red">{red}</div></div>
  <div class="metric-box"><div class="label">🟠 Orange</div><div class="val val-orange">{orange}</div></div>
  <div class="metric-box"><div class="label">🟡 Yellow</div><div class="val val-yellow">{yellow}</div></div>
  <div class="metric-box"><div class="label">🟢 Clean</div><div class="val val-green">{clean}</div></div>
</div>
""", unsafe_allow_html=True)

# ============================================================
# RENDERERS
# ============================================================

def render_alert(row, show_history=True, max_history=5):
    sev = row["severity"]
    if sev == "CLEAN":
        return
    cls = sev.lower()
    summary = (row.get("summary") or "(no summary)").strip()
    pn = row.get("part_number") or ""
    issue_id = row.get("issue_id") or ""
    stage = row.get("stage") or ""

    # Defect codes (deduped) from history
    defect_set = []
    for h in row.get("history") or []:
        d = h.get("clean_defect")
        if d and d not in defect_set:
            defect_set.append(d)
    defect_str = ", ".join(defect_set[:5])

    counts_bits = []
    if row["scrap_count"]:
        counts_bits.append(f"<b>{row['scrap_count']} scrap</b>")
    if row["rework_count"]:
        counts_bits.append(f"{row['rework_count']} rework")
    if row["pending_count"]:
        counts_bits.append(f"{row['pending_count']} pending")
    if row["wrinkle_count"]:
        counts_bits.append(f"{row['wrinkle_count']} wrinkles")
    counts = " · ".join(counts_bits) if counts_bits else f"{row['issue_count']} issues"

    history_html = ""
    if show_history and row.get("history"):
        items = sorted(row["history"], key=lambda x: x.get("created") or 0, reverse=True)[:max_history]
        for h in items:
            d = h.get("disposition_clean") or "Pending"
            scrap_class = "history-scrap" if d == "Scrap" else ""
            line = (
                f"<div class='history-line'>"
                f"<span class='{scrap_class}'>{h.get('created_str','')} · {d}</span>"
                f" — {h.get('clean_defect') or h.get('issue_title','')}"
                f"</div>"
            )
            history_html += line

    st.markdown(f"""
    <div class="alert-card {cls}">
      <span class="badge {cls}">{sev}</span>
      <span class="pn">{summary}</span>
      <div class="meta">
        {issue_id} · {pn} · <b>{stage}</b> · {counts}
        {f"<br/>Defects: {defect_str}" if defect_str else ""}
      </div>
      {history_html}
    </div>
    """, unsafe_allow_html=True)


def filter_and_sort(df, stages):
    sub = df[df["stage"].isin(stages)].copy()
    sub = sub.sort_values(
        ["sev_rank", "scrap_count", "issue_count"],
        ascending=[True, False, False],
    )
    return sub


# ============================================================
# TABS
# ============================================================

tab_floor, tab_next, tab_up, tab_search, tab_export = st.tabs([
    "🏭 On the Floor",
    "📋 Ready to Layup",
    "⏰ Upstream",
    "🔍 Search",
    "📄 Export",
])

with tab_floor:
    st.subheader("On the Floor")
    st.caption("Parts in **Layup** + **Ready to Cure** — what to check during your walk")
    on_floor = filter_and_sort(scored, ["Layup", "Ready to Cure"])
    flagged = on_floor[on_floor["severity"] != "CLEAN"]
    st.write(f"**{len(on_floor)}** parts on the floor — **{len(flagged)}** need attention")
    if flagged.empty:
        st.success("✅ No flagged parts on the floor right now.")
    else:
        for _, row in flagged.iterrows():
            render_alert(row)


with tab_next:
    st.subheader("Ready to Layup")
    st.caption("Parts about to hit the tool — plan before they start")
    next_up = filter_and_sort(scored, ["Ready to Layup"])
    flagged = next_up[next_up["severity"] != "CLEAN"]
    st.write(f"**{len(next_up)}** parts queued — **{len(flagged)}** need a plan")
    if flagged.empty:
        st.success("✅ No flagged parts ready to layup.")
    else:
        for _, row in flagged.iterrows():
            render_alert(row)


with tab_up:
    st.subheader("Upstream")
    st.caption("Material Cutting + Scheduled + Open — flag before they arrive")
    upstream = filter_and_sort(scored, ["Material Cutting", "Scheduled", "Ready to Schedule"])
    flagged = upstream[upstream["severity"].isin(["RED", "ORANGE", "HOLD"])]
    st.write(f"**{len(upstream)}** parts upstream — showing **{len(flagged)}** RED/ORANGE/HOLD")
    if flagged.empty:
        st.info("Nothing critical upstream right now.")
    else:
        for _, row in flagged.iterrows():
            render_alert(row, show_history=False)


with tab_search:
    st.subheader("Search any part")
    q = st.text_input(
        "Part name, part number, ME-ID, or defect — partial match",
        placeholder="e.g., wing skin   |   213565   |   ME-45578   |   wrinkle",
    )
    if q:
        ql = q.lower().strip()

        # Search pipeline parts
        mask = (
            scored["summary"].fillna("").str.lower().str.contains(ql, regex=False) |
            scored["part_number"].fillna("").str.lower().str.contains(ql, regex=False) |
            scored["issue_id"].fillna("").str.lower().str.contains(ql, regex=False)
        )
        hits = scored.loc[mask].sort_values(["sev_rank", "stage_rank"])
        st.write(f"**{len(hits)}** pipeline match(es)")
        for _, row in hits.head(40).iterrows():
            render_alert(row)

        # Also search loose quality history (parts NOT currently in pipeline)
        if not quality_df.empty:
            qmask = (
                quality_df["part_description"].fillna("").str.lower().str.contains(ql, regex=False) |
                quality_df["part_number"].fillna("").str.lower().str.contains(ql, regex=False) |
                quality_df["clean_defect"].fillna("").str.lower().str.contains(ql, regex=False) |
                quality_df["issue_title"].fillna("").str.lower().str.contains(ql, regex=False)
            )
            qhits = quality_df.loc[qmask]
            in_pipeline = set(hits["pn_norm"].dropna()) if not hits.empty else set()
            qhits = qhits[~qhits["pn_norm"].isin(in_pipeline)]
            if not qhits.empty:
                st.divider()
                st.write(f"**{len(qhits)}** quality-history match(es) (not in pipeline)")
                show = qhits[[
                    "created_str", "part_description", "part_number",
                    "clean_defect", "disposition_clean", "originating_area"
                ]].rename(columns={
                    "created_str": "Date",
                    "part_description": "Part",
                    "part_number": "PN",
                    "clean_defect": "Defect",
                    "disposition_clean": "Disposition",
                    "originating_area": "Area",
                })
                st.dataframe(show, hide_index=True, use_container_width=True)
    else:
        st.info("Start typing to search.")


with tab_export:
    st.subheader("Export")
    st.caption("Download the current snapshot as CSV or PDF")

    # CSV download
    flat_cols = [
        "issue_id", "part_number", "summary", "stage", "severity",
        "issue_count", "scrap_count", "rework_count", "pending_count", "wrinkle_count",
    ]
    csv_buf = scored[flat_cols].to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Download CSV",
        data=csv_buf,
        file_name=f"quality_watch_{now_pt.strftime('%Y-%m-%d_%H%M')}.csv",
        mime="text/csv",
        use_container_width=True,
    )

    # PDF download — same look as the V2 scheduled report
    if st.button("📄 Generate PDF report", use_container_width=True, type="primary"):
        with st.spinner("Building PDF…"):
            from pdf_export import build_pdf
            pdf_bytes = build_pdf(scored, now_pt)
        shift = "AM" if now_pt.hour < 12 else "PM"
        fname = f"Kanban_Quality_Watch_{now_pt.strftime('%-m-%-d-%y')}_{shift}.pdf"
        st.download_button(
            "⬇️ Download PDF",
            data=pdf_bytes,
            file_name=fname,
            mime="application/pdf",
            use_container_width=True,
        )
        st.success(f"PDF ready: **{fname}**")
