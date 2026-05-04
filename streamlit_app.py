"""
Production Lead Command Center — Hand Layup
On-demand Quality Watch + Analytics for Team Leads.

Deploy: Streamlit Community Cloud
Required secrets:
  DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN

Locked filters per spec (4/22/26):
  - Pipeline: project=ME, components=Composite Part, type!=Epic,
              status in (Scheduled, Kit, Ready to Laminate, Laminate)
  - Quality:  4 comp shop areas, 6mo, issue_status!=deleted, no battery/busbar/sleeve 5-ply
  - Severity: RED if scraps>=2 OR wrinkles>=3; ORANGE if scraps>=1 OR total>=3 OR wrinkles>=2

Snapshots: appends to manufacturing.default.kqw_snapshots so Analytics
"What Changed" can diff current vs prior shift.
Shift hand-off: 5:30 PM PT.
"""

import io
import re
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict

import streamlit as st
import pandas as pd
from databricks import sql as dbsql

# Optional Plotly import (degrade gracefully if not installed)
try:
    import plotly.express as px
    import plotly.graph_objects as go
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

# ============================================================
# CONFIG
# ============================================================
st.set_page_config(
    page_title="Production Command Center",
    page_icon="🛠️",
    layout="wide",
    initial_sidebar_state="expanded",
)

LOOKBACK_DAYS = 180
SNAPSHOT_TABLE = "manufacturing.default.kqw_snapshots"
SNAPSHOT_MIN_GAP_MINUTES = 30

STATUS_TO_STAGE = {
    "Scheduled":         "Scheduled",
    "Kit":               "Material Cutting",
    "Ready to Laminate": "Ready to Layup",
    "Laminate":          "Layup",
}
STAGE_RANK = {s: i for i, s in enumerate([
    "Scheduled", "Material Cutting", "Ready to Layup", "Layup",
])}

COMP_SHOP_AREAS = (
    "527 Lamination",
    "527 Kitting",
    "527 Hand Trim",
    "Manufacturing Engineering - Composites Fabrication",
)
COMP_SHOP_AREAS_SQL = ", ".join(f"'{a}'" for a in COMP_SHOP_AREAS)

EXCLUDED_PATTERNS = [
    "TESTdb", "TEST PANEL", "PANEL: LAYUP TRAINING",
    "ADHESIVE PULLOFF PANEL", "NDI REFERENCE STANDARD",
    "JED00711 C", "JED00713", "JED000717",
    "DEV STATOR SLEEVE", "SPINNER FOR DB2",
    "JED00722", "STIFFENER RESIN RIDGE REDUCTION",
]
EXCLUDED_REGEX = "|".join(re.escape(p) for p in EXCLUDED_PATTERNS)

SEV_RANK = {"RED": 0, "ORANGE": 1, "YELLOW": 2, "CLEAN": 3}
SEV_COLOR = {
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


def _run_statement(sql: str):
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


PIPELINE_SQL = """
SELECT
    issue_id,
    order_number,
    part_number,
    summary,
    components,
    status,
    priority,
    due_date,
    created_at,
    updated_at
FROM manufacturing.jira.issues
WHERE project_name = 'ME'
  AND issue_type != 'Epic'
  AND components = 'Composite Part'
  AND status IN ('Scheduled', 'Kit', 'Ready to Laminate', 'Laminate')
  AND upper(coalesce(summary, '')) NOT LIKE '%BATTERY%'
  AND upper(coalesce(summary, '')) NOT LIKE '%BUSBAR%'
  AND upper(coalesce(summary, '')) NOT LIKE '%SLEEVE, 5-PLY%'
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
    link_to_issue,
    CASE
      WHEN lower(coalesce(defect_code, '')) LIKE '%wnk%'
        OR lower(coalesce(issue_title, '')) LIKE '%wrinkle%'
        OR lower(coalesce(issue_title, '')) LIKE '%winkel%'
      THEN 1 ELSE 0
    END AS is_wrinkle
FROM manufacturing.onion_silver.quality_issues_view
WHERE CAST(created AS DATE) >= CURRENT_DATE() - INTERVAL {LOOKBACK_DAYS} DAYS
  AND issue_status != 'deleted'
  AND originating_area IN ({COMP_SHOP_AREAS_SQL})
  AND upper(coalesce(part_description, '')) NOT LIKE '%BATTERY%'
  AND upper(coalesce(part_description, '')) NOT LIKE '%BUSBAR%'
  AND upper(coalesce(part_description, '')) NOT LIKE '%SLEEVE, 5-PLY%'
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
    df["created_dt"] = pd.to_datetime(df["created"])
    df["created_str"] = df["created_dt"].dt.strftime("%-m/%-d")
    df["created_date"] = df["created_dt"].dt.date
    df["dow"] = df["created_dt"].dt.day_name()
    df["dow_n"] = df["created_dt"].dt.weekday  # Mon=0
    df["disposition_clean"] = df["disposition"].fillna("Pending")
    return df


# ============================================================
# SNAPSHOT / DELTA
# ============================================================

def _esc(s):
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


def _shift_for(now_pt: datetime) -> str:
    return "AM" if (now_pt.hour, now_pt.minute) < (17, 30) else "PM"


def write_snapshot_if_due(scored: pd.DataFrame, now_pt: datetime) -> str:
    if scored.empty:
        return "No pipeline data — no snapshot written."
    shift = _shift_for(now_pt)
    snap_date_str = now_pt.strftime("%Y-%m-%d")
    try:
        last = _run_query(f"""
            SELECT MAX(snapshot_ts) AS last_ts, MAX(shift) AS last_shift
            FROM {SNAPSHOT_TABLE}
            WHERE snapshot_date = DATE '{snap_date_str}' AND shift = '{shift}'
        """)
        if not last.empty and last.iloc[0]["last_ts"] is not None:
            last_ts = pd.to_datetime(last.iloc[0]["last_ts"])
            last_ts_pt = last_ts.tz_localize("UTC").tz_convert("America/Los_Angeles").tz_localize(None) \
                if last_ts.tzinfo is None else last_ts.tz_convert("America/Los_Angeles").tz_localize(None)
            now_naive = now_pt.replace(tzinfo=None)
            elapsed_min = (now_naive - last_ts_pt).total_seconds() / 60.0
            if 0 <= elapsed_min < SNAPSHOT_MIN_GAP_MINUTES:
                return f"Snapshot already taken {int(elapsed_min)} min ago. Skipping."
    except Exception:
        pass

    ts_str = now_pt.strftime("%Y-%m-%d %H:%M:%S")
    rows_sql = []
    for _, r in scored.iterrows():
        rows_sql.append(
            "("
            f"TIMESTAMP '{ts_str}', DATE '{snap_date_str}', '{shift}',"
            f"{_esc(r.get('issue_id'))},{_esc(r.get('order_number'))},"
            f"{_esc(r.get('part_number'))},{_esc(r.get('pn_norm'))},"
            f"{_esc((r.get('summary') or '')[:200])},"
            f"{_esc(r.get('status'))},{_esc(r.get('stage'))},"
            f"{_esc(r.get('severity'))},"
            f"{int(r.get('issue_count') or 0)},{int(r.get('scrap_count') or 0)},"
            f"{int(r.get('wrinkle_count') or 0)},{int(r.get('rework_count') or 0)},"
            f"{int(r.get('pending_count') or 0)}"
            ")"
        )
    CHUNK = 100
    inserted = 0
    for i in range(0, len(rows_sql), CHUNK):
        chunk = rows_sql[i:i+CHUNK]
        sql = f"INSERT INTO {SNAPSHOT_TABLE} VALUES " + ",".join(chunk)
        _run_statement(sql)
        inserted += len(chunk)
    return f"Snapshot saved — {inserted} parts at {ts_str} {shift}."


def load_prior_snapshot(now_pt: datetime) -> pd.DataFrame:
    shift = _shift_for(now_pt)
    today_str = now_pt.strftime("%Y-%m-%d")
    if shift == "AM":
        cutoff_clause = f"snapshot_date < DATE '{today_str}'"
    else:
        cutoff_clause = f"snapshot_date = DATE '{today_str}' AND shift = 'AM'"
    sql = f"""
    WITH ranked AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY pn_norm ORDER BY snapshot_ts DESC) AS rn
      FROM {SNAPSHOT_TABLE}
      WHERE {cutoff_clause}
    )
    SELECT snapshot_ts, snapshot_date, shift, me_key, mfid, pn_raw, pn_norm,
           summary, status, stage, severity,
           total_issues, scraps, wrinkles, rework, pending
    FROM ranked WHERE rn = 1
    """
    try:
        return _run_query(sql)
    except Exception:
        return pd.DataFrame()


def compute_deltas(current: pd.DataFrame, prior: pd.DataFrame) -> dict:
    out = {k: [] for k in [
        "new_in_pipeline", "gone_from_pipeline", "stage_changed",
        "new_scrap", "new_wrinkle", "severity_up", "severity_down",
    ]}
    if prior.empty:
        return out
    cur_by_me = {r["issue_id"]: r for _, r in current.iterrows()}
    pri_by_me = {r["me_key"]: r for _, r in prior.iterrows()}
    cur_keys = set(cur_by_me.keys())
    pri_keys = set(pri_by_me.keys())
    for k in cur_keys - pri_keys:
        out["new_in_pipeline"].append(cur_by_me[k])
    for k in pri_keys - cur_keys:
        out["gone_from_pipeline"].append(pri_by_me[k])
    for k in cur_keys & pri_keys:
        cur = cur_by_me[k]; pri = pri_by_me[k]
        if (cur.get("stage") or "") != (pri.get("stage") or ""):
            out["stage_changed"].append({"row": cur, "from": pri.get("stage"), "to": cur.get("stage")})
        cur_scr = int(cur.get("scrap_count") or 0); pri_scr = int(pri.get("scraps") or 0)
        if cur_scr > pri_scr:
            out["new_scrap"].append({"row": cur, "from": pri_scr, "to": cur_scr})
        cur_wnk = int(cur.get("wrinkle_count") or 0); pri_wnk = int(pri.get("wrinkles") or 0)
        if cur_wnk > pri_wnk:
            out["new_wrinkle"].append({"row": cur, "from": pri_wnk, "to": cur_wnk})
        cur_sev = SEV_RANK.get(cur.get("severity"), 99); pri_sev = SEV_RANK.get(pri.get("severity"), 99)
        if cur_sev < pri_sev:
            out["severity_up"].append({"row": cur, "from": pri.get("severity"), "to": cur.get("severity")})
        elif cur_sev > pri_sev:
            out["severity_down"].append({"row": cur, "from": pri.get("severity"), "to": cur.get("severity")})
    return out


# ============================================================
# HELPERS
# ============================================================

def normalize_pn(pn) -> str:
    if not pn or pn == "None":
        return ""
    s = str(pn).strip()
    s = re.sub(r"-X\d+.*$", "", s)
    s = re.sub(r"-S\d+$",   "", s)
    s = re.sub(r"-L\d+$",   "", s)
    s = re.sub(r"-TPDT$",   "", s)
    return s


def clean_defect_code(dc) -> str:
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


def classify_severity(scraps: int, wrinkles: int, total: int) -> str:
    if scraps >= 2 or wrinkles >= 3:
        return "RED"
    if scraps >= 1 or total >= 3 or wrinkles >= 2:
        return "ORANGE"
    if total >= 1:
        return "YELLOW"
    return "CLEAN"


def categorize_defect(defect_code: str) -> str:
    """Map defect_code to a clean category for analytics."""
    if not defect_code:
        return "Other"
    dc = str(defect_code).upper()
    if "WNK" in dc:                 return "Wrinkles"
    if "FOD" in dc:                 return "FOD / Particulate"
    if "RES" in dc:                 return "Resin Ridges"
    if "MIS" in dc:                 return "Missing Features"
    if "ACP" in dc or "FMV" in dc:  return "Cure Profile"
    if "DIM" in dc:                 return "Dim OOT"
    if "NDI" in dc:                 return "NDI Defects"
    if "RSA" in dc:                 return "Resin Starvation"
    if "EDL" in dc:                 return "Edge Delamination"
    if "SFD" in dc:                 return "Surface Depression"
    return "Other"


def score_pipeline(pipeline: pd.DataFrame, quality: pd.DataFrame) -> pd.DataFrame:
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
        scraps   = sum(1 for i in h if i.get("disposition") == "Scrap")
        rework   = sum(1 for i in h if i.get("disposition") == "Rework")
        pending  = sum(1 for i in h if not i.get("disposition"))
        wrinkles = sum(1 for i in h if int(i.get("is_wrinkle") or 0) == 1)
        sev = classify_severity(scraps, wrinkles, len(h))
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


def filter_quality_by_window(quality: pd.DataFrame, window_days: int) -> pd.DataFrame:
    if quality.empty:
        return quality
    today = pd.Timestamp.now().normalize()
    cutoff = today - pd.Timedelta(days=window_days)
    return quality[quality["created_dt"] >= cutoff].copy()


def compute_kpis(quality: pd.DataFrame) -> dict:
    if quality.empty:
        return {"issues": 0, "scraps": 0, "wrinkles": 0, "rework": 0,
                "pending": 0, "uai": 0, "scrap_rate": 0.0, "wrinkle_rate": 0.0}
    n = len(quality)
    disp = quality["disposition"].fillna("")
    scraps = int((disp == "Scrap").sum())
    rework = int((disp == "Rework").sum())
    uai    = int((disp == "Use As Is").sum())
    pending = int(((disp == "") | (disp == "Pending")).sum())
    wrinkles = int(quality["is_wrinkle"].sum())
    return {
        "issues":       n,
        "scraps":       scraps,
        "wrinkles":     wrinkles,
        "rework":       rework,
        "pending":      pending,
        "uai":          uai,
        "scrap_rate":   100.0 * scraps / n if n else 0.0,
        "wrinkle_rate": 100.0 * wrinkles / n if n else 0.0,
    }


def pct_delta(curr: float, prior: float) -> float:
    if prior == 0:
        return 0.0 if curr == 0 else 100.0
    return 100.0 * (curr - prior) / prior


def fmt_delta(curr: float, prior: float, lower_is_better: bool = True) -> tuple:
    """Return (text, color) for a delta. Green = good, Red = bad."""
    if prior == 0 and curr == 0:
        return ("—", "#999999")
    pct = pct_delta(curr, prior)
    arrow = "↑" if pct > 0 else ("↓" if pct < 0 else "→")
    sign = "+" if pct > 0 else ""
    if lower_is_better:
        color = "#1D9E75" if pct < 0 else ("#c0392b" if pct > 0 else "#999999")
    else:
        color = "#1D9E75" if pct > 0 else ("#c0392b" if pct < 0 else "#999999")
    return (f"{arrow} {sign}{pct:.0f}%", color)


# ============================================================
# UI / STYLES
# ============================================================

st.markdown("""
<style>
.block-container { padding-top: 1rem; max-width: 1200px; }
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
.metric-box .val { font-size: 24px; font-weight: 700; line-height: 1.1; margin-top: 2px; color: #1a1a1a; }
.metric-box .delta { font-size: 11px; font-weight: 600; margin-top: 4px; }
.metric-box .sub { font-size: 10px; color: #888; margin-top: 2px; }
.val-red    { color: #c0392b; }
.val-orange { color: #d4730b; }
.val-yellow { color: #d4920b; }
.val-green  { color: #1D9E75; }
.alert-card {
  border-left: 4px solid; padding: 10px 14px; margin-bottom: 8px;
  background: #f9f9f9; border-radius: 4px;
}
.alert-card.red    { border-color: #c0392b; background: #fae4e1; }
.alert-card.orange { border-color: #d4730b; background: #fef0e0; }
.alert-card.yellow { border-color: #d4920b; background: #fef5e7; }
.alert-card .pn   { font-weight: 700; font-size: 13.5px; color: #1a1a1a; }
.alert-card .meta { font-size: 11px; color: #555; margin-top: 3px; }
.alert-card .badge {
  display: inline-block; font-size: 10px; font-weight: 700;
  padding: 1px 6px; border-radius: 3px; color: white; margin-right: 6px;
}
.alert-card .badge.red    { background: #c0392b; }
.alert-card .badge.orange { background: #d4730b; }
.alert-card .badge.yellow { background: #d4920b; color: #1a1a1a; }
.history-line { font-size: 11px; color: #444; margin-top: 4px; padding-left: 8px; border-left: 2px solid #ccc; }
.history-scrap { color: #c0392b; font-weight: 600; }
.section-header {
  background: #1a2332; color: white; padding: 8px 14px;
  border-radius: 4px; margin: 18px 0 10px 0;
  font-weight: 700; font-size: 13px; letter-spacing: 0.5px;
}
.goal-card {
  background: linear-gradient(to right, #f4f4f4, #f9f9f9);
  border-radius: 8px; padding: 16px 20px; margin-bottom: 14px;
  border-left: 5px solid #2471a3;
}
.goal-card.on-pace  { border-left-color: #1D9E75; }
.goal-card.off-pace { border-left-color: #c0392b; }
.goal-card h3 { margin: 0 0 6px 0; font-size: 14px; color: #1a2332; }
.goal-card .progress {
  height: 18px; background: #e5e5e5; border-radius: 9px; overflow: hidden;
  margin: 8px 0;
}
.goal-card .progress-fill {
  height: 100%; background: linear-gradient(to right, #1D9E75, #2471a3);
}
.goal-card .progress-fill.warn { background: linear-gradient(to right, #d4920b, #d4730b); }
.goal-card .progress-fill.bad  { background: linear-gradient(to right, #c0392b, #d4730b); }
.delta-section {
  background: #f9f9f9; border-radius: 6px; padding: 12px 16px;
  margin-bottom: 14px; border-left: 4px solid #2471a3;
}
.delta-section.warn  { border-left-color: #c0392b; background: #fae4e1; }
.delta-section.move  { border-left-color: #d4730b; background: #fef0e0; }
.delta-section.good  { border-left-color: #1D9E75; background: #e8f6f0; }
.delta-section h4 { margin: 0 0 8px 0; font-size: 14px; color: #1a1a1a; }
.delta-line { font-size: 12px; color: #1a1a1a; margin: 4px 0; }
.delta-summary {
  background: #1a2332; color: white; padding: 14px 20px; border-radius: 8px;
  margin-bottom: 16px;
}
.delta-summary h3 { margin: 0 0 6px 0; font-size: 16px; color: white; }
.delta-summary .sub { font-size: 12px; color: #94a3b8; }
.delta-summary .stats { font-size: 13px; margin-top: 8px; }
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

# Sidebar
with st.sidebar:
    st.subheader("Controls")
    if st.button("🔄 Refresh data", use_container_width=True, type="primary"):
        st.cache_data.clear()
        st.rerun()
    st.caption("Cached for 5 min. Hit refresh to re-pull from Databricks.")
    st.divider()
    st.caption("**Severity rules**")
    st.caption("🔴 RED — 2+ scraps OR 3+ wrinkles")
    st.caption("🟠 ORANGE — 1 scrap, 3+ issues, OR 2+ wrinkles")
    st.caption("🟡 YELLOW — 1+ issue, no scrap")
    st.caption("🟢 CLEAN — no quality history")
    st.divider()
    st.caption(f"Lookback: {LOOKBACK_DAYS} days")
    st.caption("Areas: 527 Lam, Kitting, Hand Trim, ME-Comp Fab")
    st.caption(f"Snapshots: {SNAPSHOT_TABLE}")
    st.caption("Shift hand-off: 5:30 PM PT")
    if not HAS_PLOTLY:
        st.warning("Plotly not installed — charts will use simpler fallbacks. Add `plotly` to requirements.txt.")

# Load data
try:
    with st.spinner("Pulling pipeline + 6 months of quality history…"):
        pipeline_df = load_pipeline()
        quality_df = load_quality()
except Exception as e:
    st.error(f"❌ Couldn't connect to Databricks: {e}")
    st.stop()

if pipeline_df.empty:
    st.warning("No parts found in pipeline.")
    st.stop()

scored = score_pipeline(pipeline_df, quality_df)

# Snapshot
if "snapshot_written" not in st.session_state:
    try:
        msg = write_snapshot_if_due(scored, now_pt)
        st.session_state["snapshot_written"] = True
        st.session_state["snapshot_msg"] = msg
    except Exception as e:
        st.session_state["snapshot_msg"] = f"Snapshot write failed: {e}"

# Top metrics
sev_counts = scored["severity"].value_counts()
red    = int(sev_counts.get("RED", 0))
orange = int(sev_counts.get("ORANGE", 0))
yellow = int(sev_counts.get("YELLOW", 0))
clean  = int(sev_counts.get("CLEAN", 0))

st.markdown(f"""
<div class="metric-row">
  <div class="metric-box"><div class="label">Pipeline parts</div><div class="val">{len(scored)}</div></div>
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
    mfid = row.get("order_number") or "—"
    stage = row.get("stage") or ""

    defect_set = []
    for h in row.get("history") or []:
        d = h.get("clean_defect")
        if d and d not in defect_set:
            defect_set.append(d)
    defect_str = ", ".join(defect_set[:5])

    counts_bits = []
    if row["scrap_count"]:    counts_bits.append(f"<b>{row['scrap_count']} scrap</b>")
    if row["rework_count"]:   counts_bits.append(f"{row['rework_count']} rework")
    if row["pending_count"]:  counts_bits.append(f"{row['pending_count']} pending")
    if row["wrinkle_count"]:  counts_bits.append(f"{row['wrinkle_count']} wrinkles")
    counts = " · ".join(counts_bits) if counts_bits else f"{row['issue_count']} issues"

    history_html = ""
    if show_history and row.get("history"):
        items = sorted(row["history"], key=lambda x: x.get("created") or 0, reverse=True)[:max_history]
        for h in items:
            d = h.get("disposition_clean") or "Pending"
            scrap_class = "history-scrap" if d == "Scrap" else ""
            history_html += (
                f"<div class='history-line'>"
                f"<span class='{scrap_class}'>{h.get('created_str','')} · {d}</span>"
                f" — {h.get('clean_defect') or h.get('issue_title','')}"
                f"</div>"
            )

    st.markdown(f"""
    <div class="alert-card {cls}">
      <span class="badge {cls}">{sev}</span>
      <span class="pn">{summary}</span>
      <div class="meta">
        {issue_id} · MFID-{mfid} · {pn} · <b>{stage}</b> · {counts}
        {f"<br/>Defects: {defect_str}" if defect_str else ""}
      </div>
      {history_html}
    </div>
    """, unsafe_allow_html=True)


def filter_and_sort(df, stages):
    sub = df[df["stage"].isin(stages)].copy()
    sub = sub.sort_values(
        ["sev_rank", "scrap_count", "wrinkle_count", "issue_count"],
        ascending=[True, False, False, False],
    )
    return sub


def render_delta_line(row, prefix="", suffix=""):
    summary = (row.get("summary") or "")[:80]
    me = row.get("issue_id") or row.get("me_key") or ""
    mfid = row.get("order_number") or row.get("mfid") or "—"
    sev = row.get("severity") or ""
    sev_color = SEV_COLOR.get(sev, "#666")
    return (
        f"<div class='delta-line'>"
        f"<span style='color:{sev_color};font-weight:700;'>{sev}</span> "
        f"{prefix}<b>{summary}</b>{suffix}"
        f" <span style='color:#666;font-size:11px;'>({me} · MFID-{mfid})</span>"
        f"</div>"
    )


def render_kpi_card(label, value, delta_text=None, delta_color=None, sub=None, val_color=None):
    delta_html = f'<div class="delta" style="color:{delta_color};">{delta_text}</div>' if delta_text else ''
    sub_html = f'<div class="sub">{sub}</div>' if sub else ''
    val_style = f'style="color:{val_color};"' if val_color else ''
    return f"""
    <div class="metric-box">
      <div class="label">{label}</div>
      <div class="val" {val_style}>{value}</div>
      {delta_html}
      {sub_html}
    </div>
    """


# ============================================================
# TABS
# ============================================================

tab_floor, tab_next, tab_up, tab_search, tab_export, tab_analytics = st.tabs([
    "🏭 In Layup",
    "📋 Ready to Layup",
    "⏰ Upstream",
    "🔍 Search",
    "📄 Export",
    "📊 Analytics",
])

# ---------- IN LAYUP ----------
with tab_floor:
    st.subheader("In Layup")
    st.caption("Parts in **Layup** — what to check during your walk")
    on_floor = filter_and_sort(scored, ["Layup"])
    flagged = on_floor[on_floor["severity"] != "CLEAN"]
    st.write(f"**{len(on_floor)}** parts in layup — **{len(flagged)}** need attention")
    if flagged.empty:
        st.success("✅ No flagged parts in layup right now.")
    else:
        for _, row in flagged.iterrows():
            render_alert(row)


# ---------- READY TO LAYUP ----------
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


# ---------- UPSTREAM ----------
with tab_up:
    st.subheader("Upstream")
    st.caption("Material Cutting + Scheduled — flag before they arrive")
    upstream = filter_and_sort(scored, ["Material Cutting", "Scheduled"])
    flagged = upstream[upstream["severity"].isin(["RED", "ORANGE"])]
    st.write(f"**{len(upstream)}** parts upstream — showing **{len(flagged)}** RED/ORANGE")
    if flagged.empty:
        st.info("Nothing critical upstream right now.")
    else:
        for _, row in flagged.iterrows():
            render_alert(row, show_history=False)


# ---------- SEARCH ----------
with tab_search:
    st.subheader("Search any part")
    q = st.text_input(
        "Part name, part number, ME-ID, MFID, or defect — partial match",
        placeholder="e.g., wing skin   |   213565   |   ME-45578   |   MFID-0014992   |   wrinkle",
    )
    if q:
        ql = q.lower().strip()
        mask = (
            scored["summary"].fillna("").str.lower().str.contains(ql, regex=False) |
            scored["part_number"].fillna("").str.lower().str.contains(ql, regex=False) |
            scored["issue_id"].fillna("").str.lower().str.contains(ql, regex=False) |
            scored["order_number"].fillna("").str.lower().str.contains(ql, regex=False)
        )
        hits = scored.loc[mask].sort_values(["sev_rank", "stage_rank"])
        st.write(f"**{len(hits)}** pipeline match(es)")
        for _, row in hits.head(40).iterrows():
            render_alert(row)
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
                    "created_str": "Date", "part_description": "Part",
                    "part_number": "PN", "clean_defect": "Defect",
                    "disposition_clean": "Disposition", "originating_area": "Area",
                })
                st.dataframe(show, hide_index=True, use_container_width=True)
    else:
        st.info("Start typing to search.")


# ---------- EXPORT ----------
with tab_export:
    st.subheader("Export")
    st.caption("Download the current snapshot as CSV or PDF")
    flat_cols = [
        "issue_id", "order_number", "part_number", "summary", "stage", "severity",
        "issue_count", "scrap_count", "rework_count", "pending_count", "wrinkle_count",
    ]
    csv_buf = scored[flat_cols].to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Download CSV", data=csv_buf,
        file_name=f"quality_watch_{now_pt.strftime('%Y-%m-%d_%H%M')}.csv",
        mime="text/csv", use_container_width=True,
    )
    if st.button("📄 Generate PDF report", use_container_width=True, type="primary"):
        with st.spinner("Building PDF…"):
            from pdf_export import build_pdf
            pdf_bytes = build_pdf(scored, now_pt)
        shift = _shift_for(now_pt)
        fname = f"Kanban_Quality_Watch_{now_pt.strftime('%-m-%-d-%y')}_{shift}.pdf"
        st.download_button(
            "⬇️ Download PDF", data=pdf_bytes,
            file_name=fname, mime="application/pdf", use_container_width=True,
        )
        st.success(f"PDF ready: **{fname}**")


# ============================================================
# ANALYTICS TAB
# ============================================================
with tab_analytics:
    st.subheader("📊 Quality & Productivity Analytics")
    st.caption("Trend, breakdowns, KPIs, and shift-over-shift changes — all filterable.")

    # ---- Filter bar ----
    fcol1, fcol2, fcol3 = st.columns([2, 2, 2])
    with fcol1:
        date_range = st.radio(
            "Date range",
            ["7d", "30d", "90d", "6mo"],
            horizontal=True, index=1, key="date_range",
        )
    with fcol2:
        all_areas = sorted(quality_df["originating_area"].dropna().unique().tolist()) if not quality_df.empty else []
        area_filter = st.multiselect(
            "Originating area",
            all_areas, default=all_areas, key="area_filter",
        )
    with fcol3:
        all_disps = sorted(quality_df["disposition_clean"].dropna().unique().tolist()) if not quality_df.empty else []
        disp_filter = st.multiselect(
            "Disposition",
            all_disps, default=all_disps, key="disp_filter",
        )

    window_days = {"7d": 7, "30d": 30, "90d": 90, "6mo": 180}[date_range]
    period_label = {"7d": "last 7 days", "30d": "last 30 days",
                    "90d": "last 90 days", "6mo": "last 6 months"}[date_range]

    # Apply filters
    q_filtered = quality_df.copy()
    if area_filter:
        q_filtered = q_filtered[q_filtered["originating_area"].isin(area_filter)]
    if disp_filter:
        q_filtered = q_filtered[q_filtered["disposition_clean"].isin(disp_filter)]

    q_curr = filter_quality_by_window(q_filtered, window_days)
    today = pd.Timestamp.now().normalize()
    prior_start = today - pd.Timedelta(days=2 * window_days)
    prior_end   = today - pd.Timedelta(days=window_days)
    q_prior = q_filtered[(q_filtered["created_dt"] >= prior_start) &
                         (q_filtered["created_dt"] < prior_end)].copy()

    kpi_curr = compute_kpis(q_curr)
    kpi_prior = compute_kpis(q_prior)

    # ============================================================
    # HEADLINE KPIs
    # ============================================================
    st.markdown('<div class="section-header">QUALITY KPIs · '
                f'{period_label.upper()} vs prior {period_label}</div>',
                unsafe_allow_html=True)

    issues_d, issues_c = fmt_delta(kpi_curr["issues"], kpi_prior["issues"])
    scrap_d,  scrap_c  = fmt_delta(kpi_curr["scraps"], kpi_prior["scraps"])
    wrkl_d,   wrkl_c   = fmt_delta(kpi_curr["wrinkles"], kpi_prior["wrinkles"])
    rework_d, rework_c = fmt_delta(kpi_curr["rework"], kpi_prior["rework"])
    pend_d,   pend_c   = fmt_delta(kpi_curr["pending"], kpi_prior["pending"])
    rate_d,   rate_c   = fmt_delta(kpi_curr["scrap_rate"], kpi_prior["scrap_rate"])

    st.markdown(f"""
    <div class="metric-row">
      {render_kpi_card("Total issues", kpi_curr["issues"], issues_d, issues_c, f"prior: {kpi_prior['issues']}")}
      {render_kpi_card("Scraps", kpi_curr["scraps"], scrap_d, scrap_c, f"prior: {kpi_prior['scraps']}", "#c0392b")}
      {render_kpi_card("Wrinkles", kpi_curr["wrinkles"], wrkl_d, wrkl_c, f"prior: {kpi_prior['wrinkles']}", "#d4730b")}
      {render_kpi_card("Rework", kpi_curr["rework"], rework_d, rework_c, f"prior: {kpi_prior['rework']}", "#d4920b")}
      {render_kpi_card("Pending", kpi_curr["pending"], pend_d, pend_c, f"prior: {kpi_prior['pending']}", "#7C2D12")}
      {render_kpi_card("Scrap rate", f"{kpi_curr['scrap_rate']:.0f}%", rate_d, rate_c, f"prior: {kpi_prior['scrap_rate']:.0f}%")}
    </div>
    """, unsafe_allow_html=True)

    # ============================================================
    # PRODUCTIVITY KPIs
    # ============================================================
    st.markdown('<div class="section-header">PRODUCTIVITY KPIs · pipeline state right now</div>',
                unsafe_allow_html=True)

    in_layup_n  = int((scored["stage"] == "Layup").sum())
    in_ready_n  = int((scored["stage"] == "Ready to Layup").sum())
    in_mc_n     = int((scored["stage"] == "Material Cutting").sum())
    in_sched_n  = int((scored["stage"] == "Scheduled").sum())

    # Avg dwell time in Ready to Layup (using created_at as proxy)
    ready_parts = pipeline_df[pipeline_df["stage"] == "Ready to Layup"].copy()
    if not ready_parts.empty and "updated_at" in ready_parts.columns:
        ready_parts["age_days"] = (pd.Timestamp.now() - pd.to_datetime(ready_parts["updated_at"])).dt.days
        avg_dwell = int(ready_parts["age_days"].median()) if not ready_parts.empty else 0
        max_dwell = int(ready_parts["age_days"].max()) if not ready_parts.empty else 0
    else:
        avg_dwell = 0
        max_dwell = 0

    flagged_pct = int(round(100 * (red + orange + yellow) / max(1, len(scored))))

    st.markdown(f"""
    <div class="metric-row">
      {render_kpi_card("Active pipeline", len(scored), sub="all stages")}
      {render_kpi_card("In Layup", in_layup_n, sub="on the table")}
      {render_kpi_card("Ready to Layup", in_ready_n, sub="queued")}
      {render_kpi_card("Material Cutting", in_mc_n, sub="kit prep")}
      {render_kpi_card("Median age in Ready", f"{avg_dwell}d", sub=f"max: {max_dwell}d")}
      {render_kpi_card("Flagged %", f"{flagged_pct}%", sub=f"{red+orange+yellow} of {len(scored)}")}
    </div>
    """, unsafe_allow_html=True)

    # ============================================================
    # WRINKLE ARTS GOAL TRACKER
    # ============================================================
    st.markdown('<div class="section-header">WRINKLE ARTS GOAL · 50% reduction target</div>',
                unsafe_allow_html=True)

    # Use 12-week wrinkle window. First 4 weeks = baseline, last 4 weeks = current.
    wrinkles_only = q_filtered[q_filtered["is_wrinkle"] == 1].copy()
    if not wrinkles_only.empty:
        wrinkles_only["week"] = wrinkles_only["created_dt"].dt.to_period("W").dt.start_time
        weekly = wrinkles_only.groupby("week").size().reset_index(name="count")
        weekly = weekly.sort_values("week").tail(12)

        if len(weekly) >= 8:
            baseline_wk = weekly.head(4)["count"].sum()
            current_wk  = weekly.tail(4)["count"].sum()
            target      = baseline_wk * 0.5
            pct_to_goal = max(0, min(100, 100 * (baseline_wk - current_wk) / max(1, baseline_wk - target)))
            on_pace = current_wk <= target
            status_text = "✅ ON PACE" if on_pace else ("⚠️ OFF PACE" if current_wk < baseline_wk else "🔴 GETTING WORSE")
            status_class = "on-pace" if on_pace else "off-pace"
            fill_class = "" if on_pace else ("warn" if current_wk < baseline_wk else "bad")
            change_pct = 100 * (current_wk - baseline_wk) / max(1, baseline_wk)
            st.markdown(f"""
            <div class="goal-card {status_class}">
              <h3>{status_text}</h3>
              <div style="display:flex;justify-content:space-between;font-size:13px;color:#444;margin-bottom:6px;">
                <span><b>Baseline (first 4 weeks):</b> {baseline_wk} wrinkles</span>
                <span><b>Last 4 weeks:</b> {current_wk} wrinkles ({change_pct:+.0f}%)</span>
                <span><b>Target:</b> ≤ {int(target)}</span>
              </div>
              <div class="progress">
                <div class="progress-fill {fill_class}" style="width:{pct_to_goal:.0f}%;"></div>
              </div>
              <div style="font-size:11px;color:#666;text-align:right;">{pct_to_goal:.0f}% of the way to goal</div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.info("Not enough wrinkle history yet for ARTS goal tracking (need 8+ weeks).")
    else:
        st.info("No wrinkle data in selected window.")

    # ============================================================
    # WEEKLY TREND CHART
    # ============================================================
    st.markdown('<div class="section-header">WEEKLY ISSUE & SCRAP TREND · last 12 weeks</div>',
                unsafe_allow_html=True)

    if not q_filtered.empty:
        weekly_full = q_filtered.copy()
        weekly_full["week"] = weekly_full["created_dt"].dt.to_period("W").dt.start_time
        weekly_agg = weekly_full.groupby("week").agg(
            issues=("issue_id", "count"),
            scraps=("disposition", lambda s: int((s == "Scrap").sum())),
            wrinkles=("is_wrinkle", "sum"),
        ).reset_index().tail(12)

        if HAS_PLOTLY and not weekly_agg.empty:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=weekly_agg["week"], y=weekly_agg["issues"],
                name="Total issues", marker_color="#94a3b8",
                hovertemplate="%{x|%b %d}<br>%{y} issues<extra></extra>",
            ))
            fig.add_trace(go.Bar(
                x=weekly_agg["week"], y=weekly_agg["scraps"],
                name="Scraps", marker_color="#c0392b",
                hovertemplate="%{x|%b %d}<br>%{y} scraps<extra></extra>",
            ))
            fig.add_trace(go.Scatter(
                x=weekly_agg["week"], y=weekly_agg["wrinkles"],
                name="Wrinkles", mode="lines+markers",
                line=dict(color="#d4730b", width=3),
                hovertemplate="%{x|%b %d}<br>%{y} wrinkles<extra></extra>",
            ))
            fig.update_layout(
                barmode="overlay", height=320,
                margin=dict(l=20, r=20, t=10, b=20),
                legend=dict(orientation="h", y=1.05, x=0),
                hovermode="x unified",
                xaxis_title=None, yaxis_title="Count",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            chart_data = weekly_agg.set_index("week")[["issues", "scraps", "wrinkles"]]
            st.bar_chart(chart_data, height=260)

    # ============================================================
    # DAILY ACTIVITY HEATMAP-ish (last 30 days)
    # ============================================================
    st.markdown('<div class="section-header">DAILY ACTIVITY · last 30 days</div>',
                unsafe_allow_html=True)

    daily_window = filter_quality_by_window(q_filtered, 30)
    if not daily_window.empty:
        daily = daily_window.groupby("created_date").agg(
            issues=("issue_id", "count"),
            scraps=("disposition", lambda s: int((s == "Scrap").sum())),
            wrinkles=("is_wrinkle", "sum"),
        ).reset_index().sort_values("created_date")

        if HAS_PLOTLY:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=daily["created_date"], y=daily["issues"],
                name="Issues", marker_color="#2471a3",
                hovertemplate="%{x|%b %d}<br>%{y} issues<extra></extra>",
            ))
            fig.add_trace(go.Bar(
                x=daily["created_date"], y=daily["scraps"],
                name="Scraps", marker_color="#c0392b",
                hovertemplate="%{x|%b %d}<br>%{y} scraps<extra></extra>",
            ))
            fig.update_layout(
                barmode="group", height=260,
                margin=dict(l=20, r=20, t=10, b=20),
                legend=dict(orientation="h", y=1.05, x=0),
                xaxis_title=None, yaxis_title=None,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.bar_chart(daily.set_index("created_date")[["issues", "scraps"]], height=240)

        # Spike days callout
        if not daily.empty:
            spike = daily.loc[daily["issues"].idxmax()]
            st.caption(
                f"Worst day in window: **{spike['created_date']}** with "
                f"**{int(spike['issues'])} issues** ({int(spike['scraps'])} scraps, "
                f"{int(spike['wrinkles'])} wrinkles)"
            )

    # ============================================================
    # DEFECT BREAKDOWN + DETECTION POINTS (side by side)
    # ============================================================
    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown('<div class="section-header">DEFECT DRIVERS</div>', unsafe_allow_html=True)
        if not q_curr.empty:
            q_with_cat = q_curr.copy()
            q_with_cat["category"] = q_with_cat["defect_code"].apply(categorize_defect)
            cat_agg = q_with_cat[q_with_cat["category"] != "Other"].groupby("category").agg(
                total=("issue_id", "count"),
                scraps=("disposition", lambda s: int((s == "Scrap").sum())),
            ).reset_index().sort_values("total", ascending=True).tail(10)

            if HAS_PLOTLY and not cat_agg.empty:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    y=cat_agg["category"], x=cat_agg["total"],
                    orientation="h", name="Total",
                    marker_color="#94a3b8",
                    hovertemplate="%{y}<br>%{x} total<extra></extra>",
                ))
                fig.add_trace(go.Bar(
                    y=cat_agg["category"], x=cat_agg["scraps"],
                    orientation="h", name="Scraps",
                    marker_color="#c0392b",
                    hovertemplate="%{y}<br>%{x} scraps<extra></extra>",
                ))
                fig.update_layout(
                    barmode="overlay", height=300,
                    margin=dict(l=20, r=20, t=10, b=20),
                    legend=dict(orientation="h", y=1.05, x=0),
                    xaxis_title=None, yaxis_title=None,
                )
                st.plotly_chart(fig, use_container_width=True)
            elif not cat_agg.empty:
                st.bar_chart(cat_agg.set_index("category")[["total", "scraps"]], height=300)
        else:
            st.info("No data in selected window.")

    with col_r:
        st.markdown('<div class="section-header">DETECTION POINTS</div>', unsafe_allow_html=True)
        if not q_curr.empty:
            det = q_curr.groupby("originating_area").agg(
                total=("issue_id", "count"),
                scraps=("disposition", lambda s: int((s == "Scrap").sum())),
            ).reset_index().sort_values("total", ascending=True)
            det["area_short"] = det["originating_area"].str.replace(
                "Manufacturing Engineering - Composites Fabrication", "ME-Comp Fab"
            )

            if HAS_PLOTLY and not det.empty:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    y=det["area_short"], x=det["total"],
                    orientation="h", name="Total",
                    marker_color="#2471a3",
                    hovertemplate="%{y}<br>%{x} issues<extra></extra>",
                ))
                fig.add_trace(go.Bar(
                    y=det["area_short"], x=det["scraps"],
                    orientation="h", name="Scraps",
                    marker_color="#c0392b",
                    hovertemplate="%{y}<br>%{x} scraps<extra></extra>",
                ))
                fig.update_layout(
                    barmode="overlay", height=300,
                    margin=dict(l=20, r=20, t=10, b=20),
                    legend=dict(orientation="h", y=1.05, x=0),
                    xaxis_title=None, yaxis_title=None,
                )
                st.plotly_chart(fig, use_container_width=True)
            elif not det.empty:
                st.bar_chart(det.set_index("area_short")[["total", "scraps"]], height=300)
        else:
            st.info("No data in selected window.")

    # ============================================================
    # DAY-OF-WEEK PATTERN (wrinkles)
    # ============================================================
    st.markdown('<div class="section-header">WRINKLE DAY-OF-WEEK PATTERN · last 90 days</div>',
                unsafe_allow_html=True)

    dow_window = filter_quality_by_window(q_filtered, 90)
    wnk_dow = dow_window[dow_window["is_wrinkle"] == 1].copy()
    if not wnk_dow.empty:
        dow_agg = wnk_dow.groupby(["dow_n", "dow"]).size().reset_index(name="count")
        dow_agg = dow_agg.sort_values("dow_n")
        if HAS_PLOTLY and not dow_agg.empty:
            fig = px.bar(
                dow_agg, x="dow", y="count",
                color="count", color_continuous_scale="Reds",
                labels={"dow": "Day", "count": "Wrinkles"},
            )
            fig.update_layout(
                height=240, margin=dict(l=20, r=20, t=10, b=20),
                showlegend=False, coloraxis_showscale=False,
            )
            st.plotly_chart(fig, use_container_width=True)
        elif not dow_agg.empty:
            st.bar_chart(dow_agg.set_index("dow")[["count"]], height=220)

        peak_idx = dow_agg["count"].idxmax()
        peak_day = dow_agg.loc[peak_idx, "dow"]
        peak_count = int(dow_agg.loc[peak_idx, "count"])
        wkdays = dow_agg[dow_agg["dow_n"] < 5]["count"].sum()
        wknds = dow_agg[dow_agg["dow_n"] >= 5]["count"].sum()
        total = int(wkdays + wknds)
        wkday_pct = int(round(100 * wkdays / max(1, total)))
        st.caption(
            f"Peak day: **{peak_day}** ({peak_count} wrinkles)  ·  "
            f"Weekday share: **{wkday_pct}%**  ·  "
            f"Total: **{total}** wrinkles, 90 days"
        )
    else:
        st.info("No wrinkle data in last 90 days.")

    # ============================================================
    # TOP 10 WORST PARTS (in pipeline)
    # ============================================================
    st.markdown('<div class="section-header">TOP 10 WORST PARTS IN PIPELINE</div>',
                unsafe_allow_html=True)

    top10 = scored.copy()
    top10["score_v"] = top10["scrap_count"] * 10 + top10["wrinkle_count"] * 3 + top10["issue_count"]
    top10 = top10[top10["score_v"] > 0].sort_values("score_v", ascending=False).head(10)

    if not top10.empty:
        show = top10[[
            "summary", "issue_id", "order_number", "stage", "severity",
            "issue_count", "scrap_count", "wrinkle_count", "score_v",
        ]].rename(columns={
            "summary": "Part",
            "issue_id": "ME",
            "order_number": "MFID",
            "stage": "Stage",
            "severity": "Sev",
            "issue_count": "Iss",
            "scrap_count": "Scr",
            "wrinkle_count": "Wrk",
            "score_v": "Score",
        })
        st.dataframe(show, hide_index=True, use_container_width=True)
    else:
        st.success("No parts in pipeline have any quality history.")

    # ============================================================
    # PIPELINE COMPOSITION HEATMAP
    # ============================================================
    st.markdown('<div class="section-header">PIPELINE COMPOSITION · severity × stage</div>',
                unsafe_allow_html=True)

    pivot = scored.groupby(["severity", "stage"]).size().unstack(fill_value=0)
    stage_order = ["Scheduled", "Material Cutting", "Ready to Layup", "Layup"]
    pivot = pivot.reindex(columns=[s for s in stage_order if s in pivot.columns], fill_value=0)
    sev_order = [s for s in ["RED", "ORANGE", "YELLOW", "CLEAN"] if s in pivot.index]
    pivot = pivot.reindex(index=sev_order, fill_value=0)

    if HAS_PLOTLY and not pivot.empty:
        fig = px.imshow(
            pivot.values,
            x=pivot.columns.tolist(),
            y=pivot.index.tolist(),
            color_continuous_scale=[[0, "#f4f4f4"], [1, "#c0392b"]],
            text_auto=True, aspect="auto",
        )
        fig.update_layout(
            height=240, margin=dict(l=20, r=20, t=10, b=20),
            coloraxis_showscale=False,
            xaxis_title=None, yaxis_title=None,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.dataframe(pivot, use_container_width=True)

    # ============================================================
    # WHAT CHANGED (delta view)
    # ============================================================
    st.markdown('<div class="section-header">WHAT CHANGED · since prior shift</div>',
                unsafe_allow_html=True)

    prior = load_prior_snapshot(now_pt)
    if prior.empty:
        st.info(
            "No prior snapshot found yet. The first run kicks off the history — "
            "by your next shift, this will show what changed since now."
        )
        st.caption(st.session_state.get("snapshot_msg", ""))
    else:
        prior_ts = pd.to_datetime(prior["snapshot_ts"]).max()
        prior_shift_lbl = prior.iloc[0].get("shift", "")
        prior_date = prior.iloc[0].get("snapshot_date", "")
        deltas = compute_deltas(scored, prior)
        n_new   = len(deltas["new_in_pipeline"])
        n_gone  = len(deltas["gone_from_pipeline"])
        n_stage = len(deltas["stage_changed"])
        n_scrap = len(deltas["new_scrap"])
        n_wnk   = len(deltas["new_wrinkle"])
        n_up    = len(deltas["severity_up"])
        n_down  = len(deltas["severity_down"])
        total_changes = n_new + n_gone + n_stage + n_scrap + n_wnk + n_up + n_down

        st.markdown(f"""
        <div class="delta-summary">
          <h3>Comparing to: {prior_date} {prior_shift_lbl} ({prior_ts.strftime('%-I:%M %p UTC')})</h3>
          <div class="sub">{total_changes} change{'s' if total_changes != 1 else ''} since the prior shift</div>
          <div class="stats">
            🆕 {n_new} new  ·  🚪 {n_gone} gone  ·  ➡️ {n_stage} stage moves  ·
            🔴 {n_scrap} new scrap  ·  〰️ {n_wnk} new wrinkle  ·
            ⬆️ {n_up} severity up  ·  ⬇️ {n_down} severity down
          </div>
        </div>
        """, unsafe_allow_html=True)

        if total_changes == 0:
            st.success("No changes since the prior shift.")

        for key, css_class, title in [
            ("new_scrap",   "warn", "🔴 New scrap events"),
            ("new_wrinkle", "warn", "〰️ New wrinkle events"),
            ("severity_up", "warn", "⬆️ Severity escalated"),
            ("new_in_pipeline", "move", "🆕 New parts in pipeline"),
            ("stage_changed",   "move", "➡️ Stage changes"),
            ("gone_from_pipeline", "move", "🚪 Gone from pipeline"),
            ("severity_down", "good", "⬇️ Severity improved"),
        ]:
            items = deltas.get(key, [])
            if not items:
                continue
            html = f"<div class='delta-section {css_class}'><h4>{title}</h4>"
            for d in items:
                if isinstance(d, dict) and "row" in d:
                    suffix = ""
                    if key in ("new_scrap", "new_wrinkle"):
                        suffix = f" — <b>{d['from']} → {d['to']}</b>"
                    elif key in ("severity_up", "severity_down", "stage_changed"):
                        suffix = f" — <b>{d['from']} → {d['to']}</b>"
                    html += render_delta_line(d["row"], suffix=suffix)
                else:
                    suffix = f" — at <b>{d.get('stage','')}</b>"
                    html += render_delta_line(d, suffix=suffix)
            html += "</div>"
            st.markdown(html, unsafe_allow_html=True)

    st.divider()
    st.caption(st.session_state.get("snapshot_msg", "(no snapshot status)"))    "CLEAN":  "#1D9E75",
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


def _run_statement(sql: str):
    """Execute a non-SELECT statement (INSERT/DELETE/etc)."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


PIPELINE_SQL = """
SELECT
    issue_id,
    order_number,
    part_number,
    summary,
    components,
    status,
    priority,
    due_date,
    created_at
FROM manufacturing.jira.issues
WHERE project_name = 'ME'
  AND issue_type != 'Epic'
  AND components = 'Composite Part'
  AND status IN ('Scheduled', 'Kit', 'Ready to Laminate', 'Laminate')
  AND upper(coalesce(summary, '')) NOT LIKE '%BATTERY%'
  AND upper(coalesce(summary, '')) NOT LIKE '%BUSBAR%'
  AND upper(coalesce(summary, '')) NOT LIKE '%SLEEVE, 5-PLY%'
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
    link_to_issue,
    CASE
      WHEN lower(coalesce(defect_code, '')) LIKE '%wnk%'
        OR lower(coalesce(issue_title, '')) LIKE '%wrinkle%'
        OR lower(coalesce(issue_title, '')) LIKE '%winkel%'
      THEN 1 ELSE 0
    END AS is_wrinkle
FROM manufacturing.onion_silver.quality_issues_view
WHERE CAST(created AS DATE) >= CURRENT_DATE() - INTERVAL {LOOKBACK_DAYS} DAYS
  AND issue_status != 'deleted'
  AND originating_area IN ({COMP_SHOP_AREAS_SQL})
  AND upper(coalesce(part_description, '')) NOT LIKE '%BATTERY%'
  AND upper(coalesce(part_description, '')) NOT LIKE '%BUSBAR%'
  AND upper(coalesce(part_description, '')) NOT LIKE '%SLEEVE, 5-PLY%'
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
# SNAPSHOT — write current pipeline state, read prior shift
# ============================================================

def _esc(s):
    """Single-quote escape for SQL literals."""
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


def _shift_for(now_pt: datetime) -> str:
    """AM = before 5:30 PM PT, PM = 5:30 PM and after."""
    return "AM" if (now_pt.hour, now_pt.minute) < (17, 30) else "PM"


def write_snapshot_if_due(scored: pd.DataFrame, now_pt: datetime) -> str:
    """
    Append a snapshot row per pipeline part to SNAPSHOT_TABLE,
    but only if the last snapshot was more than SNAPSHOT_MIN_GAP_MINUTES
    ago OR was for a different shift.
    """
    if scored.empty:
        return "No pipeline data — no snapshot written."

    shift = _shift_for(now_pt)
    snap_date_str = now_pt.strftime("%Y-%m-%d")

    try:
        last = _run_query(f"""
            SELECT MAX(snapshot_ts) AS last_ts, MAX(shift) AS last_shift
            FROM {SNAPSHOT_TABLE}
            WHERE snapshot_date = DATE '{snap_date_str}'
              AND shift = '{shift}'
        """)
        if not last.empty and last.iloc[0]["last_ts"] is not None:
            last_ts = pd.to_datetime(last.iloc[0]["last_ts"])
            last_ts_pt = last_ts.tz_localize("UTC").tz_convert("America/Los_Angeles").tz_localize(None) \
                if last_ts.tzinfo is None else last_ts.tz_convert("America/Los_Angeles").tz_localize(None)
            now_naive = now_pt.replace(tzinfo=None)
            elapsed_min = (now_naive - last_ts_pt).total_seconds() / 60.0
            if 0 <= elapsed_min < SNAPSHOT_MIN_GAP_MINUTES:
                return f"Snapshot already taken {int(elapsed_min)} min ago. Skipping."
    except Exception:
        pass

    ts_str = now_pt.strftime("%Y-%m-%d %H:%M:%S")
    rows_sql = []
    for _, r in scored.iterrows():
        rows_sql.append(
            "("
            f"TIMESTAMP '{ts_str}', DATE '{snap_date_str}', '{shift}',"
            f"{_esc(r.get('issue_id'))},"
            f"{_esc(r.get('order_number'))},"
            f"{_esc(r.get('part_number'))},"
            f"{_esc(r.get('pn_norm'))},"
            f"{_esc((r.get('summary') or '')[:200])},"
            f"{_esc(r.get('status'))},"
            f"{_esc(r.get('stage'))},"
            f"{_esc(r.get('severity'))},"
            f"{int(r.get('issue_count') or 0)},"
            f"{int(r.get('scrap_count') or 0)},"
            f"{int(r.get('wrinkle_count') or 0)},"
            f"{int(r.get('rework_count') or 0)},"
            f"{int(r.get('pending_count') or 0)}"
            ")"
        )

    CHUNK = 100
    inserted = 0
    for i in range(0, len(rows_sql), CHUNK):
        chunk = rows_sql[i:i+CHUNK]
        sql = f"INSERT INTO {SNAPSHOT_TABLE} VALUES " + ",".join(chunk)
        _run_statement(sql)
        inserted += len(chunk)

    return f"Snapshot saved — {inserted} parts at {ts_str} {shift}."


def load_prior_snapshot(now_pt: datetime) -> pd.DataFrame:
    """
    Get the most recent snapshot from BEFORE the current shift's start.
    AM run compares to anything before today.
    PM run compares to today AM (or earlier if no AM today).
    """
    shift = _shift_for(now_pt)
    today_str = now_pt.strftime("%Y-%m-%d")

    if shift == "AM":
        cutoff_clause = f"snapshot_date < DATE '{today_str}'"
    else:
        cutoff_clause = f"snapshot_date = DATE '{today_str}' AND shift = 'AM'"

    sql = f"""
    WITH ranked AS (
      SELECT *,
        ROW_NUMBER() OVER (PARTITION BY pn_norm ORDER BY snapshot_ts DESC) AS rn
      FROM {SNAPSHOT_TABLE}
      WHERE {cutoff_clause}
    )
    SELECT
      snapshot_ts, snapshot_date, shift,
      me_key, mfid, pn_raw, pn_norm, summary, status, stage, severity,
      total_issues, scraps, wrinkles, rework, pending
    FROM ranked
    WHERE rn = 1
    """
    try:
        return _run_query(sql)
    except Exception:
        return pd.DataFrame()


def compute_deltas(current: pd.DataFrame, prior: pd.DataFrame) -> dict:
    """Diff current vs prior. Returns categorized change lists."""
    out = {
        "new_in_pipeline":    [],
        "gone_from_pipeline": [],
        "stage_changed":      [],
        "new_scrap":          [],
        "new_wrinkle":        [],
        "severity_up":        [],
        "severity_down":      [],
    }
    if prior.empty:
        return out

    cur_by_me = {r["issue_id"]: r for _, r in current.iterrows()}
    pri_by_me = {r["me_key"]: r for _, r in prior.iterrows()}

    cur_keys = set(cur_by_me.keys())
    pri_keys = set(pri_by_me.keys())

    for k in cur_keys - pri_keys:
        out["new_in_pipeline"].append(cur_by_me[k])
    for k in pri_keys - cur_keys:
        out["gone_from_pipeline"].append(pri_by_me[k])

    for k in cur_keys & pri_keys:
        cur = cur_by_me[k]
        pri = pri_by_me[k]

        if (cur.get("stage") or "") != (pri.get("stage") or ""):
            out["stage_changed"].append({
                "row": cur, "from": pri.get("stage"), "to": cur.get("stage"),
            })

        cur_scr = int(cur.get("scrap_count") or 0)
        pri_scr = int(pri.get("scraps") or 0)
        if cur_scr > pri_scr:
            out["new_scrap"].append({"row": cur, "from": pri_scr, "to": cur_scr})

        cur_wnk = int(cur.get("wrinkle_count") or 0)
        pri_wnk = int(pri.get("wrinkles") or 0)
        if cur_wnk > pri_wnk:
            out["new_wrinkle"].append({"row": cur, "from": pri_wnk, "to": cur_wnk})

        cur_sev = SEV_RANK.get(cur.get("severity"), 99)
        pri_sev = SEV_RANK.get(pri.get("severity"), 99)
        if cur_sev < pri_sev:
            out["severity_up"].append({
                "row": cur, "from": pri.get("severity"), "to": cur.get("severity"),
            })
        elif cur_sev > pri_sev:
            out["severity_down"].append({
                "row": cur, "from": pri.get("severity"), "to": cur.get("severity"),
            })

    return out


# ============================================================
# HELPERS
# ============================================================

def normalize_pn(pn) -> str:
    if not pn or pn == "None":
        return ""
    s = str(pn).strip()
    s = re.sub(r"-X\d+.*$", "", s)
    s = re.sub(r"-S\d+$",   "", s)
    s = re.sub(r"-L\d+$",   "", s)
    s = re.sub(r"-TPDT$",   "", s)
    return s


def clean_defect_code(dc) -> str:
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


def classify_severity(scraps: int, wrinkles: int, total: int) -> str:
    if scraps >= 2 or wrinkles >= 3:
        return "RED"
    if scraps >= 1 or total >= 3 or wrinkles >= 2:
        return "ORANGE"
    if total >= 1:
        return "YELLOW"
    return "CLEAN"


def score_pipeline(pipeline: pd.DataFrame, quality: pd.DataFrame) -> pd.DataFrame:
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
        scraps   = sum(1 for i in h if i.get("disposition") == "Scrap")
        rework   = sum(1 for i in h if i.get("disposition") == "Rework")
        pending  = sum(1 for i in h if not i.get("disposition"))
        wrinkles = sum(1 for i in h if int(i.get("is_wrinkle") or 0) == 1)

        sev = classify_severity(scraps, wrinkles, len(h))

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
.metric-box .val { font-size: 24px; font-weight: 700; line-height: 1.1; margin-top: 2px; color: #1a1a1a; }
.val-red    { color: #c0392b; }
.val-orange { color: #d4730b; }
.val-yellow { color: #d4920b; }
.val-green  { color: #1D9E75; }
.alert-card {
  border-left: 4px solid; padding: 10px 14px; margin-bottom: 8px;
  background: #f9f9f9; border-radius: 4px;
}
.alert-card.red    { border-color: #c0392b; background: #fae4e1; }
.alert-card.orange { border-color: #d4730b; background: #fef0e0; }
.alert-card.yellow { border-color: #d4920b; background: #fef5e7; }
.alert-card .pn       { font-weight: 700; font-size: 13.5px; color: #1a1a1a; }
.alert-card .meta     { font-size: 11px; color: #555; margin-top: 3px; }
.alert-card .badge {
  display: inline-block; font-size: 10px; font-weight: 700;
  padding: 1px 6px; border-radius: 3px; color: white; margin-right: 6px;
}
.alert-card .badge.red    { background: #c0392b; }
.alert-card .badge.orange { background: #d4730b; }
.alert-card .badge.yellow { background: #d4920b; color: #1a1a1a; }
.history-line { font-size: 11px; color: #444; margin-top: 4px; padding-left: 8px; border-left: 2px solid #ccc; }
.history-scrap { color: #c0392b; font-weight: 600; }
.delta-section {
  background: #f9f9f9; border-radius: 6px; padding: 12px 16px;
  margin-bottom: 14px; border-left: 4px solid #2471a3;
}
.delta-section.warn  { border-left-color: #c0392b; background: #fae4e1; }
.delta-section.move  { border-left-color: #d4730b; background: #fef0e0; }
.delta-section.good  { border-left-color: #1D9E75; background: #e8f6f0; }
.delta-section h4 { margin: 0 0 8px 0; font-size: 14px; color: #1a1a1a; }
.delta-line { font-size: 12px; color: #1a1a1a; margin: 4px 0; }
.delta-line .arrow { color: #666; padding: 0 6px; }
.delta-summary {
  background: #1a2332; color: white; padding: 14px 20px; border-radius: 8px;
  margin-bottom: 16px;
}
.delta-summary h3 { margin: 0 0 6px 0; font-size: 16px; color: white; }
.delta-summary .sub { font-size: 12px; color: #94a3b8; }
.delta-summary .stats { font-size: 13px; margin-top: 8px; }
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

# Sidebar
with st.sidebar:
    st.subheader("Controls")
    if st.button("🔄 Refresh data", use_container_width=True, type="primary"):
        st.cache_data.clear()
        st.rerun()
    st.caption("Cached for 5 min. Hit refresh to re-pull from Databricks.")

    st.divider()
    st.caption("**Severity rules**")
    st.caption("🔴 RED — 2+ scraps OR 3+ wrinkles")
    st.caption("🟠 ORANGE — 1 scrap, 3+ issues, OR 2+ wrinkles")
    st.caption("🟡 YELLOW — 1+ issue, no scrap")
    st.caption("🟢 CLEAN — no quality history")

    st.divider()
    st.caption(f"Lookback: {LOOKBACK_DAYS} days")
    st.caption("Areas: 527 Lam, Kitting, Hand Trim, ME-Comp Fab")
    st.caption("Source: jira.issues + onion_silver.quality_issues_view")
    st.caption(f"Snapshots: {SNAPSHOT_TABLE}")
    st.caption("Shift hand-off: 5:30 PM PT")

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

scored = score_pipeline(pipeline_df, quality_df)

# Snapshot write — once per session, debounced
if "snapshot_written" not in st.session_state:
    try:
        msg = write_snapshot_if_due(scored, now_pt)
        st.session_state["snapshot_written"] = True
        st.session_state["snapshot_msg"] = msg
    except Exception as e:
        st.session_state["snapshot_msg"] = f"Snapshot write failed: {e}"

# Top metrics
sev_counts = scored["severity"].value_counts()
red    = int(sev_counts.get("RED", 0))
orange = int(sev_counts.get("ORANGE", 0))
yellow = int(sev_counts.get("YELLOW", 0))
clean  = int(sev_counts.get("CLEAN", 0))

st.markdown(f"""
<div class="metric-row">
  <div class="metric-box"><div class="label">Pipeline parts</div><div class="val">{len(scored)}</div></div>
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
    mfid = row.get("order_number") or "—"
    stage = row.get("stage") or ""

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
        {issue_id} · MFID-{mfid} · {pn} · <b>{stage}</b> · {counts}
        {f"<br/>Defects: {defect_str}" if defect_str else ""}
      </div>
      {history_html}
    </div>
    """, unsafe_allow_html=True)


def filter_and_sort(df, stages):
    sub = df[df["stage"].isin(stages)].copy()
    sub = sub.sort_values(
        ["sev_rank", "scrap_count", "wrinkle_count", "issue_count"],
        ascending=[True, False, False, False],
    )
    return sub


def render_delta_line(row, prefix="", suffix=""):
    summary = (row.get("summary") or "")[:80]
    me = row.get("issue_id") or row.get("me_key") or ""
    mfid = row.get("order_number") or row.get("mfid") or "—"
    sev = row.get("severity") or ""
    sev_color = SEV_COLOR.get(sev, "#666")
    return (
        f"<div class='delta-line'>"
        f"<span style='color:{sev_color};font-weight:700;'>{sev}</span> "
        f"{prefix}<b>{summary}</b>{suffix}"
        f" <span style='color:#666;font-size:11px;'>({me} · MFID-{mfid})</span>"
        f"</div>"
    )


# ============================================================
# TABS — In Layup, Ready to Layup, Upstream, Search, Export, What Changed
# ============================================================

tab_floor, tab_next, tab_up, tab_search, tab_export, tab_delta = st.tabs([
    "🏭 In Layup",
    "📋 Ready to Layup",
    "⏰ Upstream",
    "🔍 Search",
    "📄 Export",
    "📊 What Changed",
])

with tab_floor:
    st.subheader("In Layup")
    st.caption("Parts in **Layup** — what to check during your walk")
    on_floor = filter_and_sort(scored, ["Layup"])
    flagged = on_floor[on_floor["severity"] != "CLEAN"]
    st.write(f"**{len(on_floor)}** parts in layup — **{len(flagged)}** need attention")
    if flagged.empty:
        st.success("✅ No flagged parts in layup right now.")
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
    st.caption("Material Cutting + Scheduled — flag before they arrive")
    upstream = filter_and_sort(scored, ["Material Cutting", "Scheduled"])
    flagged = upstream[upstream["severity"].isin(["RED", "ORANGE"])]
    st.write(f"**{len(upstream)}** parts upstream — showing **{len(flagged)}** RED/ORANGE")
    if flagged.empty:
        st.info("Nothing critical upstream right now.")
    else:
        for _, row in flagged.iterrows():
            render_alert(row, show_history=False)


with tab_search:
    st.subheader("Search any part")
    q = st.text_input(
        "Part name, part number, ME-ID, MFID, or defect — partial match",
        placeholder="e.g., wing skin   |   213565   |   ME-45578   |   MFID-0014992   |   wrinkle",
    )
    if q:
        ql = q.lower().strip()

        mask = (
            scored["summary"].fillna("").str.lower().str.contains(ql, regex=False) |
            scored["part_number"].fillna("").str.lower().str.contains(ql, regex=False) |
            scored["issue_id"].fillna("").str.lower().str.contains(ql, regex=False) |
            scored["order_number"].fillna("").str.lower().str.contains(ql, regex=False)
        )
        hits = scored.loc[mask].sort_values(["sev_rank", "stage_rank"])
        st.write(f"**{len(hits)}** pipeline match(es)")
        for _, row in hits.head(40).iterrows():
            render_alert(row)

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

    flat_cols = [
        "issue_id", "order_number", "part_number", "summary", "stage", "severity",
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

    if st.button("📄 Generate PDF report", use_container_width=True, type="primary"):
        with st.spinner("Building PDF…"):
            from pdf_export import build_pdf
            pdf_bytes = build_pdf(scored, now_pt)
        shift = _shift_for(now_pt)
        fname = f"Kanban_Quality_Watch_{now_pt.strftime('%-m-%-d-%y')}_{shift}.pdf"
        st.download_button(
            "⬇️ Download PDF",
            data=pdf_bytes,
            file_name=fname,
            mime="application/pdf",
            use_container_width=True,
        )
        st.success(f"PDF ready: **{fname}**")


with tab_delta:
    st.subheader("What changed since the prior shift")

    prior = load_prior_snapshot(now_pt)
    if prior.empty:
        st.info(
            "No prior snapshot found yet. The first run kicks off the history — "
            "by your next shift, this tab will show what changed since now."
        )
        st.caption(st.session_state.get("snapshot_msg", ""))
    else:
        prior_ts = pd.to_datetime(prior["snapshot_ts"]).max()
        prior_shift = prior.iloc[0].get("shift", "")
        prior_date = prior.iloc[0].get("snapshot_date", "")
        deltas = compute_deltas(scored, prior)

        n_new   = len(deltas["new_in_pipeline"])
        n_gone  = len(deltas["gone_from_pipeline"])
        n_stage = len(deltas["stage_changed"])
        n_scrap = len(deltas["new_scrap"])
        n_wnk   = len(deltas["new_wrinkle"])
        n_up    = len(deltas["severity_up"])
        n_down  = len(deltas["severity_down"])
        total_changes = n_new + n_gone + n_stage + n_scrap + n_wnk + n_up + n_down

        st.markdown(f"""
        <div class="delta-summary">
          <h3>Comparing to: {prior_date} {prior_shift} ({prior_ts.strftime('%-I:%M %p UTC')})</h3>
          <div class="sub">{total_changes} change{'s' if total_changes != 1 else ''} since the prior shift</div>
          <div class="stats">
            🆕 {n_new} new  ·  🚪 {n_gone} gone  ·  ➡️ {n_stage} stage moves  ·
            🔴 {n_scrap} new scrap  ·  〰️ {n_wnk} new wrinkle  ·
            ⬆️ {n_up} severity up  ·  ⬇️ {n_down} severity down
          </div>
        </div>
        """, unsafe_allow_html=True)

        if total_changes == 0:
            st.success("No changes since the prior shift. Pipeline state is unchanged.")

        if deltas["new_scrap"]:
            html = "<div class='delta-section warn'><h4>🔴 New scrap events</h4>"
            for d in deltas["new_scrap"]:
                html += render_delta_line(d["row"],
                    suffix=f" — scraps <b>{d['from']} → {d['to']}</b>")
            html += "</div>"
            st.markdown(html, unsafe_allow_html=True)

        if deltas["new_wrinkle"]:
            html = "<div class='delta-section warn'><h4>〰️ New wrinkle events</h4>"
            for d in deltas["new_wrinkle"]:
                html += render_delta_line(d["row"],
                    suffix=f" — wrinkles <b>{d['from']} → {d['to']}</b>")
            html += "</div>"
            st.markdown(html, unsafe_allow_html=True)

        if deltas["severity_up"]:
            html = "<div class='delta-section warn'><h4>⬆️ Severity escalated</h4>"
            for d in deltas["severity_up"]:
                html += render_delta_line(d["row"],
                    suffix=f" — <b>{d['from']} → {d['to']}</b>")
            html += "</div>"
            st.markdown(html, unsafe_allow_html=True)

        if deltas["new_in_pipeline"]:
            html = "<div class='delta-section move'><h4>🆕 New parts in pipeline</h4>"
            for r in deltas["new_in_pipeline"]:
                html += render_delta_line(r,
                    suffix=f" — entered at <b>{r.get('stage','')}</b>")
            html += "</div>"
            st.markdown(html, unsafe_allow_html=True)

        if deltas["stage_changed"]:
            html = "<div class='delta-section move'><h4>➡️ Stage changes</h4>"
            for d in deltas["stage_changed"]:
                html += render_delta_line(d["row"],
                    suffix=f" — <b>{d['from']} → {d['to']}</b>")
            html += "</div>"
            st.markdown(html, unsafe_allow_html=True)

        if deltas["gone_from_pipeline"]:
            html = "<div class='delta-section move'><h4>🚪 Gone from pipeline</h4>"
            for r in deltas["gone_from_pipeline"]:
                html += render_delta_line(r,
                    suffix=f" — was at <b>{r.get('stage','')}</b>")
            html += "</div>"
            st.markdown(html, unsafe_allow_html=True)

        if deltas["severity_down"]:
            html = "<div class='delta-section good'><h4>⬇️ Severity improved</h4>"
            for d in deltas["severity_down"]:
                html += render_delta_line(d["row"],
                    suffix=f" — <b>{d['from']} → {d['to']}</b>")
            html += "</div>"
            st.markdown(html, unsafe_allow_html=True)

    st.divider()
    st.caption(st.session_state.get("snapshot_msg", "(no snapshot status)"))
