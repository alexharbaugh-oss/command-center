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

Snapshots: appends to manufacturing.default.kqw_snapshots.
Shift hand-off: 5:30 PM PT.
"""

import io
import re
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict

import streamlit as st
import pandas as pd
from databricks import sql as dbsql

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
COMP_SHOP_AREAS_SQL = ", ".join("'" + a + "'" for a in COMP_SHOP_AREAS)

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


def _run_query(sql):
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def _run_statement(sql):
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

QUALITY_SQL = (
    "SELECT issue_id, issue_title, created, disposition, originating_area, "
    "part_number, part_description, defect_code, issue_status, serialNumber, "
    "link_to_issue, "
    "CASE WHEN lower(coalesce(defect_code, '')) LIKE '%wnk%' "
    "OR lower(coalesce(issue_title, '')) LIKE '%wrinkle%' "
    "OR lower(coalesce(issue_title, '')) LIKE '%winkel%' "
    "THEN 1 ELSE 0 END AS is_wrinkle "
    "FROM manufacturing.onion_silver.quality_issues_view "
    "WHERE CAST(created AS DATE) >= CURRENT_DATE() - INTERVAL " + str(LOOKBACK_DAYS) + " DAYS "
    "AND issue_status != 'deleted' "
    "AND originating_area IN (" + COMP_SHOP_AREAS_SQL + ") "
    "AND upper(coalesce(part_description, '')) NOT LIKE '%BATTERY%' "
    "AND upper(coalesce(part_description, '')) NOT LIKE '%BUSBAR%' "
    "AND upper(coalesce(part_description, '')) NOT LIKE '%SLEEVE, 5-PLY%'"
)


@st.cache_data(ttl=300, show_spinner=False)
def load_pipeline():
    df = _run_query(PIPELINE_SQL)
    if df.empty:
        return df
    mask = ~df["summary"].fillna("").str.contains(EXCLUDED_REGEX, case=False, regex=True)
    df = df.loc[mask].copy()
    df["stage"] = df["status"].map(STATUS_TO_STAGE)
    df["pn_norm"] = df["part_number"].apply(normalize_pn)
    df["stage_rank"] = df["stage"].map(STAGE_RANK)
    # Strip timezone from updated_at so comparisons against pd.Timestamp.now() work
    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True, errors="coerce").dt.tz_localize(None)
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_quality():
    df = _run_query(QUALITY_SQL)
    if df.empty:
        return df
    df["pn_norm"] = df["part_number"].apply(normalize_pn)
    df["clean_defect"] = df["defect_code"].apply(clean_defect_code)
    # Parse as UTC then strip tz so all downstream comparisons against naive timestamps work
    df["created_dt"] = pd.to_datetime(df["created"], utc=True, errors="coerce").dt.tz_localize(None)
    df["created_str"] = df["created_dt"].dt.strftime("%-m/%-d")
    df["created_date"] = df["created_dt"].dt.date
    df["dow"] = df["created_dt"].dt.day_name()
    df["dow_n"] = df["created_dt"].dt.weekday
    df["disposition_clean"] = df["disposition"].fillna("Pending")
    return df


# ============================================================
# SNAPSHOT / DELTA
# ============================================================

def _esc(s):
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


def _shift_for(now_pt):
    return "AM" if (now_pt.hour, now_pt.minute) < (17, 30) else "PM"


def write_snapshot_if_due(scored, now_pt):
    if scored.empty:
        return "No pipeline data — no snapshot written."
    shift = _shift_for(now_pt)
    snap_date_str = now_pt.strftime("%Y-%m-%d")
    try:
        check_sql = (
            "SELECT MAX(snapshot_ts) AS last_ts, MAX(shift) AS last_shift "
            "FROM " + SNAPSHOT_TABLE + " "
            "WHERE snapshot_date = DATE '" + snap_date_str + "' "
            "AND shift = '" + shift + "'"
        )
        last = _run_query(check_sql)
        if not last.empty and last.iloc[0]["last_ts"] is not None:
            last_ts = pd.to_datetime(last.iloc[0]["last_ts"])
            if last_ts.tzinfo is None:
                last_ts_pt = last_ts.tz_localize("UTC").tz_convert("America/Los_Angeles").tz_localize(None)
            else:
                last_ts_pt = last_ts.tz_convert("America/Los_Angeles").tz_localize(None)
            now_naive = now_pt.replace(tzinfo=None)
            elapsed_min = (now_naive - last_ts_pt).total_seconds() / 60.0
            if 0 <= elapsed_min < SNAPSHOT_MIN_GAP_MINUTES:
                return "Snapshot already taken " + str(int(elapsed_min)) + " min ago. Skipping."
    except Exception:
        pass

    ts_str = now_pt.strftime("%Y-%m-%d %H:%M:%S")
    rows_sql = []
    for _, r in scored.iterrows():
        row_sql = (
            "(TIMESTAMP '" + ts_str + "', "
            "DATE '" + snap_date_str + "', "
            "'" + shift + "', "
            + _esc(r.get("issue_id")) + ", "
            + _esc(r.get("order_number")) + ", "
            + _esc(r.get("part_number")) + ", "
            + _esc(r.get("pn_norm")) + ", "
            + _esc((r.get("summary") or "")[:200]) + ", "
            + _esc(r.get("status")) + ", "
            + _esc(r.get("stage")) + ", "
            + _esc(r.get("severity")) + ", "
            + str(int(r.get("issue_count") or 0)) + ", "
            + str(int(r.get("scrap_count") or 0)) + ", "
            + str(int(r.get("wrinkle_count") or 0)) + ", "
            + str(int(r.get("rework_count") or 0)) + ", "
            + str(int(r.get("pending_count") or 0)) + ")"
        )
        rows_sql.append(row_sql)
    CHUNK = 100
    inserted = 0
    for i in range(0, len(rows_sql), CHUNK):
        chunk = rows_sql[i:i+CHUNK]
        sql = "INSERT INTO " + SNAPSHOT_TABLE + " VALUES " + ",".join(chunk)
        _run_statement(sql)
        inserted += len(chunk)
    return "Snapshot saved — " + str(inserted) + " parts at " + ts_str + " " + shift + "."


def load_prior_snapshot(now_pt):
    shift = _shift_for(now_pt)
    today_str = now_pt.strftime("%Y-%m-%d")
    if shift == "AM":
        cutoff_clause = "snapshot_date < DATE '" + today_str + "'"
    else:
        cutoff_clause = "snapshot_date = DATE '" + today_str + "' AND shift = 'AM'"
    sql = (
        "WITH ranked AS ("
        "SELECT *, ROW_NUMBER() OVER (PARTITION BY pn_norm ORDER BY snapshot_ts DESC) AS rn "
        "FROM " + SNAPSHOT_TABLE + " "
        "WHERE " + cutoff_clause + ") "
        "SELECT snapshot_ts, snapshot_date, shift, me_key, mfid, pn_raw, pn_norm, "
        "summary, status, stage, severity, "
        "total_issues, scraps, wrinkles, rework, pending "
        "FROM ranked WHERE rn = 1"
    )
    try:
        return _run_query(sql)
    except Exception:
        return pd.DataFrame()


def compute_deltas(current, prior):
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
        cur = cur_by_me[k]
        pri = pri_by_me[k]
        if (cur.get("stage") or "") != (pri.get("stage") or ""):
            out["stage_changed"].append({"row": cur, "from": pri.get("stage"), "to": cur.get("stage")})
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
            out["severity_up"].append({"row": cur, "from": pri.get("severity"), "to": cur.get("severity")})
        elif cur_sev > pri_sev:
            out["severity_down"].append({"row": cur, "from": pri.get("severity"), "to": cur.get("severity")})
    return out


# ============================================================
# HELPERS
# ============================================================

def normalize_pn(pn):
    if not pn or pn == "None":
        return ""
    s = str(pn).strip()
    s = re.sub(r"-X\d+.*$", "", s)
    s = re.sub(r"-S\d+$", "", s)
    s = re.sub(r"-L\d+$", "", s)
    s = re.sub(r"-TPDT$", "", s)
    return s


def clean_defect_code(dc):
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


def classify_severity(scraps, wrinkles, total):
    if scraps >= 2 or wrinkles >= 3:
        return "RED"
    if scraps >= 1 or total >= 3 or wrinkles >= 2:
        return "ORANGE"
    if total >= 1:
        return "YELLOW"
    return "CLEAN"


def categorize_defect(defect_code):
    if not defect_code:
        return "Other"
    dc = str(defect_code).upper()
    if "WNK" in dc: return "Wrinkles"
    if "FOD" in dc: return "FOD / Particulate"
    if "RES" in dc: return "Resin Ridges"
    if "MIS" in dc: return "Missing Features"
    if "ACP" in dc or "FMV" in dc: return "Cure Profile"
    if "DIM" in dc: return "Dim OOT"
    if "NDI" in dc: return "NDI Defects"
    if "RSA" in dc: return "Resin Starvation"
    if "EDL" in dc: return "Edge Delamination"
    if "SFD" in dc: return "Surface Depression"
    return "Other"


def score_pipeline(pipeline, quality):
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


def filter_quality_by_window(quality, window_days):
    if quality.empty:
        return quality
    today = pd.Timestamp.now().normalize()
    cutoff = today - pd.Timedelta(days=window_days)
    return quality[quality["created_dt"] >= cutoff].copy()


def compute_kpis(quality):
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


def pct_delta(curr, prior):
    if prior == 0:
        return 0.0 if curr == 0 else 100.0
    return 100.0 * (curr - prior) / prior


def fmt_delta(curr, prior, lower_is_better=True):
    if prior == 0 and curr == 0:
        return ("—", "#999999")
    pct = pct_delta(curr, prior)
    arrow = "↑" if pct > 0 else ("↓" if pct < 0 else "→")
    sign = "+" if pct > 0 else ""
    if lower_is_better:
        color = "#1D9E75" if pct < 0 else ("#c0392b" if pct > 0 else "#999999")
    else:
        color = "#1D9E75" if pct > 0 else ("#c0392b" if pct < 0 else "#999999")
    return (arrow + " " + sign + str(int(round(pct))) + "%", color)


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
.metric-row { display: flex; gap: 8px; margin-bottom: 18px; flex-wrap: wrap; }
.metric-box { flex: 1; min-width: 140px; background: #f4f4f4; border-radius: 8px; padding: 10px 14px; }
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

st.markdown(
    '<div class="cmd-header">'
    '<h1>🛠️ Production Lead Command Center</h1>'
    '<div class="sub">Hand Layup · Quality Watch · ' + ts_str + '</div>'
    '</div>',
    unsafe_allow_html=True,
)

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
    st.caption("Lookback: " + str(LOOKBACK_DAYS) + " days")
    st.caption("Areas: 527 Lam, Kitting, Hand Trim, ME-Comp Fab")
    st.caption("Snapshots: " + SNAPSHOT_TABLE)
    st.caption("Shift hand-off: 5:30 PM PT")
    if not HAS_PLOTLY:
        st.warning("Plotly not installed — using fallback charts. Add `plotly` to requirements.txt.")

# Load data
try:
    with st.spinner("Pulling pipeline + 6 months of quality history…"):
        pipeline_df = load_pipeline()
        quality_df = load_quality()
except Exception as e:
    st.error("❌ Couldn't connect to Databricks: " + str(e))
    st.stop()

if pipeline_df.empty:
    st.warning("No parts found in pipeline.")
    st.stop()

scored = score_pipeline(pipeline_df, quality_df)

if "snapshot_written" not in st.session_state:
    try:
        msg = write_snapshot_if_due(scored, now_pt)
        st.session_state["snapshot_written"] = True
        st.session_state["snapshot_msg"] = msg
    except Exception as e:
        st.session_state["snapshot_msg"] = "Snapshot write failed: " + str(e)

# Top metrics
sev_counts = scored["severity"].value_counts()
red    = int(sev_counts.get("RED", 0))
orange = int(sev_counts.get("ORANGE", 0))
yellow = int(sev_counts.get("YELLOW", 0))
clean  = int(sev_counts.get("CLEAN", 0))


def render_kpi_card(label, value, delta_text=None, delta_color=None, sub=None, val_color=None):
    val_style = ""
    if val_color:
        val_style = ' style="color:' + str(val_color) + ';"'
    delta_html = ""
    if delta_text:
        delta_html = '<div class="delta" style="color:' + str(delta_color or "#666") + ';">' + str(delta_text) + '</div>'
    sub_html = ""
    if sub:
        sub_html = '<div class="sub">' + str(sub) + '</div>'
    return (
        '<div class="metric-box">'
        + '<div class="label">' + str(label) + '</div>'
        + '<div class="val"' + val_style + '>' + str(value) + '</div>'
        + delta_html
        + sub_html
        + '</div>'
    )


def kpi_row_html(cards):
    parts = ['<div class="metric-row">']
    for c in cards:
        parts.append(render_kpi_card(
            c.get("label", ""),
            c.get("value", ""),
            c.get("delta_text"),
            c.get("delta_color"),
            c.get("sub"),
            c.get("val_color"),
        ))
    parts.append('</div>')
    return "".join(parts)


# Headline pipeline strip
st.markdown(kpi_row_html([
    {"label": "Pipeline parts", "value": len(scored)},
    {"label": "🔴 Red",    "value": red,    "val_color": "#c0392b"},
    {"label": "🟠 Orange", "value": orange, "val_color": "#d4730b"},
    {"label": "🟡 Yellow", "value": yellow, "val_color": "#d4920b"},
    {"label": "🟢 Clean",  "value": clean,  "val_color": "#1D9E75"},
]), unsafe_allow_html=True)


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
    if row["scrap_count"]:    counts_bits.append("<b>" + str(row["scrap_count"]) + " scrap</b>")
    if row["rework_count"]:   counts_bits.append(str(row["rework_count"]) + " rework")
    if row["pending_count"]:  counts_bits.append(str(row["pending_count"]) + " pending")
    if row["wrinkle_count"]:  counts_bits.append(str(row["wrinkle_count"]) + " wrinkles")
    counts = " · ".join(counts_bits) if counts_bits else (str(row["issue_count"]) + " issues")

    history_html = ""
    if show_history and row.get("history"):
        items = sorted(row["history"], key=lambda x: x.get("created") or 0, reverse=True)[:max_history]
        for h in items:
            d = h.get("disposition_clean") or "Pending"
            scrap_class = "history-scrap" if d == "Scrap" else ""
            history_html += (
                '<div class="history-line">'
                '<span class="' + scrap_class + '">'
                + (h.get("created_str") or "") + " · " + d
                + '</span>'
                + " — "
                + (h.get("clean_defect") or h.get("issue_title") or "")
                + '</div>'
            )

    defect_line = ""
    if defect_str:
        defect_line = "<br/>Defects: " + defect_str

    html = (
        '<div class="alert-card ' + cls + '">'
        + '<span class="badge ' + cls + '">' + sev + '</span>'
        + '<span class="pn">' + summary + '</span>'
        + '<div class="meta">'
        + issue_id + " · MFID-" + str(mfid) + " · " + pn + " · <b>" + stage + "</b> · " + counts
        + defect_line
        + '</div>'
        + history_html
        + '</div>'
    )
    st.markdown(html, unsafe_allow_html=True)


def filter_and_sort(df, stages):
    sub = df[df["stage"].isin(stages)].copy()
    sub = sub.sort_values(
        ["sev_rank", "scrap_count", "wrinkle_count", "issue_count"],
        ascending=[True, False, False, False],
    )
    return sub


def render_delta_line(row, suffix=""):
    summary = (row.get("summary") or "")[:80]
    me = row.get("issue_id") or row.get("me_key") or ""
    mfid = row.get("order_number") or row.get("mfid") or "—"
    sev = row.get("severity") or ""
    sev_color = SEV_COLOR.get(sev, "#666")
    return (
        '<div class="delta-line">'
        + '<span style="color:' + sev_color + ';font-weight:700;">' + sev + '</span> '
        + '<b>' + summary + '</b>'
        + suffix
        + ' <span style="color:#666;font-size:11px;">('
        + me + ' · MFID-' + str(mfid) + ')</span>'
        + '</div>'
    )


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
    st.write("**" + str(len(on_floor)) + "** parts in layup — **" + str(len(flagged)) + "** need attention")
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
    st.write("**" + str(len(next_up)) + "** parts queued — **" + str(len(flagged)) + "** need a plan")
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
    st.write("**" + str(len(upstream)) + "** parts upstream — showing **" + str(len(flagged)) + "** RED/ORANGE")
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
            scored["summary"].fillna("").str.lower().str.contains(ql, regex=False)
            | scored["part_number"].fillna("").str.lower().str.contains(ql, regex=False)
            | scored["issue_id"].fillna("").str.lower().str.contains(ql, regex=False)
            | scored["order_number"].fillna("").str.lower().str.contains(ql, regex=False)
        )
        hits = scored.loc[mask].sort_values(["sev_rank", "stage_rank"])
        st.write("**" + str(len(hits)) + "** pipeline match(es)")
        for _, row in hits.head(40).iterrows():
            render_alert(row)
        if not quality_df.empty:
            qmask = (
                quality_df["part_description"].fillna("").str.lower().str.contains(ql, regex=False)
                | quality_df["part_number"].fillna("").str.lower().str.contains(ql, regex=False)
                | quality_df["clean_defect"].fillna("").str.lower().str.contains(ql, regex=False)
                | quality_df["issue_title"].fillna("").str.lower().str.contains(ql, regex=False)
            )
            qhits = quality_df.loc[qmask]
            in_pipeline = set(hits["pn_norm"].dropna()) if not hits.empty else set()
            qhits = qhits[~qhits["pn_norm"].isin(in_pipeline)]
            if not qhits.empty:
                st.divider()
                st.write("**" + str(len(qhits)) + "** quality-history match(es) (not in pipeline)")
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
        file_name="quality_watch_" + now_pt.strftime("%Y-%m-%d_%H%M") + ".csv",
        mime="text/csv", use_container_width=True,
    )
    if st.button("📄 Generate PDF report", use_container_width=True, type="primary"):
        with st.spinner("Building PDF…"):
            from pdf_export import build_pdf
            pdf_bytes = build_pdf(scored, now_pt)
        shift = _shift_for(now_pt)
        fname = "Kanban_Quality_Watch_" + now_pt.strftime("%-m-%-d-%y") + "_" + shift + ".pdf"
        st.download_button(
            "⬇️ Download PDF", data=pdf_bytes,
            file_name=fname, mime="application/pdf", use_container_width=True,
        )
        st.success("PDF ready: **" + fname + "**")


# ============================================================
# ANALYTICS TAB
# ============================================================
with tab_analytics:
    st.subheader("📊 Quality & Productivity Analytics")
    st.caption("Trend, breakdowns, KPIs, and shift-over-shift changes — all filterable.")

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

    window_days_map = {"7d": 7, "30d": 30, "90d": 90, "6mo": 180}
    window_days = window_days_map[date_range]
    period_label_map = {"7d": "last 7 days", "30d": "last 30 days",
                        "90d": "last 90 days", "6mo": "last 6 months"}
    period_label = period_label_map[date_range]

    q_filtered = quality_df.copy()
    if area_filter:
        q_filtered = q_filtered[q_filtered["originating_area"].isin(area_filter)]
    if disp_filter:
        q_filtered = q_filtered[q_filtered["disposition_clean"].isin(disp_filter)]

    q_curr = filter_quality_by_window(q_filtered, window_days)
    today_ts = pd.Timestamp.now().normalize()
    prior_start = today_ts - pd.Timedelta(days=2 * window_days)
    prior_end   = today_ts - pd.Timedelta(days=window_days)
    q_prior = q_filtered[(q_filtered["created_dt"] >= prior_start) &
                         (q_filtered["created_dt"] < prior_end)].copy()

    kpi_curr = compute_kpis(q_curr)
    kpi_prior = compute_kpis(q_prior)

    # ---- Quality KPIs ----
    st.markdown(
        '<div class="section-header">QUALITY KPIs · '
        + period_label.upper()
        + ' vs prior ' + period_label
        + '</div>',
        unsafe_allow_html=True,
    )

    issues_d, issues_c = fmt_delta(kpi_curr["issues"], kpi_prior["issues"])
    scrap_d,  scrap_c  = fmt_delta(kpi_curr["scraps"], kpi_prior["scraps"])
    wrkl_d,   wrkl_c   = fmt_delta(kpi_curr["wrinkles"], kpi_prior["wrinkles"])
    rework_d, rework_c = fmt_delta(kpi_curr["rework"], kpi_prior["rework"])
    pend_d,   pend_c   = fmt_delta(kpi_curr["pending"], kpi_prior["pending"])
    rate_d,   rate_c   = fmt_delta(kpi_curr["scrap_rate"], kpi_prior["scrap_rate"])

    quality_cards = [
        {"label": "Total issues", "value": kpi_curr["issues"],
         "delta_text": issues_d, "delta_color": issues_c,
         "sub": "prior: " + str(kpi_prior["issues"])},
        {"label": "Scraps", "value": kpi_curr["scraps"],
         "delta_text": scrap_d, "delta_color": scrap_c,
         "sub": "prior: " + str(kpi_prior["scraps"]), "val_color": "#c0392b"},
        {"label": "Wrinkles", "value": kpi_curr["wrinkles"],
         "delta_text": wrkl_d, "delta_color": wrkl_c,
         "sub": "prior: " + str(kpi_prior["wrinkles"]), "val_color": "#d4730b"},
        {"label": "Rework", "value": kpi_curr["rework"],
         "delta_text": rework_d, "delta_color": rework_c,
         "sub": "prior: " + str(kpi_prior["rework"]), "val_color": "#d4920b"},
        {"label": "Pending", "value": kpi_curr["pending"],
         "delta_text": pend_d, "delta_color": pend_c,
         "sub": "prior: " + str(kpi_prior["pending"]), "val_color": "#7C2D12"},
        {"label": "Scrap rate", "value": str(int(round(kpi_curr["scrap_rate"]))) + "%",
         "delta_text": rate_d, "delta_color": rate_c,
         "sub": "prior: " + str(int(round(kpi_prior["scrap_rate"]))) + "%"},
    ]
    st.markdown(kpi_row_html(quality_cards), unsafe_allow_html=True)

    # ---- Productivity KPIs ----
    st.markdown('<div class="section-header">PRODUCTIVITY KPIs · pipeline state right now</div>',
                unsafe_allow_html=True)

    in_layup_n  = int((scored["stage"] == "Layup").sum())
    in_ready_n  = int((scored["stage"] == "Ready to Layup").sum())
    in_mc_n     = int((scored["stage"] == "Material Cutting").sum())

    ready_parts = pipeline_df[pipeline_df["stage"] == "Ready to Layup"].copy()
    if not ready_parts.empty and "updated_at" in ready_parts.columns:
        # updated_at is already timezone-naive thanks to load_pipeline()
        now_naive = pd.Timestamp.now()
        try:
            ready_parts["age_days"] = (now_naive - ready_parts["updated_at"]).dt.days
            avg_dwell = int(ready_parts["age_days"].median()) if not ready_parts["age_days"].dropna().empty else 0
            max_dwell = int(ready_parts["age_days"].max()) if not ready_parts["age_days"].dropna().empty else 0
        except Exception:
            avg_dwell = 0
            max_dwell = 0
    else:
        avg_dwell = 0
        max_dwell = 0

    flagged_pct = int(round(100 * (red + orange + yellow) / max(1, len(scored))))

    prod_cards = [
        {"label": "Active pipeline", "value": len(scored), "sub": "all stages"},
        {"label": "In Layup", "value": in_layup_n, "sub": "on the table"},
        {"label": "Ready to Layup", "value": in_ready_n, "sub": "queued"},
        {"label": "Material Cutting", "value": in_mc_n, "sub": "kit prep"},
        {"label": "Median age in Ready", "value": str(avg_dwell) + "d",
         "sub": "max: " + str(max_dwell) + "d"},
        {"label": "Flagged %", "value": str(flagged_pct) + "%",
         "sub": str(red + orange + yellow) + " of " + str(len(scored))},
    ]
    st.markdown(kpi_row_html(prod_cards), unsafe_allow_html=True)

    # ---- ARTS goal tracker ----
    st.markdown('<div class="section-header">WRINKLE ARTS GOAL · 50% reduction target</div>',
                unsafe_allow_html=True)

    wrinkles_only = q_filtered[q_filtered["is_wrinkle"] == 1].copy()
    if not wrinkles_only.empty:
        wrinkles_only["week"] = wrinkles_only["created_dt"].dt.to_period("W").dt.start_time
        weekly = wrinkles_only.groupby("week").size().reset_index(name="count")
        weekly = weekly.sort_values("week").tail(12)

        if len(weekly) >= 8:
            baseline_wk = int(weekly.head(4)["count"].sum())
            current_wk  = int(weekly.tail(4)["count"].sum())
            target      = baseline_wk * 0.5
            denom = max(1, baseline_wk - target)
            pct_to_goal = max(0, min(100, 100 * (baseline_wk - current_wk) / denom))
            on_pace = current_wk <= target

            if on_pace:
                status_text = "✅ ON PACE"
                status_class = "on-pace"
                fill_class = ""
            elif current_wk < baseline_wk:
                status_text = "⚠️ OFF PACE"
                status_class = "off-pace"
                fill_class = "warn"
            else:
                status_text = "🔴 GETTING WORSE"
                status_class = "off-pace"
                fill_class = "bad"

            change_pct = 100 * (current_wk - baseline_wk) / max(1, baseline_wk)
            change_sign = "+" if change_pct > 0 else ""
            change_str = change_sign + str(int(round(change_pct))) + "%"

            goal_html = (
                '<div class="goal-card ' + status_class + '">'
                + '<h3>' + status_text + '</h3>'
                + '<div style="display:flex;justify-content:space-between;font-size:13px;color:#444;margin-bottom:6px;">'
                + '<span><b>Baseline (first 4 weeks):</b> ' + str(baseline_wk) + ' wrinkles</span>'
                + '<span><b>Last 4 weeks:</b> ' + str(current_wk) + ' wrinkles (' + change_str + ')</span>'
                + '<span><b>Target:</b> ≤ ' + str(int(target)) + '</span>'
                + '</div>'
                + '<div class="progress">'
                + '<div class="progress-fill ' + fill_class + '" style="width:' + str(int(round(pct_to_goal))) + '%;"></div>'
                + '</div>'
                + '<div style="font-size:11px;color:#666;text-align:right;">'
                + str(int(round(pct_to_goal))) + '% of the way to goal'
                + '</div>'
                + '</div>'
            )
            st.markdown(goal_html, unsafe_allow_html=True)
        else:
            st.info("Not enough wrinkle history yet for ARTS goal tracking (need 8+ weeks).")
    else:
        st.info("No wrinkle data in selected window.")

    # ---- Weekly trend chart ----
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
        elif not weekly_agg.empty:
            chart_data = weekly_agg.set_index("week")[["issues", "scraps", "wrinkles"]]
            st.bar_chart(chart_data, height=260)

    # ---- Daily activity (last 30 days) ----
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

        if not daily.empty:
            spike = daily.loc[daily["issues"].idxmax()]
            st.caption(
                "Worst day in window: **" + str(spike["created_date"])
                + "** with **" + str(int(spike["issues"])) + " issues** ("
                + str(int(spike["scraps"])) + " scraps, "
                + str(int(spike["wrinkles"])) + " wrinkles)"
            )

    # ---- Defect drivers + Detection points (side by side) ----
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

    # ---- Day-of-week wrinkle pattern (last 90 days) ----
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
        wkdays = int(dow_agg[dow_agg["dow_n"] < 5]["count"].sum())
        wknds  = int(dow_agg[dow_agg["dow_n"] >= 5]["count"].sum())
        total = wkdays + wknds
        wkday_pct = int(round(100 * wkdays / max(1, total)))
        st.caption(
            "Peak day: **" + peak_day + "** (" + str(peak_count) + " wrinkles)  ·  "
            + "Weekday share: **" + str(wkday_pct) + "%**  ·  "
            + "Total: **" + str(total) + "** wrinkles, 90 days"
        )
    else:
        st.info("No wrinkle data in last 90 days.")

    # ---- Top 10 worst parts ----
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

    # ---- Pipeline composition heatmap ----
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

    # ---- What Changed (delta view) ----
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

        plural = "s" if total_changes != 1 else ""
        summary_html = (
            '<div class="delta-summary">'
            + '<h3>Comparing to: ' + str(prior_date) + ' ' + str(prior_shift_lbl)
            + ' (' + prior_ts.strftime('%-I:%M %p UTC') + ')</h3>'
            + '<div class="sub">' + str(total_changes) + ' change' + plural
            + ' since the prior shift</div>'
            + '<div class="stats">'
            + '🆕 ' + str(n_new) + ' new  ·  '
            + '🚪 ' + str(n_gone) + ' gone  ·  '
            + '➡️ ' + str(n_stage) + ' stage moves  ·  '
            + '🔴 ' + str(n_scrap) + ' new scrap  ·  '
            + '〰️ ' + str(n_wnk) + ' new wrinkle  ·  '
            + '⬆️ ' + str(n_up) + ' severity up  ·  '
            + '⬇️ ' + str(n_down) + ' severity down'
            + '</div>'
            + '</div>'
        )
        st.markdown(summary_html, unsafe_allow_html=True)

        if total_changes == 0:
            st.success("No changes since the prior shift.")

        delta_groups = [
            ("new_scrap",          "warn", "🔴 New scrap events"),
            ("new_wrinkle",        "warn", "〰️ New wrinkle events"),
            ("severity_up",        "warn", "⬆️ Severity escalated"),
            ("new_in_pipeline",    "move", "🆕 New parts in pipeline"),
            ("stage_changed",      "move", "➡️ Stage changes"),
            ("gone_from_pipeline", "move", "🚪 Gone from pipeline"),
            ("severity_down",      "good", "⬇️ Severity improved"),
        ]
        for key, css_class, title in delta_groups:
            items = deltas.get(key, [])
            if not items:
                continue
            html = '<div class="delta-section ' + css_class + '"><h4>' + title + '</h4>'
            for d in items:
                if isinstance(d, dict) and "row" in d:
                    if key in ("new_scrap", "new_wrinkle"):
                        suffix = " — <b>" + str(d["from"]) + " → " + str(d["to"]) + "</b>"
                    elif key in ("severity_up", "severity_down", "stage_changed"):
                        suffix = " — <b>" + str(d["from"]) + " → " + str(d["to"]) + "</b>"
                    else:
                        suffix = ""
                    html += render_delta_line(d["row"], suffix=suffix)
                else:
                    suffix = " — at <b>" + str(d.get("stage", "")) + "</b>"
                    html += render_delta_line(d, suffix=suffix)
            html += '</div>'
            st.markdown(html, unsafe_allow_html=True)

    st.divider()
    st.caption(st.session_state.get("snapshot_msg", "(no snapshot status)"))
