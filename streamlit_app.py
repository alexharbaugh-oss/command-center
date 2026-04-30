"""
Production Lead Command Center — Hand Layup
On-demand Quality Watch for Team Leads.

Deploy: Streamlit Community Cloud
Required secrets (Streamlit Cloud > App > Settings > Secrets):
  DATABRICKS_HOST       e.g. "joby-aviation-main.cloud.databricks.com"
  DATABRICKS_HTTP_PATH  e.g. "/sql/1.0/warehouses/<warehouse-id>"
  DATABRICKS_TOKEN      Service principal token (read-only on warehouse)

Locked filters per spec (4/22/26):
  - Pipeline: project=ME, components=Composite Part, type!=Epic,
              status in (Scheduled, Kit, Ready to Laminate, Laminate)
  - Quality:  4 comp shop areas, 6mo, issue_status!=deleted, no battery/busbar/sleeve 5-ply
  - Severity: RED if scraps>=2 OR wrinkles>=3; ORANGE if scraps>=1 OR total>=3 OR wrinkles>=2
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

# Jira status -> pipeline stage (display name).
# 'Open' (Ready to Schedule) and 'Ready to Cure' are intentionally excluded.
STATUS_TO_STAGE = {
    "Scheduled":         "Scheduled",
    "Kit":               "Material Cutting",
    "Ready to Laminate": "Ready to Layup",
    "Laminate":          "Layup",
}
STAGE_RANK = {s: i for i, s in enumerate([
    "Scheduled", "Material Cutting", "Ready to Layup", "Layup",
])}

# Composite shop quality areas — matches locked brief
COMP_SHOP_AREAS = (
    "527 Lamination",
    "527 Kitting",
    "527 Hand Trim",
    "Manufacturing Engineering - Composites Fabrication",
)
COMP_SHOP_AREAS_SQL = ", ".join(f"'{a}'" for a in COMP_SHOP_AREAS)

# Post-SQL pattern exclusions (TESTdb, training, dev articles).
# These are in addition to the SQL-level BATTERY/BUSBAR/SLEEVE 5-PLY excludes.
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
    # Belt-and-suspenders dev/test article exclusion
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
    """Strip serial/lot suffixes so cross-referencing finds the family.
    Matches the SQL regex in the locked brief: (-X\\d+.*|-S\\d+$|-L\\d+$|-TPDT$)
    """
    if not pn or pn == "None":
        return ""
    s = str(pn).strip()
    s = re.sub(r"-X\d+.*$", "", s)
    s = re.sub(r"-S\d+$",   "", s)
    s = re.sub(r"-L\d+$",   "", s)
    s = re.sub(r"-TPDT$",   "", s)
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


def classify_severity(scraps: int, wrinkles: int, total: int) -> str:
    """Locked classification (4/22/26). Top-down, first match wins."""
    if scraps >= 2 or wrinkles >= 3:
        return "RED"
    if scraps >= 1 or total >= 3 or wrinkles >= 2:
        return "ORANGE"
    if total >= 1:
        return "YELLOW"
    return "CLEAN"


def score_pipeline(pipeline: pd.DataFrame, quality: pd.DataFrame) -> pd.DataFrame:
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
    st.caption("**Severity rules**")
    st.caption("🔴 RED — 2+ scraps OR 3+ wrinkles")
    st.caption("🟠 ORANGE — 1 scrap, 3+ issues, OR 2+ wrinkles")
    st.caption("🟡 YELLOW — 1+ issue, no scrap")
    st.caption("🟢 CLEAN — no quality history")

    st.divider()
    st.caption(f"Lookback: {LOOKBACK_DAYS} days")
    st.caption("Areas: 527 Lam, Kitting, Hand Trim, ME-Comp Fab")
    st.caption("Source: jira.issues + onion_silver.quality_issues_view")

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

# Top metrics
sev_counts = scored["severity"].value_counts()
red    = int(sev_counts.get("RED", 0))
orange = int(sev_counts.get("ORANGE", 0))
yellow = int(sev_counts.get("YELLOW", 0))
clean  = int(sev_counts.get("CLEAN", 0))

st.markdown(f"""
<div class="metric-row">
  <div class="metric-box"><div class="label">Pipeline parts</div><div class="va
