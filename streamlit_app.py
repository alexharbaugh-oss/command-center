"""
Production Lead Command Center — Hand Layup
On-demand Quality Watch + Analytics + Improvement Tracker for Team Leads.

Joby brand palette (8.1 Primary palette):
  Off-Black     #0E1620   (text primary)
  Off-White     #F5F4DF   (brand neutral; UI uses softened #FAFAF6)
  Joby Gray     #CDD0D1   (borders / dividers)
  Joby Blue     #007AE5   (primary accent / links / focus)
  Light Blue    #7CC3FF
  Lightest Blue #C1DEEF   (subtle highlights)
  Dark Blue     #1C3F99
  Dark Blue UI  #083E6F   (digital interface anchor — headers, section bars)
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
WATCHLIST_TABLE = "manufacturing.default.kqw_watchlist"
SNAPSHOT_MIN_GAP_MINUTES = 30
MIN_DAYS_FOR_VERDICT = 14
IMPROVEMENT_THRESHOLD = 0.25

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
    issue_id, order_number, part_number, summary, components,
    status, priority, due_date, created_at, updated_at
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
    df["created_dt"] = pd.to_datetime(df["created"], utc=True, errors="coerce").dt.tz_localize(None)
    df["created_str"] = df["created_dt"].dt.strftime("%-m/%-d")
    df["created_date"] = df["created_dt"].dt.date
    df["dow"] = df["created_dt"].dt.day_name()
    df["dow_n"] = df["created_dt"].dt.weekday
    df["disposition_clean"] = df["disposition"].fillna("Pending")
    return df


# ============================================================
# WATCHLIST
# ============================================================

def _esc(s):
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


@st.cache_data(ttl=60, show_spinner=False)
def load_watchlist():
    sql = (
        "SELECT pn_norm, pn_raw, summary, added_date, added_ts, "
        "intervention_date, intervention_note, status, closed_date, close_note, last_updated "
        "FROM " + WATCHLIST_TABLE + " "
        "ORDER BY status ASC, added_ts DESC"
    )
    try:
        return _run_query(sql)
    except Exception:
        return pd.DataFrame()


def add_to_watchlist(pn_norm, pn_raw, summary, intervention_date=None, intervention_note=None):
    existing_sql = (
        "SELECT pn_norm, status FROM " + WATCHLIST_TABLE + " "
        "WHERE pn_norm = " + _esc(pn_norm)
    )
    existing = _run_query(existing_sql)
    if not existing.empty:
        cur_status = existing.iloc[0]["status"]
        if cur_status == "active":
            return "Already on watchlist."
        upd = (
            "UPDATE " + WATCHLIST_TABLE + " SET "
            "status = 'active', "
            "added_ts = CURRENT_TIMESTAMP(), "
            "added_date = CURRENT_DATE(), "
            "intervention_date = " + (_esc(intervention_date.strftime("%Y-%m-%d")) if intervention_date else "NULL") + ", "
            "intervention_note = " + _esc(intervention_note) + ", "
            "closed_date = NULL, close_note = NULL, "
            "last_updated = CURRENT_TIMESTAMP() "
            "WHERE pn_norm = " + _esc(pn_norm)
        )
        _run_statement(upd)
        return "Reopened on watchlist."

    intv_date_sql = "DATE '" + intervention_date.strftime("%Y-%m-%d") + "'" if intervention_date else "NULL"
    sql = (
        "INSERT INTO " + WATCHLIST_TABLE + " VALUES ("
        + _esc(pn_norm) + ", "
        + _esc(pn_raw) + ", "
        + _esc((summary or "")[:200]) + ", "
        + "CURRENT_DATE(), CURRENT_TIMESTAMP(), "
        + intv_date_sql + ", "
        + _esc(intervention_note) + ", "
        + "'active', NULL, NULL, "
        + "CURRENT_TIMESTAMP())"
    )
    _run_statement(sql)
    return "Added to watchlist."


def update_intervention(pn_norm, intervention_date, intervention_note):
    intv_date_sql = "DATE '" + intervention_date.strftime("%Y-%m-%d") + "'" if intervention_date else "NULL"
    sql = (
        "UPDATE " + WATCHLIST_TABLE + " SET "
        "intervention_date = " + intv_date_sql + ", "
        "intervention_note = " + _esc(intervention_note) + ", "
        "last_updated = CURRENT_TIMESTAMP() "
        "WHERE pn_norm = " + _esc(pn_norm)
    )
    _run_statement(sql)


def close_watchlist_item(pn_norm, status, close_note):
    sql = (
        "UPDATE " + WATCHLIST_TABLE + " SET "
        "status = " + _esc(status) + ", "
        "closed_date = CURRENT_DATE(), "
        "close_note = " + _esc(close_note) + ", "
        "last_updated = CURRENT_TIMESTAMP() "
        "WHERE pn_norm = " + _esc(pn_norm)
    )
    _run_statement(sql)


def delete_from_watchlist(pn_norm):
    sql = "DELETE FROM " + WATCHLIST_TABLE + " WHERE pn_norm = " + _esc(pn_norm)
    _run_statement(sql)


def compute_improvement(quality_df, pn_norm, intervention_date=None):
    if quality_df.empty:
        return {"before": {"issues": 0, "scraps": 0, "wrinkles": 0},
                "after":  {"issues": 0, "scraps": 0, "wrinkles": 0},
                "verdict": "no_data", "days_after": 0, "before_window": 30, "after_window": 30}

    part_q = quality_df[quality_df["pn_norm"] == pn_norm].copy()
    today = pd.Timestamp.now().normalize()

    if intervention_date is not None:
        intv = pd.Timestamp(intervention_date).normalize()
        days_after = max(0, (today - intv).days)
        after_window = min(30, days_after) if days_after > 0 else 0
        before_window = 30
        before_start = intv - pd.Timedelta(days=30)
        before_end   = intv
        after_start  = intv
        after_end    = intv + pd.Timedelta(days=after_window)
    else:
        days_after = 30
        after_window = 30
        before_window = 30
        before_start = today - pd.Timedelta(days=60)
        before_end   = today - pd.Timedelta(days=30)
        after_start  = today - pd.Timedelta(days=30)
        after_end    = today

    def _bucket(start, end):
        sub = part_q[(part_q["created_dt"] >= start) & (part_q["created_dt"] < end)]
        if sub.empty:
            return {"issues": 0, "scraps": 0, "wrinkles": 0}
        return {
            "issues":   int(len(sub)),
            "scraps":   int((sub["disposition"] == "Scrap").sum()),
            "wrinkles": int(sub["is_wrinkle"].sum()),
        }

    before = _bucket(before_start, before_end)
    after  = _bucket(after_start,  after_end)

    if intervention_date is not None and days_after < MIN_DAYS_FOR_VERDICT:
        verdict = "too_soon"
    elif before["issues"] == 0 and after["issues"] == 0:
        verdict = "no_data"
    else:
        b_score = before["scraps"] * 3 + before["wrinkles"] * 2 + before["issues"]
        a_score = after["scraps"]  * 3 + after["wrinkles"]  * 2 + after["issues"]
        if after_window > 0 and after_window != before_window:
            a_score_normalized = a_score * (before_window / after_window)
        else:
            a_score_normalized = a_score
        if b_score == 0:
            verdict = "worsening" if a_score > 0 else "flat"
        else:
            change = (a_score_normalized - b_score) / b_score
            if change <= -IMPROVEMENT_THRESHOLD:
                verdict = "improving"
            elif change >= IMPROVEMENT_THRESHOLD:
                verdict = "worsening"
            else:
                verdict = "flat"

    return {
        "before": before, "after": after, "verdict": verdict,
        "days_after": days_after,
        "before_window": before_window, "after_window": after_window,
    }


# ============================================================
# SNAPSHOT / DELTA
# ============================================================

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
            "(TIMESTAMP '" + ts_str + "', DATE '" + snap_date_str + "', '" + shift + "', "
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
        "FROM " + SNAPSHOT_TABLE + " WHERE " + cutoff_clause + ") "
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
        "issues":       n, "scraps": scraps, "wrinkles": wrinkles,
        "rework":       rework, "pending": pending, "uai": uai,
        "scrap_rate":   100.0 * scraps / n if n else 0.0,
        "wrinkle_rate": 100.0 * wrinkles / n if n else 0.0,
    }


def pct_delta(curr, prior):
    if prior == 0:
        return 0.0 if curr == 0 else 100.0
    return 100.0 * (curr - prior) / prior


def fmt_delta(curr, prior, lower_is_better=True):
    if prior == 0 and curr == 0:
        return ("—", "#9CA3AF")
    pct = pct_delta(curr, prior)
    arrow = "↑" if pct > 0 else ("↓" if pct < 0 else "→")
    sign = "+" if pct > 0 else ""
    if lower_is_better:
        color = "#1D9E75" if pct < 0 else ("#c0392b" if pct > 0 else "#9CA3AF")
    else:
        color = "#1D9E75" if pct > 0 else ("#c0392b" if pct < 0 else "#9CA3AF")
    return (arrow + " " + sign + str(int(round(pct))) + "%", color)


# ============================================================
# UI / STYLES — Joby brand palette applied
# ============================================================

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@500;600;700&display=swap');
:root {
  /* Joby brand palette */
  --joby-off-black:    #0E1620;
  --joby-off-white:    #F5F4DF;
  --joby-gray:         #CDD0D1;
  --joby-blue:         #007AE5;
  --joby-light-blue:   #7CC3FF;
  --joby-lightest:     #C1DEEF;
  --joby-dark-blue:    #1C3F99;
  --joby-dark-blue-ui: #083E6F;

  /* Derived UI tokens */
  --bg-page:           #FAFAF6;       /* softened off-white for page */
  --bg-card:           #FFFFFF;
  --bg-card-alt:       #F7F7F2;
  --bg-inset:          #F0EFE5;
  --border-subtle:     #E8E6D8;
  --border-default:    var(--joby-gray);

  --text-primary:      var(--joby-off-black);
  --text-secondary:    #4A5568;
  --text-muted:        #6B7280;
  --text-faint:        #9CA3AF;

  /* Severity (unchanged for muscle memory) */
  --sev-red:        #C0392B;
  --sev-red-bg:     #FDECEA;
  --sev-orange:     #D4730B;
  --sev-orange-bg:  #FDF1E2;
  --sev-yellow:     #D4920B;
  --sev-yellow-bg:  #FEF7E6;
  --sev-green:      #1D9E75;
  --sev-green-bg:   #E8F6F0;

  /* Shadows */
  --shadow-sm:  0 1px 2px rgba(14, 22, 32, 0.04), 0 1px 3px rgba(14, 22, 32, 0.04);
  --shadow-md:  0 2px 4px rgba(14, 22, 32, 0.05), 0 4px 8px rgba(14, 22, 32, 0.04);
  --shadow-lg:  0 4px 12px rgba(14, 22, 32, 0.07), 0 8px 24px rgba(14, 22, 32, 0.04);

  --radius-sm: 4px;
  --radius-md: 8px;
  --radius-lg: 12px;
}

/* Base: Inter everywhere, JetBrains Mono for tabular numerics */
html, body, [class*="css"], [data-testid="stAppViewContainer"] {
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  -webkit-font-smoothing: antialiased;
  color: var(--text-primary);
}
[data-testid="stAppViewContainer"] { background: var(--bg-page); }
[data-testid="stHeader"] { background: transparent; }

.block-container {
  padding-top: 1rem;
  padding-bottom: 3rem;
  max-width: 1240px;
}

/* Sidebar */
[data-testid="stSidebar"] {
  background: var(--bg-card);
  border-right: 1px solid var(--border-subtle);
}
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 {
  color: var(--joby-dark-blue-ui);
  font-weight: 700;
  letter-spacing: -0.01em;
}

/* === Header ============================================== */
.cmd-header {
  background: linear-gradient(135deg, var(--joby-dark-blue-ui) 0%, var(--joby-dark-blue) 100%);
  color: #FFFFFF;
  padding: 18px 24px;
  border-radius: var(--radius-md);
  margin-bottom: 16px;
  display: flex;
  align-items: center;
  gap: 14px;
  box-shadow: var(--shadow-md);
}
.cmd-header .logo-mark {
  /* Drop your Joby logo here — replace contents with <img src="..."/> */
  width: 40px; height: 40px;
  background: rgba(255, 255, 255, 0.08);
  border: 1px solid rgba(255, 255, 255, 0.18);
  border-radius: var(--radius-sm);
  display: flex; align-items: center; justify-content: center;
  font-size: 18px; flex-shrink: 0;
}
.cmd-header .titles { flex: 1; min-width: 0; }
.cmd-header h1 {
  font-size: 22px; font-weight: 700; margin: 0;
  color: #FFFFFF !important;
  letter-spacing: -0.015em;
  line-height: 1.2;
}
.cmd-header .sub {
  font-size: 12px; color: var(--joby-lightest); margin-top: 4px;
  font-weight: 500; letter-spacing: 0.01em;
}

/* === Sticky KPI strip ==================================== */
.sticky-metrics {
  position: sticky; top: 0;
  z-index: 100;
  background: var(--bg-page);
  padding: 8px 0 4px 0;
  margin-bottom: 18px;
  border-bottom: 1px solid var(--border-subtle);
}

/* === Metric cards ======================================== */
.metric-row {
  display: flex; gap: 10px; flex-wrap: wrap;
}
.metric-box {
  flex: 1; min-width: 140px;
  background: var(--bg-card);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  padding: 12px 16px;
  box-shadow: var(--shadow-sm);
  transition: box-shadow 150ms ease, transform 150ms ease;
}
.metric-box:hover {
  box-shadow: var(--shadow-md);
  transform: translateY(-1px);
}
.metric-box .label {
  font-size: 10.5px; color: var(--text-muted);
  text-transform: uppercase; letter-spacing: 0.6px;
  font-weight: 600;
}
.metric-box .val {
  font-family: 'JetBrains Mono', ui-monospace, monospace;
  font-size: 26px; font-weight: 700; line-height: 1.1;
  margin-top: 4px; color: var(--text-primary);
  font-variant-numeric: tabular-nums;
  letter-spacing: -0.02em;
}
.metric-box .delta {
  font-family: 'JetBrains Mono', ui-monospace, monospace;
  font-size: 11px; font-weight: 600; margin-top: 6px;
  font-variant-numeric: tabular-nums;
}
.metric-box .sub {
  font-size: 10px; color: var(--text-faint); margin-top: 3px;
  font-variant-numeric: tabular-nums;
}

.val-red    { color: var(--sev-red); }
.val-orange { color: var(--sev-orange); }
.val-yellow { color: var(--sev-yellow); }
.val-green  { color: var(--sev-green); }

/* === Alert cards ========================================= */
.alert-card {
  border: 1px solid var(--border-subtle);
  border-left: 4px solid;
  padding: 12px 16px;
  margin-bottom: 8px;
  background: var(--bg-card);
  border-radius: var(--radius-md);
  box-shadow: var(--shadow-sm);
  transition: box-shadow 150ms ease, transform 150ms ease;
}
.alert-card:hover {
  box-shadow: var(--shadow-md);
  transform: translateX(2px);
}
.alert-card.red    { border-left-color: var(--sev-red);    background: var(--sev-red-bg); }
.alert-card.orange { border-left-color: var(--sev-orange); background: var(--sev-orange-bg); }
.alert-card.yellow { border-left-color: var(--sev-yellow); background: var(--sev-yellow-bg); }

.alert-card .pn {
  font-weight: 700; font-size: 14px; color: var(--text-primary);
  letter-spacing: -0.01em;
}
.alert-card .meta {
  font-size: 11.5px; color: var(--text-secondary); margin-top: 4px;
  font-family: 'JetBrains Mono', ui-monospace, monospace;
  font-variant-numeric: tabular-nums;
}
.alert-card .meta b {
  font-family: 'Inter', sans-serif;
  color: var(--text-primary);
}
.alert-card .badge {
  display: inline-block; font-size: 10px; font-weight: 700;
  padding: 2px 8px; border-radius: 3px; color: white; margin-right: 8px;
  letter-spacing: 0.5px;
  vertical-align: 2px;
}
.alert-card .badge.red    { background: var(--sev-red); }
.alert-card .badge.orange { background: var(--sev-orange); }
.alert-card .badge.yellow { background: var(--sev-yellow); color: var(--joby-off-black); }

.history-line {
  font-size: 11.5px; color: var(--text-secondary);
  margin-top: 5px; padding: 4px 10px;
  border-left: 2px solid var(--border-default);
  font-variant-numeric: tabular-nums;
}
.history-scrap {
  color: var(--sev-red); font-weight: 600;
}

/* === Section headers ===================================== */
.section-header {
  background: var(--joby-dark-blue-ui);
  color: #FFFFFF;
  padding: 9px 16px;
  border-radius: var(--radius-sm);
  margin: 22px 0 12px 0;
  font-weight: 700; font-size: 12px;
  letter-spacing: 0.7px;
  box-shadow: var(--shadow-sm);
}

/* === Goal card =========================================== */
.goal-card {
  background: var(--bg-card);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  padding: 18px 22px; margin-bottom: 14px;
  border-left: 5px solid var(--joby-blue);
  box-shadow: var(--shadow-sm);
}
.goal-card.on-pace  { border-left-color: var(--sev-green); background: linear-gradient(to right, var(--sev-green-bg), var(--bg-card) 70%); }
.goal-card.off-pace { border-left-color: var(--sev-red);   background: linear-gradient(to right, var(--sev-red-bg),  var(--bg-card) 70%); }
.goal-card h3 {
  margin: 0 0 8px 0; font-size: 15px;
  color: var(--joby-dark-blue-ui); font-weight: 700;
  letter-spacing: -0.01em;
}
.goal-card .progress {
  height: 16px; background: var(--bg-inset); border-radius: 8px;
  overflow: hidden; margin: 10px 0;
}
.goal-card .progress-fill {
  height: 100%;
  background: linear-gradient(to right, var(--sev-green), var(--joby-blue));
  transition: width 300ms ease;
}
.goal-card .progress-fill.warn {
  background: linear-gradient(to right, var(--sev-yellow), var(--sev-orange));
}
.goal-card .progress-fill.bad {
  background: linear-gradient(to right, var(--sev-red), var(--sev-orange));
}

/* === Delta cards ========================================= */
.delta-section {
  background: var(--bg-card);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  padding: 14px 18px; margin-bottom: 14px;
  border-left: 4px solid var(--joby-blue);
  box-shadow: var(--shadow-sm);
}
.delta-section.warn  { border-left-color: var(--sev-red);    background: var(--sev-red-bg); }
.delta-section.move  { border-left-color: var(--sev-orange); background: var(--sev-orange-bg); }
.delta-section.good  { border-left-color: var(--sev-green);  background: var(--sev-green-bg); }
.delta-section h4 {
  margin: 0 0 10px 0; font-size: 14px; color: var(--text-primary);
  font-weight: 700; letter-spacing: -0.01em;
}
.delta-line {
  font-size: 12px; color: var(--text-primary); margin: 5px 0;
  font-family: 'JetBrains Mono', ui-monospace, monospace;
  font-variant-numeric: tabular-nums;
}
.delta-line b { font-family: 'Inter', sans-serif; }

.delta-summary {
  background: linear-gradient(135deg, var(--joby-dark-blue-ui) 0%, var(--joby-dark-blue) 100%);
  color: #FFFFFF;
  padding: 16px 22px; border-radius: var(--radius-md);
  margin-bottom: 16px;
  box-shadow: var(--shadow-md);
}
.delta-summary h3 {
  margin: 0 0 6px 0; font-size: 16px; color: #FFFFFF;
  font-weight: 700; letter-spacing: -0.01em;
}
.delta-summary .sub {
  font-size: 12px; color: var(--joby-lightest);
  font-weight: 500;
}
.delta-summary .stats {
  font-size: 13px; margin-top: 10px;
  font-family: 'JetBrains Mono', ui-monospace, monospace;
  font-variant-numeric: tabular-nums;
  color: rgba(255,255,255,0.95);
}

/* === Watchlist cards ===================================== */
.watch-card {
  background: var(--bg-card);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  padding: 16px 20px;
  margin-bottom: 12px;
  border-left: 4px solid var(--text-muted);
  box-shadow: var(--shadow-sm);
  transition: box-shadow 150ms ease;
}
.watch-card:hover { box-shadow: var(--shadow-md); }
.watch-card.improving { border-left-color: var(--sev-green);  background: linear-gradient(to right, var(--sev-green-bg), var(--bg-card) 50%); }
.watch-card.flat      { border-left-color: var(--sev-yellow); background: linear-gradient(to right, var(--sev-yellow-bg), var(--bg-card) 50%); }
.watch-card.worsening { border-left-color: var(--sev-red);    background: linear-gradient(to right, var(--sev-red-bg), var(--bg-card) 50%); }
.watch-card.too_soon  { border-left-color: var(--joby-blue);  background: linear-gradient(to right, var(--joby-lightest), var(--bg-card) 50%); }
.watch-card.no_data   { border-left-color: var(--joby-gray);  background: var(--bg-card-alt); }

.watch-card h4 {
  margin: 0 0 6px 0; font-size: 14px; color: var(--text-primary);
  font-weight: 700; letter-spacing: -0.01em;
}
.watch-card .meta {
  font-size: 11.5px; color: var(--text-secondary); margin-bottom: 10px;
}
.watch-card .verdict {
  display: inline-block; font-size: 11px; font-weight: 700;
  padding: 3px 10px; border-radius: 3px; margin-right: 10px;
  letter-spacing: 0.5px;
}
.verdict-improving { background: var(--sev-green);  color: #FFFFFF; }
.verdict-flat      { background: var(--sev-yellow); color: var(--joby-off-black); }
.verdict-worsening { background: var(--sev-red);    color: #FFFFFF; }
.verdict-too_soon  { background: var(--joby-blue);  color: #FFFFFF; }
.verdict-no_data   { background: var(--joby-gray);  color: var(--joby-off-black); }

.beforeafter {
  display: flex; gap: 14px; margin: 10px 0;
  font-size: 12px;
}
.beforeafter .ba-block {
  flex: 1;
  background: var(--bg-card);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-sm);
  padding: 10px 12px;
}
.beforeafter .ba-label {
  font-size: 9.5px; color: var(--text-muted);
  text-transform: uppercase; letter-spacing: 0.6px;
  font-weight: 600; margin-bottom: 3px;
}
.beforeafter .ba-vals {
  font-family: 'JetBrains Mono', ui-monospace, monospace;
  font-size: 13px; color: var(--text-primary); font-weight: 600;
  font-variant-numeric: tabular-nums;
}
.beforeafter .ba-arrow {
  align-self: center; font-size: 18px;
  color: var(--joby-blue); padding: 0 4px;
}

/* === Tabs ================================================ */
[data-testid="stTabs"] { border-bottom: 1px solid var(--border-subtle); }
[data-testid="stTabs"] [role="tablist"] {
  gap: 4px; padding-bottom: 0;
  overflow-x: auto;
}
[data-testid="stTabs"] [role="tab"] {
  font-weight: 600; font-size: 13.5px;
  color: var(--text-secondary);
  padding: 10px 16px;
  border-radius: var(--radius-sm) var(--radius-sm) 0 0;
  transition: color 150ms ease, background 150ms ease;
}
[data-testid="stTabs"] [role="tab"]:hover {
  color: var(--joby-blue);
  background: rgba(0, 122, 229, 0.04);
}
[data-testid="stTabs"] [role="tab"][aria-selected="true"] {
  color: var(--joby-dark-blue-ui);
  border-bottom: 2px solid var(--joby-blue) !important;
}

/* === Buttons ============================================= */
.stButton button {
  font-family: 'Inter', sans-serif !important;
  font-weight: 600 !important;
  border-radius: var(--radius-sm) !important;
  transition: all 150ms ease !important;
  border: 1px solid var(--border-default) !important;
  font-size: 13px !important;
}
.stButton button:hover {
  border-color: var(--joby-blue) !important;
  color: var(--joby-blue) !important;
}
.stButton button[kind="primary"] {
  background: var(--joby-blue) !important;
  border-color: var(--joby-blue) !important;
  color: #FFFFFF !important;
}
.stButton button[kind="primary"]:hover {
  background: var(--joby-dark-blue-ui) !important;
  border-color: var(--joby-dark-blue-ui) !important;
}

/* === Inputs ============================================= */
.stTextInput input, .stTextArea textarea, .stDateInput input {
  border-radius: var(--radius-sm) !important;
  border-color: var(--border-default) !important;
  font-family: 'Inter', sans-serif !important;
}
.stTextInput input:focus, .stTextArea textarea:focus {
  border-color: var(--joby-blue) !important;
  box-shadow: 0 0 0 2px rgba(0, 122, 229, 0.15) !important;
}

/* === Headings ============================================ */
h1, h2, h3, h4 {
  color: var(--joby-dark-blue-ui);
  letter-spacing: -0.015em;
}
[data-testid="stMarkdownContainer"] h2,
[data-testid="stMarkdownContainer"] h3 {
  font-weight: 700;
}

/* === Captions ============================================ */
[data-testid="stCaptionContainer"] {
  color: var(--text-muted) !important;
  font-size: 12px !important;
}

/* === Dividers ============================================ */
hr {
  border-color: var(--border-subtle) !important;
  margin: 20px 0 !important;
}

/* === Alerts (st.success / st.info / st.warning) ========== */
[data-testid="stAlert"] {
  border-radius: var(--radius-md) !important;
  border: 1px solid var(--border-subtle) !important;
}

/* === Expanders =========================================== */
[data-testid="stExpander"] {
  border: 1px solid var(--border-subtle) !important;
  border-radius: var(--radius-md) !important;
  background: var(--bg-card) !important;
  box-shadow: var(--shadow-sm);
  margin-bottom: 8px;
}
[data-testid="stExpander"] summary {
  font-weight: 600;
  color: var(--text-primary);
}

/* === Dataframe =========================================== */
[data-testid="stDataFrame"] {
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  overflow: hidden;
}

/* === Mobile/tablet responsive ============================ */
@media (max-width: 1024px) {
  .block-container { padding-left: 1rem !important; padding-right: 1rem !important; }
  .metric-box { min-width: 120px; padding: 10px 12px; }
  .metric-box .val { font-size: 22px; }
}
@media (max-width: 640px) {
  .cmd-header { padding: 14px 16px; flex-direction: row; gap: 10px; }
  .cmd-header h1 { font-size: 18px; }
  .cmd-header .logo-mark { width: 32px; height: 32px; }
  .metric-row { gap: 6px; }
  .metric-box { min-width: calc(50% - 4px); padding: 9px 11px; }
  .metric-box .val { font-size: 19px; }
  .alert-card { padding: 10px 12px; }
  .alert-card .pn { font-size: 13px; }
  .section-header { font-size: 11px; padding: 8px 12px; }
  .beforeafter { flex-direction: column; gap: 8px; }
  .beforeafter .ba-arrow { transform: rotate(90deg); align-self: flex-start; }
}
</style>
""", unsafe_allow_html=True)

now_pt = datetime.now(timezone.utc) + timedelta(hours=-7)
ts_str = now_pt.strftime("%a %b %-d, %Y · %-I:%M %p PT")

st.markdown(
    '<div class="cmd-header">'
    '<div class="logo-mark">🛠️</div>'
    '<div class="titles">'
    '<h1>Production Lead Command Center</h1>'
    '<div class="sub">Hand Layup · Quality Watch · ' + ts_str + '</div>'
    '</div>'
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
    st.caption("Watchlist: " + WATCHLIST_TABLE)
    st.caption("Shift hand-off: 5:30 PM PT")
    if not HAS_PLOTLY:
        st.warning("Plotly not installed — using fallback charts.")

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
        delta_html = '<div class="delta" style="color:' + str(delta_color or "#6B7280") + ';">' + str(delta_text) + '</div>'
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
            c.get("label", ""), c.get("value", ""),
            c.get("delta_text"), c.get("delta_color"),
            c.get("sub"), c.get("val_color"),
        ))
    parts.append('</div>')
    return "".join(parts)


# Sticky top KPI strip
st.markdown(
    '<div class="sticky-metrics">'
    + kpi_row_html([
        {"label": "Pipeline parts", "value": len(scored)},
        {"label": "🔴 Red",    "value": red,    "val_color": "#C0392B"},
        {"label": "🟠 Orange", "value": orange, "val_color": "#D4730B"},
        {"label": "🟡 Yellow", "value": yellow, "val_color": "#D4920B"},
        {"label": "🟢 Clean",  "value": clean,  "val_color": "#1D9E75"},
    ])
    + '</div>',
    unsafe_allow_html=True,
)


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
    sev_color = SEV_COLOR.get(sev, "#6B7280")
    return (
        '<div class="delta-line">'
        + '<span style="color:' + sev_color + ';font-weight:700;">' + sev + '</span> '
        + '<b>' + summary + '</b>'
        + suffix
        + ' <span style="color:#6B7280;font-size:11px;">('
        + me + ' · MFID-' + str(mfid) + ')</span>'
        + '</div>'
    )


# ============================================================
# TABS
# ============================================================

tab_floor, tab_next, tab_up, tab_search, tab_improve, tab_export, tab_analytics = st.tabs([
    "🏭 In Layup",
    "📋 Ready to Layup",
    "⏰ Upstream",
    "🔍 Search",
    "🎯 Improvement Tracker",
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


# ============================================================
# IMPROVEMENT TRACKER TAB
# ============================================================
with tab_improve:
    st.subheader("🎯 Improvement Tracker")
    st.caption("Watch risk parts. Log corrective actions. Verify if they actually worked.")

    watchlist = load_watchlist()
    active_wl = watchlist[watchlist["status"] == "active"] if not watchlist.empty else pd.DataFrame()
    closed_wl = watchlist[watchlist["status"] != "active"] if not watchlist.empty else pd.DataFrame()

    search_universe = []
    seen_pns = set()
    if not scored.empty:
        for _, r in scored.iterrows():
            pn = r.get("pn_norm") or ""
            if pn and pn not in seen_pns:
                seen_pns.add(pn)
                search_universe.append({
                    "pn_norm": pn, "pn_raw": r.get("part_number") or "",
                    "summary": r.get("summary") or "",
                    "in_pipeline": True, "severity": r.get("severity") or "",
                })
    if not quality_df.empty:
        q_unique = quality_df.drop_duplicates(subset=["pn_norm"])
        for _, r in q_unique.iterrows():
            pn = r.get("pn_norm") or ""
            if pn and pn not in seen_pns:
                seen_pns.add(pn)
                search_universe.append({
                    "pn_norm": pn, "pn_raw": r.get("part_number") or "",
                    "summary": (r.get("part_description") or r.get("issue_title") or "")[:120],
                    "in_pipeline": False, "severity": "",
                })

    st.markdown('<div class="section-header">ADD A PART TO WATCH</div>', unsafe_allow_html=True)

    add_q = st.text_input(
        "Search by part name or part number to add",
        placeholder="e.g. forward floor bracket   |   216159-001",
        key="wl_search",
    )
    if add_q:
        ql = add_q.lower().strip()
        matches = [u for u in search_universe
                   if ql in u["pn_norm"].lower() or ql in u["pn_raw"].lower() or ql in u["summary"].lower()]
        matches = matches[:8]
        if matches:
            st.caption("Matches — click to add")
            for u in matches:
                col_a, col_b, col_c = st.columns([4, 1.5, 1])
                with col_a:
                    on_list = (not active_wl.empty) and (u["pn_norm"] in set(active_wl["pn_norm"]))
                    pipe_tag = " 🏭" if u["in_pipeline"] else ""
                    sev_tag = ""
                    if u["severity"] in SEV_COLOR:
                        sev_tag = (' <span style="color:' + SEV_COLOR[u["severity"]]
                                   + ';font-weight:700;font-size:11px;">' + u["severity"] + '</span>')
                    listed = " <span style='color:#1D9E75;font-size:11px;'>· on watchlist</span>" if on_list else ""
                    st.markdown(
                        '<div style="font-size:13px;">'
                        + '<b>' + u["summary"][:80] + '</b>' + pipe_tag + sev_tag + listed
                        + '<br/><span style="color:#6B7280;font-size:11px;font-family:JetBrains Mono,monospace;">' + u["pn_raw"] + '</span>'
                        + '</div>',
                        unsafe_allow_html=True,
                    )
                with col_b:
                    pass
                with col_c:
                    btn_key = "add_" + u["pn_norm"]
                    if st.button("➕ Watch", key=btn_key, disabled=on_list, use_container_width=True):
                        try:
                            msg = add_to_watchlist(u["pn_norm"], u["pn_raw"], u["summary"])
                            st.cache_data.clear()
                            st.success(msg + " — refreshing…")
                            st.rerun()
                        except Exception as e:
                            st.error("Add failed: " + str(e))
        else:
            st.caption("No matches.")

    st.markdown('<div class="section-header">ACTIVE WATCHLIST · ' + str(len(active_wl)) + ' parts</div>',
                unsafe_allow_html=True)

    if active_wl.empty:
        st.info("Nothing on the watchlist yet. Add a part above to start tracking.")
    else:
        verdict_label = {
            "improving": "✅ IMPROVING", "flat": "⚠️ FLAT",
            "worsening": "🔴 WORSENING", "too_soon": "⏳ TOO SOON",
            "no_data": "— NO DATA",
        }
        verdict_class = {
            "improving": "verdict-improving", "flat": "verdict-flat",
            "worsening": "verdict-worsening", "too_soon": "verdict-too_soon",
            "no_data": "verdict-no_data",
        }
        card_class = {
            "improving": "improving", "flat": "flat",
            "worsening": "worsening", "too_soon": "too_soon",
            "no_data": "no_data",
        }

        for _, w in active_wl.iterrows():
            pn = w["pn_norm"]
            intv = w["intervention_date"]
            intv_dt = pd.to_datetime(intv).date() if pd.notna(intv) else None

            comp = compute_improvement(quality_df, pn, intv_dt)

            pipe_match = scored[scored["pn_norm"] == pn]
            in_pipeline = not pipe_match.empty
            cur_stage = pipe_match.iloc[0]["stage"] if in_pipeline else "—"
            cur_sev = pipe_match.iloc[0]["severity"] if in_pipeline else "—"
            cur_sev_color = SEV_COLOR.get(cur_sev, "#6B7280")

            v = comp["verdict"]
            vc = card_class.get(v, "no_data")
            vlabel = verdict_label.get(v, "—")
            vclass = verdict_class.get(v, "verdict-no_data")

            intv_line = ""
            if intv_dt is not None:
                intv_line = (
                    "Intervention <b>" + intv_dt.strftime("%-m/%-d") + "</b> ("
                    + str(comp["days_after"]) + " days ago)"
                )
                if w.get("intervention_note"):
                    intv_line += " — " + str(w.get("intervention_note"))[:120]
            else:
                intv_line = "<i>No intervention logged yet — comparing last 30d vs prior 30d</i>"

            before = comp["before"]; after = comp["after"]
            ba_html = (
                '<div class="beforeafter">'
                + '<div class="ba-block">'
                + '<div class="ba-label">BEFORE · last ' + str(comp["before_window"]) + ' days</div>'
                + '<div class="ba-vals">' + str(before["issues"]) + ' issues · '
                + str(before["scraps"]) + ' scrap · ' + str(before["wrinkles"]) + ' wrinkle</div>'
                + '</div>'
                + '<div class="ba-arrow">→</div>'
                + '<div class="ba-block">'
                + '<div class="ba-label">AFTER · ' + str(comp["after_window"]) + ' days</div>'
                + '<div class="ba-vals">' + str(after["issues"]) + ' issues · '
                + str(after["scraps"]) + ' scrap · ' + str(after["wrinkles"]) + ' wrinkle</div>'
                + '</div>'
                + '</div>'
            )

            pipe_tag = ""
            if in_pipeline:
                pipe_tag = (
                    ' <span style="color:' + cur_sev_color + ';font-weight:700;font-size:11px;">'
                    + cur_sev + '</span> in <b>' + cur_stage + '</b>'
                )
            else:
                pipe_tag = ' <span style="color:#9CA3AF;font-size:11px;">not currently in pipeline</span>'

            card_html = (
                '<div class="watch-card ' + vc + '">'
                + '<span class="verdict ' + vclass + '">' + vlabel + '</span>'
                + '<b>' + (w.get("summary") or "")[:90] + '</b>' + pipe_tag
                + '<div class="meta">' + intv_line + ' · '
                + str(w.get("pn_raw") or "") + '</div>'
                + ba_html
                + '</div>'
            )
            st.markdown(card_html, unsafe_allow_html=True)

            with st.expander("Edit / Close · " + str(w.get("pn_raw") or pn), expanded=False):
                col1, col2, col3 = st.columns([2, 3, 1.2])
                with col1:
                    new_intv = st.date_input(
                        "Intervention date",
                        value=intv_dt if intv_dt else None,
                        key="intv_" + pn, format="MM/DD/YYYY",
                    )
                with col2:
                    new_note = st.text_input(
                        "What was done",
                        value=str(w.get("intervention_note") or ""),
                        key="intvnote_" + pn,
                        placeholder="e.g. MWI rev 4/12, retrained operators",
                    )
                with col3:
                    st.write("")
                    if st.button("💾 Save", key="save_" + pn, use_container_width=True):
                        try:
                            update_intervention(pn, new_intv, new_note or None)
                            st.cache_data.clear()
                            st.success("Saved.")
                            st.rerun()
                        except Exception as e:
                            st.error("Save failed: " + str(e))

                col4, col5, col6, col7 = st.columns(4)
                with col4:
                    if st.button("✅ Closed-success", key="cs_" + pn, use_container_width=True):
                        try:
                            close_watchlist_item(pn, "closed_success", new_note or None)
                            st.cache_data.clear(); st.success("Closed as success."); st.rerun()
                        except Exception as e:
                            st.error(str(e))
                with col5:
                    if st.button("🔴 Closed-fail", key="cf_" + pn, use_container_width=True):
                        try:
                            close_watchlist_item(pn, "closed_fail", new_note or None)
                            st.cache_data.clear(); st.success("Closed as fail."); st.rerun()
                        except Exception as e:
                            st.error(str(e))
                with col6:
                    if st.button("⚪ Closed-other", key="co_" + pn, use_container_width=True):
                        try:
                            close_watchlist_item(pn, "closed_other", new_note or None)
                            st.cache_data.clear(); st.success("Closed."); st.rerun()
                        except Exception as e:
                            st.error(str(e))
                with col7:
                    if st.button("🗑️ Remove", key="rm_" + pn, use_container_width=True):
                        try:
                            delete_from_watchlist(pn)
                            st.cache_data.clear(); st.success("Removed."); st.rerun()
                        except Exception as e:
                            st.error(str(e))

                part_q = quality_df[quality_df["pn_norm"] == pn].copy()
                if not part_q.empty:
                    part_q["week"] = part_q["created_dt"].dt.to_period("W").dt.start_time
                    wk = part_q.groupby("week").agg(
                        issues=("issue_id", "count"),
                        scraps=("disposition", lambda s: int((s == "Scrap").sum())),
                        wrinkles=("is_wrinkle", "sum"),
                    ).reset_index()
                    if HAS_PLOTLY and not wk.empty:
                        fig = go.Figure()
                        fig.add_trace(go.Bar(x=wk["week"], y=wk["issues"],
                            name="Issues", marker_color="#9CA3AF",
                            hovertemplate="%{x|%b %d}<br>%{y} issues<extra></extra>"))
                        fig.add_trace(go.Bar(x=wk["week"], y=wk["scraps"],
                            name="Scraps", marker_color="#C0392B",
                            hovertemplate="%{x|%b %d}<br>%{y} scraps<extra></extra>"))
                        if intv_dt is not None:
                            fig.add_vline(
                                x=pd.Timestamp(intv_dt),
                                line_dash="dash", line_color="#007AE5",
                                annotation_text="Intervention",
                                annotation_position="top",
                            )
                        fig.update_layout(
                            barmode="overlay", height=200,
                            margin=dict(l=20, r=20, t=20, b=20),
                            legend=dict(orientation="h", y=1.05, x=0),
                            font=dict(family="Inter, sans-serif"),
                            paper_bgcolor="white", plot_bgcolor="white",
                            xaxis_title=None, yaxis_title=None,
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.bar_chart(wk.set_index("week")[["issues", "scraps"]], height=180)
                else:
                    st.caption("No quality history found for this part in the last 6 months.")

    if not closed_wl.empty:
        with st.expander("📚 Closed cases · " + str(len(closed_wl)) + " resolved", expanded=False):
            for _, w in closed_wl.iterrows():
                status_lbl = {
                    "closed_success": "✅ Success",
                    "closed_fail":    "🔴 Fail",
                    "closed_other":   "⚪ Other",
                }.get(w["status"], w["status"])
                cd = pd.to_datetime(w["closed_date"]).date() if pd.notna(w["closed_date"]) else None
                cd_str = cd.strftime("%-m/%-d/%y") if cd else "?"
                note = (w.get("close_note") or w.get("intervention_note") or "—")[:140]
                st.markdown(
                    '<div style="padding:8px 12px;background:#F7F7F2;border:1px solid #E8E6D8;border-radius:6px;margin-bottom:6px;font-size:12px;">'
                    + '<b>' + status_lbl + '</b> · ' + cd_str + ' · '
                    + '<b>' + (w.get("summary") or "")[:80] + '</b> '
                    + '<span style="color:#6B7280;font-family:JetBrains Mono,monospace;">(' + str(w.get("pn_raw") or "") + ')</span>'
                    + '<br/><span style="color:#4A5568;font-size:11px;">' + note + '</span>'
                    + '</div>',
                    unsafe_allow_html=True,
                )


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
        date_range = st.radio("Date range", ["7d", "30d", "90d", "6mo"],
            horizontal=True, index=1, key="date_range")
    with fcol2:
        all_areas = sorted(quality_df["originating_area"].dropna().unique().tolist()) if not quality_df.empty else []
        area_filter = st.multiselect("Originating area", all_areas, default=all_areas, key="area_filter")
    with fcol3:
        all_disps = sorted(quality_df["disposition_clean"].dropna().unique().tolist()) if not quality_df.empty else []
        disp_filter = st.multiselect("Disposition", all_disps, default=all_disps, key="disp_filter")

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

    st.markdown(
        '<div class="section-header">QUALITY KPIs · ' + period_label.upper()
        + ' vs prior ' + period_label + '</div>',
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
         "delta_text": issues_d, "delta_color": issues_c, "sub": "prior: " + str(kpi_prior["issues"])},
        {"label": "Scraps", "value": kpi_curr["scraps"],
         "delta_text": scrap_d, "delta_color": scrap_c,
         "sub": "prior: " + str(kpi_prior["scraps"]), "val_color": "#C0392B"},
        {"label": "Wrinkles", "value": kpi_curr["wrinkles"],
         "delta_text": wrkl_d, "delta_color": wrkl_c,
         "sub": "prior: " + str(kpi_prior["wrinkles"]), "val_color": "#D4730B"},
        {"label": "Rework", "value": kpi_curr["rework"],
         "delta_text": rework_d, "delta_color": rework_c,
         "sub": "prior: " + str(kpi_prior["rework"]), "val_color": "#D4920B"},
        {"label": "Pending", "value": kpi_curr["pending"],
         "delta_text": pend_d, "delta_color": pend_c,
         "sub": "prior: " + str(kpi_prior["pending"]), "val_color": "#7C2D12"},
        {"label": "Scrap rate", "value": str(int(round(kpi_curr["scrap_rate"]))) + "%",
         "delta_text": rate_d, "delta_color": rate_c,
         "sub": "prior: " + str(int(round(kpi_prior["scrap_rate"]))) + "%"},
    ]
    st.markdown(kpi_row_html(quality_cards), unsafe_allow_html=True)

    st.markdown('<div class="section-header">PRODUCTIVITY KPIs · pipeline state right now</div>',
                unsafe_allow_html=True)

    in_layup_n  = int((scored["stage"] == "Layup").sum())
    in_ready_n  = int((scored["stage"] == "Ready to Layup").sum())
    in_mc_n     = int((scored["stage"] == "Material Cutting").sum())

    ready_parts = pipeline_df[pipeline_df["stage"] == "Ready to Layup"].copy()
    if not ready_parts.empty and "updated_at" in ready_parts.columns:
        now_naive = pd.Timestamp.now()
        try:
            ready_parts["age_days"] = (now_naive - ready_parts["updated_at"]).dt.days
            avg_dwell = int(ready_parts["age_days"].median()) if not ready_parts["age_days"].dropna().empty else 0
            max_dwell = int(ready_parts["age_days"].max()) if not ready_parts["age_days"].dropna().empty else 0
        except Exception:
            avg_dwell = 0; max_dwell = 0
    else:
        avg_dwell = 0; max_dwell = 0

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
                status_text = "✅ ON PACE"; status_class = "on-pace"; fill_class = ""
            elif current_wk < baseline_wk:
                status_text = "⚠️ OFF PACE"; status_class = "off-pace"; fill_class = "warn"
            else:
                status_text = "🔴 GETTING WORSE"; status_class = "off-pace"; fill_class = "bad"

            change_pct = 100 * (current_wk - baseline_wk) / max(1, baseline_wk)
            change_sign = "+" if change_pct > 0 else ""
            change_str = change_sign + str(int(round(change_pct))) + "%"

            goal_html = (
                '<div class="goal-card ' + status_class + '">'
                + '<h3>' + status_text + '</h3>'
                + '<div style="display:flex;justify-content:space-between;font-size:13px;color:#4A5568;margin-bottom:6px;flex-wrap:wrap;gap:8px;">'
                + '<span><b>Baseline (first 4 weeks):</b> <span style="font-family:JetBrains Mono,monospace;">' + str(baseline_wk) + '</span> wrinkles</span>'
                + '<span><b>Last 4 weeks:</b> <span style="font-family:JetBrains Mono,monospace;">' + str(current_wk) + '</span> wrinkles (' + change_str + ')</span>'
                + '<span><b>Target:</b> <span style="font-family:JetBrains Mono,monospace;">≤ ' + str(int(target)) + '</span></span>'
                + '</div>'
                + '<div class="progress">'
                + '<div class="progress-fill ' + fill_class + '" style="width:' + str(int(round(pct_to_goal))) + '%;"></div>'
                + '</div>'
                + '<div style="font-size:11px;color:#6B7280;text-align:right;font-family:JetBrains Mono,monospace;">'
                + str(int(round(pct_to_goal))) + '% of the way to goal'
                + '</div>'
                + '</div>'
            )
            st.markdown(goal_html, unsafe_allow_html=True)
        else:
            st.info("Not enough wrinkle history yet for ARTS goal tracking (need 8+ weeks).")
    else:
        st.info("No wrinkle data in selected window.")

    # Plotly default theme override
    PLOTLY_LAYOUT = dict(
        font=dict(family="Inter, sans-serif", color="#0E1620"),
        paper_bgcolor="white", plot_bgcolor="white",
        xaxis=dict(gridcolor="#F0EFE5"), yaxis=dict(gridcolor="#F0EFE5"),
    )

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
            fig.add_trace(go.Bar(x=weekly_agg["week"], y=weekly_agg["issues"],
                name="Total issues", marker_color="#CDD0D1",
                hovertemplate="%{x|%b %d}<br>%{y} issues<extra></extra>"))
            fig.add_trace(go.Bar(x=weekly_agg["week"], y=weekly_agg["scraps"],
                name="Scraps", marker_color="#C0392B",
                hovertemplate="%{x|%b %d}<br>%{y} scraps<extra></extra>"))
            fig.add_trace(go.Scatter(x=weekly_agg["week"], y=weekly_agg["wrinkles"],
                name="Wrinkles", mode="lines+markers",
                line=dict(color="#D4730B", width=3),
                hovertemplate="%{x|%b %d}<br>%{y} wrinkles<extra></extra>"))
            fig.update_layout(
                barmode="overlay", height=320,
                margin=dict(l=20, r=20, t=10, b=20),
                legend=dict(orientation="h", y=1.05, x=0),
                hovermode="x unified",
                xaxis_title=None, yaxis_title="Count",
                **PLOTLY_LAYOUT,
            )
            st.plotly_chart(fig, use_container_width=True)
        elif not weekly_agg.empty:
            chart_data = weekly_agg.set_index("week")[["issues", "scraps", "wrinkles"]]
            st.bar_chart(chart_data, height=260)

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
            fig.add_trace(go.Bar(x=daily["created_date"], y=daily["issues"],
                name="Issues", marker_color="#007AE5",
                hovertemplate="%{x|%b %d}<br>%{y} issues<extra></extra>"))
            fig.add_trace(go.Bar(x=daily["created_date"], y=daily["scraps"],
                name="Scraps", marker_color="#C0392B",
                hovertemplate="%{x|%b %d}<br>%{y} scraps<extra></extra>"))
            fig.update_layout(barmode="group", height=260,
                margin=dict(l=20, r=20, t=10, b=20),
                legend=dict(orientation="h", y=1.05, x=0),
                xaxis_title=None, yaxis_title=None,
                **PLOTLY_LAYOUT)
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
                fig.add_trace(go.Bar(y=cat_agg["category"], x=cat_agg["total"],
                    orientation="h", name="Total", marker_color="#CDD0D1",
                    hovertemplate="%{y}<br>%{x} total<extra></extra>"))
                fig.add_trace(go.Bar(y=cat_agg["category"], x=cat_agg["scraps"],
                    orientation="h", name="Scraps", marker_color="#C0392B",
                    hovertemplate="%{y}<br>%{x} scraps<extra></extra>"))
                fig.update_layout(barmode="overlay", height=300,
                    margin=dict(l=20, r=20, t=10, b=20),
                    legend=dict(orientation="h", y=1.05, x=0),
                    xaxis_title=None, yaxis_title=None,
                    **PLOTLY_LAYOUT)
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
                fig.add_trace(go.Bar(y=det["area_short"], x=det["total"],
                    orientation="h", name="Total", marker_color="#007AE5",
                    hovertemplate="%{y}<br>%{x} issues<extra></extra>"))
                fig.add_trace(go.Bar(y=det["area_short"], x=det["scraps"],
                    orientation="h", name="Scraps", marker_color="#C0392B",
                    hovertemplate="%{y}<br>%{x} scraps<extra></extra>"))
                fig.update_layout(barmode="overlay", height=300,
                    margin=dict(l=20, r=20, t=10, b=20),
                    legend=dict(orientation="h", y=1.05, x=0),
                    xaxis_title=None, yaxis_title=None,
                    **PLOTLY_LAYOUT)
                st.plotly_chart(fig, use_container_width=True)
            elif not det.empty:
                st.bar_chart(det.set_index("area_short")[["total", "scraps"]], height=300)
        else:
            st.info("No data in selected window.")

    st.markdown('<div class="section-header">WRINKLE DAY-OF-WEEK PATTERN · last 90 days</div>',
                unsafe_allow_html=True)

    dow_window = filter_quality_by_window(q_filtered, 90)
    wnk_dow = dow_window[dow_window["is_wrinkle"] == 1].copy()
    if not wnk_dow.empty:
        dow_agg = wnk_dow.groupby(["dow_n", "dow"]).size().reset_index(name="count")
        dow_agg = dow_agg.sort_values("dow_n")
        if HAS_PLOTLY and not dow_agg.empty:
            fig = px.bar(dow_agg, x="dow", y="count",
                color="count", color_continuous_scale="Reds",
                labels={"dow": "Day", "count": "Wrinkles"})
            fig.update_layout(height=240, margin=dict(l=20, r=20, t=10, b=20),
                showlegend=False, coloraxis_showscale=False,
                **PLOTLY_LAYOUT)
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
            "summary": "Part", "issue_id": "ME", "order_number": "MFID",
            "stage": "Stage", "severity": "Sev",
            "issue_count": "Iss", "scrap_count": "Scr",
            "wrinkle_count": "Wrk", "score_v": "Score",
        })
        st.dataframe(show, hide_index=True, use_container_width=True)
    else:
        st.success("No parts in pipeline have any quality history.")

    st.markdown('<div class="section-header">PIPELINE COMPOSITION · severity × stage</div>',
                unsafe_allow_html=True)

    pivot = scored.groupby(["severity", "stage"]).size().unstack(fill_value=0)
    stage_order = ["Scheduled", "Material Cutting", "Ready to Layup", "Layup"]
    pivot = pivot.reindex(columns=[s for s in stage_order if s in pivot.columns], fill_value=0)
    sev_order = [s for s in ["RED", "ORANGE", "YELLOW", "CLEAN"] if s in pivot.index]
    pivot = pivot.reindex(index=sev_order, fill_value=0)

    if HAS_PLOTLY and not pivot.empty:
        fig = px.imshow(pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(),
            color_continuous_scale=[[0, "#FAFAF6"], [1, "#C0392B"]],
            text_auto=True, aspect="auto")
        fig.update_layout(height=240, margin=dict(l=20, r=20, t=10, b=20),
            coloraxis_showscale=False, xaxis_title=None, yaxis_title=None,
            **PLOTLY_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.dataframe(pivot, use_container_width=True)

    st.markdown('<div class="section-header">WHAT CHANGED · since prior shift</div>',
                unsafe_allow_html=True)

    prior = load_prior_snapshot(now_pt)
    if prior.empty:
        st.info("No prior snapshot found yet. The first run kicks off the history.")
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
