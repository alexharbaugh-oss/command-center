# Production Lead Command Center

On-demand Quality Watch for Hand Layup Team Leads. No Databricks login required for users — they just bookmark the URL.

## What it does

Pulls the live Jira pipeline + 6 months of Ion quality history straight from Databricks, cross-references every part in the pipeline against quality history, and flags anything with a track record:

- **🟣 HOLD** — manually flagged, review before any work continues
- **🔴 RED** — 2+ scrap events, stop and plan before layup
- **🟠 ORANGE** — 1 scrap or 3+ issues, extra eyes needed
- **🟡 YELLOW** — 1–2 issues, no scrap, watch list
- **🟢 CLEAN** — no quality history

Five tabs: On the Floor, Ready to Layup, Upstream, Search, and Export (CSV + PDF).

## Deployment (Streamlit Community Cloud)

1. **Create a GitHub repo** (e.g. `command-center`) and push these files.
2. **Get a Databricks service principal token** — read-only on a SQL warehouse. Don't use a personal access token, it'll expire.
3. **Find the warehouse HTTP path** — Databricks → SQL Warehouses → click warehouse → Connection Details → HTTP path.
4. **Deploy:**
   - Go to [share.streamlit.io](https://share.streamlit.io)
   - Connect your repo
   - Main file: `streamlit_app.py`
5. **Add secrets** — App → Settings → Secrets, paste:
   ```toml
   DATABRICKS_HOST      = "joby-aviation-main.cloud.databricks.com"
   DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/<warehouse-id>"
   DATABRICKS_TOKEN     = "<service-principal-token>"
   ```
6. **Bookmark the URL** and share with TLs.

## Daily use (for Team Leads)

1. Open the bookmark.
2. Hit **🔄 Refresh** in the sidebar (data is cached 5 min).
3. Walk the floor with the **On the Floor** tab. Talk to operators on any flagged part.
4. Plan ahead with **Ready to Layup** — anything red/orange/hold needs a plan before it hits the tool.
5. Export CSV or PDF for the FMDS board if needed.

## Data sources

- **Pipeline:** `manufacturing.jira.issues` — project_name = 'ME', layup-relevant statuses
- **Quality:** `manufacturing.onion_silver.quality_issues` — last 180 days, originating_area in '527 Lamination' or '527 Kitting'

## Adding a part to the hold list

Sidebar → "Hold list" textarea → one ME-ID per line. Hold parts get flagged as 🟣 HOLD regardless of quality history.

> Note: V1 hold list is per-session (not shared across users). Wire it to Smartsheet later if you want a single shared source — the V2 scheduled job already reads from `Hand Layup — Quality Watch Hold List`.

## Files

```
command-center/
├── streamlit_app.py           # Main app
├── pdf_export.py              # PDF builder (matches V2 scheduled-job format)
├── requirements.txt           # Dependencies
├── .streamlit/
│   ├── config.toml            # Theme
│   └── secrets.toml.template  # Secrets template (don't commit real one)
├── .gitignore
└── README.md
```

## Customizing

- **Severity rules** — `streamlit_app.py` → `classify_severity()`
- **Excluded parts** — `streamlit_app.py` → `EXCLUDED_PATTERNS`
- **Lookback window** — `streamlit_app.py` → `LOOKBACK_DAYS`
- **PDF layout** — `pdf_export.py`

## Roadmap

V2 ideas (when you're ready):
- Smartsheet hold list sync (shared across all users)
- Slack post button ("Send this report to #hand-layup-supervisors")
- WIP Loss tracker tab
- Stops Log tab
- FMDS snapshot tab
- Earned Hours roll-up tab
