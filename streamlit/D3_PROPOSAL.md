# Proposal: Convert Disc Golf Analytics Visualizations to D3.js

**Issue:** Convert to D3.js  
**Scope:** `streamlit/` dashboard (data source unchanged: DuckDB → dbt `analytics.*`)  
**Status:** Proposal — no code changes yet

---

## 1. Current state (what D3 would replace)

The dashboard is a Streamlit app (`streamlit/app.py`) reading the shared DuckDB warehouse. Only the **All Rounds** tab is wired into navigation; the other 13 page modules exist but are unused.

| Today | Where | Notes |
|---|---|---|
| `st.dataframe` (13 call sites) | most pages | Plain tables; no interactivity beyond sorting |
| `st.metric` (10 call sites) | all_rounds, hole_analysis | Stat cards |
| Custom HTML/CSS hole badges | `pages/all_rounds.py:413+` | Color-coded circles per hole (Ace/Eagle/Birdie/…); hand-rolled `<div>` grid via `st.columns` |
| `st.line_chart` (2 call sites) | `player_profile.py`, `historic_ratings.py` | Native Vega-Lite; multi-series rating history |
| Inline `<style>` blocks | `all_rounds.py` | Dark-mode stat boxes, button overrides, ellipsis cells |

`plotly` is in `streamlit/requirements.txt` but **never imported**. There is no JavaScript, no `st.components`, no custom component anywhere in the app.

### Data available for richer viz

- `analytics.rounds` — wide denormalized table: per-round scores, per-hole scores `"1"…"27"`, per-hole results `"Result 1"…"Result 27"`, throwing stats (C1/C1X/C2 putting, fairway hits, GIR, parked), UDisc rating, player/course/layout/date dimensions. Header comment says percentages should be computed in the frontend after filtering — a natural fit for D3.
- `analytics.hole_results` — grain = one row per hole per round (includes hole distance, zone made from). Ideal for heatmaps and shot-distribution charts; currently unused by the UI.
- 15 more analytics models are specced in `ANALYTICS_MODELS_NEEDED.md` but not yet built in dbt.

---

## 2. Why D3.js

D3 adds capability the current stack cannot express cleanly:

1. **Composite grids** — replace the `st.columns` + HTML hole-badge hack with a real D3 heatmap (rounds × holes, color = result, tooltip = strokes vs par).
2. **Linked/brushable views** — e.g. brush a date range on a rating timeline and see the scorecard grid filter; impossible with Streamlit's run-per-interaction model without a custom component.
3. **Custom scales & layouts** — putting-zone radials, mirrored head-to-head bars, streak timelines, hole-difficulty dot plots with confidence intervals.
4. **Motion & tooltips** — transitions, hover details, and accessible legends without fighting `unsafe_allow_html=True`.

Caveat worth stating: D3 is a low-level toolkit, not a chart library. Each chart is more code than `st.line_chart`. For simple line/bar charts, Observable Plot (D3-based, higher-level) should be preferred *inside* the D3 integration; raw D3 for the custom layouts (heatmap, radial, timeline).

---

## 3. Integration options with Streamlit

| Option | How | Interactivity | Effort |
|---|---|---|---|
| **A. `st.components.v1.html`** | Python queries DuckDB → DataFrame → `to_json()` → inject into an HTML template that loads D3 from CDN and renders | Tooltips, hover, click-to-highlight **inside the chart**; no D3→Streamlit feedback | Low |
| **B. Minimal custom Streamlit component** | Thin React/vanilla wrapper: D3 renders client-side, `ComponentRegistrar` posts events back to Python (`setChoice`, brush extents) | Full bidirectional; D3 events can update `st.session_state` / trigger reruns | Medium |
| **C. Standalone SPA** | Vite + D3 (or SvelteKit) frontend, FastAPI + DuckDB backend replacing Streamlit entirely | Everything; client-side state, no full-rerun per interaction | High |

### Recommended path: **Option A now, Option B only if needed**

- **Option A** covers every concrete D3 chart listed in §5. Charts re-render on Streamlit rerun (filter change → Python query → fresh JSON → fresh D3 render). That is acceptable for this app: the data volume is local DuckDB, queries are fast, and users already wait for a Streamlit rerun today.
- **Option B** (custom component) is the escape hatch if we later need brush→filter linkage across charts or want D3 state to survive Streamlit reruns. Design the Option A helper API so it can be lifted into Option B without rewriting chart code (same JSON payload, same render function).
- **Do not start with Option C.** See §4.

---

## 4. Keep Streamlit vs switch frameworks

### Recommendation: **Keep Streamlit; layer D3 on top via components (Option A → B)**

Reasons grounded in this repo:

1. **The Python/DuckDB/dbt pipeline is the app's value.** The warehouse is a local file (`etl/data/warehouse.duckdb`); pages already do parameterized SQL with `?` placeholders and `@st.cache_resource` connections. A JS SPA would need an API tier (FastAPI + DuckDB) just to serve what Streamlit does for free — a second deployment surface next to the existing Flask `user-app/`.
2. **No CI, no test suite, no monorepo tooling** (per AGENTS.md). Introducing a Node build, bundler, and a new service doubles the operational surface for a small analytics tool.
3. **13 of 14 pages are not even wired into nav yet.** Rewriting them in React before they exist in Streamlit is premature; migrating page-by-page as they're activated is lower risk.
4. **Streamlit ↔ D3 is a well-trodden pattern** (`components.html` + JSON payload). It keeps filters, SQL, and caching in Python where they already live, and confines JS to rendering.

### When to reconsider a full switch (Option C)

Escalate to a Vite + D3 SPA with FastAPI only if one of these becomes true:

- Client-side filtering/aggregation of >100k rows where server round-trips per interaction feel slow.
- Need for real-time collaborative dashboards or embedding in a public web app (the current app is local/personal).
- Cross-chart brushing as a *core* UX (not a nice-to-have), which Option B would handle but awkwardly.
- The dashboard becomes a product with its own deploy cadence, separate from the ETL/dbt pipeline.

None of these apply today. **Keep Streamlit.**

*Rejected alternatives:*
- **Dash (Plotly):** Python-first and supports D3 via clientside callbacks, but it's a different framework with its own layout/callback model — a rewrite, not an increment, and Plotly charts ≠ D3.
- **Panel/Bokeh:** better JS interactivity than Streamlit, but still a rewrite with a smaller community than Streamlit for this use case.
- **`streamlit-echarts` / `st.plotly_chart`:** higher-level JS chart libs wrapped for Streamlit. They'd get 80% of common charts with less code than raw D3, but the issue asks for D3 specifically, and the custom layouts (heatmap matrix, putting radials) are where D3's flexibility matters. If we later want a quick win, ECharts is a reasonable *additional* dependency — not a replacement for this proposal.

---

## 5. Proposed D3 visualizations (prioritized)

Each chart maps to existing or specced analytics models.

| Priority | Chart | Replaces / adds | Data | Notes |
|---|---|---|---|---|
| P0 | **Hole-result heatmap** (rounds × holes grid, color = Ace→Quad+, tooltip = strokes vs par) | `all_rounds.py` HTML hole badges | `analytics.rounds` (`"1"…"27"`, `"Result 1"…"Result 27"`) | Direct port of the current concept; D3 makes it a real chart with hover + legend |
| P0 | **D3 helper infrastructure** | — | — | `streamlit/components/d3_render.py` + chart templates; JSON serialization contract |
| P1 | **Rating timeline** (multi-series line + brush) | `st.line_chart` in `historic_ratings.py`, `player_profile.py` | `analytics.player_rating_history` (needs dbt model) | Brush emits filter range back to Python (Option B) or drives an in-chart detail view (Option A) |
| P1 | **Hole difficulty dot plot** | `hole_analysis.py` metric boxes | `analytics.hole_statistics` (needs dbt model) | Sorted avg score vs par per hole, with n-rounds encoded as size |
| P2 | **Putting radial** (C1 / C1X / C2 make%) | per-round throwing-stat metrics in `all_rounds.py` | `analytics.rounds` putting columns | Small multiples per round or aggregated |
| P2 | **Head-to-head mirrored bars** | `head_to_head.py` dataframe | `analytics.head_to_head` (needs dbt model) | Player A left, Player B right, per-hole or per-stat |
| P2 | **Score distribution histogram** | new | `analytics.rounds` | Filtered by player/course/layout; bins = score vs par |
| P3 | **Streak timeline** | `hole_streaks.py` table | `analytics.hole_streaks` (needs dbt model) | Gantt-like spans over hole indices |
| P3 | **Golden birdie tracker** | `golden_birdies.py` tables | `analytics.golden_birdie_*` (needs dbt models) | Scatter of distance vs result, made/missed encoding |

---

## 6. Technical design (Option A)

### 6.1 Layout of new code

```
streamlit/
├── components/
│   ├── __init__.py
│   ├── d3_render.py          # render_d3(chart_spec, data_df, height=...) helper
│   └── charts/               # one JS template per chart type
│       ├── heatmap.js        # hole-result grid
│       ├── rating_timeline.js
│       ├── hole_dotplot.js
│       └── ...
└── pages/
    └── all_rounds.py         # first consumer: heatmap replaces HTML badges
```

### 6.2 Render helper contract

```python
# components/d3_render.py (sketch)
import json
import streamlit.components.v1 as components
from pathlib import Path

_D3_CDN = "https://cdn.jsdelivr.net/npm/d3@7"

def render_d3(chart: str, data, *, height: int = 400, config: dict | None = None):
    """Render a D3 chart from a JS template + JSON data.

    chart: filename stem under components/charts/ (e.g. "heatmap")
    data:  list of dicts or pandas DataFrame (converted via to_json(orient="records"))
    config: chart-specific options (colors, margins, legends)
    """
    payload = {
        "data": data.to_dict(orient="records") if hasattr(data, "to_dict") else data,
        "config": config or {},
    }
    js = (Path(__file__).parent / "charts" / f"{chart}.js").read_text()
    html = f"""
    <div id="d3-{chart}" style="width:100%;height:{height}px;"></div>
    <script src="{_D3_CDN}"></script>
    <script>
      const payload = {json.dumps(payload)};
      {js}
    </script>
    """
    components.html(html, height=height + 20)
```

Design rules:

- **All filtering/SQL stays in Python.** The JS receives only the already-filtered JSON. This preserves the existing parameterized-query pattern in `all_rounds.py` and the dbt contract.
- **Percentages/aggregations that depend on filters are computed in Python before serialization** (matching the `analytics.rounds` header comment: "Percentages and averages should be calculated in the frontend after filtering" — here "frontend" = the dashboard layer, which can be Python or D3; prefer Python for anything the SQL didn't already group, prefer D3 for display-only transforms).
- **Dark mode:** templates read `window.matchMedia('(prefers-color-scheme: dark)')` and a `config.theme` override, mirroring the existing CSS in `all_rounds.py`.
- **CDN vs vendored D3:** start with CDN for iteration; vendor `d3.v7.min.js` under `streamlit/components/static/` before any deployment that needs offline/reproducible builds.

### 6.3 First concrete migration (P0)

In `all_rounds.py`, after the existing filter block and stat boxes:

1. Query stays as-is (already selects hole + result columns).
2. Replace the per-round `st.columns` badge loop (`all_rounds.py:387+`) with:
   ```python
   heatmap_rows = df[["Player", "Course Layout Name", "Date Formatted",
                      "Score Display", "UDisc Rating"] +
                     [str(i) for i in range(1, max_hole + 1)] +
                     [f"Result {i}" for i in range(1, max_hole + 1)]].to_dict("records")
   render_d3("heatmap", heatmap_rows, height=140 + 28 * len(df), config={
       "colors": {"Ace": "#1E90FF", "Eagle": "#1E90FF", "Birdie": "#87CEEB",
                  "Par": "#D3D3D3", "Bogey": "#FFD700", "Double": "#FFA500",
                  "Triple": "#A0522D", "Quad+": "#2F2F2F"},
   })
   ```
3. Keep the 📊 per-round throwing-stat toggle in Streamlit (it's session-state UI, not a chart); the heatmap is the round *grid*, the metrics stay native.
4. Remove the 3 now-redundant `<style>` blocks that only styled the badge grid.

This is a drop-in visual upgrade with no data-model change and no dbt work.

### 6.4 Interactivity ladder (Option A → B)

| Level | Mechanism | Enables |
|---|---|---|
| 1 (start) | D3 tooltip / hover / click-highlight inside the component | Details on demand |
| 2 | `st.query_params` — chart writes `?player=X&start=…` via `components.html` + JS `window.parent.postMessage` is not available in plain `html()`; instead use a **hidden Streamlit widget + `st_javascript`** or… *(see note)* | Shareable filtered URLs |
| 3 | Custom component (Option B) with `setComponentValue` | Brush → Streamlit rerun with new filter bounds; D3 → session_state |

*Note on Level 2:* plain `components.html` cannot talk back to Streamlit. The practical Level-2 workaround is to render filter controls **inside** the HTML (D3 dropdowns) and pass the selection forward as part of the next Python query only after a manual "Apply" that goes through a Streamlit widget — or skip straight to Option B. Don't contort `components.html` for bidirectional state; that's what Option B is for.

---

## 7. What does **not** change

- ETL → DuckDB → dbt pipeline; `analytics.rounds` / `analytics.hole_results` schemas.
- `utils/db_connection.py`, `@st.cache_resource` connection pattern.
- Flask `user-app/` (credentials service) — untouched.
- dbt targets, `run_prod.sh`, Airflow DAG.
- The single-writer DuckDB constraint: D3 is client-side rendering only; all reads still go through the one Python connection.

---

## 8. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Streamlit rerun destroys D3 DOM each interaction → chart "flashes" | Keep payloads small; `@st.cache_data` on the query functions so reruns are cheap; accept re-render (same as today's HTML badges) |
| CDN unavailable (offline dev) | Vendor D3 locally in a follow-up; document in `streamlit/README.md` |
| XSS via `unsafe_allow_html` / JSON injection into `<script>` | Serialize with `json.dumps(..., default=str)`; never interpolate raw SQL results into HTML attributes; keep user strings inside the JSON payload, not template literals |
| Dark-mode drift between Streamlit theme and D3 colors | Single `config.theme` dict shared by all templates; read `.streamlit/config.toml` primaryColor as default |
| Two rendering systems (native Streamlit widgets + D3) feel inconsistent | Migrate a page at a time; don't mix D3 and `st.line_chart` on the same view |
| Only 2 analytics models exist; most P1–P3 charts are blocked on dbt | P0 heatmap uses existing `analytics.rounds`; sequence dbt model work ahead of each P1+ chart (see `ANALYTICS_MODELS_NEEDED.md`) |

---

## 9. Rollout plan

**Phase 0 — infra (this issue's minimal complete change)**  
- Add `components/d3_render.py` + `components/charts/heatmap.js`.  
- Add D3 CDN note to `streamlit/README.md`.  
- No behavior change to existing pages yet.

**Phase 1 — P0 charts**  
- Wire heatmap into `all_rounds.py` behind the existing filters; delete superseded badge CSS.  
- Manual verification: `cd dbt && dbt run --select analytics.*`, then `cd streamlit && streamlit run app.py`, click through All Rounds filters.

**Phase 2 — activate pages + P1 charts**  
- As each dormant page is added to `app.py` nav, port its table/metric view to a D3 chart where it adds value (rating timeline, hole dot plot).  
- Build the missing dbt analytics models first (per `ANALYTICS_MODELS_NEEDED.md`).

**Phase 3 — optional Option B custom component**  
- Only if brush→filter linkage is demanded. Lift `render_d3` into a component wrapper; chart JS stays the same.

**Out of scope for this proposal:** rewriting the app as a JS SPA; changing ETL/dbt; adding Plotly/ECharts as a parallel charting system.

---

## 10. Decision summary

| Question | Answer |
|---|---|
| Convert visualizations to D3.js? | **Yes**, starting with the All Rounds hole grid (heatmap) and a reusable render helper. |
| Keep Streamlit or switch frameworks? | **Keep Streamlit.** Embed D3 via `st.components.v1.html` (Option A). Escalate to a custom component (Option B) only if bidirectional chart↔Python state becomes essential. Do **not** rewrite as a standalone SPA (Option C) — the local DuckDB + dbt pipeline, absence of CI, and mostly-unbuilt page set make a JS rewrite poor ROI today. |
| Biggest immediate win? | Replacing the hand-rolled `st.columns` + HTML hole badges in `all_rounds.py` with a real D3 heatmap — same data, better chart, no dbt changes. |
