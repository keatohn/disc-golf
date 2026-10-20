# AGENTS.md

Disc golf scorecard data pipeline + dashboard. Four independent Python projects in one repo (no monorepo tooling, **no CI, no lint/format/typecheck, no Python test suite**). The only automated tests are dbt schema tests.

## Architecture & data flow

Order of operations: **ETL → dbt → Streamlit**. All three share one DuckDB file: `data/warehouse.duckdb` (repo root).

- `etl/` — Airflow 3 DAG fetches UDisc scorecards → Parquet (`data/<user>/` at repo root) → loads the DuckDB warehouse. Entry: `etl/airflow/dags/disc_golf_etl_dag.py` (DAG `disc_golf_etl`, Mondays 6am: fetch → load → dbt → email). The container reaches the data via the `../data:/opt/airflow/data` mount in `etl/docker-compose.yaml`.
- `dbt/` — dimensional model: source `main.raw_udisc_scorecards` (created by the ETL; **dbt fails until the warehouse exists**) → `staging` → `dimensional` (schema `dw`) → `analytics`. SQL lint: `cd dbt && sqlfluff lint models` (`dbt/.sqlfluff`, dbt templater, installed in `dbt/.venv`), which enforces lowercase keywords and no FROM/JOIN subqueries. Lint must run from `dbt/` (dbt templater uses cwd as the project dir) and can't check `dbt/macros/` (Jinja-only). `analytics.*` columns are quoted identifiers with exact case (`"Player"`, `"Course Name"`) that dashboard queries depend on — lint never enforces identifier case, so never lowercase them.
- `streamlit/` — dashboard querying `analytics.*` from the same DuckDB file. Entry: `streamlit/app.py`.
- `user-app/` — Flask app for UDisc credentials in AWS Secrets Manager; code lives in `user-app/src/` (deployed separately to Heroku).

Path contracts (run things from the right directory):
- dbt: `profiles.yml` uses `../data/warehouse.duckdb` → **always run dbt from `dbt/`**.
- streamlit: `utils/db_connection.py` resolves `data/warehouse.duckdb` from repo root.

## Commands

Venvs are per-directory, Python 3.12: `dbt/.venv` and `etl/.venv` exist; streamlit uses the system Python 3.12 install (see `streamlit/README.md`).

```bash
# dbt (from dbt/)
cd dbt
dbt deps                      # required once: dbt_utils is gitignored
dbt run                       # default target is dev
dbt test                      # schema tests (the only "test suite" here)
dbt run --select analytics.*  # refresh just the dashboard tables
sqlfluff lint models          # SQL conventions (must run from dbt/)
./run_prod.sh                 # prod run + test; LOAD_TYPE=full → --full-refresh

# dashboard
cd streamlit && streamlit run app.py     # or: streamlit run streamlit/app.py (from root)

# ETL / Airflow
cd etl && ./setup.sh          # creates .env template, installs deps
docker-compose up -d          # UI http://localhost:8080 (password: ./scripts/get-password.sh)

# query the warehouse
python etl/query_warehouse.py
duckdb data/warehouse.duckdb
```

Verification steps: `cd dbt && sqlfluff lint models && dbt build`, then run the dashboard and click through. That's all the repo offers.

## Gotchas

- **`data/` (repo root) is the one data dir and is fully gitignored** (`data/` + `*.duckdb` in `.gitignore`). It holds the ~140 MB `warehouse.duckdb` plus per-user parquet (`data/keaton/` is live pipeline storage: the ETL writes `data_<ts>.parquet` per user there and each load inserts only the newest file per user into `raw_udisc_scorecards`). Never force-add it back to git.
- **Both dbt targets write the same schemas.** `dbt_project.yml` fixes `staging`/`dw`/`analytics` and `macros/generate_schema_name.sql` ignores `target.schema`, so `--target dev` overwrites prod tables in the same file.
- **Single-writer DuckDB**: a running dbt/ETL job and an open Streamlit app can block each other (lock errors). Close one before running the other.
- **The DAG's dbt step is broken in Docker**: it computes `../../dbt` from the dags folder → `/opt/dbt`, but `docker-compose.yaml` mounts dbt at `/opt/airflow/dbt`. Scheduled-run logs show `FileNotFoundError`; run dbt manually from `dbt/` to refresh `analytics.*`.
- **Streamlit pages**: modules are in `streamlit/pages/` and imported explicitly by `app.py` (`streamlit/README.md` still says `page_modules/` — trust the code). Sidebar nav is hidden via `.streamlit/config.toml`. To add a page: file in `pages/` → export in `pages/__init__.py` → nav entry in `app.py`. Currently only the "All Rounds" tab is rendered; the other exported modules are unused.
- Each dashboard page needs specific `analytics.*` tables (mapping in `streamlit/README.md` and `streamlit/ANALYTICS_MODELS_NEEDED.md`); missing tables produce friendly in-app errors.
- Don't re-add `dbt_constraints` (incompatible with DuckDB); `macros/duckdb__create_constraints.sql` is an intentional no-op wired into `on-run-end`.
- dbt versions intentionally differ: `dbt/requirements.txt` pins dbt-core 1.10.x (local venv), `etl/requirements.txt` pins 1.8.0 (Airflow image). Don't sync them blindly.
- Config comes from `.env` files (gitignored): `etl/.env` drives `UDISC_USERS` (JSON array) + AWS Secrets Manager; UDisc passwords live in AWS Secrets Manager, not in `.env`.
- Other READMEs are partly stale (`user-app/README.md` shows root-level files; `etl/README.md` lists lib modules that were moved). Prefer the code. `*/NOTES.md` files are scratchpads (they claim to be uncommitted, but they are tracked).

## Autonomous Development Workflow

When assigned a GitHub issue:

1. Read the issue and relevant project documentation.
2. Identify the affected components before editing.
3. Implement the smallest complete solution.
4. Add or update relevant automated tests.
5. Run tests and fix failures caused by your changes.
6. Run linting/type checks where configured.
7. Review the final diff for bugs and unrelated changes.
8. Summarize the implementation and test results.

### GitHub Workflow
- Use a dedicated feature branch for each issue.
- Give the feature branch a reasonable and brief name based on the work.
- Commit messages should also be brief and high-level.
- Open a draft pull request linked to the issue.
- Never push directly to main.
- Never merge pull requests automatically.
- Do not modify unrelated functionality.

### Handling Problems
- Fix obvious bugs discovered in the code being modified.
- Do not undertake unrelated refactoring.
- If requirements are ambiguous, ask a specific question
  in the GitHub issue and stop implementation.
- If blocked by missing credentials, dependencies, or
  infrastructure, explain the blocker in the issue.
- Never claim tests passed unless they actually ran.
- If tests cannot run, document why.

### Autonomy
- Make reasonable implementation decisions independently.
- Do not ask for approval on routine technical decisions.
- Escalate changes affecting core game mechanics,
  database migrations, security, or public APIs.

## Efficient Issue Resolution
- Start by identifying the most relevant files.
- Avoid exploring unrelated parts of the repository.
- Prefer targeted searches over broad codebase exploration.
- For small bug fixes, aim for a minimal change.
- If investigation exceeds 30 steps without identifying
  the cause, summarize findings and report the blocker.
- Avoid repeatedly searching the same files.