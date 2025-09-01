# NFL Analytics Dashboard — Product Summary

> **Audience:** PMs, data scientists, engineers, and stakeholders.
>
> **Project style:** Human+AI “centaur” workflow — we intentionally pair expert judgment with AI assistance across design, implementation, and operations.

---

## 1) Executive Snapshot

**Purpose.** A continuously updated analytics dashboard to diagnose the NFL landscape: track team/player trends, surface ML‑derived win probabilities pre‑game and in‑game, and help analysts contextualize Vegas money lines.

**Primary users.** Sports analysts, data journalists, betting‑market observers, and internal product/ML teams.

**Value.** One place to go for: (a) trustworthy, fresh data; (b) interpretable models; (c) broadcast‑ready visuals.

**Success metrics .**

* **Coverage & freshness:** 100% game coverage; regular ETL SLOs (daily 06:00 PT; hourly on game days; minutely during games).
* **Model quality:** Pre‑game Brier score ↓ vs baseline; in‑game calibration slope ≈ 1.0; lift over Vegas closing implied probs.
* **Product adoption:** Weekly active users, session length; dashboard query latency < 1.5s P95.
* **Reliability:** <1 incident per month; automated rollbacks; on‑call playbook time‑to‑mitigation < 30 min.

---

## 2) Methods — Conceptual

**Data sources.**

* **nflfastR / nflreadr (R):** play‑by‑play, games, rosters, schedules, weekly stats.
* **Internal engineered tables:** position‑group aggregates, rolling percentiles, team trajectories, consistency/volatility metrics, fantasy projections (planned).
* **Odds (planned):** Vegas money lines/spreads/totals for model evaluation and features.

**Metrics & definitions (examples).**

* **Pre‑game win probability:** Model using team strength, injuries, rest, travel, weather (planned), and market signals.
* **In‑game win probability:** Real‑time model using current score, time remaining, field position, play state, and EPA‑derived features.
* **Player form:** Rolling percentiles of weekly position‑specific stats (e.g., QB EPA/play, WR targets/route, RB yards after contact).
* **Team trajectories:** Cumulative efficiency and opponent‑adjusted metrics by week.

**Modeling approach.**

* **Pre‑game:** Gradient‑boosted trees / calibrated logistic regression; cross‑season cross‑validation, temporal backtesting; calibration via isotonic or Platt scaling.
* **In‑game:** Time‑aware models on play‑by‑play with feature windows; probability updates each event minute.
* **Explainability:** Permutation importance/SHAP, partial‑dependence on key features; stability checks across seasons.

**Assumptions & caveats.**

* Player availability and late injury news can degrade pre‑game accuracy.
* Odds can be both a feature and a benchmark; keep a hold‑out to avoid leakage.
* In‑game models require low‑latency ingest of PBP; during outages fallback to last‑known state.

---

## 3) System Overview & Architecture

**Stack.** R for ETL/feature engineering; Postgres for storage; Python (scikit‑learn) for ML; FastAPI for API; Dash for UI; Docker for local; GCP for prod (Cloud SQL, Cloud Run, Cloud Scheduler, Secret Manager, Artifact Registry).

**High‑level flow.** Ingest (R) → Process (R) → Load (SQL) → Train/Score (Python) → Serve (API) → Visualize (Dash).

```mermaid
flowchart LR
  subgraph Ingest[Ingest & Process (R)]
    A1[nflfastR downloads\nstep1_download] --> A2[clean & engineer\nstep2_process]
    A2 --> A3[DB load SQL\nstep3_sql]
  end
  A3 --> B[(Postgres / Cloud SQL)]
  B --> C[ML Pipelines (Python, sklearn)]
  C --> B
  B --> D[FastAPI Service]
  D --> E[Dash Dashboard]
  classDef box fill:#f6f6f6,stroke:#999,rx:6,ry:6;
  class A1,A2,A3,B,C,D,E box;
```

**Environments.** Local (Docker) → Staging (Cloud Run + Cloud SQL test) → Production (hardened, autoscaled, monitored).

---

## 4) Repository Map

| Path                                   | Purpose                                                             |
| -------------------------------------- | ------------------------------------------------------------------- |
| `data/raw`                             | Unmodified inputs from sources; excluded from version control.      |
| `data/processed`                       | Cleaned/intermediate Parquet/CSV for QA & reuse.                    |
| `data/for_database`                    | Finalized tables staged for DB load.                                |
| `docker/db/docker-entrypoint-initdb.d` | SQL/DDL run at Postgres container init.                             |
| `documents`                            | Specs, ADRs, notebooks, readmes.                                    |
| `etl/R/step1_download`                 | R scripts to download source data (nflfastR, etc.).                 |
| `etl/R/step2_process`                  | R scripts to clean, join, and engineer features.                    |
| `etl/R/step3_sql`                      | R/SQL scripts to materialize and load DB tables.                    |
| `etl/R/misc`                           | Helpers, utilities, shared functions.                               |
| `misc`                                 | Scratch, experiments (should be pruned in prod).                    |
| `models`                               | Python ML training/scoring code, artifacts, and evaluation reports. |
| `renv/...`                             | R dependency lock & local library (managed by `renv`).              |
| `services/api/app/routers`             | FastAPI route handlers.                                             |
| `services/api/app/queries`             | SQL query strings/ORM queries.                                      |
| `services/api/app/rules`               | Business logic, validation, guards.                                 |
| `services/dashboard/pages`             | Dash page components (multi‑page app).                              |
| `services/dashboard/helpers`           | Client helpers, API clients, formatting utils.                      |
| `services/dashboard/assets/logos`      | Team/logo assets served statically.                                 |

---

## 5) ETL Plan & Schedules

**R pipeline stages.**

* **Step 1 — Download:** Pull seasons, schedules, rosters, PBP, weekly stats.
* **Step 2 — Process:** Normalize types; derive weekly aggregates; compute rolling windows and position‑specific metrics; write to `data/processed` and `data/for_database`.
* **Step 3 — Load:** Apply DDL, upsert dimension/fact tables in Postgres; validate row counts and basic constraints.

**Target schedules (America/Los\_Angeles).**

* **Daily baseline:** 06:00 PT — refresh all critical tables for the day.
* **Game days:** hourly from 08:00–23:00 PT.
* **During games:** per‑minute updates for in‑game tables (score state, win prob, player usage).

**Scheduling on GCP (pattern).**

* Use **Cloud Scheduler** (TZ set to `America/Los_Angeles`) → trigger a **Cloud Run Job** (or Cloud Workflows) that runs an ETL container.
* **Crons (examples):**

  * Daily baseline: `0 6 * * *`
  * Hourly gameday: `0 8-23 * * 1,4,5,6,0` (adjust to actual NFL days; or drive by schedule table)
  * Minutely during games: `* 9-22 * * 1,4,5,6,0` (guard with a *game‑now?* check in the job to avoid noise)

**Game‑aware throttling.**

* Maintain a `games_schedule` table; the job checks if any game is live and only then switches to per‑minute incremental updates.

---

## 6) Database & Data Contracts (high‑level)

**Core tables (examples, normalized).**

* `teams`, `players`, `seasons`, `weeks`, `games` (dimensions)
* `pbp` (play‑by‑play facts)
* `player_weekly`, `team_weekly`, `team_season` (aggregates)
* `predictions_pregame`, `predictions_ingame` (ML outputs)

**Conventions.**

* Primary keys: synthetic `*_id` where needed; enforce `(season, week, player_id)` uniqueness for weekly tables.
* Timestamps in UTC; store `as_of` for snapshotting.
* All prediction tables include `model_version`, `train_window`, `eval_metrics` (JSON), and `generated_at`.

---

## 7) ML Pipelines (Python / scikit‑learn)

**Training.** Fetch features from Postgres (read‑only user), split by season/week for temporal validation, track experiments.

**Model registry.** Store serialized artifacts (`.pkl`) and metadata in `models/` (local) or GCS bucket (prod). Tag with semantic versioning.

**Batch scoring.** Pre‑game: generate probs for all scheduled games each morning and on line moves; in‑game: stream/mini‑batch every minute while games are live.

**Serving.**

* **Option A:** The API reads from prediction tables only (simple, robust).
* **Option B:** API hosts model for ad‑hoc scoring (use only with CPU limits & backpressure).

---

## 8) Dashboard (Dash) — Pages & UX

**Pages (current/planned).**

* **Home/Now:** primetime games, headlines, live win probs.
* **Player Trends:** weekly lines with rolling form percentiles; consistency vs volatility.
* **Team Trajectories:** opponent‑adjusted efficiency by week.
* **Matchup Explorer:** side‑by‑side team metrics; pre‑game win probs; moneyline comparison.
* **In‑game View (planned):** live chart of win prob, key plays, drive summaries.
* **Fantasy Lens (planned):** position‑specific leaders, usage trends, injuries.

**Design language.** Team colors for lines; clear legends; P95 latency < 1.5s; mobile‑friendly layout for on‑air use.

---

## 9) Setup & Run — Local

**Prereqs.** R (≥4.3), Python (≥3.11), Docker, `renv`, `pip`.

**1) Start Postgres.**

```bash
docker compose up -d postgres  # if defined; otherwise standard `docker run` using /docker/db/*
```

**2) Initialize DB schema.** Place DDL in `docker/db/docker-entrypoint-initdb.d/*.sql` (applies on first start). For subsequent changes, run migrations or psql scripts.

**3) Restore R deps & run ETL.**

```r
renv::restore()
# Step 1
source("etl/R/step1_download/<script>.R")
# Step 2
source("etl/R/step2_process/<script>.R")
# Step 3 (DB load)
source("etl/R/step3_sql/<script>.R")
```

**4) Python env & services.**

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r services/api/requirements.txt
pip install -r services/dashboard/requirements.txt

# API (FastAPI)
uvicorn app.main:app --app-dir services/api/app --reload --port 8000

# Dashboard (Dash)
python services/dashboard/app.py
```

**.env (example).**

```ini
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=nfl
POSTGRES_USER=nfl_app
POSTGRES_PASSWORD=***
DATABASE_URL=postgresql+psycopg://nfl_app:***@localhost:5432/nfl
API_HOST=0.0.0.0
API_PORT=8000
DASH_HOST=0.0.0.0
DASH_PORT=8050
```

---

## 9A) Makefile — Local Ops Shortcuts

### Quickstart

```bash
# First time (local):
make up            # start Postgres + any default services
make install-deps  # restore R packages via renv
# (Run your step1/step2 R scripts to create data/for_database/*.parquet)
make load-db       # run etl/R/step3_sql/step3_parquet_to_postgres.R
make api-up        # bring up db + API
make start-dashboard
```

### Common workflows

* **Reset database and reload staged parquet:** `make reset-db`
* **Rebuild only the API image & restart:** `make api-rebuild`
* **Check container status:** `make ps`
* **Tail all service logs:** `make logs`
* **API diagnostics:**

  * Health: `make api-health` (GET `http://localhost:8000/health`)
  * Ping:   `make api-ping`   (GET `http://localhost:8000/api/ping`)
* **Enter API container shell:** `make api-shell`
* **Stop/Start services:** `make api-down`, `make api-up`, `make down`, `make up`, `make start-dashboard`, `make update-dashboard`

### Targets reference

| Target             | Purpose                                            | Under the hood                                                     |   |            |
| ------------------ | -------------------------------------------------- | ------------------------------------------------------------------ | - | ---------- |
| `up`               | Start all docker‑compose services in detached mode | `docker-compose up -d && sleep 5`                                  |   |            |
| `install-deps`     | Restore R dependencies                             | `Rscript -e 'renv::restore()'`                                     |   |            |
| `load-db`          | Load staged parquet into Postgres                  | `Rscript etl/R/step3_sql/step3_parquet_to_postgres.R`              |   |            |
| `reset-db`         | Fresh DB + load data                               | `down → up → load-db`                                              |   |            |
| `down`             | Stop and remove compose services                   | `docker-compose down`                                              |   |            |
| `logs`             | Follow logs for all services                       | `docker-compose logs -f`                                           |   |            |
| `ps`               | Show service status                                | `docker-compose ps`                                                |   |            |
| `api-build`        | Build API image                                    | `docker-compose build api`                                         |   |            |
| `api-up`           | Start DB + API                                     | `docker-compose up -d db api`                                      |   |            |
| `api-rebuild`      | Rebuild API without cache and restart              | `docker-compose build --no-cache api && docker-compose up -d api`  |   |            |
| `api-down`         | Stop API container                                 | `docker-compose stop api`                                          |   |            |
| `api-logs`         | API logs only                                      | `docker-compose logs -f api`                                       |   |            |
| `api-shell`        | Shell into API container                           | \`docker-compose exec api sh                                       |   | ... bash\` |
| `api-health`       | Quick health probe                                 | `curl -sS http://localhost:8000/health`                            |   |            |
| `api-ping`         | Echo ping route                                    | `curl -sS http://localhost:8000/api/ping`                          |   |            |
| `start-dashboard`  | Start dashboard                                    | `docker-compose up -d dashboard`                                   |   |            |
| `update-dashboard` | Rebuild + restart dashboard                        | `docker-compose build dashboard && docker-compose up -d dashboard` |   |            |

### ETL execution guide (local)

1. **Download & process (R)** — run your scripts under `etl/R/step1_download` and `etl/R/step2_process` to populate `data/for_database/*.parquet`.
2. **Load to Postgres** — `make load-db` runs `step3_parquet_to_postgres.R`, which should:

   * connect using `POSTGRES_*` env vars,
   * create/ensure schemas & indexes,
   * upsert or truncate‑insert **idempotently** into target tables,
   * log row counts and commit times.
3. **Serve** — `make api-up` (FastAPI) and `make start-dashboard` (Dash) for the UI.

### First-time vs daily recipes

* **First-time:** `make up` → `make install-deps` → run step1/step2 → `make load-db` → `make api-up` → `make start-dashboard`.
* **Daily refresh (manual):** run step1/step2 → `make load-db` → `make api-up`.
* **Clean restart:** `make reset-db`.

### Troubleshooting (quick)

* **API cannot reach DB:** check `docker-compose ps`; ensure `.env` `POSTGRES_*` matches compose service names; try `make api-up` (brings up `db` explicitly).
* **Schema drift on load:** pin R packages (`renv::snapshot()`), re‑generate DDL, and re‑run `make load-db`.
* **Dashboard shows no data:** confirm tables populated (`make load-db` logs), then `make update-dashboard`.

---

## 10) Deploy — GCP (blueprint)

1. **Cloud SQL (Postgres):** create instance + database; set users, private IP; import schema.
2. **Artifact Registry:** build and push images for API, dashboard, ETL job.
3. **Cloud Run:** deploy API & dashboard (min instances 0/1, CPU throttling, VPC connector to Cloud SQL); set env vars via **Secret Manager**.
4. **Cloud Scheduler:** three schedules (daily/hourly/minutely) to trigger **Cloud Run Jobs**; jobs read a mode flag and consult the `games_schedule` table before heavy work.
5. **IAM & networking:** least‑privilege service accounts; SQL Client role; VPC Serverless access.
6. **Monitoring:** logs‑based metrics, request latency dashboards, uptime checks, alerting policies.

---

## 11) Script Descriptions — Templates to Fill

Use this table to document each concrete script as you finalize it.

| Path                                    | Purpose                    | Inputs                 | Outputs                                   | Key Functions/Endpoints       | Schedule/Trigger |
| --------------------------------------- | -------------------------- | ---------------------- | ----------------------------------------- | ----------------------------- | ---------------- |
| `etl/R/step1_download/download_pbp.R`   | Pull PBP for seasons N..M  | nflfastR endpoints     | `data/processed/pbp.parquet`              | `download_pbp()`              | Daily 06:00 PT   |
| `etl/R/step2_process/player_weekly.R`   | Derive weekly player stats | processed PBP, rosters | `data/for_database/player_weekly.parquet` | `make_player_weekly()`        | After PBP        |
| `etl/R/step3_sql/load_player_weekly.R`  | Upsert weekly table        | staged parquet         | `player_weekly` table                     | `db_upsert_*()`               | After process    |
| `services/api/app/routers/analytics.py` | REST endpoints             | DB connection          | JSON responses                            | `/player/rolling_percentiles` | On request       |
| `services/dashboard/pages/analytics.py` | Dashboard page             | API endpoints          | Plotly/Dash components                    | `render_analytics_page()`     | On request       |

> Expand/replace with your actual scripts; keep it short and precise.

---

## 12) Operations

**Data QA gates.** Row counts vs expectations, duplicate keys, null checks, schema drift alerts.

**Model QA.** Rolling backtests, calibration plots, feature importance drift.

**Runbooks.**

* **ETL failed:** retry job → inspect logs → if schema drift, pin package versions and hotfix.
* **Model drift:** freeze model version; retrain with latest season; review calibration.
* **DB hot paths slow:** add indexes; pre‑aggregate; cache API responses (short TTL).

**Backups.** Automated Cloud SQL backups, PITR enabled; test restores quarterly.

---

## 13) Roadmap

* Ingest odds and injury reports; add weather.
* Live in‑game view with per‑minute model updates.
* Feature store for consistent training/serving features.
* Automated model registry + CI/CD with canary deployments.
* User annotations & saved views; downloadable report packs.

---

## 14) Glossary

* **EPA (Expected Points Added):** Change in expected points from play to play.
* **Brier score:** Mean squared error between predicted probabilities and outcomes.
* **Calibration:** Alignment between predicted probabilities and observed frequencies.
* **Centaur workflow:** Human experts paired with AI assistance for speed and quality.

---

### Notes on the AI‑driven approach

We document where AI contributes (docs, code generation, data checks) and where human oversight is required (data contracts, validation criteria, model sign‑off). This hybrid is an explicit design choice for velocity without compromising rigor.
