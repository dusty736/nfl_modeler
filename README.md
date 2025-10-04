# NFL Modeler

[![codecov](https://codecov.io/github/dusty736/nfl_modeler/branch/main/graph/badge.svg)](https://app.codecov.io/github/dusty736/nfl_modeler)

[Dashboard](https://nfl-modeler-dashboard-550218142616.europe-west2.run.app/)

- Dustin Burnham
- burnhamdustin@gmail.com
- BS University of Washington
- MDS University of British Columbia

A portfolio project for NFL game prediction using real-time and historical data.

## Features
- End-to-end ETL pipeline (nflfastR, nflreadr)
- Pre-game and in-game outcome models
- Cloud-hosted SQL database
- Public-facing dashboard

## Structure
- `etl/`: Data collection and cleaning
- `models/`: Feature engineering and model training
- `dashboard/`: Web app for predictions and visualizations
- `sql/`: Database schema and queries
- `docker/`: Deployment configuration

## Setup
TBD — will include Docker and Cloud Run deployment instructions.

## License
MIT

## NFL Analytics Portfolio Project – Master Checklist

This checklist outlines the key components and tools needed to build a complete, professional-grade NFL analytics project.

---

### 1. Project Setup
- [x] **Create GitHub repo** with `README.md`, `.gitignore`, and license
- [x] Set up local environment:
  - [x] R: Use `{renv}` or `{packrat}` for reproducible dependencies
  - [x] Python: Use `venv` or `conda`
- [x] Define folder structure:
  - `etl/`, `models/`, `dashboard/`, `data/`, `sql/`, `docker/`, `docs/`

---

### 2. ETL Pipeline (Extract → Transform → Load)
- [x] **Extract**: Pull data using `nflreadr` (or `nfl_data_py`)
  - [x] Download play-by-play, schedules, rosters
  - [x] Automate refresh with `cron` (local) or `Cloud Scheduler` (GCP)
- [x] **Transform**: Clean, join, and create features
  - [x] Game-level summaries
  - [x] Team stats & rolling averages
  - [x] In-game state features (score diff, time left, etc.)
- [ ] **Load**:
  - [x] Write `.parquet` files with `{arrow}` or `pyarrow`
  - [x] Set up SQL tables in **PostgreSQL** (Cloud SQL)
  - [x] Upload to database using `DBI::dbWriteTable()` or `SQLAlchemy`

---

### 3. Modeling Pipelines
#### 3.1 Pre-Game Prediction Model
- [x] Create dataset of team features before kickoff
- [x] Choose model framework:
  - R: `{parsnip}`, `{xgboost}`, `{tidymodels}`
  - Python: `scikit-learn`, `xgboost`, `lightgbm`
- [x] Train/test split and cross-validation
- [x] Evaluate with accuracy, log loss, calibration plots
- [x] Save model artifacts with `qs`, `joblib`, or `mlflow`
---

### 4. Database Hosting (Google Cloud SQL)
- [x] Create PostgreSQL instance in **Cloud SQL**
- [x] Set up secure connection with:
  - Cloud SQL Auth Proxy or
  - Static IP allowlist + strong credentials
- [x] Create schemas: `raw`, `features`, `predictions`
- [x] Create views or materialized tables for dashboard

---

### 5. Dashboard Development
- [x] Choose framework:
  - Python: `Dash`
- [x] Build interactive UI:
  - Pre-game outcome explorer
  - In-game win probability tracker
  - Model insights & explanations
- [x] Connect to SQL or `.parquet` files as data source

---

### 6. Deployment (Google Cloud)
- [x] Write `Dockerfile` for:
  - ETL container
  - Dashboard container
- [x] Push containers to **Google Artifact Registry**
- [x] Deploy containers with **Cloud Run**
- [x] Schedule ETL job with **Cloud Scheduler** (calls Cloud Run job)
- [x] Optional: use **Cloud Build** or **GitHub Actions** for CI/CD

---

### 7. Documentation & Polish
- [x] Update `README.md`:
  - Project summary
  - Architecture diagram
  - Instructions for running locally & on cloud
- [x] Add model evaluation reports or dashboards
- [x] Create public dashboard link and badge

---

### 8. Stretch Goals
- [ ] Add player-level projections (e.g., rushing yards, targets)
- [ ] Add betting line comparison
- [ ] Create API for model predictions (Cloud Run + FastAPI)
- [ ] Simulate full season outcomes

---

