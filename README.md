# NFL Modeler

## NFL Analytics Portfolio Project – Master Checklist

This checklist outlines the key components and tools needed to build a complete, professional-grade NFL analytics project.

---

### 1. Project Setup
- [ ] **Create GitHub repo** with `README.md`, `.gitignore`, and license
- [ ] Set up local environment:
  - [ ] R: Use `{renv}` or `{packrat}` for reproducible dependencies
  - [ ] Python: Use `venv` or `conda`
- [ ] Define folder structure:
  - `etl/`, `models/`, `dashboard/`, `data/`, `sql/`, `docker/`, `docs/`

---

### 2. ETL Pipeline (Extract → Transform → Load)
- [ ] **Extract**: Pull data using `nflreadr` (or `nfl_data_py`)
  - [ ] Download play-by-play, schedules, rosters
  - [ ] Automate refresh with `cron` (local) or `Cloud Scheduler` (GCP)
- [ ] **Transform**: Clean, join, and create features
  - [ ] Game-level summaries
  - [ ] Team stats & rolling averages
  - [ ] In-game state features (score diff, time left, etc.)
- [ ] **Load**:
  - [ ] Write `.parquet` files with `{arrow}` or `pyarrow`
  - [ ] Set up SQL tables in **PostgreSQL** (Cloud SQL)
  - [ ] Upload to database using `DBI::dbWriteTable()` or `SQLAlchemy`

---

### 3. Modeling Pipelines
#### 3.1 Pre-Game Prediction Model
- [ ] Create dataset of team features before kickoff
- [ ] Choose model framework:
  - R: `{parsnip}`, `{xgboost}`, `{tidymodels}`
  - Python: `scikit-learn`, `xgboost`, `lightgbm`
- [ ] Train/test split and cross-validation
- [ ] Evaluate with accuracy, log loss, calibration plots
- [ ] Save model artifacts with `qs`, `joblib`, or `mlflow`

#### 3.2 In-Game Win Probability Model
- [ ] Build dataset from play-by-play
- [ ] Engineer live-game features:
  - `down`, `distance`, `score differential`, `yardline_100`, `seconds_remaining`
- [ ] Train model:
  - Logistic regression or GAM for interpretability
  - XGBoost or GBM for performance
- [ ] Save probability predictions per play for dashboard

---

### 4. Database Hosting (Google Cloud SQL)
- [ ] Create PostgreSQL instance in **Cloud SQL**
- [ ] Set up secure connection with:
  - Cloud SQL Auth Proxy or
  - Static IP allowlist + strong credentials
- [ ] Create schemas: `raw`, `features`, `predictions`
- [ ] Create views or materialized tables for dashboard

---

### 5. Dashboard Development
- [ ] Choose framework:
  - R: `Shiny`, `golem`
  - Python: `Streamlit`, `Dash`
- [ ] Build interactive UI:
  - Pre-game outcome explorer
  - In-game win probability tracker
  - Model insights & explanations
- [ ] Connect to SQL or `.parquet` files as data source

---

### 6. Deployment (Google Cloud)
- [ ] Write `Dockerfile` for:
  - ETL container
  - Dashboard container
- [ ] Push containers to **Google Artifact Registry**
- [ ] Deploy containers with **Cloud Run**
- [ ] Schedule ETL job with **Cloud Scheduler** (calls Cloud Run job)
- [ ] Optional: use **Cloud Build** or **GitHub Actions** for CI/CD

---

### 7. Documentation & Polish
- [ ] Update `README.md`:
  - Project summary
  - Architecture diagram
  - Instructions for running locally & on cloud
- [ ] Add model evaluation reports or dashboards
- [ ] Create public dashboard link and badge
- [ ] (Optional) Record 2-min video walkthrough or demo GIF

---

### 8. Stretch Goals
- [ ] Add player-level projections (e.g., rushing yards, targets)
- [ ] Add betting line comparison
- [ ] Create API for model predictions (Cloud Run + FastAPI)
- [ ] Simulate full season outcomes

---

