# modeling/Python/predict.py
# Purpose: Load saved pregame models from a specific run, generate predictions
#          Supports secure connection to Google Cloud SQL (PostgreSQL) via Unix socket.
# Usage examples:
#   python3 modeling/Python/predict.py --run-id latest --season 2025 --week 3
#   python3 modeling/Python/predict.py --run-id 20250916T101200Z_ab12cd3 --all
# Output: modeling/models/pregame_outcome/predictions/predictions_{run_id}_{scope}.csv

import argparse
import os
from pathlib import Path
from datetime import datetime
import sys
import json
import joblib
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# -----------------------------
# Config (mirror training)
# -----------------------------
SEED = 42

# --- Database Connection Defaults ---
# NOTE: The script will prioritize environment variables over these defaults.
# For Cloud SQL via Proxy (Recommended): Set CLOUD_SQL_INSTANCE_CONNECTION_NAME
# For Local/External IP: Set DB_HOST, DB_USER, DB_PASS
DB_NAME = "nfl"
# HARDCODED: This is the full path to the named Unix socket file created by the proxy
DB_HOST = "/tmp/nfl-modeling:europe-west2:nfl-pg-01" 
DB_PORT = 5432
DB_USER = "nfl_app"
DB_PASS = "CHOOSE_A_STRONG_PASS"
SCHEMA_TABLE = "prod.game_level_modeling_tbl" # schema-qualified table

TARGET = "home_win"

# Never allow market inputs or post-game info
drop_market = ["spread_line", "spread_home"]
drop_for_home_win = [
    "home_score", "away_score",                 # postgame
    "margin", "total_points", "spread_covered",  # other targets/leaks
    *drop_market
]
# Non-predictive/temporal ID columns (no time features)
drop_non_predictive = ["game_id", "kickoff"]

# Paths
SCRIPT_PATH = Path(__file__).resolve()
ROOT_DIR = SCRIPT_PATH.parent.parent              # modeling/
SAVE_ROOT = ROOT_DIR / "models" / "pregame_outcome" # modeling/models/pregame_outcome
RUNS_DIR = SAVE_ROOT / "runs"
PRED_DIR = SAVE_ROOT / "predictions"
PRED_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Helpers
# -----------------------------
def _find_latest_run(runs_dir: Path) -> str:
    if not runs_dir.exists():
        raise FileNotFoundError(f"No runs directory at: {runs_dir}")
    # sort by mtime desc
    candidates = [d for d in runs_dir.iterdir() if d.is_dir()]
    if not candidates:
        raise FileNotFoundError(f"No runs found under: {runs_dir}")
    latest = max(candidates, key=lambda p: p.stat().st_mtime)
    return latest.name

def _load_models(run_dir: Path):
    """Load best available models: prefer calibrated if present."""
    models_dir = run_dir / "models"
    if not models_dir.exists():
        raise FileNotFoundError(f"Models folder not found: {models_dir}")

    loaded = {}
    # LR (no calibrated variant)
    lr_path = models_dir / "lr_en.joblib"
    if lr_path.exists():
        loaded["LR_EN"] = joblib.load(lr_path)
    # RF: prefer isotonic
    rf_cal = models_dir / "rf_isotonic.joblib"
    rf_raw = models_dir / "rf.joblib"
    if rf_cal.exists():
        loaded["RF"] = joblib.load(rf_cal)
    elif rf_raw.exists():
        loaded["RF"] = joblib.load(rf_raw)
    # XGB: prefer isotonic
    xgb_cal = models_dir / "xgb_isotonic.joblib"
    xgb_raw = models_dir / "xgb.joblib"
    if xgb_cal.exists():
        loaded["XGB"] = joblib.load(xgb_cal)
    elif xgb_raw.exists():
        loaded["XGB"] = joblib.load(xgb_raw)

    if not loaded:
        raise FileNotFoundError(f"No model files found in {models_dir}")
    return loaded

def _connect_engine():
    """Establishes SQLAlchemy engine connection using config or environment variables."""
    # env overrides (Docker/CI friendly)
    db = os.getenv("DB_NAME", DB_NAME)
    user = os.getenv("DB_USER", DB_USER)
    pwd = os.getenv("DB_PASS", DB_PASS)
    
    # HARDCODED LOGIC: Relying entirely on the DB_HOST constant for the socket path
    socket_path = DB_HOST 

    if socket_path and socket_path != 'None':
        # Unix socket connection: Use the named pipe path as the host in the query string.
        # Format: postgresql+psycopg2://<user>:<pass>@/<db>?host=<full_socket_path>
        conn_str = (
            f"postgresql+psycopg2://{user}:{pwd}@"
            f"/{db}?host={socket_path}"
        )
        print(f"[Info] Connecting via HARDCODED Cloud SQL Unix socket: host={socket_path}")
    else:
        # This should not happen in the hardcoded setup, but retained as a failsafe
        raise RuntimeError("Failed to retrieve hardcoded socket path (DB_HOST).")

    return create_engine(conn_str)

def _fetch_data(engine, season: int | None, week: int | None, run_all: bool):
    base_sql = f"SELECT * FROM {SCHEMA_TABLE}"
    params = {}
    if run_all:
        sql = text(base_sql)
    else:
        # Flexible WHERE builder
        clauses = []
        if season is not None:
            clauses.append("season = :season")
            params["season"] = int(season)
        if week is not None:
            clauses.append("week = :week")
            params["week"] = int(week)
        if clauses:
            sql = text(base_sql + " WHERE " + " AND ".join(clauses))
        else:
            # nothing specified -> safe guard
            raise ValueError("Specify --all or provide at least --season or --week.")
    
    # Use explicit connection handling
    with engine.connect() as conn:
        df = pd.read_sql_query(sql, conn, params=params)
    return df

def _prepare_features(df: pd.DataFrame):
    if TARGET not in df.columns:
        # fine; outcome can be missing for future games
        pass

    injury_cols = [c for c in df.columns
                   if c.startswith("home_inj_") or c.startswith("away_inj_") or c.startswith("diff_inj_")]

    planned_drops = set(drop_for_home_win + drop_non_predictive + injury_cols)
    to_drop = [c for c in planned_drops if c in df.columns]

    X = df.drop(columns=[c for c in [TARGET] if c in df.columns] + to_drop, errors="ignore")
    return X, to_drop

def _check_expected_columns(pipeline, X: pd.DataFrame):
    """Warn if model expects columns that aren't present."""
    try:
        pre = pipeline.named_steps["preprocess"]
        # during training we passed explicit column name lists to ColumnTransformer
        expected = []
        for name, trans, cols in pre.transformers_:
            if cols is None or cols == "drop":
                continue
            # cols is list-like of column names we expect at transform time
            expected.extend(list(cols))
        missing = [c for c in expected if c not in X.columns]
        if missing:
            print(f"[WARN] Missing expected columns for this pipeline ({len(missing)}): {missing[:20]}{'...' if len(missing)>20 else ''}")
    except Exception as e:
        print("[INFO] Could not verify expected columns:", repr(e))
         
def _ensure_predictions_table(engine):
    ddl = text("""
    CREATE TABLE IF NOT EXISTS prod.pregame_predictions_tbl (
        season                 INTEGER       NOT NULL,
        week                   INTEGER       NOT NULL,
        game_id                TEXT          NOT NULL,
        home_team              TEXT,
        away_team              TEXT,
        pred_proba_home_win    DOUBLE PRECISION NOT NULL,
        pred_home_win          SMALLINT      NOT NULL,
        actual_home_win        SMALLINT,
        run_id                 TEXT          NOT NULL,
        created_at             TIMESTAMPTZ   NOT NULL DEFAULT now(),
        PRIMARY KEY (game_id, run_id)
    );
    CREATE INDEX IF NOT EXISTS pregame_predictions_season_week_idx
        ON prod.pregame_predictions_tbl (season, week);
    """)
    with engine.begin() as conn:
        conn.execute(ddl)

def _upsert_predictions(engine, df_out: pd.DataFrame, run_id: str):
    """
    Upsert rows into prod.pregame_predictions_tbl using (game_id, run_id) as PK.
    """
    if df_out.empty:
        print("[INFO] No rows to upsert.")
        return

    # Prepare records (NaN -> None)
    rows = []
    for _, r in df_out.iterrows():
        rows.append({
            "season": int(r.get("season")) if pd.notna(r.get("season")) else None,
            "week": int(r.get("week")) if pd.notna(r.get("week")) else None,
            "game_id": str(r.get("game_id")) if pd.notna(r.get("game_id")) else None,
            "home_team": None if pd.isna(r.get("home_team")) else str(r.get("home_team")),
            "away_team": None if pd.isna(r.get("away_team")) else str(r.get("away_team")),
            "pred_proba_home_win": float(r.get("pred_proba_home_win")),
            "pred_home_win": int(r.get("pred_home_win")),
            "actual_home_win": None if ("actual_home_win" not in r or pd.isna(r.get("actual_home_win")))
                                     else int(r.get("actual_home_win")),
            "run_id": run_id,
        })

    sql = text("""
        INSERT INTO prod.pregame_predictions_tbl
            (season, week, game_id, home_team, away_team,
             pred_proba_home_win, pred_home_win, actual_home_win, run_id)
        VALUES
            (:season, :week, :game_id, :home_team, :away_team,
             :pred_proba_home_win, :pred_home_win, :actual_home_win, :run_id)
        ON CONFLICT (game_id, run_id) DO UPDATE
        SET
            season = EXCLUDED.season,
            week = EXCLUDED.week,
            home_team = EXCLUDED.home_team,
            away_team = EXCLUDED.away_team,
            pred_proba_home_win = EXCLUDED.pred_proba_home_win,
            pred_home_win = EXCLUDED.pred_home_win,
            actual_home_win = EXCLUDED.actual_home_win,
            created_at = now();
    """)

    # Row-wise upsert (simple and fine for weekly volumes)
    with engine.begin() as conn:
        conn.execute(sql, rows) # Execute multiple rows at once if supported by dialect
    
    print(f"Upserted {len(rows)} rows into prod.pregame_predictions_tbl")

# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Generate pregame outcome predictions (soft-vote) from saved models.")
    ap.add_argument("--run-id", default="latest", help="Run ID under runs/. Use 'latest' to pick most recent.")
    ap.add_argument("--season", type=int, default=None, help="Season to predict (e.g., 2025).")
    ap.add_argument("--week", type=int, default=None, help="Week to predict (1..).")
    ap.add_argument("--all", action="store_true", help="Predict for all rows in the table (ignores season/week).")
    ap.add_argument("--to-db", action="store_true",
                   help="If set, upsert predictions into prod.pregame_predictions_tbl")
    args = ap.parse_args()

    run_id = args.run_id
    if run_id == "latest":
        run_id = _find_latest_run(RUNS_DIR)

    run_dir = RUNS_DIR / run_id
    if not run_dir.exists():
        raise FileNotFoundError(f"Run directory not found: {run_dir}")

    models = _load_models(run_dir)
    model_keys = list(models.keys())
    print(f"Using run: {run_id}")
    print(f"Loaded models: {model_keys}")

    engine = _connect_engine()
    df = _fetch_data(engine, args.season, args.week, args.all)
    if df.empty:
        print("No rows returned from the database for the given filters. Nothing to do.")
        return

    # Keep scheduling/ID columns for output
    out_cols = [c for c in ["season","week","game_id","home_team","away_team"] if c in df.columns]

    X, dropped = _prepare_features(df)
    print(f"Rows loaded: {df.shape[0]} | Features after drops: {X.shape[1]} | Dropped cols: {len(dropped)}")

    # Sanity check required columns vs model expectations (warn only)
    for m in models.values():
        _check_expected_columns(m, X)

    # Probabilities per model
    probas = []
    for name in model_keys:
        try:
            # predict_proba returns [P(class 0), P(class 1)]. We only want P(home_win) (class 1)
            p = models[name].predict_proba(X)[:, 1]
            probas.append(p)
        except Exception as e:
            print(f"[WARN] Model {name} failed to predict: {e}")

    if not probas:
        raise RuntimeError("No models produced probabilities. Aborting.")

    proba_vote = np.vstack(probas).mean(axis=0)
    pred_vote = (proba_vote >= 0.5).astype(int)

    # Outcome if exists
    outcome = df[TARGET].astype("float").where(df[TARGET].notna(), np.nan) if TARGET in df.columns else np.nan

    out_df = df[out_cols].copy()
    out_df["pred_proba_home_win"] = np.round(proba_vote, 3)
    out_df["pred_home_win"] = pred_vote.astype(int)
    if TARGET in df.columns:
        out_df["actual_home_win"] = outcome

    # Scope label for filename
    if args.all:
        scope = "all"
    elif args.season is not None and args.week is not None:
        scope = f"s{args.season}_w{args.week}"
    elif args.season is not None:
        scope = f"s{args.season}"
    elif args.week is not None:
        scope = f"w{args.week}"
    else:
        scope = "unspecified"

    out_path = PRED_DIR / f"predictions_{run_id}_{scope}.csv"
    out_df.to_csv(out_path, index=False)
    print(f"Saved predictions -> {out_path}")
     
    if args.to_db:
        # Ensure table exists, then upsert
        _ensure_predictions_table(engine)
        # include run_id in output frame for transparency (optional)
        out_df_db = out_df.copy()
        out_df_db["run_id"] = run_id
        _upsert_predictions(engine, out_df_db, run_id)

if __name__ == "__main__":
    main()
