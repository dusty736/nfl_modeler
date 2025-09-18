# modeling/Python/pregame_margin_abs_predgen.py
# Purpose: Load saved pregame ABSOLUTE MARGIN models from a specific run, generate predictions.
# Usage examples:
#   python3 modeling/Python/pregame_margin_abs_predgen.py --run-id latest --season 2025 --week 3
#   python3 modeling/Python/pregame_margin_abs_predgen.py --run-id 20250916T101200Z_ab12cd3 --all --with-pi --to-db
# Output: modeling/models/pregame_margin_abs/predictions/predictions_{run_id}_{scope}.csv

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

DB_NAME = "nfl"
DB_HOST = "localhost"
DB_PORT = 5432
DB_USER = "nfl_user"
DB_PASS = "nfl_pass"
SCHEMA_TABLE = "prod.game_level_modeling_tbl"  # schema-qualified table

TARGET = "abs_margin"  # actual absolute margin, may be missing for future games

# Never allow market inputs or post-game info
drop_market = ["spread_line", "spread_home"]
drop_for_margin_abs = [
    "home_score", "away_score",     # postgame
    "margin", "total_points",       # other labels/leaks
    "total_line",                   # market
    *drop_market
]
# Non-predictive/temporal ID columns (no time features)
drop_non_predictive = ["game_id", "kickoff"]

# Paths
SCRIPT_PATH = Path(__file__).resolve()
ROOT_DIR = SCRIPT_PATH.parent.parent                         # modeling/
SAVE_ROOT = ROOT_DIR / "models" / "pregame_margin_abs"       # modeling/models/pregame_margin_abs
RUNS_DIR = SAVE_ROOT / "runs"
PRED_DIR = SAVE_ROOT / "predictions"
PRED_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Helpers
# -----------------------------
def _find_latest_run(runs_dir: Path) -> str:
    if not runs_dir.exists():
        raise FileNotFoundError(f"No runs directory at: {runs_dir}")
    candidates = [d for d in runs_dir.iterdir() if d.is_dir()]
    if not candidates:
        raise FileNotFoundError(f"No runs found under: {runs_dir}")
    latest = max(candidates, key=lambda p: p.stat().st_mtime)
    return latest.name

def _load_models(run_dir: Path):
    """Load regression pipelines saved in training (LR_EN, RF, XGB)."""
    models_dir = run_dir / "models"
    if not models_dir.exists():
        raise FileNotFoundError(f"Models folder not found: {models_dir}")

    loaded = {}
    for name, fname in [("LR_EN","lr_en.joblib"), ("RF","rf.joblib"), ("XGB","xgb.joblib")]:
        p = models_dir / fname
        if p.exists():
            loaded[name] = joblib.load(p)

    if not loaded:
        raise FileNotFoundError(f"No model files found in {models_dir}")
    return loaded

def _load_conformal_summary(run_dir: Path):
    """Load conformal residual quantiles from training Step 3 (if present)."""
    path = run_dir / "tables" / "conformal_summary_margin_abs.json"
    if path.exists():
        with open(path, "r") as f:
            return json.load(f)
    return None

def _connect_engine():
    conn_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(conn_str)

def _fetch_data(engine, season: int | None, week: int | None, run_all: bool):
    base_sql = f"SELECT * FROM {SCHEMA_TABLE}"
    params = {}
    if run_all:
        sql = text(base_sql)
    else:
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
            raise ValueError("Specify --all or provide at least --season or --week.")
    return pd.read_sql_query(sql, engine, params=params)

def _prepare_features(df: pd.DataFrame):
    # If abs_margin not present but margin is, compute it (for convenience when scoring past rows)
    if "abs_margin" not in df.columns and "margin" in df.columns:
        df = df.copy()
        df["abs_margin"] = df["margin"].abs()

    injury_cols = [c for c in df.columns
                   if c.startswith("home_inj_") or c.startswith("away_inj_") or c.startswith("diff_inj_")]

    planned_drops = set(drop_for_margin_abs + drop_non_predictive + injury_cols)
    to_drop = [c for c in planned_drops if c in df.columns]

    X = df.drop(columns=[c for c in [TARGET] if c in df.columns] + to_drop, errors="ignore")
    return df, X, to_drop

def _check_expected_columns(pipeline, X: pd.DataFrame):
    """Warn if model expects columns that aren't present."""
    try:
        pre = pipeline.named_steps["preprocess"]
        expected = []
        for _, _, cols in pre.transformers_:
            if cols is None or cols == "drop":
                continue
            expected.extend(list(cols))
        missing = [c for c in expected if c not in X.columns]
        if missing:
            print(f"[WARN] Missing expected columns ({len(missing)}): {missing[:20]}{'...' if len(missing)>20 else ''}")
    except Exception as e:
        print("[INFO] Could not verify expected columns:", repr(e))

def _ensure_predictions_table(engine):
    ddl = text("""
    CREATE TABLE IF NOT EXISTS prod.pregame_margin_abs_predictions_tbl (
        season              INTEGER       NOT NULL,
        week                INTEGER       NOT NULL,
        game_id             TEXT          NOT NULL,
        home_team           TEXT,
        away_team           TEXT,
        pred_lr             DOUBLE PRECISION NOT NULL,
        pred_rf             DOUBLE PRECISION NOT NULL,
        pred_xgb            DOUBLE PRECISION NOT NULL,
        pred_vote           DOUBLE PRECISION NOT NULL,
        vote_pi80_lo        DOUBLE PRECISION,
        vote_pi80_hi        DOUBLE PRECISION,
        vote_pi90_lo        DOUBLE PRECISION,
        vote_pi90_hi        DOUBLE PRECISION,
        actual_abs_margin   DOUBLE PRECISION,
        run_id              TEXT          NOT NULL,
        created_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),
        PRIMARY KEY (game_id, run_id)
    );
    CREATE INDEX IF NOT EXISTS pregame_margin_abs_predictions_season_week_idx
        ON prod.pregame_margin_abs_predictions_tbl (season, week);
    """)
    with engine.begin() as conn:
        conn.execute(ddl)

def _upsert_predictions(engine, df_out: pd.DataFrame, run_id: str):
    """
    Upsert rows into prod.pregame_margin_abs_predictions_tbl using (game_id, run_id) as PK.
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
            "pred_lr": float(r.get("pred_lr")),
            "pred_rf": float(r.get("pred_rf")),
            "pred_xgb": float(r.get("pred_xgb")),
            "pred_vote": float(r.get("pred_vote")),
            "vote_pi80_lo": None if pd.isna(r.get("vote_pi80_lo")) else float(r.get("vote_pi80_lo")),
            "vote_pi80_hi": None if pd.isna(r.get("vote_pi80_hi")) else float(r.get("vote_pi80_hi")),
            "vote_pi90_lo": None if pd.isna(r.get("vote_pi90_lo")) else float(r.get("vote_pi90_lo")),
            "vote_pi90_hi": None if pd.isna(r.get("vote_pi90_hi")) else float(r.get("vote_pi90_hi")),
            "actual_abs_margin": None if ("actual_abs_margin" not in r or pd.isna(r.get("actual_abs_margin")))
                                   else float(r.get("actual_abs_margin")),
            "run_id": run_id,
        })

    sql = text("""
        INSERT INTO prod.pregame_margin_abs_predictions_tbl
            (season, week, game_id, home_team, away_team,
             pred_lr, pred_rf, pred_xgb, pred_vote,
             vote_pi80_lo, vote_pi80_hi, vote_pi90_lo, vote_pi90_hi,
             actual_abs_margin, run_id)
        VALUES
            (:season, :week, :game_id, :home_team, :away_team,
             :pred_lr, :pred_rf, :pred_xgb, :pred_vote,
             :vote_pi80_lo, :vote_pi80_hi, :vote_pi90_lo, :vote_pi90_hi,
             :actual_abs_margin, :run_id)
        ON CONFLICT (game_id, run_id) DO UPDATE
        SET
            season = EXCLUDED.season,
            week = EXCLUDED.week,
            home_team = EXCLUDED.home_team,
            away_team = EXCLUDED.away_team,
            pred_lr = EXCLUDED.pred_lr,
            pred_rf = EXCLUDED.pred_rf,
            pred_xgb = EXCLUDED.pred_xgb,
            pred_vote = EXCLUDED.pred_vote,
            vote_pi80_lo = EXCLUDED.vote_pi80_lo,
            vote_pi80_hi = EXCLUDED.vote_pi80_hi,
            vote_pi90_lo = EXCLUDED.vote_pi90_lo,
            vote_pi90_hi = EXCLUDED.vote_pi90_hi,
            actual_abs_margin = EXCLUDED.actual_abs_margin,
            created_at = now();
    """)

    with engine.begin() as conn:
        for rec in rows:
            conn.execute(sql, rec)

    print(f"Upserted {len(rows)} rows into prod.pregame_margin_abs_predictions_tbl")

# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Generate pregame absolute-margin predictions (soft-vote) from saved models.")
    ap.add_argument("--run-id", default="latest", help="Run ID under runs/. Use 'latest' to pick most recent.")
    ap.add_argument("--season", type=int, default=None, help="Season to predict (e.g., 2025).")
    ap.add_argument("--week", type=int, default=None, help="Week to predict (1..).")
    ap.add_argument("--all", action="store_true", help="Predict for all rows in the table (ignores season/week).")
    ap.add_argument("--with-pi", action="store_true", help="Attach conformal prediction intervals (if available).")
    ap.add_argument("--to-db", action="store_true", help="If set, upsert predictions into prod.pregame_margin_abs_predictions_tbl")
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

    # Load conformal summary (optional)
    conformal = _load_conformal_summary(run_dir)
    if args.with_pi and conformal is None:
        print("[WARN] --with-pi requested but conformal_summary_margin_abs.json not found; continuing without PIs.")

    engine = _connect_engine()
    df = _fetch_data(engine, args.season, args.week, args.all)
    if df.empty:
        print("No rows returned from the database for the given filters. Nothing to do.")
        return

    # Keep scheduling/ID columns for output
    out_cols = [c for c in ["season","week","game_id","home_team","away_team"] if c in df.columns]

    df, X, dropped = _prepare_features(df)
    print(f"Rows loaded: {df.shape[0]} | Features after drops: {X.shape[1]} | Dropped cols: {len(dropped)}")

    # Sanity check required columns vs model expectations (warn only)
    for m in models.values():
        _check_expected_columns(m, X)

    # Predict per model (clip to >= 0 for absolute margin)
    preds = {}
    for name in model_keys:
        try:
            p = models[name].predict(X)
            p = np.clip(p, 0, None)
            preds[name] = p
        except Exception as e:
            print(f"[WARN] Model {name} failed to predict: {e}")

    if not preds:
        raise RuntimeError("No models produced predictions. Aborting.")

    # Soft vote (simple average of available models)
    stacked = np.vstack([preds[k] for k in preds.keys()])
    vote = stacked.mean(axis=0)
    vote = np.clip(vote, 0, None)

    # Optional PIs using validation residual quantiles from training run
    vote_pi80_lo = vote_pi80_hi = vote_pi90_lo = vote_pi90_hi = None
    if args.with_pi and conformal is not None and "VOTE_SOFT" in conformal:
        try:
            q80 = float(conformal["VOTE_SOFT"]["pi_80"]["q"])
            q90 = float(conformal["VOTE_SOFT"]["pi_90"]["q"])
            vote_pi80_lo = np.clip(vote - q80, 0, None)
            vote_pi80_hi = vote + q80
            vote_pi90_lo = np.clip(vote - q90, 0, None)
            vote_pi90_hi = vote + q90
        except Exception as e:
            print("[WARN] Could not attach PIs from conformal summary:", repr(e))

    # Outcome if exists
    actual_abs = None
    if TARGET in df.columns:
        actual_abs = df[TARGET].astype(float)
    elif "margin" in df.columns:
        actual_abs = df["margin"].abs().astype(float)

    out_df = df[out_cols].copy()
    out_df["pred_lr"]  = preds.get("LR_EN", np.nan)
    out_df["pred_rf"]  = preds.get("RF", np.nan)
    out_df["pred_xgb"] = preds.get("XGB", np.nan)
    out_df["pred_vote"] = vote
    if actual_abs is not None:
        out_df["actual_abs_margin"] = actual_abs

    # Attach PI columns (vote only) if available
    if isinstance(vote_pi80_lo, np.ndarray):
        out_df["vote_pi80_lo"] = vote_pi80_lo
        out_df["vote_pi80_hi"] = vote_pi80_hi
    if isinstance(vote_pi90_lo, np.ndarray):
        out_df["vote_pi90_lo"] = vote_pi90_lo
        out_df["vote_pi90_hi"] = vote_pi90_hi

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
        _ensure_predictions_table(engine)
        out_df_db = out_df.copy()
        out_df_db["run_id"] = run_id
        _upsert_predictions(engine, out_df_db, run_id)

if __name__ == "__main__":
    main()
