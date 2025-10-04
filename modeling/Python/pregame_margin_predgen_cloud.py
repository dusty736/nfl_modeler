# Purpose: Generate pregame margin-bin predictions from saved models and optionally upsert to DB.
# Usage:
#   python3 modeling/Python/pregame_margin_predgen.py --run-id latest --all --to-db
#   python3 modeling/Python/pregame_margin_predgen.py --run-id latest --season 2025 --week 3 --to-db
# Output CSV:
#   modeling/models/pregame_margin_bins/predictions/predictions_{run_id}_{scope}.csv

import argparse
import os
from pathlib import Path
from datetime import datetime, timezone
import json
import joblib
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

# -----------------------------
# Config (mirror training)
# -----------------------------
SEED = 42

DB_NAME = "nfl"
# HARDCODED: This is the full path to the named Unix socket file created by the proxy
# This matches the working configuration used in the other script.
DB_HOST = "/tmp/nfl-modeling:europe-west2:nfl-pg-01"
DB_PORT = 5432
DB_USER = "nfl_app"
DB_PASS = "CHOOSE_A_STRONG_PASS"
SCHEMA_TABLE = "prod.game_level_modeling_tbl"  # training table

# Strictly no market inputs or post-game info
drop_market = ["spread_line", "spread_home", "spread_covered", "total_line"]
drop_leakage = ["home_score", "away_score", "total_points", "margin", "abs_margin", "home_win"]
drop_non_predictive = ["game_id", "kickoff"]

BIN_ORDER = ["coin_flip", "one_score", "two_scores", "blowout"]

# Paths
SCRIPT_PATH = Path(__file__).resolve()
ROOT_DIR   = SCRIPT_PATH.parent.parent                       # modeling/
SAVE_ROOT  = ROOT_DIR / "models" / "pregame_margin_bins"     # modeling/models/pregame_margin_bins
RUNS_DIR   = SAVE_ROOT / "runs"
PRED_DIR   = SAVE_ROOT / "predictions"
PRED_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Helpers
# -----------------------------
def _utc_now() -> str:
    """Returns current time in UTC, formatted for database insertion."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _find_latest_run(runs_dir: Path) -> str:
    """Finds the name of the most recently modified run directory."""
    if not runs_dir.exists():
        raise FileNotFoundError(f"No runs directory at: {runs_dir}")
    candidates = [d for d in runs_dir.iterdir() if d.is_dir()]
    if not candidates:
        raise FileNotFoundError(f"No runs found under: {runs_dir}")
    latest = max(candidates, key=lambda p: p.stat().st_mtime)
    return latest.name

def _load_models(run_dir: Path):
    """
    Load models saved by training (LR, RF, XGB).
    Models are expected in {run_dir}/models/{model_name}.joblib
    """
    models_dir = run_dir / "models"
    if not models_dir.exists():
        raise FileNotFoundError(f"Models folder not found: {models_dir}")

    loaded = {}
    paths = {
        "LR":  models_dir / "lr_clf.joblib",
        "RF":  models_dir / "rf_clf.joblib",
        "XGB": models_dir / "xgb_clf.joblib",
    }
    for name, p in paths.items():
        if p.exists():
            try:
                loaded[name] = joblib.load(p)
            except Exception as e:
                print(f"[WARN] Could not load {name}: {e}")

    if not loaded:
        raise FileNotFoundError(f"No model files found in {models_dir}")
    return loaded

def _connect_engine():
    """
    Establishes SQLAlchemy engine connection using the hardcoded Unix socket path.
    Bypasses faulty environment variable checks and TCP fallbacks.
    """
    # env overrides (Docker/CI friendly)
    db = os.getenv("DB_NAME", DB_NAME)
    user = os.getenv("DB_USER", DB_USER)
    pwd = os.getenv("DB_PASS", DB_PASS)
    
    # Use the hardcoded socket path as the host
    socket_path = DB_HOST 

    if socket_path and socket_path != 'None':
        # Unix socket connection: This format is required by SQLAlchemy/psycopg2 
        # for connecting via the Cloud SQL Proxy's named pipe.
        # Format: postgresql+psycopg2://<user>:<pass>@/<db>?host=<full_socket_path>
        conn_str = (
            f"postgresql+psycopg2://{user}:{pwd}@"
            f"/{db}?host={socket_path}"
        )
        print(f"[Info] Connecting via HARDCODED Cloud SQL Unix socket: host={socket_path}")
    else:
        # Failsafe if the hardcoded path is somehow invalid
        raise RuntimeError(
            "Connection path (DB_HOST) is invalid. Ensure the full Cloud SQL "
            "instance connection name is set correctly."
        )

    return create_engine(conn_str)

def _fetch_data(engine, season: int | None, week: int | None, run_all: bool):
    """Fetches data from the modeling table based on season/week filters or all rows."""
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
    """
    Matches training: drop market/leak/non-predictive features and injury columns.
    Returns feature DataFrame X and a list of dropped columns.
    """
    injury_cols = [c for c in df.columns
                     if c.startswith("home_inj_") or c.startswith("away_inj_") or c.startswith("diff_inj_")]
    planned_drops = set(drop_market + drop_leakage + drop_non_predictive + injury_cols)
    # Ensure 'abs_margin' is dropped from features if present, but kept in df_raw for actuals
    to_drop = [c for c in planned_drops if c in df.columns]
    X = df.drop(columns=[c for c in ["abs_margin"] if c in df.columns] + to_drop, errors="ignore")
    return X, to_drop

def _check_expected_columns(pipeline, X: pd.DataFrame):
    """Warn if a pipeline's preprocessing step expects columns that aren't present."""
    try:
        # Assuming the pipeline has a 'preprocess' step, typically a ColumnTransformer
        pre = pipeline.named_steps.get("preprocess")
        expected = []
        if pre is not None and hasattr(pre, "transformers_"):
            for _, _, cols in pre.transformers_:
                if cols is None or cols == "drop":
                    continue
                expected.extend(list(cols))
        missing = [c for c in expected if c not in X.columns]
        if missing:
            print(f"[WARN] Missing expected columns for this pipeline ({len(missing)}): "
                    f"{missing[:20]}{'...' if len(missing)>20 else ''}")
    except Exception as e:
        print("[INFO] Could not verify expected columns:", repr(e))

def _to_bin_label(abs_margin: float | None) -> str | None:
    """Converts absolute margin to the corresponding bin label (target variable)."""
    if abs_margin is None or pd.isna(abs_margin):
        return None
    try:
        a = float(abs_margin)
    except Exception:
        return None
    
    if a <= 3:      return "coin_flip"
    if a <= 8:      return "one_score"
    if a <= 16:     return "two_scores"
    return "blowout"

def _ensure_bins_predictions_table(engine):
    """
    Ensures the predictions table exists, has necessary columns (like game_id),
    and enforces a unique index for upsert operations.
    """
    ddl = text("""
      CREATE TABLE IF NOT EXISTS prod.pregame_margin_bins_preds_tbl (
        predicted_at_utc           timestamptz NOT NULL,
        run_id                     text        NOT NULL,
        model_name                 text        NOT NULL,
        season                     int         NOT NULL,
        week                       int         NOT NULL,
        home_team                  text        NOT NULL,
        away_team                  text        NOT NULL,
        game_id                    text        NULL,
        season_type                text        NULL,
        game_type                  text        NULL,
        p_coin_flip                double precision NOT NULL,
        p_one_score                double precision NOT NULL,
        p_two_scores               double precision NOT NULL,
        p_blowout                  double precision NOT NULL,
        predicted_bin              text        NOT NULL,
        predicted_bin_confidence   double precision NOT NULL,
        closeness_index            double precision NOT NULL,
        actual_abs_margin          double precision NULL,
        true_bin                   text NULL,
        is_final                   boolean NULL,
        predicted_correct          boolean NULL
      );
    """)

    # Note: game_id is set to NULL initially but will be populated during upsert if available.
    # The unique index should not include game_id as it would violate the intent of ON CONFLICT.
    create_unique = text("""
      CREATE UNIQUE INDEX IF NOT EXISTS pregame_margin_bins_preds_uniq
        ON prod.pregame_margin_bins_preds_tbl (season, week, home_team, away_team, model_name);
    """)

    # Backfill column for tables that predated this schema change
    alter_add_game_id = text("""
      ALTER TABLE prod.pregame_margin_bins_preds_tbl
      ADD COLUMN IF NOT EXISTS game_id text NULL;
    """)

    # Optional helper index
    idx_szn_wk = text("""
      CREATE INDEX IF NOT EXISTS pregame_margin_bins_preds_szn_wk_idx
        ON prod.pregame_margin_bins_preds_tbl (season, week);
    """)

    with engine.begin() as conn:
        conn.execute(ddl)
        conn.execute(alter_add_game_id)  # <-- ensure column exists if table predated this change
        conn.execute(create_unique)
        conn.execute(idx_szn_wk)

def _upsert_bins_predictions(engine, df_out: pd.DataFrame):
    """
    Performs a database UPSERT (INSERT ON CONFLICT UPDATE) for the prediction results.
    Includes a DELETE+INSERT fallback if the unique index is missing.
    """
    if df_out.empty:
        print("[INFO] No rows to upsert.")
        return

    sql_upsert = text("""
        INSERT INTO prod.pregame_margin_bins_preds_tbl (
          predicted_at_utc, run_id, model_name,
          season, week, home_team, away_team, game_id, season_type, game_type,
          p_coin_flip, p_one_score, p_two_scores, p_blowout,
          predicted_bin, predicted_bin_confidence, closeness_index,
          actual_abs_margin, true_bin, is_final, predicted_correct
        )
        VALUES (
          :predicted_at_utc, :run_id, :model_name,
          :season, :week, :home_team, :away_team, :game_id, :season_type, :game_type,
          :p_coin_flip, :p_one_score, :p_two_scores, :p_blowout,
          :predicted_bin, :predicted_bin_confidence, :closeness_index,
          :actual_abs_margin, :true_bin, :is_final, :predicted_correct
        )
        ON CONFLICT (season, week, home_team, away_team, model_name) DO UPDATE SET
          predicted_at_utc          = EXCLUDED.predicted_at_utc,
          run_id                    = EXCLUDED.run_id,
          game_id                   = EXCLUDED.game_id,
          p_coin_flip               = EXCLUDED.p_coin_flip,
          p_one_score               = EXCLUDED.p_one_score,
          p_two_scores              = EXCLUDED.p_two_scores,
          p_blowout                 = EXCLUDED.p_blowout,
          predicted_bin             = EXCLUDED.predicted_bin,
          predicted_bin_confidence  = EXCLUDED.predicted_bin_confidence,
          closeness_index           = EXCLUDED.closeness_index,
          actual_abs_margin         = EXCLUDED.actual_abs_margin,
          true_bin                  = EXCLUDED.true_bin,
          is_final                  = EXCLUDED.is_final,
          predicted_correct         = EXCLUDED.predicted_correct;
    """)

    # Fallback SQL (DELETE then INSERT)
    sql_delete = text("""
        DELETE FROM prod.pregame_margin_bins_preds_tbl
        WHERE season=:season AND week=:week
          AND home_team=:home_team AND away_team=:away_team
          AND model_name=:model_name
    """)
    sql_insert = text("""
        INSERT INTO prod.pregame_margin_bins_preds_tbl (
          predicted_at_utc, run_id, model_name,
          season, week, home_team, away_team, game_id, season_type, game_type,
          p_coin_flip, p_one_score, p_two_scores, p_blowout,
          predicted_bin, predicted_bin_confidence, closeness_index,
          actual_abs_margin, true_bin, is_final, predicted_correct
        )
        VALUES (
          :predicted_at_utc, :run_id, :model_name,
          :season, :week, :home_team, :away_team, :game_id, :season_type, :game_type,
          :p_coin_flip, :p_one_score, :p_two_scores, :p_blowout,
          :predicted_bin, :predicted_bin_confidence, :closeness_index,
          :actual_abs_margin, :true_bin, :is_final, :predicted_correct
        )
    """)

    # Convert rows to plain dicts (NaN -> None; numpy types -> native types)
    rows = []
    for _, r in df_out.iterrows():
        rec = {}
        for k, v in r.to_dict().items():
            if isinstance(v, (np.bool_, bool)):
                rec[k] = bool(v)
            elif pd.isna(v):
                rec[k] = None
            else:
                rec[k] = v
        rows.append(rec)

    # Try fast path first (ON CONFLICT)
    try:
        with engine.begin() as conn:
            for rec in rows:
                conn.execute(sql_upsert, rec)
        print(f"Upserted {len(rows)} rows into prod.pregame_margin_bins_preds_tbl (ON CONFLICT).")
    except ProgrammingError as e:
        msg = str(e).lower()
        if "no unique or exclusion constraint" in msg or "duplicate key value violates unique constraint" in msg:
            print("[WARN] ON CONFLICT unavailable (missing unique index or similar issue). Falling back to DELETE+INSERT.")
            # Fallback path
            with engine.begin() as conn:
                for rec in rows:
                    conn.execute(sql_delete, {
                        "season": rec["season"], "week": rec["week"],
                        "home_team": rec["home_team"], "away_team": rec["away_team"],
                        "model_name": rec["model_name"],
                    })
                    conn.execute(sql_insert, rec)
            print(f"Upserted {len(rows)} rows via DELETE+INSERT fallback.")
        else:
            raise

def _pick_best_model(run_dir: Path, metric="QWK") -> str:
    """
    Chooses the best performing model based on a specified metric (default QWK)
    from the run's test metrics CSV. Falls back to ENSEMBLE.
    """
    try:
        tpath = run_dir / "tables" / "test_metrics_all_models.csv"
        tdf = pd.read_csv(tpath, index_col=0)
        if metric in tdf.columns and not tdf.empty:
            return str(tdf[metric].idxmax())
    except Exception as e:
        print(f"[WARN] Could not determine BEST model from metrics: {e}")
    return "ENSEMBLE"

# -----------------------------
# Main Execution
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Generate pregame margin-bin predictions from saved models.")
    ap.add_argument("--run-id", default="latest", help="Run ID under runs/. Use 'latest' to pick most recent.")
    ap.add_argument("--season", type=int, default=None, help="Season to predict (e.g., 2025).")
    ap.add_argument("--week", type=int, default=None, help="Week to predict (1..).")
    ap.add_argument("--all", action="store_true", help="Predict for all rows in the table (ignores season/week).")
    ap.add_argument("--to-db", action="store_true", help="Upsert predictions into prod.pregame_margin_bins_preds_tbl.")
    ap.add_argument("--model", type=str, default="ENSEMBLE",
                    help="ENSEMBLE | LR | RF | XGB | BEST (by QWK in tables/test_metrics_all_models.csv)")
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

    # Resolve chosen model
    chosen = args.model.upper()
    if chosen == "BEST":
        chosen = _pick_best_model(run_dir, metric="QWK")
        print(f"[INFO] BEST model by QWK => {chosen}")

    engine = _connect_engine()
    df_raw = _fetch_data(engine, args.season, args.week, args.all)
    if df_raw.empty:
        print("No rows returned from the database for the given filters. Nothing to do.")
        return

    # Keep meta columns for output
    meta_cols = [c for c in ["season","week","game_id","home_team","away_team","season_type","game_type"]
                 if c in df_raw.columns]
    out_meta = df_raw[meta_cols].copy()

    # Features
    X, dropped = _prepare_features(df_raw)
    print(f"Rows loaded: {df_raw.shape[0]} | Raw columns: {df_raw.shape[1]} | Dropped cols: {len(dropped)}")

    # Sanity check: Ensure features match what the model expects
    for m in models.values():
        _check_expected_columns(m, X)

    # Predict probabilities
    def _predict_proba(model):
        """Generates probabilities, aligned to BIN_ORDER, from a single model."""
        return model.predict_proba(X)  # (n, 4) aligned to BIN_ORDER from training

    if chosen == "ENSEMBLE":
        probas = []
        for name, mdl in models.items():
            try:
                # Assuming all models fit the same input feature set (X)
                probas.append(_predict_proba(mdl))
            except Exception as e:
                print(f"[WARN] Model {name} failed to predict: {e}")
        if not probas:
            raise RuntimeError("No models produced probabilities. Aborting.")
        # Ensemble: mean of probabilities across all successfully loaded models
        P = np.mean(np.stack(probas, axis=0), axis=0)
        model_name_for_output = "ENSEMBLE"
    else:
        if chosen not in models:
            raise ValueError(f"Requested model '{chosen}' not found. Available: {model_keys} + ENSEMBLE/BEST")
        P = _predict_proba(models[chosen])
        model_name_for_output = chosen

    # --- Build Output Frame ---

    # Predicted bin and confidence
    pred_idx = P.argmax(axis=1)
    out_df = out_meta.copy()
    for i, b in enumerate(BIN_ORDER):
        out_df[f"p_{b}"] = P[:, i]
    out_df["predicted_bin"] = [BIN_ORDER[i] for i in pred_idx]
    out_df["predicted_bin_confidence"] = P.max(axis=1)
    # Custom metric: Closeness Index (0.0 to 1.0)
    out_df["closeness_index"] = out_df["p_coin_flip"] + 0.5 * out_df["p_one_score"]

    # Actuals if available (requires 'abs_margin' column in raw data)
    if "abs_margin" in df_raw.columns:
        out_df["actual_abs_margin"] = df_raw["abs_margin"].astype(float)
        out_df["true_bin"] = out_df["actual_abs_margin"].apply(_to_bin_label)
        out_df["is_final"] = out_df["true_bin"].notna()
        out_df["predicted_correct"] = np.where(out_df["is_final"],
                                               (out_df["true_bin"].astype(str) == out_df["predicted_bin"]),
                                               np.nan)
    else:
        # Fill with NaN if actuals are not in the raw data (e.g., predicting future games)
        out_df["actual_abs_margin"] = np.nan
        out_df["true_bin"] = np.nan
        out_df["is_final"] = np.nan
        out_df["predicted_correct"] = np.nan

    # Run metadata
    out_df.insert(0, "model_name", model_name_for_output)
    out_df.insert(0, "run_id", run_id)
    out_df.insert(0, "predicted_at_utc", _utc_now())

    # Determine scope for filename
    if args.all:
        scope = "all"
    elif args.season is not None and args.week is not None:
        scope = f"s{args.season}_w{int(args.week):02d}"
    elif args.season is not None:
        scope = f"s{args.season}"
    elif args.week is not None:
        scope = f"w{int(args.week)}"
    else:
        scope = "unspecified"

    # Save to CSV
    out_path = PRED_DIR / f"predictions_{run_id}_{scope}.csv"
    out_df.to_csv(out_path, index=False)
    print(f"Saved predictions -> {out_path}")

    # Upsert to DB
    if args.to_db:
        # Ensure table + unique index exist before upsert
        _ensure_bins_predictions_table(engine)
        cols_for_db = [
            "predicted_at_utc","run_id","model_name",
            "season","week","home_team","away_team","game_id","season_type","game_type",
            "p_coin_flip","p_one_score","p_two_scores","p_blowout",
            "predicted_bin","predicted_bin_confidence","closeness_index",
            "actual_abs_margin","true_bin","is_final","predicted_correct"
        ]
        missing = [c for c in ["season","week","home_team","away_team"] if c not in out_df.columns]
        if missing:
            raise RuntimeError(f"Missing required columns for DB upsert: {missing}")
        _upsert_bins_predictions(engine, out_df[cols_for_db])

if __name__ == "__main__":
    main()
