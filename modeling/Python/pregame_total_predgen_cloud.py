#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pregame Total — Prediction Generator
Usage examples:
  python3 modeling/Python/pregame_total_predgen_cloud.py --run-id latest --all --with-pi --to-db
  python3 modeling/Python/pregame_total_predgen.py --run-id 20250916T230102Z_ab12cd3 --season 2025 --week 3 --model vote --with-pi
Output CSV:
  modeling/models/pregame_total/runs/{run_id}/predictions/total_predictions_{scope}.csv
"""

import argparse, os, json, sys, re, math
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import joblib

from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

# -----------------------------
# Constants / Paths
# -----------------------------
SCRIPT_PATH = Path(__file__).resolve()
ROOT_DIR    = SCRIPT_PATH.parent.parent                       # modeling/
SAVE_ROOT   = ROOT_DIR / "models" / "pregame_total"           # modeling/models/pregame_total
RUNS_DIR    = SAVE_ROOT / "runs"

DB_NAME = "nfl"
# Hardcoded Cloud SQL Instance Connection Name (assumed to be correct)
DB_HOST = "/tmp/nfl-modeling:europe-west2:nfl-pg-01" 
DB_PORT = 5432
DB_USER = "nfl_app"
DB_PASS = "CHOOSE_A_STRONG_PASS"

MODEL_TBL   = "prod.game_level_modeling_tbl"
GAMES_TBL   = "prod.games_tbl"    
TARGET      = "total_points"      

SEED = 42
np.random.seed(SEED)

# Regex for Prediction Interval columns (for robust filtering and column matching)
PI_COL_PATTERN = re.compile(r"^(lr|rf|xgb|vote)_pi_\d+_(lo|hi)$")

# -----------------------------
# Helpers
# -----------------------------
def _utc_now():
    """Returns current time in UTC, formatted for database insertion."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _resolve_run_id(run_id: str) -> str:
    """Finds the latest run ID if 'latest' is specified."""
    if run_id != "latest":
        return run_id
    if not RUNS_DIR.exists():
        raise FileNotFoundError(f"No runs directory at {RUNS_DIR}")
    dirs = [p.name for p in RUNS_DIR.iterdir() if p.is_dir()]
    if not dirs:
        raise FileNotFoundError("No runs found under models/pregame_total/runs/")
    # Sort lexicographically; your run ids are ISO-like timestamps so this works
    dirs.sort()
    return dirs[-1]

def _engine():
    """
    Establishes SQLAlchemy engine connection using the hardcoded Unix socket path (DB_HOST).
    This bypasses environment variable checks for guaranteed use of the specified socket.
    """
    # Environment variable overrides for credentials (using constants as defaults)
    db = os.getenv("DB_NAME", DB_NAME)
    user = os.getenv("DB_USER", DB_USER)
    pwd = os.getenv("DB_PASS", DB_PASS)
    
    # Use the hardcoded constant DB_HOST for the Cloud SQL Unix socket path
    socket_path = DB_HOST 

    if socket_path and socket_path != 'None':
        # Unix socket connection: This format is required by SQLAlchemy/psycopg2 
        # for connecting via the Cloud SQL Proxy's named pipe.
        conn_str = (
            f"postgresql+psycopg2://{user}:{pwd}@"
            f"/{db}?host={socket_path}"
        )
        print(f"[Info] Connecting via HARDCODED Cloud SQL Unix socket: host={socket_path}")
    else:
        # Failsafe if DB_HOST is invalid (shouldn't happen if hardcoded)
        raise RuntimeError(
            "DB_HOST is invalid. Ensure the full Cloud SQL instance connection name is set correctly."
        )

    return create_engine(conn_str)

def _load_slice(engine, season=None, week=None, use_all=False):
    # Selection: --all => season >= 2025; else by season[/week]
    if use_all:
        where = "season >= :smin"
        params = {"smin": 2025}
    elif season is not None:
        if week is not None:
            where = "season = :sn AND week = :wk"
            params = {"sn": season, "wk": week}
        else:
            where = "season = :sn"
            params = {"sn": season}
    else:
        raise ValueError("Provide --all OR --season [--week].")

    q = text(f"SELECT * FROM {MODEL_TBL} WHERE {where}")
    df = pd.read_sql_query(q, engine, params=params)
    if df.empty:
        print("[Info] Query returned 0 rows — nothing to predict.")
    return df

def _load_models(run_dir: Path):
    """Loads trained models (LR, RF, XGB) from the run directory."""
    models_dir = run_dir / "models"
    paths = {
        "lr":  models_dir / "lr_en.joblib",
        "rf":  models_dir / "rf.joblib",
        "xgb": models_dir / "xgb.joblib",
    }
    models = {}
    for key, p in paths.items():
        if p.exists():
            try:
                models[key] = joblib.load(p)
            except Exception as e:
                print(f"[Warn] Failed to load {key} model at {p}: {e}")
    if not models:
        # NOTE: This will raise an error if no models were loaded.
        raise FileNotFoundError(f"No models found in {models_dir}") 
    return models

def _load_conformal(run_dir: Path, levels):
    """
    Returns dict like:
      {"lr": {"80": q, "90": q}, "rf": {...}, "xgb": {...}, "vote": {...}}
    """
    fp1 = run_dir / "tables" / "conformal_summary.json"
    fp2 = run_dir / "metrics" / "conformal_summary.json"
    fp = fp1 if fp1.exists() else (fp2 if fp2.exists() else None)
    if fp is None:
        print("[Warn] conformal_summary.json not found; PIs will be skipped.")
        return None
    try:
        raw = json.loads(Path(fp).read_text())
    except Exception as e:
        print(f"[Warn] Could not parse conformal_summary.json ({e}); PIs will be skipped.")
        return None

    # Normalize to short keys
    out = {"lr": {}, "rf": {}, "xgb": {}, "vote": {}}
    key_map_options = [
        {"LR_EN": "lr", "RF": "rf", "XGB": "xgb", "VOTE_SOFT": "vote"},
        {"lr": "lr", "rf": "rf", "xgb": "xgb", "VOTE_SOFT": "vote", "vote":"vote"},
    ]
    for km in key_map_options:
        try:
            for k_in, k_out in km.items():
                if k_in in raw:
                    for lev in levels:
                        tag = f"pi_{lev}"
                        levs = str(lev)
                        if tag in raw[k_in]:
                            out[k_out][levs] = float(raw[k_in][tag]["q"])
            # If we populated at least one, we’re good
            if any(out[m] for m in out):
                return out
        except Exception:
            pass
    print("[Warn] Could not reconcile keys in conformal_summary.json; PIs will be skipped.")
    return None

def _vote_from_available(preds: dict):
    """Calculates the mean prediction from all available model arrays."""
    arrs = [v for v in preds.values() if v is not None and isinstance(v, np.ndarray)]
    if not arrs:
        return None
    return np.nanmean(np.vstack(arrs), axis=0)

def _predict(models: dict, X: pd.DataFrame, want: str):
    """Generates predictions for the requested model(s)."""
    # want in {"lr","rf","xgb","vote","all"}
    out = {"lr": None, "rf": None, "xgb": None, "vote": None}
    if want in ("lr","all") and "lr" in models:
        out["lr"] = models["lr"].predict(X)
    if want in ("rf","all") and "rf" in models:
        out["rf"] = models["rf"].predict(X)
    if want in ("xgb","all") and "xgb" in models:
        out["xgb"] = models["xgb"].predict(X)
    
    # Vote logic
    if want == "vote":
        # Compute from all available models (need to re-predict X if they weren't selected above)
        tmp = {}
        for k in ("lr","rf","xgb"):
            if k in models:
                tmp[k] = models[k].predict(X)
        out["vote"] = _vote_from_available(tmp)
    elif want == "all":
        # Compute vote from the predictions generated above (if they were generated)
        out["vote"] = _vote_from_available({k: out[k] for k in ("lr","rf","xgb")})
    else:
        # Specific single model requested: no vote
        pass
    return out

def _add_pis(df_out: pd.DataFrame, preds: dict, conf: dict, levels):
    """Adds prediction intervals (PIs) to the output DataFrame."""
    if conf is None:
        return df_out
    # For each model present, add lo/hi columns for requested levels
    for model_key, arr in preds.items():
        if arr is None: 
            continue
        if model_key not in conf or not conf[model_key]:
            continue
        for lev in levels:
            levs = str(lev)
            if levs not in conf[model_key]:
                continue
            q = conf[model_key][levs]
            # Column names are NOT prefixed with 'pred_' for PI columns in this original code
            df_out[f"{model_key}_pi_{lev}_lo"] = arr - q
            df_out[f"{model_key}_pi_{lev}_hi"] = arr + q
    return df_out

def _ensure_db_table(engine, levels):
    """Ensures the predictions table exists, creating it if necessary."""
    # Wide table; includes fixed columns for 80 & 90 by default
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE SCHEMA IF NOT EXISTS prod;
        """))
        # Build PI columns for LR/RF/XGB/VOTE and each level
        pi_cols = []
        # NOTE: The database schema expects PI columns to be prefixed with 'pred_' 
        for m in ("lr","rf","xgb","vote"):
            for lev in levels:
                pi_cols.append(f"pred_{m}_pi_{lev}_lo DOUBLE PRECISION") 
                pi_cols.append(f"pred_{m}_pi_{lev}_hi DOUBLE PRECISION")
        pi_cols_sql = ",\n             ".join(pi_cols)
        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS prod.pregame_total_predictions_tbl (
            run_id TEXT NOT NULL,
            season INTEGER NOT NULL,
            week INTEGER NOT NULL,
            game_id TEXT NOT NULL,
            pred_lr DOUBLE PRECISION,
            pred_rf DOUBLE PRECISION,
            pred_xgb DOUBLE PRECISION,
            pred_vote DOUBLE PRECISION,
            created_at TIMESTAMPTZ DEFAULT now(),
            {pi_cols_sql},
            PRIMARY KEY (season, week, game_id, run_id)
        );
        """))

def _upsert(engine, df: pd.DataFrame, run_id: str, levels):
    """Performs a database UPSERT (INSERT ON CONFLICT UPDATE) for the prediction results."""
    if df.empty:
        print("[Info] Nothing to upsert.")
        return
    # Ensure table exists
    _ensure_db_table(engine, levels)

    # Standard pred columns: 'pred_lr', 'pred_rf', etc.
    pred_cols = [c for c in df.columns if c.startswith("pred_")] 
    
    # Find PI columns that were generated (without 'pred_')
    raw_pi_cols = [c for c in df.columns if PI_COL_PATTERN.search(c)]
    
    # Map raw PI column names (e.g., 'vote_pi_80_lo') to DB column names (e.g., 'pred_vote_pi_80_lo')
    db_pi_cols = [f"pred_{c}" for c in raw_pi_cols]
    
    # Prepare the DataFrame for insertion by renaming columns to match the DB schema
    df_to_insert = df.rename(columns={c: f"pred_{c}" for c in raw_pi_cols})
    
    # Build list of all columns to insert (DB names)
    base_cols = ["season","week","game_id"]
    insert_db_cols = base_cols + pred_cols + db_pi_cols # Columns derived from DataFrame
    
    # The full list of placeholders and columns for the SQL statement MUST include 'run_id'
    insert_cols_full = ["run_id"] + insert_db_cols 
    placeholders = ", ".join([f":{c}" for c in insert_cols_full])

    # Build ON CONFLICT update set
    update_cols = pred_cols + db_pi_cols
    
    # FIX: Prevent SQL Syntax Error if update_cols is empty
    if not update_cols:
        on_conflict_clause = "ON CONFLICT (season, week, game_id, run_id) DO NOTHING"
    else:
        set_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
        on_conflict_clause = f"ON CONFLICT (season, week, game_id, run_id) DO UPDATE SET {set_clause}"


    sql = text(f"""
        INSERT INTO prod.pregame_total_predictions_tbl ({", ".join(insert_cols_full)})
        VALUES ({placeholders})
        {on_conflict_clause};
    """)

    # Prepare rows as dicts from the RENAMED DataFrame
    records = []
    for _, r in df_to_insert.iterrows():
        rec = {"run_id": run_id} # Initialize 'run_id'
        # FIX: Iterate only over columns that are in the DataFrame (insert_db_cols),
        # since 'run_id' is already set above.
        for c in insert_db_cols: 
             # Handle NaN/None conversion
            val = r[c]
            if pd.isna(val):
                rec[c] = None
            elif isinstance(val, (np.float32, np.float64)):
                rec[c] = float(val)
            else:
                rec[c] = val
        records.append(rec)

    with engine.begin() as conn:
        conn.execute(sql, records)

# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Pregame Total — Prediction Generator")
    ap.add_argument("--run-id", default="latest", help="Run id under models/pregame_total/runs (or 'latest')")
    sel = ap.add_mutually_exclusive_group(required=True)
    sel.add_argument("--all", action="store_true", help="Predict for all season >= 2025")
    sel.add_argument("--season", type=int, help="Season to predict")
    ap.add_argument("--week", type=int, help="Week to predict (optional with --season)")
    ap.add_argument("--model", choices=["lr","rf","xgb","vote","all"], default="vote",
                     help="Which model(s) to score (default: vote)")
    ap.add_argument("--with-pi", action="store_true", help="Attach conformal prediction intervals if available")
    ap.add_argument("--pi-levels", default="80,90", help="Comma list of PI coverages, e.g. '80,90'")
    ap.add_argument("--to-db", action="store_true", help="Upsert results to prod.pregame_total_predictions_tbl")
    ap.add_argument("--dry-run", action="store_true", help="Do not write CSV/DB; just print a sample")
    args = ap.parse_args()

    try:
        run_id = _resolve_run_id(args.run_id)
        run_dir = RUNS_DIR / run_id
        models = _load_models(run_dir)

        # Parse PI levels
        levels = [int(x.strip()) for x in args.pi_levels.split(",") if x.strip()]

        engine = _engine()
        df = _load_slice(engine, season=args.season, week=args.week, use_all=args.all)
        if df.empty:
            print("No rows to predict. Exiting.")
            return

        # Build feature frame: the fitted pipelines will select their own columns (remainder='drop').
        # We *can* drop the target to avoid accidental leakage into a custom transform.
        if TARGET in df.columns:
            X = df.drop(columns=[TARGET])
        else:
            X = df.copy()

        # Predict
        preds = _predict(models, X, args.model)

        # Assemble output
        out_cols = ["game_id","season","week"]
        out = pd.DataFrame(df[out_cols].copy())

        if preds["lr"] is not None:  out["pred_lr"]   = preds["lr"]
        if preds["rf"] is not None:  out["pred_rf"]   = preds["rf"]
        if preds["xgb"] is not None: out["pred_xgb"]  = preds["xgb"]
        if preds["vote"] is not None: out["pred_vote"] = preds["vote"]

        # Add PIs
        if args.with_pi:
            conf = _load_conformal(run_dir, levels)
            out = _add_pis(out, preds, conf, levels)

        # File naming
        if args.all:
            scope = "all"
        elif args.week is not None:
            scope = f"{args.season}_w{int(args.week):02d}"
        else:
            scope = f"{args.season}"

        pred_dir = run_dir / "predictions"
        pred_dir.mkdir(parents=True, exist_ok=True)
        out_path = pred_dir / f"total_predictions_{scope}.csv"

        # Dry-run print
        print(f"\nRun: {run_id}")
        # Note: PI columns (vote_pi...) do not start with 'pred_' in the DF, 
        # so this list should only show the core prediction columns.
        print(f"Rows: {len(out)} | Models included: {[c for c in out.columns if c.startswith('pred_')]}")
        print(out.head(10).to_string(index=False))

        if not args.dry_run:
            # Save CSV
            out.to_csv(out_path, index=False)
            print(f"Saved predictions -> {out_path}")

            # Upsert to DB
            if args.to_db:
                _upsert(engine, out, run_id, levels) 
                print("Upserted predictions into prod.pregame_total_predictions_tbl")
    except Exception as e:
        import sys
        print(f"[ERROR] Script failed: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
