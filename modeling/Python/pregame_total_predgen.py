#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pregame Total — Prediction Generator
Usage examples:
  python3 modeling/Python/pregame_total_predgen.py --run-id latest --all --with-pi --to-db
  python3 modeling/Python/pregame_total_predgen.py --run-id 20250916T230102Z_ab12cd3 --season 2025 --week 3 --model vote --with-pi
"""

import argparse, os, json, sys, re, math
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import joblib

from sqlalchemy import create_engine, text

# -----------------------------
# Constants / Paths
# -----------------------------
SCRIPT_PATH = Path(__file__).resolve()
ROOT_DIR    = SCRIPT_PATH.parent.parent                   # modeling/
SAVE_ROOT   = ROOT_DIR / "models" / "pregame_total"       # modeling/models/pregame_total
RUNS_DIR    = SAVE_ROOT / "runs"

DB_NAME = os.getenv("DB_NAME", "nfl")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "nfl_user")
DB_PASS = os.getenv("DB_PASS", "nfl_pass")

MODEL_TBL  = "prod.game_level_modeling_tbl"
GAMES_TBL  = "prod.games_tbl"   # only used to *optionally* fetch vegas total_line if you want it later
TARGET     = "total_points"     # not used as a feature here

SEED = 42
np.random.seed(SEED)

# -----------------------------
# Helpers
# -----------------------------
def _utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _resolve_run_id(run_id: str) -> str:
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
    conn_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
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
        raise FileNotFoundError(f"No models found in {models_dir}")
    return models

def _load_conformal(run_dir: Path, levels):
    """
    Returns dict like:
      {"lr": {"80": q, "90": q}, "rf": {...}, "xgb": {...}, "vote": {...}}
    Accepts either key scheme in file:
      LR_EN/RF/XGB/VOTE_SOFT or lr/rf/xgb/vote
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
                        if tag in raw[k_in]:
                            out[k_out][str(lev)] = float(raw[k_in][tag]["q"])
            # If we populated at least one, we’re good
            if any(out[m] for m in out):
                return out
        except Exception:
            pass
    print("[Warn] Could not reconcile keys in conformal_summary.json; PIs will be skipped.")
    return None

def _vote_from_available(preds: dict):
    arrs = [v for v in preds.values() if v is not None and isinstance(v, np.ndarray)]
    if not arrs:
        return None
    return np.nanmean(np.vstack(arrs), axis=0)

def _predict(models: dict, X: pd.DataFrame, want: str):
    # want in {"lr","rf","xgb","vote","all"}
    out = {"lr": None, "rf": None, "xgb": None, "vote": None}
    if want in ("lr","all") and "lr" in models:
        out["lr"] = models["lr"].predict(X)
    if want in ("rf","all") and "rf" in models:
        out["rf"] = models["rf"].predict(X)
    if want in ("xgb","all") and "xgb" in models:
        out["xgb"] = models["xgb"].predict(X)
    # vote uses whatever we actually computed above (or all available if want == "vote")
    if want == "vote":
        # compute from all available models
        tmp = {}
        for k in ("lr","rf","xgb"):
            if k in models:
                tmp[k] = models[k].predict(X)
        out["vote"] = _vote_from_available(tmp)
    elif want == "all":
        out["vote"] = _vote_from_available({k: out[k] for k in ("lr","rf","xgb")})
    else:
        # Specific single model requested: no vote unless we can form it from loaded preds
        pass
    return out

def _add_pis(df_out: pd.DataFrame, preds: dict, conf: dict, levels):
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
            df_out[f"{model_key}_pi_{lev}_lo"] = arr - q
            df_out[f"{model_key}_pi_{lev}_hi"] = arr + q
    return df_out

def _ensure_db_table(engine, levels):
    # Wide table; includes fixed columns for 80 & 90 by default
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE SCHEMA IF NOT EXISTS prod;
        """))
        # Build PI columns for LR/RF/XGB/VOTE and each level
        pi_cols = []
        for m in ("lr","rf","xgb","vote"):
            for lev in levels:
                pi_cols.append(f"{m}_pi_{lev}_lo DOUBLE PRECISION")
                pi_cols.append(f"{m}_pi_{lev}_hi DOUBLE PRECISION")
        pi_cols_sql = ",\n            ".join(pi_cols)
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
    if df.empty:
        print("[Info] Nothing to upsert.")
        return
    # Ensure table exists
    _ensure_db_table(engine, levels)

    # Build dynamic column list from df
    base_cols = ["season","week","game_id"]
    pred_cols = [c for c in df.columns if c.startswith("pred_")]
    pi_cols   = [c for c in df.columns if re.match(r"^(lr|rf|xgb|vote)_pi_\d+_(lo|hi)$", c)]
    cols = base_cols + pred_cols + pi_cols

    # Insert columns include run_id + cols
    insert_cols = ["run_id"] + cols
    placeholders = ", ".join([f":{c}" for c in insert_cols])

    # Build ON CONFLICT update set (exclude keys and run_id)
    update_cols = pred_cols + pi_cols
    set_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

    sql = text(f"""
        INSERT INTO prod.pregame_total_predictions_tbl ({", ".join(insert_cols)})
        VALUES ({placeholders})
        ON CONFLICT (season, week, game_id, run_id)
        DO UPDATE SET {set_clause};
    """)

    # Prepare rows as dicts
    records = []
    for _, r in df.iterrows():
        rec = {"run_id": run_id}
        for c in cols:
            rec[c] = r[c]
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

    if preds["lr"] is not None:   out["pred_lr"]   = preds["lr"]
    if preds["rf"] is not None:   out["pred_rf"]   = preds["rf"]
    if preds["xgb"] is not None:  out["pred_xgb"]  = preds["xgb"]
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

if __name__ == "__main__":
    main()
