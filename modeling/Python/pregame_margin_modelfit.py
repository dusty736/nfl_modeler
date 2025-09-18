# STEP 1 — Absolute Margin
# Load data, define target/exclusions, make time-aware split, fit a baseline regressor.
# Prints baseline metrics and dataset shapes. Saves config + baseline pipeline + registry row.

import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np
import os, sys, json, csv, joblib, subprocess, re
from pathlib import Path
from datetime import datetime, timezone

pd.options.display.max_columns = 200

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.dummy import DummyRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

# -----------------------------
# Run scaffolding
# -----------------------------
SCRIPT_PATH = Path(__file__).resolve()
ROOT_DIR    = SCRIPT_PATH.parent.parent                    # modeling/
SAVE_ROOT   = ROOT_DIR / "models" / "pregame_margin_abs"   # <— new task-specific root
SAVE_ROOT.mkdir(parents=True, exist_ok=True)

def _utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
try:
    sha = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                         capture_output=True, text=True, check=False)
    if sha.returncode == 0 and sha.stdout.strip():
        RUN_ID = f"{RUN_ID}_{sha.stdout.strip()}"
except Exception:
    pass

RUN_DIR = SAVE_ROOT / "runs" / RUN_ID
for sub in ["logs", "metrics", "tables", "predictions", "models", "plots", "extras"]:
    (RUN_DIR / sub).mkdir(parents=True, exist_ok=True)

class _Tee:
    def __init__(self, *files): self.files = files
    def write(self, obj):
        for f in self.files:
            f.write(obj); f.flush()
    def flush(self):
        for f in self.files: f.flush()

_log_f = open(RUN_DIR / "logs" / "run.log", "w", buffering=1)
sys.stdout = _Tee(sys.stdout, _log_f)
sys.stderr = _Tee(sys.stderr, _log_f)

RUN_STARTED_AT = _utc_now()

def _write_json(path, obj):
    with open(path, "w") as f:
        json.dump(obj, f, indent=2, default=str)
        
# =========================================================
# Registry (regression flavor)
# =========================================================
_REG_FIELDS = [
    "run_id","started_at","script_path","data_range","target",
    "model_name","is_calibrated","n_train","n_val","n_test",
    "rmse","mae","r2","model_path"
]

def _append_registry(row_dict):
    reg_path = SAVE_ROOT / "registry.csv"
    file_exists = reg_path.exists()
    with open(reg_path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=_REG_FIELDS)
        if not file_exists:
            w.writeheader()
        w.writerow({k: row_dict.get(k, "") for k in _REG_FIELDS})
        
# =========================================================
# Config
# =========================================================
SEED = 42

DB_NAME = "nfl"
DB_HOST = "localhost"
DB_PORT = 5432
DB_USER = "nfl_user"
DB_PASS = "nfl_pass"

MODEL_TBL  = "prod.game_level_modeling_tbl"  # features + margin
SEASON_MIN, SEASON_MAX = 2016, 2025          # inclusive

TARGET = "abs_margin"                         # <— new target

# Strictly no market inputs
drop_market = ["spread_line", "spread_home", "spread_covered"]

# Leak-prone / non-predictive for ABS margin
drop_for_margin_abs = [
    "home_score", "away_score",   # post-hoc labels, leak
    "margin",                     # used to derive abs_margin
    "total_points",               # other target
    "total_line", 
    "home_win", 
    "total_line",                 # market column
    *drop_market
]
drop_non_predictive = ["game_id","kickoff"]

# =========================================================
# Connect & load
# =========================================================
conn_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conn_str)

q_model = text(f"""
    SELECT *
    FROM {MODEL_TBL}
    WHERE season BETWEEN :smin AND :smax
""")
df = pd.read_sql_query(q_model, engine, params={"smin": SEASON_MIN, "smax": SEASON_MAX})

# --- Absolute margin target
if "margin" not in df.columns:
    raise ValueError(f"Required column 'margin' not found in table {MODEL_TBL}.")
df["abs_margin"] = df["margin"].abs().astype(float)

# -----------------------------
# Target & drops
# -----------------------------
# Injury columns: drop all
injury_cols = [c for c in df.columns
               if c.startswith("home_inj_") or c.startswith("away_inj_") or c.startswith("diff_inj_")]

planned_drops = set(drop_for_margin_abs + drop_non_predictive + injury_cols)
to_drop = [c for c in planned_drops if c in df.columns]

X = df.drop(columns=[TARGET] + to_drop, errors="ignore")
y = df[TARGET].astype(float)

# -----------------------------
# Feature typing & preprocessing
# -----------------------------
cat_auto = X.select_dtypes(include=["object","category"]).columns.tolist()
bool_cols = [c for c in X.columns if X[c].dtype == "bool"]
cat_explicit = [c for c in ["season","week"] if c in X.columns]
cat_features = sorted(set(cat_auto) | set(bool_cols) | set(cat_explicit))
num_features = [c for c in X.columns if c not in cat_features]

# Persist config snapshot
_config = {
    "seed": SEED,
    "db": {
        "name": DB_NAME, "host": DB_HOST, "port": DB_PORT, "user": DB_USER,
        "table": MODEL_TBL, "where": f"season BETWEEN {SEASON_MIN} AND {SEASON_MAX}"
    },
    "target": TARGET,
    "season_min": SEASON_MIN,
    "season_max": SEASON_MAX,
    "drops": {
        "for_target": sorted([c for c in drop_for_margin_abs if c in df.columns]),
        "non_predictive": sorted([c for c in drop_non_predictive if c in df.columns]),
        "injury_cols": sorted([c for c in injury_cols if c in df.columns]),
    },
    "features": {
        "numeric": num_features,
        "categorical": cat_features
    },
    "script_path": str(SCRIPT_PATH),
    "run_id": RUN_ID,
    "started_at": RUN_STARTED_AT
}
(RUN_DIR / "extras").mkdir(parents=True, exist_ok=True)
_write_json(RUN_DIR / "config.json", _config)

numeric_transformer = Pipeline(steps=[
    ("imputer", SimpleImputer(strategy="median")),
    ("scaler", StandardScaler()),
])

categorical_transformer = Pipeline(steps=[
    ("imputer", SimpleImputer(strategy="most_frequent")),
    ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
])

preprocessor = ColumnTransformer(
    transformers=[
        ("num", numeric_transformer, num_features),
        ("cat", categorical_transformer, cat_features),
    ],
    remainder="drop",
)

# -----------------------------
# Time-aware split:
#   Train/Val: seasons <= 2023 (random 80/20)
#   Test: season == 2024
#   Action: seasons >= 2025 (labels may be missing) — not used in Step 1
# -----------------------------
label_mask        = df[TARGET].notna()
trainval_mask     = (df["season"] <= 2023) & label_mask
test2024_mask     = (df["season"] == 2024) & label_mask
# action2025_mask   = (df["season"] >= 2025)  # reserved for later steps

X_trainval = X.loc[trainval_mask]
y_trainval = y.loc[trainval_mask]

X_test     = X.loc[test2024_mask]
y_test     = y.loc[test2024_mask]

# Train/Val split (no stratify for regression)
X_train, X_val, y_train, y_val = train_test_split(
    X_trainval, y_trainval,
    test_size=0.20,
    random_state=SEED
)

print("Shapes:")
print(f"  X_train: {X_train.shape}, X_val: {X_val.shape}, X_test(2024): {X_test.shape}")
print(f"  y_train: {y_train.shape}, y_val: {y_val.shape}, y_test: {y_test.shape}")

def _summ(y, name):
    return f"{name}: mean={np.mean(y):.2f} sd={np.std(y):.2f} min={np.min(y):.1f} max={np.max(y):.1f} n={len(y)}"

print("\nTarget summary (abs_margin):")
if len(y_train) > 0: print(" ", _summ(y_train, "Train"))
if len(y_val)   > 0: print(" ", _summ(y_val,   "Val  "))
if len(y_test)  > 0: print(" ", _summ(y_test,  "Test "))

print("\nColumns dropped for leakage / non-predictive (present in data):")
print(sorted([c for c in to_drop if c in df.columns]))

print("\nFeature counts by type (after drop, before OHE):")
print(f"  Numeric: {len(num_features)}")
print(f"  Categorical (incl. season/week/bool): {len(cat_features)}")

# -----------------------------
# Baseline: DummyRegressor (mean)
# -----------------------------
def _reg_metrics(y_true, y_pred):
    return {
        "RMSE": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "MAE":  float(mean_absolute_error(y_true, y_pred)),
        "R2":   float(r2_score(y_true, y_pred)),
    }

def _print_metrics(name, y_val_pred, y_test_pred):
    mv = _reg_metrics(y_val,  y_val_pred)
    mt = _reg_metrics(y_test, y_test_pred)
    print(f"\n{name} — VAL (2016–2023): RMSE={mv['RMSE']:.3f} | MAE={mv['MAE']:.3f} | R^2={mv['R2']:.3f}")
    print(f"{name} — TEST (2024)     : RMSE={mt['RMSE']:.3f} | MAE={mt['MAE']:.3f} | R^2={mt['R2']:.3f}")
    return mv, mt

baseline = DummyRegressor(strategy="mean")

pipe_baseline = Pipeline(steps=[
    ("preprocess", preprocessor),
    ("model", baseline),
])

pipe_baseline.fit(X_train, y_train)

# Validation / Test predictions (clip to >= 0 for absolutes)
yhat_val  = np.clip(pipe_baseline.predict(X_val),  0, None)
yhat_test = np.clip(pipe_baseline.predict(X_test), 0, None)

mv_base, mt_base = _print_metrics("DUMMY_MEAN", yhat_val, yhat_test)

# -----------------------------
# Save snapshots & registry
# -----------------------------
TABLES_DIR = RUN_DIR / "tables"
MODELS_DIR = RUN_DIR / "models"
PRED_DIR   = RUN_DIR / "predictions"
PLOTS_DIR  = RUN_DIR / "plots"
for d in [TABLES_DIR, MODELS_DIR, PRED_DIR, PLOTS_DIR]:
    Path(d).mkdir(parents=True, exist_ok=True)

_schema = {col: str(dtype) for col, dtype in X.dtypes.items()}
_write_json(RUN_DIR / "extras" / "schema_snapshot.json", _schema)

_base_path = MODELS_DIR / "baseline_dummy_mean.joblib"
joblib.dump(pipe_baseline, _base_path)

_append_registry({
    "run_id": RUN_ID,
    "started_at": RUN_STARTED_AT,
    "script_path": str(SCRIPT_PATH),
    "data_range": f"{SEASON_MIN}-{SEASON_MAX}",
    "target": TARGET,
    "model_name": "DUMMY_MEAN",
    "is_calibrated": 0,
    "n_train": X_train.shape[0],
    "n_val":   X_val.shape[0],
    "n_test":  X_test.shape[0],
    "rmse": mt_base["RMSE"], "mae": mt_base["MAE"], "r2": mt_base["R2"],
    "model_path": str(_base_path)
})

print("\nStep 1 complete. Ready for Step 2: train LR-EN / RF / XGB on abs_margin?")

# Restore stdio
try:
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
finally:
    try: _log_f.close()
    except Exception: pass
    
# ============= STEP 2 — Train LR-EN, RF, XGB on abs_margin =============
from sklearn.model_selection import GridSearchCV, KFold
from sklearn.linear_model import ElasticNet
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor

# Safety: functions from Step 1 should exist; redefine if running standalone
def _rmse(y_true, y_pred): return float(np.sqrt(mean_squared_error(y_true, y_pred)))
def _mae (y_true, y_pred): return float(mean_absolute_error(y_true, y_pred))
def _r2  (y_true, y_pred): return float(r2_score(y_true, y_pred))
def _reg_metrics(y_true, y_pred):
    return {"RMSE": _rmse(y_true, y_pred), "MAE": _mae(y_true, y_pred), "R2": _r2(y_true, y_pred)}

def _print_metrics(name, y_val_pred, y_test_pred):
    mv = _reg_metrics(y_val,  y_val_pred)
    mt = _reg_metrics(y_test, y_test_pred)
    print(f"\n{name} — VAL (2016–2023): RMSE={mv['RMSE']:.3f} | MAE={mv['MAE']:.3f} | R^2={mv['R2']:.3f}")
    print(f"{name} — TEST (2024)     : RMSE={mt['RMSE']:.3f} | MAE={mt['MAE']:.3f} | R^2={mt['R2']:.3f}")
    return mv, mt

# Helper to map one-hot features back to original column (for coefficient aggregation)
def _orig_from_processed(name: str, cat_features: list) -> str:
    # ColumnTransformer names: "num__<col>", "cat__<col>_<level>" after OneHot
    if name.startswith("num__"):
        return name[5:]
    if name.startswith("cat__"):
        s = name.split("__", 1)[1]
        best = None
        for f in cat_features:
            pref = f + "_"
            if s.startswith(pref) and (best is None or len(f) > len(best)):
                best = f
        return best if best is not None else s
    return name

# -----------------------------
# CV config
# -----------------------------
cv = KFold(n_splits=5, shuffle=True, random_state=SEED)

# -----------------------------
# ElasticNet (LR-EN)
# -----------------------------
lr_en = ElasticNet(
    alpha=0.1,
    l1_ratio=0.5,
    max_iter=10000,
    fit_intercept=True,
    random_state=SEED,
)

pipe_lr = Pipeline(steps=[
    ("preprocess", preprocessor),
    ("model", lr_en),
])

param_grid_lr = {
    "model__alpha":    [0.001, 0.01, 0.1, 1.0],
    "model__l1_ratio": [0.0, 0.5, 1.0],
}

grid_lr = GridSearchCV(
    estimator=pipe_lr,
    param_grid=param_grid_lr,
    scoring="neg_mean_squared_error",
    cv=cv,
    n_jobs=-1,
    verbose=1,
    refit=True,
)
grid_lr.fit(X_train, y_train)

print("\nLR-EN — Best Params (CV):", grid_lr.best_params_)
print("LR-EN — Best CV RMSE   :", np.sqrt(-grid_lr.best_score_))

best_lr = grid_lr.best_estimator_

y_val_lr  = np.clip(best_lr.predict(X_val),  0, None)
y_test_lr = np.clip(best_lr.predict(X_test), 0, None)
mv_lr, mt_lr = _print_metrics("LR-EN", y_val_lr, y_test_lr)

# Save LR-EN model
_lr_path = MODELS_DIR / "lr_en.joblib"
joblib.dump(best_lr, _lr_path)

# Coefficient audit (top 25 |coef| aggregated to original variables)
try:
    pre = best_lr.named_steps["preprocess"]
    feat_names = pre.get_feature_names_out()
    coefs = best_lr.named_steps["model"].coef_.ravel()
    cat_features = pre.transformers_[1][2] if len(pre.transformers_) > 1 else []
    agg = {}
    for fname, coef in zip(feat_names, coefs):
        orig = _orig_from_processed(fname, cat_features)
        val = abs(float(coef))
        agg[orig] = max(val, agg.get(orig, 0.0))
    coef_df = (pd.DataFrame({"variable": list(agg.keys()), "abs_coef": list(agg.values())})
                 .sort_values("abs_coef", ascending=False).head(25))
    coef_df.to_csv(TABLES_DIR / "lr_en_top25_coeffs.csv", index=False)
    print("Saved -> tables/lr_en_top25_coeffs.csv")
except Exception as e:
    print("[Warn] LR-EN coefficient dump failed:", repr(e))

_append_registry({
    "run_id": RUN_ID,
    "started_at": RUN_STARTED_AT,
    "script_path": str(SCRIPT_PATH),
    "data_range": f"{SEASON_MIN}-{SEASON_MAX}",
    "target": "abs_margin",
    "model_name": "LR_EN",
    "is_calibrated": 0,
    "n_train": X_train.shape[0],
    "n_val":   X_val.shape[0],
    "n_test":  X_test.shape[0],
    "rmse": mt_lr["RMSE"], "mae": mt_lr["MAE"], "r2": mt_lr["R2"],
    "model_path": str(_lr_path)
})

# -----------------------------
# RandomForestRegressor
# -----------------------------
rf = RandomForestRegressor(
    n_estimators=600,
    max_depth=None,
    min_samples_leaf=1,
    random_state=SEED,
    n_jobs=-1,
)

pipe_rf = Pipeline(steps=[
    ("preprocess", preprocessor),
    ("model", rf),
])

param_grid_rf = {
    "model__n_estimators": [300, 600, 1000],
    "model__max_depth": [None, 12, 24],
}

grid_rf = GridSearchCV(
    estimator=pipe_rf,
    param_grid=param_grid_rf,
    scoring="neg_mean_squared_error",
    cv=cv,
    n_jobs=-1,
    verbose=1,
    refit=True,
)
grid_rf.fit(X_train, y_train)

print("\nRF — Best Params (CV):", grid_rf.best_params_)
print("RF — Best CV RMSE   :", np.sqrt(-grid_rf.best_score_))

best_rf = grid_rf.best_estimator_

y_val_rf  = np.clip(best_rf.predict(X_val),  0, None)
y_test_rf = np.clip(best_rf.predict(X_test), 0, None)
mv_rf, mt_rf = _print_metrics("RF", y_val_rf, y_test_rf)

# Save RF model
_rf_path = MODELS_DIR / "rf.joblib"
joblib.dump(best_rf, _rf_path)

# Feature importances
try:
    feat_names = best_rf.named_steps["preprocess"].get_feature_names_out()
    importances = best_rf.named_steps["model"].feature_importances_
    imp_df = (pd.DataFrame({"feature": feat_names, "importance": importances})
                .sort_values("importance", ascending=False).head(25))
    imp_df.to_csv(TABLES_DIR / "rf_top25_importances.csv", index=False)
    print("Saved -> tables/rf_top25_importances.csv")
except Exception as e:
    print("[Warn] RF importance dump failed:", repr(e))

_append_registry({
    "run_id": RUN_ID,
    "started_at": RUN_STARTED_AT,
    "script_path": str(SCRIPT_PATH),
    "data_range": f"{SEASON_MIN}-{SEASON_MAX}",
    "target": "abs_margin",
    "model_name": "RF",
    "is_calibrated": 0,
    "n_train": X_train.shape[0],
    "n_val":   X_val.shape[0],
    "n_test":  X_test.shape[0],
    "rmse": mt_rf["RMSE"], "mae": mt_rf["MAE"], "r2": mt_rf["R2"],
    "model_path": str(_rf_path)
})

# -----------------------------
# XGBRegressor
# -----------------------------
xgb = XGBRegressor(
    objective="reg:squarederror",
    tree_method="hist",
    random_state=SEED,
    n_estimators=800,
    max_depth=5,
    learning_rate=0.1,
    n_jobs=-1,
)

pipe_xgb = Pipeline(steps=[
    ("preprocess", preprocessor),
    ("model", xgb),
])

param_grid_xgb = {
    "model__n_estimators": [400, 800, 1200],
    "model__max_depth": [3, 5, 7],
    "model__learning_rate": [0.03, 0.10, 0.30],
}

grid_xgb = GridSearchCV(
    estimator=pipe_xgb,
    param_grid=param_grid_xgb,
    scoring="neg_mean_squared_error",
    cv=cv,
    n_jobs=-1,
    verbose=1,
    refit=True,
)
grid_xgb.fit(X_train, y_train)

print("\nXGB — Best Params (CV):", grid_xgb.best_params_)
print("XGB — Best CV RMSE   :", np.sqrt(-grid_xgb.best_score_))

best_xgb = grid_xgb.best_estimator_

y_val_xgb  = np.clip(best_xgb.predict(X_val),  0, None)
y_test_xgb = np.clip(best_xgb.predict(X_test), 0, None)
mv_xgb, mt_xgb = _print_metrics("XGB", y_val_xgb, y_test_xgb)

# Save XGB model
_xgb_path = MODELS_DIR / "xgb.joblib"
joblib.dump(best_xgb, _xgb_path)

# XGB gains (map booster slots to feature names)
try:
    pre = best_xgb.named_steps["preprocess"]
    feat_names = pre.get_feature_names_out()
    booster = best_xgb.named_steps["model"].get_booster()
    gain_dict = booster.get_score(importance_type="gain")
    mapped = []
    for k, v in gain_dict.items():
        try:
            idx = int(k[1:])
            mapped.append((feat_names[idx], v))
        except Exception:
            mapped.append((k, v))
    imp_df = pd.DataFrame(mapped, columns=["feature","gain"]).sort_values("gain", ascending=False).head(25)
    imp_df.to_csv(TABLES_DIR / "xgb_top25_gain.csv", index=False)
    print("Saved -> tables/xgb_top25_gain.csv")
except Exception as e:
    print("[Warn] XGB importance dump failed:", repr(e))

_append_registry({
    "run_id": RUN_ID,
    "started_at": RUN_STARTED_AT,
    "script_path": str(SCRIPT_PATH),
    "data_range": f"{SEASON_MIN}-{SEASON_MAX}",
    "target": "abs_margin",
    "model_name": "XGB",
    "is_calibrated": 0,
    "n_train": X_train.shape[0],
    "n_val":   X_val.shape[0],
    "n_test":  X_test.shape[0],
    "rmse": mt_xgb["RMSE"], "mae": mt_xgb["MAE"], "r2": mt_xgb["R2"],
    "model_path": str(_xgb_path)
})

# -----------------------------
# Soft Ensemble (average of predictions)
# -----------------------------
y_val_vote  = np.clip((y_val_lr  + y_val_rf  + y_val_xgb) / 3.0, 0, None)
y_test_vote = np.clip((y_test_lr + y_test_rf + y_test_xgb) / 3.0, 0, None)
mv_vote, mt_vote = _print_metrics("VOTE_SOFT", y_val_vote, y_test_vote)

# -----------------------------
# Unified VAL/TEST summary tables
# -----------------------------
def _pack_metrics(y_true, y_pred):
    return {"RMSE": _rmse(y_true,y_pred), "MAE": _mae(y_true,y_pred), "R2": _r2(y_true,y_pred)}

val_summary = pd.DataFrame({
    "LR_EN":     _pack_metrics(y_val, y_val_lr),
    "RF":        _pack_metrics(y_val, y_val_rf),
    "XGB":       _pack_metrics(y_val, y_val_xgb),
    "VOTE_SOFT": _pack_metrics(y_val, y_val_vote),
})

test_summary = pd.DataFrame({
    "LR_EN":     _pack_metrics(y_test, y_test_lr),
    "RF":        _pack_metrics(y_test, y_test_rf),
    "XGB":       _pack_metrics(y_test, y_test_xgb),
    "VOTE_SOFT": _pack_metrics(y_test, y_test_vote),
})

val_summary.round(6).to_csv(TABLES_DIR / "validation_summary.csv")
test_summary.round(6).to_csv(TABLES_DIR / "test_summary.csv")

print("\n=== Validation Summary (2016–2023) ===")
print(val_summary.round(4).to_string())
print("\n=== Test Summary (2024) ===")
print(test_summary.round(4).to_string())

# -----------------------------
# Combined TEST predictions table (2024)
# -----------------------------
try:
    sched_cols = [c for c in ["season","week","home_team","away_team","season_type","game_type"] if c in df.columns]
    _test = df.loc[X_test.index, sched_cols + ["abs_margin"]].copy()
    _test.rename(columns={"abs_margin":"actual_abs_margin"}, inplace=True)
    _test["pred_lr"]   = y_test_lr
    _test["pred_rf"]   = y_test_rf
    _test["pred_xgb"]  = y_test_xgb
    _test["pred_vote"] = y_test_vote
    _test = _test.sort_values(["season","week","home_team","away_team"])
    _test.to_csv(PRED_DIR / "test_2024_margin_abs_predictions.csv", index=False)
    print("\nSaved 2024 TEST predictions -> predictions/test_2024_margin_abs_predictions.csv")
except Exception as e:
    print("[Warn] Could not save combined TEST predictions:", repr(e))

print("\nStep 2 complete — models trained & saved; summaries and predictions written.")

# ============= STEP 3 — Diagnostics + Conformal PIs for abs_margin =============
import numpy as np
import pandas as pd
import json
from pathlib import Path
import matplotlib.pyplot as plt

# Safety: expect these from Steps 1–2
# df, X, y, X_val, y_val, X_test, y_test, RUN_DIR, TABLES_DIR, PLOTS_DIR, PRED_DIR
# best_lr, best_rf, best_xgb, y_val_lr, y_val_rf, y_val_xgb, y_test_lr, y_test_rf, y_test_xgb

# ---------- Helpers ----------
def _rmse(y_true, y_pred): return float(np.sqrt(((y_true - y_pred) ** 2).mean()))
def _mae (y_true, y_pred): return float(np.mean(np.abs(y_true - y_pred)))
def _r2  (y_true, y_pred):
    # guard against degenerate variance
    ss_res = float(np.sum((y_true - y_pred)**2))
    ss_tot = float(np.sum((y_true - np.mean(y_true))**2))
    return float(1.0 - ss_res/ss_tot) if ss_tot > 0 else float("nan")

def pred_vs_actual_plot(y_true, y_pred, title, out_name):
    fig, ax = plt.subplots(figsize=(6,6))
    ax.scatter(y_true, y_pred, s=12, alpha=0.5)
    lo, hi = float(np.min([y_true.min(), y_pred.min()])), float(np.max([y_true.max(), y_pred.max()]))
    pad = max(1.0, 0.02 * (hi - lo))
    lims = [max(0.0, lo - pad), hi + pad]
    ax.plot(lims, lims, linestyle="--", linewidth=1)
    ax.set_xlim(lims); ax.set_ylim(lims)
    ax.set_title(title)
    ax.set_xlabel("Actual abs margin"); ax.set_ylabel("Predicted abs margin")
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / out_name, dpi=200)
    plt.close(fig)

def residual_hist_plot(y_true, y_pred, title, out_name, bins=25):
    resid = y_true - y_pred
    fig, ax = plt.subplots(figsize=(7,4))
    ax.hist(resid, bins=bins, alpha=0.85)
    ax.axvline(0, linestyle="--", linewidth=1)
    ax.set_title(title + f"  (mean={resid.mean():.2f}, sd={resid.std():.2f})")
    ax.set_xlabel("Residual (Actual - Pred)"); ax.set_ylabel("Count")
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / out_name, dpi=200)
    plt.close(fig)

def decile_table(y_true, y_pred, bins=10):
    df_ = pd.DataFrame({"y": y_true.values, "p": y_pred})
    df_["decile"] = pd.qcut(df_["p"], q=bins, labels=False, duplicates="drop")
    # compute pointwise errors, then aggregate by decile
    df_["abs_err"] = np.abs(df_["y"] - df_["p"])
    df_["sqr_err"] = (df_["y"] - df_["p"]) ** 2
    g = (df_.groupby("decile", as_index=False)
              .agg(n=("y","size"),
                   mean_pred=("p","mean"),
                   mean_actual=("y","mean"),
                   mae=("abs_err","mean"),
                   rmse=("sqr_err", lambda s: float(np.sqrt(np.mean(s))))))
    g["bias"] = g["mean_actual"] - g["mean_pred"]
    return g

def _pack_metrics(y_true, y_pred):
    return {"RMSE": _rmse(y_true,y_pred), "MAE": _mae(y_true,y_pred), "R2": _r2(y_true,y_pred)}

# ---------- Ensure predictions exist (if running this cell standalone) ----------
def _ensure_preds_step2():
    global y_val_lr, y_val_rf, y_val_xgb, y_test_lr, y_test_rf, y_test_xgb, y_val_vote, y_test_vote
    # Compute if missing
    if 'y_val_lr'  not in globals():  y_val_lr  = np.clip(best_lr.predict(X_val),  0, None)
    if 'y_val_rf'  not in globals():  y_val_rf  = np.clip(best_rf.predict(X_val),  0, None)
    if 'y_val_xgb' not in globals():  y_val_xgb = np.clip(best_xgb.predict(X_val), 0, None)
    if 'y_test_lr'  not in globals(): y_test_lr  = np.clip(best_lr.predict(X_test),  0, None)
    if 'y_test_rf'  not in globals(): y_test_rf  = np.clip(best_rf.predict(X_test),  0, None)
    if 'y_test_xgb' not in globals(): y_test_xgb = np.clip(best_xgb.predict(X_test), 0, None)
    y_val_vote  = np.clip((y_val_lr  + y_val_rf  + y_val_xgb) / 3.0, 0, None)
    y_test_vote = np.clip((y_test_lr + y_test_rf + y_test_xgb) / 3.0, 0, None)

_ensure_preds_step2()

# ---------- Per-model diagnostics (TEST 2024) ----------
models = {
    "LR_EN": y_test_lr,
    "RF": y_test_rf,
    "XGB": y_test_xgb,
    "VOTE_SOFT": y_test_vote,
}
for name, yhat in models.items():
    pred_vs_actual_plot(y_test, yhat, f"{name} — Pred vs Actual (2024 Test, abs_margin)", f"pva_test_{name.lower()}_margin_abs.png")
    residual_hist_plot(y_test, yhat, f"{name} — Residuals (2024 Test, abs_margin)", f"resid_hist_test_{name.lower()}_margin_abs.png")
    tbl = decile_table(y_test, yhat, bins=10)
    tbl.to_csv(TABLES_DIR / f"deciles_test_{name.lower()}_margin_abs.csv", index=False)

# ---------- Residuals by week (VOTE_SOFT) ----------
try:
    weeks_test = df.loc[X_test.index, "week"].values
    resid_vote = y_test - y_test_vote
    wdf = pd.DataFrame({"week": weeks_test, "resid": resid_vote})
    wsum = (wdf.groupby("week", as_index=False)
                .agg(mae=("resid", lambda r: float(np.mean(np.abs(r)))),
                     bias=("resid", "mean"),
                     sd=("resid", "std")))
    wsum = wsum.sort_values("week")
    wsum.to_csv(TABLES_DIR / "weekly_residuals_vote_test_margin_abs.csv", index=False)

    fig, ax = plt.subplots(figsize=(7,4))
    ax.plot(wsum["week"], wsum["mae"], marker="o", label="MAE")
    ax.plot(wsum["week"], wsum["bias"], marker="o", label="Bias")
    ax.set_title("VOTE_SOFT — Residuals by Week (2024 Test, abs_margin)")
    ax.set_xlabel("Week"); ax.set_ylabel("Points")
    ax.axhline(0, linestyle="--", linewidth=1)
    ax.legend()
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "weekly_residuals_vote_test_margin_abs.png", dpi=200)
    plt.close(fig)
except Exception as e:
    print("[Warn] Weekly residual plot failed:", repr(e))

# ---------- Conformal prediction intervals (VAL-calibrated) ----------
def conformal_band(y_true_val, y_pred_val, alpha=0.1):
    resid = np.abs(y_true_val - y_pred_val)
    return float(np.quantile(resid, 1 - alpha))

alphas = [0.20, 0.10]  # 80% and 90%

# Validation predictions (should already exist)
y_val_vote = np.clip((y_val_lr + y_val_rf + y_val_xgb)/3.0, 0, None)

conformal = {}
for name, (yv, yt) in {
    "LR_EN":    (y_val_lr,  y_test_lr),
    "RF":       (y_val_rf,  y_test_rf),
    "XGB":      (y_val_xgb, y_test_xgb),
    "VOTE_SOFT":(y_val_vote,y_test_vote),
}.items():
    conformal[name] = {}
    for a in alphas:
        q = conformal_band(y_val, yv, alpha=a)
        lo = np.clip(yt - q, 0, None)  # abs-margin cannot go below 0
        hi = yt + q
        cover = float(np.mean((y_test.values >= lo) & (y_test.values <= hi)))
        width = float(np.mean(hi - lo))
        conformal[name][f"pi_{int((1-a)*100)}"] = {"q": q, "coverage": cover, "avg_width": width}

with open(TABLES_DIR / "conformal_summary_margin_abs.json", "w") as f:
    json.dump(conformal, f, indent=2)

# ---------- TEST (2024) predictions with PIs ----------
try:
    base_path = PRED_DIR / "test_2024_margin_abs_predictions.csv"
    if base_path.exists():
        tdf = pd.read_csv(base_path)
    else:
        sched_cols = [c for c in ["season","week","home_team","away_team","season_type","game_type"] if c in df.columns]
        tdf = df.loc[X_test.index, sched_cols + ["abs_margin"]].copy()
        tdf.rename(columns={"abs_margin":"actual_abs_margin"}, inplace=True)
        tdf["pred_lr"]   = y_test_lr
        tdf["pred_rf"]   = y_test_rf
        tdf["pred_xgb"]  = y_test_xgb
        tdf["pred_vote"] = y_test_vote

    name_map = {"lr":"LR_EN", "rf":"RF", "xgb":"XGB", "vote":"VOTE_SOFT"}
    for name, arr in [("lr", y_test_lr), ("rf", y_test_rf), ("xgb", y_test_xgb), ("vote", y_test_vote)]:
        for a in alphas:
            key = f"pi_{int((1-a)*100)}"
            q = conformal[name_map[name]][key]["q"]
            tdf[f"{name}_{key}_lo"] = np.clip(arr - q, 0, None)
            tdf[f"{name}_{key}_hi"] = arr + q

    out_path = PRED_DIR / "test_2024_margin_abs_predictions_with_PI.csv"
    tdf.to_csv(out_path, index=False)
    print(f"Saved TEST predictions with PIs -> {out_path}")
except Exception as e:
    print("[Warn] Could not save TEST PIs:", repr(e))

# ---------- ACTION (2025+) predictions with PIs ----------
try:
    action2025_mask = (df["season"] >= 2025)
    X_action = X.loc[action2025_mask]
    if X_action.shape[0] > 0:
        y_action_lr   = np.clip(best_lr.predict(X_action),  0, None)
        y_action_rf   = np.clip(best_rf.predict(X_action),  0, None)
        y_action_xgb  = np.clip(best_xgb.predict(X_action), 0, None)
        y_action_vote = np.clip((y_action_lr + y_action_rf + y_action_xgb)/3.0, 0, None)

        sched_cols = [c for c in ["season","week","home_team","away_team","season_type","game_type"] if c in df.columns]
        adf = (df.loc[X_action.index, sched_cols]
                 .assign(pred_lr=y_action_lr, pred_rf=y_action_rf, pred_xgb=y_action_xgb, pred_vote=y_action_vote)
                 .sort_values(["season","week","home_team","away_team"]))

        name_map = {"lr":"LR_EN", "rf":"RF", "xgb":"XGB", "vote":"VOTE_SOFT"}
        for name, arr in [("lr", y_action_lr), ("rf", y_action_rf), ("xgb", y_action_xgb), ("vote", y_action_vote)]:
            for a in alphas:
                key = f"pi_{int((1-a)*100)}"
                q = conformal[name_map[name]][key]["q"]
                adf[f"{name}_{key}_lo"] = np.clip(arr - q, 0, None)
                adf[f"{name}_{key}_hi"] = arr + q

        out_path = PRED_DIR / "action_2025_margin_abs_predictions_with_PI.csv"
        adf.to_csv(out_path, index=False)
        print(f"Saved ACTION predictions with PIs -> {out_path}")
    else:
        print("No 2025+ action rows found; skipping ACTION predictions.")
except Exception as e:
    print("[Warn] ACTION predictions with PIs failed:", repr(e))

# ---------- Combined diagnostics JSON dump ----------
diag = {
    "TEST_2024": {
        "LR_EN":     _pack_metrics(y_test, y_test_lr),
        "RF":        _pack_metrics(y_test, y_test_rf),
        "XGB":       _pack_metrics(y_test, y_test_xgb),
        "VOTE_SOFT": _pack_metrics(y_test, y_test_vote),
    },
    "VAL_2016_2023": {
        "LR_EN":     _pack_metrics(y_val, y_val_lr),
        "RF":        _pack_metrics(y_val, y_val_rf),
        "XGB":       _pack_metrics(y_val, y_val_xgb),
        "VOTE_SOFT": _pack_metrics(y_val, y_val_vote),
    }
}
with open(RUN_DIR / "metrics" / "diagnostics_margin_abs.json", "w") as f:
    json.dump(diag, f, indent=2)

print("\nStep 3 complete — diagnostics, deciles, weekly residuals, conformal PIs, and enriched TEST/ACTION tables are written.")
