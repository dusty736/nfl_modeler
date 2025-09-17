# STEP 1 — Load data, define target/exclusions, make time-aware split, fit a baseline regressor
# (Run this cell/script as-is. It will print baseline metrics and dataset shapes.)
# Target: total_points
# Vegas baseline (TEST 2024): prod.games_tbl.total_line (joined by game_id)

import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np
import os, sys, json, csv, joblib, subprocess, re
import matplotlib.pyplot as plt
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
from sklearn.linear_model import LinearRegression

from sklearn.model_selection import GridSearchCV, KFold
from sklearn.pipeline import Pipeline
from sklearn.linear_model import ElasticNet
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

# ------------ helpers ------------
def _rmse(y_true, y_pred): return float(np.sqrt(mean_squared_error(y_true, y_pred)))
def _mae (y_true, y_pred): return float(mean_absolute_error(y_true, y_pred))
def _r2  (y_true, y_pred): return float(r2_score(y_true, y_pred))
def _metrics(y_true, y_pred): return {"RMSE": _rmse(y_true,y_pred), "MAE": _mae(y_true,y_pred), "R2": _r2(y_true,y_pred)}


def _reg_metrics(y_true, y_pred):
    return {
        "RMSE": _rmse(y_true, y_pred),
        "MAE":  _mae(y_true, y_pred),
        "R2":   _r2(y_true, y_pred),
    }

def _print_metrics(name, y_val_pred, y_test_pred):
    mv = _reg_metrics(y_val,  y_val_pred)
    mt = _reg_metrics(y_test, y_test_pred)
    print(f"\n{name} — VAL (2016–2023): RMSE={mv['RMSE']:.3f} | MAE={mv['MAE']:.3f} | R^2={mv['R2']:.3f}")
    print(f"{name} — TEST (2024)     : RMSE={mt['RMSE']:.3f} | MAE={mt['MAE']:.3f} | R^2={mt['R2']:.3f}")
    return mv, mt
  
# -----------------------------
# Run scaffolding
# -----------------------------
SCRIPT_PATH = Path(__file__).resolve()
ROOT_DIR    = SCRIPT_PATH.parent.parent                   # modeling/
SAVE_ROOT   = ROOT_DIR / "models" / "pregame_total"       # modeling/models/pregame_total
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
        
# For feature name mapping back to original columns
def _orig_from_processed(name: str, cat_features: list) -> str:
    if name.startswith("num__"):
        return name[5:]
    if name.startswith("cat__"):
        s = name.split("__", 2)[-1]
        best = None
        for f in cat_features:
            pref = f + "_"
            if s.startswith(pref) and (best is None or len(f) > len(best)):
                best = f
        return best if best is not None else s
    return name

TABLES_DIR = RUN_DIR / "tables"
MODELS_DIR = RUN_DIR / "models"
PRED_DIR   = RUN_DIR / "predictions"
PLOTS_DIR  = RUN_DIR / "plots"
for d in [TABLES_DIR, MODELS_DIR, PRED_DIR, PLOTS_DIR]:
    Path(d).mkdir(parents=True, exist_ok=True)
    
# =========================================================
# 1) DIAGNOSTICS — Pred vs Actual, Residuals, Decile tables
# =========================================================
def pred_vs_actual_plot(y_true, y_pred, title, out_name):
    fig, ax = plt.subplots(figsize=(6,6))
    ax.scatter(y_true, y_pred, s=12, alpha=0.5)
    lims = [min(y_true.min(), y_pred.min())-1, max(y_true.max(), y_pred.max())+1]
    ax.plot(lims, lims, linestyle="--", linewidth=1)
    ax.set_xlim(lims); ax.set_ylim(lims)
    ax.set_title(title)
    ax.set_xlabel("Actual total points"); ax.set_ylabel("Predicted total points")
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / out_name, dpi=200)
    plt.close(fig)

def residual_hist_plot(y_true, y_pred, title, out_name):
    resid = y_true - y_pred
    fig, ax = plt.subplots(figsize=(7,4))
    ax.hist(resid, bins=25, alpha=0.85)
    ax.axvline(0, linestyle="--", linewidth=1)
    ax.set_title(title + f"  (mean={resid.mean():.2f}, sd={resid.std():.2f})")
    ax.set_xlabel("Residual (Actual - Pred)"); ax.set_ylabel("Count")
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / out_name, dpi=200)
    plt.close(fig)

def decile_table(y_true, y_pred, bins=10):
    df_ = pd.DataFrame({"y": y_true.values, "p": y_pred})
    df_["decile"] = pd.qcut(df_["p"], q=bins, labels=False, duplicates="drop")
    g = (df_.groupby("decile", as_index=False)
           .agg(n=("y","size"), mean_pred=("p","mean"), mean_actual=("y","mean"),
                rmse=("y", lambda a: np.sqrt(np.mean((a - df_.loc[a.index, "p"])**2))),
                mae =("y", lambda a: np.mean(np.abs(a - df_.loc[a.index, "p"])))))
    g["bias"] = g["mean_actual"] - g["mean_pred"]
    return g
        
# Registry (regression flavor)
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
        
# -----------------------------
# Config
# -----------------------------
SEED = 42

DB_NAME = "nfl"
DB_HOST = "localhost"
DB_PORT = 5432
DB_USER = "nfl_user"
DB_PASS = "nfl_pass"
MODEL_TBL  = "prod.game_level_modeling_tbl"  # features + target
GAMES_TBL  = "prod.games_tbl"                # for Vegas total_line comparison
SEASON_MIN, SEASON_MAX = 2016, 2025          # inclusive

TARGET = "total_points"

# Strictly no market inputs
drop_market = ["spread_line", "spread_home"]  # per your instruction

# Leak-prone / non-predictive
drop_for_total = [
    "home_score","away_score","margin","spread_covered",
    "total_points", "home_win", "total_line",  # target
    *drop_market
]
drop_non_predictive = ["game_id","kickoff"]

# -----------------------------
# Connect & load
# -----------------------------
conn_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conn_str)

# Main modeling table
q_model = text(f"""
    SELECT *
    FROM {MODEL_TBL}
    WHERE season BETWEEN :smin AND :smax
""")
df = pd.read_sql_query(q_model, engine, params={"smin": SEASON_MIN, "smax": SEASON_MAX})

# Vegas total_line for comparison (joined by game_id)
q_vegas = text(f"""
    SELECT game_id, total_line
    FROM {GAMES_TBL}
    WHERE season BETWEEN :smin AND :smax
""")
vegas_df = pd.read_sql_query(q_vegas, engine, params={"smin": SEASON_MIN, "smax": SEASON_MAX})

if "game_id" in df.columns and "game_id" in vegas_df.columns:
    df = df.merge(vegas_df, on="game_id", how="left", suffixes=("", "_vegas"))
else:
    print("[Warn] Could not merge total_line from prod.games_tbl (missing game_id). Vegas comparison will be skipped.")
    df["total_line"] = np.nan
    
# -----------------------------
# Target & drops
# -----------------------------
if TARGET not in df.columns:
    raise ValueError(f"Target column '{TARGET}' not found in table {MODEL_TBL}.")

# Injury columns: drop all
injury_cols = [c for c in df.columns
               if c.startswith("home_inj_") or c.startswith("away_inj_") or c.startswith("diff_inj_")]

planned_drops = set(drop_for_total + drop_non_predictive + injury_cols)
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
        "for_target": sorted([c for c in drop_for_total if c in df.columns]),
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
#   Action: seasons >= 2025 (labels may be missing)
# -----------------------------
label_mask        = df[TARGET].notna()
trainval_mask     = (df["season"] <= 2023) & label_mask
test2024_mask     = (df["season"] == 2024) & label_mask
action2025_mask   = (df["season"] >= 2025)  # action set; labels may be missing

X_trainval = X.loc[trainval_mask]
y_trainval = y.loc[trainval_mask]

X_test     = X.loc[test2024_mask]
y_test     = y.loc[test2024_mask]

X_action   = X.loc[action2025_mask]  # no y for action

# Train/Val split (no stratify for regression)
X_train, X_val, y_train, y_val = train_test_split(
    X_trainval, y_trainval,
    test_size=0.20,
    random_state=SEED
)

print("Shapes:")
print(f"  X_train: {X_train.shape}, X_val: {X_val.shape}, X_test(2024): {X_test.shape}, X_action(2025+): {X_action.shape}")
print(f"  y_train: {y_train.shape}, y_val: {y_val.shape}, y_test: {y_test.shape}")

def _summ(y, name):
    return f"{name}: mean={np.mean(y):.2f} sd={np.std(y):.2f} min={np.min(y):.1f} max={np.max(y):.1f} n={len(y)}"

print("\nTarget summary (total_points):")
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
baseline = DummyRegressor(strategy="mean")

pipe_baseline = Pipeline(steps=[
    ("preprocess", preprocessor),
    ("model", baseline),
])

pipe_baseline.fit(X_train, y_train)

# Validation predictions
yhat_val = pipe_baseline.predict(X_val)

# Test predictions
yhat_test = pipe_baseline.predict(X_test)

def _reg_metrics(y_true, y_pred):
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae  = float(mean_absolute_error(y_true, y_pred))
    r2   = float(r2_score(y_true, y_pred))
    return rmse, mae, r2

rmse, mae, r2 = _reg_metrics(y_test, yhat_test)

print("\nBaseline (DummyRegressor: mean) metrics on TEST (2024):")
print(f"  RMSE : {rmse:.3f}")
print(f"  MAE  : {mae:.3f}")
print(f"  R^2  : {r2:.3f}")

# -----------------------------
# Vegas baseline (TEST 2024): compare to total_line
# -----------------------------
vegas_test = df.loc[X_test.index, "total_line"] if "total_line" in df.columns else pd.Series(index=X_test.index, dtype=float)
if vegas_test.notna().any():
    v_rmse, v_mae, v_r2 = _reg_metrics(y_test[vegas_test.notna()], vegas_test.dropna())
    print("\nVegas — TEST (2024) baseline using total_line:")
    print(f"  RMSE : {v_rmse:.3f}")
    print(f"  MAE  : {v_mae:.3f}")
    print(f"  R^2  : {v_r2:.3f}")
else:
    print("\nVegas — 'total_line' not available for TEST; skipping comparison.")
    
# -----------------------------
# Save config snapshots
# -----------------------------
_schema = {col: str(dtype) for col, dtype in X.dtypes.items()}
_write_json(RUN_DIR / "extras" / "schema_snapshot.json", _schema)

# Save baseline pipeline
_base_path = RUN_DIR / "models" / "baseline_dummy_mean.joblib"
joblib.dump(pipe_baseline, _base_path)

# Registry row (baseline)
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
    "rmse": rmse, "mae": mae, "r2": r2,
    "model_path": str(_base_path)
})

print("\nStep 1 complete. Ready for Step 2: build LR/XGB/RF regressors?")
try:
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
finally:
    try: _log_f.close()
    except Exception: pass
    
# ============== ElasticNet (LR-EN) ==============
def _reg_metrics(y_true, y_pred):
    return {
        "RMSE": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "MAE":  float(mean_absolute_error(y_true, y_pred)),
        "R2":   float(r2_score(y_true, y_pred)),
    }
    
lr_en = ElasticNet(
    alpha=0.1,        # tuned via grid
    l1_ratio=0.5,     # tuned via grid
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
    "model__l1_ratio": [0.0, 0.5, 1.0],   # 0=L2 ridge-ish, 1=L1 lasso-ish
}

cv = KFold(n_splits=5, shuffle=True, random_state=SEED)

grid_lr = GridSearchCV(
    estimator=pipe_lr,
    param_grid=param_grid_lr,
    scoring="neg_mean_squared_error",   # we'll sqrt for RMSE
    cv=cv,
    n_jobs=-1,
    verbose=1,
    refit=True,
)
grid_lr.fit(X_train, y_train)

print("\nLR-EN — Best Params (CV):", grid_lr.best_params_)
print("LR-EN — Best CV RMSE   :", np.sqrt(-grid_lr.best_score_))

best_lr = grid_lr.best_estimator_

y_val_lr  = best_lr.predict(X_val)
y_test_lr = best_lr.predict(X_test)
mv_lr, mt_lr = _print_metrics("LR-EN", y_val_lr, y_test_lr)

# Save LR-EN
_lr_path = MODELS_DIR / "lr_en.joblib"
joblib.dump(best_lr, _lr_path)

# Coeff audit (top 25 |coef| aggregated back to original vars)
try:
    pre = best_lr.named_steps["preprocess"]
    feat_names = pre.get_feature_names_out()
    coefs = best_lr.named_steps["model"].coef_.ravel()
    cat_features = pre.transformers_[1][2]  # from ColumnTransformer ("cat")
    agg = {}
    for fname, coef in zip(feat_names, coefs):
        orig = _orig_from_processed(fname, cat_features)
        val = abs(float(coef))
        agg[orig] = max(val, agg.get(orig, 0.0))
    coef_df = (pd.DataFrame({"variable": list(agg.keys()), "abs_coef": list(agg.values())})
                 .sort_values("abs_coef", ascending=False).head(25))
    coef_df.to_csv(TABLES_DIR / "lr_en_top25_coeffs.csv", index=False)
    print("\nTop 25 LR-EN variables by |coef| (aggregated) saved -> lr_en_top25_coeffs.csv")
except Exception as e:
    print("[Warn] LR-EN coefficient dump failed:", repr(e))

# Registry row
_append_registry({
    "run_id": RUN_ID,
    "started_at": RUN_STARTED_AT,
    "script_path": str(SCRIPT_PATH),
    "data_range": f"{SEASON_MIN}-{SEASON_MAX}",
    "target": "total_points",
    "model_name": "LR_EN",
    "is_calibrated": 0,
    "n_train": X_train.shape[0],
    "n_val":   X_val.shape[0],
    "n_test":  X_test.shape[0],
    "rmse": mt_lr["RMSE"], "mae": mt_lr["MAE"], "r2": mt_lr["R2"],
    "model_path": str(_lr_path)
})

# ============== RandomForestRegressor ==============
def _reg_metrics(y_true, y_pred):
    return {
        "RMSE": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "MAE":  float(mean_absolute_error(y_true, y_pred)),
        "R2":   float(r2_score(y_true, y_pred)),
    }
    
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

y_val_rf  = best_rf.predict(X_val)
y_test_rf = best_rf.predict(X_test)
mv_rf, mt_rf = _print_metrics("RF", y_val_rf, y_test_rf)

# Save RF
_rf_path = MODELS_DIR / "rf.joblib"
joblib.dump(best_rf, _rf_path)

# Feature importances
try:
    feat_names = best_rf.named_steps["preprocess"].get_feature_names_out()
    importances = best_rf.named_steps["model"].feature_importances_
    imp_df = (pd.DataFrame({"feature": feat_names, "importance": importances})
                .sort_values("importance", ascending=False).head(25))
    imp_df.to_csv(TABLES_DIR / "rf_top25_importances.csv", index=False)
    print("Top 25 RF feature importances saved -> rf_top25_importances.csv")
except Exception as e:
    print("[Warn] RF importance dump failed:", repr(e))

# Registry row
_append_registry({
    "run_id": RUN_ID,
    "started_at": RUN_STARTED_AT,
    "script_path": str(SCRIPT_PATH),
    "data_range": f"{SEASON_MIN}-{SEASON_MAX}",
    "target": "total_points",
    "model_name": "RF",
    "is_calibrated": 0,
    "n_train": X_train.shape[0],
    "n_val":   X_val.shape[0],
    "n_test":  X_test.shape[0],
    "rmse": mt_rf["RMSE"], "mae": mt_rf["MAE"], "r2": mt_rf["R2"],
    "model_path": str(_rf_path)
})

# ============== XGBRegressor ==============
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

y_val_xgb  = best_xgb.predict(X_val)
y_test_xgb = best_xgb.predict(X_test)
mv_xgb, mt_xgb = _print_metrics("XGB", y_val_xgb, y_test_xgb)

# Save XGB
_xgb_path = MODELS_DIR / "xgb.joblib"
joblib.dump(best_xgb, _xgb_path)

# XGB importances (gain)
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
    print("Top 25 XGB features by gain saved -> xgb_top25_gain.csv")
except Exception as e:
    print("[Warn] XGB importance dump failed:", repr(e))

# Registry row
_append_registry({
    "run_id": RUN_ID,
    "started_at": RUN_STARTED_AT,
    "script_path": str(SCRIPT_PATH),
    "data_range": f"{SEASON_MIN}-{SEASON_MAX}",
    "target": "total_points",
    "model_name": "XGB",
    "is_calibrated": 0,
    "n_train": X_train.shape[0],
    "n_val":   X_val.shape[0],
    "n_test":  X_test.shape[0],
    "rmse": mt_xgb["RMSE"], "mae": mt_xgb["MAE"], "r2": mt_xgb["R2"],
    "model_path": str(_xgb_path)
})

# ============== Soft Ensemble (average of predictions) ==============
y_val_vote  = (y_val_lr  + y_val_rf  + y_val_xgb ) / 3.0
y_test_vote = (y_test_lr + y_test_rf + y_test_xgb) / 3.0
mv_vote, mt_vote = _print_metrics("VOTE_SOFT", y_val_vote, y_test_vote)

# -------------- Vegas baseline on TEST (2024) --------------
vegas_col = "total_line" if "total_line" in df.columns else None
if vegas_col:
    vegas_test = df.loc[X_test.index, vegas_col]
    mask = vegas_test.notna()
    if mask.any():
        v_metrics = _reg_metrics(y_test[mask], vegas_test[mask].astype(float))
        print("\nVegas — TEST (2024) baseline using total_line:")
        print(f"  RMSE={v_metrics['RMSE']:.3f} | MAE={v_metrics['MAE']:.3f} | R^2={v_metrics['R2']:.3f}")
    else:
        print("\nVegas — total_line missing on TEST; skipping comparison.")
else:
    print("\nVegas — total_line column not present; skipping comparison.")

# -------------- Unified VAL/TEST summary tables --------------
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

# Optional: add Vegas row if available
if vegas_col and df.loc[X_test.index, vegas_col].notna().any():
    test_summary["VEGAS_LINE"] = _pack_metrics(y_test[mask], vegas_test[mask].astype(float))

print("\n=== Validation Summary (2016–2023) ===")
print(val_summary.round(4).to_string())
print("\n=== Test Summary (2024) ===")
print(test_summary.round(4).to_string())

# Save summaries
val_summary.round(6).to_csv(TABLES_DIR / "validation_summary.csv")
test_summary.round(6).to_csv(TABLES_DIR / "test_summary.csv")

# -------------- Combined TEST predictions table (2024) --------------
try:
    sched_cols = [c for c in ["season","week","home_team","away_team","season_type","game_type"] if c in df.columns]
    _test = df.loc[X_test.index, sched_cols + ["total_points"]].copy()
    _test["pred_lr"]   = y_test_lr
    _test["pred_rf"]   = y_test_rf
    _test["pred_xgb"]  = y_test_xgb
    _test["pred_vote"] = y_test_vote
    if vegas_col in df.columns:
        _test["vegas_total_line"] = df.loc[X_test.index, vegas_col]
    _test = _test.sort_values(["season","week","home_team","away_team"])
    _test.to_csv(PRED_DIR / "test_2024_total_predictions.csv", index=False)
    print("\nSaved 2024 TEST predictions -> test_2024_total_predictions.csv")
except Exception as e:
    print("[Warn] Could not save combined TEST predictions:", repr(e))
    
# -------------- Action set (2025+) predictions --------------
if 'X_action' in locals() and X_action.shape[0] > 0:
    y_action_lr   = best_lr.predict(X_action)
    y_action_rf   = best_rf.predict(X_action)
    y_action_xgb  = best_xgb.predict(X_action)
    y_action_vote = (y_action_lr + y_action_rf + y_action_xgb) / 3.0

    sched_cols = [c for c in ["season","week","home_team","away_team","season_type","game_type"] if c in df.columns]
    action_preds = (
        df.loc[X_action.index, sched_cols]
          .assign(
              pred_lr   = y_action_lr,
              pred_rf   = y_action_rf,
              pred_xgb  = y_action_xgb,
              pred_vote = y_action_vote
          )
          .sort_values(["season","week","home_team","away_team"])
    )
    action_preds.to_csv(PRED_DIR / "action_2025_total_predictions.csv", index=False)
    print("Saved 2025 action predictions -> action_2025_total_predictions.csv")
else:
    print("No 2025 action set (X_action) available.")
    
# -------- Ensure predictions exist (in case you ran this cell standalone after Step 1) --------
def _ensure_preds():
    global y_val_lr, y_val_rf, y_val_xgb, y_test_lr, y_test_rf, y_test_xgb, y_val_vote, y_test_vote
    if 'y_val_lr'  not in globals():  y_val_lr  = best_lr.predict(X_val)
    if 'y_val_rf'  not in globals():  y_val_rf  = best_rf.predict(X_val)
    if 'y_val_xgb' not in globals():  y_val_xgb = best_xgb.predict(X_val)
    if 'y_test_lr'  not in globals(): y_test_lr  = best_lr.predict(X_test)
    if 'y_test_rf'  not in globals(): y_test_rf  = best_rf.predict(X_test)
    if 'y_test_xgb' not in globals(): y_test_xgb = best_xgb.predict(X_test)
    y_val_vote  = (y_val_lr  + y_val_rf  + y_val_xgb ) / 3.0
    y_test_vote = (y_test_lr + y_test_rf + y_test_xgb) / 3.0

_ensure_preds()

# TEST (2024) diagnostics for each model + vote
models = {
    "LR_EN": y_test_lr,
    "RF": y_test_rf,
    "XGB": y_test_xgb,
    "VOTE_SOFT": y_test_vote,
}
for name, yhat in models.items():
    pred_vs_actual_plot(y_test, yhat, f"{name} — Pred vs Actual (2024 Test)", f"pva_test_{name.lower()}.png")
    residual_hist_plot(y_test, yhat, f"{name} — Residuals (2024 Test)", f"resid_hist_test_{name.lower()}.png")
    tbl = decile_table(y_test, yhat, bins=10)
    tbl.to_csv(TABLES_DIR / f"deciles_test_{name.lower()}.csv", index=False)

# Residuals by week (VOTE_SOFT)
try:
    weeks_test = df.loc[X_test.index, "week"].values
    resid_vote = y_test - y_test_vote
    wdf = pd.DataFrame({"week": weeks_test, "resid": resid_vote})
    wsum = (wdf.groupby("week", as_index=False)
                .agg(mae=("resid", lambda r: np.mean(np.abs(r))),
                     bias=("resid", "mean"),
                     sd=("resid", "std"))).sort_values("week")
    wsum.to_csv(TABLES_DIR / "weekly_residuals_vote_test.csv", index=False)

    fig, ax = plt.subplots(figsize=(7,4))
    ax.plot(wsum["week"], wsum["mae"], marker="o", label="MAE")
    ax.plot(wsum["week"], wsum["bias"], marker="o", label="Bias")
    ax.set_title("VOTE_SOFT — Residuals by Week (2024 Test)")
    ax.set_xlabel("Week"); ax.set_ylabel("Points")
    ax.axhline(0, linestyle="--", linewidth=1)
    ax.legend()
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "weekly_residuals_vote_test.png", dpi=200)
    plt.close(fig)
except Exception as e:
    print("[Warn] Weekly residual plot failed:", repr(e))
    
# =========================================================
# 2) CONFORMAL PREDICTION INTERVALS (val-calibrated)
# =========================================================
# Use absolute residual quantiles on VALIDATION (2016–2023) as calibration
def conformal_band(y_true_val, y_pred_val, alpha=0.1):
    # symmetric PI: yhat ± q_alpha of |residual|
    resid = np.abs(y_true_val - y_pred_val)
    q = np.quantile(resid, 1 - alpha)
    return float(q)

# Compute val predictions if missing
if 'y_val_lr' not in globals():  y_val_lr  = best_lr.predict(X_val)
if 'y_val_rf' not in globals():  y_val_rf  = best_rf.predict(X_val)
if 'y_val_xgb' not in globals(): y_val_xgb = best_xgb.predict(X_val)
y_val_vote = (y_val_lr + y_val_rf + y_val_xgb) / 3.0

alphas = [0.20, 0.10]  # 80% and 90%
conformal = {}
for name, yv, yt in [
    ("LR_EN", y_val_lr, y_test_lr),
    ("RF",    y_val_rf, y_test_rf),
    ("XGB",   y_val_xgb, y_test_xgb),
    ("VOTE_SOFT", y_val_vote, y_test_vote),
]:
    conformal[name] = {}
    for a in alphas:
        q = conformal_band(y_val, yv, alpha=a)
        lo = yt - q
        hi = yt + q
        cover = np.mean((y_test.values >= lo) & (y_test.values <= hi))
        width = float(np.mean(hi - lo))
        conformal[name][f"pi_{int((1-a)*100)}"] = {"q": q, "coverage": float(cover), "avg_width": width}

# Save conformal summary
with open(TABLES_DIR / "conformal_summary.json", "w") as f:
    json.dump(conformal, f, indent=2)

name_map = {"lr": "LR_EN", "rf": "RF", "xgb": "XGB", "vote": "VOTE_SOFT"}    

# Add PIs to TEST predictions table
try:
    test_pred_path = PRED_DIR / "test_2024_total_predictions.csv"
    if test_pred_path.exists():
        tdf = pd.read_csv(test_pred_path)
    else:
        sched_cols = [c for c in ["season","week","home_team","away_team","season_type","game_type"] if c in df.columns]
        tdf = df.loc[X_test.index, sched_cols + ["total_points"]].copy()
        tdf["pred_lr"]   = y_test_lr
        tdf["pred_rf"]   = y_test_rf
        tdf["pred_xgb"]  = y_test_xgb
        tdf["pred_vote"] = y_test_vote
        if "total_line" in df.columns: tdf["vegas_total_line"] = df.loc[X_test.index, "total_line"]

    for name, yhat in [("lr","y_test_lr"), ("rf","y_test_rf"), ("xgb","y_test_xgb"), ("vote","y_test_vote")]:
        arr = globals()[yhat]
        for a in alphas:
            key = f"pi_{int((1-a)*100)}"
            q = conformal[name_map[name]][key]["q"]
            tdf[f"{name}_{key}_lo"] = arr - q
            tdf[f"{name}_{key}_hi"] = arr + q

    tdf.to_csv(PRED_DIR / "test_2024_total_predictions_with_PI.csv", index=False)
    print("Saved TEST (2024) predictions with conformal PIs -> test_2024_total_predictions_with_PI.csv")
except Exception as e:
    print("[Warn] Could not save TEST PIs:", repr(e))

# ACTION (2025+) PIs
if 'X_action' in locals() and X_action.shape[0] > 0:
    y_action_lr   = best_lr.predict(X_action)
    y_action_rf   = best_rf.predict(X_action)
    y_action_xgb  = best_xgb.predict(X_action)
    y_action_vote = (y_action_lr + y_action_rf + y_action_xgb) / 3.0

    sched_cols = [c for c in ["season","week","home_team","away_team","season_type","game_type"] if c in df.columns]
    adf = (df.loc[X_action.index, sched_cols]
             .assign(pred_lr=y_action_lr, pred_rf=y_action_rf, pred_xgb=y_action_xgb, pred_vote=y_action_vote)
             .sort_values(["season","week","home_team","away_team"]))

    for name, arr in [("lr", y_action_lr), ("rf", y_action_rf), ("xgb", y_action_xgb), ("vote", y_action_vote)]:
        for a in alphas:
            key = f"pi_{int((1-a)*100)}"
            q = conformal[name_map[name]][key]["q"]
            adf[f"{name}_{key}_lo"] = arr - q
            adf[f"{name}_{key}_hi"] = arr + q

    adf.to_csv(PRED_DIR / "action_2025_total_predictions_with_PI.csv", index=False)
    print("Saved ACTION (2025+) predictions with PIs -> action_2025_total_predictions_with_PI.csv")
    
# =========================================================
# 3) VEGAS HEAD-TO-HEAD (TEST 2024)
#    Win rate = share of games where |model error| < |vegas error|
# =========================================================
if "total_line" in df.columns:
    vegas_test = df.loc[X_test.index, "total_line"].astype(float)
    mask = vegas_test.notna()
    if mask.any():
        y_t = y_test[mask]
        v   = vegas_test[mask]
        vegas_err = np.abs(y_t.values - v.values)

        rows = []
        for name, yhat in models.items():
            m_err = np.abs(y_t.values - yhat[mask])
            win_rate = float(np.mean(m_err < vegas_err))
            delta_mae = float(np.mean(m_err - vegas_err))   # negative => better than Vegas
            delta_rmse = float(np.sqrt(np.mean((y_t.values - yhat[mask])**2)) -
                               np.sqrt(np.mean((y_t.values - v.values)**2)))
            rows.append({"Model": name,
                         "WinRate_vs_Vegas": round(win_rate, 3),
                         "MAE_minus_Vegas": round(delta_mae, 3),
                         "RMSE_minus_Vegas": round(delta_rmse, 3)})
        comp = pd.DataFrame(rows).sort_values("WinRate_vs_Vegas", ascending=False)
        comp.to_csv(TABLES_DIR / "vegas_head_to_head_test.csv", index=False)
        print("\nSaved Vegas head-to-head (TEST 2024) -> vegas_head_to_head_test.csv")
    else:
        print("Vegas total_line missing on TEST; skipping head-to-head.")
else:
    print("total_line not present; skipping Vegas head-to-head.")
    
# =========================================================
# 4) Quick JSON drop of diagnostics
# =========================================================
diag = {
    "TEST_2024": {
        "LR_EN":     _metrics(y_test, y_test_lr),
        "RF":        _metrics(y_test, y_test_rf),
        "XGB":       _metrics(y_test, y_test_xgb),
        "VOTE_SOFT": _metrics(y_test, y_test_vote),
    },
    "VAL_2016_2023": {
        "LR_EN":     _metrics(y_val, y_val_lr),
        "RF":        _metrics(y_val, y_val_rf),
        "XGB":       _metrics(y_val, y_val_xgb),
        "VOTE_SOFT": _metrics(y_val, y_val_vote),
    }
}
with open(RUN_DIR / "metrics" / "diagnostics.json", "w") as f:
    json.dump(diag, f, indent=2)

print("\nStep 3 complete — diagnostics, conformal PIs, and Vegas showdown are in place.")

