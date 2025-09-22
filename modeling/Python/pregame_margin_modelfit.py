# STEP 1 — Load data, define target/exclusions, make time-aware split, fit a baseline classifier
# Target: margin_bin in ["coin_flip","one_score","two_scores","blowout"]

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
from sklearn.dummy import DummyClassifier
from sklearn.metrics import (
    accuracy_score, balanced_accuracy_score, f1_score,
    cohen_kappa_score, log_loss, confusion_matrix
)

# ------------ helpers ------------
def _utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _brier_multiclass(P, y_int, k):
    Y = np.eye(k)[y_int]
    return float(np.mean(np.sum((P - Y)**2, axis=1)))

def _emd_1d_bins(P, y_int):
    n, k = P.shape
    Pc = np.cumsum(P, axis=1)
    Y = np.zeros_like(P); Y[np.arange(n), y_int] = 1.0
    Yc = np.cumsum(Y, axis=1)
    return float(np.mean(np.sum(np.abs(Pc - Yc), axis=1)))

def _cls_metrics(y_true_int, P):
    k = P.shape[1]
    y_pred = P.argmax(axis=1)
    m = {
        "ACCURACY": float(accuracy_score(y_true_int, y_pred)),
        "BAL_ACC":  float(balanced_accuracy_score(y_true_int, y_pred)),
        "MACRO_F1": float(f1_score(y_true_int, y_pred, average="macro")),
        "QWK":      float(cohen_kappa_score(y_true_int, y_pred, weights="quadratic")),
        "LOG_LOSS": float(log_loss(y_true_int, P, labels=list(range(k)))),
        "BRIER_MC": _brier_multiclass(P, y_true_int, k),
        "EMD":      float(_emd_1d_bins(P, y_true_int)),
    }
    return m, y_pred

def _print_metrics(name, y_true_int, P_val, P_test, bin_order):
    mv, _ = _cls_metrics(y_true_int["val"],  P_val)
    mt, _ = _cls_metrics(y_true_int["test"], P_test)
    print(f"\n{name} — VAL (2016–2023): "
          f"ACC={mv['ACCURACY']:.3f} | BAL_ACC={mv['BAL_ACC']:.3f} | F1={mv['MACRO_F1']:.3f} | "
          f"QWK={mv['QWK']:.3f} | LL={mv['LOG_LOSS']:.3f}")
    print(f"{name} — TEST (2024)     : "
          f"ACC={mt['ACCURACY']:.3f} | BAL_ACC={mt['BAL_ACC']:.3f} | F1={mt['MACRO_F1']:.3f} | "
          f"QWK={mt['QWK']:.3f} | LL={mt['LOG_LOSS']:.3f}")
    return mv, mt

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

# -----------------------------
# Run scaffolding
# -----------------------------
SCRIPT_PATH = Path(__file__).resolve()
ROOT_DIR    = SCRIPT_PATH.parent.parent                         # modeling/
SAVE_ROOT   = ROOT_DIR / "models" / "pregame_margin_bins"       # modeling/models/pregame_margin_bins
SAVE_ROOT.mkdir(parents=True, exist_ok=True)

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

TABLES_DIR = RUN_DIR / "tables"
MODELS_DIR = RUN_DIR / "models"
PRED_DIR   = RUN_DIR / "predictions"
PLOTS_DIR  = RUN_DIR / "plots"
for d in [TABLES_DIR, MODELS_DIR, PRED_DIR, PLOTS_DIR]:
    Path(d).mkdir(parents=True, exist_ok=True)

# =========================================================
# Registry (classification flavor)
# =========================================================
_REG_FIELDS = [
    "run_id","started_at","script_path","data_range","target",
    "model_name","n_train","n_val","n_test",
    "accuracy","balanced_accuracy","macro_f1","log_loss","qwk","model_path"
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

MODEL_TBL  = "prod.game_level_modeling_tbl"
SEASON_MIN, SEASON_MAX = 2016, 2025

# Strictly no market inputs
drop_market = ["spread_line", "spread_home", "spread_covered", "total_line"]

# Leak-prone / non-predictive
drop_leakage = ["home_score","away_score","total_points","margin","abs_margin","home_win"]
drop_non_predictive = ["game_id","kickoff"]

# -----------------------------
# Connect & load
# -----------------------------
conn_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conn_str)

q_model = text(f"""
    SELECT *
    FROM {MODEL_TBL}
    WHERE season BETWEEN :smin AND :smax
""")
df = pd.read_sql_query(q_model, engine, params={"smin": SEASON_MIN, "smax": SEASON_MAX})

# -----------------------------
# Build absolute margin & 4-class bins (target)
# -----------------------------
if "margin" not in df.columns:
    raise ValueError(f"Required column 'margin' not found in {MODEL_TBL}.")
df["abs_margin"] = df["margin"].abs().astype(float)

def _bin(a):
    if a <= 3:   return "coin_flip"
    if a <= 8:   return "one_score"
    if a <= 16:  return "two_scores"
    return "blowout"

df["margin_bin"] = df["abs_margin"].apply(_bin).astype("category")
BIN_ORDER = ["coin_flip","one_score","two_scores","blowout"]
df["margin_bin"] = df["margin_bin"].cat.set_categories(BIN_ORDER, ordered=True)

# -----------------------------
# Drop leakage / market / non-predictive / injuries
# -----------------------------
injury_cols = [c for c in df.columns
               if c.startswith("home_inj_") or c.startswith("away_inj_") or c.startswith("diff_inj_")]

planned_drops = set(drop_market + drop_leakage + drop_non_predictive + injury_cols)
to_drop = [c for c in planned_drops if c in df.columns]

X = df.drop(columns=to_drop + ["margin_bin"], errors="ignore")
y = df["margin_bin"].copy()

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
    "db": {"name": DB_NAME, "host": DB_HOST, "port": DB_PORT, "user": DB_USER,
           "table": MODEL_TBL, "where": f"season BETWEEN {SEASON_MIN} AND {SEASON_MAX}"},
    "target": "margin_bin",
    "season_min": SEASON_MIN,
    "season_max": SEASON_MAX,
    "drops": {
        "market": sorted([c for c in drop_market if c in df.columns]),
        "leakage": sorted([c for c in drop_leakage if c in df.columns]),
        "non_predictive": sorted([c for c in drop_non_predictive if c in df.columns]),
        "injury_cols": sorted([c for c in injury_cols if c in df.columns]),
    },
    "features": {"numeric": num_features, "categorical": cat_features},
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
#   Train/Val: seasons <= 2023 (stratified)
#   Test: season == 2024
#   Action: seasons >= 2025
# -----------------------------
label_mask        = df["abs_margin"].notna()
trainval_mask     = (df["season"] <= 2023) & label_mask
test2024_mask     = (df["season"] == 2024) & label_mask
action2025_mask   = (df["season"] >= 2025)

X_trainval = X.loc[trainval_mask]
y_trainval = y.loc[trainval_mask]

X_test     = X.loc[test2024_mask]
y_test     = y.loc[test2024_mask]
X_action   = X.loc[action2025_mask]

X_train, X_val, y_train, y_val = train_test_split(
    X_trainval, y_trainval,
    test_size=0.20,
    random_state=SEED,
    stratify=y_trainval
)

# Encoded labels for metrics
BIN_TO_INT = {b:i for i,b in enumerate(BIN_ORDER)}
y_train_int = y_train.astype(str).map(BIN_TO_INT).to_numpy()
y_val_int   = y_val.astype(str).map(BIN_TO_INT).to_numpy()
y_test_int  = y_test.astype(str).map(BIN_TO_INT).to_numpy()
N_CLASSES   = len(BIN_ORDER)

def _freq(s):
    c = s.value_counts(normalize=False).rename("n")
    p = (s.value_counts(normalize=True)*100).rename("pct")
    return pd.concat([c, p.round(2)], axis=1)

print("Shapes:")
print(f"  X_train: {X_train.shape}, X_val: {X_val.shape}, X_test(2024): {X_test.shape}, X_action(2025+): {X_action.shape}")

print("\nClass balance — TRAIN:")
print(_freq(y_train).to_string())
print("\nClass balance — VAL:")
print(_freq(y_val).to_string())
print("\nClass balance — TEST 2024:")
print(_freq(y_test).to_string())

print("\nColumns dropped for leakage / non-predictive (present in data):")
print(sorted([c for c in to_drop if c in df.columns]))

print("\nFeature counts by type (after drop, before OHE):")
print(f"  Numeric: {len(num_features)}")
print(f"  Categorical (incl. season/week/bool): {len(cat_features)}")

# -----------------------------
# Baseline: DummyClassifier (most_frequent)
# -----------------------------
baseline = DummyClassifier(strategy="most_frequent", random_state=SEED)

pipe_baseline = Pipeline(steps=[
    ("preprocess", preprocessor),
    ("clf", baseline),
])

pipe_baseline.fit(X_train, y_train_int)

P_val_base  = pipe_baseline.predict_proba(X_val)
P_test_base = pipe_baseline.predict_proba(X_test)

mv_base, mt_base = _print_metrics(
    "DUMMY_MF",
    {"val": y_val_int, "test": y_test_int},
    P_val_base, P_test_base, BIN_ORDER
)

# Save baseline pipeline
_base_path = MODELS_DIR / "baseline_dummy_mf.joblib"
joblib.dump(pipe_baseline, _base_path)

# Registry row (baseline)
_append_registry({
    "run_id": RUN_ID,
    "started_at": RUN_STARTED_AT,
    "script_path": str(SCRIPT_PATH),
    "data_range": f"{SEASON_MIN}-{SEASON_MAX}",
    "target": "margin_bin",
    "model_name": "DUMMY_MF",
    "n_train": X_train.shape[0],
    "n_val":   X_val.shape[0],
    "n_test":  X_test.shape[0],
    "accuracy": mt_base["ACCURACY"],
    "balanced_accuracy": mt_base["BAL_ACC"],
    "macro_f1": mt_base["MACRO_F1"],
    "log_loss": mt_base["LOG_LOSS"],
    "qwk": mt_base["QWK"],
    "model_path": str(_base_path)
})

print("\nStep 1 complete. Ready for Step 2: build LR/RF/XGB classifiers?")
try:
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
finally:
    try: _log_f.close()
    except Exception: pass


# ================== STEP 2 — LR / RF / XGB via GridSearchCV ==================
import numpy as np
import pandas as pd
import joblib
from pathlib import Path

from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV, StratifiedKFold
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier

try:
    from xgboost import XGBClassifier
    _HAS_XGB = True
except Exception:
    _HAS_XGB = False
    print("[Warn] xgboost not available; skipping XGB model.")

# CV & scoring
cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=SEED)
SCORING = "neg_log_loss"

def _evaluate_split_full(name, y_true_int, P, split):
    m, y_pred = _cls_metrics(y_true_int, P)
    m["model"] = name
    m["split"] = split
    return m, y_pred

def _save_confusion(y_true_int, y_pred_int, prefix):
    cm = confusion_matrix(y_true_int, y_pred_int, labels=list(range(N_CLASSES)))
    cm_df = pd.DataFrame(cm, index=BIN_ORDER, columns=BIN_ORDER)
    cm_df.to_csv(TABLES_DIR / f"{prefix}_counts.csv")
    (cm_df.div(cm_df.sum(axis=1).replace(0,1), axis=0)
          .round(6).to_csv(TABLES_DIR / f"{prefix}_rownorm.csv"))

def _pack_pred_df(meta_df, P, y_true_int):
    pred_idx = P.argmax(axis=1)
    out = meta_df.copy()
    for i, b in enumerate(BIN_ORDER):
        out[f"p_{b}"] = P[:, i]
    out["predicted_bin"] = [BIN_ORDER[i] for i in pred_idx]
    out["predicted_bin_confidence"] = P.max(axis=1)
    out["true_bin"] = [BIN_ORDER[i] for i in y_true_int]
    return out

models = {}

# LR (multinomial)
pipe_lr = Pipeline(steps=[
    ("preprocess", preprocessor),
    ("clf", LogisticRegression(
        multi_class="multinomial",
        solver="lbfgs",
        max_iter=3000,
        class_weight="balanced",
        n_jobs=-1,
        random_state=SEED
    ))
])
param_lr = {"clf__C": [0.01, 0.1, 1.0, 3.0, 10.0]}
models["LR"] = (pipe_lr, param_lr)

# RF
pipe_rf = Pipeline(steps=[
    ("preprocess", preprocessor),
    ("clf", RandomForestClassifier(
        n_estimators=600,
        max_depth=None,
        min_samples_leaf=1,
        class_weight="balanced_subsample",
        n_jobs=-1,
        random_state=SEED
    ))
])
param_rf = {
    "clf__n_estimators": [400, 800, 1200],
    "clf__max_depth": [None, 12, 24],
    "clf__min_samples_leaf": [1, 2, 4]
}
models["RF"] = (pipe_rf, param_rf)

# XGB
if _HAS_XGB:
    pipe_xgb = Pipeline(steps=[
        ("preprocess", preprocessor),
        ("clf", XGBClassifier(
            objective="multi:softprob",
            num_class=N_CLASSES,
            eval_metric="mlogloss",
            tree_method="hist",
            n_estimators=800,
            max_depth=5,
            learning_rate=0.10,
            subsample=1.0,
            colsample_bytree=1.0,
            n_jobs=-1,
            random_state=SEED
        ))
    ])
    param_xgb = {
        "clf__n_estimators": [400, 800, 1200],
        "clf__max_depth": [3, 5, 7],
        "clf__learning_rate": [0.05, 0.10, 0.20],
        "clf__subsample": [0.8, 1.0],
        "clf__colsample_bytree": [0.8, 1.0],
    }
    models["XGB"] = (pipe_xgb, param_xgb)

TABLES_DIR.mkdir(parents=True, exist_ok=True)
MODELS_DIR.mkdir(parents=True, exist_ok=True)
PRED_DIR.mkdir(parents=True, exist_ok=True)

sched_cols = [c for c in ["season","week","home_team","away_team","season_type","game_type"] if c in df.columns]
VAL_META  = df.loc[X_val.index,  sched_cols].copy()
TEST_META = df.loc[X_test.index, sched_cols + ["abs_margin"]].copy().rename(columns={"abs_margin":"actual_abs_margin"})

all_val_metrics, all_test_metrics = [], []

for name, (pipe, param_grid) in models.items():
    print(f"\n[Grid] {name} — tuning (scoring={SCORING}) …")
    grid = GridSearchCV(
        estimator=pipe,
        param_grid=param_grid,
        scoring=SCORING,
        cv=cv,
        n_jobs=-1,
        verbose=1,
        refit=True,
        error_score="raise"
    )
    grid.fit(X_train, y_train_int)

    print(f"{name} — Best Params:", grid.best_params_)
    print(f"{name} — Best CV {SCORING}:", grid.best_score_)

    best = grid.best_estimator_
    joblib.dump(best, MODELS_DIR / f"{name.lower()}_clf.joblib")

    P_val  = best.predict_proba(X_val)
    P_test = best.predict_proba(X_test)

    mv, yv = _evaluate_split_full(name, y_val_int,  P_val,  "VAL")
    mt, yt = _evaluate_split_full(name, y_test_int, P_test, "TEST")
    all_val_metrics.append(mv); all_test_metrics.append(mt)

    _save_confusion(y_val_int,  yv, f"confusion_val_{name.lower()}")
    _save_confusion(y_test_int, yt, f"confusion_test_{name.lower()}")

    df_val  = _pack_pred_df(VAL_META.copy(),  P_val,  y_val_int)
    df_test = _pack_pred_df(TEST_META.copy(), P_test, y_test_int)
    df_val.to_csv (PRED_DIR / f"val_probs_{name.lower()}.csv",  index=False)
    df_test.to_csv(PRED_DIR / f"test_2024_probs_{name.lower()}.csv", index=False)

# Consolidated metric tables
val_df  = pd.DataFrame(all_val_metrics).set_index("model")
test_df = pd.DataFrame(all_test_metrics).set_index("model")
val_df.round(6).to_csv(TABLES_DIR / "val_metrics_all_models.csv")
test_df.round(6).to_csv(TABLES_DIR / "test_metrics_all_models.csv")

print("\nSaved:")
print("  models/<lr|rf|xgb>_clf.joblib")
print("  tables/val_metrics_all_models.csv")
print("  tables/test_metrics_all_models.csv")
print("  tables/confusion_val_*.csv + _rownorm.csv")
print("  tables/confusion_test_*.csv + _rownorm.csv")
print("  predictions/val_probs_*.csv")
print("  predictions/test_2024_probs_*.csv")

# Registry rows for tuned models (use TEST metrics)
for name, metrics in test_df.to_dict(orient="index").items():
    _append_registry({
        "run_id": RUN_ID,
        "started_at": RUN_STARTED_AT,
        "script_path": str(SCRIPT_PATH),
        "data_range": f"{SEASON_MIN}-{SEASON_MAX}",
        "target": "margin_bin",
        "model_name": name,
        "n_train": X_train.shape[0],
        "n_val":   X_val.shape[0],
        "n_test":  X_test.shape[0],
        "accuracy": metrics["ACCURACY"],
        "balanced_accuracy": metrics["BAL_ACC"],
        "macro_f1": metrics["MACRO_F1"],
        "log_loss": metrics["LOG_LOSS"],
        "qwk": metrics["QWK"],
        "model_path": str(MODELS_DIR / f"{name.lower()}_clf.joblib")
    })

print("\nStep 2 complete — tuned classifiers trained; metrics + predictions exported.")

# ================== STEP 3 — Comparisons, Ensemble, “Closest” lists ==================
import json

# Load back best pipelines
available = []
paths = {"LR": MODELS_DIR / "lr_clf.joblib", "RF": MODELS_DIR / "rf_clf.joblib", "XGB": MODELS_DIR / "xgb_clf.joblib"}
for name, p in paths.items():
    if p.exists():
        try:
            available.append((name, joblib.load(p)))
        except Exception as e:
            print(f"[Warn] Could not load {name}: {e}")
if not available:
    raise RuntimeError("No trained models found; run Step 2 first.")

# Evaluate & re-save preds (ensures consistency)
def _evaluate_split_simple(y_true_int, P):
    m, y_pred = _cls_metrics(y_true_int, P)
    return m, y_pred

VAL_META  = df.loc[X_val.index,  sched_cols].copy()
TEST_META = df.loc[X_test.index, sched_cols + ["abs_margin"]].copy().rename(columns={"abs_margin":"actual_abs_margin"})

val_metrics, test_metrics = [], []
val_prob_dfs, test_prob_dfs = {}, {}

for name, pipe in available:
    print(f"\n[Eval] {name} …")
    P_val  = pipe.predict_proba(X_val)
    P_test = pipe.predict_proba(X_test)

    m_val,  yv = _evaluate_split_simple(y_val_int,  P_val)
    m_test, yt = _evaluate_split_simple(y_test_int, P_test)
    m_val["model"] = name; m_test["model"] = name
    val_metrics.append(m_val); test_metrics.append(m_test)

    # Save predictions with probabilities
    df_val  = _pack_pred_df(VAL_META,  P_val,  y_val_int)
    df_test = _pack_pred_df(TEST_META, P_test, y_test_int)
    df_val.to_csv (PRED_DIR / f"val_probs_{name.lower()}.csv",  index=False)
    df_test.to_csv(PRED_DIR / f"test_2024_probs_{name.lower()}.csv", index=False)
    val_prob_dfs[name]  = df_val
    test_prob_dfs[name] = df_test

# Bar plots for ACC/QWK
def _barplot_compare(val_df, test_df, metric, out_png):
    try:
        fig, ax = plt.subplots(figsize=(6.5, 4))
        ax.bar(val_df.index,  val_df[metric], alpha=0.8, label="VAL")
        ax.bar(test_df.index, test_df[metric], alpha=0.5, label="TEST")
        ax.set_ylim(0, 1)
        ax.set_title(f"Model comparison — {metric}")
        ax.legend()
        fig.tight_layout()
        fig.savefig(PLOTS_DIR / out_png, dpi=200)
        plt.close(fig)
    except Exception as e:
        print(f"[Warn] Plot '{metric}' failed: {e}")

val_df  = pd.DataFrame(val_metrics).set_index("model").sort_index()
test_df = pd.DataFrame(test_metrics).set_index("model").sort_index()
val_df.round(6).to_csv(TABLES_DIR / "val_metrics_all_models.csv")
test_df.round(6).to_csv(TABLES_DIR / "test_metrics_all_models.csv")

_barplot_compare(val_df, test_df, "ACCURACY", "acc_compare.png")
_barplot_compare(val_df, test_df, "QWK",      "qwk_compare.png")

# Ensemble = equal-weight average of probs
def _stack_probs(prob_dfs):
    arrs = []
    for _, dfp in prob_dfs.items():
        arrs.append(np.stack([dfp[f"p_{b}"].values for b in BIN_ORDER], axis=1))
    A = np.stack(arrs, axis=0)  # (m, n, k)
    return A.mean(axis=0)       # (n, k)

P_val_ens  = _stack_probs(val_prob_dfs)
P_test_ens = _stack_probs(test_prob_dfs)

m_val_ens,  yv_ens = _evaluate_split_simple(y_val_int,  P_val_ens)
m_test_ens, yt_ens = _evaluate_split_simple(y_test_int, P_test_ens)
m_val_ens["model"]  = "ENSEMBLE"
m_test_ens["model"] = "ENSEMBLE"

val_df  = pd.concat([val_df,  pd.DataFrame([m_val_ens]).set_index("model")])
test_df = pd.concat([test_df, pd.DataFrame([m_test_ens]).set_index("model")])
val_df.round(6).to_csv(TABLES_DIR / "val_metrics_all_models.csv")
test_df.round(6).to_csv(TABLES_DIR / "test_metrics_all_models.csv")

# Save ensemble predictions
def _pack_pred_df_no_true(meta_df, P):
    pred_idx = P.argmax(axis=1)
    out = meta_df.copy()
    for i, b in enumerate(BIN_ORDER):
        out[f"p_{b}"] = P[:, i]
    out["predicted_bin"] = [BIN_ORDER[i] for i in pred_idx]
    out["predicted_bin_confidence"] = P.max(axis=1)
    return out

VAL_ENS  = _pack_pred_df_no_true(VAL_META.copy(),  P_val_ens)
TEST_ENS = _pack_pred_df_no_true(TEST_META.copy(), P_test_ens)
VAL_ENS.to_csv (PRED_DIR / "val_probs_ensemble.csv",  index=False)
TEST_ENS.to_csv(PRED_DIR / "test_2024_probs_ensemble.csv", index=False)

# “Closest” Top-25 lists by CI=P(coin)+0.5*P(one)
def _closeness_index(P): return P[:,0] + 0.5*P[:,1]
def _top_closest(df_probs, topn=25):
    P = np.stack([df_probs[f"p_{b}"].values for b in BIN_ORDER], axis=1)
    ci = _closeness_index(P)
    out = df_probs.copy()
    out["closeness_index"] = ci
    return (out.sort_values(["closeness_index","predicted_bin_confidence"], ascending=[False, False])
               .head(topn))

for name, dfp in test_prob_dfs.items():
    _top_closest(dfp, topn=25).to_csv(PRED_DIR / f"test_2024_top25_closest_{name.lower()}.csv", index=False)
_top_closest(TEST_ENS, topn=25).to_csv(PRED_DIR / "test_2024_top25_closest_ensemble.csv", index=False)

# Diagnostics JSON
diag = {
    "TEST_2024": test_df.to_dict(orient="index"),
    "VAL_2016_2023": val_df.to_dict(orient="index")
}
with open(RUN_DIR / "metrics" / "diagnostics_bins.json", "w") as f:
    json.dump(diag, f, indent=2)

print("\nStep 3 complete — comparisons, ensemble, 'closest' rankings, diagnostics saved.")

# ================== STEP 4 — Action scoring (+ actuals if available) & optional DB upsert ==================
import argparse

def _to_bin_label(abs_margin: float) -> str:
    try:
        a = float(abs_margin)
    except Exception:
        return None
    if np.isnan(a):
        return None
    if a <= 3:   return "coin_flip"
    if a <= 8:   return "one_score"
    if a <= 16:  return "two_scores"
    return "blowout"

parser = argparse.ArgumentParser(description="Predict margin bins for a slice (season/week) and optionally upsert to DB.")
parser.add_argument("--season", type=int, default=None, help="Season to score (default: >=2025 action set).")
parser.add_argument("--week", type=int, default=None, help="Week to score (requires --season).")
parser.add_argument("--model", type=str, default="ENSEMBLE",
                    help="LR | RF | XGB | ENSEMBLE | BEST (by QWK in tables/test_metrics_all_models.csv).")
parser.add_argument("--write-db", action="store_true", help="If set, upsert predictions into Postgres.")
parser.add_argument("--table", type=str, default="prod.pregame_margin_bins_preds_tbl",
                    help="Destination table for upsert (schema-qualified).")
parser.add_argument("--outfile-prefix", type=str, default="action",
                    help="Prefix for output CSVs in predictions/.")
args, _ = parser.parse_known_args()

# Slice to score
if args.season is not None:
    sel = (df["season"] == args.season)
    if args.week is not None:
        sel &= (df["week"] == args.week)
else:
    sel = (df["season"] >= 2025)
if not sel.any():
    raise RuntimeError("No rows match the requested slice. Check season/week or ensure action rows exist.")

sched_cols = [c for c in ["season","week","home_team","away_team","season_type","game_type"] if c in df.columns]
META = df.loc[sel, sched_cols].copy().sort_values(["season","week","home_team","away_team"])

# Feature frame matches training columns (use X_val columns as canonical)
feature_cols = list(X_val.columns)
X_action = df.loc[META.index, feature_cols].copy()

# Load models
bundle = []
for name, p in paths.items():
    if p.exists():
        try:
            bundle.append((name, joblib.load(p)))
        except Exception as e:
            print(f"[Warn] Could not load {name}: {e}")
if not bundle:
    raise RuntimeError("No trained models available for action scoring.")

def _best_by_table(table_path, metric="QWK"):
    try:
        tdf = pd.read_csv(table_path, index_col=0)
        if metric in tdf.columns:
            return tdf[metric].idxmax()
    except Exception:
        pass
    return "ENSEMBLE"

if args.model.upper() == "BEST":
    args.model = _best_by_table(TABLES_DIR / "test_metrics_all_models.csv", metric="QWK")

def _predict_with_ensemble(models_bundle, X_):
    prob_list = [pipe.predict_proba(X_) for _, pipe in models_bundle]
    return np.mean(prob_list, axis=0)

if args.model.upper() == "ENSEMBLE":
    P = _predict_with_ensemble(bundle, X_action)
else:
    picked = dict(bundle).get(args.model.upper(), None)
    if picked is None:
        raise ValueError(f"Requested model '{args.model}' not found. Available: {[n for n,_ in bundle]} + ENSEMBLE")
    P = picked.predict_proba(X_action)

# Pack output (with actuals if any finished)
pred_idx = P.argmax(axis=1)
out_df = META.copy()
for i, b in enumerate(BIN_ORDER):
    out_df[f"p_{b}"] = P[:, i]
out_df["predicted_bin"] = [BIN_ORDER[i] for i in pred_idx]
out_df["predicted_bin_confidence"] = P.max(axis=1)
out_df["closeness_index"] = P[:,0] + 0.5*P[:,1]

# Bring actuals if present
actual_abs = df.loc[META.index, "abs_margin"] if "abs_margin" in df.columns else pd.Series(index=META.index, dtype=float)
out_df["actual_abs_margin"] = actual_abs
true_bin = actual_abs.apply(_to_bin_label)
out_df["true_bin"]  = pd.Categorical(true_bin, categories=BIN_ORDER, ordered=True)
out_df["is_final"]  = out_df["true_bin"].notna()
out_df["predicted_correct"] = np.where(out_df["is_final"],
                                       (out_df["true_bin"].astype(str) == out_df["predicted_bin"]),
                                       np.nan)

# Run metadata
out_df.insert(0, "model_name", args.model.upper())
out_df.insert(0, "run_id", RUN_DIR.name)
out_df.insert(0, "predicted_at_utc", _utc_now())

# Save CSV
suffix = f"{args.outfile_prefix}_{args.model.lower()}"
if args.season is not None:
    suffix += f"_s{args.season}"
    if args.week is not None:
        suffix += f"_w{args.week:02d}"
out_path = PRED_DIR / f"{suffix}_probs.csv"
out_df.to_csv(out_path, index=False)
print(f"Saved -> {out_path}")

# Evaluate completed subset in the action slice
completed_mask = out_df["is_final"].fillna(False).values
if completed_mask.any():
    label_to_int_local = {b:i for i,b in enumerate(BIN_ORDER)}
    y_true_int = out_df.loc[completed_mask, "true_bin"].astype(str).map(label_to_int_local).to_numpy()
    P_completed = np.stack([out_df.loc[completed_mask, f"p_{b}"].values for b in BIN_ORDER], axis=1)

    y_pred_int = P_completed.argmax(axis=1)
    m, _ = _cls_metrics(y_true_int, P_completed)
    for i, cls in enumerate(BIN_ORDER):
        m[f"F1_{cls}"] = float(f1_score(y_true_int, y_pred_int, average=None, labels=list(range(len(BIN_ORDER))))[i])

    pd.DataFrame([m]).to_csv(TABLES_DIR / f"{suffix}_completed_metrics.csv", index=False)
    cm = confusion_matrix(y_true_int, y_pred_int, labels=list(range(len(BIN_ORDER))))
    cm_df = pd.DataFrame(cm, index=BIN_ORDER, columns=BIN_ORDER)
    cm_df.to_csv(TABLES_DIR / f"{suffix}_completed_confusion_counts.csv")
    (cm_df.div(cm_df.sum(axis=1).replace(0,1), axis=0)
          .round(6).to_csv(TABLES_DIR / f"{suffix}_completed_confusion_rownorm.csv"))
    print(f"[Eval] Completed subset (n={completed_mask.sum()}) — metrics saved.")
else:
    print("[Eval] No completed games yet in this slice.")

# Optional UPSERT
if args.write_db:
    print(f"[DB] Upserting into {args.table} …")
    DB_NAME = os.getenv("DB_NAME", "nfl")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_USER = os.getenv("DB_USER", "nfl_user")
    DB_PASS = os.getenv("DB_PASS", "nfl_pass")

    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    schema, table = args.table.split(".", 1) if "." in args.table else ("public", args.table)

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        predicted_at_utc     timestamptz NOT NULL,
        run_id               text        NOT NULL,
        model_name           text        NOT NULL,
        season               int         NOT NULL,
        week                 int         NOT NULL,
        home_team            text        NOT NULL,
        away_team            text        NOT NULL,
        season_type          text        NULL,
        game_type            text        NULL,
        p_coin_flip          double precision NOT NULL,
        p_one_score          double precision NOT NULL,
        p_two_scores         double precision NOT NULL,
        p_blowout            double precision NOT NULL,
        predicted_bin        text        NOT NULL,
        predicted_bin_confidence double precision NOT NULL,
        closeness_index      double precision NOT NULL,
        actual_abs_margin    double precision NULL,
        true_bin             text NULL,
        is_final             boolean NULL,
        predicted_correct    boolean NULL,
        PRIMARY KEY (season, week, home_team, away_team, model_name)
    );
    """
    upsert_sql = f"""
    INSERT INTO {schema}.{table} (
        predicted_at_utc, run_id, model_name,
        season, week, home_team, away_team, season_type, game_type,
        p_coin_flip, p_one_score, p_two_scores, p_blowout,
        predicted_bin, predicted_bin_confidence, closeness_index,
        actual_abs_margin, true_bin, is_final, predicted_correct
    )
    VALUES (
        %(predicted_at_utc)s, %(run_id)s, %(model_name)s,
        %(season)s, %(week)s, %(home_team)s, %(away_team)s, %(season_type)s, %(game_type)s,
        %(p_coin_flip)s, %(p_one_score)s, %(p_two_scores)s, %(p_blowout)s,
        %(predicted_bin)s, %(predicted_bin_confidence)s, %(closeness_index)s,
        %(actual_abs_margin)s, %(true_bin)s, %(is_final)s, %(predicted_correct)s
    )
    ON CONFLICT (season, week, home_team, away_team, model_name) DO UPDATE SET
        predicted_at_utc         = EXCLUDED.predicted_at_utc,
        run_id                   = EXCLUDED.run_id,
        p_coin_flip              = EXCLUDED.p_coin_flip,
        p_one_score              = EXCLUDED.p_one_score,
        p_two_scores             = EXCLUDED.p_two_scores,
        p_blowout                = EXCLUDED.p_blowout,
        predicted_bin            = EXCLUDED.predicted_bin,
        predicted_bin_confidence = EXCLUDED.predicted_bin_confidence,
        closeness_index          = EXCLUDED.closeness_index,
        actual_abs_margin        = EXCLUDED.actual_abs_margin,
        true_bin                 = EXCLUDED.true_bin,
        is_final                 = EXCLUDED.is_final,
        predicted_correct        = EXCLUDED.predicted_correct;
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
        conn.execute(text(upsert_sql), out_df.to_dict(orient="records"))

    print(f"[DB] Upserted {len(out_df)} rows into {schema}.{table}.")
