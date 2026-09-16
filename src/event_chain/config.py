"""The event chain's knobs. ⚠️ The EVENT itself (gain %, horizon, rule) is NOT here — it is
`utils.event_target`, because `pool__targets`, `gold.stocks_event_features` and
`feature_selection.run` read it too, and a second copy would be the one that drifts.
"""

from __future__ import annotations

import os

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

TICKER = "VCB"
# `d`, the window a sample reads. ⚠️ It enters the purge (`d + h - 1`) and the table name.
LOOKBACK = 20
# The date split `train_test_creator` cuts. The selection's holdout is the val start, so
# the feature ranking never reads a val or a test row.
TRAIN_RATIO = 0.70
VAL_RATIO = 0.15

# ⚠️ 10 draws FAIL a pool, 20 PASS one (CLAUDE.md §5). A selection is a filter here, and
# a pool that fails its own null is recorded as such and still offered to the model only
# when `--keep-failed` says so.
NULL_DRAWS = 10

# ⚠️ A ROOT OF ITS OWN. `final_features` unions every run under a root that shares
# (schema, target, setup); the chain's archive at `reports/feature_selection` holds other
# targets' runs, and a probe left there joins the chain (`PRB-1`).
REPORT_ROOT = os.path.join(REPO_ROOT, "reports", "feature_selection_event")
OUTPUT_ROOT = os.path.join(REPO_ROOT, "reports", "event_chain")

# The FEATURE GROUPS offered to the selection, one run each. ⚠️ ONE POOL PER RUN is what
# keeps every run under PostgreSQL's 1,600-column limit and the design matrix inside the
# 4 GB card; the survivors are then unioned into ONE table by `final_features`, which
# refuses a union wider than the limit.
#
# Ordered narrow → wide, so a partial run has done the cheap, VCB-specific groups first.
POOLS = (
    "pool__basic",            # own price, volume, orders, foreign and prop flow (~90)
    "pool__event_features",   # the event's own history, vol-scaled threshold, bank sector, calendar (~45)
    "pool__market_breadth",   # the cross-section compressed to one row a day (~7)
    "pool__news_daily",       # news / disclosure counts (~14)
    "pool__stock_market",     # the six VN indices x 27 measures (~160)
    "pool__economy_vietnam",  # VN macro (~88)
    "pool__bonds",            # VN government yields (~117)
    "pool__fa",               # bank fundamentals, on publish_date (~190)
    "pool__funds",            # HOSE ETFs (~389)
    "pool__basic_bank",       # the 20 banks as peer channels (~540)
    "pool__ta",               # technical indicators (~930)
)

# Channels the one-feature logistic baseline tries, first present wins.
BASELINE_CHANNELS = (
    "evt_thr_z_20", "evt_vol_20", "drv_realized_vol_10", "drv_parkinson_21",
    "drv_garman_klass_21",
)

_TRAIN = dict(batch_size=64, lr=0.001, weight_decay=0.0001, max_epochs=100,
              patience=15, grad_clip=1.0, lr_factor=0.5, lr_patience=5, log_every=10)

# (package, variant, `model:` block, extra top-level keys). ⚠️ The GRID IS FIXED BEFORE
# ANY TEST SCORE IS READ; the best model is chosen on VAL ROC-AUC and its test number is
# reported beside every other row, never alone.
MODELS = (
    ("baseline", "_prior", {"type": "BASELINE", "kind": "prior"}, {}),
    ("baseline", "_logistic_channel", {"type": "BASELINE", "kind": "logistic_channel",
                                       "target_channel": None, "C": 1.0}, {}),
    ("baseline", "_logistic_stats_c001", {"type": "BASELINE", "kind": "logistic_stats", "C": 0.01}, {}),
    ("baseline", "_logistic_stats_c01", {"type": "BASELINE", "kind": "logistic_stats", "C": 0.1}, {}),
    ("gbt", "_d2", {"type": "GBT", "max_depth": 2, "n_estimators": 400, "learning_rate": 0.02,
                    "subsample": 0.8, "colsample_bytree": 0.5, "min_child_weight": 20.0}, {}),
    ("gbt", "_d3", {"type": "GBT", "max_depth": 3, "n_estimators": 300, "learning_rate": 0.03,
                    "subsample": 0.8, "colsample_bytree": 0.5, "min_child_weight": 10.0}, {}),
    ("gbt", "_d4", {"type": "GBT", "max_depth": 4, "n_estimators": 200, "learning_rate": 0.03,
                    "subsample": 0.7, "colsample_bytree": 0.3, "min_child_weight": 20.0,
                    "gamma": 1.0}, {}),
    ("forest", "_et_leaf20", {"type": "FOREST", "kind": "et", "min_samples_leaf": 20,
                              "max_features": 0.3, "n_estimators": 500}, {}),
    ("forest", "_et_leaf50", {"type": "FOREST", "kind": "et", "min_samples_leaf": 50,
                              "max_features": 0.3, "n_estimators": 500}, {}),
    ("forest", "_rf_leaf30", {"type": "FOREST", "kind": "rf", "min_samples_leaf": 30,
                              "max_features": 0.2, "n_estimators": 500}, {}),
    ("lstm", "_h16", {"type": "LSTM", "hidden_size": 16, "num_layers": 1, "dropout": 0.2},
     {"train": _TRAIN}),
    ("gru", "_h16", {"type": "GRU", "hidden_size": 16, "num_layers": 1, "dropout": 0.2},
     {"train": _TRAIN}),
    ("cnn", "_c16", {"type": "CNN", "channels": 16, "kernel_size": 3, "num_layers": 2,
                     "dropout": 0.2}, {"train": _TRAIN}),
    ("mlp", "_h16", {"type": "MLP", "hidden_size": 16, "dropout": 0.2}, {"train": _TRAIN}),
    ("tcn", "_c16", {"type": "TCN", "channels": 16, "kernel_size": 3, "num_layers": 2,
                     "dropout": 0.2}, {"train": _TRAIN}),
)

# Draws for the event ROC-AUC null in the report (block = d + h). Cheap: a vector shuffle.
REPORT_NULL_DRAWS = 1000


# ══════════════════════════════════════════════════════════════════════════════════════
# ⚠️ THE TABULAR SETUP (2026-09-17) — `python -m event_chain --setup tabular`
#
# d = 1: every model reads the LAST ROW, from a table that carries ALL channels of the pools
# whose selection ran (`final_features` channels="all"), and three LINEAR estimators
# (`model.event_linear`) answer the three questions the event is made of — how big the move,
# which way, and the event itself — combined by a geometric mean fixed BEFORE the chain ran.
#
# ⚠️ WHY IT EXISTS, MEASURED: the windowed grid above (d=20, 15 models, 3 trials) put its
# val-chosen model at test AUC 0.520-0.564. A research harness over the same data — 10-fold
# rolling-origin CV, validation years 2014..2023, NO test row read — scored the window
# networks and trees 0.52-0.63 and last-row linear models ~0.67, with the geometric mean of
# the three kinds ~0.69 (`.claude/context/event_chain.md` §6). Every hyper-parameter below is
# that CV's choice, frozen here before the tabular chain's own val or test was scored.
# ══════════════════════════════════════════════════════════════════════════════════════
TABULAR_LOOKBACK = 1
TABULAR_SCOPE = "tab"
TABULAR_POOLS = (
    "pool__event_features",   # evt_/sec_/mkt_/cal_ + har_ (log vol) + px_ (range) + flow_ (~67)
    "pool__basic",            # drv_* derived microstructure (~62) + raw levels the linear models skip
    "pool__market_context",   # mctx_ VN-Index/VN30, glb_ US risk LAGGED one session, bond_ (~60)
    "pool__market_breadth",   # cross-sectional dispersion (~7)
    "pool__news_daily",       # disclosure counts (~14)
)

# The channel blocks, by NAME PREFIX (`model.event_linear` `columns`).
# ⚠️ `mkt_` is spelt out: `pool__market_breadth` also names its channels `mkt_xs_*`, and the
# CV that chose these blocks did not include them.
_EVT = ("evt_", "sec_", "mkt_rate_", "mkt_ret_h", "mkt_disp_h", "cal_")
_DRV = ("drv_",)
_NOT_DRV = ("drv_vwap_raw",)   # a price LEVEL wearing a derived name
_HAR = ("har_",)
_PXFLOW = ("px_", "flow_")
_GLOBAL = ("glb_", "bond_")
_MCTX = ("mctx_",)

TABULAR_MODELS = (
    ("baseline", "_prior", {"type": "BASELINE", "kind": "prior"}, {}),
    # magnitude: ridge on log|r_5| — CV10 0.667 on the pipeline's channels (har+evt+drv, alpha 100, 4-year half-life)
    ("event_linear", "_mag_har_a100_hl4", {"type": "EVENT_LINEAR", "kind": "magnitude_ridge",
                                           "columns": list(_HAR + _EVT + _DRV), "alpha": 100.0,
                                           "half_life_years": 4.0, "exclude": list(_NOT_DRV)}, {}),
    # magnitude without the har block — CV10 0.665
    ("event_linear", "_mag_a10_hl4", {"type": "EVENT_LINEAR", "kind": "magnitude_ridge",
                                      "columns": list(_EVT + _DRV), "alpha": 10.0,
                                      "half_life_years": 4.0, "exclude": list(_NOT_DRV)}, {}),
    # the event itself: L2 logistic — CV10 0.670 (C 0.03), 0.669 (C 0.1)
    ("event_linear", "_evt_c003", {"type": "EVENT_LINEAR", "kind": "event_logit",
                                   "columns": list(_EVT + _DRV), "C": 0.03,
                                   "exclude": list(_NOT_DRV)}, {}),
    ("event_linear", "_evt_c01", {"type": "EVENT_LINEAR", "kind": "event_logit",
                                  "columns": list(_EVT + _DRV), "C": 0.1,
                                  "exclude": list(_NOT_DRV)}, {}),
    # direction given a >= 3 % move — CV10 0.607 as a stand-alone event score (0.638 in research, TZL-1)
    ("event_linear", "_dir_c001", {"type": "EVENT_LINEAR", "kind": "direction_logit",
                                   "columns": list(_EVT + _DRV + _PXFLOW + _GLOBAL), "C": 0.01,
                                   "min_move": 0.03, "exclude": list(_NOT_DRV)}, {}),
    # ⚠️ ADDED 2026-09-17, AFTER THE FIRST TABULAR TRIAL, ON CV EVIDENCE ONLY (§6c): XGBoost
    # fitted on the 20 BANK names (`model.event_panel`, peers read from unified_schema_bank)
    # and scored on VCB — CV10 0.675, the best single model of a 70-candidate second search;
    # its fold errors sit in other years than the linear kinds'. RUNBOOK G6 builds its pools.
    ("event_panel", "_xgb_d2_n600", {"type": "EVENT_PANEL_XGB", "universe": "BANK",
                                     "columns": list(_HAR + _EVT + _DRV + _PXFLOW + _MCTX + _GLOBAL),
                                     "exclude": list(_NOT_DRV), "n_estimators": 600,
                                     "max_depth": 2, "learning_rate": 0.03, "subsample": 0.8,
                                     "colsample_bytree": 0.5, "min_child_weight": 20.0}, {}),
    # the tree families on the same last-row table, for comparison
    ("gbt", "_d2", {"type": "GBT", "max_depth": 2, "n_estimators": 300, "learning_rate": 0.03,
                    "subsample": 0.8, "colsample_bytree": 0.5, "min_child_weight": 20.0}, {}),
    ("forest", "_et_leaf30", {"type": "FOREST", "kind": "et", "min_samples_leaf": 30,
                              "max_features": 0.3, "n_estimators": 500}, {}),
)

# ⚠️ FIXED BEFORE THE CHAIN RAN: a geometric mean of member probabilities, members named by
# run-name prefix. Not chosen on val, so it is eligible as "best on val" like any single run.
#
# ⚠️ RE-MEASURED ON THE PIPELINE'S OWN CHANNELS before the chain ran (CV10, 2014-2023): the
# magnitude ridge 0.667, the event logit 0.670, the direction logit 0.607 — down from 0.638
# in the research harness, whose US series were joined on the SAME DATE (`TZL-1`) — and the
# geometric means geo3 0.682, geo2 0.683.
TABULAR_ENSEMBLES = {
    "ensemble_geo3": ("event_linear_mag_har_a100_hl4", "event_linear_evt_c003",
                      "event_linear_dir_c001"),
    "ensemble_geo2": ("event_linear_mag_har_a100_hl4", "event_linear_evt_c003"),
    # ⚠️ ADDED 2026-09-17 with the panel member, on CV10 alone: geo4 0.695 (fold min 0.534),
    # geo3p 0.697 (0.532), against geo3 0.682 (0.504). A greedy search over the same pool
    # reached 0.703 in-sample and 0.647 leave-one-year-out, so no weights were fitted.
    "ensemble_geo4": ("event_linear_mag_har_a100_hl4", "event_linear_evt_c003",
                      "event_linear_dir_c001", "event_panel_xgb_d2_n600"),
    "ensemble_geo3p": ("event_linear_mag_har_a100_hl4", "event_linear_evt_c003",
                       "event_panel_xgb_d2_n600"),
}

# The chain's two setups. `window` is every trial before 2026-09-17, unchanged.
SETUPS = {
    "window": dict(lookback=LOOKBACK, pools=POOLS, models=MODELS, scope=None, channels="shortlist",
                   aux_targets=(), ensembles={}),
    "tabular": dict(lookback=TABULAR_LOOKBACK, pools=TABULAR_POOLS, models=TABULAR_MODELS,
                    scope=TABULAR_SCOPE, channels="all", aux_targets=("return_{h}day",),
                    ensembles=TABULAR_ENSEMBLES),
}
