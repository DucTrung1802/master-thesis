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
