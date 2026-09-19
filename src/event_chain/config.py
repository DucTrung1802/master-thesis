"""The event chain's knobs. ⚠️ The EVENT itself (gain %, horizon, rule) is NOT here — it is
`utils.event_target`, because `pool__targets`, `gold.stocks_event_features` and
`feature_selection.run` read it too, and a second copy would be the one that drifts.
"""

from __future__ import annotations

import os

from utils import event_target

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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

# Channels the one-feature logistic baseline tries, first present wins.
BASELINE_CHANNELS = (
    "evt_thr_z_20", "evt_vol_20", "drv_realized_vol_10", "drv_parkinson_21",
    "drv_garman_klass_21",
)

# Draws for the event ROC-AUC null in the report (block = d + h). Cheap: a vector shuffle.
REPORT_NULL_DRAWS = 1000

# ⚠️ THE ROLLING REFIT (2026-09-17): every refittable run is refitted every this many TEST
# sessions on all rows before the block (purged `d + h - 1`), and the blocks' scores are
# pooled into one test AUC — how the model would be used, where the train-only fit is
# frozen two years before the test starts. Fixed at one month BEFORE any rolling number
# was computed; it selects nothing.
ROLLING_REFIT_SESSIONS = 21


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
# ══════════════════════════════════════════════════════════════════════════════════════
# ⚠️ ONE LOSS FOR THE WHOLE GRID (2026-09-19, the user's decision)
#
# **Every member minimises BINARY CROSS-ENTROPY on the 0/1 event label**, unweighted, with
# no `scale_pos_weight`. The grid used to carry three: log-loss on the event (5 members),
# squared error on `log|log(1+r_h)|` (2), and log-loss on a DIFFERENT label (1).
#
# ⚠️ **WHY LOG-LOSS AND NOT A RANKING LOSS**, which matches the decision better on its face:
# the basket's decision has two halves and both were measured (`measure_auc_split.py`,
# 2026-09-18) — **84 % of the edge is choosing the NAME inside a session** (a rank) and
# **16 % is choosing the SESSION** through the `min_prob` cut (a LEVEL). Log-loss is a
# proper scoring rule, so one number serves both. `rank:pairwise` is invariant to any
# monotone transform inside a query, so its scores do not travel across sessions and the
# cut — that 16 % — cannot be made at all. **The one thing that would flip this is dropping
# the cut** and trading a fixed basket every session.
#
# ⚠️ **NO CLASS WEIGHTING.** `model/gbt/model.py` already records why: re-weighting the rare
# class DE-CALIBRATES the probability, and `min_prob` is read as a probability.
#
# ⚠️ **THREE OF THE ELEVEN MEMBERS STILL CANNOT EARLY-STOP, AND THAT IS A PROPERTY OF THE
# FAMILY**: a forest is one round (`xgbrf`: `num_parallel_tree` at full step size), a
# logistic is convex with `C` as its capacity knob, and the prior does not fit. They write
# a one-row `loss_history.csv`, which is the honest answer and not a gap.
EARLY_STOPPING_ROUNDS = 50

# ⚠️ **VAL CARRIES THREE JOBS NOW** (2026-09-19, the user's decision, taken over the
# alternative of carving the stop block out of train): it chooses the model
# (`BASKET_CHOOSE_ON`), it chooses `min_prob`, and it stops the boosting. **So
# `val_daily_auc` is a selection score and no longer an estimate of anything out of
# sample** — TEST is the only honest read, and the trial log says so in the verdict.
# ⚠️ **THE REFIT PATHS HAVE NO VAL** (`report.refit_test` fits on train+val, the rolling
# refit on everything before the block), so they cannot stop again: each refit reuses the
# ROUND this fit chose, frozen from `training.best_epoch`. One mode, two paths.

# ⚠️ **d > 1 SINCE 2026-09-19 (the user's decision: keep the deep-learning models).** At
# `d = 1` a sequence model has nothing to read, so LSTM/TCN could not be members at all.
# ⚠️ **THE BILL IS REAL AND IS NOT THE DL TRAINING**: the window tensor is `d` times bigger
# (420k x 10 x 151 is 2.5 GB as float32 and the engine casts to **float64**, so ~5 GB), the
# selection layer builds six statistics per channel instead of collapsing to one (`WST-1`
# short-circuits `d == 1` ONLY), and `gbt`/`forest` go back to a legitimate 906-column
# design — the 1,320 s -> 77 s the same fix bought is spent again here. Budget a re-run of
# selection, the final table, the dataset and the grid.
# ⚠️ **10 IS A CHOICE, NOT A STANDARD**: the repo's documented pairing is `d = 20, h = 5`
# (purge 24), measured on a ONE-TICKER series of ~4k rows, not a 420k-row panel. 10 gives
# two horizons of context at half the tensor; the purge is `d + h - 1 = 14` (§5 rule 6).
BASKET_LOOKBACK = 10
# The channel blocks, by NAME PREFIX (`model.event_linear` `columns`).
# ⚠️ `mkt_` is spelt out: `pool__market_breadth` also names its channels `mkt_xs_*`, and the
# CV that chose these blocks did not include them.
_EVT = ("evt_", "sec_", "mkt_rate_", "mkt_ret_h", "mkt_disp_h", "cal_")
_DRV = ("drv_",)
_NOT_DRV = ("drv_vwap_raw",)   # a price LEVEL wearing a derived name
_HAR = ("har_",)
_PXFLOW = ("px_", "flow_")
_GLOBAL = ("glb_", "bond_")

# ══════════════════════════════════════════════════════════════════════════════════════
# ⚠️ THE MBB SETUP (2026-09-17) — `python -m event_chain --setup tabular_mbb`
#
# The tabular chain on MBB (HOSE), with a grid chosen for MBB by the same kind of
# rolling-origin CV (validation years 2015-2024 of the train+val rows, no test row read):
# `.claude/context/event_chain.md` §7. The first MBB trial ran `tabular` unchanged — VCB's
# grid — and its val-chosen panel scored CV 0.665. Measured, in CV10 order:
#
# | change | CV10 |
# |---|---|
# | VCB's panel member (`har/evt/drv/px/flow` + `mctx_` + `glb_`/`bond_`) | 0.665 |
# | the same panel WITHOUT `mctx_`/`glb_`/`bond_` (`_PANEL`) | **0.723** |
# | ... depth 3 | 0.733 (seeds 1-3: 0.719-0.729) |
# | an L2 logit on the same panel rows, C 0.03, 4-year half-life | 0.718 (worst fold 0.559) |
# | geometric mean of the depth-3 tree and the logit | **0.739** |
#
# ⚠️ WHY THE MARKET BLOCKS HURT A PANEL: a date-level channel is identical for all 20 banks
# on a day, so a tree can isolate DATES and learn their event rate — memorising the calendar
# of rallies rather than a state that recurs. The own-ticker linear kinds keep them.
# ⚠️ `cal_` is date-level too and is NOT removable: without it CV 0.613, without the two Tet
# channels 0.688 — a seasonal effect that recurs every year, unlike a rally's date.
# ⚠️ Measured and REJECTED: VN30 or BANK+VN30 as peers (0.683-0.691), dropping the
# state-owned or the recent listings (0.667-0.717), `own_weight` 3-10, half-lives, depth 1,
# `pool__basic`'s `drv_` block (it FAILED MBB's selection null; with it 0.726, noise),
# `pool__ta` (failed its null, z +0.42), the own-ticker linear kinds in the ensemble
# (0.709-0.722, below the panel pair alone).
# ══════════════════════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════════════════════
# ⚠️ THE BASKET SETUP (2026-09-17) — `python -m event_chain --setup basket`
#
# The question changes from "will THIS ticker rise +g % in h sessions?" to "WHICH names
# should I buy at the close of day N — at most `BASKET_SIZE` of them — so that each is
# +g % at the close of N+h?". The same label, the same stages, on a PANEL universe
# (`unified_schema_liquid`, 228 tradeable names), scored per DATE: the top `BASKET_SIZE`
# scores of a session are its basket (`event_chain.basket`).
#
# ⚠️ WHY LIQUID AND NOT VN30: CLAUDE.md §2 measured the cross-sectional rank failing at
# 30 names and clearing at 100+; LIQUID is the narrowest screen above that line whose
# every name trades (1 bn VND median turnover). ⚠️ It is a SURVIVOR universe
# (`STILL_TRADING_2026_06`) and its membership is not point-in-time (§2c).
#
# ⚠️ EVERY KNOB BELOW WAS FIXED BEFORE ANY BASKET NUMBER WAS READ — the model kinds and
# their hyper-parameters are the VCB tabular CV's (§6), carried over unchanged, because a
# panel CV of 700k rows per fold was not run. The one change is scale: the forest's leaf
# floor grows with the row count (30 rows of 3k -> 200 rows of 500k).
# ══════════════════════════════════════════════════════════════════════════════════════
BASKET_TICKER = "LIQUID"
# X — how many names a session's basket holds. ⚠️ A PARAMETER, `--top-k` overrides it; the
# model is fitted once and the basket is cut from its scores, so X moves no fit.
BASKET_SIZE = 5
BASKET_SCOPE = "bsk"
# ⚠️ NO DATE-LEVEL POOL (`pool__market_context`, `pool__market_breadth`): a panel selection
# ranks every channel WITHIN a session (`feature_normalize="cs_rank"`), where a channel equal for
# every name is a constant — it cannot rank names by itself and cannot clear a null, so offering
# it would spend ~15 min of selection per pool on a foregone answer. Both are materialised for
# LIQUID (RUNBOOK G8) should a tree-interaction test want them.
BASKET_POOLS = (
    "pool__event_features",   # evt_/sec_/mkt_/cal_/har_/px_/flow_ — the event's own state
    "pool__basic",            # drv_* microstructure
)
# ⚠️ A NAME AT ITS CEILING ON N IS BUYABLE (the user's decision, 2026-09-17): an order at the
# ceiling price can still fill when sellers remain, so the basket does NOT drop those names.
# `True` restores the repo's `PRF-0` entry screen (`backtest.portfolio.mark_ceiling`); the flag
# is still carried per row (`at_ceiling_n`) so a report can say how many picks were at the ceiling.
BASKET_EXCLUDE_CEILING = False
# The within-date shuffle null of the basket hit rate (picks drawn at random from the
# same session's buyable names). 200 = the headline chain's convention (CLAUDE.md §6).
BASKET_NULL_DRAWS = 200
# ⚠️ ONE REFIT A QUARTER, NOT A MONTH: a panel refit fits ~600k rows, ~100x a
# single-ticker one, so the monthly schedule of `ROLLING_REFIT_SESSIONS` would cost hours
# per model. Fixed before any rolling number was computed; it selects nothing.
BASKET_REFIT_SESSIONS = 63
# The round-trip cost the trading track charges, and the sweep beside it — the repo's
# own constants (`backtest.portfolio.ROUND_TRIP_COST` 50 bps, `COST_SWEEP`).
BASKET_COSTS = (0.0, 0.0030, 0.0050)
# Estimators too slow to refit ~9 times on 600k rows; refitted once on train+val only.
BASKET_NO_ROLLING = ("FOREST",)
# ⚠️ THE CHOICE RULE (2026-09-18): the WITHIN-SESSION ROC-AUC, averaged over sessions — the
# only AUC this basket can trade. A POOLED AUC also scores name-sessions against each other
# ACROSS sessions, and no basket ever makes that comparison: at the close of N the decision is
# which of THAT session's names to buy. The gap between the two is the part of the ranking that
# is pure timing — measured 0.654 pooled vs 0.631 within-session on the first tradeable trial,
# so 0.023 of the pooled figure was a question nobody asks. Ties: pooled AUC, then val hit@X.
# ⚠️ ITS HISTORY, because a choice rule changed after a test read is `NUL-1`'s shape: the first
# basket trial chose on `val_hit`, the second and third on `val_auc` (pooled), and this is the
# fourth rule. The trial log keeps every row, so the earlier choices stay readable.
BASKET_CHOOSE_ON = "val_daily_auc"   # "val_auc" | "val_daily_auc" | "val_hit"
# ⚠️ A BASKET MAY HOLD FEWER THAN X NAMES (2026-09-17, the user's request): a session's basket
# is its top X names whose P(event) is at least `min_prob`, and a session none of whose top X
# reaches the cut holds CASH. The cut is chosen on VAL (the chosen row's frozen val scores) over
# `BASKET_MIN_PROB_GRID`, among cuts that trade at least `BASKET_MIN_ACTIVE` val sessions, by
# `BASKET_MIN_PROB_ON`:
#   "ev" — the mean net h-session return per SESSION (a cash session returns 0) at the last
#          `BASKET_COSTS` round trip; ties go to the lower cut (the fuller basket).
# ⚠️ A SECOND RULE WAS DECLARED BESIDE IT AND READ ON TEST — the largest one-sided 95 % lower
# bound of the per-session hit rate, n_eff = active sessions / h. It chose 0.59, which traded 4-5
# TEST sessions (one basket), and was dropped for that: two rules were read on test (NUL-1,
# `event_chain.md` §6-§7). `--min-prob` overrides the cut at pick time.
# ⚠️ WHEN THE BASKET IS BOUGHT (2026-09-18, the user's execution): "close" prices every basket at
# the CLOSE of session N — the label's own price, and a fill nobody who reads the close can get —
# while "next_open" prices it at the OPEN of N+1 and sells at the close of N+h, which is what an
# evening scrape and a morning order can actually trade. It moves the MONEY metrics only: the
# label, the AUCs and the model choice are close-to-close either way (`event_chain.md` §7; `EXE-1`).
# ⚠️ THE PICKS GAP UP OVERNIGHT (+1.7 % on average, +4.4 % on the names at their ceiling), so the
# two pricings are not a detail: "next_open" also wants `BASKET_EXCLUDE_CEILING = True`.
BASKET_ENTRY = "next_open"   # "close" | "next_open"
BASKET_MIN_PROB_GRID = tuple(round(0.10 + 0.01 * i, 2) for i in range(51))   # 0.10 .. 0.60
BASKET_MIN_PROB_ON = "ev"
BASKET_MIN_ACTIVE = 25   # 5 non-overlapping baskets at h = 5

# ⚠️ XGBoost on the panel's last row (`model.event_boost`), chosen by a rolling-origin CV over
# the LIQUID train+val rows (validation years 2016-2023, the CV of the deleted close-priced flow, `event_chain.md` §6; no test row read):
# depth 4/6/8/10 gave pooled AUC 0.667/0.666/0.668/0.665; lr 0.01 x 1,600 trees, a 4-year
# half-life, lambda 5 and 128 bins moved it by <= 0.002; date-level pools LOWERED it (0.661, the
# date memorisation of `PDL-1`), within-session ranks and exchange-band / limit-hit channels
# added nothing (0.664, 0.668), and a pairwise/ndcg ranker scored 0.650 pooled, 0.659-0.662 within.
_BOOST = {"type": "EVENT_BOOST", "kind": "xgb", "max_depth": 8, "n_estimators": 2000,
          "learning_rate": 0.02, "subsample": 0.8, "colsample_bytree": 0.4,
          "min_child_weight": 500.0, "device": "cuda", "seed": 42,
          # ⚠️ 800 was the CV's CHOICE; with early stopping it is a cap and the val curve
          # picks the round. Raised so the cap cannot bind silently — `training.best_epoch`
          # records what was taken, and a run that stops at 1,999 is a run to re-read.
          "early_stopping_rounds": EARLY_STOPPING_ROUNDS}

# ⚠️ THE TORCH TRAINING BLOCK, recovered verbatim from the windowed grid this chain deleted
# on 2026-09-18 (`git show 1593f663^:src/event_chain/config.py`). It is the one family that
# has ALWAYS early-stopped on validation loss — `model/common/trainer.py` restores the best
# epoch — so the 2026-09-19 requirement is met natively here and bolted on everywhere else.
_TRAIN = dict(batch_size=64, lr=0.001, weight_decay=0.0001, max_epochs=100,
              patience=15, grad_clip=1.0, lr_factor=0.5, lr_patience=5, log_every=10)

# ⚠️ **THE DEEP-LEARNING MEMBERS, ADDED 2026-09-19 (the user's decision).** They are the
# reason `BASKET_LOOKBACK` left 1: a sequence model at `d = 1` reads a single row and is an
# MLP with extra machinery. ⚠️ **THE WINDOWED GRID THEY COME FROM LOST** — 15 models over 3
# VCB trials put their val-chosen model at test AUC 0.520-0.564 against a block-shuffled
# null p95 of 0.646-0.661 (`CLAUDE.md` §2) — but that was ONE TICKER's ~4k rows, and this is
# a 228-name panel with 420k. **That difference is the whole hypothesis; nothing here has
# been measured on this panel yet.**
# ⚠️ **AND THEY CANNOT BE REFITTED BY TODAY'S REPORT**: `report.REFITTABLE` covers
# estimators that are `build_model` + `fit` with no training loop, so the refit and rolling
# columns are BLANK for these rows rather than wrong — read the frozen test column for them.
BASKET_DL_MODELS = (
    ("mlp", "_h32", {"type": "MLP", "hidden_size": 32, "dropout": 0.2}, {"train": _TRAIN}),
    ("lstm", "_h32", {"type": "LSTM", "hidden_size": 32, "num_layers": 1, "dropout": 0.2},
     {"train": _TRAIN}),
    ("tcn", "_c32", {"type": "TCN", "channels": 32, "kernel_size": 3, "num_layers": 2,
                     "dropout": 0.2}, {"train": _TRAIN}),
)

BASKET_MODELS = (
    ("baseline", "_prior", {"type": "BASELINE", "kind": "prior"}, {}),
    # ⚠️ THE THREE LINEAR KINDS, chosen on a VCB CV10 before any test row was read (0.667 /
    # 0.670 / 0.607) and kept through every basket trial since. Their channel blocks are the
    # `_EVT` / `_DRV` / `_HAR` / `_PXFLOW` / `_GLOBAL` prefixes above.
    # ⚠️ **WAS `_mag_har_a100_hl4`, A RIDGE ON `log|log(1+r_h)|` WITH A 4-YEAR HALF-LIFE**,
    # until the one-loss decision of 2026-09-19. It kept its CHANNELS (the `har_` block is
    # in the pool because of it) and lost its loss: squared error on a volatility proxy is
    # not the event's loss, and the age weight made it a WEIGHTED log-loss beside unweighted
    # members — a fourth loss hiding in a config key. ⚠️ **THIS IS AN EXPERIMENT, NOT A
    # RENAME**: the magnitude route measured CV AUC ~0.67 on VCB and 0.6180 val within-session
    # AUC on the last basket trial, and what the HAR block is worth under log-loss has never
    # been measured. `kind='magnitude_ridge'` stays in `model.event_linear` for that test.
    ("event_linear", "_har_c003", {"type": "EVENT_LINEAR", "kind": "event_logit",
                                   "columns": list(_HAR + _EVT + _DRV), "C": 0.03,
                                   "exclude": list(_NOT_DRV)}, {}),
    ("event_linear", "_evt_c003", {"type": "EVENT_LINEAR", "kind": "event_logit",
                                   "columns": list(_EVT + _DRV), "C": 0.03,
                                   "exclude": list(_NOT_DRV)}, {}),
    # ⚠️ `_dir_c001` (`direction_logit`) WAS HERE AND IS GONE (2026-09-19). It is the only
    # member that fitted a DIFFERENT LABEL — `1{r>0}` on the rows whose move cleared 3 % —
    # so one loss could not cover it, and it had already measured nothing: val within-session
    # AUC **0.5007**, test 0.5021, z **+0.68** on the `uphold` trial. Both facts point the
    # same way, which is the only reason to drop a member that a CV once chose.
    # the tree families on the panel's last row
    # ⚠️ `device: cuda` since 2026-09-19 — the panel is 151 columns and 420k rows, where the
    # card wins; `model/gbt/model.py`'s docstring carries what that changes (a SAMPLED
    # XGBoost draws from a different RNG stream on CUDA, so these are not the CPU runs made
    # faster — they are different trees, and `device` is part of the experimental setup).
    # ⚠️ `n_estimators` IS A CAP NOW, NOT A CHOICE (2026-09-19): with `early_stopping_rounds`
    # the val curve picks the round, so 300 was a ceiling the fit could hit without anyone
    # seeing it. Raised to 2,000 for that reason and for no other — a cap that BINDS is a
    # hyper-parameter pretending to be a limit, and `training.best_epoch` now records which
    # round was actually taken.
    ("gbt", "_d2", {"type": "GBT", "max_depth": 2, "n_estimators": 2000, "learning_rate": 0.03,
                    "subsample": 0.8, "colsample_bytree": 0.5, "min_child_weight": 200.0,
                    "device": "cuda", "early_stopping_rounds": EARLY_STOPPING_ROUNDS}, {}),
    ("gbt", "_d4", {"type": "GBT", "max_depth": 4, "n_estimators": 2000, "learning_rate": 0.03,
                    "subsample": 0.8, "colsample_bytree": 0.5, "min_child_weight": 200.0,
                    "device": "cuda", "early_stopping_rounds": EARLY_STOPPING_ROUNDS}, {}),
    # ⚠️ WAS `_et_leaf200` (sklearn ExtraTrees, 1,103.8 s of CPU = 84 % of the grid's whole
    # fit time) until 2026-09-19. `xgbrf` is XGBoost's own random forest, matched to it at
    # 200 trees: **371.6 s, a 3x cut** — and ⚠️ **the card is NOT why**: the same fit is
    # 372.6 s on the host. It is a DIFFERENT estimator, not a faster one, and
    # `model/forest/model.py`'s docstring has the table and what does not carry over.
    ("forest", "_rf200_d12", {"type": "FOREST", "kind": "xgbrf", "n_estimators": 200,
                              "max_depth": 12, "min_child_weight": 50.0,
                              "max_features": 0.3, "device": "cuda"}, {}),
    # ⚠️ ADDED FOR THE SECOND BASKET TRIAL on the CV above — CV pooled AUC 0.668 / within 0.666
    ("event_boost", "_xgb_d8", dict(_BOOST), {}),
    # ⚠️ `_mag_d8` (`kind='magnitude'`, `reg:squarederror`) WAS HERE AND IS GONE (2026-09-19,
    # the one-loss decision). ⚠️ **AND IT HAD ALREADY VANISHED ONCE WITHOUT ANYONE NOTICING**:
    # on the `uphold` trial it FITTED (run folder `…__20260919-033648`, 94.5 s) and finished
    # 46 s AFTER `event_chain.report` had read `index.csv`, so the board held 8 of the 9
    # declared models and `ensemble_boost` / `ensemble_boost_eq` were skipped on one log line
    # that scrolled past. `report.leaderboard` raises on that gap now (`BRD-1`).
) + BASKET_DL_MODELS

# ⚠️ FIXED BEFORE THE CHAIN RAN, no weights fitted: VCB's `geo3`/`geo2`, and the same two
# with the depth-4 tree as a member (a panel is where interactions have rows to be learned).
BASKET_ENSEMBLES = {
    # ⚠️ WAS FIVE MORE, AND THE ONE-LOSS GRID TOOK THREE OF THEM (2026-09-19). An ensemble
    # here is a FIXED geometric mean, declared before the chain ran and never refitted, so
    # one whose member has left the grid cannot be "adjusted" — it is a different ensemble
    # and is deleted with its member. `ensemble_geo3` needed `event_linear_dir_c001`;
    # `ensemble_boost` and `ensemble_boost_eq` needed `event_boost_mag_d8`.
    # ⚠️ The two survivors below swapped `event_linear_mag_har_a100_hl4` for its log-loss
    # successor `event_linear_har_c003` — **the same CHANNELS under a different loss, so
    # their CV numbers do not carry over** and are recorded as history, not as a bar.
    "ensemble_geo2": ("event_linear_har_c003", "event_linear_evt_c003"),    # was CV 0.683
    "ensemble_geo2t": ("event_linear_har_c003", "event_linear_evt_c003", "gbt_d4"),
    # ⚠️ ADDED FOR THE SECOND BASKET TRIAL, on CV alone (pooled AUC · within-session AUC). A
    # member LISTED TWICE COUNTS TWICE in the geometric mean — the weights are the CV's
    # (xgb 1/2, event logit 1/4), fixed before the chain scored them. This one is untouched
    # by the loss change, and it is the row the last two trials chose.
    "ensemble_xl": ("event_boost_xgb_d8", "event_boost_xgb_d8",
                    "event_linear_evt_c003"),                                   # CV ~0.672 · 0.667
    # ⚠️ NEW 2026-09-19, and declared HERE, before any d>1 row has been scored: the same
    # shape as `ensemble_xl` with the sequence model in the event logit's place. If it is
    # added after reading a test score it is `NUL-1`, so it is added now or never.
    "ensemble_xl_seq": ("event_boost_xgb_d8", "event_boost_xgb_d8", "lstm_h32"),
}

# The chain's setup. It names its `ticker` (the LIQUID panel) and runs on it unless
# `--ticker` says otherwise.
SETUPS = {
    # ⚠️ ONE SETUP SINCE 2026-09-18. `window`, `tabular` and `tabular_mbb` — VCB and MBB,
    # close-to-close — were deleted with the label they were trained on (`event_chain.md` §6:
    # the close of N is not a fill). `top_k` is what makes a setup a BASKET: its report is
    # `event_chain.basket`, scored per date, at most `top_k` names and cash below the cut.
    "basket": dict(ticker=BASKET_TICKER, lookback=BASKET_LOOKBACK, pools=BASKET_POOLS,
                   models=BASKET_MODELS, scope=BASKET_SCOPE, channels="all",
                   aux_targets=(event_target.RULE_RETURN[event_target.EVENT_RULE],),
                   ensembles=BASKET_ENSEMBLES,
                   top_k=BASKET_SIZE),
}
