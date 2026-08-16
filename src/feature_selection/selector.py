# src\feature_selection\selector.py
"""Rank the joined features against one target, then prune the redundant ones.

The output is a `SelectionResult`: a per-method score table, an ensemble ranking,
the kept feature list, and the walk-forward evidence for whether keeping them was
worth anything.

Six rankers, chosen so that no single model's inductive bias decides the answer:

| method | sees | blind to |
|---|---|---|
| `spearman` | monotone rank association | interactions, non-monotone shapes |
| `mutual_info` | any dependence, including non-monotone | direction |
| `xgb_gain` | interactions, thresholds | correlated features share credit arbitrarily |
| `xgb_shap` | the same, attributed per sample | the same correlation problem, less arbitrarily |
| `lasso` | linear signal, with redundancy already priced in | non-linearity |
| `permutation` | out-of-sample contribution to a fitted model | features the model never used |

⚠️ **`permutation` is the only one measured OUT OF SAMPLE**, and it is the one to
believe when it disagrees. The other five are fitted on the whole labelled sample
and answer "what does this data support", not "what generalises". They are kept
because on ~4k rows an out-of-sample-only ranking is noisy, and the ensemble is a
rank average — a feature that only one method likes does not survive it.

## The three things that make this correct rather than plausible

**1. The label is FORWARD-looking, so the CV must be purged.**
`return_5day = close[t+5]/close[t] - 1` means the row at `t` embeds prices up to
`t+5`. A random K-fold puts `t+1` in train and `t` in test and the model reads its
own answer. `PurgedWalkForward` is expanding-window, in date order, and drops the
`horizon` rows of train immediately before each test block. Skipping this is not a
subtlety — it is the difference between an R² of 0.4 and the truth.

**2. Overlapping labels mean the effective sample is n/horizon, not n.**
Consecutive `return_5day` values share 4 of their 5 days. Nothing here can fix
that; what it does is refuse to report a t-statistic that pretends otherwise, and
report the per-fold spread instead of one number.

**3. Imputation is fitted on train only.** A median over the whole column is a
value computed from the test period. Inside the CV every fold imputes from its own
train slice. The whole-sample ranking uses a whole-sample median and says so —
that ranking is descriptive, and the walk-forward numbers are the ones that carry
a generalisation claim.

⚠️ **Constant columns are dropped, not scored.** For a single-ticker schema
`exchange`, `ticker` and the eight GICS columns never vary — a "feature" with one
value is not weakly predictive, it is not a feature. They are reported in
`dropped_constant` so the drop is visible rather than assumed.
"""

import time
import warnings
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from scipy import stats
from sklearn.exceptions import ConvergenceWarning
from sklearn.feature_selection import mutual_info_regression
from sklearn.linear_model import LassoCV
from sklearn.preprocessing import StandardScaler

from feature_selection import gpu, gpu_rankers, windows
from feature_selection.windows import WINDOW_STATS

# ── the rankers ───────────────────────────────────────────────────────────────
# Every ranker implemented here. ⚠️ **NOT the default ensemble** — `METHODS` is, and
# it is a MEASURED SUBSET (§19). This tuple exists so a run archived under a different
# default is still read with ITS OWN columns: `selection_cut.live_methods` iterates
# this, not the default, because a `mutual_info` column in a 2026-08 run folder is a
# method that ran and must not be silently ignored by a later, narrower default.
ALL_METHODS = (
    "spearman", "mutual_info", "xgb_gain", "xgb_shap", "lasso", "permutation",
)

# ⚠️ **THE DEFAULT ENSEMBLE — THREE MEMBERS SINCE 2026-08-16, MEASURED (§19).** It was
# all six of `ALL_METHODS` from 2026-08-03. Three were dropped from the DEFAULT, none
# deleted: pass `methods=ALL_METHODS` to reproduce an older run exactly.
#
#   dropped        why, measured on VCB pool__basic (84 channels), return_5day AND
#                  return_rel_5day, k=10 and k=20, against a 40-draw random-k control
#   ─────────────  ──────────────────────────────────────────────────────────────────
#   lasso          **87.2 % of the average archived run's wall clock** (90-96 % on the
#                  19 country runs) — and it ranks at CHANCE (52nd percentile, min
#                  2.5th). On a return target it zeroes every coefficient, so its rank
#                  column is a CONSTANT and `ensemble` is bit-identical with and
#                  without it. It is the whole of the 13.7x target-cost gap in
#                  CLAUDE.md §15c-target.
#   mutual_info    the WORST standalone ranker measured (35.5th percentile mean, 7.5th
#                  min — below chance) and the most expensive once lasso is gone
#                  (46 % of ranking time on a return target). Its unique claim, "any
#                  dependence, model-free", is largely covered by the tree members.
#   xgb_gain       a STRUCTURAL DUPLICATE of `xgb_shap` — rho = 0.864 across the
#                  archive, from THE SAME FIT — so the blend gave one model 2 of 6
#                  votes. Second worst standalone (42nd percentile, 25th min), and §4
#                  already said gain splits credit arbitrarily where SHAP does not.
#
# ⚠️ **`permutation` is the one member that cannot be dropped.** Every other
# leave-one-out subset scored at or ABOVE the full six; `ensemble - permutation` was
# the only one clearly below it, at the 55th percentile against chance's 50th, in all
# four target x k cells.
METHODS = ("spearman", "xgb_shap", "permutation")

# ── progress ──────────────────────────────────────────────────────────────────
# ⚠️ **A MODULE FLAG, NOT A CONSTRUCTOR ARGUMENT, AND DELIBERATELY SO.** A knob on
# `FeatureSelector` invites being recorded in `SelectionResult.setup` — and `setup`
# is two thirds of the key `final_features.plan_from_reports` groups runs by. One
# extra entry there moves every existing table's fingerprint and reports the whole
# chain below it STALE (the STL-1 domino), for a change that alters no number.
# Progress is an output-formatting choice; it must not be able to reach the record.
PROGRESS = True

# The phases `run()` announces, in order. ⚠️ The percentage is a fraction of PHASES
# COMPLETED and says so on every line — the phases are NOT equal in cost
# (`permutation` alone is 12,255 s at 1,458 channels), so this is a position in the
# run, never an estimate of time remaining. The honest time fraction in this package
# is the null's draw counter, where every unit is the same procedure.
_RUN_PHASES = (
    "prepare + coverage",
    "window design",
    "spearman vs target",
    "rank (the ensemble's methods)",
    "aggregate + blend",
    "channel corr matrix",
    "prune",
    "stability",
    "walk-forward",
)


@contextmanager
def silenced():
    """Suppress phase progress inside this block — used per null draw."""
    global PROGRESS
    previous, PROGRESS = PROGRESS, False
    try:
        yield
    finally:
        PROGRESS = previous


def pooled_ic(prediction: np.ndarray, y: pd.Series) -> float:
    """Spearman rank correlation of prediction against realised return.

    ⚠️ Not R². For a forward equity return, R² is deeply negative for any honest
    model (a level forecast of a near-noise series) while the RANKING can still be
    useful — so R² ranks features by their contribution to something the rest of
    this module says not to decide on.

    ⚠️ **POOLED, which is the right answer for ONE ticker and the wrong one for a
    cross-section.** Over a single company's time series there is only one axis to
    correlate along. Over `N` stocks × `T` days a pooled rank correlation mixes
    "which day was good" with "which stock was good on that day", and only the
    second is tradeable — `cross_sectional.CrossSectionalSelector` overrides
    `_ic` with the per-date version for exactly that reason.
    """
    ic = stats.spearmanr(prediction, y).statistic
    return 0.0 if np.isnan(ic) else float(ic)


# --------------------------------------------------------------------- CV split


class PurgedWalkForward:
    """Expanding-window splits in date order, with a purge gap before each test.

    Fold `i` trains on `[0, train_end)` and tests on `[test_start, test_end)`, with
    `train_end = test_start - gap`.

    ## ⚠️ The gap is `lookback + horizon - 1`, not `horizon`

    Sample `N` reads inputs from rows `[N-d+1, N]` and carries the label for
    `[N, N+h]`. For a TRAINING sample at `M` to share nothing with a TEST sample at
    `N`, its label period must end before the test sample's window begins:

        M + h < N - d + 1     ⟹     N - M > d + h - 1

    So at `d=20, h=5` the gap is **24 rows, not 5** — an un-windowed purge would
    leave 19 rows of the test sample's own input window inside the training set.
    This is the single easiest way to make a windowed model look predictive when it
    is not, and it gets worse as the lookback grows.

    `lookback=1` recovers a gap of exactly `horizon`, which is the un-windowed case.

    Args:
        n_splits: number of test blocks.
        horizon: label horizon `h` in ROWS. `pool__basic` is one row per session,
            so a row offset is a trading-day offset — the same assumption
            `LEAD(close_adjust, h)` makes in the `pool__targets` asset.
        min_train: rows required in the first fold's training set.
        lookback: `d`, the window length in rows.
    """

    def __init__(
        self,
        n_splits: int = 5,
        horizon: int = 5,
        min_train: int = 500,
        lookback: int = 1,
    ):
        if n_splits < 1:
            raise ValueError("n_splits must be >= 1")
        if lookback < 1:
            raise ValueError("lookback must be >= 1")
        self.n_splits = n_splits
        self.horizon = horizon
        self.min_train = min_train
        self.lookback = lookback

    @property
    def gap(self) -> int:
        """Rows dropped between the end of train and the start of test."""
        return self.lookback + self.horizon - 1

    def split(self, n_samples: int) -> List[Tuple[np.ndarray, np.ndarray]]:
        gap = self.gap
        usable = n_samples - self.min_train - gap
        if usable < self.n_splits:
            raise ValueError(
                f"{n_samples} samples cannot be split into {self.n_splits} purged "
                f"folds with min_train={self.min_train}, horizon={self.horizon} and "
                f"lookback={self.lookback} (purge gap {gap})."
            )
        block = usable // self.n_splits
        folds = []
        for i in range(self.n_splits):
            test_start = self.min_train + gap + i * block
            test_end = test_start + block if i < self.n_splits - 1 else n_samples
            train_end = test_start - gap
            folds.append(
                (np.arange(0, train_end), np.arange(test_start, test_end))
            )
        return folds


# ------------------------------------------------------------------- the result


@dataclass
class SelectionResult:
    """Everything one selection run produced. Nothing here is recomputed lazily."""

    target: str
    features: List[str]
    scores: pd.DataFrame  # index=feature, columns=`methods` (0-1 normalised)
    ranks: pd.DataFrame  # index=feature, columns=`methods` + "ensemble"
    kept: List[str]
    dropped_constant: List[str]
    dropped_correlated: Dict[str, str]  # dropped feature -> the one it duplicated
    corr: pd.DataFrame  # feature-feature Spearman, ordered by ensemble rank
    target_corr: pd.Series  # signed Spearman of each feature vs the target
    stability: pd.DataFrame  # index=feature, columns=fold -> ensemble rank
    validation: pd.DataFrame = field(default_factory=pd.DataFrame)
    n_rows: int = 0
    corr_threshold: float = 0.9
    coverage: pd.Series = field(default_factory=pd.Series, repr=False)
    # ⚠️ The ensemble's MEMBERSHIP, recorded because it changes the answer. The
    # default narrowed from six rankers to three on 2026-08-16 (CONTEXT §19), so a
    # result that does not say which members voted cannot be compared with one from
    # before or after. Read this, never the module's current `METHODS`.
    methods: List[str] = field(default_factory=lambda: list(METHODS))
    # Methods whose raw scores were identical for every feature — they separated
    # nothing and contributed a constant to the ensemble. Reported rather than
    # dropped: "LASSO zeroed every coefficient" is a finding about the data (no
    # linear signal survives cross-validated shrinkage), not a bug to hide.
    dead_methods: List[str] = field(default_factory=list)
    device: str = "cpu"
    timings: Dict[str, float] = field(default_factory=dict)
    # --- the windowed setup. `lookback=1` is the un-windowed selector. ---
    lookback: int = 1
    horizon: int = 5
    window_stats: List[str] = field(default_factory=lambda: ["last"])
    purge_gap: int = 5
    normalize: str = "none"
    # Design columns that normalisation made constant (zscore ⇒ mean, sd).
    dropped_design_columns: List[str] = field(default_factory=list)
    # The holdout the selection never saw. 0 = none configured.
    n_holdout: int = 0
    n_purged_at_boundary: int = 0
    holdout_start: str = ""
    # Normalised scores per (channel, stat) — the detail behind the channel scores.
    design_scores: pd.DataFrame = field(default_factory=pd.DataFrame, repr=False)
    # Which window statistic carried each channel, per method.
    best_stat: pd.DataFrame = field(default_factory=pd.DataFrame, repr=False)

    @property
    def setup(self) -> pd.Series:
        """The experimental setup in one object — everything that changes an answer."""
        return pd.Series(
            {
                "target": self.target,
                "horizon_h": self.horizon,
                "lookback_d": self.lookback,
                "normalize": self.normalize,
                "purge_gap_rows": self.purge_gap,
                "window_stats": ", ".join(self.window_stats),
                "dropped_by_normalize": len(self.dropped_design_columns),
                "dev_samples": self.n_rows,
                "holdout_start": self.holdout_start or "—",
                "holdout_rows": self.n_holdout,
                "purged_at_boundary": self.n_purged_at_boundary,
                "channels": len(self.features),
                "design_columns": self.design_scores.shape[0],
                "kept": len(self.kept),
                "device": self.device,
                # ⚠️ In `setup` because it changes the answer; NOT in
                # `contract.SETUP_KEYS`, because adding a key there makes every run
                # archived without it ungroupable (issue MTH-1).
                "methods": ", ".join(self.methods),
            },
            name="setup",
        )

    def stat_profile(self, method: str = "xgb_shap", top: int = 20) -> pd.DataFrame:
        """`channel × window-stat` scores — does a channel matter through its level,
        its trend or its dispersion?"""
        return windows.stat_matrix(
            self.design_scores, self.features[:top], method, self.window_stats
        )

    @property
    def timing_table(self) -> pd.DataFrame:
        """Seconds per step, slowest first, with the device each one ran on.

        The point of showing it: it is what makes "this uses the GPU" checkable
        rather than a claim. The two CPU rows are the two rankers whose estimator
        has no GPU implementation — see `gpu.py`.
        """
        frame = pd.DataFrame(
            {"step": list(self.timings), "seconds": list(self.timings.values())}
        )
        frame["device"] = frame["step"].str.extract(r"\((\w+)\)")
        frame["step"] = frame["step"].str.replace(r"\s*\(\w+\)", "", regex=True)
        frame["share"] = frame["seconds"] / frame["seconds"].sum()
        return frame.sort_values("seconds", ascending=False).reset_index(drop=True)

    @property
    def ranking(self) -> pd.DataFrame:
        """The headline table, BEST FIRST.

        ⚠️ `ensemble` is a mean RANK, so lower is better and the sort is ascending.
        """
        out = self.ranks[["ensemble"]].join(self.scores)
        out.insert(1, "kept", out.index.isin(self.kept))
        out.insert(
            2,
            "dropped_for",
            [self.dropped_correlated.get(f, "") for f in out.index],
        )
        return out.sort_values("ensemble", ascending=True)


# ----------------------------------------------------------------- the selector


class FeatureSelector:
    """Rank → prune → validate, over one joined panel and one target column.

    Args:
        panel: the joined frame — features, the target, and a `date` column.
        target: the target column name, e.g. `"return_5day"`.
        date_col: the time index used for ordering and for the walk-forward split.
        exclude: columns that are never candidates (identity, other targets).
            Non-numeric and constant columns are excluded automatically.
        max_features: cap on the pre-prune top-N carried into the correlation prune.
            ⚠️ **`None` (the default) means no cap, and that is the honest setting.**
            A fixed cap is the same number on a 27-channel pool and a 1,458-channel
            one; §9i and §13c both measured all channels beating the pruned 12 in
            every fold. It also TRUNCATES `dropped_for` — everything below the break
            is written out as `kept=False` with no reason, so a report cannot tell
            "redundant" from "never examined". Set an integer only to reproduce an
            older run; `selection_cut.suitable` derives the count from the data.
        corr_threshold: absolute Spearman above which the lower-ranked of a pair is
            dropped as redundant.
        horizon: the target's forward horizon in rows — sets the CV purge gap.
        n_splits: walk-forward folds, used for both stability and validation.
        random_state: seeded so a re-run reproduces.
        device: `"auto"` (GPU once the pool is wide enough to pay for it), `"cuda"`
            (force it, raise if unavailable) or `"cpu"`.
        subsample, colsample_bytree: XGBoost's stochastic regularisation.
            ⚠️ **These are what make `device` change the answer.** With both at
            1.0 a `cuda` run and a `cpu` run produce bit-identical trees and an
            identical kept set; at the 0.8 defaults the two devices draw different
            random rows and columns from the same seed and 4,189 of 8,280 tree
            nodes end up splitting on a different feature. Set both to 1.0 for a
            device-reproducible run; keep the defaults for the regularisation and
            pin `device` instead. Full evidence in `gpu.py` §1.
    """

    def __init__(
        self,
        panel: pd.DataFrame,
        target: str,
        date_col: str = "date",
        exclude: Sequence[str] = (),
        max_features: Optional[int] = None,
        corr_threshold: float = 0.9,
        horizon: int = 5,
        n_splits: int = 5,
        min_train: int = 500,
        random_state: int = 18,
        device: str = "auto",
        subsample: float = 0.8,
        colsample_bytree: float = 0.8,
        lookback: int = 1,
        window_stats: Sequence[str] = WINDOW_STATS,
        normalize: str = "none",
        holdout_start: Optional[str] = None,
        permutation_repeats: int = 10,
        methods: Sequence[str] = METHODS,
    ):
        # ⚠️ **THE ENSEMBLE'S MEMBERSHIP CHANGES THE ANSWER, so it is recorded** — in
        # `SelectionResult.setup`, and from there into `metadata.json` and the report
        # README. It is NOT in `contract.SETUP_KEYS`: adding a key there makes every
        # run archived without it ungroupable and fails `validate_shortlist` on all 21
        # (the STL-1 domino, `final_features/CONTEXT.md` §5a). Tracked as **MTH-1**.
        self.methods = tuple(methods)
        if not self.methods:
            raise ValueError("methods must name at least one ranker.")
        unknown = [m for m in self.methods if m not in ALL_METHODS]
        if unknown:
            raise ValueError(
                f"unknown ranker(s) {unknown}; implemented: {list(ALL_METHODS)}. "
                f"Pass methods=ALL_METHODS to reproduce a pre-2026-08-16 run."
            )
        self.lasso_converged: Optional[bool] = None
        # ⚠️ Only validated here — `"auto"` decides on the CANDIDATE COUNT, which is
        # not known until the constant and non-numeric columns have been dropped,
        # so `run()` does the resolving. A typo still fails in the constructor.
        if device not in ("auto", "cuda", "cpu"):
            raise ValueError(f"device must be 'auto', 'cuda' or 'cpu', got {device!r}")
        self.device_preference = device
        self.device = device
        # ⚠️ These two are the ONLY reason a cuda run and a cpu run disagree — see
        # the class docstring. Set both to 1.0 for a device-reproducible run.
        self.subsample = subsample
        self.colsample_bytree = colsample_bytree
        self.timings: Dict[str, float] = {}
        if lookback < 1:
            raise ValueError(f"lookback must be >= 1, got {lookback}")
        self.lookback = lookback
        # `lookback=1` with only `last` is the identity, so the un-windowed path
        # stays exactly what it was rather than becoming a special case of the new
        # one that happens to agree.
        self.window_stats = (
            ("last",) if lookback == 1 else tuple(window_stats)
        )
        self.normalize = normalize
        # ⚠️ Rows on or after this date are removed from EVERYTHING the selection
        # touches — ranking, pruning, the walk-forward CV. They are scored exactly
        # once, by `score_holdout`. See `_prepare`.
        self.holdout_start = pd.Timestamp(holdout_start) if holdout_start else None
        if target not in panel.columns:
            raise ValueError(f"target {target!r} is not a column of the panel.")
        self.panel = panel
        self.target = target
        self.date_col = date_col
        self.exclude = set(exclude) | {target, date_col}
        self.max_features = max_features
        self.corr_threshold = corr_threshold
        self.horizon = horizon
        self.cv = PurgedWalkForward(
            n_splits=n_splits,
            horizon=horizon,
            min_train=min_train,
            lookback=lookback,
        )
        self.random_state = random_state
        # ⚠️ The one knob that trades the permutation ranker's PRECISION for wall
        # clock, and it is here because on a cross-sectional panel that ranker is
        # 57 % of the run: 10 repeats over a 162-column design is 1,620 predictions
        # on a 40,000-row fold. Lowering it makes the permutation column noisier —
        # it is a mean over `n_repeats` shuffles — which matters most for the
        # features it separates least. Keep 10 for anything quoted; drop it only
        # for a universe wide enough that the run would not otherwise finish, and
        # say so when reporting.
        if permutation_repeats < 1:
            raise ValueError(
                f"permutation_repeats must be >= 1, got {permutation_repeats}"
            )
        self.permutation_repeats = permutation_repeats

    # ------------------------------------------------------------ preparation

    def _prepare(self) -> Tuple[pd.DataFrame, pd.Series, List[str], pd.Series]:
        """Sort by date, drop the unlabelled tail, split X/y, drop dead columns.

        ⚠️ **The holdout is removed HERE, before anything looks at anything.** Not
        in `run()`, not in the CV — in the first function that touches the data, so
        that no ranker, no prune and no fold can see it. A holdout that the feature
        RANKING has already read is not a holdout.
        """
        frame = self.panel.sort_values(self.date_col).reset_index(drop=True)

        # ⚠️ The last `horizon` rows have a NULL target by construction — their
        # future has not happened. The pool keeps them so the tables still join;
        # a fit must not.
        labelled = frame[frame[self.target].notna()].reset_index(drop=True)

        if self.holdout_start is not None:
            is_holdout = labelled[self.date_col] >= self.holdout_start
            self.holdout_frame = labelled[is_holdout].reset_index(drop=True)
            development = labelled[~is_holdout].reset_index(drop=True)
            # ⚠️ AND the boundary is purged from the DEVELOPMENT side. The last
            # `lookback + horizon - 1` development rows carry labels computed from
            # prices inside the holdout, or windows that reach into it. Leaving
            # them in trains the final model on the answer it is about to be
            # scored against — the same leak the walk-forward gap exists to stop,
            # at the one boundary a fold-level gap does not cover.
            before = len(development)
            development = self._purge_boundary(development)
            self.n_holdout = len(self.holdout_frame)
            self.n_purged_at_boundary = before - len(development)
            labelled = development
            if labelled.empty:
                raise ValueError(
                    f"holdout_start={self.holdout_start.date()} leaves no "
                    f"development rows."
                )
        else:
            self.holdout_frame = pd.DataFrame()
            self.n_holdout = 0
            self.n_purged_at_boundary = 0

        # ⚠️ The DEVELOPMENT frame is announced here and nowhere else. Everything
        # downstream — the folds, the windows, the per-date IC — addresses rows by
        # position into exactly this frame, so a subclass that needs each row's date
        # or ticker has to capture it at the one moment the frame is final.
        self._on_development(labelled)

        candidates = [
            c
            for c in labelled.columns
            if c not in self.exclude
            and pd.api.types.is_numeric_dtype(labelled[c])
            and not pd.api.types.is_bool_dtype(labelled[c])
        ]
        coverage = labelled[candidates].notna().mean()

        nunique = labelled[candidates].nunique(dropna=True)
        dropped_constant = sorted(nunique[nunique <= 1].index.tolist())
        # A column that is entirely NaN also has nunique 0 and lands here, which
        # is the right bin for it: it carries nothing either way.
        features = [c for c in candidates if c not in dropped_constant]
        if not features:
            raise ValueError(
                "no usable features — every candidate column was constant, "
                "all-NaN or non-numeric."
            )

        X = labelled[features].astype(float)
        y = labelled[self.target].astype(float)
        return X, y, dropped_constant, coverage

    # ------------------------------------------------------- the four hooks
    #
    # ⚠️ These four methods are the ONLY places this class assumes its panel is one
    # company's time series. `cross_sectional.CrossSectionalSelector` overrides
    # exactly these and inherits the six rankers, the ensemble, the prune, the
    # stability pass and the holdout protocol unchanged. That is the point of
    # factoring them out: the cross-sectional study has to be the SAME pipeline on a
    # different panel shape, or its numbers are not comparable with §6 of CONTEXT.md.

    def _on_development(self, frame: pd.DataFrame) -> None:
        """Called once, with the final development frame, before X is built."""

    def _design(
        self, X: pd.DataFrame, source: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """Channel panel → windowed design matrix.

        One contiguous series, so `sliding_window_view` may run straight down it.

        `source` is the frame `X`'s rows were taken from, for a subclass that needs
        each row's date or ticker. `None` means the development frame announced by
        `_on_development`.
        """
        return windows.window_design(
            X, self.lookback, self.window_stats, self.normalize
        )

    def _splits(self, index: pd.Index) -> List[Tuple[np.ndarray, np.ndarray]]:
        """Walk-forward folds as POSITIONS into a frame carrying `index`.

        One row is one session here, so a row split is a date split.
        """
        return self.cv.split(len(index))

    def _ic(
        self, prediction: np.ndarray, y: pd.Series, dates: Optional[pd.Series] = None
    ) -> float:
        """The number every fold, holdout and null draw is judged on.

        `dates` is ignored here and required by the cross-sectional override, which
        computes one IC per date and averages them.
        """
        return pooled_ic(prediction, y)

    def _effective_n(
        self, y: pd.Series, dates: Optional[pd.Series] = None
    ) -> float:
        """Independent observations behind the IC computed over `y`.

        ⚠️ On one company this is `n / h`, because consecutive `return_5day` values
        share `h-1` of their `h` days. It is emphatically **not** `n / h` on a
        cross-section — `N` stocks on the same day are one observation of the
        market, not `N` — which `cross_sectional` overrides to say.
        """
        return max(1.0, len(y) / max(1, self.horizon))

    def _purge_boundary(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Drop the development rows whose labels or windows touch the holdout.

        `lookback + horizon - 1` ROWS, which on one contiguous series is the same
        number of sessions.
        """
        gap = self.cv.gap
        if gap and len(frame) > gap:
            return frame.iloc[:-gap].reset_index(drop=True)
        return frame

    # ---------------------------------------------------------------------------

    def _columns_of(
        self, channels: Sequence[str], available: Sequence[str]
    ) -> Dict[str, List[str]]:
        """`{channel: its design columns}`, restricted to columns that exist.

        Normalisation removes the statistics it makes constant, so the nominal
        `channel × stat` grid is wider than the design matrix. Selecting a channel
        must hand the model the columns that are actually there.
        """
        present = set(available)
        return {
            channel: [
                column
                for stat in self.window_stats
                if (column := windows.design_column(channel, stat)) in present
            ]
            for channel in channels
        }

    @staticmethod
    def _impute(train: pd.DataFrame, *others: pd.DataFrame) -> List[pd.DataFrame]:
        """Median-impute every frame with the TRAIN median. Never the pooled one."""
        median = train.median()
        median = median.fillna(0.0)  # a column NaN throughout the train slice
        return [f.fillna(median) for f in (train, *others)]

    # ---------------------------------------------------------------- rankers

    def _xgb(self):
        """The one place a model is configured. `device` comes from `gpu.py`."""
        from xgboost import XGBRegressor

        return XGBRegressor(
            n_estimators=300,
            max_depth=4,
            learning_rate=0.05,
            subsample=self.subsample,
            colsample_bytree=self.colsample_bytree,
            reg_lambda=1.0,
            random_state=self.random_state,
            n_jobs=-1,
            **gpu.xgb_params(self.device),
        )

    def _score_methods(
        self, X: pd.DataFrame, y: pd.Series, target_corr: pd.Series
    ) -> pd.DataFrame:
        """Raw (un-normalised, non-negative) importance per method.

        ⚠️ **Only the methods in `self.methods` are COMPUTED**, not computed and then
        dropped. That is the whole point of the default narrowing to three (§19): on
        the 19 archived country runs `lasso` alone was 90-96 % of the wall clock, so
        skipping a member has to skip its cost, or the change is cosmetic.
        """
        Xi = self._impute(X)[0]
        scores = pd.DataFrame(index=X.columns, dtype=float)
        wanted = set(self.methods)

        # 1. Spearman — monotone association, magnitude only. The signed version
        #    is computed once in `run()` (on the GPU) and reused here; the sign
        #    belongs in `target_corr`, the magnitude in the ensemble.
        #    ⚠️ Free: `target_corr` is computed by `run()` regardless, because the
        #    SIGN goes in the report whether or not `spearman` is in the ensemble.
        if "spearman" in wanted:
            scores["spearman"] = target_corr.reindex(Xi.columns).abs()

        # 2. Mutual information — catches dependence a rank correlation misses.
        #    ⚠️ **ON THE GPU WHENEVER `device="cuda"` (2026-08-10).**
        #    `gpu_rankers.mutual_info` is the SAME Kraskov estimator, not a histogram
        #    substitute — same formula, same per-column `scale`, same tie-breaking
        #    noise from the same legacy `RandomState` — verified equal to sklearn's
        #    to **8.9e-16** in `test_gpu_rankers.py`.
        #    ⚠️ It is also SLOWER on this hardware: sklearn reaches the k-th
        #    neighbour through a KDTree at O(n log n) per column, while the joint
        #    distance matrix is O(n²). Measured at n=4,211 with a warm loky pool:
        #    200 cols 1.0 s vs 7.6 s, 678 cols 3.5 s vs 14.5 s. It runs on the GPU
        #    anyway because `device` means the device — a flag that silently kept
        #    two of six rankers on the host was the thing that made "is the economy
        #    run on the GPU?" unanswerable. Pass `device="cpu"` for the fast path.
        #    ⚠️ Out of the DEFAULT ensemble since 2026-08-16 — worst standalone ranker
        #    measured and the most expensive once `lasso` is gone (§19).
        if "mutual_info" in wanted:
            started = time.perf_counter()
            if self.device == "cuda":
                scores["mutual_info"] = gpu_rankers.mutual_info(
                    Xi.to_numpy(np.float64),
                    y.to_numpy(np.float64),
                    random_state=self.random_state,
                    device="cuda",
                )
                self.timings["mutual_info (cuda)"] = time.perf_counter() - started
            else:
                scores["mutual_info"] = mutual_info_regression(
                    Xi, y, random_state=self.random_state,
                    n_jobs=gpu.n_jobs_for(Xi.shape[1]),
                )
                self.timings["mutual_info (cpu)"] = time.perf_counter() - started

        # 3-4. XGBoost gain and SHAP, from ONE fit — both on the GPU when there
        #      is one, including the SHAP values (see `gpu.tree_shap`).
        #
        #      ⚠️ **`xgb_gain` LEFT THE DEFAULT AND `xgb_shap` DID NOT** (2026-08-16).
        #      They are rho = 0.864 across the archive because they describe the same
        #      booster, so carrying both gave one model 2 of 6 votes in the blend for
        #      no extra information. The fit is shared, so the cost of dropping gain is
        #      zero either way — this is about the WEIGHTING, not the wall clock.
        if {"xgb_gain", "xgb_shap"} & wanted:
            started = time.perf_counter()
            model = self._xgb().fit(Xi, y)
            if "xgb_gain" in wanted:
                booster_gain = model.get_booster().get_score(importance_type="gain")
                scores["xgb_gain"] = [booster_gain.get(c, 0.0) for c in Xi.columns]
            if "xgb_shap" in wanted:
                scores["xgb_shap"] = gpu.tree_shap(model, Xi)
            self.timings[f"xgb gain + shap ({self.device})"] = (
                time.perf_counter() - started
            )

        # 5. LASSO on standardised inputs — |coef| is comparable across columns
        #    only after scaling, and prices and order counts differ by ~1e6 here.
        #
        #    ⚠️ The CV folds are TIME-ORDERED, not random. `LassoCV(cv=5)` defaults
        #    to plain KFold, which with a 5-day forward label puts day t+1 in train
        #    and day t in validation — the penalty would then be tuned against a
        #    leaked score. Handing it the purged walk-forward folds costs nothing
        #    and removes the leak.
        #
        #    ⚠️ **NO LONGER UNCONDITIONALLY CPU-BOUND** (2026-08-10).
        #    `gpu_rankers.lasso_cv` solves the SAME objective on the SAME purged
        #    folds by FISTA instead of coordinate descent, and beats sklearn above
        #    `gpu.GPU_LASSO_MIN_COLUMNS` design columns — 2.0× at 5,000, and the
        #    archived `usa` run spent **215 minutes** here at 8,747. Below that
        #    threshold coordinate descent wins and is kept; the dispatch is measured,
        #    not assumed, and both paths select the same alpha (see
        #    `test_gpu_rankers.py`).
        #
        #    ⚠️ **AND IT LEFT THE DEFAULT ENSEMBLE ON 2026-08-16 BECAUSE OF THAT COST.**
        #    Measured over the 21 archived runs: `lasso` is **87.2 % of the average
        #    run's wall clock** and 90-96 % of every country run — while ranking at
        #    CHANCE (52nd percentile against a random-k control) and, on a return
        #    target, zeroing every coefficient so that its rank column is CONSTANT and
        #    the ensemble is bit-identical without it. It is also the entire 13.7x
        #    target-cost gap in CLAUDE.md §15c-target: a level target keeps the solver
        #    working, a return target collapses it. §19.
        if "lasso" in wanted:
            started = time.perf_counter()
            purged = self._splits(Xi.index)
            scaled = StandardScaler().fit_transform(Xi)
            scores["lasso"] = self._lasso_scores(scaled, y, purged, Xi.shape[1], started)

        # 6. Permutation importance, OUT OF SAMPLE on the last walk-forward fold.
        #    The only member that answers "does this generalise".
        #
        #    ⚠️ **THE ONE MEMBER THAT CANNOT BE DROPPED** (measured 2026-08-16, §19).
        #    Every other leave-one-out subset scored at or above the full six;
        #    `ensemble - permutation` was the only one clearly below it — 55th
        #    percentile against chance's 50th — in all four target x k cells.
        if "permutation" in wanted:
            scores["permutation"] = self._permutation_scores(X, Xi, y)

        return scores[list(self.methods)]

    def _lasso_scores(self, scaled, y, purged, n_columns: int, started: float):
        """|coef| from a cross-validated LASSO on the purged folds."""
        # ⚠️ **NO WIDTH GATE ANY MORE (2026-08-10).** This used to be
        # `device == "cuda" and Xi.shape[1] >= gpu.GPU_LASSO_MIN_COLUMNS`, which kept
        # 18 of the 19 country pools on sklearn's coordinate descent — `lasso` was
        # then **52.8 %** of the japan run's wall clock while the run reported
        # `device=cuda`. The threshold was measured and honest (CD beats FISTA below
        # ~2,000 design columns) and it made `device` mean "the device, sometimes".
        # `device="cuda"` now means every ranker; `GPU_LASSO_MIN_COLUMNS` is kept as
        # the documented crossover and is what `device="cpu"`/`"auto"` still trade on.
        use_gpu_lasso = self.device == "cuda"
        if use_gpu_lasso:
            coef, _alpha, converged = gpu_rankers.lasso_cv(
                scaled, y.to_numpy(np.float64), purged, device=self.device
            )
            self.lasso_converged = converged
            self.timings["lasso (cuda)"] = time.perf_counter() - started
            return np.abs(coef)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", ConvergenceWarning)
            lasso = LassoCV(
                cv=purged,
                random_state=self.random_state,
                max_iter=5000,
                n_jobs=gpu.n_jobs_for(n_columns),
            ).fit(scaled, y)
        self.lasso_converged = not any(
            issubclass(w.category, ConvergenceWarning) for w in caught
        )
        self.timings["lasso (cpu)"] = time.perf_counter() - started
        return np.abs(lasso.coef_)

    def _permutation_scores(self, X: pd.DataFrame, Xi: pd.DataFrame, y: pd.Series):
        """Out-of-sample importance on the last walk-forward fold.

        ⚠️ **NOT `sklearn.inspection.permutation_importance` ANY MORE** (2026-08-10).
        That function holds an `X.copy()` and rewrites one column of a pandas
        DataFrame per permutation — an O(n·p) copy to change O(n) values — and on
        `device="cuda"` it also made one host→device round trip per permutation.
        Measured on `basic+economy_vietnam`, forcing the GPU made the WHOLE RUN
        6.8× slower and this step was all of it (85 s → 986 s).
        `gpu.permutation_importance_batched` keeps the definition, writes one
        column, and batches the predicts: **49× faster on cpu, 63× on cuda**.
        """
        started = time.perf_counter()
        train_idx, test_idx = self._splits(Xi.index)[-1]
        X_tr, X_te = self._impute(X.iloc[train_idx], X.iloc[test_idx])
        fold_model = self._xgb().fit(X_tr, y.iloc[train_idx])

        # ⚠️ Scored on the SAME metric the selection is judged by (§`_validate`), not
        # on R². It used to be `"r2"`, which meant the ensemble ranked features by
        # their contribution to a calibration the validation section explicitly says
        # not to decide on.
        #
        # ⚠️ It goes through `self._ic`, so a cross-sectional run permutes against a
        # PER-DATE IC. `_ic` recovers each row's date from `y_test.index`, so the
        # scorer closes over the UNPERMUTED y — the permutation moves a feature
        # column, never a label or its index.
        y_test = y.iloc[test_idx]

        def score(prediction: np.ndarray) -> float:
            return self._ic(prediction, y_test)

        importances = gpu.permutation_importance_batched(
            fold_model,
            X_te,
            score,
            n_repeats=self.permutation_repeats,
            random_state=self.random_state,
            device=self.device,
        )
        self.timings[f"permutation ({self.device})"] = time.perf_counter() - started
        # Negative = permuting it HELPED, i.e. it hurt out of sample. Clipped to 0
        # so it contributes nothing rather than a negative weight.
        return np.clip(importances, 0.0, None)

    @staticmethod
    def _normalise(scores: pd.DataFrame) -> pd.DataFrame:
        """Min-max each method to 0-1 so the blend is not decided by units."""
        lo, hi = scores.min(), scores.max()
        span = (hi - lo).replace(0.0, np.nan)
        return ((scores - lo) / span).fillna(0.0)

    # ---------------------------------------------------------------- pruning

    def _prune(
        self, order: Sequence[str], corr: pd.DataFrame
    ) -> Tuple[List[str], Dict[str, str]]:
        """Greedy redundancy prune down the ensemble ranking.

        Walk the features best-first; keep one unless it correlates above the
        threshold with something already kept, in which case record WHICH one it
        duplicated. That mapping is the useful output — "dropped: 12 features" is
        not reviewable, "volume_negotiated ≈ value_negotiated" is.
        """
        kept: List[str] = []
        dropped: Dict[str, str] = {}
        for feature in order:
            clash = next(
                (
                    k
                    for k in kept
                    if abs(corr.loc[feature, k]) >= self.corr_threshold
                ),
                None,
            )
            if clash is None:
                kept.append(feature)
            else:
                dropped[feature] = clash
            # ⚠️ Uncapped by default. Stopping early leaves every channel below the
            # break indistinguishable from one examined and kept nothing — see the
            # class docstring, and `selection_cut` for what decides the count now.
            if self.max_features is not None and len(kept) >= self.max_features:
                break
        return kept, dropped

    # ------------------------------------------------------------- stability

    def _stability(
        self, design: pd.DataFrame, y: pd.Series, channels: Sequence[str]
    ) -> pd.DataFrame:
        """Per-fold SHAP ranking — does a channel matter in every era, or one?

        A channel ranked 2nd in 2012 and 40th since is not a feature, it is a
        regime. This is a cheap SHAP-only ranking (the full ensemble per fold is
        not worth its cost) rendered as a rank so folds are comparable.

        The fit is on the design matrix; the SHAP values are aggregated back to
        channels with the same MAX rule as the main ranking, so a fold's ordering
        is comparable with the ensemble's.
        """
        started = time.perf_counter()
        ranks = {}
        for i, (train_idx, _) in enumerate(self._splits(design.index), start=1):
            X_tr = self._impute(design.iloc[train_idx])[0]
            model = self._xgb().fit(X_tr, y.iloc[train_idx])
            per_column = pd.DataFrame(
                {"shap": gpu.tree_shap(model, X_tr)}, index=design.columns
            )
            fold = windows.aggregate_to_channels(per_column, channels)["shap"]
            ranks[f"fold {i}"] = fold.rank(ascending=False, method="min")
        self.timings[f"stability ({self.device})"] = time.perf_counter() - started
        return pd.DataFrame(ranks)

    # ------------------------------------------------------------ validation

    def _validate(
        self,
        design: pd.DataFrame,
        y: pd.Series,
        kept: Sequence[str],
        channels: Sequence[str],
    ) -> pd.DataFrame:
        """Walk-forward out-of-sample scores for the kept channels vs all of them.

        Reported per fold, not averaged into one number: with overlapping labels
        the fold spread IS the uncertainty, and a single mean would hide it.

        `ic` is the Spearman rank correlation of prediction against realised
        return — the metric that matters for ranking, and far more stable than R²,
        which one bad fold can drive deeply negative.

        ⚠️ **The comparison is between CHANNEL SETS, so `n_features` counts channels
        and the model gets every one of that channel's stat columns.** Selecting
        `close_adjust` means the model sees its level, its slope and its dispersion
        — which is what a sequence model fed that channel would see too.
        """
        started = time.perf_counter()
        # ⚠️ Intersected with the design's ACTUAL columns: normalisation drops the
        # statistics it makes constant (zscore ⇒ mean, sd), so the nominal
        # channel × stat grid is wider than the matrix.
        columns_of = self._columns_of(channels, design.columns)
        sets = (
            ("all channels", list(channels)),
            ("selected", list(kept)),
        )
        rows = []
        for i, (train_idx, test_idx) in enumerate(self._splits(design.index), start=1):
            for label, chosen in sets:
                cols = [c for channel in chosen for c in columns_of[channel]]
                X_tr, X_te = self._impute(
                    design.iloc[train_idx][cols], design.iloc[test_idx][cols]
                )
                y_tr, y_te = y.iloc[train_idx], y.iloc[test_idx]
                model = self._xgb().fit(X_tr, y_tr)
                pred = model.predict(X_te)
                ic = self._ic(pred, y_te)
                rows.append(
                    {
                        "fold": f"fold {i}",
                        "feature_set": label,
                        "n_channels": len(chosen),
                        "n_columns": len(cols),
                        "n_train": len(train_idx),
                        "n_test": len(test_idx),
                        # ⚠️ Carried per fold rather than recomputed downstream as
                        # `n_test / h`. On a cross-section that formula is wrong by
                        # the width of the panel, and `ic_summary` has no way to
                        # know the panel's shape — so the class that does, says.
                        "n_eff_test": round(self._effective_n(y_te), 1),
                        "ic": float(ic),
                        "r2": float(
                            1.0
                            - np.sum((y_te - pred) ** 2)
                            / np.sum((y_te - y_te.mean()) ** 2)
                        ),
                        "hit_rate": float(np.mean(np.sign(pred) == np.sign(y_te))),
                    }
                )
        self.timings[f"validation ({self.device})"] = time.perf_counter() - started
        return pd.DataFrame(rows)

    # --------------------------------------------------------------- holdout

    def score_holdout(self, result: "SelectionResult") -> pd.DataFrame:
        """Train on ALL development data, score the holdout ONCE.

        This is the only number in the package that has not been contaminated by
        the selection. It exists to be looked at at the END of a study, not tuned
        against: score it, write it down, and if you then change anything, the
        holdout is spent and a new one has to be carved out of data you have not
        touched.

        ⚠️ The comparison is `selected` vs `all channels` vs a `shuffled` control
        trained the same way. The control is what tells you whether a positive
        holdout IC means anything at all — with one score there is no fold spread
        to read, so the control IS the error bar.

        Raises:
            ValueError: no holdout was configured.
        """
        if self.holdout_frame.empty:
            raise ValueError(
                "no holdout — construct the selector with holdout_start=<date>."
            )
        channels = result.features
        dev_X = self.panel.sort_values(self.date_col).reset_index(drop=True)
        dev_X = dev_X[dev_X[self.target].notna()].reset_index(drop=True)
        is_holdout = dev_X[self.date_col] >= self.holdout_start
        development = self._purge_boundary(dev_X[~is_holdout].reset_index(drop=True))

        def design_of(frame: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
            # ⚠️ The frame ITSELF is passed as `source`, not the development
            # binding. The holdout and the development slice are two different
            # frames whose row positions do not line up, and a cross-sectional
            # `_design` windows per ticker — handed the wrong ids it would splice
            # one company's window on to another's, silently, after the study is
            # over.
            design = self._design(frame[channels].astype(float), source=frame)
            return design, frame[self.target].astype(float).loc[design.index]

        dev_design, dev_y = design_of(development)
        # ⚠️ The holdout's own windows are built from the HOLDOUT ROWS ALONE. Letting
        # a window reach back across the boundary would hand it development data —
        # harmless for leakage in this direction, but it would make the first
        # `lookback` holdout samples a different kind of sample from the rest.
        hold_design, hold_y = design_of(self.holdout_frame)
        # The holdout's own dates, for a cross-sectional `_ic`. `None` on the
        # single-ticker path, where `_ic` does not look at them.
        hold_dates = (
            self.holdout_frame[self.date_col].loc[hold_design.index]
            if self.date_col in self.holdout_frame.columns
            else None
        )

        # ⚠️ Intersected across BOTH designs. A statistic can be constant in the
        # holdout slice but not in development (a channel that never moved in
        # 2024-2026), and asking for it on one side only would raise here — after
        # the study, at the one moment the code must not fail.
        shared = set(dev_design.columns) & set(hold_design.columns)
        columns_of = self._columns_of(channels, sorted(shared))
        dev_design, hold_design = dev_design[sorted(shared)], hold_design[sorted(shared)]
        rng = np.random.default_rng(self.random_state)
        rows = []
        for label, chosen in (
            ("selected", list(result.kept)),
            ("all channels", list(channels)),
        ):
            cols = [c for ch in chosen for c in columns_of[ch]]
            X_tr, X_te = self._impute(dev_design[cols], hold_design[cols])
            for variant in ("real", "shuffled control"):
                y_tr = (
                    dev_y
                    if variant == "real"
                    else pd.Series(
                        rng.permutation(dev_y.to_numpy()), index=dev_y.index
                    )
                )
                model = self._xgb().fit(X_tr, y_tr)
                pred = model.predict(X_te)
                ic = self._ic(pred, hold_y, dates=hold_dates)
                rows.append(
                    {
                        "feature_set": label,
                        "labels": variant,
                        "n_channels": len(chosen),
                        "n_train": len(X_tr),
                        "n_holdout": len(X_te),
                        "n_eff_holdout": round(
                            self._effective_n(hold_y, dates=hold_dates), 1
                        ),
                        "ic": float(ic),
                        "hit_rate": float(
                            np.mean(np.sign(pred) == np.sign(hold_y))
                        ),
                    }
                )
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------- run

    def _tick(self, done: int) -> None:
        """Announce a completed phase. One line per phase, `PROGRESS` permitting.

        ⚠️ Costs two `perf_counter()` calls and one `print` per PHASE — nine per
        run, against phases that are seconds to hours. It cannot show up in a
        timing, which is the whole requirement: the `seconds` it prints are the
        same numbers already collected in `self.timings`, not a second measurement.
        """
        if not PROGRESS:
            return
        now = time.perf_counter()
        seconds, self._phase_t0 = now - self._phase_t0, now
        total = len(_RUN_PHASES)
        print(
            f"  [{done}/{total} phases {done / total:>4.0%}] "
            f"{_RUN_PHASES[done - 1]:<22} {seconds:8.1f}s",
            flush=True,
        )

    def run(self, validate: bool = True, stability: bool = True) -> SelectionResult:
        """Do the whole thing and return a `SelectionResult`."""
        self._phase_t0 = time.perf_counter()
        X, y, dropped_constant, coverage = self._prepare()
        channels = list(X.columns)
        self._tick(1)

        # ── the windowed design matrix ─────────────────────────────────────────
        # `design` is what every MODEL sees: one column per (channel, stat). `X`
        # stays the per-day channel panel, and remains what the CORRELATION PRUNE
        # sees — the model reads a whole channel or none of it, so redundancy is a
        # property of channels, not of individual summary columns.
        started = time.perf_counter()
        design = self._design(X)
        # ⚠️ Normalisation makes some statistics CONSTANT by construction: under
        # `zscore` every window has mean 0 and sd 1. Dropping them here — and
        # naming them — is what makes that visible rather than leaving six columns
        # of zeros silently diluting the per-channel MAX.
        design_nunique = design.nunique(dropna=True)
        dropped_design = sorted(design_nunique[design_nunique <= 1].index.tolist())
        if dropped_design:
            design = design.drop(columns=dropped_design)
        if design.empty:
            raise ValueError(
                f"normalize={self.normalize!r} left no non-constant design columns."
            )
        # A window ending at row N needs N >= lookback-1, so the first lookback-1
        # labels have no sample. Align y to the design's own index.
        y_windowed = y.loc[design.index]
        self.timings["window design (cpu)"] = time.perf_counter() - started
        self._tick(2)

        self.device = gpu.resolve_device(
            self.device_preference, n_features=design.shape[1]
        )

        # Both Spearman passes run on the GPU and share one code path, so the
        # magnitude the ensemble sees and the sign the chart shows can never
        # disagree about ties or missing values.
        started = time.perf_counter()
        design_corr = gpu.spearman_vector(design, y_windowed, device=self.device)
        self.timings[f"spearman vs target ({self.device})"] = (
            time.perf_counter() - started
        )
        self._tick(3)

        raw_design = self._score_methods(design, y_windowed, design_corr)
        self._tick(4)
        # ⚠️ Aggregated to CHANNELS before anything ranks. Scoring, ranking and
        # pruning all happen at channel level because that is the unit the model
        # consumes; `raw_design` is kept for the per-stat diagnostics.
        raw = windows.aggregate_to_channels(raw_design, channels)

        # A method whose raw scores are all equal ranked nothing. It still enters
        # the blend as a constant (harmless), but the caller has to be told —
        # otherwise "the ensemble of six" quietly became an ensemble of five.
        dead_methods = [m for m in self.methods if raw[m].nunique() <= 1]
        scores = self._normalise(raw)
        ranks = scores.rank(ascending=False, method="min")
        # The ensemble is a RANK average, not a score average: one method with a
        # long-tailed raw scale (xgb_gain routinely spans 3 orders of magnitude)
        # would otherwise decide the blend on its own after min-max.
        ranks["ensemble"] = ranks[list(self.methods)].mean(axis=1)

        order = ranks["ensemble"].sort_values().index.tolist()
        self._tick(5)
        # ⚠️ The single most expensive step on a wide pool, and the reason `gpu.py`
        # has a hand-written rank-correlation at all: this is O(p²). Computed on the
        # CHANNEL panel, not the design matrix — `p` is 27, not 162.
        started = time.perf_counter()
        corr = gpu.spearman_matrix(X, device=self.device).loc[order, order]
        self.timings[f"channel corr matrix ({self.device})"] = (
            time.perf_counter() - started
        )
        self._tick(6)
        kept, dropped_correlated = self._prune(order, corr)
        self._tick(7)

        # The signed association a reader looks at stays defined on the channel's
        # value at day N (`last`), so a lookback=20 chart is comparable with a
        # lookback=1 one. Which STAT actually carried a channel is a separate,
        # richer output — `best_stat` and `stat_scores` below.
        target_corr = (
            design_corr.reindex([windows.design_column(c, "last") for c in order])
            .set_axis(order)
            .fillna(0.0)
            .rename("spearman_vs_target")
        )

        normalised_design = self._normalise(raw_design)
        best_stat = pd.DataFrame(
            {
                method: windows.best_stat_per_channel(
                    normalised_design, order, method
                )
                for method in self.methods
            }
        )

        # ⚠️ Hoisted out of the `SelectionResult(...)` call, where they used to be
        # inline expressions. Same order, same arguments, same results — but the
        # two most expensive optional steps in the run are now separately
        # announceable instead of disappearing into a constructor.
        stability_frame = (
            self._stability(design, y_windowed, channels).loc[order]
            if stability
            else pd.DataFrame()
        )
        self._tick(8)
        validation_frame = (
            self._validate(design, y_windowed, kept, channels)
            if validate
            else pd.DataFrame()
        )
        self._tick(9)

        return SelectionResult(
            target=self.target,
            features=order,
            scores=scores.loc[order],
            ranks=ranks.loc[order],
            kept=kept,
            dropped_constant=dropped_constant,
            dropped_correlated=dropped_correlated,
            corr=corr,
            target_corr=target_corr,
            stability=stability_frame,
            validation=validation_frame,
            n_rows=len(design),
            corr_threshold=self.corr_threshold,
            coverage=coverage,
            methods=list(self.methods),
            dead_methods=dead_methods,
            device=self.device,
            timings=dict(self.timings),
            lookback=self.lookback,
            horizon=self.horizon,
            window_stats=list(self.window_stats),
            design_scores=normalised_design,
            best_stat=best_stat,
            purge_gap=self.cv.gap,
            normalize=self.normalize,
            dropped_design_columns=dropped_design,
            n_holdout=self.n_holdout,
            n_purged_at_boundary=self.n_purged_at_boundary,
            holdout_start=(
                str(self.holdout_start.date()) if self.holdout_start else ""
            ),
        )
