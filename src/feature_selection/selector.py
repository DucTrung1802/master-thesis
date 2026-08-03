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
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from scipy import stats
from sklearn.exceptions import ConvergenceWarning
from sklearn.feature_selection import mutual_info_regression
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LassoCV
from sklearn.preprocessing import StandardScaler

from feature_selection import gpu, windows
from feature_selection.windows import WINDOW_STATS

# The ensemble members, in the order they appear in the score table.
METHODS = ("spearman", "mutual_info", "xgb_gain", "xgb_shap", "lasso", "permutation")


def _ic_scorer(estimator, X, y) -> float:
    """Spearman IC as an sklearn scorer — the metric this package decides on.

    Rank correlation of prediction against realised return. Higher is better, so it
    is a scorer rather than a loss, and `permutation_importance` reads a DROP in it
    as importance without any sign juggling.

    ⚠️ Not R². For a forward equity return, R² is deeply negative for any honest
    model (a level forecast of a near-noise series) while the RANKING can still be
    useful — so R² ranks features by their contribution to something the rest of
    this module says not to decide on.
    """
    prediction = estimator.predict(X)
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
    scores: pd.DataFrame  # index=feature, columns=METHODS (0-1 normalised)
    ranks: pd.DataFrame  # index=feature, columns=METHODS + "ensemble"
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
                "purge_gap_rows": self.purge_gap,
                "window_stats": ", ".join(self.window_stats),
                "samples": self.n_rows,
                "channels": len(self.features),
                "design_columns": self.design_scores.shape[0],
                "kept": len(self.kept),
                "device": self.device,
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
        max_features: int = 20,
        corr_threshold: float = 0.9,
        horizon: int = 5,
        n_splits: int = 5,
        min_train: int = 500,
        random_state: int = 42,
        device: str = "auto",
        subsample: float = 0.8,
        colsample_bytree: float = 0.8,
        lookback: int = 1,
        window_stats: Sequence[str] = WINDOW_STATS,
    ):
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

    # ------------------------------------------------------------ preparation

    def _prepare(self) -> Tuple[pd.DataFrame, pd.Series, List[str], pd.Series]:
        """Sort by date, drop the unlabelled tail, split X/y, drop dead columns."""
        frame = self.panel.sort_values(self.date_col).reset_index(drop=True)

        # ⚠️ The last `horizon` rows have a NULL target by construction — their
        # future has not happened. The pool keeps them so the tables still join;
        # a fit must not.
        labelled = frame[frame[self.target].notna()].reset_index(drop=True)

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
        """Raw (un-normalised, non-negative) importance per method."""
        Xi = self._impute(X)[0]
        scores = pd.DataFrame(index=X.columns, dtype=float)

        # 1. Spearman — monotone association, magnitude only. The signed version
        #    is computed once in `run()` (on the GPU) and reused here; the sign
        #    belongs in `target_corr`, the magnitude in the ensemble.
        scores["spearman"] = target_corr.reindex(Xi.columns).abs()

        # 2. Mutual information — catches dependence a rank correlation misses.
        #    ⚠️ CPU-bound by definition: sklearn's Kraskov kNN estimator has no
        #    GPU implementation, and substituting a histogram estimator to win a
        #    benchmark would silently change what the column means. Threaded.
        started = time.perf_counter()
        scores["mutual_info"] = mutual_info_regression(
            Xi, y, random_state=self.random_state, n_jobs=gpu.n_jobs_for(Xi.shape[1])
        )
        self.timings["mutual_info (cpu)"] = time.perf_counter() - started

        # 3-4. XGBoost gain and SHAP, from one fit — both on the GPU when there
        #      is one, including the SHAP values (see `gpu.tree_shap`).
        started = time.perf_counter()
        model = self._xgb().fit(Xi, y)
        booster_gain = model.get_booster().get_score(importance_type="gain")
        scores["xgb_gain"] = [booster_gain.get(c, 0.0) for c in Xi.columns]
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
        #    ⚠️ Also CPU-bound: sklearn's coordinate descent has no GPU path, and
        #    cuML — which does — has no Windows wheel. Threaded across the folds.
        started = time.perf_counter()
        purged = self.cv.split(len(Xi))
        scaled = StandardScaler().fit_transform(Xi)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", ConvergenceWarning)
            lasso = LassoCV(
                cv=purged,
                random_state=self.random_state,
                max_iter=5000,
                n_jobs=gpu.n_jobs_for(Xi.shape[1]),
            ).fit(scaled, y)
        self.lasso_converged = not any(
            issubclass(w.category, ConvergenceWarning) for w in caught
        )
        scores["lasso"] = np.abs(lasso.coef_)
        self.timings["lasso (cpu)"] = time.perf_counter() - started

        # 6. Permutation importance, OUT OF SAMPLE on the last walk-forward fold.
        #    The only member that answers "does this generalise".
        #
        #    ⚠️ `n_jobs=1` on CUDA, and that is not a pessimisation. sklearn
        #    parallelises by FORKING the fitted model into worker processes, and
        #    each copy would claim its own slab of a 4 GB card; the predictions
        #    themselves already run on the GPU inside one process.
        started = time.perf_counter()
        train_idx, test_idx = self.cv.split(len(Xi))[-1]
        X_tr, X_te = self._impute(X.iloc[train_idx], X.iloc[test_idx])
        fold_model = self._xgb().fit(X_tr, y.iloc[train_idx])
        perm = permutation_importance(
            fold_model,
            X_te,
            y.iloc[test_idx],
            n_repeats=10,
            random_state=self.random_state,
            # ⚠️ Scored on the SAME metric the selection is judged by (§`_validate`),
            # not on R². It used to be `"r2"`, which meant the ensemble ranked
            # features by their contribution to a calibration the validation section
            # explicitly says not to decide on.
            scoring=_ic_scorer,
            n_jobs=1 if self.device == "cuda" else gpu.n_jobs_for(X.shape[1]),
        )
        # Negative = permuting it HELPED, i.e. it hurt out of sample. Clipped to 0
        # so it contributes nothing rather than a negative weight.
        scores["permutation"] = np.clip(perm.importances_mean, 0.0, None)
        self.timings[f"permutation ({self.device})"] = time.perf_counter() - started

        return scores[list(METHODS)]

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
            if len(kept) >= self.max_features:
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
        for i, (train_idx, _) in enumerate(self.cv.split(len(design)), start=1):
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
        columns_of = {
            channel: [
                windows.design_column(channel, stat) for stat in self.window_stats
            ]
            for channel in channels
        }
        sets = (
            ("all channels", list(channels)),
            ("selected", list(kept)),
        )
        rows = []
        for i, (train_idx, test_idx) in enumerate(self.cv.split(len(design)), start=1):
            for label, chosen in sets:
                cols = [c for channel in chosen for c in columns_of[channel]]
                X_tr, X_te = self._impute(
                    design.iloc[train_idx][cols], design.iloc[test_idx][cols]
                )
                y_tr, y_te = y.iloc[train_idx], y.iloc[test_idx]
                model = self._xgb().fit(X_tr, y_tr)
                pred = model.predict(X_te)
                ic = stats.spearmanr(pred, y_te).statistic
                rows.append(
                    {
                        "fold": f"fold {i}",
                        "feature_set": label,
                        "n_channels": len(chosen),
                        "n_columns": len(cols),
                        "n_train": len(train_idx),
                        "n_test": len(test_idx),
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

    # ------------------------------------------------------------------- run

    def run(self, validate: bool = True, stability: bool = True) -> SelectionResult:
        """Do the whole thing and return a `SelectionResult`."""
        X, y, dropped_constant, coverage = self._prepare()
        channels = list(X.columns)

        # ── the windowed design matrix ─────────────────────────────────────────
        # `design` is what every MODEL sees: one column per (channel, stat). `X`
        # stays the per-day channel panel, and remains what the CORRELATION PRUNE
        # sees — the model reads a whole channel or none of it, so redundancy is a
        # property of channels, not of individual summary columns.
        started = time.perf_counter()
        design = windows.window_design(X, self.lookback, self.window_stats)
        # A window ending at row N needs N >= lookback-1, so the first lookback-1
        # labels have no sample. Align y to the design's own index.
        y_windowed = y.loc[design.index]
        self.timings["window design (cpu)"] = time.perf_counter() - started

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

        raw_design = self._score_methods(design, y_windowed, design_corr)
        # ⚠️ Aggregated to CHANNELS before anything ranks. Scoring, ranking and
        # pruning all happen at channel level because that is the unit the model
        # consumes; `raw_design` is kept for the per-stat diagnostics.
        raw = windows.aggregate_to_channels(raw_design, channels)

        # A method whose raw scores are all equal ranked nothing. It still enters
        # the blend as a constant (harmless), but the caller has to be told —
        # otherwise "the ensemble of six" quietly became an ensemble of five.
        dead_methods = [m for m in METHODS if raw[m].nunique() <= 1]
        scores = self._normalise(raw)
        ranks = scores.rank(ascending=False, method="min")
        # The ensemble is a RANK average, not a score average: one method with a
        # long-tailed raw scale (xgb_gain routinely spans 3 orders of magnitude)
        # would otherwise decide the blend on its own after min-max.
        ranks["ensemble"] = ranks[list(METHODS)].mean(axis=1)

        order = ranks["ensemble"].sort_values().index.tolist()
        # ⚠️ The single most expensive step on a wide pool, and the reason `gpu.py`
        # has a hand-written rank-correlation at all: this is O(p²). Computed on the
        # CHANNEL panel, not the design matrix — `p` is 27, not 162.
        started = time.perf_counter()
        corr = gpu.spearman_matrix(X, device=self.device).loc[order, order]
        self.timings[f"channel corr matrix ({self.device})"] = (
            time.perf_counter() - started
        )
        kept, dropped_correlated = self._prune(order, corr)

        # The signed association a reader looks at stays defined on the channel's
        # value at day N (`last`), so a lookback=20 chart is comparable with a
        # lookback=1 one. Which STAT actually carried a channel is a separate,
        # richer output — `best_stat` and `stat_scores` below.
        target_corr = (
            design_corr.rename(index=dict(zip(design.columns, design.columns)))
            .reindex([windows.design_column(c, "last") for c in order])
            .set_axis(order)
            .rename("spearman_vs_target")
        )

        normalised_design = self._normalise(raw_design)
        best_stat = pd.DataFrame(
            {
                method: windows.best_stat_per_channel(
                    normalised_design, order, method
                )
                for method in METHODS
            }
        )

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
            stability=(
                self._stability(design, y_windowed, channels).loc[order]
                if stability
                else pd.DataFrame()
            ),
            validation=(
                self._validate(design, y_windowed, kept, channels)
                if validate
                else pd.DataFrame()
            ),
            n_rows=len(design),
            corr_threshold=self.corr_threshold,
            coverage=coverage,
            dead_methods=dead_methods,
            device=self.device,
            timings=dict(self.timings),
            lookback=self.lookback,
            horizon=self.horizon,
            window_stats=list(self.window_stats),
            design_scores=normalised_design,
            best_stat=best_stat,
            purge_gap=self.cv.gap,
        )
