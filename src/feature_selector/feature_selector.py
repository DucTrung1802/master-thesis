"""Feature selection for the unified-schema pools.

Reworked interface (branch `train_test_creator_v2`):

    FeatureSelector(driver, schema, group, target, max_features).run()

reads `<schema>.pool__<group>` and `<schema>.pool__targets`, joins them on
`date`, ranks every numeric feature against the (renamed) `target` column with a
**weighted ensemble** of XGBoost-gain, XGBoost-SHAP, LASSO and ElasticNet, keeps
the top-`max_features`, and then **drops features that are highly correlated**
(|r| >= `corr_threshold`, default 0.9) keeping the more important one of each
pair. `max_features` is a **cap**: the surviving count after the correlation
prune is `<=` it.

Regression vs. classification is auto-detected from the target (binary 0/1 ->
classification): tree models switch to `XGBClassifier`, the linear models switch
to L1 / elastic-net `LogisticRegression`.

**Lookback / windowed importance.** With `lookback > 1` the selector matches a
sequence model whose sample is a `(lookback, n_features)` window ending on day
`t` with label `target_t`. Each window is flattened into `lookback * n_features`
lag columns (`<feature>__lag0` = day `t`, ... `__lag{L-1}` = day `t-(L-1)`), the
ensemble is fitted on those, and every feature's per-lag importances are summed
back into a single **per-feature** score. The correlation prune and the saved
table still operate on the original (per-day) feature columns, so the output
schema is unchanged. `lookback = 1` (default) is the plain per-row behaviour.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

import random

import numpy as np
import pandas as pd

import xgboost as xgb
import shap

from sklearn.linear_model import (
    LassoCV,
    ElasticNetCV,
    LogisticRegressionCV,
)
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer

RANDOM_SEED = 42
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

DATE_COL = "date"


class FeatureSelectorType(Enum):
    """Ensemble members. `weight` is the blend weight of each (normalized)
    importance; weights sum to 1.0."""

    XGB = {"name": "xgb", "weight": 0.35}
    XGB_SHAP = {"name": "xgb_shap", "weight": 0.35}
    LASSO = {"name": "lasso", "weight": 0.15}
    ELASTIC_NET = {"name": "elastic_net", "weight": 0.15}


@dataclass
class SelectionResult:
    """Outcome of a FeatureSelector run."""

    group: str
    target: str
    task: str  # "regression" | "classification"
    lookback: int  # window length used for importance (1 = per-row)
    max_features: int  # the requested cap (post-prune count is <= this)
    importance: pd.DataFrame  # feature + per-method + final_importance, sorted desc
    top_features: List[str]  # top-`max_features` by final_importance (pre-prune)
    kept_features: List[str]  # survivors after correlation prune (the real count)
    dropped_features: List[str]  # dropped by correlation prune
    corr_pairs: pd.DataFrame  # highly-correlated pairs that triggered a drop
    frame: pd.DataFrame = field(repr=False)  # date + kept_features + target (all rows)


class FeatureSelector:
    def __init__(
        self,
        driver,
        schema: str,
        group: str,
        target: str,
        max_features: int,
        corr_threshold: float = 0.9,
        lookback: int = 1,
        tree_only: bool = False,
        device: str = "auto",
        logger=None,
        random_state: int = RANDOM_SEED,
        targets_table: str = "pool__targets",
        date_col: str = DATE_COL,
        non_feature_cols: Optional[List[str]] = None,
    ):
        if max_features is not None and (
            not isinstance(max_features, int) or max_features <= 0
        ):
            raise ValueError(
                f"max_features must be a positive integer, got {max_features!r}"
            )
        if not (0 < corr_threshold < 1):
            raise ValueError(
                f"corr_threshold must be in (0, 1), got {corr_threshold!r}"
            )
        if not isinstance(lookback, int) or lookback <= 0:
            raise ValueError(f"lookback must be a positive integer, got {lookback!r}")

        self._driver = driver
        self._schema = schema
        self._group = group
        self._target = target
        self._max_features = max_features
        self._corr_threshold = corr_threshold
        self._lookback = lookback
        self._tree_only = tree_only  # skip LASSO/ElasticNet (fast on wide pools)
        self._device = device        # "auto" | "cuda" | "cpu" -> XGBoost device
        self._logger = logger
        self._random_state = random_state
        self._targets_table = targets_table
        self._date_col = date_col
        self._pool_table = f"pool__{group}"
        self._extra_non_feature = set(non_feature_cols or [])

        self._task: Optional[str] = None

    # ------------------------------------------------------------------ utils
    def _log(self, msg: str) -> None:
        print(msg)
        if self._logger is not None:
            try:
                self._logger.log_info(msg)
            except Exception:
                pass

    def _is_classification(self, y: pd.Series) -> bool:
        return set(pd.unique(y.dropna())).issubset({0.0, 1.0})

    _DEVICE_CACHE: Optional[str] = None

    @classmethod
    def _detect_cuda(cls) -> bool:
        """True if XGBoost was built with CUDA and a GPU fit actually works."""
        if cls._DEVICE_CACHE is None:
            dev = "cpu"
            try:
                if xgb.build_info().get("USE_CUDA"):
                    probe = xgb.XGBRegressor(
                        n_estimators=1, tree_method="hist", device="cuda"
                    )
                    probe.fit(np.zeros((4, 2)), np.zeros(4))
                    dev = "cuda"
            except Exception:
                dev = "cpu"
            cls._DEVICE_CACHE = dev
        return cls._DEVICE_CACHE == "cuda"

    def _xgb_device(self) -> str:
        if self._device == "auto":
            return "cuda" if self._detect_cuda() else "cpu"
        return self._device

    # ------------------------------------------------------------------- load
    def _load(self):
        """Read the pool + target, join on date, and build:
          - the per-day (level) labelled subset used for task detection and the
            correlation prune, and
          - the design matrix used for fitting (per-row, or flattened windows
            when `lookback > 1`)."""
        feats = self._driver.select(
            schema_name=self._schema, table_name=self._pool_table,
            order_by=[self._date_col],
        )
        if feats.empty:
            raise ValueError(
                f"{self._schema}.{self._pool_table} is empty (check the group name)."
            )
        tgt = self._driver.select(
            schema_name=self._schema, table_name=self._targets_table,
            columns=[self._date_col, self._target], order_by=[self._date_col],
        )
        if tgt.empty or self._target not in tgt.columns:
            raise ValueError(
                f"target '{self._target}' not found in "
                f"{self._schema}.{self._targets_table}."
            )

        feats[self._date_col] = pd.to_datetime(feats[self._date_col])
        tgt[self._date_col] = pd.to_datetime(tgt[self._date_col])
        tgt = tgt.rename(columns={self._target: "target"})

        # Candidate features = every column coercible to numeric (this drops the
        # text identity/GICS columns generically, without hard-coding them).
        candidates = []
        for c in feats.columns:
            if c == self._date_col or c in self._extra_non_feature:
                continue
            coerced = pd.to_numeric(feats[c], errors="coerce")
            if coerced.notna().any():
                feats[c] = coerced
                candidates.append(c)
        if not candidates:
            raise ValueError(f"no numeric feature columns in {self._pool_table}.")

        self._feats_all = feats[[self._date_col] + candidates].sort_values(
            self._date_col
        ).reset_index(drop=True)
        self._tgt_all = tgt
        self._candidates = candidates

        # Per-day labelled subset (rows with a non-null label -> drops the tail).
        level = self._feats_all.merge(tgt, on=self._date_col, how="inner").dropna(
            subset=["target"]
        )
        self._level_X = level[candidates]
        self._y_level = level["target"]
        self._task = (
            "classification" if self._is_classification(self._y_level) else "regression"
        )

        self._build_design()
        self._log(
            f"loaded {self._pool_table}: {len(candidates)} candidate features, "
            f"lookback={self._lookback}, {len(self._yfit)} fitting samples "
            f"({len(self._fit_cols)} design columns), task={self._task}"
        )

    def _build_design(self):
        """Design matrix for the importance models.

        lookback == 1 : one row per labelled day, columns = candidate features.
        lookback  > 1 : one row per labelled day with a full L-day history,
                        columns = `<feature>__lag{k}` for k in 0..L-1.
        `self._col_to_feature` maps each design column back to its base feature."""
        L = self._lookback
        if L == 1:
            self._Xfit = self._level_X.reset_index(drop=True)
            self._yfit = self._y_level.reset_index(drop=True)
            self._col_to_feature = {c: c for c in self._candidates}
        else:
            base = self._feats_all  # already ordered by date, contiguous index
            lagged = {self._date_col: base[self._date_col]}
            col_to_feature: Dict[str, str] = {}
            for f in self._candidates:
                for k in range(L):
                    col = f"{f}__lag{k}"  # lag0 = day t, lag{L-1} = day t-(L-1)
                    lagged[col] = base[f].shift(k)
                    col_to_feature[col] = f
            lag_df = pd.DataFrame(lagged).iloc[L - 1:]  # need full L-day history
            merged = lag_df.merge(self._tgt_all, on=self._date_col, how="inner").dropna(
                subset=["target"]
            )
            flat_cols = [c for c in lag_df.columns if c != self._date_col]
            self._Xfit = merged[flat_cols].reset_index(drop=True)
            self._yfit = merged["target"].reset_index(drop=True)
            self._col_to_feature = col_to_feature
            if len(flat_cols) > 3000:
                self._log(
                    f"  WARNING: {len(flat_cols)} design columns "
                    f"({len(self._candidates)}x{L}); the linear models "
                    f"(LASSO/ElasticNet) may be very slow at this width."
                )
        self._fit_cols = list(self._Xfit.columns)

    def _linear_matrix(self) -> np.ndarray:
        """Median-imputed + standardized design matrix for the linear models
        (LASSO / ElasticNet / Logistic can't take NaN)."""
        imputed = SimpleImputer(strategy="median").fit_transform(self._Xfit)
        return StandardScaler().fit_transform(imputed)

    # -------------------------------------------------------- per-method fits
    #   Each returns a Series of importance indexed by the *design* columns.
    def _importance_xgb(self) -> pd.Series:
        params = dict(
            n_estimators=400, max_depth=3, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8,
            random_state=self._random_state, n_jobs=-1, importance_type="gain",
            tree_method="hist", device=self._xgb_device(),
        )
        if self._task == "classification":
            model = xgb.XGBClassifier(
                objective="binary:logistic", eval_metric="logloss", **params
            )
        else:
            model = xgb.XGBRegressor(objective="reg:squarederror", **params)
        model.fit(self._Xfit, self._yfit)
        self._xgb_model = model
        return pd.Series(model.feature_importances_, index=self._fit_cols)

    def _importance_xgb_shap(self) -> pd.Series:
        model = getattr(self, "_xgb_model", None)
        if model is None:
            self._importance_xgb()
            model = self._xgb_model
        booster = model.get_booster()
        device = self._xgb_device()

        if device == "cuda":
            # GPU SHAP via XGBoost's native GPUTreeShap (pred_contribs): keeps the
            # whole method on the GPU. Returns (n, n_features + 1); last col = bias.
            booster.set_param({"device": "cuda"})
            dmat = xgb.DMatrix(self._Xfit, feature_names=self._fit_cols)
            contribs = booster.predict(dmat, pred_contribs=True)
            if contribs.ndim == 3:  # multiclass: (n, n_class, p+1)
                contribs = np.abs(contribs).mean(axis=1)
            mean_abs = np.abs(contribs[:, :-1]).mean(axis=0)
        else:
            explainer = shap.TreeExplainer(model)
            sv = explainer.shap_values(self._Xfit)
            if isinstance(sv, list):  # older shap returns [class0, class1] for binary
                sv = sv[-1]
            mean_abs = np.abs(sv).mean(axis=0)
        return pd.Series(mean_abs, index=self._fit_cols)

    def _importance_lasso(self) -> pd.Series:
        X = self._linear_matrix()
        if self._task == "classification":
            model = LogisticRegressionCV(
                penalty="l1", solver="saga", cv=3, Cs=5, max_iter=2000,
                scoring="neg_log_loss", n_jobs=-1, random_state=self._random_state,
            )
            model.fit(X, self._yfit)
            coef = np.abs(model.coef_).ravel()
        else:
            model = LassoCV(cv=5, random_state=self._random_state, n_jobs=-1)
            model.fit(X, self._yfit)
            coef = np.abs(model.coef_)
        return pd.Series(coef, index=self._fit_cols)

    def _importance_elastic_net(self) -> pd.Series:
        X = self._linear_matrix()
        if self._task == "classification":
            model = LogisticRegressionCV(
                penalty="elasticnet", solver="saga", l1_ratios=[0.5], cv=3, Cs=5,
                max_iter=2000, scoring="neg_log_loss", n_jobs=-1,
                random_state=self._random_state,
            )
            model.fit(X, self._yfit)
            coef = np.abs(model.coef_).ravel()
        else:
            model = ElasticNetCV(
                cv=5, l1_ratio=0.5, random_state=self._random_state, n_jobs=-1
            )
            model.fit(X, self._yfit)
            coef = np.abs(model.coef_)
        return pd.Series(coef, index=self._fit_cols)

    # ------------------------------------------------------------- combine
    def _aggregate(self, s: pd.Series) -> pd.Series:
        """Collapse design-column importances to one score per base feature
        (sum over the lag columns), reindexed to the candidate order."""
        per_feature = s.rename(index=self._col_to_feature).groupby(level=0).sum()
        return per_feature.reindex(self._candidates).fillna(0.0)

    @staticmethod
    def _normalize(s: pd.Series) -> pd.Series:
        lo, hi = s.min(), s.max()
        if hi == lo:
            return pd.Series(0.0, index=s.index)
        return (s - lo) / (hi - lo)

    def _fit_all(self) -> pd.DataFrame:
        fit_fns = {
            FeatureSelectorType.XGB: self._importance_xgb,
            FeatureSelectorType.XGB_SHAP: self._importance_xgb_shap,
            FeatureSelectorType.LASSO: self._importance_lasso,
            FeatureSelectorType.ELASTIC_NET: self._importance_elastic_net,
        }
        if self._tree_only:  # XGB + SHAP only (fast on wide pools; no CPU linear CV)
            members = [FeatureSelectorType.XGB, FeatureSelectorType.XGB_SHAP]
        else:
            members = list(FeatureSelectorType)
        total_w = sum(m.value["weight"] for m in members)  # renormalize to sum 1

        df = pd.DataFrame(index=self._candidates)
        df["final_importance"] = 0.0
        for ftype in members:
            name = ftype.value["name"]
            weight = ftype.value["weight"] / total_w
            series = self._aggregate(fit_fns[ftype]())
            df[name] = series
            df["final_importance"] += weight * self._normalize(series)
            self._log(f"  fitted {name} (weight {weight:.3f})")
        df = (
            df.reset_index()
            .rename(columns={"index": "feature"})
            .sort_values("final_importance", ascending=False)
            .reset_index(drop=True)
        )
        return df

    # ------------------------------------------------------ correlation prune
    def _prune_correlated(self, top_features: List[str]):
        """Drop the lower-importance member of every |r| >= threshold pair.
        Correlation is measured on the per-day (level) feature values."""
        if len(top_features) < 2:
            return top_features, [], pd.DataFrame(
                columns=["feature_1", "feature_2", "corr"]
            )

        corr = self._level_X[top_features].corr()
        rank = {f: i for i, f in enumerate(top_features)}  # 0 = most important

        pairs = []
        dropped = set()
        cols = list(corr.columns)
        for i in range(len(cols)):
            for j in range(i + 1, len(cols)):
                r = corr.iloc[i, j]
                if pd.notna(r) and abs(r) >= self._corr_threshold:
                    f1, f2 = cols[i], cols[j]
                    pairs.append({"feature_1": f1, "feature_2": f2, "corr": r})
                    if f1 in dropped or f2 in dropped:
                        continue
                    loser = f2 if rank[f1] <= rank[f2] else f1
                    dropped.add(loser)

        kept = [f for f in top_features if f not in dropped]
        corr_pairs = pd.DataFrame(pairs, columns=["feature_1", "feature_2", "corr"])
        self._log(
            f"  correlation prune (|r| >= {self._corr_threshold}): "
            f"dropped {len(dropped)}, kept {len(kept)}"
        )
        return kept, sorted(dropped), corr_pairs

    # ----------------------------------------------------------------- run
    def run(self) -> SelectionResult:
        self._load()
        importance = self._fit_all()

        n_top = (
            len(importance)
            if self._max_features is None
            else min(self._max_features, len(importance))
        )
        top_features = importance.head(n_top)["feature"].tolist()

        kept, dropped, corr_pairs = self._prune_correlated(top_features)

        # Full frame (all pool rows, target NaN at the tail) for persisting.
        frame = self._feats_all[[self._date_col] + kept].merge(
            self._tgt_all, on=self._date_col, how="left"
        )

        return SelectionResult(
            group=self._group,
            target=self._target,
            task=self._task,
            lookback=self._lookback,
            max_features=self._max_features,
            importance=importance,
            top_features=top_features,
            kept_features=kept,
            dropped_features=dropped,
            corr_pairs=corr_pairs,
            frame=frame,
        )
