"""A binary EVENT classifier fitted on a PEER PANEL and scored on the dataset's own ticker.

The dataset (`train_test_creator`, one ticker) supplies the rows the model is scored on and
the ticker's own training rows; the PEERS — every other ticker of a unified universe, `BANK`
by default — are read from that universe's `pool__*` tables at fit time, with the same
channels, the dataset's own scaler and the same date cut.

⚠️ **MEASURED BEFORE IT WAS BUILT** (2026-09-17, `.claude/context/event_chain.md` §6c): on
the rolling-origin CV over 2014-2023 that chose the tabular setup, XGBoost trained on the 20
BANK names and scored on VCB reached CV AUC 0.675 — the best single model of that search —
and a geometric mean of it with the three linear kinds 0.695 against 0.682 without it,
lifting 7 of 10 folds. Its errors sit in different years from the linear kinds' (2015,
2019, 2021), which is what an ensemble member is for.

⚠️ **THE PEER ROWS ARE NOT IN THE DATASET HASH.** They are read from the database on every
fit, so a run that verifies against its dataset can still have trained on different peer
rows. `provenance()` records the universe, tickers, row count, date range and a digest of
the peer matrix, and the engine writes it into the run's metadata.

⚠️ **THE SAME DATE CUT AS THE OWN ROWS.** A peer row dated d carries a label that looks h
sessions past d, exactly like an own row dated d, so peers are kept only up to the LAST
DATE of the rows `fit` receives (`set_fit_context`) — never past it. The own ticker's rows
in the peer universe are dropped: they arrive through `X`.

⚠️ **SCALED WITH THE DATASET'S SCALER, NOT IMPUTED.** A scaled channel of a peer row gets
the dataset's own affine map, so a tree threshold learned on peers means the same thing on
the own rows. A peer NaN stays NaN (XGBoost routes it); the own rows arrive imputed with
the dataset's train medians.
"""

from __future__ import annotations

import hashlib
from typing import Callable, Dict, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

KEY = ("date", "exchange", "ticker")
DEFAULT_POOLS = ("pool__event_features", "pool__basic", "pool__market_context")

# (universe, own ticker, columns, label, pools) -> frame; kept for the life of the process
# because `event_chain.report` refits one model several times on growing date cuts.
_PEERS: Dict[Tuple, pd.DataFrame] = {}


def read_peers(universe: str, own: str, columns: Sequence[str], label: str,
               pools: Sequence[str], targets: str = "pool__targets") -> pd.DataFrame:
    """`[date, ticker, <label>, *columns]` for every ticker of `universe` except `own`."""
    from feature_selection.unified_reader import UnifiedSchemaReader

    wanted = list(columns)
    with UnifiedSchemaReader(universe) as reader:
        frame = reader.read(targets, columns=list(KEY) + [label])
        for pool in pools:
            types = reader.column_types(pool)
            if not types:
                raise ValueError(f"{reader.schema}.{pool} does not exist — materialise it for {universe}")
            take = [c for c in wanted if c in types]
            if not take:
                continue
            part = reader.read(pool, columns=list(KEY) + take)
            frame = frame.merge(part, on=list(KEY), how="left", validate="one_to_one")
            wanted = [c for c in wanted if c not in take]
    if wanted:
        raise ValueError(
            f"unified_schema_{universe.lower()} holds no {wanted[:5]}{'…' if len(wanted) > 5 else ''} "
            f"in {list(pools)} — materialise those pools for {universe} (RUNBOOK G6)"
        )
    frame = frame[frame["ticker"].str.upper() != own.upper()]
    frame = frame.dropna(subset=[label]).sort_values(["date", "ticker"]).reset_index(drop=True)
    return frame[["date", "ticker", label] + list(columns)]


PEER_READER: Callable[..., pd.DataFrame] = read_peers


class EventPanel:
    """`.fit(X, y)` / `.predict_logit(X)` over `(n, lookback, n_features)`; last row only."""

    needs_dataset = True

    def __init__(self, n_features: int, lookback: int, universe: str = "BANK",
                 columns: Optional[Sequence[str]] = None,
                 exclude: Optional[Sequence[str]] = None,
                 pools: Sequence[str] = DEFAULT_POOLS,
                 n_estimators: int = 600, max_depth: int = 2, learning_rate: float = 0.03,
                 subsample: float = 0.8, colsample_bytree: float = 0.5,
                 min_child_weight: float = 20.0, reg_lambda: float = 1.0, max_bin: int = 64,
                 own_weight: float = 1.0, half_life_years: Optional[float] = None,
                 n_jobs: int = 4, seed: int = 42):
        self.universe = str(universe).upper()
        self.columns = tuple(columns) if columns else ()
        self.exclude = tuple(exclude) if exclude else ()
        self.pools = tuple(pools)
        self.params = dict(n_estimators=int(n_estimators), max_depth=int(max_depth),
                           learning_rate=float(learning_rate), subsample=float(subsample),
                           colsample_bytree=float(colsample_bytree),
                           min_child_weight=float(min_child_weight),
                           reg_lambda=float(reg_lambda), max_bin=int(max_bin),
                           n_jobs=int(n_jobs), random_state=int(seed), tree_method="hist",
                           eval_metric="logloss")
        self.own_weight = float(own_weight)
        self.half_life_years = None if half_life_years in (None, 0) else float(half_life_years)
        self.n_features = int(n_features)
        self.task = "classification"
        self.index_: list = list(range(self.n_features))
        self.names_: list = []
        self.own_: Optional[str] = None
        self.label_: Optional[str] = None
        self.peers_: Optional[pd.DataFrame] = None
        self.fit_dates_ = None
        self.fit_summary_: dict = {}
        self.n_params = 0

    # ------------------------------------------------------------ context
    def set_task(self, task: str) -> None:
        if task != "classification":
            raise ValueError(f"{type(self).__name__} scores a 0/1 event; task={task!r} has no path")
        self.task = task

    def set_dataset(self, dataset) -> None:
        meta = dataset.meta or {}
        names = list((meta.get("features") or {}).get("feature_columns") or [])
        if len(names) != self.n_features:
            raise ValueError(f"the dataset names {len(names)} feature columns for {self.n_features} channels")
        chosen = [i for i, n in enumerate(names)
                  if (not self.columns or n.startswith(self.columns))
                  and not (self.exclude and n.startswith(self.exclude))]
        if not chosen:
            raise ValueError(f"no feature column starts with {self.columns} outside {self.exclude}")
        self.index_ = chosen
        self.names_ = [names[i] for i in chosen]
        schema = str((meta.get("source") or {}).get("schema") or "")
        if not schema.startswith("unified_schema_"):
            raise ValueError(f"the dataset names no unified source schema ({schema!r})")
        self.own_ = schema[len("unified_schema_"):].upper()
        if self.own_ == self.universe:
            raise ValueError(f"the dataset IS the {self.universe} panel — there is no own ticker to score")
        self.label_ = str((meta.get("target") or {}).get("stored_target")
                          or (meta.get("target") or {}).get("column"))
        key = (self.universe, self.own_, tuple(self.names_), self.label_, self.pools)
        if key not in _PEERS:
            _PEERS[key] = PEER_READER(self.universe, self.own_, self.names_, self.label_, self.pools)
        peers = _PEERS[key].copy()
        scaled = list((meta.get("features") or {}).get("scaled_columns") or [])
        scaler = getattr(dataset, "feature_scaler", None)
        if scaled and scaler is None:
            raise ValueError("the dataset scales channels but carries no feature_scaler")
        position = {c: j for j, c in enumerate(scaled)}
        for c in self.names_:
            if c in position:
                j = position[c]
                peers[c] = (peers[c].astype(float) - scaler.mean_[j]) / scaler.scale_[j]
        peers["date"] = pd.to_datetime(peers["date"])
        self.peers_ = peers
        self.set_fit_context(dates=dataset.dates_train)

    def set_fit_context(self, dates=None, aux=None) -> None:
        """The label dates of the rows the NEXT `fit` receives; the peer cut follows them."""
        self.fit_dates_ = None if dates is None else np.asarray(dates)

    # ------------------------------------------------------------- design
    def _design(self, X: np.ndarray) -> np.ndarray:
        return np.asarray(X, dtype=float)[:, -1, :][:, self.index_]

    def _age_weights(self, dates: np.ndarray, end: np.datetime64) -> np.ndarray:
        if self.half_life_years is None:
            return np.ones(len(dates))
        age = (end - dates.astype("datetime64[D]")).astype(float) / 365.25
        return np.power(0.5, age / self.half_life_years)

    # ---------------------------------------------------------------- fit
    def fit(self, X: np.ndarray, y: np.ndarray) -> "EventPanel":
        from xgboost import XGBClassifier

        if self.peers_ is None:
            raise ValueError("call set_dataset before fit — the peers come from the dataset's universe")
        own = self._design(X)
        if self.fit_dates_ is None or len(self.fit_dates_) != len(own):
            raise ValueError(
                f"fit got {len(own)} rows and {None if self.fit_dates_ is None else len(self.fit_dates_)} "
                f"dates — call set_fit_context with the fit rows' dates"
            )
        own_dates = np.asarray(self.fit_dates_, dtype="datetime64[D]")
        cut = own_dates.max()
        peers = self.peers_[self.peers_["date"].to_numpy().astype("datetime64[D]") <= cut]
        Xp = peers[self.names_].to_numpy(dtype=float)
        yp = (peers[self.label_].to_numpy(dtype=float) >= 0.5).astype(int)
        Z = np.vstack([own, Xp])
        target = np.concatenate([(np.asarray(y, dtype=float).ravel() >= 0.5).astype(int), yp])
        dates = np.concatenate([own_dates, peers["date"].to_numpy().astype("datetime64[D]")])
        w = self._age_weights(dates, cut)
        w[:len(own)] *= self.own_weight
        self.model_ = XGBClassifier(**self.params)
        self.model_.fit(Z, target, sample_weight=w)
        digest = hashlib.sha256(np.ascontiguousarray(np.nan_to_num(Xp, nan=-9e9)).tobytes())
        self.fit_summary_ = {
            "universe": self.universe, "own_ticker": self.own_, "label": self.label_,
            "channels": len(self.names_), "own_rows": int(len(own)),
            "peer_rows": int(len(peers)), "peer_tickers": int(peers["ticker"].nunique()),
            "peer_positives": int(yp.sum()),
            "peer_dates": [str(peers["date"].min().date()), str(peers["date"].max().date())] if len(peers) else None,
            "peer_digest": digest.hexdigest()[:16], "date_cut": str(cut),
        }
        self.n_params = int(self.params["n_estimators"] * (2 ** (self.params["max_depth"] + 1) - 1))
        return self

    # ------------------------------------------------------------ predict
    def predict_logit(self, X: np.ndarray) -> np.ndarray:
        return self.model_.predict(self._design(X), output_margin=True)

    predict = predict_logit

    def provenance(self) -> dict:
        """What the last `fit` trained on beyond the dataset — written into the run's metadata."""
        return dict(self.fit_summary_)


def build_model(n_features: int, lookback: int, **kwargs) -> EventPanel:
    return EventPanel(n_features, lookback, **kwargs)


def arch_dict(n_features: int, lookback: int, **kwargs) -> dict:
    """Serializable architecture record for model/arch.json (rebuild via build_model)."""
    return {
        "class": "EventPanel",
        "module": "model",
        "builder": "build_model",
        "kwargs": {"n_features": int(n_features), "lookback": int(lookback), **kwargs},
    }
