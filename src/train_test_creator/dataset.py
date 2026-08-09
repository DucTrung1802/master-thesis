# src\train_test_creator\dataset.py
"""Turn a `<target>__final__d<d>_h<h>` table into windowed train/val/test tensors.

    python -m train_test_creator                       # print the plan, write nothing
    python -m train_test_creator --save                # write the dataset folder
    python -m train_test_creator --ticker bank --table rank_5day__final__d20_h5

## What changed, and why the old notebook could not stay

`train_test_creator.ipynb` read a VIEW named `<target>__lb<L>__final` that
`unified_schema_creator.ipynb` used to build. That view no longer exists — selection
moved to `feature_selection`, and `final_features` materialises the result as
`<target>__final__d<d>_h<h>`. Four things follow from reading the new table instead:

1. **`d` and `h` are read from the TABLE NAME, never passed in.** The old notebook
   had `LOOKBACK_DAY = 5` as a free parameter while the view it read was built at
   `lb20`; windowing at a `d` other than the one the channels were selected under
   silently answers a different question. `parse_final_table` is the only source of
   both numbers.
2. **The table is read through `UnifiedSchemaReader`, not `driver.select`.** psycopg2
   returns `numeric` as `Decimal` and pandas carries that as dtype `object` — 17 of
   the 203 VCB channels are `numeric`, so the old raw read handed `StandardScaler`
   object columns. `read()` casts from `information_schema`.
3. **Feature selection is gone from this stage.** The old notebook re-ran XGBoost
   gain + SHAP + permutation on the view it had just read. That work is upstream now
   and its provenance lives in the table's `COMMENT`; re-selecting here would produce
   a second, unrecorded shortlist fitted on this split.
4. **The splits are PURGED.** See below — this is the substantive fix.

## ⚠️ The purge, which the old notebook did not have

Sample `i` reads rows `[i-d+1, i]` and carries the label for `[i, i+h]`. For a train
sample at `i` to share nothing with a val sample at `j`, its label period must end
before the val sample's window opens:

    i + h < j - d + 1     ⟹     j - i > d + h - 1

So `d + h - 1` = **24 samples** are dropped from the end of train and the end of val,
which is exactly `feature_selection.PurgedWalkForward.gap` — the same gap the channels
were selected under. The old notebook instead started each split `d-1` rows early and
justified it with "no leakage — features are scaled with train-only statistics,
targets only look forward". Both halves of that are true and neither is the issue:
the leak is the train LABEL that reaches `h` days into the val window, and scaling has
nothing to do with it.

⚠️ Windows are still allowed to warm up across the boundary — a val window may read
train ROWS. That is not leakage; those rows are the past, and a live model would have
them. Only labels are purged.

## ⚠️ Imputation is the TRAIN median, not `ffill().bfill()`

The old notebook forward- then back-filled. `bfill` fills a LEADING gap with the first
FUTURE observation, and on this table that is not a rounding error: 38 of 203 channels
have leading gaps, the longest being `prop_buy_vol` at 3,382 of 4,230 rows. Under
`bfill` 80% of that column — the whole training set — would be a value first observed
in 2023.

So: **train median, matching `FeatureSelector._impute` exactly**, which is the
imputation the ranking that chose these channels was computed under.

## ⚠️ A channel with ZERO coverage in the train slice is dropped

`prop_buy_vol` is empty for every train row and populated later. Imputed, it is a
constant through training and a live signal at test — the model cannot learn a
response to it and gets one anyway. `on_untrainable="drop"` (the default) removes it
and records the reason; `"keep"` and `"raise"` are there for when that is not wanted.

## What this stage does NOT assert

That the features are worth having. Every source run of the VCB table computed no
null (`feature_selection/CONTEXT.md` §14b), so the 203 channels are what some run
ranked highly and nothing more. This module reshapes them; it does not vouch for them.
"""

import json
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

from feature_selection.outstanding import OUTSTANDING_FILENAME
from feature_selection.report import DEFAULT_REPORT_ROOT
from feature_selection.unified_reader import KEY_COLS, UnifiedSchemaReader

# The tables `final_features` builds. ⚠️ `d` and `h` are parsed OUT of this, never
# passed alongside it — see the module docstring.
FINAL_TABLE = re.compile(
    r"^(?P<target>[a-z][a-z0-9_]*?)__final__d(?P<lookback>\d+)_h(?P<horizon>\d+)$"
)

# The six tensors `model/common/data.py` loads by name, plus what it optionally
# loads. ⚠️ These filenames are a CONTRACT with the model stage — `load_dataset`
# looks for exactly these, so renaming one silently returns `None` there rather
# than raising here.
TENSOR_FILES = (
    "X_train.npy", "y_train.npy",
    "X_val.npy", "y_val.npy",
    "X_test.npy", "y_test.npy",
)

# `src/train_test_set`, anchored to the repo rather than the CWD — a notebook run
# from its own folder must write to the same place a `python -m` run does.
DEFAULT_OUTPUT_ROOT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "train_test_set"
)

# Columns that are neither a feature nor the label.
META_COLS = tuple(KEY_COLS)

# The table every label lives in. Same constant as `final_features.builder`, and
# the reason `resolve_target` needs no name heuristic: a column is a label iff it
# is here.
TARGETS_TABLE = "pool__targets"

# Bounded encodings that must survive standardisation unchanged: cyclical `_sin`/
# `_cos` pairs live in [-1, 1] and 0/1 flags carry their meaning in the levels.
# ⚠️ On the VCB table this finds ZERO columns — no datetime channel was selected —
# so it is here for the table that does select one, not for this one.
CYCLICAL_SUFFIXES = ("_sin", "_cos")

# How far outside the train distribution a test value has to land to be counted as
# drift. 5 standard deviations of a train-fitted scaler is far enough that it is a
# statement about the column, not about the tail.
DRIFT_SIGMA = 5.0


def parse_final_table(table: str) -> Tuple[str, int, int]:
    """`"return_5day__final__d20_h5"` → `("return_5day", 20, 5)`.

    ⚠️ The single source of `d` and `h`. `final_features.builder.table_name` puts
    them in the name precisely so a consumer cannot disagree with the selection about
    what the window length was.
    """
    match = FINAL_TABLE.match(table or "")
    if not match:
        raise ValueError(
            f"{table!r} is not a final feature table — expected "
            f"'<target>__final__d<lookback>_h<horizon>' as built by final_features."
        )
    return (
        match["target"],
        int(match["lookback"]),
        int(match["horizon"]),
    )


def dataset_name(
    ticker: str, table: str, train_ratio: float, val_ratio: float, scaler_tag: str
) -> str:
    """`vcb__return_5day__final__d20_h5__tr70_val15_test15__std`.

    ⚠️ The folder NAMES ITS INPUT — the source table appears verbatim. The previous
    scheme (`vcb_return_5day_lb20_h5_final_...`) rebuilt a table name out of parts
    and so could describe a table that was never read; this one cannot. Same argument
    as the report-folder rename in `feature_selection` (commit 9f8f5b0).
    """
    test_ratio = round(1.0 - train_ratio - val_ratio, 4)
    return (
        f"{ticker.lower()}__{table}"
        f"__tr{round(train_ratio * 100):d}"
        f"_val{round(val_ratio * 100):d}"
        f"_test{round(test_ratio * 100):d}"
        f"__{scaler_tag}"
    )


@dataclass
class SplitBounds:
    """Where the two cuts fall, in dates and in per-ticker row counts."""

    train_end_date: pd.Timestamp
    val_end_date: pd.Timestamp
    n_dates: int
    train_dates: int
    val_dates: int
    test_dates: int


@dataclass
class WindowedDataset:
    """The tensors plus everything needed to say what they are."""

    name: str
    schema: str
    table: str
    target: str
    selected_for: str
    lookback: int
    horizon: int
    feature_columns: List[str]
    scaled_columns: List[str]
    bounded_columns: List[str]
    dropped_columns: Dict[str, str]
    bounds: SplitBounds
    purge_gap: int
    X: Dict[str, np.ndarray]
    y: Dict[str, np.ndarray]
    dates: Dict[str, np.ndarray]
    tickers: Dict[str, np.ndarray]
    feature_scaler: StandardScaler
    target_scaler: Optional[StandardScaler]
    coverage: pd.DataFrame
    drift: pd.DataFrame
    source_comment: str = ""
    rows_read: int = 0
    rows_unlabelled: int = 0

    @property
    def n_features(self) -> int:
        return len(self.feature_columns)

    def shapes(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "split": split,
                    "samples": int(self.X[split].shape[0]),
                    "lookback": int(self.X[split].shape[1]),
                    "features": int(self.X[split].shape[2]),
                    "first_date": self.dates[split][0] if len(self.dates[split]) else None,
                    "last_date": self.dates[split][-1] if len(self.dates[split]) else None,
                }
                for split in ("train", "val", "test")
            ]
        )


class TrainTestCreator:
    """Read one final feature table and build purged, windowed, scaled tensors.

    Usage:
        creator = TrainTestCreator("vcb", "return_5day__final__d20_h5")
        data = creator.build()
        creator.save(data)

    Args:
        ticker: the `unified_schema_<ticker>` to read.
        table: a `final_features` table. `d` and `h` come from its NAME.
        train_ratio / val_ratio: shares of the DATE axis; test takes the remainder.
        scale_target: standardise `y` and keep the scaler for inverse-transform.
        on_untrainable: what to do with a channel that has no train-slice coverage —
            `"drop"` (default), `"keep"` or `"raise"`.
        purge: keep the `d + h - 1` label purge at each boundary. `False` reproduces
            the un-purged split for comparison and should not be used for a result.
    """

    def __init__(
        self,
        ticker: str = "vcb",
        table: str = "return_5day__final__d20_h5",
        train_ratio: float = 0.70,
        val_ratio: float = 0.15,
        scale_target: bool = True,
        on_untrainable: str = "drop",
        purge: bool = True,
        output_root: str = DEFAULT_OUTPUT_ROOT,
    ):
        if not 0 < train_ratio < 1 or not 0 < val_ratio < 1:
            raise ValueError("train_ratio and val_ratio must each be in (0, 1).")
        if train_ratio + val_ratio >= 1:
            raise ValueError(
                f"train_ratio + val_ratio = {train_ratio + val_ratio} leaves no test "
                f"split."
            )
        if on_untrainable not in ("drop", "keep", "raise"):
            raise ValueError("on_untrainable must be 'drop', 'keep' or 'raise'.")

        self.ticker = ticker.lower()
        self.table = table
        self.target, self.lookback, self.horizon = parse_final_table(table)
        self.train_ratio = train_ratio
        self.val_ratio = val_ratio
        self.scale_target = scale_target
        self.on_untrainable = on_untrainable
        self.purge = purge
        self.output_root = output_root
        self.scaler_tag = "std"
        # The column actually read. Resolved in `read()` against the table, because
        # a rank target names a table that stores what it is ranked FROM.
        self.stored_target = self.target
        # What the channels were SELECTED for, read from the shortlists in
        # `read()`. Not the table name: the name drops a `cs_` prefix.
        self.selected_for = self.target
        # Table columns that no current shortlist names as a channel and that are
        # not labels — i.e. the table is older than the shortlists. Reported.
        self.stale_channels: List[str] = []

    # ------------------------------------------------------------------ naming

    @property
    def purge_gap(self) -> int:
        """`d + h - 1`, or 0 when purging is off.

        ⚠️ Not `h`. An un-windowed purge of `h` rows leaves `d - 1` rows of the test
        sample's own input window inside the training set — see
        `feature_selection.PurgedWalkForward`.
        """
        return self.lookback + self.horizon - 1 if self.purge else 0

    @property
    def name(self) -> str:
        return dataset_name(
            self.ticker, self.table, self.train_ratio, self.val_ratio, self.scaler_tag
        )

    @property
    def schema(self) -> str:
        return f"unified_schema_{self.ticker}"

    @property
    def schema_table(self) -> str:
        return f"{self.schema}.{self.table}"

    def output_dir(self) -> str:
        return os.path.join(self.output_root, self.name)

    # -------------------------------------------------------------------- read

    def selection(self, root: str = DEFAULT_REPORT_ROOT) -> pd.DataFrame:
        """Every `outstanding.csv` row for this schema at this `(d, h)`.

        ⚠️ The shortlists are the record of what the table was built FROM, so they are
        read rather than guessed at. `final_features` groups on the same three keys.
        """
        frames = []
        for name in sorted(os.listdir(root)):
            path = os.path.join(root, name, OUTSTANDING_FILENAME)
            if os.path.exists(path):
                frames.append(pd.read_csv(path))
        if not frames:
            raise FileNotFoundError(
                f"no {OUTSTANDING_FILENAME} under {root} — run "
                f"`python -m feature_selection.outstanding` first."
            )
        rows = pd.concat(frames, ignore_index=True)
        return rows[
            (rows["schema"] == self.schema)
            & (rows["lookback_d"] == self.lookback)
            & (rows["horizon_h"] == self.horizon)
        ]

    def resolve_target(
        self, columns: Sequence[str], label_columns: Sequence[str]
    ) -> str:
        """The stored label column, from the shortlists and `pool__targets`.

        ⚠️ **There is no `return_{h}day` fallback any more, and no reading of the
        table's NAME.** The old version reconstructed a column name from the horizon
        whenever the name's target was absent, which duplicated
        `final_features.builder._stored_target` in a second place that could drift
        from it. Two records already answer this exactly:

        * **`outstanding.csv` lists every CHANNEL** the table was built from, so any
          column that is not a key and not a channel is not a feature;
        * **`pool__targets` lists every LABEL the schema has**, so of those leftovers
          the label is the one that is also a target column.

        The intersection is exactly one column on both current tables, and the method
        raises rather than choosing when it is not.

        ⚠️ **`outstanding.csv`'s own `target` column is NOT the answer** and cannot be.
        It reads `cs_rank_5day` for the bank runs — a rank is computed within a date
        across a chosen universe and is deliberately never stored
        (`final_features/CONTEXT.md` §5). That value is what the channels were
        SELECTED for, and it is recorded as `selected_for`; the column that exists is
        `return_5day`.
        """
        keys = set(KEY_COLS)
        channels = set(self.selection()["channel"])
        labels = {c for c in label_columns if c not in keys}

        not_a_feature = [c for c in columns if c not in keys and c not in channels]
        candidates = [c for c in not_a_feature if c in labels]

        if len(candidates) == 1:
            # ⚠️ Reported, not raised. A table built before the shortlists were last
            # regenerated legitimately holds channels no current run names — see
            # `stale_channels`, which the caller prints.
            self.stale_channels = [c for c in not_a_feature if c not in labels]
            return candidates[0]
        if not candidates:
            raise ValueError(
                f"{self.schema}.{self.table} has no column that is both absent from "
                f"the shortlists and present in {TARGETS_TABLE} {sorted(labels)}. "
                f"Non-channel columns were {not_a_feature}."
            )
        raise ValueError(
            f"{self.schema}.{self.table} has {len(candidates)} possible label columns "
            f"{candidates} — the shortlists do not separate them."
        )

    def read(self) -> Tuple[pd.DataFrame, str]:
        """The table as a typed frame, plus its `COMMENT` (the provenance sentence)."""
        with UnifiedSchemaReader(self.ticker) as reader:
            frame = reader.read(self.table, order_by=KEY_COLS)
            label_columns = list(reader.column_types(TARGETS_TABLE))
            with reader.driver._cursor_ctx() as cur:
                cur.execute(
                    "SELECT obj_description(%s::regclass)",
                    (f"{reader.schema}.{self.table}",),
                )
                row = cur.fetchone()
            comment = row[0] if row and row[0] else ""

        self.stored_target = self.resolve_target(frame.columns, label_columns)
        # ⚠️ Taken from the shortlists, not rebuilt from the table name — the name
        # drops a `cs_` prefix (final_features/CONTEXT.md §3), so it cannot say
        # whether the selection target was cross-sectional.
        selected = sorted(set(self.selection()["target"]))
        self.selected_for = selected[0] if len(selected) == 1 else ", ".join(selected)
        return frame, comment

    # ------------------------------------------------------------------- build

    def build(self, frame: Optional[pd.DataFrame] = None) -> WindowedDataset:
        """Read (or accept) the panel and produce the tensors."""
        comment = ""
        if frame is None:
            frame, comment = self.read()
        rows_read = len(frame)

        missing = [c for c in META_COLS if c not in frame.columns]
        if missing:
            raise ValueError(f"{self.table} is missing key column(s) {missing}.")

        # ⚠️ The unlabelled tail is the LAST `h` sessions of each ticker, where the
        # forward window is not complete yet. Dropped per ticker, not globally —
        # a multi-ticker table's members can end on different dates.
        labelled = frame.dropna(subset=[self.stored_target]).reset_index(drop=True)
        labelled = labelled.sort_values(list(KEY_COLS)).reset_index(drop=True)
        rows_unlabelled = rows_read - len(labelled)
        if labelled.empty:
            raise ValueError(
                f"{self.table} has no labelled rows for {self.stored_target!r}."
            )

        bounds = self._bounds(labelled["date"])
        feature_cols = [
            c for c in labelled.columns if c not in META_COLS and c != self.stored_target
        ]

        # The rows a TRAIN sample can reach: everything up to the last train label.
        # ⚠️ Every train-only statistic below is computed on THIS slice, not on
        # `date < train_end_date` — the purged tail is not part of any train sample,
        # so letting it into a median or a scaler would put val-adjacent rows into
        # the statistics the purge exists to keep out.
        fit_mask = self._fit_mask(labelled, bounds)
        if not fit_mask.any():
            raise ValueError(
                f"the train slice is empty at train_ratio={self.train_ratio} with "
                f"purge gap {self.purge_gap} — the panel is too short."
            )

        features = labelled[feature_cols].astype("float64")
        coverage = self._coverage(features, fit_mask)
        keep, dropped = self._screen(coverage, feature_cols)
        features = features[keep]

        median = features[fit_mask].median().fillna(0.0)
        features = features.fillna(median)

        bounded, scale_cols = self._classify(features)
        ordered = scale_cols + bounded
        features = features[ordered]

        feature_scaler = StandardScaler().fit(features.loc[fit_mask, scale_cols].values)
        if scale_cols:
            features[scale_cols] = feature_scaler.transform(features[scale_cols].values)

        target = pd.to_numeric(labelled[self.stored_target], errors="coerce")
        if self.scale_target:
            target_scaler = StandardScaler().fit(
                target[fit_mask].values.reshape(-1, 1)
            )
            y_full = target_scaler.transform(target.values.reshape(-1, 1)).ravel()
        else:
            target_scaler = None
            y_full = target.values

        drift = self._drift(features, labelled, bounds, scale_cols)
        X, y, dates, tickers = self._window(features, y_full, labelled, bounds)

        return WindowedDataset(
            name=self.name,
            schema=f"unified_schema_{self.ticker}",
            table=self.table,
            target=self.stored_target,
            selected_for=self.selected_for,
            lookback=self.lookback,
            horizon=self.horizon,
            feature_columns=ordered,
            scaled_columns=scale_cols,
            bounded_columns=bounded,
            dropped_columns=dropped,
            bounds=bounds,
            purge_gap=self.purge_gap,
            X=X,
            y=y,
            dates=dates,
            tickers=tickers,
            feature_scaler=feature_scaler,
            target_scaler=target_scaler,
            coverage=coverage,
            drift=drift,
            source_comment=comment,
            rows_read=rows_read,
            rows_unlabelled=rows_unlabelled,
        )

    # ------------------------------------------------------------------ splits

    def _bounds(self, dates: pd.Series) -> SplitBounds:
        """Cut the DATE axis, not the row axis.

        ⚠️ On a multi-ticker table a row-index cut would put the same date in train
        for one ticker and in val for another, and the two would then share a label
        period across the boundary. Cutting on dates gives every ticker the same
        boundary. On a one-ticker table the two are identical.
        """
        unique = pd.Index(sorted(pd.unique(dates)))
        n = len(unique)
        train_end = int(n * self.train_ratio)
        val_end = int(n * (self.train_ratio + self.val_ratio))
        if not 0 < train_end < val_end < n:
            raise ValueError(
                f"{n} distinct dates cannot be split {self.train_ratio}/"
                f"{self.val_ratio}/rest into three non-empty blocks."
            )
        return SplitBounds(
            train_end_date=unique[train_end],
            val_end_date=unique[val_end],
            n_dates=n,
            train_dates=train_end,
            val_dates=val_end - train_end,
            test_dates=n - val_end,
        )

    def _fit_mask(self, labelled: pd.DataFrame, bounds: SplitBounds) -> pd.Series:
        """Rows carried by a TRAIN sample's label — the only rows a statistic may see."""
        mask = pd.Series(False, index=labelled.index)
        for _, group in labelled.groupby(list(KEY_COLS[1:]), sort=False):
            in_train = (group["date"] < bounds.train_end_date).to_numpy()
            last = int(in_train.sum()) - 1 - self.purge_gap
            if last >= 0:
                mask.loc[group.index[: last + 1]] = True
        return mask

    def _sample_ranges(
        self, group: pd.DataFrame, bounds: SplitBounds
    ) -> Dict[str, Tuple[int, int]]:
        """Half-open `[start, stop)` label-row ranges per split, for one ticker.

        A sample is identified by its LABEL row `i`; it needs `i >= d-1` for a full
        window. The purge removes the last `d + h - 1` samples of train and of val,
        so no train label period reaches into the next split's first window.
        """
        dates = group["date"].to_numpy()
        n = len(dates)
        cut1 = int(np.searchsorted(dates, np.datetime64(bounds.train_end_date)))
        cut2 = int(np.searchsorted(dates, np.datetime64(bounds.val_end_date)))
        gap = self.purge_gap
        warmup = self.lookback - 1
        return {
            "train": (warmup, max(warmup, cut1 - gap)),
            "val": (max(cut1, warmup), max(max(cut1, warmup), cut2 - gap)),
            "test": (max(cut2, warmup), n),
        }

    # -------------------------------------------------------------- diagnostics

    def _coverage(self, features: pd.DataFrame, fit_mask: pd.Series) -> pd.DataFrame:
        """Non-null share per channel, overall and inside the train slice.

        ⚠️ The train figure is the one that matters. A channel at 20% overall could be
        20% everywhere, or 0% in train and 70% at test — and only the second is a
        problem.
        """
        train = features[fit_mask]
        return pd.DataFrame(
            {
                "coverage": features.notna().mean(),
                "train_coverage": train.notna().mean(),
                "train_nunique": train.nunique(dropna=True),
            }
        ).sort_values("train_coverage")

    def _screen(
        self, coverage: pd.DataFrame, columns: Sequence[str]
    ) -> Tuple[List[str], Dict[str, str]]:
        """Keep every channel a train sample can actually learn from.

        ⚠️ Returns the survivors in the TABLE's column order, not in `coverage`'s —
        `coverage` is sorted worst-first for reading, and reindexing the panel by it
        would silently reorder every feature vector the model sees.
        """
        untrainable = set(
            coverage.index[
                (coverage["train_coverage"] <= 0) | (coverage["train_nunique"] <= 1)
            ]
        )
        if not untrainable:
            return list(columns), {}
        reason = (
            "constant across the train slice — imputation makes it a constant in "
            "training and it varies at test, so the model is handed a live signal it "
            "could never fit a response to"
        )
        if self.on_untrainable == "raise":
            raise ValueError(f"{sorted(untrainable)} are {reason}.")
        if self.on_untrainable == "keep":
            return list(columns), {}
        return [c for c in columns if c not in untrainable], {
            c: reason for c in sorted(untrainable)
        }

    def _classify(self, features: pd.DataFrame) -> Tuple[List[str], List[str]]:
        """`(bounded, scaled)` — cyclical encodings and 0/1 flags are left alone."""
        bounded = []
        for column in features.columns:
            values = features[column].dropna()
            if column.endswith(CYCLICAL_SUFFIXES) or set(np.unique(values)) <= {0.0, 1.0}:
                bounded.append(column)
        bounded = sorted(bounded)
        scaled = [c for c in features.columns if c not in set(bounded)]
        return bounded, scaled

    def _drift(
        self,
        scaled: pd.DataFrame,
        labelled: pd.DataFrame,
        bounds: SplitBounds,
        scale_cols: Sequence[str],
    ) -> pd.DataFrame:
        """Share of test rows more than `DRIFT_SIGMA` train-sigmas from the train mean.

        ⚠️ Not a filter — a diagnostic. The panel is non-stationary by construction
        (`close_adjust` rises over seventeen years), so a train-fitted scaler puts part
        of the test set outside the range the model ever saw. This says how much, per
        channel, instead of letting it be discovered from a bad test score.
        """
        if not len(scale_cols):
            return pd.DataFrame(columns=["test_beyond_5sd", "test_mean_z"])
        is_test = (labelled["date"] >= bounds.val_end_date).to_numpy()
        test = scaled.loc[is_test, list(scale_cols)]
        if test.empty:
            return pd.DataFrame(columns=["test_beyond_5sd", "test_mean_z"])
        return pd.DataFrame(
            {
                "test_beyond_5sd": (test.abs() > DRIFT_SIGMA).mean(),
                "test_mean_z": test.mean(),
            }
        ).sort_values("test_beyond_5sd", ascending=False)

    # ----------------------------------------------------------------- windows

    def _window(
        self,
        features: pd.DataFrame,
        y_full: np.ndarray,
        labelled: pd.DataFrame,
        bounds: SplitBounds,
    ) -> Tuple[Dict, Dict, Dict, Dict]:
        """Stack `(d, n)` windows per ticker, then concatenate in date order.

        ⚠️ Windowed PER TICKER. A single global stride over a multi-ticker panel would
        build windows whose first days belong to one company and last days to another.
        """
        values = features.to_numpy(dtype=np.float32)
        date_str = labelled["date"].dt.strftime("%Y-%m-%d").to_numpy()
        # ⚠️ An explicit unicode dtype, not `.astype(str).to_numpy()` — pandas returns
        # dtype `object` for that, `np.save` writes a pickled object array, and both
        # `model/common/data.load_dataset` and `model.lstm.train._split_tickers` read
        # with `allow_pickle=False`. The file would exist and be unreadable, which is
        # how a panel silently gets scored as one series.
        ticker_arr = np.asarray(labelled["ticker"].astype(str).tolist(), dtype=np.str_)

        chunks: Dict[str, List] = {s: [] for s in ("train", "val", "test")}
        for _, group in labelled.groupby(list(KEY_COLS[1:]), sort=False):
            positions = group.index.to_numpy()
            ranges = self._sample_ranges(group, bounds)
            for split, (start, stop) in ranges.items():
                for i in range(start, stop):
                    rows = positions[i - self.lookback + 1 : i + 1]
                    chunks[split].append((date_str[positions[i]], rows, positions[i]))

        X, y, dates, tickers = {}, {}, {}, {}
        for split, items in chunks.items():
            if not items:
                raise ValueError(
                    f"the {split} split holds no complete window — {bounds.n_dates} "
                    f"dates is too few for lookback={self.lookback} and purge gap "
                    f"{self.purge_gap}."
                )
            items.sort(key=lambda item: (item[0], item[2]))
            X[split] = np.stack([values[rows] for _, rows, _ in items]).astype(np.float32)
            y[split] = y_full[[pos for _, _, pos in items]].astype(np.float32)
            dates[split] = np.array([d for d, _, _ in items])
            tickers[split] = ticker_arr[[pos for _, _, pos in items]]
        return X, y, dates, tickers

    # ------------------------------------------------------------------- save

    def save(
        self, data: WindowedDataset, replace: bool = False
    ) -> str:
        """Write the tensors, the scalers and `metadata.json`.

        ⚠️ Refuses to overwrite unless `replace=True`. A half-rewritten dataset folder
        would still load — `model/common/data.py` hashes the six tensors, so a stale
        `metadata.json` beside fresh tensors passes every check it makes.
        """
        directory = self.output_dir()
        if os.path.isdir(directory) and os.listdir(directory) and not replace:
            raise FileExistsError(
                f"{directory} already holds a dataset. Pass replace=True to overwrite "
                f"it — any model run referencing its hash will no longer verify."
            )
        os.makedirs(directory, exist_ok=True)

        for split in ("train", "val", "test"):
            np.save(os.path.join(directory, f"X_{split}.npy"), data.X[split])
            np.save(os.path.join(directory, f"y_{split}.npy"), data.y[split])
            np.save(os.path.join(directory, f"dates_{split}.npy"), data.dates[split])
            np.save(os.path.join(directory, f"tickers_{split}.npy"), data.tickers[split])

        joblib.dump(data.feature_scaler, os.path.join(directory, "feature_scaler.pkl"))
        if data.target_scaler is not None:
            joblib.dump(data.target_scaler, os.path.join(directory, "target_scaler.pkl"))

        data.coverage.to_csv(os.path.join(directory, "coverage.csv"), index_label="channel")
        data.drift.to_csv(os.path.join(directory, "drift.csv"), index_label="channel")

        with open(os.path.join(directory, "metadata.json"), "w", encoding="utf-8") as fh:
            json.dump(self.metadata(data), fh, indent=2, ensure_ascii=False)
        return directory

    @staticmethod
    def drift_summary(data: WindowedDataset) -> Dict:
        """How much of the test set sits outside the train range, per channel."""
        if data.drift.empty:
            return {"scaled_channels": 0}
        beyond = data.drift["test_beyond_5sd"]
        return {
            "scaled_channels": int(len(beyond)),
            "sigma": DRIFT_SIGMA,
            "channels_over_1pct": int((beyond > 0.01).sum()),
            "channels_fully_outside": int((beyond >= 1.0).sum()),
            "worst": beyond.head(5).round(4).to_dict(),
            "note": (
                "share of TEST rows more than 5 train-sigmas from the train mean. A "
                "channel at 1.0 was never inside the range the model was fitted on."
            ),
        }

    def metadata(self, data: WindowedDataset) -> Dict:
        """Everything needed to rebuild this dataset, and to know what it is not."""
        return {
            "dataset_name": data.name,
            "created_at": pd.Timestamp.now().strftime("%Y-%m-%d"),
            "source": {
                "schema": data.schema,
                "table": data.table,
                "comment": data.source_comment,
                "rows_read": data.rows_read,
                "rows_unlabelled_tail": data.rows_unlabelled,
            },
            "target": {
                "column": data.target,
                # ⚠️ What the channels were SELECTED for, which is not always what is
                # stored: a rank target's table stores the quantity it is ranked from
                # (final_features/CONTEXT.md §5). A reader that wants the rank
                # re-ranks with feature_selection.cross_sectional.
                "selected_for": data.selected_for,
                "derived": data.selected_for != data.target,
                "horizon_h": data.horizon,
                "scaled": data.target_scaler is not None,
                "scaler": "StandardScaler" if data.target_scaler is not None else None,
            },
            "window": {
                "lookback_d": data.lookback,
                "source": "parsed from the table name",
            },
            "split": {
                "axis": "date",
                "train_ratio": self.train_ratio,
                "val_ratio": self.val_ratio,
                "test_ratio": round(1 - self.train_ratio - self.val_ratio, 4),
                # ⚠️ The two CUTS, i.e. the first date of val and of test. They are
                # not the last train/val label date — the purge sits between them,
                # so read `date_ranges` for what each split actually covers.
                "val_start_date": str(data.bounds.train_end_date.date()),
                "test_start_date": str(data.bounds.val_end_date.date()),
                "distinct_dates": data.bounds.n_dates,
                "purge_gap_rows": data.purge_gap,
                "purge_rule": "lookback + horizon - 1 (feature_selection.PurgedWalkForward)",
                "date_ranges": {
                    split: [str(data.dates[split][0]), str(data.dates[split][-1])]
                    for split in ("train", "val", "test")
                },
            },
            "features": {
                "n_features": data.n_features,
                "feature_columns": data.feature_columns,
                "scaled_columns": data.scaled_columns,
                "bounded_columns": data.bounded_columns,
                "dropped_columns": data.dropped_columns,
                "selection": (
                    "done upstream by feature_selection; this table is the union of "
                    "its runs' shortlists (final_features/CONTEXT.md §6). Nothing is "
                    "selected here."
                ),
            },
            "imputation": {
                "features": "train-slice median, matching FeatureSelector._impute",
                "all_nan_in_train": "0.0",
                "not_used": "ffill/bfill — bfill fills a leading gap with a future value",
            },
            "shapes": {
                f"{kind}_{split}": list(getattr(data, kind)[split].shape)
                for kind in ("X", "y")
                for split in ("train", "val", "test")
            },
            # ⚠️ Recorded because a model run that scores badly on test should be able
            # to find out here, rather than from a plot, that part of its input was
            # never inside the range it was trained on. See `_drift`.
            "drift": self.drift_summary(data),
            "evidence": (
                "⚠️ The channels in this dataset come from runs that computed no null "
                "(feature_selection/CONTEXT.md §14b). They are what some run ranked "
                "highly; no bar was cleared."
            ),
        }


# ------------------------------------------------------------------------- CLI


def main(argv: Optional[Sequence[str]] = None) -> WindowedDataset:
    argv = list(sys.argv[1:] if argv is None else argv)

    def option(flag: str, default: str) -> str:
        return argv[argv.index(flag) + 1] if flag in argv else default

    creator = TrainTestCreator(
        ticker=option("--ticker", "vcb"),
        table=option("--table", "return_5day__final__d20_h5"),
        train_ratio=float(option("--train", "0.70")),
        val_ratio=float(option("--val", "0.15")),
        purge="--no-purge" not in argv,
    )

    print(f"{'=' * 78}\n{creator.schema_table}")
    print(f"  target     {creator.target}  (h={creator.horizon})")
    print(f"  lookback   d={creator.lookback}   purge gap {creator.purge_gap} samples")
    print(f"  dataset    {creator.name}")
    print(f"  output     {creator.output_dir()}")

    data = creator.build()
    print(f"  target     column {creator.stored_target!r}, selected for "
          f"{creator.selected_for!r}")
    if creator.stale_channels:
        # ⚠️ The table holds channels no CURRENT shortlist names, i.e. it predates the
        # last `feature_selection.outstanding` run. It is still readable — but it is
        # not what `python -m final_features --apply --replace` would build today.
        print(
            f"  ⚠️ STALE   {len(creator.stale_channels)} column(s) of the table are "
            f"in no current shortlist: {creator.stale_channels[:4]}"
            + (" …" if len(creator.stale_channels) > 4 else "")
        )
    print(f"\nrows {data.rows_read} read, {data.rows_unlabelled} unlabelled tail dropped")
    print(f"features {data.n_features} kept, {len(data.dropped_columns)} dropped")
    for column, reason in data.dropped_columns.items():
        print(f"    ⚠️ {column}: {reason}")
    print()
    print(data.shapes().to_string(index=False))

    drift = creator.drift_summary(data)
    if drift.get("channels_over_1pct"):
        print(
            f"\n⚠️ drift: {drift['channels_over_1pct']} of {drift['scaled_channels']} "
            f"channels put >1% of the TEST set beyond {DRIFT_SIGMA} train-sigmas; "
            f"{drift['channels_fully_outside']} put ALL of it there. See drift.csv."
        )

    if "--save" in argv:
        directory = creator.save(data, replace="--replace" in argv)
        print(f"\nsaved to {directory}")
    else:
        print("\nplan only — pass --save to write the dataset")
    return data


if __name__ == "__main__":
    main()
