# src\result_evaluator\evaluator.py
"""Score a finished run folder, and put every run on one leaderboard.

    python -m result_evaluator                          # leaderboard of every run
    python -m result_evaluator --run <run_id>           # rescore one run
    python -m result_evaluator --rescore                # rescore every run in place

## Why this is a separate package from `model`

A run folder is finished when it holds `results/predictions_<split>.csv`. Everything
after that — the metrics, the bar, the verdict — is a **reading** of that file and
needs no model, no GPU and no training. Keeping it separate has one concrete payoff
the project already used once: `dir_auc` was backfilled across every existing run
without retraining any of them. `model/CONTEXT.md` §9 records that. Anything computed
inside a training notebook cannot be fixed that way.

## ⚠️ Every model type is scored by the SAME core

`metrics.evaluate` reads a per-sample **score** against the realised **forward
return**, so an LSTM regressor, a direction classifier and a cross-sectional ranker
produce the same four core numbers and land in one table. The per-task extras are
additive. See `metrics.py` for the argument.

## ⚠️ What a prediction file has to contain

`date, y_true, y_pred` (regression) or `date, y_true, y_prob` (classification), where
**`y_true` is the realised forward return in return units** for a regressor and the
0/1 label for a classifier. A classifier's run therefore also needs the realised
return to fill the core block; it is read from the dataset the run references, which
is why `evaluate_run` resolves the dataset rather than trusting the CSV alone.
"""

from __future__ import annotations

import json
import os
import re
import sys
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from result_evaluator import metrics as M
from utils import runtime

# The label table every schema has. Same constant as `final_features.builder` and
# `train_test_creator.dataset` — the authoritative record of what a return IS.
TARGETS_TABLE = "pool__targets"

# Where runs live. Anchored to the repo, not the CWD — a notebook in
# `src/model/lstm` and a `python -m` from `src` must find the same folder.
_SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_RUNS_DIR = os.path.join(_SRC, "model", "runs")

# The splits a run is scored on. ⚠️ `train` is deliberately absent: a metric on the
# rows the weights were fitted to is not an evaluation, and putting it in the same
# table as `test` invites reading it as one.
SPLITS = ("val", "test")

# The file each split's predictions live in, and the two score column names in use.
PREDICTIONS = "predictions_{split}.csv"
SCORE_COLUMNS = ("y_pred", "y_prob", "score")


def _read_predictions(run_dir: str, split: str) -> Optional[pd.DataFrame]:
    path = os.path.join(run_dir, "results", PREDICTIONS.format(split=split))
    if not os.path.exists(path):
        return None
    frame = pd.read_csv(path)
    missing = [c for c in ("y_true",) if c not in frame.columns]
    if missing:
        raise ValueError(f"{path} has no {missing} column — it cannot be scored.")
    return frame


def _score_column(frame: pd.DataFrame) -> str:
    for column in SCORE_COLUMNS:
        if column in frame.columns:
            return column
    raise ValueError(
        f"no score column in {list(frame.columns)} — expected one of {SCORE_COLUMNS}."
    )


# Pulled out of a `run_id` when the run folder has no `metadata.json`. The format is
# `<model>__<target>__lb<L>__final__<timestamp>` (model/CONTEXT.md §3).
_LOOKBACK_IN_NAME = re.compile(r"__(?:lb|d)(\d+)")
_HORIZON_IN_NAME = re.compile(r"(?:^|_)(\d+)day|__h(\d+)")


def run_metadata(run_dir: str) -> Dict:
    """The run's `metadata.json`, or `{}` when it is gone.

    ⚠️ **Missing metadata is not an error here**, and that is the point of the
    package. `src/model/runs/*/` is git-ignored, so a checkout can hold a run's
    `results/` and nothing else — 27 of the 28 runs in this repo are in exactly that
    state. A scorer that insisted on `metadata.json` would be unable to read the runs
    it exists to read. `_setup` infers what it needs and records that it did.
    """
    path = os.path.join(run_dir, "metadata.json")
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _infer_task(run_dir: str) -> str:
    """`classification` if any prediction file carries `y_prob`, else `regression`.

    Read from the FILE, not from the run name — the score column is what the metrics
    actually consume, and a name can be wrong in a way a column cannot.
    """
    for split in SPLITS:
        frame = _read_predictions(run_dir, split)
        if frame is not None:
            return M.CLASSIFICATION if "y_prob" in frame.columns else M.REGRESSION
    return M.REGRESSION


def _setup(meta: Dict, run_dir: str = "") -> Dict:
    """`(task, horizon, lookback)` for a run, from its metadata or from its name.

    ⚠️ The null's block size is `lookback + horizon`, so a lookback that is too small
    produces a bar that is too low and a verdict that is too kind — the exact failure
    this package exists to prevent. Anything not found is recorded in `inferred`
    rather than passed off as read.
    """
    config = meta.get("config", {}) or {}
    dataset = meta.get("dataset", {}) or {}
    lineage = meta.get("lineage", {}) or {}
    inferred = []

    task = config.get("task")
    if task is None:
        task, _ = (_infer_task(run_dir), inferred.append("task")) if run_dir else (
            M.REGRESSION,
            inferred.append("task"),
        )

    lookback = dataset.get("lookback") or config.get("lookback") or lineage.get("lookback_d")
    if lookback is None:
        match = _LOOKBACK_IN_NAME.search(os.path.basename(run_dir))
        lookback = int(match.group(1)) if match else 1
        inferred.append("lookback")

    horizon = config.get("horizon") or lineage.get("horizon_h")
    if horizon is None:
        match = _HORIZON_IN_NAME.search(os.path.basename(run_dir))
        horizon = int(match.group(1) or match.group(2)) if match else 5
        inferred.append("horizon")

    return {
        "task": task,
        "horizon": int(horizon),
        "lookback": int(lookback),
        "inferred": inferred,
    }


def evaluate_run(
    run_dir: str,
    write: bool = True,
    draws: int = M.NULL_DRAWS,
    seed: int = 0,
) -> pd.DataFrame:
    """Score one run folder from its prediction files. Returns one row per split."""
    meta = run_metadata(run_dir)
    setup = _setup(meta, run_dir)

    scored: Dict[str, Dict] = {}
    for split in SPLITS:
        frame = _read_predictions(run_dir, split)
        if frame is None:
            continue
        column = _score_column(frame)
        score = frame[column].to_numpy(dtype=float)

        # ⚠️ The core block is measured against the realised RETURN. For a regressor
        # `y_true` already is it; for a classifier `y_true` is a 0/1 label, so the
        # return is read from the dataset the run references. Without that a
        # classifier's `ic` and `long_short` would be computed on a binary outcome
        # and would not mean the same thing as a regressor's.
        y_true = frame["y_true"].to_numpy(dtype=float)
        y_return, source = y_true, "y_true"
        y_label = None
        if setup["task"] == M.CLASSIFICATION:
            y_label = y_true
            y_return, source = _realised_return(meta, frame, setup["horizon"])
            if y_return is None:
                # ⚠️ Score what is still well defined and BLANK what is not.
                # `dir_auc` and `hit_rate` need only the up/down label, which a
                # classifier's `y_true` IS — they stay comparable with a regressor's.
                # `ic` and `long_short` are defined against a RETURN; computing them
                # on a 0/1 label yields a different quantity in different units, and
                # putting that in the same column is issue CMP-1.
                scored[split] = _label_only_metrics(
                    y_true, score, setup, draws, seed, source
                )
                continue

        # ⚠️ A `ticker` column with more than one name makes this a PANEL, and a panel
        # scored as a series is wrong three ways at once (metrics.py, "the panel
        # case"). The grain is read from the FILE rather than from a config flag, so a
        # run cannot claim to be one thing and be scored as the other.
        panel = (
            "ticker" in frame.columns and frame["ticker"].nunique() > 1
        )
        if panel:
            scored[split] = M.evaluate_panel(
                dates=frame["date"],
                tickers=frame["ticker"],
                y_true=y_return,
                score=score,
                task=setup["task"],
                horizon=setup["horizon"],
                lookback=setup["lookback"],
                draws=draws,
                seed=seed,
            )
        else:
            scored[split] = M.evaluate(
                y_true=y_return,
                score=score,
                task=setup["task"],
                horizon=setup["horizon"],
                lookback=setup["lookback"],
                y_pred=score if column != "y_prob" else None,
                y_label=y_label,
                draws=draws,
                seed=seed,
            )
        # ⚠️ Carried into metrics.json so a number can never be read without knowing
        # which of its inputs were guessed from a folder name.
        scored[split]["inferred_setup"] = ", ".join(setup["inferred"])
        # ⚠️ And WHAT the core block was measured against. `long_short` on a realised
        # return is a return spread; on a 0/1 label it is a hit-rate spread, in
        # different units. Both are useful, one column holding both silently is not.
        scored[split]["return_source"] = source

    if not scored:
        raise FileNotFoundError(
            f"{run_dir} holds no {PREDICTIONS.format(split='<split>')} — there is "
            f"nothing to score. The training stage writes them."
        )

    table = M.summarise(scored)
    if write:
        results = os.path.join(run_dir, "results")
        os.makedirs(results, exist_ok=True)
        with open(os.path.join(results, "metrics.json"), "w", encoding="utf-8") as fh:
            json.dump(scored, fh, indent=2, default=_jsonable)
        table.to_csv(os.path.join(results, "metrics.csv"), index_label="split")
        with open(os.path.join(results, "verdict.txt"), "w", encoding="utf-8") as fh:
            for split, values in scored.items():
                fh.write(f"{split}: {M.verdict(values)}\n")
    return table


def _label_only_metrics(
    y_label: np.ndarray, score: np.ndarray, setup: Dict, draws: int, seed: int,
    source: str,
) -> Dict:
    """The subset of the core block a 0/1 label alone can support.

    `dir_auc` and `hit_rate` ask "does a higher score mean up more often", which the
    label answers exactly. `ic` and `long_short` ask a question about RETURNS, and are
    left NaN rather than answered in the wrong units. See CMP-1.
    """
    out = M.evaluate(
        y_true=y_label - 0.5,          # centre so `> 0` is the positive class
        score=score,
        task=M.CLASSIFICATION,
        horizon=setup["horizon"],
        lookback=setup["lookback"],
        y_label=y_label,
        draws=draws,
        seed=seed,
    )
    for key in ("ic", "long_short", "ic_p", "ic_bar", "ic_null_mean"):
        out[key] = float("nan")
    out["ic_clears"] = False
    out["return_source"] = source
    out["inferred_setup"] = ", ".join(setup["inferred"])
    return out


def _realised_return(meta: Dict, frame: pd.DataFrame, horizon: int) -> tuple:
    """`(values | None, source)` — the forward return behind a classifier's 0/1 label.

    ⚠️ **Read from `pool__targets`, joined on `(date, ticker)` — NOT from the dataset
    the run references.** That was the bug behind issue CMP-1 and it could not have
    worked: a classification dataset's `y_test` **is the 0/1 label**
    (`target_scaler=None`), so `load_dataset(reference)` handed back the very thing it
    was called to replace, silently and with no error. `pool__targets` is the
    authoritative record of every label the schema has — the same principle that
    replaced the `return_{h}day` name heuristic in `train_test_creator` (TGT-1).

    ⚠️ **Returns `None` when the schema is unknown, and the caller then reports `ic`
    and `long_short` as NaN.** 27 of 30 runs have no `metadata.json`, so nothing
    records which schema they came from. Substituting the LABEL there — as this used
    to — put a hit-rate spread and a return spread in one `long_short` column. A blank
    is honest; a number in the wrong units is not.
    """
    lineage = meta.get("lineage") or {}
    schema = lineage.get("schema") or (meta.get("dataset") or {}).get("schema")
    if not schema or "date" not in frame.columns:
        return None, "unavailable (no schema recorded)"

    ticker = schema.replace("unified_schema_", "")
    column = f"return_{int(horizon)}day"
    try:
        from feature_selection.unified_reader import UnifiedSchemaReader

        with UnifiedSchemaReader(ticker) as reader:
            labels = reader.read(TARGETS_TABLE, order_by=("date",))
    except Exception as error:  # noqa: BLE001 — named in the output, not swallowed
        return None, f"unavailable ({type(error).__name__})"
    if column not in labels.columns:
        return None, f"unavailable ({TARGETS_TABLE} has no {column})"

    keys = ["date"] + (["ticker"] if "ticker" in frame.columns else [])
    left = frame[keys].copy()
    left["date"] = pd.to_datetime(left["date"])
    labels["date"] = pd.to_datetime(labels["date"])
    merged = left.merge(labels[keys + [column]], on=keys, how="left")
    values = merged[column].to_numpy(dtype=float)
    if not np.isfinite(values).any():
        return None, f"unavailable (no {column} matched on {keys})"
    return values, f"{schema}.{TARGETS_TABLE}.{column}"


def _jsonable(value):
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    return str(value)


# ------------------------------------------------------------------- leaderboard


def run_folders(runs_dir: str = DEFAULT_RUNS_DIR) -> List[str]:
    """Every folder holding at least one prediction file.

    ⚠️ Keyed on `results/predictions_*.csv`, NOT on `metadata.json` — see
    `run_metadata`. A run is scoreable exactly when its predictions exist.
    """
    if not os.path.isdir(runs_dir):
        return []
    return [
        os.path.join(runs_dir, name)
        for name in sorted(os.listdir(runs_dir))
        if any(
            os.path.exists(
                os.path.join(runs_dir, name, "results", PREDICTIONS.format(split=s))
            )
            for s in SPLITS
        )
    ]


def leaderboard(
    runs_dir: str = DEFAULT_RUNS_DIR,
    rescore: bool = False,
    split: str = "test",
    draws: int = M.NULL_DRAWS,
) -> pd.DataFrame:
    """One row per run: what it was, and whether it cleared its own null.

    ⚠️ Sorted by `ic`, and that is not an endorsement — the column beside it,
    `ic_clears`, is what says whether the ordering means anything. A leaderboard whose
    top row does not clear its bar is a ranking of noise, and the printed table says so
    in `verdict`.
    """
    rows = []
    for folder in run_folders(runs_dir):
        meta = run_metadata(folder)
        setup = _setup(meta, folder)
        try:
            table = (
                evaluate_run(folder, write=True, draws=draws)
                if rescore
                else _cached_metrics(folder, draws)
            )
        except Exception as error:  # noqa: BLE001 — a broken run is reported, not hidden
            rows.append({"run_id": os.path.basename(folder), "error": str(error)})
            continue
        if split not in table.index:
            continue
        run_id = meta.get("run_id", os.path.basename(folder))
        row = {
            "run_id": run_id,
            "model": (meta.get("model") or {}).get("type")
            or (meta.get("config", {}).get("model") or {}).get("type")
            # A pruned run folder has no metadata; the run_id is `<model>__…` by
            # convention (model/CONTEXT.md §3), so the prefix is the last resort.
            or run_id.split("__")[0].upper(),
            "task": setup["task"],
            "dataset": (meta.get("dataset") or {}).get("dataset_name"),
            "lookback": setup["lookback"],
            "horizon": setup["horizon"],
            "n_features": (meta.get("dataset") or {}).get("n_features"),
        }
        row.update(table.loc[split].to_dict())
        row["verdict"] = M.verdict(table.loc[split].to_dict())
        rows.append(row)

    frame = pd.DataFrame(rows)
    if "ic" in frame.columns:
        frame = frame.sort_values("ic", ascending=False)
    return frame.reset_index(drop=True)


def rebuild_index(runs_dir: str = DEFAULT_RUNS_DIR, draws: int = M.NULL_DRAWS) -> str:
    """Regenerate `index.csv` from the scored runs, one column set for every row.

    ⚠️ **This is the fix for a ragged index (issue IDX-1).** `append_run` grows the
    header as metrics are added, so the file accumulated two eras — nine old rows
    carrying `val_RMSE`/`test_spearman_ic` and nothing else, new rows carrying the core
    block and nothing else, and a naive `dropna()` silently selecting one of them.
    Every run is scoreable from its `predictions_*.csv`, so the honest form of the file
    is one produced in a single pass from a single definition.

    ⚠️ **It writes `index.INDEX_COLUMNS`, the SAME schema `append_run` writes**
    (2026-08-10). It used to write the `leaderboard()` shape instead — `model`/`ic`/
    `dir_auc`, test split only — so running `--rebuild-index` silently replaced the
    leaderboard's schema with a different one under the same filename, and a reader
    written against a trained run's row broke against a rebuilt one. `leaderboard()`
    keeps its own shape: it is a VIEW for reading, not the file.

    ⚠️ Rewrites the file. The run FOLDERS are untouched and are the source of truth,
    so this is recoverable by running it again.
    """
    from result_evaluator.index import INDEX_COLUMNS, index_row

    rows = []
    for folder in run_folders(runs_dir):
        meta = run_metadata(folder)
        run_id = meta.get("run_id", os.path.basename(folder))
        try:
            table = evaluate_run(folder, write=True, draws=draws)
        except Exception as error:  # noqa: BLE001 — a broken run is reported, not hidden
            rows.append({"run_id": run_id, "verdict": f"ERROR: {error}"})
            continue
        scored = {split: table.loc[split].to_dict() for split in table.index}
        rows.append(
            index_row(
                run_id=run_id,
                scored=scored,
                meta=meta,
                run_dir=os.path.relpath(folder, os.path.dirname(runs_dir)),
                verdict=M.verdict(scored["test"]) if "test" in scored else "",
            )
        )

    path = os.path.join(runs_dir, "index.csv")
    frame = pd.DataFrame(rows, columns=INDEX_COLUMNS)
    if "test_ic" in frame.columns:
        frame = frame.sort_values("test_ic", ascending=False, na_position="last")
    frame.to_csv(path, index=False)
    return path


def _cached_metrics(run_dir: str, draws: int) -> pd.DataFrame:
    """`results/metrics.json` if it was written by this module, else compute it."""
    path = os.path.join(run_dir, "results", "metrics.json")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as handle:
            scored = json.load(handle)
        # ⚠️ An old run's metrics.json predates the core block. Recomputing is the
        # only way it joins the same leaderboard — the point of §"Why this is a
        # separate package".
        if all(set(M.CORE_METRICS) <= set(v) for v in scored.values()):
            return M.summarise(scored)
    return evaluate_run(run_dir, write=True, draws=draws)


# --------------------------------------------------------------------------- CLI


def main(argv: Optional[Sequence[str]] = None) -> pd.DataFrame:
    argv = list(sys.argv[1:] if argv is None else argv)

    def option(flag: str, default=None):
        return argv[argv.index(flag) + 1] if flag in argv else default

    # ⚠️ `show_gpu=False`: scoring is numpy over `predictions_*.csv`, which is why
    # `--rescore` re-scores 29 runs without a GPU at all. The RUNTIME is the number
    # that matters here — the block-shuffled null is `draws` re-scorings per run, and
    # that is the whole cost of the stage.
    # ⚠️ **THIS FUNCTION PRINTS `⚠️`, AND A WINDOWS CONSOLE IS cp1252, WHICH HAS NO CODE
    # POINT FOR IT.** Measured 2026-08-16: `python -m result_evaluator` died with
    # `UnicodeEncodeError` at the closing summary — AFTER scoring every run and writing
    # `index.csv`, so the work was done and only the report was lost. That is CLAUDE.md
    # §5 rule 18, and the identical guard has been in `train_test_creator.main` since
    # 2026-08-10; this entry point simply never got it. `errors="replace"` degrades the
    # glyph to `?` on a console that cannot show it rather than discarding the output.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")

    with runtime.RunTimer(
        f"result_evaluator  {os.path.basename(str(option('--runs', DEFAULT_RUNS_DIR)))}"
        f"{'  --rescore' if '--rescore' in argv else ''}"
        f"{'  --rebuild-index' if '--rebuild-index' in argv else ''}"
        f"  draws={option('--draws', M.NULL_DRAWS)}",
        show_gpu=False,
    ):
        return _main(argv, option)


def _main(argv: Sequence[str], option) -> pd.DataFrame:
    runs_dir = option("--runs", DEFAULT_RUNS_DIR)
    if "--rebuild-index" in argv:
        path = rebuild_index(runs_dir, draws=int(option("--draws", M.NULL_DRAWS)))
        print(f"rewrote {path} from every run's predictions")
        return pd.read_csv(path)
    single = option("--run")
    draws = int(option("--draws", M.NULL_DRAWS))

    pd.set_option("display.width", 220)
    pd.set_option("display.max_columns", 40)

    if single:
        folder = single if os.path.isdir(single) else os.path.join(runs_dir, single)
        table = evaluate_run(folder, draws=draws)
        print(f"{'=' * 78}\n{os.path.basename(folder)}")
        print(table.to_string())
        for split in table.index:
            print(f"\n{split}: {M.verdict(table.loc[split].to_dict())}")
        return table

    board = leaderboard(runs_dir, rescore="--rescore" in argv, draws=draws)
    if board.empty:
        print(f"no runs under {runs_dir}")
        return board
    columns = [
        c
        for c in (
            "run_id", "model", "task", "lookback", "n", "n_eff",
            *M.CORE_METRICS, "ic_p", "ic_clears", "dir_auc_p", "dir_auc_clears",
            "return_source",
        )
        if c in board.columns
    ]
    print(board[columns].to_string(index=False))
    cleared = int(board.get("ic_clears", pd.Series(dtype=bool)).sum()) + int(
        board.get("dir_auc_clears", pd.Series(dtype=bool)).sum()
    )
    print(
        f"\n{len(board)} run(s); {cleared} split-metric(s) clear a block-shuffled "
        f"bar. ⚠️ That bar does not price in feature selection, architecture search "
        f"or early stopping."
    )
    return board


if __name__ == "__main__":
    main()
