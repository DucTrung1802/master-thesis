"""The event report — how well each model ranks the sessions the event follows.

⚠️ **`result_evaluator`'s `dir_auc` IS NOT THE EVENT'S AUC.** For a classifier it is
measured against the realised RETURN's sign (`return_{h}day > 0`), so that a classifier and
a regressor share one column. The question this chain asks is different — *does a higher
score mean the +g % event follows?* — so the metrics here are computed against the 0/1
label the run was trained on, from the run's own `predictions_{val,test}.csv`.

| metric | reads | why |
|---|---|---|
| `auc` | ROC-AUC vs the label | threshold-free ranking quality |
| `auc_bar`, `auc_null_max`, `auc_z`, `auc_p` | the same AUC under `REPORT_NULL_DRAWS` BLOCK shuffles of the label (block `d + h`) | the label is autocorrelated over `h`, so an i.i.d. shuffle would make the bar too easy |
| `pr_auc`, `pr_lift` | average precision, and its ratio to the base rate | the rare class is the one that is traded |
| `p_at_10`, `p_at_5` | precision in the top 10 % / 5 % of scores | "if I act on the most confident days, how often does the event follow?" |
| `brier_skill` | `1 - brier / brier(train base rate)` | calibration against the climatology a model had to beat |
| `thr`, `test_precision`, `test_recall` | the VAL threshold maximising F1, applied to TEST once | a decision rule chosen without the test labels |

⚠️ **NOTHING HERE PRICES THE SEARCH** (`NUL-1`): the null is per run, and the grid is
~15 runs. The best-on-val model's test row is the number to quote; the other rows are
there so it can be read against them. Every model run of a complete trial is a row of
`reports/event_chain/trials.csv` (`event_chain.trial`), so the size of the search is on disk
beside the number.
"""

from __future__ import annotations

import os
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score

from event_chain import config as C

# Rows that can never be "the best model": a constant ranks nothing, and a blend's val
# row is optimistic because its members were picked on val.
NOT_ELIGIBLE = ("BASELINE_PRIOR", "BLEND")


# ------------------------------------------------------------------ metrics
def _block_perm(n: int, block: int, rng) -> np.ndarray:
    starts = np.arange(0, n, block)
    order = rng.permutation(len(starts))
    return np.concatenate([np.arange(starts[i], min(starts[i] + block, n)) for i in order])


def event_metrics(y: np.ndarray, p: np.ndarray, block: int, train_rate: float,
                  draws: int = C.REPORT_NULL_DRAWS, seed: int = 0,
                  horizon: int = 5) -> Dict[str, float]:
    y = np.asarray(y, dtype=float).ravel() >= 0.5
    p = np.asarray(p, dtype=float).ravel()
    ok = np.isfinite(p)
    y, p = y[ok], p[ok]
    n, pos = len(y), int(y.sum())
    out: Dict[str, float] = {"n": n, "positives": pos, "base_rate": pos / max(n, 1),
                             "n_eff": n / max(1, horizon)}
    if pos == 0 or pos == n:
        return out
    auc = roc_auc_score(y, p)
    rng = np.random.default_rng(seed)
    null = np.array([roc_auc_score(y[_block_perm(n, block, rng)], p) for _ in range(draws)])
    order = np.argsort(-p, kind="stable")
    k10, k5 = max(1, int(round(0.10 * n))), max(1, int(round(0.05 * n)))
    brier = float(np.mean((p - y) ** 2))
    brier_ref = float(np.mean((train_rate - y) ** 2))
    out.update(
        auc=float(auc),
        auc_null_mean=float(null.mean()),
        auc_bar=float(np.quantile(null, 0.95)),
        auc_null_max=float(null.max()),
        auc_z=float((auc - null.mean()) / (null.std(ddof=1) or np.nan)),
        auc_p=float((1 + (null >= auc).sum()) / (draws + 1)),
        null_draws=int(draws),
        null_block=int(block),
        pr_auc=float(average_precision_score(y, p)),
        p_at_10=float(y[order[:k10]].mean()),
        p_at_5=float(y[order[:k5]].mean()),
        recall_at_10=float(y[order[:k10]].sum() / pos),
        brier=brier,
        brier_skill=float(1.0 - brier / brier_ref) if brier_ref > 0 else np.nan,
    )
    out["pr_lift"] = out["pr_auc"] / out["base_rate"]
    out["p10_lift"] = out["p_at_10"] / out["base_rate"]
    return out


def val_threshold(y: np.ndarray, p: np.ndarray) -> float:
    """The score cut that maximises F1 on VAL."""
    y = np.asarray(y, dtype=float) >= 0.5
    p = np.asarray(p, dtype=float)
    best, thr = -1.0, 0.5
    for t in np.unique(np.quantile(p, np.linspace(0.5, 0.995, 100))):
        pred = p >= t
        tp = float((pred & y).sum())
        if tp == 0:
            continue
        f1 = 2 * tp / (pred.sum() + y.sum())
        if f1 > best:
            best, thr = f1, float(t)
    return thr


def at_threshold(y: np.ndarray, p: np.ndarray, thr: float) -> Dict[str, float]:
    y = np.asarray(y, dtype=float) >= 0.5
    pred = np.asarray(p) >= thr
    tp = float((pred & y).sum())
    return {
        "signals": int(pred.sum()),
        "precision": tp / pred.sum() if pred.sum() else np.nan,
        "recall": tp / y.sum() if y.sum() else np.nan,
    }


# --------------------------------------------------------------------- runs
def _runs_dir(runs_dir: Optional[str]) -> str:
    from model.common import engine

    return runs_dir or engine.RUNS_DIR


def model_runs(chain, runs_dir: Optional[str] = None) -> pd.DataFrame:
    """The latest run per run name on the dataset's CURRENT hash."""
    from model.common.data import load_dataset

    runs_dir = _runs_dir(runs_dir)
    index = pd.read_csv(os.path.join(runs_dir, "index.csv"))
    name = chain.creator().name
    # ⚠️ ON THE DATASET'S CURRENT HASH, not its name: `dataset` rebuilds the folder in
    # place (`replace=True`), and a run fitted on the previous tensors would otherwise sit
    # on the board beside runs it is not comparable with.
    current = load_dataset(name).hash
    rows = index[(index["dataset_name"] == name)
                 & (index["dataset_hash"].astype(str) == current)].copy()
    rows["run_name"] = rows["run_id"].str.rsplit("__", n=1).str[0]
    rows = rows.sort_values("created_at").groupby("run_name").tail(1)
    rows["dir"] = rows["run_id"].map(lambda r: os.path.join(runs_dir, r))
    return rows[rows["dir"].map(os.path.isdir)].reset_index(drop=True)


def leaderboard(chain, runs_dir: Optional[str] = None) -> pd.DataFrame:
    from model.common.data import load_dataset

    dataset = load_dataset(chain.creator().name)
    train_rate = float(np.mean(dataset.y_train))
    block = int(dataset.lookback + chain.horizon)
    table = []
    for _, run in model_runs(chain, runs_dir).iterrows():
        pred = {}
        for split in ("val", "test"):
            path = os.path.join(run["dir"], "results", f"predictions_{split}.csv")
            if os.path.exists(path):
                pred[split] = pd.read_csv(path)
        if set(pred) != {"val", "test"} or "y_prob" not in pred["test"]:
            continue
        row = {"run_name": run["run_name"], "model_type": run["model_type"],
               "n_params": run.get("n_params")}
        for split, frame in pred.items():
            m = event_metrics(frame["y_true"], frame["y_prob"], block, train_rate,
                              horizon=chain.horizon)
            row.update({f"{split}_{k}": v for k, v in m.items()})
        thr = val_threshold(pred["val"]["y_true"], pred["val"]["y_prob"])
        row["thr"] = thr
        row.update({f"test_{k}_at_thr": v for k, v in
                    at_threshold(pred["test"]["y_true"], pred["test"]["y_prob"], thr).items()})
        row["run_id"] = run["run_id"]
        table.append(row)
    frame = pd.DataFrame(table)
    if frame.empty:
        return frame
    frame = frame.sort_values("val_auc", ascending=False, na_position="last").reset_index(drop=True)
    blend = _blend(frame, block, train_rate, chain.horizon, _runs_dir(runs_dir))
    if blend is not None:
        frame = pd.concat([frame, pd.DataFrame([blend])], ignore_index=True)
    return frame.sort_values("val_auc", ascending=False, na_position="last").reset_index(drop=True)


def _blend(frame: pd.DataFrame, block: int, train_rate: float, horizon: int, runs_dir: str,
           k: int = 3):
    """The mean probability of the `k` best-on-VAL models — chosen without a test label.

    ⚠️ Its VAL row is optimistic by construction (its members were picked on val), so it
    is sorted among the others by that number but must be read on TEST.
    """
    members = frame[~frame["model_type"].isin(NOT_ELIGIBLE)].head(k)
    if len(members) < 2:
        return None
    probs = {}
    for split in ("val", "test"):
        stack = []
        for run_id in members["run_id"]:
            pred = pd.read_csv(os.path.join(runs_dir, run_id, "results", f"predictions_{split}.csv"))
            stack.append(pred["y_prob"].to_numpy(dtype=float))
        probs[split] = (pred["y_true"].to_numpy(dtype=float), np.mean(stack, axis=0))
    row = {"run_name": f"blend_top{len(members)}__" + "+".join(
        r.split("__")[0] for r in members["run_name"]), "model_type": "BLEND", "n_params": None,
        "run_id": ""}
    for split, (y, p) in probs.items():
        m = event_metrics(y, p, block, train_rate, horizon=horizon)
        row.update({f"{split}_{key}": v for key, v in m.items()})
    thr = val_threshold(*probs["val"])
    row["thr"] = thr
    row.update({f"test_{key}_at_thr": v for key, v in at_threshold(*probs["test"], thr).items()})
    row["blend_members"] = ",".join(members["run_name"])
    return row


def best_on_val(board: pd.DataFrame) -> Optional[pd.Series]:
    eligible = board[~board["model_type"].isin(NOT_ELIGIBLE)]
    return None if eligible.empty else eligible.iloc[0]


# -------------------------------------------------------------- walk-forward
def walk_forward(chain, board: pd.DataFrame, runs_dir: Optional[str] = None) -> pd.DataFrame:
    """Expanding-window yearly refits of the val-best ESTIMATOR, over the post-holdout years.

    ⚠️ Only years the selection never read (on or after the holdout) are scored, and each
    fold's training set ends `d + h - 1` samples before the fold starts — the same purge as
    the dataset. The features stay the dataset's (train-slice scaler), so nothing in a fold
    was fitted on its own year.
    """
    import importlib

    import yaml

    from model.common.data import load_dataset
    from model.common.engine import model_spec

    runs_dir = _runs_dir(runs_dir)
    estimators = board[board["model_type"].str.startswith(("GBT", "FOREST", "BASELINE_LOGISTIC"))]
    if estimators.empty:
        return pd.DataFrame()
    dataset = load_dataset(chain.creator().name)
    X = np.concatenate([dataset.X_train, dataset.X_val, dataset.X_test]).astype(float)
    y = np.concatenate([dataset.y_train, dataset.y_val, dataset.y_test]).astype(int)
    dates = pd.to_datetime(np.concatenate([dataset.dates_train, dataset.dates_val, dataset.dates_test]))
    gap = int(dataset.lookback + chain.horizon - 1)
    holdout = pd.Timestamp(chain.holdout_start())

    picks = [estimators.iloc[0]["run_name"]]
    channel = board[board["model_type"] == "BASELINE_LOGISTIC_CHANNEL"]
    if not channel.empty and channel.iloc[0]["run_name"] not in picks:
        picks.append(channel.iloc[0]["run_name"])

    rows = []
    for run_name in picks:
        run_id = board.loc[board["run_name"] == run_name, "run_id"].iloc[0]
        path = os.path.join(runs_dir, run_id, "config.yaml")
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh)
        package = {"GBT": "gbt", "FOREST": "forest"}.get(
            str(board.loc[board["run_name"] == run_name, "model_type"].iloc[0]).split("_")[0], "baseline")
        module = importlib.import_module(f"model.{package}.model")
        arch = module.arch_dict(n_features=dataset.n_features, lookback=dataset.lookback,
                                **model_spec(cfg))
        for year in sorted(set(dates[dates >= holdout].year)):
            start = max(pd.Timestamp(f"{year}-01-01"), holdout)
            test = (dates >= start) & (dates < pd.Timestamp(f"{year + 1}-01-01"))
            first = int(np.argmax(test))
            train = np.arange(len(y)) < max(0, first - gap)
            if test.sum() < 20 or y[test].sum() == 0 or y[train].sum() == 0:
                continue
            est = module.build_model(**arch["kwargs"])
            if getattr(est, "needs_dataset", False):
                est.set_dataset(dataset)
            if hasattr(est, "set_task"):
                est.set_task("classification")
            est.fit(X[train], y[train])
            score = est.predict_logit(X[test])
            rows.append({
                "model": run_name.split("__")[0], "year": year, "train_n": int(train.sum()),
                "test_n": int(test.sum()), "positives": int(y[test].sum()),
                "base_rate": float(y[test].mean()), "auc": float(roc_auc_score(y[test], score)),
                "pr_auc": float(average_precision_score(y[test], score)),
            })
    return pd.DataFrame(rows)


# ------------------------------------------------------------------- write
def _fmt(v, digits=3):
    if v is None or (isinstance(v, float) and not np.isfinite(v)):
        return "—"
    if isinstance(v, (int, np.integer)):
        return f"{int(v):,}"
    return f"{float(v):.{digits}f}"


def write(chain, walkforward: bool = True, runs_dir: Optional[str] = None,
          output_dir: Optional[str] = None, selection_before: Optional[str] = None) -> Dict:
    """Score every run, write the report files, and return what was computed.

    Returns `{"text", "path", "output_dir", "board", "selection", "walkforward"}` so that
    `event_chain.trial.record` logs exactly the numbers the report printed.
    """
    from model.common.data import load_dataset

    output_dir = output_dir or chain.output_dir
    os.makedirs(output_dir, exist_ok=True)
    dataset = load_dataset(chain.creator().name)
    meta = dataset.meta or {}
    board = leaderboard(chain, runs_dir)
    board.to_csv(os.path.join(output_dir, "leaderboard.csv"), index=False)
    runs = chain.runs(before=selection_before)
    if not runs.empty:
        runs.drop(columns=["dir"]).to_csv(os.path.join(output_dir, "selection.csv"), index=False)
    wf = walk_forward(chain, board, runs_dir) if (walkforward and not board.empty) else pd.DataFrame()
    if not wf.empty:
        wf.to_csv(os.path.join(output_dir, "walkforward.csv"), index=False)

    split = meta.get("split", {})
    lines: List[str] = [
        f"# Event report — {chain.ticker} `{chain.event.column}`",
        "",
        f"**Event:** {chain.event.describe()}. **Window** d={dataset.lookback}, "
        f"**purge** {split.get('purge_gap_rows')} samples. **Table** "
        f"`{chain.schema}.{chain.table}` → dataset `{dataset.name}` (hash `{dataset.hash}`).",
        "",
        "| split | label dates | samples | positives | base rate |",
        "|---|---|---|---|---|",
    ]
    for s in ("train", "val", "test"):
        ys = getattr(dataset, f"y_{s}")
        rng = split.get("date_ranges", {}).get(s, ["?", "?"])
        lines.append(f"| {s} | {rng[0]} → {rng[1]} | {len(ys):,} | {int(ys.sum()):,} | {ys.mean():.3f} |")
    lines += ["", f"Feature selection read only rows before **{chain.holdout_start()}** "
                  f"(the val start). Features in the model: **{dataset.n_features}**.", ""]
    if not runs.empty:
        lines += ["## Selection, one pool per run (null = block-shuffled re-selections)", "",
                  "| pool | channels | kept | CV IC | null p95 | null max | z | clears |",
                  "|---|---|---|---|---|---|---|---|"]
        for _, r in runs.sort_values("z", ascending=False, na_position="last").iterrows():
            lines.append(f"| {r['tables']} | {_fmt(r['channels'])} | {_fmt(r['kept'])} | {_fmt(r['ic'], 4)} | "
                         f"{_fmt(r['null_bar'], 4)} | {_fmt(r['null_max'], 4)} | {_fmt(r['z'], 2)} | "
                         f"{'yes' if r['clears'] else 'no'} |")
        lines.append("")
    best = best_on_val(board) if not board.empty else None
    if best is not None:
        lines += [
            "## Leaderboard — sorted by VAL ROC-AUC (chosen on val, test read once)", "",
            "| model | val AUC | test AUC | test null p95 / max | test z | test PR-AUC (lift) | "
            "test P@10% (lift) | test P@5% | Brier skill | val-thr signals / precision / recall |",
            "|---|---|---|---|---|---|---|---|---|---|",
        ]
        for _, r in board.iterrows():
            lines.append(
                f"| `{r['run_name'].split('__')[0]}` | {_fmt(r.get('val_auc'))} | **{_fmt(r.get('test_auc'))}** | "
                f"{_fmt(r.get('test_auc_bar'))} / {_fmt(r.get('test_auc_null_max'))} | {_fmt(r.get('test_auc_z'), 2)} | "
                f"{_fmt(r.get('test_pr_auc'))} ({_fmt(r.get('test_pr_lift'), 2)}x) | "
                f"{_fmt(r.get('test_p_at_10'))} ({_fmt(r.get('test_p10_lift'), 2)}x) | {_fmt(r.get('test_p_at_5'))} | "
                f"{_fmt(r.get('test_brier_skill'))} | {_fmt(r.get('test_signals_at_thr'))} / "
                f"{_fmt(r.get('test_precision_at_thr'))} / {_fmt(r.get('test_recall_at_thr'))} |"
            )
        lines += [
            "",
            f"**Best on val:** `{best['run_name'].split('__')[0]}` — val AUC {_fmt(best['val_auc'])}, "
            f"**test AUC {_fmt(best['test_auc'])}** against a block-shuffled null p95 of "
            f"{_fmt(best['test_auc_bar'])} (max {_fmt(best['test_auc_null_max'])}, z {_fmt(best['test_auc_z'], 2)}); "
            f"test PR-AUC {_fmt(best['test_pr_auc'])} on a base rate of {_fmt(best['test_base_rate'])}; "
            f"of the {_fmt(best['test_signals_at_thr'])} test sessions over the val threshold, "
            f"{_fmt(best['test_precision_at_thr'])} were followed by the event.",
            "",
        ]
    if not wf.empty:
        lines += ["## Walk-forward — yearly expanding refits over years the selection never read", "",
                  "| model | year | train n | test n | positives | base rate | AUC | PR-AUC |",
                  "|---|---|---|---|---|---|---|---|"]
        for _, r in wf.iterrows():
            lines.append(f"| `{r['model']}` | {r['year']} | {r['train_n']:,} | {r['test_n']:,} | "
                         f"{r['positives']} | {r['base_rate']:.3f} | {r['auc']:.3f} | {r['pr_auc']:.3f} |")
        lines.append("")
    lines += [
        "## Read before quoting",
        "",
        f"- `n_eff` ≈ samples / h: the test split carries ~{_fmt(len(dataset.y_test) / chain.horizon, 0)} "
        "independent observations, and fewer positives.",
        "- The null prices ONE run; the grid is several runs (NUL-1). Quote the val-chosen row.",
        "- The base rate drifts across splits (see the first table), so a fixed probability "
        "threshold does not travel; the val-F1 threshold is reported for that reason.",
    ]
    path = os.path.join(output_dir, "report.md")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return {
        "text": "\n".join(lines) + f"\n\nwritten: {path}",
        "path": path,
        "output_dir": output_dir,
        "board": board,
        "selection": runs,
        "walkforward": wf,
    }
