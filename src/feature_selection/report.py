# src\feature_selection\report.py
"""One selection run → one self-describing folder on disk.

`FeatureSelector.run()` returns a `SelectionResult` that lives in memory and dies
with the kernel. This turns it into an artefact: tables as CSV, figures as PNG, and
a `metadata.json` that records **everything needed to reproduce the run and to know
what it may be compared against.**

    report = write_report(result, selector=selector, panel=panel,
                          schema="unified_schema_vcb",
                          tables=["pool__basic", "pool__targets"])
    report.path          # reports/feature_selection/2026-08-04__vcb__return_5day__<id>

## ⚠️ Why the metadata is this long

Because §8 of CONTEXT.md is a list of ways two runs can look comparable and not be.
A ranking is meaningless without the target, the representation, the purge gap and
the null that was current when it was produced — `zscore` moved its own bar 43 %
without touching the data, and `device` changes the kept set outright. A CSV of
feature names with no setup beside it is the artefact that gets quoted a month later
against a different configuration. **So the setup travels with the numbers, in the
same folder, or the folder is not worth writing.**

## What lands, and what each file answers

| file | answers |
|---|---|
| `metadata.json` | what was run, on what, with which knobs, and what came out |
| `README.md` | the same thing, for a human, in ~40 lines |
| `feature_importance.csv` | **the deliverable** — per channel, all six methods + ensemble rank + kept flag |
| `design_scores.csv` | per `(channel, stat)` — the detail the channel scores aggregate by MAX |
| `validation.csv` | per fold, per feature set: IC, R², hit rate, `n_eff` |
| `target_correlation.csv` | signed Spearman of each channel against the target |
| `channel_correlation.csv` | the feature↔feature matrix the redundancy prune used |
| `stability.csv` | per-fold SHAP rank, when `stability=True` |
| `figures/*.png` | the same tables, seen |

## ⚠️ Three things this does NOT do

**It does not write to the database.** CONTEXT §1: a selection is a result object
and a set of figures, not a table. That rule is unchanged — this writes to a
filesystem path the caller chooses.

**It does not decide whether the run is any good.** `metadata.json` carries the null
and the holdout **if they were computed** and records `null: null` if they were not.
An absent null is reported as absent, never as a pass — §8's whole point.

**It does not re-run anything.** Everything written is read off the result object,
so a report costs seconds and can be produced for a run that already happened.
"""

import json
import os
import platform
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence

import matplotlib
import numpy as np
import pandas as pd

from feature_selection import plots

# Where a report lands unless the caller says otherwise. Anchored to the REPO ROOT,
# not the CWD — a notebook in `src/feature_selection` and a script at the root must
# write to the same place, which is the same trap `unified_reader.DEFAULT_LOG_FILE`
# documents.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_REPORT_ROOT = os.path.join(REPO_ROOT, "reports", "feature_selection")

# Bumped when the folder layout or the metadata schema changes, so a consumer can
# tell a v1 report from a v2 one instead of guessing from which keys are present.
REPORT_SCHEMA_VERSION = "1.0"


def _git_commit() -> Optional[str]:
    """The commit the run was produced at, or None outside a repo.

    ⚠️ Recorded because the pipeline is under active development: a ranking from
    before the cross-sectional hooks landed is not comparable with one from after,
    and the commit is the only thing that says which is which.
    """
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=REPO_ROOT, capture_output=True, text=True, timeout=10,
        )
        if out.returncode == 0:
            dirty = subprocess.run(
                ["git", "status", "--porcelain"],
                cwd=REPO_ROOT, capture_output=True, text=True, timeout=10,
            )
            suffix = "+dirty" if dirty.stdout.strip() else ""
            return out.stdout.strip() + suffix
    except Exception:  # noqa: BLE001 - a missing git is not a failed report
        pass
    return None


def _versions() -> Dict[str, str]:
    """The libraries whose behaviour can move a number here."""
    out = {"python": sys.version.split()[0], "platform": platform.platform()}
    for name in ("numpy", "pandas", "scipy", "sklearn", "xgboost", "matplotlib"):
        try:
            module = __import__(name)
            out[name] = getattr(module, "__version__", "?")
        except ImportError:
            out[name] = "not installed"
    return out


def _json_safe(value):
    """NumPy scalars, Timestamps and NaN into something `json.dump` accepts."""
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    if isinstance(value, pd.Series):
        return _json_safe(value.to_dict())
    if isinstance(value, float) and np.isnan(value):
        return None
    return value


def _slug(text: str) -> str:
    keep = [c if c.isalnum() else "_" for c in str(text).lower()]
    return "".join(keep).strip("_")[:40] or "run"


@dataclass
class Report:
    """Where a report landed and what it holds."""

    path: str
    run_id: str
    metadata: Dict
    files: List[str] = field(default_factory=list)

    @property
    def importance_csv(self) -> str:
        return os.path.join(self.path, "feature_importance.csv")

    @property
    def figures(self) -> List[str]:
        directory = os.path.join(self.path, "figures")
        if not os.path.isdir(directory):
            return []
        return [os.path.join(directory, f) for f in sorted(os.listdir(directory))]

    def summary(self) -> pd.Series:
        """The headline numbers, for printing at the end of a notebook cell."""
        results = self.metadata.get("results", {})
        ic = results.get("ic_summary") or {}
        null = self.metadata.get("null")
        return pd.Series(
            {
                "run_id": self.run_id,
                "schema": self.metadata["input"]["schema"],
                "tables": ", ".join(self.metadata["input"]["tables"]),
                "target": self.metadata["target"]["name"],
                "rows": self.metadata["input"]["panel_rows"],
                "channels": results.get("n_channels"),
                "kept": results.get("n_kept"),
                "ic_mean": ic.get("ic_mean"),
                "ic_trend": ic.get("ic_trend_per_fold"),
                "hit_rate": ic.get("hit_rate"),
                "null_bar": (null or {}).get("null_p95_BAR"),
                "clears_null": (null or {}).get("clears_bar"),
                "files": len(self.files),
                "path": self.path,
            },
            name="report",
        )

    def __repr__(self) -> str:  # pragma: no cover - convenience only
        return f"<Report {self.run_id} at {self.path} ({len(self.files)} files)>"


# ------------------------------------------------------------------- the figures


def _save(fig, directory: str, name: str, written: List[str]) -> None:
    path = os.path.join(directory, name)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    matplotlib.pyplot.close(fig)
    written.append(path)


def write_figures(
    result,
    directory: str,
    top: int = 30,
    null=None,
    y: Optional[pd.Series] = None,
) -> List[str]:
    """Every figure the result can support, numbered in reading order.

    ⚠️ **A figure that would say nothing is skipped, not drawn empty.** There is no
    stability heatmap when `stability=False`, no coverage chart when every channel
    is 100 % complete, and no null histogram when no null was run — an empty axis
    labelled "null" is worse than no file, because a reader assumes it was tested.
    """
    import matplotlib.pyplot as plt

    os.makedirs(directory, exist_ok=True)
    written: List[str] = []
    plots.use_theme()

    fig, ax = plt.subplots(figsize=(9, max(3.5, 0.30 * min(top, len(result.features)))))
    plots.plot_ensemble_ranking(result, top=top, ax=ax)
    _save(fig, directory, "01_ensemble_ranking.png", written)

    fig, ax = plt.subplots(figsize=(9, max(3.5, 0.32 * min(top, len(result.features)))))
    plots.plot_method_heatmap(result.scores, top=top, ax=ax)
    _save(fig, directory, "02_method_heatmap.png", written)

    fig, ax = plt.subplots(figsize=(9, max(3.5, 0.30 * min(top, len(result.features)))))
    plots.plot_target_correlation(result.target_corr, result.target, top=top, ax=ax)
    _save(fig, directory, "03_target_correlation.png", written)

    if not result.corr.empty:
        size = max(5.0, 0.34 * min(top, result.corr.shape[0]))
        fig, ax = plt.subplots(figsize=(size + 1.5, size))
        plots.plot_correlation_heatmap(result.corr, top=top, ax=ax)
        _save(fig, directory, "04_channel_correlation.png", written)

    # ⚠️ Only meaningful once the window exists — at lookback=1 every channel is
    # carried by `last` by construction and the chart is a single column.
    if result.lookback > 1 and not result.design_scores.empty:
        fig, ax = plt.subplots(figsize=(7, max(3.5, 0.30 * min(20, len(result.features)))))
        plots.plot_stat_profile(result, method="xgb_shap", top=20, ax=ax)
        _save(fig, directory, "05_stat_profile.png", written)

    if not result.validation.empty:
        fig, ax = plt.subplots(figsize=(9, 4))
        plots.plot_validation(result.validation, ax=ax)
        _save(fig, directory, "06_validation.png", written)

    if not result.stability.empty:
        fig, ax = plt.subplots(figsize=(8, max(3.5, 0.30 * min(top, len(result.features)))))
        plots.plot_stability_heatmap(result.stability, top=top, ax=ax)
        _save(fig, directory, "07_stability.png", written)

    if len(result.coverage) and float(result.coverage.min()) < 1.0:
        incomplete = int((result.coverage < 1.0).sum())
        fig, ax = plt.subplots(figsize=(8, max(2.2, 0.28 * incomplete)))
        plots.plot_coverage(result.coverage, ax=ax)
        _save(fig, directory, "08_coverage.png", written)

    if y is not None and len(y):
        fig, ax = plt.subplots(figsize=(8, 3.6))
        plots.plot_target_distribution(y, result.target, ax=ax)
        _save(fig, directory, "09_target_distribution.png", written)

    if null is not None and len(getattr(null, "draws", [])):
        fig, ax = plt.subplots(figsize=(8, 4))
        plots.plot_null(null, ax=ax)
        _save(fig, directory, "10_null.png", written)

    return written


# ------------------------------------------------------------------- the metadata


def build_metadata(
    result,
    selector=None,
    panel: Optional[pd.DataFrame] = None,
    schema: str = "",
    tables: Sequence[str] = (),
    database: str = "",
    join_log: Optional[List[Dict]] = None,
    null=None,
    holdout: Optional[pd.DataFrame] = None,
    universe: Optional[Sequence[str]] = None,
    notes: str = "",
    extra: Optional[Dict] = None,
) -> Dict:
    """Everything about the run, as a JSON-serialisable dict.

    ⚠️ `null=None` becomes `"null": null` in the file — **an absent null is recorded
    as absent.** It is never omitted, because a missing key reads as "not applicable"
    and a null that was never computed is not the same as one that was not needed.
    """
    from feature_selection import evaluation

    date_col = getattr(selector, "date_col", "date")
    target = result.target

    panel_info: Dict = {
        "database": database,
        "schema": schema,
        "tables": list(tables),
        "join_log": join_log or [],
        "panel_rows": int(len(panel)) if panel is not None else None,
        "panel_columns": int(panel.shape[1]) if panel is not None else None,
    }
    if panel is not None and date_col in panel.columns:
        panel_info["first_date"] = str(pd.Timestamp(panel[date_col].min()).date())
        panel_info["last_date"] = str(pd.Timestamp(panel[date_col].max()).date())
        panel_info["sessions"] = int(panel[date_col].nunique())
    if panel is not None and "ticker" in panel.columns:
        panel_info["tickers"] = int(panel["ticker"].nunique())
    if universe is not None:
        panel_info["universe_size"] = len(universe)
        panel_info["universe"] = list(universe)

    target_info: Dict = {
        "name": target,
        "horizon_days": int(result.horizon),
        "definition": (
            f"forward {result.horizon}-session return on close_adjust"
            if target.startswith("return_") and "rel" not in target
            else f"{target} (see cross_sectional.py / DataPreprocessor for the definition)"
        ),
    }
    if panel is not None and target in panel.columns:
        column = pd.to_numeric(panel[target], errors="coerce")
        target_info.update(
            {
                "labelled_rows": int(column.notna().sum()),
                "unlabelled_rows": int(column.isna().sum()),
                "mean": float(column.mean()),
                "sd": float(column.std()),
                "min": float(column.min()),
                "max": float(column.max()),
            }
        )

    setup = {k: _json_safe(v) for k, v in result.setup.to_dict().items()}
    if selector is not None:
        setup.update(
            {
                "max_features": getattr(selector, "max_features", None),
                "corr_threshold": getattr(selector, "corr_threshold", None),
                "n_splits": getattr(selector.cv, "n_splits", None),
                "min_train": getattr(selector.cv, "min_train", None),
                "random_state": getattr(selector, "random_state", None),
                "subsample": getattr(selector, "subsample", None),
                "colsample_bytree": getattr(selector, "colsample_bytree", None),
                "permutation_repeats": getattr(selector, "permutation_repeats", None),
                "device_preference": getattr(selector, "device_preference", None),
                "selector_class": type(selector).__name__,
                # ⚠️ Cross-sectional runs only. `None` here means the run was a
                # single time series, which is the difference §9b turns on.
                "panel_col": getattr(selector, "panel_col", None),
                "feature_normalize": getattr(selector, "feature_normalize", None),
            }
        )

    results: Dict = {
        "n_channels": len(result.features),
        "n_kept": len(result.kept),
        "kept": list(result.kept),
        "dropped_constant": list(result.dropped_constant),
        "dropped_correlated": dict(result.dropped_correlated),
        "dead_methods": list(result.dead_methods),
        "top_10_by_ensemble": list(result.features[:10]),
        "timings_seconds": {k: round(v, 2) for k, v in result.timings.items()},
    }
    if not result.validation.empty:
        results["ic_summary"] = _json_safe(
            evaluation.ic_summary(result.validation, result.horizon).to_dict()
        )
        results["ic_summary_all_channels"] = _json_safe(
            evaluation.ic_summary(
                result.validation, result.horizon, feature_set="all channels"
            ).to_dict()
        )

    metadata = {
        "report_schema_version": REPORT_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "git_commit": _git_commit(),
        "environment": _versions(),
        "input": panel_info,
        "target": target_info,
        "setup": setup,
        "results": results,
        # ⚠️ Explicitly null when not computed. See the docstring.
        "null": _json_safe(null.summary().to_dict()) if null is not None else None,
        "holdout": (
            _json_safe(holdout.to_dict(orient="records"))
            if holdout is not None and not holdout.empty
            else None
        ),
        "notes": notes,
    }
    if extra:
        metadata["extra"] = _json_safe(extra)
    return metadata


# --------------------------------------------------------------------- the README


def _readme(metadata: Dict, result, files: Sequence[str]) -> str:
    inp, tgt, setup, res = (
        metadata["input"], metadata["target"], metadata["setup"], metadata["results"]
    )
    ic = res.get("ic_summary") or {}
    null = metadata.get("null")

    lines = [
        f"# Feature-importance run — `{tgt['name']}`",
        "",
        f"*Generated {metadata['generated_at']} at commit "
        f"`{metadata['git_commit'] or 'unknown'}`.*",
        "",
        "## Input",
        "",
        f"- **schema** `{inp['schema']}`"
        + (f" (database `{inp['database']}`)" if inp.get("database") else ""),
        f"- **tables** {', '.join('`' + t + '`' for t in inp['tables'])}",
        f"- **panel** {inp.get('panel_rows'):,} rows x {inp.get('panel_columns')} columns"
        if inp.get("panel_rows") else "- **panel** —",
    ]
    if inp.get("first_date"):
        lines.append(
            f"- **range** {inp['first_date']} to {inp['last_date']} "
            f"({inp.get('sessions')} sessions"
            + (f", {inp['tickers']} tickers" if inp.get("tickers") else "")
            + ")"
        )
    lines += [
        "",
        "## Target",
        "",
        f"- **`{tgt['name']}`** — {tgt['definition']}",
        f"- horizon **{tgt['horizon_days']}** sessions; "
        f"{tgt.get('labelled_rows', 0):,} labelled, "
        f"{tgt.get('unlabelled_rows', 0):,} unlabelled",
    ]
    if tgt.get("sd") is not None:
        lines.append(
            f"- mean {tgt['mean']:+.5f}, sd {tgt['sd']:.5f}, "
            f"range {tgt['min']:+.4f} to {tgt['max']:+.4f}"
        )

    lines += [
        "",
        "## Setup",
        "",
        "| knob | value |",
        "|---|---|",
    ]
    for key in (
        "selector_class", "lookback_d", "horizon_h", "normalize", "feature_normalize",
        "panel_col", "purge_gap_rows", "window_stats", "n_splits", "min_train",
        "max_features", "corr_threshold", "device", "random_state",
        "permutation_repeats", "holdout_start", "dev_samples", "design_columns",
    ):
        if setup.get(key) is not None:
            lines.append(f"| `{key}` | {setup[key]} |")

    lines += [
        "",
        "## Result",
        "",
        f"- **{res['n_kept']} of {res['n_channels']} channels kept** after the "
        f"|rho| >= {setup.get('corr_threshold')} redundancy prune",
        f"- **top 10 by ensemble rank**: {', '.join('`' + f + '`' for f in res['top_10_by_ensemble'])}",
    ]
    if res.get("dead_methods"):
        lines.append(
            f"- ⚠️ **dead methods** (separated nothing): "
            f"{', '.join('`' + m + '`' for m in res['dead_methods'])}"
        )
    if ic:
        lines += [
            "",
            "| metric | selected | all channels |",
            "|---|---|---|",
        ]
        other = res.get("ic_summary_all_channels") or {}
        for key in ("ic_mean", "ic_trend_per_fold", "hit_rate", "n_eff_per_fold"):
            a, b = ic.get(key), other.get(key)
            fmt = lambda v: "—" if v is None else f"{v:+.4f}" if abs(v) < 10 else f"{v:.1f}"
            lines.append(f"| `{key}` | {fmt(a)} | {fmt(b)} |")

    lines += ["", "## The bar", ""]
    if null:
        verdict = "**CLEARS**" if null.get("clears_bar") else "**DOES NOT CLEAR**"
        lines += [
            f"- observed **{null['observed_ic']:+.4f}** against a p95 bar of "
            f"**{null['null_p95_BAR']:+.4f}** ({null['draws']} draws) — {verdict}",
            f"- null mean {null['null_mean']:+.4f}, sd {null['null_sd']:.4f}, "
            f"max {null['null_max']:+.4f}; z = **{null['z_vs_null']:+.2f}**, "
            f"p = {null['p_value']:.4f}",
        ]
    else:
        lines.append(
            "- ⚠️ **NO NULL WAS COMPUTED FOR THIS RUN.** A positive IC is not a "
            "result on its own — see `CONTEXT.md` §6b and §8. Treat the ranking "
            "below as descriptive until a shuffled-label null has been run for "
            "*this* configuration."
        )
    if metadata.get("holdout"):
        lines += ["", "## Holdout", "", "| feature set | labels | IC | hit rate |", "|---|---|---|---|"]
        for row in metadata["holdout"]:
            lines.append(
                f"| {row.get('feature_set')} | {row.get('labels')} | "
                f"{row.get('ic', float('nan')):+.4f} | {row.get('hit_rate', float('nan')):.4f} |"
            )

    lines += ["", "## Files", ""]
    for path in files:
        parent = os.path.basename(os.path.dirname(path))
        name = os.path.basename(path)
        lines.append(f"- `figures/{name}`" if parent == "figures" else f"- `{name}`")
    if metadata.get("notes"):
        lines += ["", "## Notes", "", metadata["notes"]]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------- the entry


def write_report(
    result,
    selector=None,
    panel: Optional[pd.DataFrame] = None,
    schema: str = "",
    tables: Sequence[str] = (),
    database: str = "",
    join_log: Optional[List[Dict]] = None,
    null=None,
    holdout: Optional[pd.DataFrame] = None,
    universe: Optional[Sequence[str]] = None,
    notes: str = "",
    extra: Optional[Dict] = None,
    root: str = DEFAULT_REPORT_ROOT,
    run_id: Optional[str] = None,
    figures: bool = True,
    top: int = 30,
) -> Report:
    """Write one run's tables, figures and metadata to `root/<run_id>/`.

    Args:
        result: the `SelectionResult`.
        selector: the selector that produced it — the source of the knobs that are
            not on the result (`max_features`, `random_state`, the CV shape). Its
            `panel` is also used when `panel` is not given.
        panel: the joined frame, for the input and target sections.
        schema, tables, database, join_log: where the data came from. `join_log`
            comes straight off `UnifiedSchemaReader.join_log` and records **which
            keys each merge actually used** — the thing §3 says not to assume.
        null: an `evaluation.NullResult`, if one was computed. ⚠️ Omitting it is
            recorded as "no null was computed", not as a pass.
        holdout: `selector.score_holdout(result)` output, if it was run.
        universe: the tickers a cross-sectional run covered.
        run_id: defaults to `<date>__<schema>__<target>__<HHMMSS>`.

    Returns:
        A `Report` naming the folder and carrying the metadata dict.
    """
    started = time.perf_counter()
    if panel is None and selector is not None:
        panel = getattr(selector, "panel", None)

    if run_id is None:
        stamp = datetime.now().strftime("%Y-%m-%d__%H%M%S")
        run_id = f"{stamp[:10]}__{_slug(schema or 'panel')}__{_slug(result.target)}__{stamp[-6:]}"
    path = os.path.join(root, run_id)
    figures_dir = os.path.join(path, "figures")
    os.makedirs(path, exist_ok=True)

    written: List[str] = []

    def dump(frame: pd.DataFrame, name: str, index_label: Optional[str] = None) -> None:
        if frame is None or (hasattr(frame, "empty") and frame.empty):
            return
        target_path = os.path.join(path, name)
        frame.to_csv(target_path, index=index_label is not None, index_label=index_label)
        written.append(target_path)

    # ── the deliverable ────────────────────────────────────────────────────────
    # `result.ranking` is already ensemble-sorted, carries `kept` and
    # `dropped_for`, and holds every method's 0-1 score. The signed correlation is
    # joined on because a ranking without a SIGN cannot be read as a strategy.
    importance = result.ranking.join(result.target_corr)
    importance.insert(0, "rank", np.arange(1, len(importance) + 1))
    if not result.best_stat.empty:
        importance = importance.join(
            result.best_stat.add_prefix("best_stat__"), how="left"
        )
    dump(importance, "feature_importance.csv", index_label="channel")

    dump(result.design_scores, "design_scores.csv", index_label="design_column")
    dump(result.validation, "validation.csv")
    dump(result.target_corr.to_frame(), "target_correlation.csv", index_label="channel")
    dump(result.corr, "channel_correlation.csv", index_label="channel")
    dump(result.stability, "stability.csv", index_label="channel")
    if len(result.coverage):
        dump(result.coverage.to_frame("coverage"), "coverage.csv", index_label="channel")
    if holdout is not None and not holdout.empty:
        dump(holdout, "holdout.csv")
    if null is not None and len(getattr(null, "draws", [])):
        dump(
            pd.DataFrame({"draw": np.arange(1, len(null.draws) + 1), "ic": null.draws}),
            "null_draws.csv",
        )

    # ── the figures ────────────────────────────────────────────────────────────
    if figures:
        y = None
        if panel is not None and result.target in panel.columns:
            y = pd.to_numeric(panel[result.target], errors="coerce").dropna()
        written += write_figures(result, figures_dir, top=top, null=null, y=y)

    # ── the metadata, last, so it can list the files ───────────────────────────
    metadata = build_metadata(
        result, selector=selector, panel=panel, schema=schema, tables=tables,
        database=database, join_log=join_log, null=null, holdout=holdout,
        universe=universe, notes=notes, extra=extra,
    )
    metadata["run_id"] = run_id
    metadata["artifacts"] = [
        {
            "file": os.path.relpath(f, path).replace("\\", "/"),
            "bytes": os.path.getsize(f),
        }
        for f in written
    ]
    metadata["report_seconds"] = round(time.perf_counter() - started, 2)

    metadata_path = os.path.join(path, "metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as handle:
        json.dump(_json_safe(metadata), handle, indent=2, ensure_ascii=False)
    written.append(metadata_path)

    readme_path = os.path.join(path, "README.md")
    with open(readme_path, "w", encoding="utf-8") as handle:
        handle.write(_readme(metadata, result, written))
    written.append(readme_path)

    return Report(path=path, run_id=run_id, metadata=metadata, files=written)


def load_report(path: str) -> Dict:
    """Read a report's `metadata.json` back."""
    with open(os.path.join(path, "metadata.json"), encoding="utf-8") as handle:
        return json.load(handle)


def compare_reports(paths: Sequence[str]) -> pd.DataFrame:
    """One row per report — the table for deciding which runs are comparable.

    ⚠️ Built to make INCOMPARABILITY visible. Two runs differing in `normalize`,
    `target` or `lookback_d` are different experiments whose ICs must not be set
    beside each other, and this puts those columns next to the IC so the difference
    is seen before the number is.
    """
    rows = []
    for path in paths:
        meta = load_report(path)
        ic = (meta.get("results") or {}).get("ic_summary") or {}
        null = meta.get("null") or {}
        rows.append(
            {
                "run_id": meta.get("run_id"),
                "schema": meta["input"].get("schema"),
                "target": meta["target"].get("name"),
                "rows": meta["input"].get("panel_rows"),
                "tickers": meta["input"].get("tickers"),
                "lookback_d": meta["setup"].get("lookback_d"),
                "horizon_h": meta["setup"].get("horizon_h"),
                "normalize": meta["setup"].get("normalize"),
                "feature_normalize": meta["setup"].get("feature_normalize"),
                "device": meta["setup"].get("device"),
                "kept": (meta.get("results") or {}).get("n_kept"),
                "ic_mean": ic.get("ic_mean"),
                "hit_rate": ic.get("hit_rate"),
                "null_bar": null.get("null_p95_BAR"),
                "z_vs_null": null.get("z_vs_null"),
                "clears_null": null.get("clears_bar"),
                "commit": meta.get("git_commit"),
            }
        )
    return pd.DataFrame(rows)
