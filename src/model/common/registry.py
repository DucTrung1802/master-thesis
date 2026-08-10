"""Append-only run registry: one row per run in `<runs_dir>/index.csv`.

One shared leaderboard for every model AND task. The **core** columns
(`*_ic`, `*_dir_auc`, `*_hit_rate`, `*_long_short`) are filled by every task,
because `result_evaluator.metrics` computes them from `(score, realised return)` and
nothing else — so a regressor and a classifier are directly comparable on them.
Task-specific columns (`test_RMSE*`, `test_pr_auc`, `test_base_rate`) are blank for
the other task.

⚠️ **The `*_clears` and `*_p` columns are the ones that matter.** A run at the top of
the board on `test_ic` whose `test_ic_clears` is False is the best-ranked noise, not
the best model. See `result_evaluator/metrics.py`.

⚠️ **THE SCHEMA IS NOT DEFINED HERE ANY MORE** (2026-08-10). It is
`result_evaluator.index.INDEX_COLUMNS`, because this module and
`result_evaluator.rebuild_index` are two writers of one file and they used to disagree:
appending wrote `model_type`/`val_ic`/`test_ic`, rebuilding wrote `model`/`ic`/`dir_auc`
and the test split only. `--rebuild-index` therefore silently replaced the leaderboard's
schema. Issue **IDX-1** fixed two metric ERAS inside the file; keeping the column list
in one module is what stops the two writers recreating them. The import runs
`model → result_evaluator`, the direction it already ran.

⚠️ **The header is MIGRATED, never assumed.** `csv.DictWriter` writes fields in
`INDEX_COLUMNS` order; appending to a file whose header is a different order writes rows
that misalign with their own header, silently. `append_run` compares the two and
rewrites the file with the union when they differ, so an index built before a metric
existed keeps all its rows and gains a blank column.
"""

from __future__ import annotations

import csv
import os
from typing import List

from result_evaluator.index import INDEX_COLUMNS

# Kept as a module-level alias so existing readers of `registry._COLUMNS` still work.
_COLUMNS = INDEX_COLUMNS


def _existing_header(path: str) -> List[str]:
    with open(path, newline="", encoding="utf-8") as handle:
        return next(csv.reader(handle), [])


def _migrate(path: str, header: List[str]) -> List[str]:
    """Rewrite `index.csv` under `header + new columns`, keeping every existing row."""
    with open(path, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    merged = header + [c for c in _COLUMNS if c not in header]
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=merged, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in merged})
    return merged


def append_run(runs_dir: str, row: dict) -> str:
    """Append `row` (any subset of the header) to `runs_dir/index.csv`.

    Creates the header on first write; migrates it when `INDEX_COLUMNS` has grown.
    """
    os.makedirs(runs_dir, exist_ok=True)
    path = os.path.join(runs_dir, "index.csv")

    if not os.path.exists(path):
        fields = list(_COLUMNS)
        with open(path, "w", newline="", encoding="utf-8") as handle:
            csv.DictWriter(handle, fieldnames=fields).writeheader()
    else:
        fields = _existing_header(path)
        if any(column not in fields for column in _COLUMNS):
            fields = _migrate(path, fields)

    with open(path, "a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writerow({k: row.get(k, "") for k in fields})
    return path
