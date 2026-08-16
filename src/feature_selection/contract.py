# src\feature_selection\contract.py
"""The handoff `feature_selection` → `final_features`, defined ONCE, imported by both.

    feature_selection.run          writes  reports/<run>/metadata.json
    feature_selection.outstanding  writes  reports/<run>/outstanding.csv
    final_features.builder         reads   BOTH, and builds a table from them

Added 2026-08-15. Before it, each side carried its own copy of what the other
promised: `final_features.SETUP_KEYS` named nine `metadata.json` knobs and
`CUT_KEYS` two `outstanding.csv` columns, while `feature_selection.outstanding`
listed its columns in `COLUMNS` and nothing compared the two lists. **Both are here
now and both sides import them**, so a column added on one side and not the other
is an import error rather than a discovery three stages later.

## ⚠️ What was actually broken when this was written, measured on the archive

**1. `source_table` was a GUESS, and it has run out of pools.**
`outstanding.source_table` mapped a channel to its pool from a hard-coded 27-name
`pool__basic` frozenset, a `pool__economy_<country>` name prefix, and a fallback to
whichever of `pool__ta`/`pool__fa` was in the run. Measured against the pools that
exist today:

| channel | run's tables | mapped to | truth |
|---|---|---|---|
| `hose__vnindex__close_adjust` | `pool__stock_market` | **`unknown`** | `pool__stock_market` |
| `vn10y__bonds__close` | `pool__bonds` | **`unknown`** | `pool__bonds` |
| `saxo__usdvnd__close` | `pool__forex_saxo` | **`unknown`** | `pool__forex_saxo` |
| `hose__acb__close_adjust` | `pool__basic_bank` | **`unknown`** | `pool__basic_bank` |
| `saxo__usdvnd__close` | `pool__ta` + `pool__forex_saxo` | **`pool__ta`** ⚠️ | `pool__forex_saxo` |

`unknown` makes `final_features.plan_from_reports` raise; the last row is worse,
because it does not — it names a real table that does not hold that column, and the
error arrives from PostgreSQL at `--apply` time. So the map is now RECORDED at read
time by `UnifiedSchemaReader.join`, which is the only place that knows the answer,
and `metadata.json` carries `input.columns_by_table`. The name-based guess survives
only as the fallback for the 19 folders archived before this existed.

**2. A finished run that never wrote `outstanding.csv` was SILENTLY INVISIBLE.**
`_read_outstanding` skipped any folder without one. Measured 2026-08-15: the two
newest runs (`…__basic__return_5day`, `…__basic__close_adjust_5day` — both produced
through `kaggle_gpu`, which merges a run folder and prints a reminder rather than
writing the shortlist) had none, so `final_features` planned 19 runs and reported no
error. `missing_shortlists` finds them and the reader now raises. Same rule as
CLAUDE.md §5 rule 12: silence is never how something gets left out.

## ⚠️ Why this module lives in `feature_selection` and not in `final_features`

`final_features` already imports `feature_selection` (the reader, the report root,
the shortlist filename). The reverse import would be a cycle, and the direction is
right anyway: the producer states its promise, and the consumer checks the promise it
was handed.
"""

from __future__ import annotations

import json
import os
import re
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

# ---------------------------------------------------------------- the file names

# The shortlist one run writes. ⚠️ `feature_selection.outstanding` re-exports this as
# `OUTSTANDING_FILENAME`, which `pipeline.stages` and `train_test_creator` import —
# the name they know is unchanged; there is simply one definition of the string now.
SHORTLIST_FILENAME = "outstanding.csv"

# The run's own record. Its absence is what marks a directory as "not a run folder".
METADATA_FILENAME = "metadata.json"


# ------------------------------------------------------- what the consumer reads

# From `metadata.json`'s `setup` block: what makes two runs THE SAME EXPERIMENT, and
# therefore what `final_features` groups on. ⚠️ Every knob here can move a selection,
# so a difference in any of them means two runs' channels do not belong in one table.
SETUP_KEYS: Tuple[str, ...] = (
    "lookback_d", "horizon_h", "normalize", "feature_normalize",
    "corr_threshold", "n_splits", "min_train", "random_state", "selector_class",
    "methods", "design_dtype",
)

# ⚠️ **`methods` JOINED THIS TUPLE 2026-08-16 — issue MTH-1, and it is the reason
# `LEGACY_SETUP_DEFAULTS` below exists.** The default ensemble narrowed from six
# rankers to three that day (CONTEXT §19), which changes which channels a run keeps —
# so two runs from opposite sides of the change are NOT the same experiment. It was
# left out of this tuple until now for one good reason: every run archived before the
# change records no `methods` at all, and an absent SETUP_KEY raises. That is what the
# defaults table solves, so the key can be grouped on without orphaning the archive.

# The value a run that never recorded its ensemble reads as. ⚠️ **NOT the six.** Those
# runs did use all six — that was the default — but CLAUDE.md §5 rule 2 is that an
# absent measurement is recorded as absent, never inferred. Writing "the six" here
# would turn a reasonable guess into a fact on disk that nothing measured, and would
# silently merge a legacy run with a deliberate `methods=ALL_METHODS` reproduction.
# Those two are distinguishable and must stay so.
METHODS_UNRECORDED = "unrecorded"

# SETUP_KEYS a pre-existing run may legitimately omit, and what to read instead. ⚠️
# Anything added here weakens the grouping for old runs, so it takes a measured reason
# and a comment. Adding a key to SETUP_KEYS WITHOUT an entry here is still the default
# and still correct — it rejects the archive loudly rather than grouping it wrongly.
LEGACY_SETUP_DEFAULTS: Dict[str, object] = {
    "methods": METHODS_UNRECORDED,
    # ⚠️ `design_dtype` joined SETUP_KEYS 2026-08-16 alongside the universe run that
    # needed `float32` (`windows.window_design`: 4.03 GB per million rows measured,
    # so ALL's 2.39 M rows need 9.6 GB against 7.1 GB free). Unlike `methods`, the
    # absent value here IS inferable and is inferred: `float64` was not merely the
    # default before that day, it was the ONLY thing the code could produce — there
    # was no parameter. Recording a fact the code made certain is not a guess.
    "design_dtype": "float64",
}


def canonical_methods(value: object) -> str:
    """`methods` as a sorted, comma-joined string — the form the grouping compares.

    ⚠️ **Sorted because the ensemble is order-insensitive and the grouping must be
    too.** `FeatureSelector` combines its rankers as `ranks[methods].mean(axis=1)`
    (`selector.py`), so `("spearman", "xgb_shap")` and `("xgb_shap", "spearman")`
    produce bit-identical output — they are one experiment. Comparing the raw strings
    would split them into two tables and report a difference that does not exist.

    `metadata.json` keeps the ORDER THE RUN USED, which is the honest record; the sort
    happens here, at the comparison, not at the write.
    """
    if value is None:
        return METHODS_UNRECORDED
    if isinstance(value, str):
        parts = [p.strip() for p in value.split(",")]
    else:
        parts = [str(p).strip() for p in value]
    parts = [p for p in parts if p]
    return ", ".join(sorted(parts)) if parts else METHODS_UNRECORDED


def normalise_setup(setup: Dict) -> Dict:
    """One run's `setup`, with legacy gaps filled and `methods` canonicalised.

    ⚠️ **Both sides of the handoff must call this before comparing two setups** —
    `final_features` groups on the result and `validate_shortlist` checks it. Doing it
    in one place is the whole point of this module: the alternative is `final_features`
    filling a default that `feature_selection` does not know about.

    Does not mutate the caller's dict, and never invents a value for a key that is
    absent from `LEGACY_SETUP_DEFAULTS` — those still surface as missing.
    """
    filled = dict(setup or {})
    for key, default in LEGACY_SETUP_DEFAULTS.items():
        if filled.get(key) is None:
            filled[key] = default
    if "methods" in filled:
        filled["methods"] = canonical_methods(filled["methods"])
    return filled

# ⚠️ **`max_features` is deliberately NOT here** (removed 2026-08-09, issue STL-1).
# It was `12` in every pre-2026-08-10 `metadata.json` and it has not determined a
# shortlist since `selection_cut` replaced the fixed cap with a per-run measured cut —
# the same runs kept 10 to 236 channels. Grouping on it recorded a number that was no
# longer true of anything. The parameters that DO determine the cut are these, and
# they are stamped into the SHORTLIST by `feature_selection.outstanding` because the
# cut runs after `metadata.json` is written:
CUT_KEYS: Tuple[str, ...] = ("cut_fdr_q", "cut_corr_threshold")

# Alias kept for `final_features`, which reads these keys from a different file than
# the rest of the setup and says so in its own grouping code.
SETUP_FROM_SHORTLIST: Tuple[str, ...] = CUT_KEYS

# The columns of `outstanding.csv` that `final_features` actually reads. It is a
# SUBSET of `outstanding.COLUMNS` on purpose: the shortlist carries diagnostics
# (`coverage`, `beat_in_tie`, `kept_by`) that no consumer needs and that must stay
# free to change without breaking the handoff.
#
#   channel/source_table/schema  → the SELECT and the FROM of the generated SQL
#   target/lookback_d/horizon_h  → the grouping key and the table NAME
#   run_id/evidence              → the provenance COMMENT
REQUIRED_SHORTLIST_COLUMNS: Tuple[str, ...] = (
    "channel", "source_table", "schema",
    "target", "lookback_d", "horizon_h",
    "run_id", "evidence",
) + CUT_KEYS

# What `source_table` says when it cannot answer. ⚠️ A guess would be worse: a wrong
# table that EXISTS produces SQL that runs, and a channel read out of the wrong pool
# is a silent data error. This value makes `final_features` raise.
UNKNOWN_SOURCE = "unknown"


# ------------------------------------------------------------- SQL identifiers

# Anything interpolated into SQL as an identifier is matched against this first —
# schema, table and column names cannot be bound parameters. Same pattern as
# `unified_reader.TICKER_PATTERN`, same reason.
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# ⚠️ PostgreSQL truncates identifiers past this SILENTLY, which would collide two
# tables into one. Checked rather than trusted.
MAX_IDENTIFIER_BYTES = 63


def identifier_problem(name: str, what: str) -> Optional[str]:
    """Why `name` cannot be interpolated into SQL, or None if it can."""
    if not IDENTIFIER.match(name or ""):
        return f"{what} {name!r} is not a bare SQL identifier"
    if len(name.encode()) > MAX_IDENTIFIER_BYTES:
        return (
            f"{what} {name!r} is {len(name.encode())} bytes; PostgreSQL truncates "
            f"past {MAX_IDENTIFIER_BYTES} and two truncated names can collide"
        )
    return None


def identifier(name: str, what: str) -> str:
    """`name`, or a `ValueError` naming what it was supposed to be."""
    problem = identifier_problem(name, what)
    if problem:
        raise ValueError(problem + ".")
    return name


# ------------------------------------------------------------- the two checks


def validate_shortlist(
    frame: pd.DataFrame,
    setup: Optional[Dict] = None,
    label: str = SHORTLIST_FILENAME,
) -> List[str]:
    """Everything wrong with one run's shortlist, as sentences. Empty list = fine.

    Returns rather than raises, so a caller writing twenty files can finish the
    nineteen good ones and then report all the bad ones at once — CLAUDE.md §5
    rule 20, one level up: finish the unit, put it on disk, then describe it.

    Args:
        frame: the built shortlist, before it is written.
        setup: the run's `metadata.json` `setup` block, when the caller has it. The
            SETUP_KEYS check is skipped when it is None rather than guessed at.
        label: what to call the thing in the messages — a folder name, usually.
    """
    problems: List[str] = []

    if frame is None or frame.empty:
        return [f"{label}: the shortlist is EMPTY — no channel survived the cut."]

    missing = [c for c in REQUIRED_SHORTLIST_COLUMNS if c not in frame.columns]
    if missing:
        problems.append(
            f"{label}: missing column(s) {missing} that final_features reads. "
            f"Re-run `python -m feature_selection.outstanding`."
        )

    # ⚠️ The one that has actually fired. See the module docstring's table.
    if "source_table" in frame.columns:
        unknown = sorted(set(frame.loc[frame["source_table"] == UNKNOWN_SOURCE, "channel"]))
        if unknown:
            problems.append(
                f"{label}: {len(unknown)} channel(s) have source_table="
                f"{UNKNOWN_SOURCE!r} and final_features cannot build a FROM clause "
                f"for them: {unknown[:5]}{' …' if len(unknown) > 5 else ''}. The run's "
                f"metadata.json carries no input.columns_by_table, so the pool had to "
                f"be guessed from the channel NAME — re-run the selection so the map "
                f"is recorded, or extend outstanding.source_table."
            )

    for column, what in (
        ("channel", "channel"), ("source_table", "source table"), ("schema", "schema")
    ):
        if column not in frame.columns:
            continue
        for value in sorted({str(v) for v in frame[column]}):
            problem = identifier_problem(value, what)
            if problem:
                problems.append(f"{label}: {problem} — it is interpolated into SQL.")

    # A run is one schema and one target by construction. Two of either means the
    # file describes more than one experiment, and `final_features` would split it
    # across groups without saying so.
    for column in ("schema", "target"):
        if column in frame.columns and frame[column].nunique(dropna=False) > 1:
            problems.append(
                f"{label}: {frame[column].nunique()} distinct {column} values in one "
                f"run's shortlist ({sorted(set(frame[column]))}) — a run is one "
                f"{column}."
            )

    if setup is not None:
        # ⚠️ Normalised FIRST, so a key with a documented legacy default (`methods`,
        # issue MTH-1) is not reported as broken — it has a defined reading. Every
        # other absence still surfaces here, which is the point.
        absent = [k for k in SETUP_KEYS if k not in normalise_setup(setup)]
        if absent:
            problems.append(
                f"{label}: metadata.json setup is missing {absent}, which "
                f"final_features groups on. The run predates those keys; it cannot "
                f"be grouped with runs that have them."
            )
    return problems


def run_folders(root: str) -> List[str]:
    """Every directory under `root` that is a feature-selection run folder."""
    if not os.path.isdir(root):
        return []
    return [
        name
        for name in sorted(os.listdir(root))
        if os.path.exists(os.path.join(root, name, METADATA_FILENAME))
    ]


def missing_shortlists(root: str) -> List[str]:
    """Run folders that finished but never wrote `outstanding.csv`.

    ⚠️ **These are the runs a consumer cannot see.** They are not a failure state on
    disk — every other artefact is there — which is exactly why they went unnoticed:
    a report folder full of figures and a `metadata.json` looks like a complete run,
    and `final_features` skipped it without a word. See the module docstring.
    """
    return [
        name
        for name in run_folders(root)
        if not os.path.exists(os.path.join(root, name, SHORTLIST_FILENAME))
    ]


def read_setup(root: str, folder: str) -> Dict:
    """One run's `metadata.json` `setup` block."""
    path = os.path.join(root, folder, METADATA_FILENAME)
    with open(path, encoding="utf-8") as handle:
        return json.load(handle).get("setup", {})


def describe(root: str) -> pd.DataFrame:
    """One row per run folder: does it satisfy the contract, and if not, why.

        python -c "from feature_selection import contract; print(contract.describe('reports/feature_selection'))"

    The read-only view of the handoff — what `final_features` will see when it next
    runs, without running it.
    """
    rows = []
    for folder in run_folders(root):
        path = os.path.join(root, folder, SHORTLIST_FILENAME)
        if not os.path.exists(path):
            rows.append({
                "run": folder, "channels": 0, "ok": False,
                "problem": f"no {SHORTLIST_FILENAME} — run "
                           f"`python -m feature_selection.outstanding`",
            })
            continue
        frame = pd.read_csv(path)
        problems = validate_shortlist(frame, read_setup(root, folder), label=folder)
        rows.append({
            "run": folder,
            "channels": len(frame),
            "ok": not problems,
            "problem": "; ".join(p.split(": ", 1)[-1] for p in problems),
        })
    return pd.DataFrame(rows)
